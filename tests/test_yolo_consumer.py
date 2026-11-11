"""Тесты yolo-consumer (Triton-only: параллельный декод + async-поток + watermark).

Модель и Kafka замочены: проверяется ЛОГИКА потребителя — min-offset watermark
(коммит не перепрыгивает необработанные in-flight, проваленный кадр не стопорит
его), окно кадров «в полёте» к Triton, публикация только после produce+flush
(at-least-once), очистка состояния при revoke, _committed-гард от повторных
коммитов, poison-счётчики и зомби-порог, а также регресс гонки 2026-11-05:
corr обязан регистрироваться в pending ДО submit_async — быстрый callback,
вызванный синхронно внутри отправки, не имеет права потерять кадр.
"""
import json
import queue
import threading
import time
from collections import Counter, deque
from types import SimpleNamespace
from unittest.mock import MagicMock

import cv2
import numpy as np
import pytest

from app.consumers import yolo_frames as yf
from app.core.frame_envelope import pack
from app.core.metrics import poison_messages_total


def _jpeg() -> bytes:
    """Минимальный валидный JPEG 16x16."""
    ok, buf = cv2.imencode(".jpg", np.full((16, 16, 3), 128, dtype=np.uint8))
    assert ok
    return buf.tobytes()


def _msg(partition: int, offset: int, jpeg_bytes: bytes, stream_id: int = 7,
         frame_index: int | None = None,
         width: int | None = None, height: int | None = None):
    """Фейковое Kafka-сообщение с реальным полезным грузом.

    width/height — поля jpeg-intake (producer кладёт размеры кадра); None —
    старый формат меты без них.
    """
    meta = {
        "frame_index": offset if frame_index is None else frame_index,
        "source": "test",
        "stream_id": stream_id,
        "captured_at": "2026-10-30T12:00:00+00:00",
    }
    if width is not None:
        meta["width"] = width
    if height is not None:
        meta["height"] = height
    payload = pack(meta, jpeg_bytes)
    m = MagicMock()
    m.partition.return_value = partition
    m.offset.return_value = offset
    m.value.return_value = payload
    m.topic.return_value = "raw_frames"
    m.error.return_value = None
    m.timestamp.return_value = (1, int(time.time() * 1000))
    return m


def _meta(frame_index: int, stream_id: int = 1) -> dict:
    return {
        "frame_index": frame_index,
        "stream_id": stream_id,
        "source": "s",
        "captured_at": None,
        "frame_bytes": b"",
    }


def _det(confidence: float = 0.9) -> SimpleNamespace:
    """Объект, похожий на Detection: так же читается в _detection_message."""
    return SimpleNamespace(class_name="person", confidence=confidence,
                           bbox=[1.0, 2.0, 3.0, 4.0])


class FakeConsumer:
    """Выдаёт сообщения из очереди, запоминает store_offsets/commits."""

    def __init__(self, cfg):
        self.queue: deque = deque()
        self.delay_next = 0.0  # сек «дороги» до следующего сообщения (эммуляция)
        self.stored: list[int] = []
        self.commits = 0
        self.closed = False

    def subscribe(self, topics, **callbacks):
        pass

    def poll(self, timeout=None):
        if not self.queue:
            return None
        if self.delay_next:
            time.sleep(self.delay_next)
            self.delay_next = 0.0
        return self.queue.popleft()

    def store_offsets(self, offsets=None, message=None):
        # Как настоящий confluent_kafka: tp.offset — СЛЕДУЮЩИЙ потребляемый offset
        if offsets is not None:
            self.stored.extend(tp.offset for tp in offsets)
        elif message is not None:
            self.stored.append(message.offset() + 1)

    def commit(self, asynchronous=True):
        self.commits += 1

    def close(self):
        self.closed = True


class FakeProducer:
    def __init__(self, cfg):
        self.sent: list[tuple[str, bytes]] = []

    def produce(self, topic, value=None, **kwargs):
        self.sent.append((topic, value))

    def poll(self, timeout=None):
        pass

    def flush(self):
        pass


class FakeAsyncDetector:
    """Детектор с async-контрактом TritonDetector: submit мгновенный, ответ —
    через completion-поток в очередь результатов (как tritonclient.async_infer).

    sync=True — ответ кладётся СИНХРОННО внутри submit_async: так ведёт себя
    быстрый gRPC, у которого done-callback дёргается до возврата отправки
    (гонка 2026-11-05, регресс-тест ниже).
    """

    supports_async = True
    # Режим cv2-подготовки по умолчанию; jpeg-тесты ставят True.
    jpeg_intake = False

    def __init__(self, latency: float = 0.01, sync: bool = False):
        self.latency = latency
        self.sync = sync
        self._results: queue.Queue = queue.Queue()
        self.submitted: list[int] = []    # frame_index по порядку отправки
        self.corr_of: dict[int, int] = {}  # frame_index → corr (для юнит-тестов)
        self.fail_indices: set[int] = set()
        self.raise_on: int | None = None   # frame_index, на котором submit бросает
        self._lock = threading.Lock()
        self._now = 0
        self.max_inflight = 0

    def prepare(self, frame):
        return ("tensor", frame)

    def prepare_jpeg(self, jpeg, width, height):
        return ("jpeg", jpeg, width, height)

    def submit_async(self, corr, prepared, frame_index=0, timestamp=0.0, conf=0.25):
        assert isinstance(corr, int), "corr обязан приходить от потребителя"
        if self.raise_on == frame_index:
            raise RuntimeError("gRPC channel down")
        with self._lock:
            self.submitted.append(frame_index)
            self.corr_of[frame_index] = corr
            self._now += 1
            self.max_inflight = max(self.max_inflight, self._now)

        def _reply():
            time.sleep(self.latency)
            with self._lock:
                self._now -= 1
            if frame_index in self.fail_indices:
                self._results.put((corr, RuntimeError("engine hiccup")))
            else:
                self._results.put((corr, []))

        if self.sync:
            _reply()
        else:
            threading.Thread(target=_reply, daemon=True).start()

    def next_result(self, timeout=None):
        return self._results.get(timeout=timeout)


def _poison_metric_value() -> float:
    # Семпл счётчика называется <имя>_total (плюс <имя>_created — не считаем)
    samples = poison_messages_total.collect()[0].samples
    return sum(s.value for s in samples if s.name.endswith("_total"))


@pytest.fixture
def consumer(monkeypatch):
    """YOLOConsumer с замоченными Kafka и детектором.

    TritonDetector подменён на уровне модуля: конструктор consumer обязан
    остаться офлайн (никакого реального gRPC-подключения к Triton).
    """
    fc, fp = FakeConsumer(None), FakeProducer(None)
    monkeypatch.setattr(yf, "Consumer", lambda cfg: fc)
    monkeypatch.setattr(yf, "Producer", lambda cfg: fp)
    det = FakeAsyncDetector()
    monkeypatch.setattr("app.processing.triton_detector.TritonDetector",
                        lambda channels=None, jpeg_intake=False: det)

    c = yf.YOLOConsumer(io_workers=2, window=4, channels=2)
    c.fc, c.fp, c.det = fc, fp, det
    yield c
    c.pool.shutdown(wait=False)
    c.io_pool.shutdown(wait=False)


def _submit(c, msg, frame_index: int) -> int:
    """Отправить кадр настоящим _submit_frame, вернуть corr для ручного закрытия."""
    c._submit_frame((msg, _meta(frame_index), ("tensor", frame_index)))
    return c.det.corr_of[frame_index]


def _spawn_async(c):
    """Поднять потоки отправки + диспетчер, как run()."""
    c._send_threads = [
        threading.Thread(target=c._send_worker, name=f"yolo-send-{i}", daemon=True)
        for i in range(c.send_workers)
    ]
    disp = threading.Thread(target=c._dispatch_thread, name="yolo-dispatch", daemon=True)
    for t in c._send_threads:
        t.start()
    disp.start()
    return list(c._send_threads) + [disp]


def _shutdown_async(c, threads):
    c.decoded.put(None)
    for t in threads:
        t.join(timeout=10)
    assert all(not t.is_alive() for t in threads), "потоки не вышли"


# ---- Min-offset watermark коммит ------------------------------------------

class TestWatermarkCommit:
    """Коммит = min(in-flight) - 1: не перепрыгивает необработанное,
    не стопорится ядом, догоняет при пустом in-flight."""

    def test_out_of_order_completion_holds_watermark_at_min(self, consumer):
        c = consumer
        msgs = [_msg(0, o, _jpeg(), frame_index=o) for o in (0, 1, 2)]
        for m in msgs:
            c._enter_inflight(m)
        corrs = [_submit(c, m, o) for o, m in enumerate(msgs)]

        # Ответы приходят в кривом порядке: 0 и 2 обработаны, 1 ещё в полёте.
        # Watermark обязан встать на 0 (store 1), не перепрыгивая offset 1.
        c._handle_result(corrs[0], [])
        c._handle_result(corrs[2], [])
        c._publish_ready()
        assert c.fc.stored == [1]
        assert c.fc.commits == 1

        # Замыкаем дырку: in-flight пуст → watermark = последний увиденный.
        c._handle_result(corrs[1], [])
        c._publish_ready()
        assert c.fc.stored == [1, 3]

    def test_failed_frame_does_not_stall_watermark(self, consumer):
        """Яд на среднем offset: _decode_submit снимает его с in-flight —
        watermark идёт дальше, яд не зацикливается доставкой."""
        c = consumer
        good0 = _msg(0, 0, _jpeg(), frame_index=0)
        bad1 = _msg(0, 1, b"\xff\xfe\xff garbage", frame_index=1)
        good2 = _msg(0, 2, _jpeg(), frame_index=2)
        for m in (good0, bad1, good2):
            c._enter_inflight(m)
            c._slots.acquire()  # как в run(): слот берётся до submit

        c._decode_submit(bad1)   # яд: ничего не эмитит, снимает in-flight
        c._decode_submit(good2)  # кадр: в decoded

        assert c.decoded.qsize() == 1
        corr0 = _submit(c, good0, 0)
        # Кадр из трубы несём как есть: meta/prepared уже готовы декодом.
        c._submit_frame(c.decoded.get_nowait())
        c._handle_result(corr0, [])
        c._handle_result(c.det.corr_of[2], [])
        c._publish_ready()
        assert c.fc.stored == [3]  # встал над трупом без задержки
        assert c.poison_count == 1

    def test_no_commit_before_anything_seen(self, consumer):
        """Ни одного poll'а — нечего коммитить: пустой store/commit."""
        c = consumer
        c._commit_watermarks()
        assert c.fc.stored == []
        assert c.fc.commits == 0

    def test_revoke_clears_ghost_debtors(self, consumer):
        """Rebalance: отзыв партиции обязан стереть её состояние. Иначе
        in-flight кадр отозванной партиции — «призрак» в min(должников):
        кадры обрабатываются, а committed offset стоит навечно, лаг растёт
        со скоростью подачи (поймано бенчмарком 2026-11-05)."""
        c = consumer
        from confluent_kafka import TopicPartition

        p0 = _msg(0, 0, _jpeg(), frame_index=0)
        p1 = _msg(1, 5, _jpeg(), frame_index=5)
        c._enter_inflight(p0)
        c._enter_inflight(p1)

        # Кадр p1 «теряется» навсегда: партицию 1 отозвали в другой член
        # группы, её кадры мы больше не обработаем.
        c._on_revoke(c.consumer, [TopicPartition("raw_frames", 1)])
        assert 1 not in c._last_seen
        assert 1 not in c._inflight or not c._inflight[1]

        # p0 закрывается — watermark p0 едет дальше, p1 не вспоминаем.
        corr0 = _submit(c, p0, 0)
        c._handle_result(corr0, [])
        c._publish_ready()
        assert c.fc.stored == [1]           # только p0: offset 0 → store 1
        assert c.fc.commits == 1

    def test_watermark_per_partition(self, consumer):
        """Watermark партиции независим: in-flight p1 не тормозит p0."""
        c = consumer
        p0 = _msg(0, 5, _jpeg(), frame_index=5)
        p1 = _msg(1, 9, _jpeg(), frame_index=9)
        c._enter_inflight(p0)
        c._enter_inflight(p1)

        corr0 = _submit(c, p0, 5)
        _submit(c, p1, 9)  # ответ не придёт до конца теста
        c._handle_result(corr0, [])
        c._publish_ready()
        # p0 закрыт (store 6), p1 ещё in-flight на 9 → store 9 (водораздел 8)
        assert Counter(c.fc.stored) == Counter([6, 9])

    def test_committed_guard_skips_unchanged_watermark(self, consumer):
        """_committed-гард: граница не двинулась — store/commit не зовём
        (иначе librdkafka отвечает _NO_OFFSET и засирает лог)."""
        c = consumer
        msg = _msg(0, 0, _jpeg(), frame_index=0)
        c._enter_inflight(msg)
        corr = _submit(c, msg, 0)
        c._handle_result(corr, [])
        c._publish_ready()
        assert c.fc.stored == [1]
        assert c.fc.commits == 1

        # Повторный вызов при той же границе — тишина.
        c._commit_watermarks()
        assert c.fc.stored == [1]
        assert c.fc.commits == 1

    def test_rebalance_during_publish_tolerated(self, consumer):
        """Rebalance во время публикации: store_offsets бросает KafkaException —
        это не краш, кадры уже в detections, дубли — цена at-least-once."""
        c = consumer
        from confluent_kafka import KafkaException

        c.fc.store_offsets = MagicMock(side_effect=KafkaException())
        msg = _msg(0, 0, _jpeg(), frame_index=1)
        c._enter_inflight(msg)
        corr = _submit(c, msg, 1)
        c._handle_result(corr, [])
        c._publish_ready()  # не должно бросить

        assert len(c.fp.sent) == 1  # кадр обработан и отправлен


# ---- Backpressure decoded-очереди ------------------------------------------

class TestEmitBackpressure:
    """Бэклог обязан жить в Kafka (диск брокера, виден в queue_wait), а не
    в RAM консьюмера: _emit ждёт место в decoded, слоты держат poll.
    Дроп — только на шатдауне: offset не закоммичен, кадр доставится заново."""

    def test_full_queue_blocks_instead_of_dropping(self, consumer):
        c = consumer
        c.decoded = queue.Queue(maxsize=1)
        c.decoded.put(("fill",))

        t = threading.Thread(target=c._emit, args=(("item",),), daemon=True)
        t.start()
        time.sleep(0.15)
        assert t.is_alive()  # ждёт место, а не выбросил кадр

        assert c.decoded.get_nowait() == ("fill",)
        t.join(timeout=3)
        assert not t.is_alive()
        assert c.decoded.get_nowait() == ("item",)

    def test_drop_only_when_closing(self, consumer):
        """На шатдауне ждать смысла нет: дроп честный — offset не
        закоммичен, брокер отдаст кадр после рестарта (at-least-once)."""
        c = consumer
        c.decoded = queue.Queue(maxsize=1)
        c.decoded.put(("fill",))
        c._closing = True

        started = time.monotonic()
        c._emit(("item",))  # вернётся, а не зависнет
        assert time.monotonic() - started < 2.0
        assert c.decoded.qsize() == 1
        assert c.decoded.get_nowait() == ("fill",)  # дропнут именно новый кадр


# ---- Async-конвейер (отправка, окно, публикация) ----------------------------

class TestAsyncStream:
    """Кадры уходят ОДИН ЗА ОДИМ сразу по готовности (батчит их Triton),
    окно ограничивает полёт, ответы публикуются пачками, watermark считается
    по должникам — порядок возврата не важен."""

    def test_per_frame_submit_window_and_commit(self, consumer):
        c = consumer
        for off in range(6):
            msg = _msg(0, off, _jpeg(), frame_index=off)
            c._enter_inflight(msg)
            c.decoded.put((msg, _meta(off), ("tensor", off)))

        threads = _spawn_async(c)
        deadline = time.time() + 10
        while time.time() < deadline and len(c.fp.sent) < 6:
            time.sleep(0.02)
        _shutdown_async(c, threads)

        assert sorted(c.det.submitted) == [0, 1, 2, 3, 4, 5]
        assert c.det.max_inflight <= 4, "окно в полёте превышено"
        assert len(c.fp.sent) == 6
        assert 6 in c.fc.stored
        assert not c._inflight[0]
        assert not c._pending

    def test_error_result_poisons_without_stalling_watermark(self, consumer):
        c = consumer
        c.det.fail_indices = {1}
        for off in range(3):
            msg = _msg(0, off, _jpeg(), frame_index=off)
            c._enter_inflight(msg)
            c.decoded.put((msg, _meta(off), ("tensor", off)))

        threads = _spawn_async(c)
        deadline = time.time() + 10
        while time.time() < deadline and (len(c.fp.sent) < 2 or c.poison_count < 1):
            time.sleep(0.02)
        _shutdown_async(c, threads)

        assert len(c.fp.sent) == 2          # упавший кадр без детекции
        assert c.poison_count == 1
        assert 3 in c.fc.stored             # watermark дошёл за ядом
        assert not c._inflight[0]

    def test_submit_failure_releases_window_and_poisons(self, consumer):
        """submit_async бросил (канал лёг): кадр закрывается как проваленный —
        pending чист, окно освобождено, должник снят, poison учтён."""
        c = consumer
        c.det.raise_on = 0
        msg = _msg(0, 0, _jpeg(), frame_index=0)
        c._enter_inflight(msg)
        c._slots.acquire()

        c._decode_submit(msg)
        item = c.decoded.get_nowait()
        c._submit_frame(item)  # жетон окна _submit_frame берёт сам

        assert not c._pending
        assert c._win_slots.acquire(blocking=False)  # слот окна вернулся
        assert not c._inflight[0]
        assert c.poison_count == 1
        assert c.consecutive_failures == 1

    def test_sentinel_drains_inflight_then_exits(self, consumer):
        """Sentinel в трубе: новые кадры не шлём, ОТПРАВЛЕННЫЕ дорабатываем
        и публикуем (detекции жалко, at-least-once не нарушается)."""
        c = consumer
        c.det.latency = 0.2
        msg = _msg(0, 0, _jpeg(), frame_index=0)
        c._enter_inflight(msg)
        c.decoded.put((msg, _meta(0), ("tensor", 0)))
        c.decoded.put(None)

        threads = _spawn_async(c)
        for t in threads:
            t.join(timeout=10)

        assert all(not t.is_alive() for t in threads)
        assert c._closing is True
        assert len(c.fp.sent) == 1
        assert 1 in c.fc.stored

    def test_multiple_senders_share_window(self, consumer):
        """Четыре потока отправки, окно 4: суммарно в полёте никогда больше
        четырёх кадров, все кадры дошли, граница закрылась."""
        c = consumer
        c.send_workers = 4
        c.det.latency = 0.05
        # Труба в бою ограничена window×2 (=8) и лишнее держит Kafka; здесь
        # мы НАМЕРЕННО заводим 12 кадров до старта потоков — трубу расширяем,
        # окно в полёте при этом остаётся 4 (замеряем именно его).
        c.decoded = queue.Queue(maxsize=32)
        for off in range(12):
            msg = _msg(0, off, _jpeg(), frame_index=off)
            c._enter_inflight(msg)
            c.decoded.put((msg, _meta(off), ("tensor", off)))

        threads = _spawn_async(c)
        deadline = time.time() + 15
        while time.time() < deadline and len(c.fp.sent) < 12:
            time.sleep(0.02)
        _shutdown_async(c, threads)

        assert c.det.max_inflight <= 4
        assert len(c.fp.sent) == 12
        assert 12 in c.fc.stored
        assert not c._inflight[0]

    def test_prepare_preprocesses_in_decode_stage(self, consumer):
        """Letterbox делается в decode-воркере, и в трубу едет тензор,
        а не пиксели."""
        c = consumer
        _meta_out, payload = c._prepare(_msg(0, 0, _jpeg(), frame_index=0))
        assert payload[0] == "tensor"

    def test_prepare_decodes_full_size(self, consumer):
        """Декод идёт ПОЛНЫМ размером: стримы бывают и в 480p, урезать
        детали до детекции нельзя. prepare получает кадр 1:1."""
        c = consumer
        big = np.full((1080, 1920, 3), 128, dtype=np.uint8)
        ok, buf = cv2.imencode(".jpg", big)
        assert ok
        _meta_out, (_tensor, frame) = c._prepare(
            _msg(0, 0, buf.tobytes(), frame_index=0))
        assert frame.shape[:2] == (1080, 1920)

    def test_prepare_jpeg_passes_bytes_through(self, consumer):
        """jpeg-режим: байты уходят в детектор как есть, кадр НЕ декодируется;
        размеры берутся из меты (обратный ход боксов считает детектор)."""
        c = consumer
        c.det.jpeg_intake = True
        jb = _jpeg()
        _meta_out, payload = c._prepare(
            _msg(0, 0, jb, frame_index=0, width=1280, height=720))
        assert payload[0] == "jpeg"
        assert payload[1] == jb
        assert (payload[2], payload[3]) == (1280, 720)

    def test_prepare_jpeg_requires_dims(self, consumer):
        """jpeg-режим без width/height в мете (старый producer) — ядовитое
        сообщение, а НЕ тихий откат в cv2-декод: декод здесь — то, от чего
        режим уходил."""
        c = consumer
        c.det.jpeg_intake = True
        with pytest.raises(ValueError):
            c._prepare(_msg(0, 0, _jpeg(), frame_index=0))

    def test_idle_dispatcher_does_not_busy_spin(self, consumer):
        """Без кадров в полёте диспетчер обязан СПАТЬ в очереди результатов.
        Busy-spin (next_result(timeout=0) по кругу + is_alive() ctypes-вызовами
        под GIL) голодал decode-воркеры и poll: в бенче 20261105-221647
        замерные decode и queue_raw раздуло с <10 мс до 10–50 мс на любой
        нагрузке — фантомный «этаж», а не реальные пиксели."""
        c = consumer
        calls: list[int] = []
        real_next = c.det.next_result

        def counting_next(timeout=None):
            calls.append(1)
            return real_next(timeout=timeout)

        c.det.next_result = counting_next
        disp = threading.Thread(target=c._dispatch_thread, daemon=True)
        disp.start()
        time.sleep(0.5)  # idle: ни одного кадра в полёте
        n_idle = len(calls)
        c._closing = True
        disp.join(timeout=2)
        assert not disp.is_alive()
        # ожидание по 0.05с: ~10–20 пробуждений за 0.5с; спин дал бы тысячи
        assert n_idle < 100, f"диспетчер сделал {n_idle} вызовов за 0.5с — busy-spin"

    def test_unknown_corr_logs_error(self, consumer, caplog):
        """Результат без запроса — признак бага регистрации: молча глотать
        нельзя (потерянный кадр = зависший watermark), обязан быть ОШИБКА."""
        c = consumer
        with caplog.at_level("ERROR", logger="app.consumers.yolo_frames"):
            c._handle_result(999, [])  # не должно бросить
        assert "неизвестным corr" in caplog.text

    def test_minio_only_for_frames_with_detections(self, consumer):
        """Пустые кадры в MinIO не пишем — хранить нечего, место не жжём."""
        c = consumer
        saved: list[int] = []
        c._save_frame_to_minio = lambda sid, fi, data: saved.append(fi)

        m0 = _msg(0, 0, _jpeg(), frame_index=0)
        m1 = _msg(0, 1, _jpeg(), frame_index=1)
        for m in (m0, m1):
            c._enter_inflight(m)
        corr0 = _submit(c, m0, 0)
        corr1 = _submit(c, m1, 1)

        c._handle_result(corr0, [_det()])  # с детекциями → в MinIO
        c._handle_result(corr1, [])        # пусто → НЕ в MinIO
        c._publish_ready()

        deadline = time.time() + 5
        while time.time() < deadline and not saved:
            time.sleep(0.02)
        assert saved == [0]
        assert len(c.fp.sent) == 2  # детекции ушли ОБА кадра


# ---- Регресс гонки регистрации corr (бенчмарк 2026-11-05 17:32) --------------

def test_sync_callback_inside_submit_does_not_lose_frame(consumer):
    """Быстрый gRPC дёргает done-callback СИНХРОННО внутри submit_async.
    Если corr регистрировать после отправки — _handle_result не найдёт кадр
    в pending, и он навсегда останется должником-призраком: watermark встанет
    (так падал бенчмарк). Фикс: corr генерит консьюмер и регистрирует ДО
    submit. Здесь fake отвечает прямо внутри submit_async — при баге кадр
    потерялся бы, pending бы засорился, граница бы не дошла."""
    c = consumer
    c.det.sync = True
    for off in range(6):
        msg = _msg(0, off, _jpeg(), frame_index=off)
        c._enter_inflight(msg)
        c.decoded.put((msg, _meta(off), ("tensor", off)))

    threads = _spawn_async(c)
    deadline = time.time() + 10
    while time.time() < deadline and len(c.fp.sent) < 6:
        time.sleep(0.02)
    _shutdown_async(c, threads)

    assert len(c.fp.sent) == 6, "кадры потеряны синхронным callback"
    assert not c._pending, "в pending остались призраки"
    assert not c._inflight[0], "должники не закрыты"
    assert 6 in c.fc.stored, "watermark не дошёл до конца"


# ---- Яд, зомби-порог, формат детекции ---------------------------------------

def test_decode_failure_skipped_and_poisoned(consumer):
    """Ядовитое сообщение: _decode_submit НЕ эмитит, снимает in-flight,
    poison учтён, слот освобождён."""
    c = consumer
    before = _poison_metric_value()
    bad = _msg(0, 1, b"\xff\xfe\xff garbage", frame_index=2)

    # Контракт: слот занимает main-поток ДО submit (см. run())
    c._enter_inflight(bad)
    c._slots.acquire()
    c._decode_submit(bad)

    assert c.decoded.empty()
    assert c._inflight[0] == set()  # яд не тормозит watermark
    assert _poison_metric_value() - before == 1
    assert c.consecutive_failures == 1
    assert c._slots.acquire(blocking=False)  # слот вернулся в пул


def test_zombie_threshold_exits_process(consumer, monkeypatch):
    """N подряд падений — тоже выход, а не бесконечный skip."""
    c = consumer

    class _Exited(BaseException):
        pass

    monkeypatch.setattr(yf.os, "_exit", lambda code: (_ for _ in ()).throw(_Exited(code)))

    for i in range(yf.MAX_CONSECUTIVE_FAILURES - 1):
        c._mark_failure(ValueError("boom"), "raw_frames", i)

    with pytest.raises(_Exited):
        c._mark_failure(ValueError("boom"), "raw_frames", yf.MAX_CONSECUTIVE_FAILURES - 1)


def test_detection_message_json_fields(consumer):
    """Формат детекции, который ждёт detections-консьюмер: сквозные поля
    (stream_id, frame_index, captured_at) + objects/num_detections."""
    payload = json.loads(
        yf.YOLOConsumer._detection_message(_meta(9, stream_id=3), [_det(0.55)]))
    assert payload["video_id"] is None
    assert payload["stream_id"] == 3
    assert payload["frame_index"] == 9
    assert payload["captured_at"] is None
    assert payload["num_detections"] == 1
    obj = payload["objects"][0]
    assert obj["class"] == "person"
    assert obj["confidence"] == 0.55
    assert obj["bbox"] == {"x1": 1.0, "y1": 2.0, "x2": 3.0, "y2": 4.0}
