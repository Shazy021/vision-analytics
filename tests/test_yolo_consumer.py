"""Тесты yolo-consumer (конвейер: параллельный декод + батч + watermark-коммит).

Модель и Kafka замочены: проверяется ЛОГИКА потребителя —
min-offset watermark (коммит не перепрыгивает необработанные in-flight,
проваленный яд не стопорит его), динамическая сборка батча из decoded-
очереди (grace/hot/cold, sentinel, потолок batch_max), один инференс на
батч, коммит только после produce+flush, пропуск яда без зацикливания,
деградация батча до по-кадрового, выход при липкой CUDA-ошибке и
зомби-пороге.
"""
import json
import queue
import threading
import time
from collections import Counter, deque
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
         frame_index: int | None = None):
    """Фейковое Kafka-сообщение с реальным полезным грузом."""
    payload = pack({
        "frame_index": offset if frame_index is None else frame_index,
        "source": "test",
        "stream_id": stream_id,
        "captured_at": "2026-10-30T12:00:00+00:00",
    }, jpeg_bytes)
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


class FakeConsumer:
    """Выдаёт сообщения из очереди, запоминает store_offsets/commits."""

    def __init__(self, cfg):
        self.queue: deque = deque()
        self.delay_next = 0.0  # сек «дороги» до следующего сообщения (эммуляция)
        self.stored: list[int] = []
        self.commits = 0
        self.closed = False

    def subscribe(self, topics):
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


def _poison_metric_value() -> float:
    # Семпл счётчика называется <имя>_total (плюс <имя>_created — не считаем)
    samples = poison_messages_total.collect()[0].samples
    return sum(s.value for s in samples if s.name.endswith("_total"))


@pytest.fixture
def consumer(monkeypatch):
    """YOLOConsumer с замоченными Kafka/моделью; быстрый grace для тестов."""
    fc, fp = FakeConsumer(None), FakeProducer(None)
    monkeypatch.setattr(yf, "Consumer", lambda cfg: fc)
    monkeypatch.setattr(yf, "Producer", lambda cfg: fp)
    # YOLO(...) не должен реально грузить/качать модель
    monkeypatch.setattr("app.processing.detector.YOLO", lambda *a, **k: MagicMock())

    c = yf.YOLOConsumer(batch_max=4, grace_ms=1, io_workers=2)
    det = MagicMock()
    det.detect_batch.side_effect = lambda refs: [[] for _ in refs]
    det.detect.side_effect = lambda frame, frame_index=0, **kw: []
    c.detector = det
    c.fc, c.fp, c.det = fc, fp, det
    yield c
    c.pool.shutdown(wait=False)
    c.io_pool.shutdown(wait=False)


# ---- Min-offset watermark коммит ------------------------------------------

class TestWatermarkCommit:
    """Коммит = min(in-flight) - 1: не перепрыгивает необработанное,
    не стопорится ядом, догоняет при пустом in-flight."""

    def test_out_of_order_completion_holds_watermark_at_min(self, consumer):
        c = consumer
        msgs = [_msg(0, o, _jpeg(), frame_index=o) for o in (0, 1, 2)]
        for m in msgs:
            c._enter_inflight(m)

        # Готовность декода — кривой порядок: 0 и 2 обработаны, 1 ещё in-flight.
        # Watermark обязан встать на 0 (store 1), не перепрыгивая offset 1.
        c._infer_and_publish([
            (msgs[0], _meta(0), None),
            (msgs[2], _meta(2), None),
        ])
        assert c.fc.stored == [1]
        assert c.fc.commits == 1

        # Замыкаем дырку: in-flight пуст → watermark = последний увиденный.
        c._infer_and_publish([(msgs[1], _meta(1), None)])
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
        c._infer_and_publish([
            (good0, _meta(0), None),
            c.decoded.get_nowait(),
        ])
        assert c.fc.stored == [3]  # встал над трупом без задержки
        assert c.poison_count == 1

    def test_no_commit_before_anything_seen(self, consumer):
        """Ни одного poll'а — нечего коммитить: пустой store/commit."""
        c = consumer
        c._commit_watermarks()
        assert c.fc.stored == []
        assert c.fc.commits == 0

    def test_watermark_per_partition(self, consumer):
        """Watermark партиции независим: in-flight p1 не тормозит p0."""
        c = consumer
        p0 = _msg(0, 5, _jpeg(), frame_index=5)
        p1 = _msg(1, 9, _jpeg(), frame_index=9)
        c._enter_inflight(p0)
        c._enter_inflight(p1)

        c._infer_and_publish([(p0, _meta(5), None)])
        # p0 закрыт (store 6), p1 ещё in-flight на 9 → store 9 (водораздел 8)
        assert Counter(c.fc.stored) == Counter([6, 9])


# ---- Сборка батча из decoded-очереди ---------------------------------------

class TestCollectDecoded:
    def test_hot_queue_drains_into_one_batch(self, consumer):
        c = consumer
        for i in range(3):
            c.decoded.put((_msg(0, i, _jpeg(), frame_index=i), _meta(i), None))

        batch = c._collect_decoded()
        # Порядок — FIFO готовности декода, без переупорядочивания
        assert [mm["frame_index"] for _, mm, _ in batch] == [0, 1, 2]

    def test_batch_capped_at_batch_max(self, consumer):
        """Потолок batch_max: один detect_batch на десятки 1080p-кадров —
        гигабайтные пики RAM (float32-тензоры препроцессинга), которые
        аллокатор torch не отдаёт ОС."""
        c = consumer  # batch_max=4
        for i in range(10):
            c.decoded.put((_msg(0, i, _jpeg(), frame_index=i), _meta(i), None))

        batch = c._collect_decoded()
        assert len(batch) == 4
        assert c.decoded.qsize() == 6

    def test_cold_queue_respects_grace_and_returns_empty(self, consumer):
        c = consumer
        started = time.monotonic()
        batch = c._collect_decoded()
        assert batch == []
        # grace=1мс: не сидим дольше окна (с запасом на планировщик)
        assert time.monotonic() - started < 0.5

    def test_sentinel_returns_none(self, consumer):
        c = consumer
        c.decoded.put(None)
        assert c._collect_decoded() is None

    def test_sentinel_mid_drain_drops_batch(self, consumer):
        """Sentinel после первых кадров: собранный сбрасывается — offset'ы
        не закоммичены, брокер отдаст кадры после рестарта (at-least-once)."""
        c = consumer
        c.decoded.put((_msg(0, 0, _jpeg(), frame_index=0), _meta(0), None))
        c.decoded.put(None)
        assert c._collect_decoded() is None
        assert c._closing is True


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


# ---- Инференс и публикация --------------------------------------------------

def test_cross_partition_batch_single_inference_and_commit(consumer):
    """Батч со ВСЕХ партиций — ОДИН вызов detect_batch; коммит — один на
    батч, watermark каждой партиции по её in-flight/last_seen."""
    c = consumer
    pairs = [
        (_msg(0, 0, _jpeg(), frame_index=10), _meta(10), None),
        (_msg(0, 1, _jpeg(), frame_index=11), _meta(11), None),
        (_msg(1, 0, _jpeg(), frame_index=20), _meta(20, 2), None),
    ]
    for msg, _m, _f in pairs:
        c._enter_inflight(msg)

    c._infer_and_publish(pairs)

    assert c.det.detect_batch.call_count == 1
    refs = c.det.detect_batch.call_args_list[0].args[0]
    assert [ref.index for ref in refs] == [10, 11, 20]

    assert len(c.fp.sent) == 3
    indices = sorted(json.loads(v)["frame_index"] for _, v in c.fp.sent)
    assert indices == [10, 11, 20]

    # in-flight пуст → watermark = last_seen: p0 закрывает offset 1, p1 — 0
    assert Counter(c.fc.stored) == Counter([2, 1])
    assert c.fc.commits == 1


def test_decode_failure_skipped_and_poisoned(consumer):
    """Ядовитое сообщение: _decode_submit НЕ эмитит (tombstone'ов больше
    нет), снимает in-flight, poison учтён, слот освобождён."""
    c = consumer
    before = _poison_metric_value()
    bad = _msg(0, 1, b"\xff\xfe\xff garbage", frame_index=2)

    # Контракт: слот занимает main-поток ДО submit (см. run())
    c._enter_inflight(bad)
    c._slots.acquire()
    c._decode_submit(bad)

    assert c.decoded.empty()  # tombstone не нужен: дырки seq больше нет
    assert c._inflight[0] == set()  # яд не тормозит watermark
    assert _poison_metric_value() - before == 1
    assert c.consecutive_failures == 1


def test_batch_failure_degrades_to_per_frame(consumer):
    """Если батч-инференс упал транзиентно — деградация до по-кадрового:
    здоровые кадры не должны страдать от чужих."""
    c = consumer
    c.det.detect_batch.side_effect = RuntimeError("transient GPU hiccup")
    pairs = [
        (_msg(0, 0, _jpeg(), frame_index=1), _meta(1), None),
        (_msg(0, 1, _jpeg(), frame_index=2), _meta(2), None),
    ]
    for msg, _m, _f in pairs:
        c._enter_inflight(msg)

    c._infer_and_publish(pairs)

    assert c.det.detect.call_count == 2
    assert len(c.fp.sent) == 2
    assert c.fc.commits == 1


def test_rebalance_during_batch_tolerated(consumer):
    """Rebalance во время батча: store_offsets бросает KafkaException —
    это не краш, кадры уже в detections, дубли — цена at-least-once."""
    c = consumer
    from confluent_kafka import KafkaException

    c.fc.store_offsets = MagicMock(side_effect=KafkaException())
    pairs = [
        (_msg(0, 0, _jpeg(), frame_index=1), _meta(1), None),
    ]
    for msg, _m, _f in pairs:
        c._enter_inflight(msg)

    c._infer_and_publish(pairs)  # не должно бросить

    assert len(c.fp.sent) == 1  # кадр обработан и отправлен


def test_fatal_cuda_error_exits_process(consumer, monkeypatch):
    """Липкая CUDA-ошибка: процесс обязан умереть (супервизор поднимет со
    свежим контекстом), коммит НЕ происходит. os._exit — потому что мы
    можем быть в worker-потоке, где SystemExit умер бы молча."""
    c = consumer

    class _FakeAcceleratorError(RuntimeError):
        __name__ = "AcceleratorError"

    c.det.detect_batch.side_effect = _FakeAcceleratorError("CUDA error: unknown")
    pairs = [
        (_msg(0, 0, _jpeg(), frame_index=1), _meta(1), None),
    ]
    for msg, _m, _f in pairs:
        c._enter_inflight(msg)

    class _Exited(BaseException):
        pass

    monkeypatch.setattr(yf.os, "_exit", lambda code: (_ for _ in ()).throw(_Exited(code)))

    with pytest.raises(_Exited):
        c._infer_and_publish(pairs)

    assert c.fc.commits == 0
    assert c.fp.sent == []


def test_zombie_threshold_exits_process(consumer, monkeypatch):
    """N подряд не-CUDA падений — тоже выход, а не бесконечный skip."""
    c = consumer

    class _Exited(BaseException):
        pass

    monkeypatch.setattr(yf.os, "_exit", lambda code: (_ for _ in ()).throw(_Exited(code)))

    for i in range(yf.MAX_CONSECUTIVE_FAILURES - 1):
        c._mark_failure(ValueError("boom"), "raw_frames", i)

    with pytest.raises(_Exited):
        c._mark_failure(ValueError("boom"), "raw_frames", yf.MAX_CONSECUTIVE_FAILURES - 1)
