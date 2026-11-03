"""Тесты yolo-consumer (конвейер: параллельный декод + sequencer + батчи).

Модель и Kafka замочены: проверяется ЛОГИКА потребителя —
последовательность sequencer'а (непрерывный префикс партиции, tombstones),
динамическая сборка батча из decoded-очереди (grace/hot/cold, sentinel),
один инференс на батч, коммит только после produce+flush, пропуск яда
без зацикливания, деградация батча до по-кадрового, выход при липкой
CUDA-ошибке и зомби-пороге.
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

    def store_offsets(self, message=None):
        # Как настоящий confluent_kafka: message= → коммитится offset+1
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


# ---- Sequencer ----------------------------------------------------------

class TestSequencer:
    """Непрерывный префикс партиции: порядок стрима при любом порядке декода."""

    def test_out_of_order_decode_releases_contiguous_prefix_only(self, consumer):
        c = consumer
        # Декод финишировал в «кривом» порядке: p0:s0, p1:s0, p0:s2 (!), p0:s1
        # (seq — per-partition, см. _part_seq в run())
        c._accept(0, _msg(0, 0, _jpeg(), frame_index=10), {"frame_index": 10}, None)
        c._accept(0, _msg(1, 0, _jpeg(), frame_index=20), {"frame_index": 20}, None)
        c._accept(2, _msg(0, 2, _jpeg(), frame_index=12), {"frame_index": 12}, None)

        batch = c._release_ready()
        # p0:s2 ждёт p0:s1 — выпускаются только s0 обеих партиций
        assert [(m.partition(), mm["frame_index"]) for m, mm, _ in batch] == [(0, 10), (1, 20)]

        c._accept(1, _msg(0, 1, _jpeg(), frame_index=11), {"frame_index": 11}, None)
        batch = c._release_ready()
        # Теперь префикс p0 замыкается: s1, потом s2 — порядок стрима цел
        assert [(m.partition(), mm["frame_index"]) for m, mm, _ in batch] == [(0, 11), (0, 12)]

    def test_tombstone_advances_sequence(self, consumer):
        c = consumer
        c._accept(0, _msg(0, 0, _jpeg(), frame_index=1), None, None)  # яд
        c._accept(1, _msg(0, 1, _jpeg(), frame_index=2), {"frame_index": 2}, None)

        batch = c._release_ready()
        assert [mm["frame_index"] for _, mm, _ in batch] == [2]

    def test_release_caps_at_batch_max_proportional(self, consumer):
        """Выпуск ≤ batch_max, квота пропорциональна глубине префиксов:
        равная дрень при разной подаче партиций (2–5 стримов на партицию)
        топит жирные быстрее тощих — латентность растёт с одинаковым
        темпом у всех только при дрени ∝ backlog."""
        c = consumer  # batch_max=4
        for p in (0, 1):
            for s in range(6):
                c._accept(s, _msg(p, s, _jpeg(), frame_index=p * 100 + s),
                          {"frame_index": p * 100 + s}, None)

        first = c._release_ready()
        assert len(first) == 4
        # равные префиксы (6/6) → равные квоты 2+2
        assert [mm["frame_index"] for _, mm, _ in first] == [0, 1, 100, 101]

        second = c._release_ready()
        assert [mm["frame_index"] for _, mm, _ in second] == [2, 3, 102, 103]

        third = c._release_ready()
        assert [mm["frame_index"] for _, mm, _ in third] == [4, 5, 104, 105]
        assert c._release_ready() == []

    def test_release_quota_follows_prefix_depth(self, consumer):
        """Партиция с трёхкратным завалом получает трёхкратную квоту."""
        c = consumer  # batch_max=4
        for s in range(12):
            c._accept(s, _msg(0, s, _jpeg(), frame_index=s), {"frame_index": s}, None)
        for s in range(4):
            c._accept(s, _msg(1, s, _jpeg(), frame_index=100 + s), {"frame_index": 100 + s}, None)

        batch = c._release_ready()
        # префиксы 12/4 → квоты 3/1
        assert [mm["frame_index"] for _, mm, _ in batch] == [0, 1, 2, 100]


# ---- Сборка батча из decoded-очереди -------------------------------------

class TestCollectDecoded:
    def test_hot_queue_drains_into_one_batch(self, consumer):
        c = consumer
        # seq per-partition: p0 получает 0,1; p1 — 0
        c.decoded.put((0, _msg(0, 0, _jpeg(), frame_index=0), {"frame_index": 0}, None))
        c.decoded.put((0, _msg(1, 0, _jpeg(), frame_index=1), {"frame_index": 1}, None))
        c.decoded.put((1, _msg(0, 1, _jpeg(), frame_index=2), {"frame_index": 2}, None))

        batch = c._collect_decoded()
        # Квоты ∝ префиксам: p0 (2 кадра) > p1 (1) → p0 вперёд
        assert [mm["frame_index"] for _, mm, _ in batch] == [0, 2, 1]

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

    def test_absorb_stops_at_pending_cap(self, consumer):
        """pending ≤ inflight_cap: без потолка absorb перекладывал в него
        весь decoded, а release отдаёт по batch_max — под перегрузкой
        кадры оседали в словарях консьюмера минутами (e2e p95 24с при
        пустом Kafka). Теперь лишнее ждёт в decoded — там его видит
        queue_wait и держит RAM под семафором слотов."""
        c = consumer  # inflight_cap = 8×4 = 32
        # Юнит-тест ceiling'а absorb: нужен завал глубже maxsize decoded
        # (в проде глубина ограничена сверху тем же inflight_cap и
        # backpressure _emit — это покрыто TestEmitBackpressure).
        c.decoded = queue.Queue()
        for i in range(40):
            c.decoded.put((i, _msg(0, i, _jpeg(), frame_index=i),
                           {"frame_index": i}, None))

        c._absorb_buffered()
        assert c._pending_count() == c.inflight_cap
        assert c.decoded.qsize() == 40 - c.inflight_cap


# ---- Backpressure decoded-очереди ------------------------------------------

class TestEmitBackpressure:
    """Потеря seq навсегда стопорит непрерывный префикс партиции в
    sequencer'е (lag растёт вечно — этот stall ловили в бенче на догоне
    бэклога после рестарта), поэтому _emit ждёт, а не дропает."""

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


# ---- Инференс и публикация ------------------------------------------------

def test_cross_partition_batch_single_inference_and_commit(consumer):
    """Батч со ВСЕХ партиций — ОДИН вызов detect_batch; порядок внутри
    партиции сохраняется, коммит — один на батч, offset'ы по последней
    message каждой партиции."""
    c = consumer
    pairs = [
        (_msg(0, 0, _jpeg(), frame_index=10), {"frame_index": 10, "stream_id": 1, "source": "s", "captured_at": None, "frame_bytes": b""}, None),
        (_msg(0, 1, _jpeg(), frame_index=11), {"frame_index": 11, "stream_id": 1, "source": "s", "captured_at": None, "frame_bytes": b""}, None),
        (_msg(1, 0, _jpeg(), frame_index=20), {"frame_index": 20, "stream_id": 2, "source": "s", "captured_at": None, "frame_bytes": b""}, None),
    ]

    c._infer_and_publish(pairs)

    assert c.det.detect_batch.call_count == 1
    refs = c.det.detect_batch.call_args_list[0].args[0]
    assert [ref.index for ref in refs] == [10, 11, 20]

    assert len(c.fp.sent) == 3
    indices = sorted(json.loads(v)["frame_index"] for _, v in c.fp.sent)
    assert indices == [10, 11, 20]

    assert Counter(c.fc.stored) == Counter([2, 1])
    assert c.fc.commits == 1


def test_decode_failure_becomes_tombstone_and_poison(consumer):
    """Ядовитое сообщение: _decode_submit кладёт tombstone (sequencer
    пройдёт мимо), poison учтён, слот освобождён."""
    c = consumer
    before = _poison_metric_value()
    bad = _msg(0, 1, b"\xff\xfe\xff garbage", frame_index=2)

    # Контракт: слот занимает main-поток ДО submit (см. run())
    c._slots.acquire()
    c._decode_submit(0, bad)

    seq, msg, meta, frame = c.decoded.get_nowait()
    assert meta is None and frame is None  # tombstone
    assert _poison_metric_value() - before == 1
    assert c.consecutive_failures == 1


def test_batch_failure_degrades_to_per_frame(consumer):
    """Если батч-инференс упал транзиентно — деградация до по-кадрового:
    здоровые кадры не должны страдать от чужих."""
    c = consumer
    c.det.detect_batch.side_effect = RuntimeError("transient GPU hiccup")
    pairs = [
        (_msg(0, 0, _jpeg(), frame_index=1), {"frame_index": 1, "stream_id": 1, "source": "s", "captured_at": None, "frame_bytes": b""}, None),
        (_msg(0, 1, _jpeg(), frame_index=2), {"frame_index": 2, "stream_id": 1, "source": "s", "captured_at": None, "frame_bytes": b""}, None),
    ]

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
        (_msg(0, 0, _jpeg(), frame_index=1), {"frame_index": 1, "stream_id": 1, "source": "s", "captured_at": None, "frame_bytes": b""}, None),
    ]

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
        (_msg(0, 0, _jpeg(), frame_index=1), {"frame_index": 1, "stream_id": 1, "source": "s", "captured_at": None, "frame_bytes": b""}, None),
    ]

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
