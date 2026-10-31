"""Тесты yolo-consumer (микро-батчи + пул потоков + ручные коммиты).

Модель и Kafka замочены: проверяется ЛОГИКА потребителя —
динамическая сборка батчей по партициям (горячая/холодная очередь),
порядок внутри партиции, коммит только после produce+flush, пропуск яда
без зацикливания, деградация батча до по-кадрового, выход при липкой
CUDA-ошибке.
"""
import base64
import json
import time
from collections import Counter, deque
from unittest.mock import MagicMock

import cv2
import numpy as np
import pytest

from app.consumers import yolo_frames as yf
from app.core.metrics import poison_messages_total


def _jpeg() -> bytes:
    """Минимальный валидный JPEG 16x16."""
    ok, buf = cv2.imencode(".jpg", np.full((16, 16, 3), 128, dtype=np.uint8))
    assert ok
    return buf.tobytes()


def _msg(partition: int, offset: int, jpeg_bytes: bytes, stream_id: int = 7,
         frame_index: int | None = None):
    """Фейковое Kafka-сообщение с реальным полезным грузом."""
    payload = json.dumps({
        "frame_data": base64.b64encode(jpeg_bytes).decode(),
        "frame_index": offset if frame_index is None else frame_index,
        "source": "test",
        "stream_id": stream_id,
        "captured_at": "2026-10-30T12:00:00+00:00",
    }).encode()
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
        self.delay_next = 0.0  # сек «дороги» до следующего сообщения (эмуляция)
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
    return c


def test_batches_by_partition_keeps_order_and_commits_after_produce(consumer):
    """Батчи собираются ПО ПАРТИЦИЯМ, порядок внутри партии сохраняется,
    коммит — после produce всех кадров батча."""
    c = consumer
    m0 = _msg(0, 0, _jpeg(), frame_index=10)
    m1 = _msg(0, 1, _jpeg(), frame_index=11)
    m2 = _msg(1, 0, _jpeg(), frame_index=20)
    c.fc.queue.extend([m0, m1, m2])

    c.step()

    # Инференс шёл батчами по партициям: два вызова (2 кадра и 1 кадр)
    assert c.det.detect_batch.call_count == 2
    sizes = sorted(len(call.args[0]) for call in c.det.detect_batch.call_args_list)
    assert sizes == [1, 2]
    # Порядок внутри партиции 0: сначала кадр 10, потом 11
    p0_calls = [call for call in c.det.detect_batch.call_args_list if len(call.args[0]) == 2]
    assert [ref.index for ref in p0_calls[0].args[0]] == [10, 11]

    # Все три кадра улетели в detections
    assert len(c.fp.sent) == 3
    indices = sorted(json.loads(v)["frame_index"] for _, v in c.fp.sent)
    assert indices == [10, 11, 20]

    # Offset зафиксирован последним сообщением каждой партиции (offset+1)
    assert Counter(c.fc.stored) == Counter([2, 1])
    assert c.fc.commits == 2  # по одному на партицию


def test_dynamic_batch_does_not_wait_when_queue_cold(consumer):
    """Динамический батч: если первое сообщение пришлось ЖДАТЬ (очередь
    холодная), не тратим латентность на «партнёров» — обрабатываем то, что
    есть, остаток подберёт следующая итерация."""
    c = consumer
    c.fc.delay_next = 0.05  # первое сообщение «пришло» спустя 50мс
    c.fc.queue.extend([
        _msg(0, 0, _jpeg(), frame_index=1),
        _msg(0, 1, _jpeg(), frame_index=2),
    ])

    c.step()

    # Батч из одного кадра, второе сообщение осталось в очереди
    assert c.det.detect_batch.call_count == 1
    assert len(c.det.detect_batch.call_args_list[0].args[0]) == 1
    assert len(c.fc.queue) == 1

    # Следующая итерация подбирает остаток
    c.step()
    assert len(c.fp.sent) == 2


def test_poison_message_skipped_but_committed(consumer):
    """Ядовитое сообщение (битый base64/JPEG) пропускается, остальные кадры
    батча проходят, offset яда коммитится — иначе он бы зацикливался."""
    c = consumer
    before = _poison_metric_value()
    good = _msg(0, 0, _jpeg(), frame_index=1)
    bad = _msg(0, 1, b"\xff\xfe\xff not-base64", frame_index=2)
    c.fc.queue.extend([good, bad])

    c.step()

    assert len(c.fp.sent) == 1
    assert json.loads(c.fp.sent[0][1])["frame_index"] == 1
    assert _poison_metric_value() - before == 1
    # Коммит покрывает оба offset'а (включая ядовитый): last.offset+1 = 2
    assert Counter(c.fc.stored) == Counter([2])
    assert c.fc.commits == 1


def test_batch_failure_degrades_to_per_frame(consumer):
    """Если батч-инференс упал транзиентно — деградация до по-кадрового:
    здоровые кадры не должны страдать от чужих."""
    c = consumer
    c.det.detect_batch.side_effect = RuntimeError("transient GPU hiccup")
    c.fc.queue.extend([
        _msg(0, 0, _jpeg(), frame_index=1),
        _msg(0, 1, _jpeg(), frame_index=2),
    ])

    c.step()

    assert c.det.detect.call_count == 2
    assert len(c.fp.sent) == 2
    assert c.fc.commits == 1


def test_rebalance_during_batch_tolerated(consumer):
    """Rebalance во время батча: store_offsets бросает KafkaException —
    это не краш, кадры уже в detections, дубли — цена at-least-once."""
    c = consumer
    from confluent_kafka import KafkaException

    c.fc.store_offsets = MagicMock(side_effect=KafkaException())
    c.fc.queue.extend([_msg(0, 0, _jpeg(), frame_index=1)])

    c.step()  # не должно бросить

    assert len(c.fp.sent) == 1  # кадр обработан и отправлен


def test_fatal_cuda_error_exits_without_commit(consumer):
    """Липкая CUDA-ошибка: процесс выходит (супервизор поднимет со свежим
    контекстом), коммит НЕ происходит — кадры будут доставлены заново."""
    c = consumer

    class _FakeAcceleratorError(RuntimeError):
        __name__ = "AcceleratorError"

    c.det.detect_batch.side_effect = _FakeAcceleratorError("CUDA error: unknown")
    c.fc.queue.extend([_msg(0, 0, _jpeg(), frame_index=1)])

    with pytest.raises(SystemExit):
        c.step()

    assert c.fc.commits == 0
    assert c.fp.sent == []
