"""
Тесты обёртки YOLO.

Модель замочена: скачивать веса и гонять инференс — задача Ultralytics,
нам нужно проверить только наше преобразование результата в свой формат
(включая векторный съём боксов с GPU — одним .cpu(), без per-box sync)
и выбор FP16 для CUDA.
"""
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from app.processing.detector import YOLODetector


def _boxes(cls_values, conf_values, bbox_values):
    """Фейковые boxes: cls/conf/xyxy — «CUDA-тензоры», читаемые так же,
    как в проде: .cpu().numpy(). Вызов .cpu() засчитывается — тест ломается,
    если парсер вернётся к per-box индексации (3 GPU-sync на бокс)."""
    boxes = MagicMock()
    boxes.__len__ = MagicMock(return_value=len(cls_values))
    for name, arr in (
        ("cls", np.array(cls_values)),
        ("conf", np.array(conf_values, dtype=np.float32)),
        ("xyxy", np.array(bbox_values, dtype=np.float32)),
    ):
        tensor = MagicMock()
        tensor.cpu.return_value.numpy.return_value = arr
        setattr(boxes, name, tensor)
    return boxes


@pytest.fixture
def mock_model():
    """Фейковая YOLO-модель, возвращающая заданные результаты (2 детекции)."""
    model = MagicMock()

    fake_result = MagicMock()
    fake_result.boxes = _boxes([2, 0], [0.9, 0.7], [[100, 80, 300, 200], [50, 60, 150, 400]])
    fake_result.names = {0: "person", 2: "car"}

    # model(...) возвращает список с одним результатом
    model.return_value = [fake_result]

    return model


def test_detect_returns_detections(mock_model):
    """detect конвертирует результат YOLO в наш формат."""
    with patch("app.processing.detector.YOLO", return_value=mock_model):
        detector = YOLODetector(weights_path="fake.pt", device="cpu")
        detector.model = mock_model

        fake_frame = np.zeros((480, 640, 3), dtype=np.uint8)
        detections = detector.detect(fake_frame, frame_index=5, timestamp=1.5)

    assert len(detections) == 2

    assert detections[0].class_name == "car"
    assert detections[0].confidence == pytest.approx(0.9)
    assert detections[0].bbox == [100, 80, 300, 200]
    assert detections[0].frame_index == 5
    assert detections[0].timestamp == 1.5

    assert detections[1].class_name == "person"
    assert detections[1].confidence == pytest.approx(0.7)
    assert detections[1].bbox == [50, 60, 150, 400]

    # Векторный съём: каждый тензор снят с «GPU» ровно один раз
    boxes = mock_model.return_value[0].boxes
    assert boxes.cls.cpu.call_count == 1
    assert boxes.conf.cpu.call_count == 1
    assert boxes.xyxy.cpu.call_count == 1


def test_detect_empty_result(mock_model):
    """Ничего не найдено → пустой список."""
    empty_result = MagicMock()
    empty_result.boxes = None
    mock_model.return_value = [empty_result]

    with patch("app.processing.detector.YOLO", return_value=mock_model):
        detector = YOLODetector(weights_path="fake.pt", device="cpu")
        detector.model = mock_model

        fake_frame = np.zeros((480, 640, 3), dtype=np.uint8)
        assert detector.detect(fake_frame) == []


def test_detect_batch_maps_results_per_frame(mock_model):
    """detect_batch: результат i относится к кадру i (frame_index из рефа)."""
    r1 = MagicMock()
    r1.boxes = _boxes([0], [0.5], [[1, 2, 3, 4]])
    r1.names = {0: "person"}
    r2 = MagicMock()
    r2.boxes = None  # кадр без детекций — пустой список, не ошибка
    r2.names = {0: "person"}
    mock_model.return_value = [r1, r2]

    frames = [
        SimpleNamespace(image=np.zeros((8, 8, 3), dtype=np.uint8), index=10, timestamp=0.0),
        SimpleNamespace(image=np.zeros((8, 8, 3), dtype=np.uint8), index=11, timestamp=0.0),
    ]
    with patch("app.processing.detector.YOLO", return_value=mock_model):
        detector = YOLODetector(weights_path="fake.pt", device="cpu")
        detector.model = mock_model
        out = detector.detect_batch(frames)

    assert [d.frame_index for d in out[0]] == [10]
    assert out[1] == []


@pytest.mark.parametrize("device,want_half", [
    ("cpu", False),
    ("cuda", True),
    ("0", True),  # номер GPU как строка — тоже CUDA
])
def test_half_only_for_cuda(device, want_half):
    """FP16 включаем только для CUDA-устройств; device уже конкретный —
    'auto' резолвится слоем settings, до детектора не доезжает."""
    with patch("app.processing.detector.YOLO", return_value=MagicMock()):
        detector = YOLODetector(weights_path="fake.pt", device=device)
    assert detector.half is want_half


def test_fp16_kwargs_reach_the_model():
    """Версия-адаптация не теряется в пути до predict: CUDA-вызов обязан
    нести quantize=16 (Ultralytics ≥8.4) или half=True (старые)."""
    empty = MagicMock()
    empty.boxes = None
    model = MagicMock(return_value=[empty])
    with patch("app.processing.detector.YOLO", return_value=model):
        detector = YOLODetector(weights_path="fake.pt", device="cuda")
        detector.detect(np.zeros((8, 8, 3), dtype=np.uint8))

    kwargs = model.call_args.kwargs
    assert kwargs.get("quantize") == 16 or kwargs.get("half") is True


def test_no_fp16_kwargs_on_cpu():
    empty = MagicMock()
    empty.boxes = None
    model = MagicMock(return_value=[empty])
    with patch("app.processing.detector.YOLO", return_value=model):
        detector = YOLODetector(weights_path="fake.pt", device="cpu")
        detector.detect(np.zeros((8, 8, 3), dtype=np.uint8))

    assert "half" not in model.call_args.kwargs
    assert "quantize" not in model.call_args.kwargs
