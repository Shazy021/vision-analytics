"""
Тесты обёртки YOLO.

Модель замочена: скачивать веса и гонять инференс — задача Ultralytics,
нам нужно проверить только наше преобразование результата в свой формат.
"""
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from app.detection.yolo import YOLODetector


@pytest.fixture
def mock_model():
    """Фейковая YOLO-модель, возвращающая заданные результаты (2 детекции)."""
    model = MagicMock()

    fake_boxes = MagicMock()
    fake_boxes.__len__ = MagicMock(return_value=2)

    # Детекция 1: car, confidence 0.9; Детекция 2: person, confidence 0.7
    cls_values = {0: 2, 1: 0}  # Индекс 0 → класс 2 (car), 1 → класс 0 (person)
    conf_values = {0: 0.9, 1: 0.7}
    bbox_values = {0: [100, 80, 300, 200], 1: [50, 60, 150, 400]}

    fake_boxes.cls.__getitem__ = lambda self, i: cls_values[i]
    fake_boxes.conf.__getitem__ = lambda self, i: conf_values[i]
    fake_boxes.xyxy.__getitem__ = lambda self, i: MagicMock(tolist=lambda: bbox_values[i])

    fake_result = MagicMock()
    fake_result.boxes = fake_boxes
    fake_result.names = {0: "person", 2: "car"}

    # model(...) возвращает список с одним результатом
    model.return_value = [fake_result]

    return model


def test_detect_returns_detections(mock_model):
    """detect конвертирует результат YOLO в наш формат."""
    with patch("app.detection.yolo.YOLO", return_value=mock_model):
        detector = YOLODetector(weights_path="fake.pt", device="cpu")
        detector.model = mock_model

        fake_frame = np.zeros((480, 640, 3), dtype=np.uint8)
        detections = detector.detect(fake_frame, frame_index=5, timestamp=1.5)

    assert len(detections) == 2

    assert detections[0].class_name == "car"
    assert detections[0].confidence == 0.9
    assert detections[0].bbox == [100, 80, 300, 200]
    assert detections[0].frame_index == 5
    assert detections[0].timestamp == 1.5

    assert detections[1].class_name == "person"
    assert detections[1].confidence == 0.7
    assert detections[1].bbox == [50, 60, 150, 400]


def test_detect_empty_result(mock_model):
    """Ничего не найдено → пустой список."""
    empty_result = MagicMock()
    empty_result.boxes = None
    mock_model.return_value = [empty_result]

    with patch("app.detection.yolo.YOLO", return_value=mock_model):
        detector = YOLODetector(weights_path="fake.pt", device="cpu")
        detector.model = mock_model

        fake_frame = np.zeros((480, 640, 3), dtype=np.uint8)
        assert detector.detect(fake_frame) == []
