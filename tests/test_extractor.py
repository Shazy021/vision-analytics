import cv2
import numpy as np
import pytest

from app.processing.extractor import VideoCapture


@pytest.fixture(scope='session')
def test_video(tmp_path_factory):
    """
    Создаёт тестовое видео: 3 секунды, 30 FPS, 90 кадров.
    Каждый кадр — чёрный квадрат с номером кадра.
    """
    video_path = str(tmp_path_factory.mktemp("data") / "test.mp4")

    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    writer = cv2.VideoWriter(video_path, fourcc, 30.0, (640, 480))

    for i in range(90): # 3 секунды * 30 фпс = 90 кадров
        frame = np.zeros((480, 640, 3), dtype=np.uint8)
        cv2.putText(frame, str(i), (250, 250),
                    cv2.FONT_HERSHEY_SIMPLEX, 3, (255, 255, 255), 3)
        writer.write(frame)

    writer.release()
    return video_path


def test_video_capture_context_manager(test_video):
    """Контекст-менеджер открывает и закрывает видео"""
    with VideoCapture(test_video) as cap:
        assert cap.isOpened()
        ret, frame = cap.read()
        assert ret
        assert frame is not None


def test_video_capture_invalid_path():
    """Невалидный путь - понятная ошибка"""
    with pytest.raises(ValueError, match="Cannot open video"):
        with VideoCapture("nonexistent.mp4") as _:
            pass
