import pytest
import cv2
import numpy as np
import os
from app.video.extractor import frame_generator, Frame, VideoCapture

# pytest-фикстура — подготавливает данные для тестов
# scope="session" — создаётся один раз на все тесты в файле
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

@pytest.mark.asyncio
async def test_frame_generator_returns_frames(test_video):
    """ Проверяем что генератор выдаёт кадры """
    frames = []
    async for frame in frame_generator(test_video, fps=5):
        frames.append(frame)

    # 3 сек * 5 FPS = 15 кадров (приблезительно +- 1)
    assert 17 >= len(frames) >= 13

    # Каждый кадр объект Frame
    assert isinstance(frames[0], Frame)
    assert frames[0].index >= 0
    assert frames[0].timestamp >= 0


@pytest.mark.asyncio
async def test_frame_generator_fps_reduces_count(test_video):
    """Меньше фпс -> меньше кадров"""
    frames_5fps = []
    async for f in frame_generator(test_video, fps=5):
        frames_5fps.append(f)

    frames_1fps = []
    async for f in frame_generator(test_video, fps=1):
        frames_1fps.append(f)

    # 1 FPS должно дать меньше кадров, чем 5 FPS
    assert len(frames_1fps) < len(frames_5fps)

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
        with VideoCapture("nonexistent.mp4") as cap:
            pass