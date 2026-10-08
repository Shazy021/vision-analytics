from dataclasses import dataclass
from typing import AsyncGenerator

import cv2

# dataclass — автоматически создаёт __init__, __repr__, __eq__
# Без dataclass тебе пришлось бы писать:
#   class Frame:
#       def __init__(self, index, image, timestamp):
#           self.index = index
#           self.image = image
#           self.timestamp = timestamp
# С dataclass — 3 строчки вместо 7

@dataclass
class Frame:
    index: int # Номер кадра в видео (0, 1, 2, ...)
    image: any # numpy-массив — сама картинка
    timestamp: float # Секунды от начала видео

class VideoCapture:
    """
    Контекст-менеджер для cv2.VideoCapture.

    Гарантирует, что файл будет закрыт в ЛЮБОМ случае —
    даже если произошла ошибка.

    Использование:
        with VideoCapture("video.mp4") as cap:
            ret, frame = cap.read()
        # cap автоматически закрыт, даже если была ошибка
    """
    def __init__(self, video_path: str):
        self.video_path = video_path
        self.cap = None

    def __enter__(self):
        """ Вызывается при входе в with-block"""
        self.cap = cv2.VideoCapture(self.video_path)
        if not self.cap.isOpened():
            raise ValueError(f"Cannot open video: {self.video_path}")
        return self.cap

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Вызывается при выходе из with-блок — ЛЮБОМ: нормальном или с ошибкой"""
        if self.cap is not None:
            self.cap.release()
        return False # False = не подавлять исключения, пусть летят дальше

async def frame_generator(
    video_path: str,
    fps: int = 5
) -> AsyncGenerator[Frame, None]:
    """
    Асинхронный генератор кадров из видео.

    Что происходит:
    1. Открываем видео через OpenCV
    2. Считаем, через сколько кадров брать следующий
       (видео 30 FPS, мы хотим 5 FPS → берём каждый 6-й кадр)
    3. Yield'им кадр за кадром
    4. В конце обязательно освобождаем ресурсы

    Аргументы:
        video_path: путь к видео-файлу
        fps: сколько кадров в секунду извлекать (по умолчанию 5)

    Возвращает:
        AsyncGenerator[Frame, None] — кадры по одному
    """
    with VideoCapture(video_path) as cap:
        original_fps = cap.get(cv2.CAP_PROP_FPS)
        if original_fps <= 0:
            original_fps = 30

        interval = max(1, int(original_fps/fps))
        frame_index = 0

        while True:
            ret, frame = cap.read()
            if not ret:
                break

            if frame_index % interval == 0:
                timestamp = frame_index/original_fps
                yield Frame(index=frame_index, image=frame, timestamp=timestamp)

            frame_index += 1
