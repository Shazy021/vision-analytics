import cv2


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
