from sqlalchemy import Column, DateTime, Float, ForeignKey, Index, Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.core.database import Base


class Video(Base):
    """Загруженные видео."""
    __tablename__ = "videos"

    id = Column(Integer, primary_key=True, autoincrement=True)
    filename = Column(String(255), nullable=False)
    filepath = Column(String(512), nullable=True)
    object_key = Column(String(512), nullable=True)  # ключ в MinIO, напр. "videos/5.mp4"

    duration = Column(Float, nullable=True)
    fps = Column(Integer, nullable=True)
    resolution = Column(String(20), nullable=True)
    uploaded_at = Column(DateTime, server_default=func.now())

    # Статус обработки: processing -> completed / partial / failed
    status = Column(String(20), nullable=False, default="processing")
    task_id = Column(String(255), nullable=True)  # ID Celery-задачи

    # cascade: удаление Video автоматически удаляет его Detection
    detections = relationship("Detection", back_populates="video", cascade="all, delete-orphan")


class Detection(Base):
    """Найденные объекты — общая таблица для видео и стримов."""
    __tablename__ = "detections"

    id = Column(Integer, primary_key=True, autoincrement=True)
    video_id = Column(Integer, ForeignKey("videos.id", ondelete="CASCADE"), nullable=True)
    stream_id = Column(Integer, ForeignKey("streams.id", ondelete="CASCADE"), nullable=True)

    frame_index = Column(Integer, nullable=False)
    class_name = Column(String(50), nullable=False)
    confidence = Column(Float, nullable=False)
    bbox_x1 = Column(Float, nullable=False)
    bbox_y1 = Column(Float, nullable=False)
    bbox_x2 = Column(Float, nullable=False)
    bbox_y2 = Column(Float, nullable=False)

    # ДВЕ системы времени: timestamp — секунда от начала файла (только видео),
    # detected_at — реальное время детекции (работает и для файлов, и для стримов).
    timestamp = Column(Float, nullable=True)
    detected_at = Column(DateTime, server_default=func.now(), nullable=False)

    video = relationship("Video", back_populates="detections")
    stream = relationship("Stream", back_populates="detections")

    __table_args__ = (
        Index("idx_detections_video_id", "video_id"),
        Index("idx_detections_stream_id", "stream_id"),
        Index("idx_detections_class", "class_name"),
        Index("idx_detections_detected_at", "detected_at"),
        Index("idx_detections_video_class", "video_id", "class_name"),
        Index("idx_detections_stream_class", "stream_id", "class_name"),
    )


class Stream(Base):
    """RTSP/IP камеры."""
    __tablename__ = "streams"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)
    rtsp_url = Column(String(512), nullable=False)

    # ДВА разных статуса — это ядро паттерна desired-state + reconciler.
    #
    # desired_status = НАМЕРЕНИЕ («что пользователь хочет»). Пишет только API
    # (start/stop). Это «заказ», а не факт: стрим мог быть заказан, но producer
    # ещё не поднят, упал или упирается в backoff.
    #
    # status = ФАКТ («что есть на самом деле»). Пишет только stream_manager
    # (reconciler): active / inactive / error. API этот флаг НЕ выставляет —
    # он лишь читает и отдаёт наружу.
    #
    # Раньше оба смысла были слиты в одном status, и выставлял его API через
    # живость своего Popen. Из-за этого статус жил в памяти API-контейнера и
    # умирал вместе с ним. Теперь намерение и факт переживают рестарт API,
    # потому что оба лежат в БД, а факт обновляет независимый сервис.
    desired_status = Column(String(20), nullable=False, default="inactive")  # active/inactive
    status = Column(String(20), nullable=False, default="inactive")          # active/inactive/error
    # Причина перехода status в "error" (например, «producer умер через 3с,
    # превышен лимит рестартов»). Пусто, когда статуса ошибки нет.
    status_reason = Column(String(255), nullable=True)

    fps = Column(Integer, nullable=True, default=5)
    created_at = Column(DateTime, server_default=func.now())
    pid = Column(Integer, nullable=True)

    detections = relationship("Detection", back_populates="stream", cascade="all, delete-orphan")
