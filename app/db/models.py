from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Index
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.core.database import Base

# Base - базовый класс для всех моделей SQLAlchemy
# Он связывает Python-класс с SQL-таблицей
# Определяется в database.py

class Video(Base):
    """
    Таблица videos - информация о загруженных видео.

    Base.__tablename__ - имя таблицы в PostgreSQL
    Column(...) описание колонки
    """
    __tablename__ = "videos"

    # primary_key=True — это главный ключ, уникальный идентификатор
    # autoincrement=True — PostgreSQL сама назначает id (1, 2, 3, ...)
    id = Column(Integer, primary_key=True, autoincrement=True)

    # nullable=False - поле обязательное, нельзя оставлять пустым
    filename = Column(String(255), nullable=False)
    filepath = Column(String(512), nullable=True)
    object_key = Column(String(512), nullable=True)     # ключ в MinIO (например, "videos/5.mp4")

    # nullable=True — поле необязательное (значение может быть NULL)
    duration = Column(Float, nullable=True)
    fps = Column(Integer, nullable=True)
    resolution = Column(String(20), nullable=True)

    # server_default=func.now() — при вставке PostgreSQL автоматически
    # подставит текущее время
    uploaded_at = Column(DateTime, server_default=func.now())

    # Статус обработки: processing -> completed / failed
    status = Column(String(20), nullable=False, default="processing")

    task_id = Column(String(255), nullable=True)  # ID Celery-задачи

    # relationship — НЕ колонка в БД. Это Python-атрибут для удобного доступа.
    # video.detections → список всех Detection для этого видео
    # back_populates — двусторонняя связь: Video знает свои Detection,
    # Detection знает свой Video
    detections = relationship("Detection", back_populates="video", cascade="all, delete-orphan")

    # cascade="all, delete-orphan" — если удалить Video,
    # автоматически удалятся все его Detection

class Detection(Base):
    """Таблица detections - найденные объекты."""
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

    # Видео: секунда от начала файла (5.0, 10.5, ...)
    timestamp = Column(Float, nullable=True)

    # Реальное время детекции — работает И для файлов И для стримов
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
    """Таблица streams - RTSP/IP камеры."""
    __tablename__ = "streams"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)           # "Парковка камера 1"
    rtsp_url = Column(String(512), nullable=False)       # rtsp://...
    status = Column(String(20), nullable=False, default="inactive")  # active/inactive
    fps = Column(Integer, nullable=True, default=5)      # частота захвата
    created_at = Column(DateTime, server_default=func.now())
    pid = Column(Integer, nullable=True)

    detections = relationship("Detection", back_populates="stream", cascade="all, delete-orphan")