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

    # nullable=True — поле необязательное (значение может быть NULL)
    duration = Column(Float, nullable=True)
    fps = Column(Integer, nullable=True)
    resolution = Column(String(20), nullable=True)

    # server_default=func.now() — при вставке PostgreSQL автоматически
    # подставит текущее время
    uploaded_at = Column(DateTime, server_default=func.now())

    # Статус обработки: processing -> completed / failed
    status = Column(String(20), nullable=False, default="processing")

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

    # ForeignKey - внешний ключ: ссылается на videos.id
    # ondelete="CASCADE" — если видео удаляется, удаляются и его детекции
    video_id = Column(Integer, ForeignKey("videos.id", ondelete="CASCADE"), nullable=False)

    frame_index = Column(Integer, nullable=False)
    class_name = Column(String(50), nullable=False)
    confidence = Column(Float, nullable=False)

    # 4 координаты бокса
    bbox_x1 = Column(Float, nullable=False)
    bbox_y1 = Column(Float, nullable=False)
    bbox_x2 = Column(Float, nullable=False)
    bbox_y2 = Column(Float, nullable=False)

    timestamp = Column(Float, nullable=False)

    # Обратная связь: detection.video -> объект Video
    video = relationship("Video", back_populates="detections")

    __table_args__ = (
        Index("idx_detections_video_id", "video_id"),
        Index("idx_detections_class", "class_name"),
        Index("idx_detections_timestamp", "timestamp"),
        Index("idx_detections_video_class", "video_id", "class_name"),
    )