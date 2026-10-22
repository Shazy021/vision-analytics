"""
Pydantic-схемы для Video — определяют формат ответов API.

Зачем отдельные схемы, а не SQLAlchemy-модель: контроль, какие поля
утекают наружу, и Swagger-документация генерируется по ним.
"""
from datetime import datetime

from pydantic import BaseModel, ConfigDict


class VideoBase(BaseModel):
    """Поля, общие для создания и чтения видео."""
    filename: str
    filepath: str | None = None
    object_key: str | None = None  # ключ в MinIO, напр. "videos/5.mp4"
    duration: float | None = None
    fps: int | None = None
    resolution: str | None = None


class VideoCreate(VideoBase):
    """Запрос на создание видео — только filename обязателен."""
    pass


class VideoRead(VideoBase):
    """Ответ API: id/status/uploaded_at приходят из БД,
    detections_count и video_url вычисляются на лету."""
    id: int
    status: str
    uploaded_at: datetime | None = None
    detections_count: int = 0
    video_url: str | None = None

    # Разрешает читать данные из SQLAlchemy-объектов
    model_config = ConfigDict(from_attributes=True)
