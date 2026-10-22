"""
Pydantic-схемы для Stream — определяют формат запросов/ответов API.

Зачем отдельные схемы, а не SQLAlchemy-модель: контроль полей, безопасность,
Swagger-документация (см. schemas/video.py).
"""
from pydantic import BaseModel, ConfigDict


class StreamCreate(BaseModel):
    """Запрос на создание стрима."""
    name: str
    rtsp_url: str
    fps: int = 5


class StreamResponse(BaseModel):
    """Ответ API о стриме."""
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    rtsp_url: str
    status: str
    fps: int
    detections_count: int = 0
