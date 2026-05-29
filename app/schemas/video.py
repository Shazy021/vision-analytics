"""
Pydantic-схемы для Video — определяют формат ответов API.

Зачем отдельные схемы, а не просто возвращать SQLAlchemy-модель?
1. Контроль: возвращаем только нужные поля, не всё из БД
2. Безопасность: не утекают внутренние данные
3. Документация: FastAPI генерирует Swagger по этим схемам
4. Вычисляемые поля: например, detections_count — в БД нет, но API возвращает
"""
from datetime import datetime
from pydantic import BaseModel, ConfigDict

# ============================================================
# БАЗОВАЯ СХЕМА — общие поля для создания и чтения
# ============================================================
class VideoBase(BaseModel):
    """Поля, общие для создания и чтения видео"""
    filename: str
    duration: float | None = None
    fps: int | None = None
    resolution: str | None = None

# ============================================================
# СХЕМА СОЗДАНИЯ — что присылает клиент при загрузке видео
# ============================================================
class VideoCreate(VideoBase):
    """Запрос на создание видео — только filename обязателен"""
    pass  # наследует всё от VideoBase

# ============================================================
# СХЕМА ЧТЕНИЯ — что получает клиент в ответ
# ============================================================
class VideoRead(VideoBase):
    """
    Ответ API с информацией о видео.

    Отличается от VideoBase:
    - id: генерируется БД, клиент не присылает
    - status: заполняется сервером (default="processing")
    - uploaded_at: генерируется БД (server_default=func.now())
    - detections_count: вычисляемое поле, в БД его нет
    """
    id: int
    status: str
    uploaded_at: datetime | None = None
    detections_count: int = 0  # количество детекций, считаем отдельным запросом

    # ConfigDict — настройка Pydantic
    # from_attributes=True — разрешает читать данные из SQLAlchemy-объектов
    # Раньше это называлось orm_mode = True (устарело)
    model_config = ConfigDict(from_attributes=True)