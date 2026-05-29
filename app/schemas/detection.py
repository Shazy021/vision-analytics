"""
Pydantic-схемы для Detection.
"""
from pydantic import BaseModel, ConfigDict


class DetectionBase(BaseModel):
    """Общие поля детекции"""
    class_name: str
    confidence: float
    bbox_x1: float
    bbox_y1: float
    bbox_x2: float
    bbox_y2: float
    frame_index: int
    timestamp: float


class DetectionRead(DetectionBase):
    """Ответ API с детекцией"""
    id: int
    video_id: int

    model_config = ConfigDict(from_attributes=True)