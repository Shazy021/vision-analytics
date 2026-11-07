"""Общие типы трассы инференса.

Detection живёт отдельно от TritonDetector
(app/processing/triton_detector.py), потому что формат детекций должен быть
доступен без tritonclient/torch: его читают консьюмеры, processor и тесты.
"""
from dataclasses import dataclass


@dataclass
class Detection:
    class_name: str
    confidence: float
    bbox: list[float]  # xyxy в пикселях исходного кадра
    frame_index: int
    timestamp: float
