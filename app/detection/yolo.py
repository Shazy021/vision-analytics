from dataclasses import dataclass
from ultralytics import YOLO
from app.core.config import settings

@dataclass
class Detection:
    class_name: str
    confidence: float
    bbox: list[float]
    frame_index: int
    timestamp: float

class YOLODetector:
    """
    Обёртка вокруг YOLO от Ultralytics.
    """
    def __init__(self, weights_path: str = None, device: str = None):
        """
        Грузим модель при создании объекта
        
        weights_path: путь к файлу весов (.pt)
        device: "cpu" или "cuda" (GPU)
        
        Если не указаны — берём из .env
        """
        weights_path = weights_path or settings.weights_path
        device = device or settings.device

        self.model = YOLO(weights_path)
        self.device = device

    def detect(self, frame, frame_index: int = 0, timestamp: float = 0.0, conf: float = 0.25) -> list[Detection]:
        """
         Детекция объектов на одном кадре.
        
        Аргументы:
            frame: numpy-массив — картинка из OpenCV
            frame_index: номер кадра (для записи в результат)
            timestamp: время кадра в секундах
            conf: порог уверенности, всё ниже — отбрасывается
        
        Возвращает:
            Список Detection — найденные объекты
        """
        results = self.model(frame, conf=conf, device=self.device, verbose=False)
        return self._parse_results(results[0], frame_index, timestamp)
    
    def detect_batch(self, frames: list, conf: float = 0.25) -> list[list[Detection]]:
        """
        Детекция на нескольких кадрах сразу (батчами).
        
        Зачем: GPU эффективнее обрабатывает несколько картинок
        одновременно (параллелизм на уровне чипа).
        На CPU разницы почти нет.
        """
        images = [f.image for f in frames]
        results = self.model(images, conf=conf, device=self.device, verbose=False)

        all_detections = []
        for i, result in enumerate(results):
            detections = self._parse_results(result, frames[i].index, frames[i].timestamp)
            all_detections.append(detections)

        return all_detections
    
    def _parse_results(self, result, frame_index: int, timestamp: float) -> list[Detection]:
        """
        Конвертация результата Ultralytics в наш формат Detection.
        
        result.boxes — объект с найденными боксами
        result.boxes.cls — номера классов (0=person, 2=car, ...)
        result.boxes.conf — уверенность для каждого бокса
        result.boxes.xyxy — координаты [x1, y1, x2, y2]
        
        result.names — словарь {0: "person", 1: "bicycle", 2: "car", ...}
        """
        detections = []

        # Если ничего не нашли - пустой список
        if result.boxes is None or len(result.boxes) == 0:
            return detections
        
        for i in range(len(result.boxes)):
            class_id = int(result.boxes.cls[i])
            class_name = result.names[class_id]

            confidance = float(result.boxes.conf[i])
            bbox = result.boxes.xyxy[i].tolist()

            detections.append(Detection(
                class_name=class_name,
                confidence=confidance,
                bbox=bbox,
                frame_index=frame_index,
                timestamp=timestamp
            ))

        return detections
    