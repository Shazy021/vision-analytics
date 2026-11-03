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
    """Обёртка вокруг Ultralytics YOLO: модель грузится один раз, детекция — по кадрам."""

    def __init__(self, weights_path: str = None, device: str = None):
        # Если не указаны — берём из .env
        weights_path = weights_path or settings.weights_path
        device = device or settings.device

        self.model = YOLO(weights_path)
        self.device = device
        # FP16 на CUDA: forward ~1.7–2x быстрее на современных RTX, веса и
        # активации весят вдвое меньше. device здесь уже конкретный
        # ("cuda"/"0"/"cpu") — settings резолвит "auto" на своём уровне,
        # поэтому достаточно проверки «не cpu».
        self.half = str(device).lower() != "cpu"
        self._half_kwargs = self._fp16_kwargs()

    @staticmethod
    def _fp16_kwargs() -> dict:
        """FP16-аргументы predict под версию Ultralytics.

        В 8.4 half переименован в quantize (16 = FP16); старый half ещё
        принимается, но пишет deprecated-варнинг на КАЖДЫЙ вызов (спам в
        логах консьюмера батч за батчем). Определяем один раз при старте."""
        try:
            from ultralytics.cfg import DEFAULT_CFG
            if hasattr(DEFAULT_CFG, "quantize"):
                return {"quantize": 16}
        except Exception:
            pass
        return {"half": True}

    def detect(self, frame, frame_index: int = 0, timestamp: float = 0.0, conf: float = 0.25) -> list[Detection]:
        """
        Детекция объектов на одном кадре.

        conf — порог уверенности, всё ниже отбрасывается.
        """
        results = self.model(
            frame, conf=conf, device=self.device, verbose=False,
            **(self._half_kwargs if self.half else {}),
        )
        return self._parse_results(results[0], frame_index, timestamp)

    def detect_batch(self, frames: list, conf: float = 0.25) -> list[list[Detection]]:
        """
        Детекция на нескольких кадрах сразу (батчами).

        GPU эффективнее обрабатывает несколько картинок одновременно.
        На CPU разницы почти нет.
        """
        images = [f.image for f in frames]
        results = self.model(
            images, conf=conf, device=self.device, verbose=False,
            **(self._half_kwargs if self.half else {}),
        )

        all_detections = []
        for i, result in enumerate(results):
            detections = self._parse_results(result, frames[i].index, frames[i].timestamp)
            all_detections.append(detections)

        return all_detections

    def _parse_results(self, result, frame_index: int, timestamp: float) -> list[Detection]:
        """Конвертация результата Ultralytics в наш формат Detection.

        Тензоры снимаются с GPU ОДНИМ .cpu(): индексация CUDA-тензора по
        боксам (cls[i], conf[i], xyxy[i]) — это полный GPU-sync на каждый
        доступ, 3N синхронизаций на кадр с N боксов. Под нагрузкой это
        добавляло миллисекунды к каждому кадру и блокировало конвейер.
        """
        boxes = result.boxes
        # Если ничего не нашли — пустой список
        if boxes is None or len(boxes) == 0:
            return []

        cls = boxes.cls.cpu().numpy()
        conf = boxes.conf.cpu().numpy()
        xyxy = boxes.xyxy.cpu().numpy()

        return [
            Detection(
                class_name=result.names[int(c)],
                confidence=float(cf),
                bbox=box.tolist(),
                frame_index=frame_index,
                timestamp=timestamp,
            )
            for c, cf, box in zip(cls, conf, xyxy)
        ]
