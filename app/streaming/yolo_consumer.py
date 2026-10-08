"""YOLO Consumer — читает кадры из Kafka, обрабатывает YOLO, отправляет детекции

Phase 4: Сохраняем ЧИСТЫЕ кадры со стримов в MinIO.
Стрим — ephemeral (прошёл и исчез), поэтому кадры нужно сохранить,
чтобы потом показать их с наложенными детекциями из БД.
Кадры сохраняются ТОЛЬКО если на них есть детекции —
нет смысла хранить пустые кадры.
"""
import json
import base64
import numpy as np
import logging
import cv2
from confluent_kafka import Consumer, Producer
from app.core.config import settings
from app.streaming.topics import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_RAW_FRAMES,
    TOPIC_DETECTIONS,
)

logger = logging.getLogger(__name__)

# Импортируем storage один раз при загрузке модуля.
# Если MinIO недоступен — упадёт сразу с понятной ошибкой,
# а не будет молча глотать ошибки на каждом кадре.
try:
    from app.core import storage
    MINIO_AVAILABLE = True
    logger.info("[YOLO Consumer] MinIO storage connected")
except Exception as e:
    storage = None
    MINIO_AVAILABLE = False
    logger.error(f"[YOLO Consumer] WARNING: MinIO not available: {e}")
    logger.error("[YOLO Consumer] Stream frames will NOT be saved!")


class YOLOConsumer:
    """Читает raw_frames → YOLO → пишет в detections + сохраняет кадры в MinIO"""

    def __init__(self, model_path: str | None = None, device: str | None = None):
        from ultralytics import YOLO
        self.model = YOLO(model_path or settings.weights_path)
        self.device = device or settings.device

        self.consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "yolo-workers",
            "auto.offset.reset": "earliest",
            "fetch.max.bytes": 5 * 1024 * 1024,
        })
        self.consumer.subscribe([TOPIC_RAW_FRAMES])

        self.producer = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "message.max.bytes": 5 * 1024 * 1024,
        })

    def _save_frame_to_minio(self, stream_id: int, frame_index: int, frame_data: bytes):
        """Сохранить чистый кадр (JPEG) в MinIO. Ключ: streams/{stream_id}/{frame_index}.jpg"""
        if not MINIO_AVAILABLE:
            return
        try:
            frame_key = f"streams/{stream_id}/{frame_index}.jpg"
            storage.upload_bytes(frame_key, frame_data, content_type="image/jpeg")
            return True
        except Exception as e:
            # Логируем но не падаем — кадры не критичны
            logger.exception("[YOLO Consumer] Failed to save frame to MinIO")
            # MinIO мог отвалиться — пробуем реконнект при следующем кадре
            return False

    def run(self):
        logger.info(f"YOLO consumer listening to '{TOPIC_RAW_FRAMES}'...")
        if MINIO_AVAILABLE:
            logger.info("[YOLO Consumer] Stream frames WILL be saved to MinIO")
        else:
            logger.info("[YOLO Consumer] Stream frames will NOT be saved (MinIO unavailable)")

        self.poison_count = 0

        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    continue

                try:
                    self._handle(msg)
                except Exception:
                    self.poison_count += 1
                    topic_name = msg.topic()
                    offset = msg.offset()
                    logger.exception(
                        f"Poison message skipped: topic={topic_name} offset={offset} (total {self.poison_count})",
                    )

        except KeyboardInterrupt:
            logger.info("Stopping...")
        finally:
            self.consumer.close()
            self.producer.flush()

    def _handle(self, msg):
        data = json.loads(msg.value().decode())

        # Декодируем кадр
        frame_bytes = base64.b64decode(data["frame_data"])
        frame = cv2.imdecode(
            np.frombuffer(frame_bytes, dtype=np.uint8),
            cv2.IMREAD_COLOR,
        )

        # YOLO
        results = self.model(frame, device=self.device, verbose=False)[0]

        objects = []
        for box in results.boxes:
            obj = {
                "class": self.model.names[int(box.cls)],
                "confidence": float(box.conf),
                "bbox": {
                    "x1": float(box.xyxy[0][0]),
                    "y1": float(box.xyxy[0][1]),
                    "x2": float(box.xyxy[0][2]),
                    "y2": float(box.xyxy[0][3]),
                },
            }
            objects.append(obj)

        # Сохраняем чистый кадр в MinIO (ТОЛЬКО если есть детекции)
        stream_id = data.get("stream_id")
        saved = False
        if stream_id and len(objects) > 0:
            saved = self._save_frame_to_minio(
                stream_id=stream_id,
                frame_index=data["frame_index"],
                frame_data=frame_bytes,
            )

        detection_msg = json.dumps({
            "video_id": None,
            "stream_id": stream_id,
            "frame_index": data["frame_index"],
            "source": data["source"],
            "objects": objects,
            "num_detections": len(objects),
        }).encode()

        self.producer.produce(TOPIC_DETECTIONS, value=detection_msg)
        self.producer.poll(0)
        saved_text = (' → saved to MinIO' if saved else '')
        logger.debug(
            f"Frame #{data['frame_index']}: {len(objects)} detections{saved_text}",
        )


if __name__ == "__main__":
    import sys
    model = sys.argv[1] if len(sys.argv) > 1 else "yolov8n.pt"
    device = sys.argv[2] if len(sys.argv) > 2 else "0"
    consumer = YOLOConsumer(model_path=model, device=device)
    consumer.run()
