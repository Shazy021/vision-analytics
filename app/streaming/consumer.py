"""Kafka Consumer — читает детекции и сохраняет в БД"""
import logging
import json
from confluent_kafka import Consumer

# Настройка логгера
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)
from app.streaming.topics import KAFKA_BOOTSTRAP_SERVERS, TOPIC_DETECTIONS
from app.core.database import SessionLocal
from app.db.models import Detection


class DetectionConsumer:
    """Читает детекции из Kafka и пишет в PostgreSQL"""

    def __init__(self):
        self.consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "detection-writer",
            "auto.offset.reset": "earliest",
        })
        self.consumer.subscribe([TOPIC_DETECTIONS])

    def run(self):
        logger.info(f"Listening to Kafka topic '{TOPIC_DETECTIONS}'...")

        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    continue

                detection = json.loads(msg.value().decode())
                self._save_to_db(detection)

        except KeyboardInterrupt:
            logger.info("\nStopping...")
        finally:
            self.consumer.close()

    def _save_to_db(self, detection: dict):
        db = SessionLocal()
        try:
            for obj in detection.get("objects", []):
                db_obj = Detection(
                    video_id=detection.get("video_id"),
                    stream_id=detection.get("stream_id"),
                    frame_index=detection["frame_index"],
                    class_name=obj["class"],
                    confidence=obj["confidence"],
                    bbox_x1=obj["bbox"]["x1"],
                    bbox_y1=obj["bbox"]["y1"],
                    bbox_x2=obj["bbox"]["x2"],
                    bbox_y2=obj["bbox"]["y2"],
                )
                db.add(db_obj)
            db.commit()
            logger.info(f"Saved {len(detection.get('objects', []))} detections for frame #{detection['frame_index']}")
        except Exception as e:
            db.rollback()
            print(f"Error saving to DB: {e}")
        finally:
            db.close()


if __name__ == "__main__":
    consumer = DetectionConsumer()
    consumer.run()