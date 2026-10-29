"""Kafka Consumer — читает детекции и сохраняет в БД"""
import json
import logging
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer

from app.core.database import SessionLocal
from app.core.metrics import (
    frame_e2e_latency_seconds,
    frame_queue_wait_seconds,
    frame_write_duration_seconds,
    frame_written_total,
    parse_epoch_seconds,
    queue_wait_seconds,
    start_metrics_server,
)
from app.core.topics import KAFKA_BOOTSTRAP_SERVERS, TOPIC_DETECTIONS
from app.db.models import StreamDetection

# Настройка логгера (после импортов — E402)
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


def _parse_captured_at(raw: str | None) -> datetime | None:
    """captured_at из сообщения → naive UTC (тот же стиль, что у now() в колонке).

    None при отсутствии/битом значении — тогда detected_at поставит
    server_default (now()), как было до появления captured_at: старые
    сообщения в топике не должны ломать запись.
    """
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        logger.warning("Некорректный captured_at %r — detected_at возьмём из now()", raw)
        return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


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

                wait = queue_wait_seconds(msg)
                if wait is not None:
                    frame_queue_wait_seconds.labels(topic=TOPIC_DETECTIONS).observe(wait)

                detection = json.loads(msg.value().decode())
                self._save_to_db(detection)

        except KeyboardInterrupt:
            logger.info("\nStopping...")
        finally:
            self.consumer.close()

    def _save_to_db(self, detection: dict) -> str:
        """Сохранить кадр детекций; вернуть итог для метрик.

        Результат — значение лейбла frame_written_total:
        ok (записан, captured_at валиден) | missing_ts (записан, но e2e
        не измерить) | skipped (нет stream_id) | error (БД отвалилась).
        """
        started = time.perf_counter()
        # Топик detections — исключительно стримовый (видео пишут в БД
        # напрямую из video-consumer), поэтому stream_id обязателен.
        stream_id = detection.get("stream_id")
        if stream_id is None:
            logger.warning("Skipping detection without stream_id: %s", detection)
            frame_written_total.labels(result="skipped").inc()
            return "skipped"

        # Отдельно от _parse_captured_at (там naive datetime для колонки):
        # epoch нужен для e2e-задержки; битое/отсутствующее значение — None,
        # e2e не меряем, но пишем как раньше (fallback на now()).
        captured_epoch = parse_epoch_seconds(detection.get("captured_at"))

        db = SessionLocal()
        try:
            captured_at = _parse_captured_at(detection.get("captured_at"))
            for obj in detection.get("objects", []):
                fields = dict(
                    stream_id=stream_id,
                    frame_index=detection["frame_index"],
                    class_name=obj["class"],
                    confidence=obj["confidence"],
                    bbox_x1=obj["bbox"]["x1"],
                    bbox_y1=obj["bbox"]["y1"],
                    bbox_x2=obj["bbox"]["x2"],
                    bbox_y2=obj["bbox"]["y2"],
                )
                # Явный None перезаписал бы server_default (now()) на NULL
                # и получил бы NOT NULL violation — поэтому поле добавляем
                # только когда время кадра действительно есть.
                if captured_at is not None:
                    fields["detected_at"] = captured_at
                db_obj = StreamDetection(**fields)
                db.add(db_obj)
            db.commit()
            logger.info(f"Saved {len(detection.get('objects', []))} detections for frame #{detection['frame_index']}")
        except Exception:
            db.rollback()
            logger.exception("Error saving to DB")
            frame_written_total.labels(result="error").inc()
            return "error"
        else:
            result = "ok" if captured_epoch is not None else "missing_ts"
            frame_written_total.labels(result=result).inc()
            if captured_epoch is not None:
                # e2e: захват кадра → коммит. Отрицательное значение — перекос
                # часов producer'а, мусор в гистограмме портит квантили.
                latency = time.time() - captured_epoch
                if latency >= 0:
                    frame_e2e_latency_seconds.labels(stream_id=str(stream_id)).observe(latency)
            return result
        finally:
            db.close()
            frame_write_duration_seconds.observe(time.perf_counter() - started)


if __name__ == "__main__":
    # Порт до коннекта к Kafka: /metrics должен жить даже в ожидании брокера
    start_metrics_server(9092)
    consumer = DetectionConsumer()
    consumer.run()
