"""YOLO Consumer — читает кадры из Kafka, обрабатывает YOLO, отправляет детекции

Phase 4: Сохраняем ЧИСТЫЕ кадры со стримов в MinIO.
Стрим — ephemeral (прошёл и исчез), поэтому кадры нужно сохранить,
чтобы потом показать их с наложенными детекциями из БД.
Кадры сохраняются ТОЛЬКО если на них есть детекции —
нет смысла хранить пустые кадры.
"""
import base64
import json
import logging

import cv2
import numpy as np
from confluent_kafka import Consumer, Producer

from app.core.cuda import is_fatal_cuda_error
from app.core.topics import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_DETECTIONS,
    TOPIC_RAW_FRAMES,
)

logger = logging.getLogger(__name__)

# CUDA sticky error (CUDA_ERROR_UNKNOWN, illegal memory access, ECC):
# контекст мёртв, и КАЖДЫЙ последующий вызов будет падать — восстановление
# внутри процесса невозможно. Единственный правильный вариант — выйти с
# ненулевым кодом, чтобы супервизор (docker restart) поднял свежий процесс
# со свежим CUDA-контекстом.
MAX_CONSECUTIVE_FAILURES = 10


def _is_fatal_cuda_error(exc: BaseException) -> bool:
    """Sticky CUDA error: torch.AcceleratorError или сообщение 'CUDA error'.

    Проверяем по имени класса, а не через isinstance(torch.AcceleratorError),
    чтобы не тянуть torch в импорт модуля (он нужен только в рантайме YOLO).
    OOM ('CUDA out of memory') сюда НЕ попадает — он не липкий.
    """
    if type(exc).__name__ == "AcceleratorError":
        return True
    text = str(exc)
    return "CUDA error" in text or "cuCtxSynchronize" in text

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
        # Ленивый импорт: YOLODetector тянет ultralytics/torch, а модуль
        # импортируется и там, где инференс не нужен (тесты, линт).
        # Тот же принцип, что с проверкой AcceleratorError по имени класса.
        from app.processing.detector import YOLODetector
        # Одна точка входа в инференс на всю платформу: видео-трасса
        # (processor.py) и стримовая используют один и тот же YOLODetector —
        # пороги и разбор боксов не разъедутся.
        self.detector = YOLODetector(weights_path=model_path, device=device)

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
        except Exception:
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
        self.consecutive_failures = 0

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
                    self.consecutive_failures = 0
                except Exception as exc:
                    self.poison_count += 1
                    self.consecutive_failures += 1
                    topic_name = msg.topic()
                    offset = msg.offset()
                    if is_fatal_cuda_error(exc):
                        # Липкая CUDA-ошибка: следующий кадр упадёт так же,
                        # и следующий, и дальше вечно — «пропуск» тут только
                        # сжигает offset'ы. Выходим, контейнер рестартует
                        # (restart-политика) с чистым CUDA-контекстом.
                        logger.critical(
                            f"CUDA context is dead (topic={topic_name} offset={offset}): {exc}. "
                            "Sticky CUDA errors are unrecoverable in-process — "
                            "exiting, the supervisor will restart the consumer.",
                        )
                        raise SystemExit(1)
                    logger.exception(
                        f"Poison message skipped: topic={topic_name} offset={offset} (total {self.poison_count})",
                    )
                    if self.consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                        # Не CUDA, но тоже зомби-режим (например, перманентный OOM):
                        # N подряд падений — надёжнее рестартнуться, чем молча
                        # сжигать кадры.
                        logger.critical(
                            f"{self.consecutive_failures} consecutive failures — "
                            "exiting for restart.",
                        )
                        raise SystemExit(1)

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

        # YOLO — разбор боксов общий с видео-трассой (YOLODetector)
        detections = self.detector.detect(frame, frame_index=data["frame_index"])
        objects = [
            {
                "class": d.class_name,
                "confidence": d.confidence,
                "bbox": {"x1": d.bbox[0], "y1": d.bbox[1], "x2": d.bbox[2], "y2": d.bbox[3]},
            }
            for d in detections
        ]

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
            # Время захвата кадра сквозняком: инференс и очередь не должны
            # сдвигать detected_at — writer поставит из него, не от now().
            "captured_at": data.get("captured_at"),
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

    # Веса: аргумент CLI > WEIGHTS_PATH (settings). Без аргумента — None,
    # чтобы YOLOConsumer взял путь из настроек; fallback "yolov8n.pt" здесь
    # захардкодил бы nano навсегда, как бы ни был озаглавлен compose-командой.
    model = sys.argv[1] if len(sys.argv) > 1 else None
    # Без явного аргумента устройство берётся из настроек:
    # DEVICE=auto → cuda при наличии GPU, иначе cpu (проект не падает без NVIDIA)
    device = sys.argv[2] if len(sys.argv) > 2 else None
    consumer = YOLOConsumer(model_path=model, device=device)
    consumer.run()
