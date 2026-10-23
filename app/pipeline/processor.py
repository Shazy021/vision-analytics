"""
Пайплайн обработки видео — связывает все компоненты вместе.

Видео → кадры → YOLO-детекция → сохранение в БД

Phase 4: Видео читается НАПРЯМУЮ из MinIO по HTTP.
OpenCV (cv2.VideoCapture) поддерживает HTTP URL —
никакого скачивания на диск.
"""
import logging
from pathlib import Path

from sqlalchemy.orm import Session

from app.core import storage
from app.core.config import settings
from app.db.models import Detection, Video
from app.detection.yolo import YOLODetector
from app.video.extractor import VideoCapture

logger = logging.getLogger(__name__)


def _get_video_source(video: Video) -> str:
    """
    Получить источник видео для OpenCV.

    Если есть object_key — генерируем presigned URL,
    OpenCV читает по HTTP напрямую из MinIO.
    Если нет — используем старый filepath с диска.
    """
    if video.object_key:
        url = storage.get_presigned_url(video.object_key, expires_hours=24)
        logger.info("Reading %s directly from MinIO via HTTP", video.object_key)
        return url

    if video.filepath and Path(video.filepath).exists():
        logger.info("Reading from local disk: %s", video.filepath)
        return video.filepath

    raise FileNotFoundError(
        f"Video file not found: object_key={video.object_key}, filepath={video.filepath}"
    )


def process_video(video_id: int, db: Session) -> None:
    """
    Обработать видео: кадры → детекции → БД.

    Видео читается НАПРЯМУЮ из MinIO по HTTP — никакого скачивания.
    OpenCV (через FFmpeg) стримит кадры по сети.

    Кадры НЕ сохраняем — у нас есть целое видео в MinIO,
    а у детекций есть timestamp. Кадр можно достать по таймстампу.
    """
    # ============================================================
    # Шаг 1: Находим видео в БД
    # ============================================================
    video = db.query(Video).filter(Video.id == video_id).first()
    if not video:
        raise ValueError(f"Video with id={video_id} not found")

    # ============================================================
    # Шаг 2: Получаем источник видео (URL или путь)
    # ============================================================
    video_source = _get_video_source(video)

    # ============================================================
    # Шаг 3: Ставим статус "processing"
    # ============================================================
    db.query(Detection).filter(Detection.video_id == video_id).delete()
    video.status = "processing"
    db.commit()

    try:
        # ============================================================
        # Шаг 4-5: Извлекаем кадры и детектируем объекты
        # ============================================================
        detector = YOLODetector()
        all_detections = []

        with VideoCapture(video_source) as cap:
            frame_index = 0
            fps = cap.get(5)
            total_frames = int(cap.get(7))
            duration = total_frames / fps if fps > 0 else 0

            # Параметры разбора: оверрайд конкретного видео (выбран при
            # загрузке), None = платформенные дефолты из settings.
            extract_fps = video.extract_fps or settings.extract_fps
            max_frames = settings.max_frames if video.max_frames is None else video.max_frames
            step = max(1, int(fps / extract_fps)) if fps > 0 else 1

            frames_to_process = total_frames // step
            # Сколько реально разберём: лимит max_frames считается по
            # прочитанным кадрам, в единицах выборки это max_frames // step.
            target_frames = frames_to_process
            if max_frames > 0:
                target_frames = min(target_frames, max(1, max_frames // step))
            logger.info(
                "Video: %d frames, %s fps, extracting every %dth "
                "= ~%d frames to process (target %d with limit)",
                total_frames, fps, step, frames_to_process, target_frames,
            )

            # Цель прогресса известна до первого кадра — дашборд сразу может
            # рисовать процент, а не «неизвестно из неизвестного».
            video.frames_to_process = target_frames
            video.processed_frames = 0
            db.commit()

            hit_limit = False
            processed = 0

            while True:
                ret, frame = cap.read()
                if not ret:
                    break

                if frame_index % step == 0:
                    timestamp = frame_index / fps if fps > 0 else 0.0

                    processed += 1
                    if processed % 50 == 1:
                        logger.info(
                            "Processing frame %d/%d (%d/%d)...",
                            frame_index, total_frames, processed, target_frames,
                        )
                        # Прогресс — факт в БД (см. Video.frames_to_process):
                        # дашборд опрашивает API, воркеру не нужен result backend.
                        video.processed_frames = processed
                        db.commit()

                    detections = detector.detect(
                        frame,
                        frame_index=frame_index,
                        timestamp=timestamp,
                    )
                    all_detections.extend(detections)

                frame_index += 1

                if max_frames > 0 and frame_index >= max_frames:
                    logger.info("Reached max_frames limit (%s), stopping", max_frames)
                    hit_limit = True
                    break

        # ============================================================
        # Шаг 6: Сохраняем детекции в БД
        # ============================================================
        for det in all_detections:
            db_detection = Detection(
                video_id=video.id,
                class_name=det.class_name,
                confidence=det.confidence,
                bbox_x1=det.bbox[0],
                bbox_y1=det.bbox[1],
                bbox_x2=det.bbox[2],
                bbox_y2=det.bbox[3],
                frame_index=det.frame_index,
                timestamp=det.timestamp,
            )
            db.add(db_detection)

        # ============================================================
        # Шаг 7: Обновляем метаданные видео
        # ============================================================
        video.duration = duration
        video.fps = int(fps) if fps > 0 else None
        video.processed_frames = processed

        # ============================================================
        # Шаг 8: Ставим статус "completed"
        # ============================================================
        video.status = "partial" if hit_limit else "completed"
        db.commit()

        logger.info("Processed video %s: %d detections found", video_id, len(all_detections))

    except Exception:
        video.status = "failed"
        db.commit()
        raise
