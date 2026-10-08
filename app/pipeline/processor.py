"""
Пайплайн обработки видео — связывает все компоненты вместе.

Видео → кадры → YOLO-детекция → сохранение в БД

Phase 4: Видео читается НАПРЯМУЮ из MinIO по HTTP.
OpenCV (cv2.VideoCapture) поддерживает HTTP URL —
никакого скачивания на диск.
"""
from pathlib import Path

from sqlalchemy.orm import Session

from app.core import storage
from app.core.config import settings
from app.db.models import Detection, Video
from app.detection.yolo import YOLODetector
from app.video.extractor import VideoCapture


def _get_video_source(video: Video) -> str:
    """
    Получить источник видео для OpenCV.

    Если есть object_key — генерируем presigned URL,
    OpenCV читает по HTTP напрямую из MinIO.
    Если нет — используем старый filepath с диска.
    """
    if video.object_key:
        url = storage.get_presigned_url(video.object_key, expires_hours=24)
        print(f"[Processor] Reading {video.object_key} directly from MinIO via HTTP")
        return url

    if video.filepath and Path(video.filepath).exists():
        print(f"[Processor] Reading from local disk: {video.filepath}")
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

            extract_fps = settings.extract_fps
            step = max(1, int(fps / extract_fps)) if fps > 0 else 1

            frames_to_process = total_frames // step
            print(f"Video: {total_frames} frames, {fps} fps, "
                  f"extracting every {step}th = ~{frames_to_process} frames to process")

            hit_limit = False

            while True:
                ret, frame = cap.read()
                if not ret:
                    break

                if frame_index % step == 0:
                    timestamp = frame_index / fps if fps > 0 else 0.0

                    processed = frame_index // step
                    if processed % 50 == 0:
                        print(f"Processing frame {frame_index}/{total_frames} "
                              f"({processed}/{frames_to_process})...")

                    detections = detector.detect(
                        frame,
                        frame_index=frame_index,
                        timestamp=timestamp,
                    )
                    all_detections.extend(detections)

                frame_index += 1

                if settings.max_frames > 0 and frame_index >= settings.max_frames:
                    print(f"Reached max_frames limit ({settings.max_frames}), stopping")
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

        # ============================================================
        # Шаг 8: Ставим статус "completed"
        # ============================================================
        video.status = "partial" if hit_limit else "completed"
        db.commit()

        print(f"Processed video {video_id}: {len(all_detections)} detections found")

    except Exception:
        video.status = "failed"
        db.commit()
        raise
