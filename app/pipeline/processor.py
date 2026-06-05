"""
Пайплайн обработки видео — связывает все компоненты вместе.

Видео → кадры → YOLO-детекция → сохранение в БД

Это "оркестратор" — он не делает саму работу,
а координирует работу других модулей:
- VideoCapture / frame_generator — извлечение кадров
- YOLODetector — обнаружение объектов
- БД — сохранение результатов
"""
import os
from pathlib import Path

from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.models import Video, Detection
from app.detection.yolo import YOLODetector
from app.video.extractor import VideoCapture

def process_video(video_id: int, db: Session) -> None:
    """
    Обработать видео: кадры → детекции → БД.

    video_id — ID записи в таблице videos
    db — SQLAlchemy-сессия (передаём извне, чтобы управлять транзакцией)

    Шаги:
    1. Находим видео в БД по ID
    2. Ставим статус "processing"
    3. Открываем видео, извлекаем кадры
    4. Каждый кадр прогоняем через YOLO
    5. Сохраняем все детекции в БД
    6. Обновляем метаданные видео (duration, fps)
    7. Ставим статус "completed"
    """
    # ============================================================
    # Шаг 1: Находим видео в БД
    # ============================================================
    video = db.query(Video).filter(Video.id == video_id).first()
    if not video:
        raise ValueError(f"Video with id={video_id} not found")

    filepath = video.filepath

    if not filepath or not Path(filepath).exists():
        video.status = "failed"
        db.commit()
        raise FileNotFoundError(f"Video file not found: {filepath}")
    
    # ============================================================
    # Шаг 2: Ставим статус "processing"
    # ============================================================
    db.query(Detection).filter(Detection.video_id == video_id).delete()
    video.status = "processing"
    db.commit()

    try:
        # ============================================================
        # Шаг 3-4: Извлекаем кадры и детектируем объекты
        # ============================================================
        detector = YOLODetector()
        all_detections = []

        with VideoCapture(filepath) as cap:
            frame_index = 0
            fps = cap.get(5)
            total_frames = int(cap.get(7))
            duration = total_frames / fps if fps > 0 else 0

            extract_fps = settings.extract_fps
            step = max(1, int(fps / extract_fps)) if fps > 0 else 1

            # Сколько кадров будем обрабатывать (для оценки)
            frames_to_process = total_frames // step
            print(f"Video: {total_frames} frames, {fps} fps, "
                  f"extracting every {step}th = ~{frames_to_process} frames to process")

            while True:
                ret, frame = cap.read()
                if not ret:
                    break

                if frame_index % step == 0:
                    timestamp = frame_index / fps if fps > 0 else 0.0

                    # Логируем прогресс каждые 50 обработанных кадров
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

                # Ограничение для тестов (0 = без лимита)
                if settings.max_frames > 0 and frame_index >= settings.max_frames:
                    print(f"Reached max_frames limit ({settings.max_frames}), stopping")
                    break
    
        # ============================================================
        # Шаг 5: Сохраняем детекции в БД
        # ============================================================
        for det in all_detections:
            db_detection = Detection(
                video_id = video.id,
                class_name = det.class_name,
                confidence = det.confidence,
                bbox_x1=det.bbox[0],
                bbox_y1=det.bbox[1],
                bbox_x2=det.bbox[2],
                bbox_y2=det.bbox[3],
                frame_index=det.frame_index,
                timestamp=det.timestamp,
            )
            db.add(db_detection)
        
        # ============================================================
        # Шаг 6: Обновляем метаданные видео
        # ============================================================
        video.duration = duration
        video.fps = int(fps) if fps > 0 else None

        # ============================================================
        # Шаг 7: Ставим статус "completed"
        # ============================================================
        video.status = "completed"
        db.commit()

        print(f"Processed video {video_id}: {len(all_detections)} detections found")

    except Exception as e:
        # Если что-то пошло не так — помечаем как failed
        video.status = "failed"
        db.commit()
        raise