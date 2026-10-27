"""
Пайплайн обработки видео — связывает все компоненты вместе.

Видео → кадры → YOLO-детекция → сохранение в БД

Phase 4: Видео читается НАПРЯМУЮ из MinIO по HTTP.
OpenCV (cv2.VideoCapture) поддерживает HTTP URL —
никакого скачивания на диск.
"""
import logging
import math
from pathlib import Path

import cv2
from sqlalchemy import func, update
from sqlalchemy.orm import Session

from app.core import storage
from app.core.config import settings
from app.db.models import Detection, Video
from app.processing.detector import YOLODetector
from app.processing.extractor import VideoCapture
from app.processing.planner import allocate_sample_budgets, chunk_slot, plan_chunks

logger = logging.getLogger(__name__)

# Запас при seek в начало чанка: cv2 садится на keyframe ДО цели и
# докачивает, кадры до границы выбрасываются как preroll. 2с покрывает
# GOP наших исходников (~1с); на камерах с большим GOP увеличить.
SEEK_MARGIN_MS = 2000.0
_EPS_S = 1e-6
# Сколько попыток декода чанка при преждевременном обрыве потока
# (сеть/повреждение). Постоянная поломка (битый файл) не лечится
# ретраями — после последней попытки задача умирает как терминальная.
CHUNK_ATTEMPTS = 3
# Как часто чанк докладывать в processed_frames: дашборд показывает
# живой прогресс, а не «0» до конца чанка (чанк — минуты работы).
PROGRESS_REPORT_EVERY = 50


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


def _decode_chunk(source: str, start_s: float, end_s: float, interval: float,
                  budget: int, detector: YOLODetector,
                  on_sample=None) -> tuple[list, int, bool]:
    """Декод одного чанка [start_s, end_s) с time-grid сэмплированием.

    on_sample(sampled_so_far) вызывается после каждого сэмпла — воркер
    показывает дашборду прогресс внутри чанка, а не только по его итогам.

    Возвращает (детекции, число сэмплов, достигнут_ли_бюджет).
    Бросает RuntimeError, если поток оборвался ДО конца диапазона —
    это повод для ретрая, а не тихое «обрали половину».
    """
    detections: list = []
    sampled = 0
    with VideoCapture(source) as cap:
        fps = cap.get(5)
        if start_s > 0:
            cap.set(cv2.CAP_PROP_POS_MSEC, max(0.0, start_s * 1000.0 - SEEK_MARGIN_MS))

        # Владение: чанк отвечает за отметки next_mark..last_mark сетки
        # k/extract_fps, лежащие в [start_s, end_s). Сэмпл отметки —
        # первый декодированный кадр с t >= отметки (иммунитет к VFR).
        next_mark = max(0, math.ceil(start_s / interval - _EPS_S))
        last_mark = int(math.floor((end_s - _EPS_S) / interval))

        last_t = None
        while True:
            ret, frame = cap.read()
            if not ret:
                break
            t_s = cap.get(cv2.CAP_PROP_POS_MSEC) / 1000.0
            last_t = t_s
            if t_s + _EPS_S < start_s:
                continue  # preroll: seek сел на keyframe до границы
            if t_s + _EPS_S >= end_s:
                break
            while next_mark <= last_mark and next_mark * interval <= t_s + _EPS_S:
                frame_index = int(round(t_s * fps)) if fps > 0 else 0
                detections.extend(
                    detector.detect(frame, frame_index=frame_index, timestamp=t_s)
                )
                sampled += 1
                next_mark += 1
                if on_sample is not None:
                    on_sample(sampled)
                if budget > 0 and sampled >= budget:
                    break
            if budget > 0 and sampled >= budget:
                break

    hit_budget = budget > 0 and sampled >= budget
    # Преждевременный обрыв: дошли не до конца диапазона и дело не в бюджете.
    # Допуск 1с: последний чанк заканчивается ровно на EOF файла.
    if not hit_budget and (last_t is None or last_t + 1.0 < end_s):
        raise RuntimeError(
            f"Chunk [{start_s:.1f},{end_s:.1f}) ended early at {last_t}s — "
            "stream aborted before chunk end"
        )
    return detections, sampled, hit_budget


def process_video_chunk(video_id: int, start_s: float, end_s: float, db: Session) -> None:
    """Обработать временной срез [start_s, end_s) видео.

    Параллельные чанки одного видео живут в разных репликах consumer'а,
    поэтому все изменения состояния идут атомарными SQL-операциями:

    * идемпотентность — DELETE детекций в СОБСТВЕННОМ диапазоне перед
      вставкой: redelivery чанка не плодит дубли;
    * прогресс — processed_frames += N (атомарный инкремент);
    * финализация — chunks_done += 1, кто довёл до chunks_total, тот
      ставит терминальный статус (условно, чтобы не затереть 'failed').
    """
    video = db.query(Video).filter(Video.id == video_id).first()
    if not video:
        raise ValueError(f"Video with id={video_id} not found")

    video_source = _get_video_source(video)
    extract_fps = video.extract_fps or settings.extract_fps
    interval = 1.0 / extract_fps

    # Глобальный бюджет max_frames распределяем по чанкам сами: план
    # детерминирован (duration + chunk_sec), свой слот находится по start_s.
    budget = 0
    if video.duration and (video.max_frames if video.max_frames is not None
                           else settings.max_frames) > 0:
        fps_meta = video.fps or 0
        step = max(1, int(fps_meta / extract_fps)) if fps_meta > 0 else 1
        total_budget = max(1, ((video.max_frames if video.max_frames is not None
                                else settings.max_frames)) // step)
        chunks = plan_chunks(video.duration, settings.video_chunk_sec)
        slot = chunk_slot(chunks, start_s)
        if slot >= 0:
            budget = allocate_sample_budgets(len(chunks), total_budget)[slot]

    detector = YOLODetector()
    last_err: Exception | None = None
    # Прогресс внутри чанка: каждые PROGRESS_REPORT_EVERY сэмплов —
    # атомарный инкремент. При ретрае частично зарепортанное откатывается,
    # повтор пересчитает эти сэмплы заново (итог остаётся точным).
    reported = 0

    def _report(sampled_so_far: int) -> None:
        nonlocal reported
        if sampled_so_far - reported >= PROGRESS_REPORT_EVERY:
            delta = sampled_so_far - reported
            db.execute(
                update(Video)
                .where(Video.id == video_id)
                .values(processed_frames=func.coalesce(Video.processed_frames, 0) + delta)
            )
            db.commit()
            reported = sampled_so_far

    for attempt in range(1, CHUNK_ATTEMPTS + 1):
        reported = 0
        try:
            detections, sampled, _hit = _decode_chunk(
                video_source, start_s, end_s, interval, budget, detector,
                on_sample=_report,
            )
            last_err = None
            break
        except Exception as exc:
            if reported:
                db.execute(
                    update(Video)
                    .where(Video.id == video_id)
                    .values(processed_frames=func.coalesce(Video.processed_frames, 0) - reported)
                )
                db.commit()
            last_err = exc
            logger.warning(
                "Video %s chunk [%.1f,%.1f) attempt %d/%d failed: %s",
                video_id, start_s, end_s, attempt, CHUNK_ATTEMPTS, exc,
            )
    if last_err is not None:
        raise last_err

    # Идемпотентность: стираем собственный диапазон (redelivery-безопасно)
    db.query(Detection).filter(
        Detection.video_id == video_id,
        Detection.timestamp >= start_s,
        Detection.timestamp < end_s,
    ).delete(synchronize_session=False)

    for det in detections:
        db.add(Detection(
            video_id=video.id,
            class_name=det.class_name,
            confidence=det.confidence,
            bbox_x1=det.bbox[0],
            bbox_y1=det.bbox[1],
            bbox_x2=det.bbox[2],
            bbox_y2=det.bbox[3],
            frame_index=det.frame_index,
            timestamp=det.timestamp,
        ))

    # Прогресс и финализация — атомарные SQL-инкременты (чанки идут
    # параллельно в разных репликах, ORM-объектами считать нельзя).
    # coalesce: колонки NULL у свежего видео, NULL + N в SQL снова NULL.
    # Прибавляем остаток: часть сэмплов уже зарепортана внутри чанка.
    db.execute(
        update(Video)
        .where(Video.id == video_id)
        .values(
            processed_frames=func.coalesce(Video.processed_frames, 0) + (sampled - reported),
            chunks_done=func.coalesce(Video.chunks_done, 0) + 1,
        )
    )
    db.commit()

    video = db.get(Video, video_id)
    if (video.chunks_total and video.chunks_done is not None
            and video.chunks_done >= video.chunks_total and video.status == "processing"):
        max_frames = video.max_frames if video.max_frames is not None else settings.max_frames
        video.status = "partial" if max_frames > 0 else "completed"
        db.commit()
        logger.info(
            "Video %s finalized: %d/%d chunks, status=%s",
            video_id, video.chunks_done, video.chunks_total, video.status,
        )
    logger.info(
        "Video %s chunk [%.1f,%.1f) done: %d samples, %d detections",
        video_id, start_s, end_s, sampled, len(detections),
    )
