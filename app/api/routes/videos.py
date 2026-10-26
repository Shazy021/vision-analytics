import json
import logging
import os
import shutil
from pathlib import Path

import cv2
from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, Response, UploadFile
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.core import storage  # ← MinIO клиент (модуль, не класс)
from app.core.config import settings
from app.core.database import get_db
from app.core.topics import TOPIC_VIDEO_REQUESTS
from app.db.models import Detection, Video
from app.schemas.video import VideoCreate, VideoRead

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/videos", tags=["Videos"])

# Директория для загруженных видео (оставляем для совместимости)
UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)

# Kafka-продюсер задач на обработку видео. Ленивый синглтон: создание
# Producer — это TCP-соединение с брокером, заводить его на каждый upload
# расточительно, а на уровне модуля — нельзя (тесты импортируют маршруты
# без Kafka, да и api может стартовать раньше брокера).
_kafka_producer = None


def _get_kafka_producer():
    global _kafka_producer
    if _kafka_producer is None:
        from confluent_kafka import Producer

        from app.core.topics import KAFKA_BOOTSTRAP_SERVERS

        _kafka_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    return _kafka_producer


def _enqueue_video_processing(video_id: int) -> None:
    """Поставить задачу на обработку в топик video_requests.

    Вызывается ПОСЛЕ commit с object_key: консьюмер по пришедшему video_id
    читает видео из БД — запись обязана быть видна, иначе гонка
    «задача пришла, а ссылки на файл нет».

    flush() обязателен: produce() лишь кладет сообщение в локальный буфер,
    без него сообщение может остаться неотправленным к моменту ответа
    клиенту.
    """
    producer = _get_kafka_producer()
    producer.produce(
        TOPIC_VIDEO_REQUESTS,
        value=json.dumps({"video_id": video_id}).encode(),
    )
    producer.flush(timeout=5)


@router.post("", response_model=VideoRead, status_code=201)
def create_video(video_in: VideoCreate, db: Session = Depends(get_db)):
    """Создать запись о видео (без файла)"""
    video = Video(**video_in.model_dump())
    db.add(video)
    db.commit()
    db.refresh(video)
    return video


@router.post("/upload", response_model=VideoRead, status_code=201)
def upload_video(
    file: UploadFile = File(...),
    extract_fps: int | None = Form(None, ge=1, le=30),
    max_frames: int | None = Form(None, ge=0),
    db: Session = Depends(get_db),
):
    """
    Загрузить видео-файл и запустить обработку в фоне.

    Опциональные параметры разбора (иначе — платформенные дефолты):
      extract_fps — как часто брать кадры (раз/с);
      max_frames  — потолок прочитанных кадров (0 = без лимита).

    Flow:
    1. Создаём запись в БД (нужен ID для ключа в MinIO)
    2. Сохраняем временный файл → загружаем в MinIO через fput_object (multipart)
    3. Удаляем временный файл
    4. Публикуем задачу в Kafka (топик video_requests) → ответ клиенту (СРАЗУ!)
    5. video_consumer читает видео НАПРЯМУЮ из MinIO по HTTP (без скачивания)
    """
    # Проверяем расширение
    allowed = {".mp4", ".avi", ".mov", ".mkv"}
    ext = Path(file.filename).suffix.lower()
    if ext not in allowed:
        raise HTTPException(
            status_code=400,
            detail=f"File type {ext} not allowed. Allowed: {allowed}"
        )

    # Создаём запись в БД СНАЧАЛА (нужен ID для ключа)
    video = Video(
        filename=file.filename,
        status="processing",
        extract_fps=extract_fps,
        max_frames=max_frames,
    )
    db.add(video)
    db.commit()
    db.refresh(video)

    # Сохраняем на диск временно → fput_object (multipart upload для больших файлов)
    # Для 7GB файла upload_bytes() убьёт RAM, а fput_object() льёт частями
    temp_path = UPLOAD_DIR / f"{video.id}{ext}"
    with open(temp_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    object_key = f"videos/{video.id}{ext}"
    try:
        storage.upload_file(object_key, str(temp_path), content_type="video/mp4")

        # Удаляем временный файл — он в MinIO
        os.remove(temp_path)

        # Сохраняем S3 ключ в БД
        video.object_key = object_key
        db.commit()

        # Задача на обработку — в Kafka (см. _enqueue_video_processing)
        _enqueue_video_processing(video.id)
    except Exception:
        # MinIO/Kafka упали ПОСЛЕ создания записи в БД — помечаем failed,
        # иначе видео висит в "processing" навечно, а файла в S3 нет.
        logger.exception("Upload of video %s failed after DB record was created", video.id)
        video.status = "failed"
        db.commit()
        temp_path.unlink(missing_ok=True)
        raise HTTPException(
            status_code=500,
            detail="Failed to store the video or enqueue processing",
        )

    video_read = VideoRead.model_validate(video)
    video_read.detections_count = 0
    return video_read


@router.get("", response_model=list[VideoRead])
def list_videos(skip: int = 0, limit: int = 50, db: Session = Depends(get_db)):
    """Получить список видео с пагинацией"""
    videos = (
        db.query(Video)
        .offset(skip)
        .limit(limit)
        .all()
    )

    result = []
    for video in videos:
        count = db.query(func.count(Detection.id)).filter(
            Detection.video_id == video.id
        ).scalar()
        video_dict = VideoRead.model_validate(video)
        video_dict.detections_count = count
        result.append(video_dict)
    return result


@router.get("/params")
def processing_params():
    """Параметры разбора видео: как часто берём кадры и потолок.

    Дашборд показывает это рядом с прогрессом — «на скольких кадрах ищем»
    нельзя вывести из данных клиента, это конфигурация пайплайна.

    Объявлено ДО /{video_id}: FastAPI матчит маршруты по порядку, и без
    этого «params» ушёл бы в валидацию int и получил 422.
    """
    return {
        "extract_fps": settings.extract_fps,
        "max_frames": settings.max_frames,
    }


@router.get("/{video_id}", response_model=VideoRead)
def get_video(video_id: int, db: Session = Depends(get_db)):
    """Получить одно видео по ID + presigned URL для просмотра"""
    video = db.query(Video).filter(Video.id == video_id).first()
    if not video:
        raise HTTPException(status_code=404, detail="Video not found")

    count = db.query(func.count(Detection.id)).filter(
        Detection.video_id == video_id
    ).scalar()

    video_read = VideoRead.model_validate(video)
    video_read.detections_count = count

    # Presigned URL — дашборд может открыть видео напрямую из MinIO.
    # public=True: ссылку открывает БРАУЗЕР в <video>, значит хост должен
    # быть доступен с машины пользователя (S3_PUBLIC_ENDPOINT), а не
    # внутреннее имя minio:9000 из docker-сети.
    if video.object_key:
        try:
            video_read.video_url = storage.get_presigned_url(video.object_key, public=True)
        except Exception:
            pass  # MinIO недоступен — без URL

    return video_read


@router.get("/{video_id}/status")
def get_video_status(video_id: int, db: Session = Depends(get_db)):
    """Проверить статус обработки видео.

    Единственный источник правды — БД: video_consumer пишет туда status и
    прогресс (processed_frames/frames_to_process) по ходу разбора кадров.
    """
    video = db.query(Video).filter(Video.id == video_id).first()
    if not video:
        raise HTTPException(status_code=404, detail="Video not found")

    return {
        "video_id": video_id,
        "status": video.status,
        "processed_frames": video.processed_frames,
        "frames_to_process": video.frames_to_process,
    }


@router.get("/{video_id}/frame")
def get_video_frame(video_id: int, timestamp: float, db: Session = Depends(get_db)):
    """
    Получить кадр из видео по таймстампу.

    У нас есть целое видео в MinIO + timestamp детекции в БД.
    Достаём кадр из видео по времени, боксы подставим из БД на фронте.
    """
    video = db.query(Video).filter(Video.id == video_id).first()
    if not video:
        raise HTTPException(status_code=404, detail="Video not found")

    if not video.object_key:
        raise HTTPException(status_code=404, detail="Video file not in S3")

    # Возвращаем presigned URL на само видео + timestamp
    # Фронт сам откроет видео на нужной секунде
    try:
        video_url = storage.get_presigned_url(video.object_key)
    except Exception:
        raise HTTPException(status_code=500, detail="Failed to generate video URL")

    return {
        "video_id": video_id,
        "timestamp": timestamp,
        "video_url": video_url,
    }


@router.get("/{video_id}/frame-image")
def get_video_frame_image(
    video_id: int,
    timestamp: float = Query(..., description="Секунда от начала видео"),
    db: Session = Depends(get_db),
):
    """
    Извлечь ОДИН кадр из видео по таймстампу и отдать как JPEG.

    Используется для превью в дашборде: ползунок таймлайна двигается —
    фронт запрашивает кадр как картинку, поверх рисуются боксы детекций
    (matplotlib/PIL). Streamlit нативно показывает st.image(JPEG bytes),
    а видео-плеер с оверлеем боксов в нём сделать тяжело.

    Под капотом:
      1. Берём presigned URL на видео в MinIO.
      2. cv2.VideoCapture(url) — OpenCV через FFmpeg читает по HTTP.
      3. cap.set(CAP_PROP_POS_MSEC, timestamp*1000) — seek на нужную секунду.
      4. Кодируем кадр в JPEG и отдаём как Response.

    Альтернатива через /frame (возвращает video_url+timestamp) — работает,
    но требует HTML5 <video>-плеера с программным seek, что в Streamlit
    нормально не делается. JPEG-кадр — универсальнее.
    """
    video = db.query(Video).filter(Video.id == video_id).first()
    if not video:
        raise HTTPException(status_code=404, detail="Video not found")

    if not video.object_key:
        raise HTTPException(status_code=404, detail="Video file not in S3")

    try:
        video_url = storage.get_presigned_url(video.object_key, expires_hours=1)
    except Exception:
        raise HTTPException(status_code=500, detail="Failed to generate video URL")

    # cv2.VideoCapture умеет открывать http(s):// URL через FFmpeg-backend.
    # seek по времени (CAP_PROP_POS_MSEC) точнее, чем по frame_index —
    # не нужно знать FPS источника, OpenCV сам переведёт.
    cap = cv2.VideoCapture(video_url)
    if not cap.isOpened():
        raise HTTPException(
            status_code=502,
            detail="Failed to open video stream from MinIO (FFmpeg/CV2 issue)",
        )

    try:
        cap.set(cv2.CAP_PROP_POS_MSEC, timestamp * 1000.0)
        ret, frame = cap.read()
        if not ret or frame is None:
            raise HTTPException(
                status_code=422,
                detail=f"Failed to read frame at t={timestamp}s (out of range?)",
            )

        # Кодируем в JPEG. Качество 90 — достаточно для превью,
        # не раздуваем трафик между api-контейнером и браузером.
        ok, buf = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, 90])
        if not ok:
            raise HTTPException(status_code=500, detail="Failed to encode frame as JPEG")

        return Response(content=buf.tobytes(), media_type="image/jpeg")
    finally:
        cap.release()


@router.get("/{video_id}/detections")
def video_detections(
    video_id: int,
    limit: int = 100,
    start_ts: float | None = Query(
        None,
        description="Нижняя граница timestamp (секунды от начала видео).",
    ),
    end_ts: float | None = Query(
        None,
        description="Верхняя граница timestamp (секунды от начала видео).",
    ),
    closest_ts: float | None = Query(
        None,
        description="Если задано — вернуть ТОЛЬКО детекции ближайшего обработанного "
                    "кадра с timestamp <= closest_ts. Это решает «кашу из боксов»: "
                    "при диапазоне ±0.5с в окно попадает ~5 обработанных кадров "
                    "(extract_fps=5 → шаг 0.2с) и их боксы накладываются друг на друга. "
                    "Параметр ищет MAX(timestamp) WHERE timestamp <= closest_ts "
                    "и фильтрует детекции строго по этому timestamp.",
    ),
    db: Session = Depends(get_db),
):
    """
    Детекции для видео с тремя режимами фильтрации по времени:

    1. Без параметров — последние N по времени.
    2. start_ts / end_ts — диапазон (для трекинга движения по окну).
    3. closest_ts — ОДИН ближайший обработанный кадр (для оверлея на превью).
       Возвращает боксы ровно одного timestamp — без «каши».
    """
    video = db.query(Video).filter(Video.id == video_id).first()
    if not video:
        raise HTTPException(status_code=404, detail="Video not found")

    # Режим closest_ts — приоритетный, остальные параметры игнорируем
    if closest_ts is not None:
        # Шаг 1: найти MAX(timestamp) WHERE timestamp <= closest_ts
        closest_row = (
            db.query(Detection.timestamp)
            .filter(
                Detection.video_id == video_id,
                Detection.timestamp <= closest_ts,
            )
            .order_by(Detection.timestamp.desc())
            .limit(1)
            .first()
        )
        if not closest_row:
            # Нет детекций раньше closest_ts — попробуем ближайший ПОСЛЕ
            closest_row = (
                db.query(Detection.timestamp)
                .filter(Detection.video_id == video_id)
                .order_by(Detection.timestamp.asc())
                .limit(1)
                .first()
            )
            if not closest_row:
                return []

        target_ts = closest_row[0]
        detections = (
            db.query(Detection)
            .filter(
                Detection.video_id == video_id,
                Detection.timestamp == target_ts,
            )
            .all()
        )
        return [
            {
                "id": d.id,
                "frame_index": d.frame_index,
                "timestamp": d.timestamp,
                "class_name": d.class_name,
                "confidence": d.confidence,
                "video_id": d.video_id,
                "bbox": [d.bbox_x1, d.bbox_y1, d.bbox_x2, d.bbox_y2],
            }
            for d in detections
        ]

    # Режим диапазона
    query = db.query(Detection).filter(Detection.video_id == video_id)

    if start_ts is not None:
        query = query.filter(Detection.timestamp >= start_ts)
    if end_ts is not None:
        query = query.filter(Detection.timestamp <= end_ts)

    # Если фильтр по времени есть — сортируем по возрастанию timestamp
    # (боксы идут в естественном порядке, не «последние сверху»).
    # Иначе — последние N по времени.
    if start_ts is not None or end_ts is not None:
        detections = query.order_by(Detection.timestamp.asc()).limit(max(limit, 500)).all()
    else:
        detections = (
            query
            .order_by(Detection.timestamp.desc())
            .limit(limit)
            .all()
        )

    return [
        {
            "id": d.id,
            "frame_index": d.frame_index,
            "timestamp": d.timestamp,
            "class_name": d.class_name,
            "confidence": d.confidence,
            "video_id": d.video_id,
            "bbox": [d.bbox_x1, d.bbox_y1, d.bbox_x2, d.bbox_y2],
        }
        for d in detections
    ]


@router.delete("/{video_id}")
def delete_video(video_id: int, db: Session = Depends(get_db)):
    """Удалить видео + файлы из S3 + детекции из БД"""
    video = db.query(Video).filter(Video.id == video_id).first()
    if not video:
        raise HTTPException(status_code=404, detail="Video not found")

    # Удаляем видео из S3
    if video.object_key:
        storage.delete_file(video.object_key)

    # Удаляем детекции + видео из БД
    db.query(Detection).filter(Detection.video_id == video_id).delete()
    db.delete(video)
    db.commit()

    return {"message": f"Video {video_id} deleted"}
