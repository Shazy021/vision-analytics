import os
import shutil
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Query
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.core.config import settings
from app.core.database import get_db
from app.core import storage                       # ← MinIO клиент (модуль, не класс)
from app.db.models import Video, Detection
from app.schemas.video import VideoCreate, VideoRead
from app.tasks.processing import process_video_task

router = APIRouter(prefix="/api/videos", tags=["Videos"])

# Директория для загруженных видео (оставляем для совместимости)
UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)


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
    db: Session = Depends(get_db),
):
    """
    Загрузить видео-файл и запустить обработку в фоне.

    Flow:
    1. Создаём запись в БД (нужен ID для ключа в MinIO)
    2. Сохраняем временный файл → загружаем в MinIO через fput_object (multipart)
    3. Удаляем временный файл
    4. Отправляем задачу в Celery → ответ клиенту (СРАЗУ!)
    5. Воркер читает видео НАПРЯМУЮ из MinIO по HTTP (без скачивания)
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
    storage.upload_file(object_key, str(temp_path), content_type="video/mp4")

    # Удаляем временный файл — он в MinIO
    os.remove(temp_path)

    # Сохраняем S3 ключ в БД
    video.object_key = object_key
    db.commit()

    # Отправляем задачу Celery
    task = process_video_task.delay(video.id)
    video.task_id = task.id
    db.commit()

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

    # Presigned URL — дашборд может открыть видео напрямую из MinIO
    if video.object_key:
        try:
            video_read.video_url = storage.get_presigned_url(video.object_key)
        except Exception:
            pass  # MinIO недоступен — без URL

    return video_read


@router.get("/{video_id}/status")
def get_video_status(video_id: int, db: Session = Depends(get_db)):
    """Проверить статус обработки видео."""
    video = db.query(Video).filter(Video.id == video_id).first()
    if not video:
        raise HTTPException(status_code=404, detail="Video not found")

    result = {"video_id": video_id, "status": video.status}

    if video.task_id:
        task = process_video_task.AsyncResult(video.task_id)
        result["task_status"] = task.status
        if task.failed():
            result["error"] = str(task.result)

    return result


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
