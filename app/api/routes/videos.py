import os
import shutil
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.core.config import settings
from app.core.database import get_db
from app.db.models import Video, Detection
from app.schemas.video import VideoCreate, VideoRead
from app.tasks.processing import process_video_task
from app.tasks.processing import process_video_task

router = APIRouter(prefix="/api/videos", tags=["Videos"])

# Директория для загруженных видео
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

    Celery task — отдельный воркер обрабатывает видео параллельно.
    API моментально отвечает, не дожидаясь YOLO.

    Что происходит:
    1. Сохраняем файл на диск
    2. Создаём запись в БД (status="processing")
    3. Отправляем задачу в Celery → ответ клиенту (СРАЗУ!)
    4. Воркер забирает задачу и запускает YOLO
    """
    # Проверяем расширение
    allowed = {".mp4", ".avi", ".mov", ".mkv"}
    ext = Path(file.filename).suffix.lower()
    if ext not in allowed:
        raise HTTPException(
            status_code=400,
            detail=f"File type {ext} not allowed. Allowed: {allowed}"
        )

    # Сохраняем файл
    filepath = UPLOAD_DIR / file.filename
    with open(filepath, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    # Создаём запись в БД
    video = Video(
        filename=file.filename,
        filepath=str(filepath),
        status="processing",
    )
    db.add(video)
    db.commit()
    db.refresh(video)

    task = process_video_task.delay(video.id)
    video.task_id = task.id
    db.commit()

    # Считаем detections_count (пока 0)
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
    """Получить одно видео по ID"""
    video = db.query(Video).filter(Video.id == video_id).first()
    if not video:
        raise HTTPException(status_code=404, detail="Video not found")

    count = db.query(func.count(Detection.id)).filter(
        Detection.video_id == video_id
    ).scalar()

    video_read = VideoRead.model_validate(video)
    video_read.detections_count = count
    return video_read

@router.get("/{video_id}/status")
def get_video_status(video_id: int, db: Session = Depends(get_db)):
    """
    Проверить статус обработки видео.
    
    Возвращает:
    - video status из БД (processing / completed / failed)
    - task status из Celery (PENDING / STARTED / SUCCESS / FAILURE)
    """
    video = db.query(Video).filter(Video.id == video_id).first()
    if not video:
        raise HTTPException(status_code=404, detail="Video not found")
    
    result = {"video_id": video_id, "status": video.status}

    # Если есть task_id - проверяем Celery
    if video.task_id:
        task = process_video_task.AsyncResult(video.task_id)
        result["task_status"] = task.status
        if task.failed():
            result["error"] = str(task.result)

    return result