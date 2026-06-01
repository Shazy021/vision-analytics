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
from app.pipeline.processor import process_video

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
def upload_video(file: UploadFile = File(...), db: Session = Depends(get_db)):
    """
    Загрузить видео-файл и создать запись в БД.

    UploadFile — специальный тип FastAPI для загрузки файлов.
    File(...) — означает что параметр обязателен.

    Что происходит:
    1. Сохраняем файл на диск (uploads/filename)
    2. Создаём запись в БД
    3. Запускаем обработку (YOLO)
    4. Возвращаем результат
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

    # Запускаем обработку
    try:
        process_video(video.id, db)
    except Exception as e:
        print(f"Processing failed: {e}")

    # Обновляем после обработки
    db.refresh(video)

    count = db.query(func.count(Detection.id)).filter(
        Detection.video_id == video.id
    ).scalar()

    video_read = VideoRead.model_validate(video)
    video_read.detections_count = count
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