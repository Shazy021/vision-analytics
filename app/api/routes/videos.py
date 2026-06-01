import os
import shutil
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File,  BackgroundTasks
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
def upload_video(
    background_tasks: BackgroundTasks,  # ← FastAPI автоматически передаёт
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    """
    Загрузить видео-файл и запустить обработку в фоне.

    BackgroundTasks — встроенный механизм FastAPI.
    Функция добавляется в очередь и выполняется ПОСЛЕ отправки ответа.

    Что происходит:
    1. Сохраняем файл на диск
    2. Создаём запись в БД (status="processing")
    3. Отправляем ответ клиенту (СРАЗУ!)
    4. В фоне запускается process_video
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

    # ============================================================
    # ФОНОВАЯ ОБРАБОТКА — не блокирует ответ!
    # ============================================================
    # Проблема: background_tasks запускается в том же процессе,
    # но нам нужна своя БД-сессия (сессия запроса уже закроется).
    # Поэтому передаём video_id, а не db-сессию.
    background_tasks.add_task(run_processing, video.id)

    # Считаем detections_count (пока 0)
    video_read = VideoRead.model_validate(video)
    video_read.detections_count = 0
    return video_read

def run_processing(video_id: int) -> None:
    """
    Фоновая задача — создаём свою БД-сессию.

    Почему не можем использовать сессию из запроса?
    Потому что после отправки ответа FastAPI закрывает сессию
    (finally в get_db). Если бы мы использовали ту же сессию —
    получили бы "session is closed".

    Поэтому создаём новую сессию специально для фоновой задачи.
    """
    from app.core.database import SessionLocal
    db = SessionLocal()
    try:
        process_video(video_id, db)
    except Exception as e:
        print(f"Background processing failed for video {video_id}: {e}")
    finally:
        db.close()

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