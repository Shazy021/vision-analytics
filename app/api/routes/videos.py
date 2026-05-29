"""
CRUD-эндпоинты для видео.

CRUD = Create, Read, Update, Delete — 4 базовые операции с данными.
Мы делаем Create и Read. Update и Delete пока не нужны — добавим потом.

Роутер (APIRouter) — это мини-приложение с набором эндпоинтов.
Потом мы "примонтируем" его к главному приложению.
"""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.core.database import get_db
from app.db.models import Video, Detection
from app.schemas.video import VideoCreate, VideoRead

# prefix — все URL начинаются с /api/videos
# tags — группировка в Swagger (все эти эндпоинты под заголовком "Videos")
router = APIRouter(prefix="/api/videos", tags=["Videos"])

@router.post("", response_model=VideoRead, status_code=201)
def create_video(video_in: VideoCreate, db: Session = Depends(get_db)):
    """
    Создать запись о видео.

    video_in: VideoCreate — Pydantic валидирует тело запроса.
    Если клиент пришлёт {"filename": 123} — Pydantic вернёт 422
    (filename должен быть str, а не int).

    response_model=VideoRead — FastAPI валидирует ответ по этой схеме.
    Также используется для Swagger-документации.

    status_code=201 — стандартный HTTP-код для "создано"
    (200 = OK, 201 = Created)
    """
    video = Video(**video_in.model_dump())
    # video_in.model_dump() — превращает Pydantic-схему в dict
    # Video(**{...}) — создаём SQLAlchemy-объект из dict
    db.add(video)
    db.commit()
    db.refresh(video)  # обновляем объект — получаем id, uploaded_at, status
    return video

@router.get("", response_model=list[VideoRead])
def list_videos(skip: int = 0, limit: int = 50, db: Session = Depends(get_db)):
    """
    Получить список видео с пагинацией.

    Пагинация — не возвращаем все 10000 видео разом,
    а по частям: skip=0&limit=50 — первые 50,
    skip=50&limit=50 — следующие 50.

    ?skip=0&limit=50 — query-параметры (в URL после ?)
    FastAPI автоматически парсит их и валидирует типы.
    """
    videos = (
        db.query(Video)
        .offset(skip)      # пропустить skip записей
        .limit(limit)      # взять limit записей
        .all()
    )

    # Считаем detections_count для каждого видео
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
    """
    Получить одно видео по ID.

    {video_id} — path-параметр (часть URL).
    FastAPI автоматически валидирует что это int.

    HTTPException(404) — если видео не найдено,
    возвращаем стандартный 404 Not Found.
    """
    video = db.query(Video).filter(Video.id == video_id).first()
    if not video:
        raise HTTPException(status_code=404, detail="Video not found")

    count = db.query(func.count(Detection.id)).filter(
        Detection.video_id == video_id
    ).scalar()

    video_read = VideoRead.model_validate(video)
    video_read.detections_count = count
    return video_read