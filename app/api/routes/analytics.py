"""
Аналитические эндпоинты — агрегация данных по детекциям.

Здесь мы используем GROUP BY, COUNT, AVG — агрегатные функции SQL.
Это то ради чего мы храним данные в БД, а не в файлах.
"""
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, desc

from app.core.database import get_db
from app.db.models import Video, Detection

router = APIRouter(prefix="/api/analytics", tags=["Analytics"])


@router.get("/stats")
def get_overall_stats(db: Session = Depends(get_db)):
    """
    Общая статистика по платформе.

    func.count() — SQL COUNT()
    func.avg() — SQL AVG()
    Без GROUP BY — считает по всей таблице
    """
    total_videos = db.query(func.count(Video.id)).scalar()
    total_detections = db.query(func.count(Detection.id)).scalar()
    avg_confidence = db.query(func.avg(Detection.confidence)).scalar()

    return {
        "total_videos": total_videos,
        "total_detections": total_detections,
        "avg_confidence": round(avg_confidence or 0, 3),
    }


@router.get("/class-distribution")
def get_class_distribution(
    video_id: int | None = Query(None, description="Фильтр по video_id"),
    db: Session = Depends(get_db),
):
    """
    Распределение детекций по классам (person, car, dog...).

    GROUP BY class_name — группируем по имени класса
    COUNT(*) — считаем сколько детекций каждого класса
    ORDER BY count DESC — сначала самые частые

    Пример ответа:
    [
        {"class_name": "person", "count": 150},
        {"class_name": "car", "count": 80},
        {"class_name": "dog", "count": 12}
    ]
    """
    query = db.query(
        Detection.class_name,
        func.count(Detection.id).label("count"),
    )

    # Если передан video_id — фильтруем только по этому видео
    if video_id is not None:
        query = query.filter(Detection.video_id == video_id)

    result = query.group_by(Detection.class_name).order_by(desc("count")).all()

    # Превращаем SQLAlchemy-результат в list[dict]
    return [
        {"class_name": row.class_name, "count": row.count}
        for row in result
    ]


@router.get("/timeline")
def get_timeline(
    video_id: int | None = Query(None, description="Фильтр по video_id"),
    bucket: float = Query(1.0, description="Размер бакета в секундах"),
    db: Session = Depends(get_db),
):
    """
    Временная линия детекций — сколько объектов в каждый момент времени.

    Это как гистограмма: ось X — время, ось Y — количество детекций.
    "bucket" — размер «корзины» (1 сек, 5 сек, 60 сек...).

    Как работает:
    1. timestamp = 1.4 сек, bucket = 5 сек → bucket_number = 0 (0-5 сек)
    2. timestamp = 6.2 сек, bucket = 5 сек → bucket_number = 1 (5-10 сек)
    3. timestamp = 12.0 сек, bucket = 5 сек → bucket_number = 2 (10-15 сек)

    GROUP BY bucket_number — считаем сколько детекций в каждом бакете
    """
    # floor(timestamp / bucket) — номер бакета
    # Например: timestamp=7.3, bucket=5 → 7.3/5 = 1.46 → floor = 1
    # Бакет 1 = с 5-й по 10-ю секунду
    bucket_expr = func.floor(Detection.timestamp / bucket).label("bucket_number")

    query = db.query(
        bucket_expr,
        func.count(Detection.id).label("count"),
    )

    if video_id is not None:
        query = query.filter(Detection.video_id == video_id)

    result = query.group_by(bucket_expr).order_by(bucket_expr).all()

    return [
        {
            # Восстанавливаем время начала бакета: bucket_number * bucket
            "time_start": round(row.bucket_number * bucket, 2),
            "count": row.count,
        }
        for row in result
    ]