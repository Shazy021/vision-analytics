"""
Аналитические эндпоинты — агрегация данных по детекциям.
"""
from fastapi import APIRouter, Depends, Query
from sqlalchemy import desc, func
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.db.models import Detection, Video

router = APIRouter(prefix="/api/analytics", tags=["Analytics"])


@router.get("/stats")
def get_overall_stats(
    source: str | None = Query(None, description="Фильтр: 'video' или 'stream'"),
    db: Session = Depends(get_db),
):
    query = db.query(func.count(Detection.id))
    conf_query = db.query(func.avg(Detection.confidence))

    if source == "video":
        query = query.filter(Detection.video_id.isnot(None))
        conf_query = conf_query.filter(Detection.video_id.isnot(None))
    elif source == "stream":
        query = query.filter(Detection.stream_id.isnot(None))
        conf_query = conf_query.filter(Detection.stream_id.isnot(None))

    total_detections = query.scalar()
    avg_confidence = conf_query.scalar()

    total_videos = db.query(func.count(Video.id)).scalar()

    return {
        "total_videos": total_videos,
        "total_detections": total_detections,
        "avg_confidence": round(avg_confidence or 0, 3),
    }


@router.get("/class-distribution")
def get_class_distribution(
    video_id: int | None = Query(None, description="Фильтр по video_id"),
    stream_id: int | None = Query(None, description="Фильтр по stream_id"),
    source: str | None = Query(None, description="Фильтр: 'video' или 'stream'"),
    db: Session = Depends(get_db),
):
    query = db.query(
        Detection.class_name,
        func.count(Detection.id).label("count"),
    )

    if video_id is not None:
        query = query.filter(Detection.video_id == video_id)

    if stream_id is not None:
        query = query.filter(Detection.stream_id == stream_id)

    if source == "video":
        query = query.filter(Detection.video_id.isnot(None))
    elif source == "stream":
        query = query.filter(Detection.stream_id.isnot(None))

    result = query.group_by(Detection.class_name).order_by(desc("count")).all()

    return [
        {"class_name": row.class_name, "count": row.count}
        for row in result
    ]

@router.get("/stream-activity")
def get_stream_activity(
    stream_id: int | None = Query(None, description="Фильтр по stream_id"),
    minutes: int = Query(10, description="Сколько последних минут"),
    db: Session = Depends(get_db),
):
    """Детекции по реальному времени (detected_at) — для стримов."""
    from datetime import datetime, timedelta

    since = datetime.utcnow() - timedelta(minutes=minutes)

    query = db.query(
        func.date_trunc("minute", Detection.detected_at).label("minute_bucket"),
        func.count(Detection.id).label("count"),
    ).filter(
        Detection.stream_id.isnot(None),
        Detection.detected_at >= since,
    )

    if stream_id is not None:
        query = query.filter(Detection.stream_id == stream_id)

    result = query.group_by("minute_bucket").order_by("minute_bucket").all()

    return [
        {
            "time": row.minute_bucket.isoformat() if row.minute_bucket else "",
            "count": row.count,
        }
        for row in result
        if row.minute_bucket is not None
    ]


@router.get("/timeline")
def get_timeline(
    video_id: int | None = Query(None, description="Фильтр по video_id"),
    stream_id: int | None = Query(None, description="Фильтр по stream_id"),
    source: str | None = Query(None, description="Фильтр: 'video' или 'stream'"),
    bucket: float = Query(1.0, description="Размер бакета в секундах"),
    db: Session = Depends(get_db),
):
    bucket_label = func.floor(Detection.timestamp / bucket).label("bucket_number")

    query = db.query(
        bucket_label,
        func.count(Detection.id).label("count"),
    ).filter(Detection.timestamp.isnot(None))

    if video_id is not None:
        query = query.filter(Detection.video_id == video_id)

    if stream_id is not None:
        query = query.filter(Detection.stream_id == stream_id)

    if source == "video":
        query = query.filter(Detection.video_id.isnot(None))
    elif source == "stream":
        query = query.filter(Detection.stream_id.isnot(None))

    result = query.group_by(bucket_label).order_by(bucket_label).all()

    return [
        {
            "time_start": round(row.bucket_number * bucket, 2),
            "count": row.count,
        }
        for row in result
        if row.bucket_number is not None
    ]
