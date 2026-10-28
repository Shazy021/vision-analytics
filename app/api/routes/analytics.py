"""
Аналитические эндпоинты — агрегация данных по детекциям.

Две таблицы (video_detections / stream_detections) вместо общей:
источник запроса определяет, какую читать. Без фильтра — объединяем
обе популяции на стороне приложения (COUNT/SUM и merge по классам).
"""
from fastapi import APIRouter, Depends, Query
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.db.models import StreamDetection, Video, VideoDetection

router = APIRouter(prefix="/api/analytics", tags=["Analytics"])


def _selected_tables(source: str | None) -> list[type]:
    """Какие таблицы детекций попадают в запрос по значению source."""
    tables = []
    if source != "stream":
        tables.append(VideoDetection)
    if source != "video":
        tables.append(StreamDetection)
    return tables


@router.get("/stats")
def get_overall_stats(
    source: str | None = Query(None, description="Фильтр: 'video' или 'stream'"),
    db: Session = Depends(get_db),
):
    total_detections = 0
    weighted_conf_sum = 0.0
    for table in _selected_tables(source):
        cnt, avg = db.query(func.count(table.id), func.avg(table.confidence)).one()
        total_detections += cnt
        if avg is not None:
            weighted_conf_sum += cnt * avg
    # Средневзвешенное по количеству детекций — просто усреднить два AVG
    # нельзя: в таблицах разное число строк.
    avg_confidence = weighted_conf_sum / total_detections if total_detections else None

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
    # Детекция принадлежит ровно одному источнику, поэтому одновременный
    # фильтр по video_id и stream_id — пустое множество.
    if video_id is not None and stream_id is not None:
        return []

    if video_id is not None or source == "video":
        selections = [(VideoDetection, VideoDetection.video_id == video_id)]
    elif stream_id is not None or source == "stream":
        selections = [(StreamDetection, StreamDetection.stream_id == stream_id)]
    else:
        selections = [(VideoDetection, None), (StreamDetection, None)]

    counts: dict[str, int] = {}
    for table, cond in selections:
        query = db.query(table.class_name, func.count(table.id))
        if cond is not None:
            query = query.filter(cond)
        for cls, cnt in query.group_by(table.class_name).all():
            counts[cls] = counts.get(cls, 0) + cnt

    return [
        {"class_name": cls, "count": cnt}
        for cls, cnt in sorted(counts.items(), key=lambda kv: kv[1], reverse=True)
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
        func.date_trunc("minute", StreamDetection.detected_at).label("minute_bucket"),
        func.count(StreamDetection.id).label("count"),
    ).filter(
        StreamDetection.detected_at >= since,
    )

    if stream_id is not None:
        query = query.filter(StreamDetection.stream_id == stream_id)

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
    """Детекции по времени ВНУТРИ видео (timestamp — секунды от начала файла).

    Системой времени «секунда от начала» обладают только видео-детекции,
    поэтому запрос идёт по video_detections. stream_id исторически давал
    пусто (у стримов timestamp отсутствовал) — поведение сохранено.
    """
    if stream_id is not None:
        return []

    bucket_label = func.floor(VideoDetection.timestamp / bucket).label("bucket_number")

    query = db.query(
        bucket_label,
        func.count(VideoDetection.id).label("count"),
    )

    if video_id is not None:
        query = query.filter(VideoDetection.video_id == video_id)

    result = query.group_by(bucket_label).order_by(bucket_label).all()

    return [
        {
            "time_start": round(row.bucket_number * bucket, 2),
            "count": row.count,
        }
        for row in result
        if row.bucket_number is not None
    ]
