"""API endpoints для управления RTSP стримами

Заметка про БД: ВСЕ эндпоинты используют Depends(get_db), а не SessionLocal().
Это сделано сознательно — только так FastAPI dependency_overrides в тестах
(test_api.py, test_preview_api.py) подменяют прод-движок на in-memory sqlite.
SessionLocal() в обход Depends(get_db) ломает герметичность тестов.
Сессию закрывает сам get_db (yield + finally) — вручную db.close() не нужен.
"""
import re

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.core import storage
from app.core.database import get_db
from app.db.models import Detection, Stream
from app.schemas.stream import StreamCreate, StreamResponse

router = APIRouter(prefix="/api/streams", tags=["streams"])

# ВАЖНО (паттерн desired-state + reconciler):
# Здесь больше НЕТ словаря запущенных Popen. API не порождает и не убивает
# процессы producer'а — он только пишет НАМЕРЕНИЕ (desired_status) в БД.
# Реальными процессами владеет отдельный сервис app/streaming/manager.py.
# Благодаря этому рестарт/масштабирование API не роняет и не дублирует стримы.


@router.get("", response_model=list[StreamResponse])
def list_streams(db: Session = Depends(get_db)):
    streams = db.query(Stream).all()
    result = []
    for s in streams:
        count = db.query(Detection).filter(Detection.stream_id == s.id).count()
        # status — это ФАКТ, который пишет stream_manager (reconciler).
        # API его только отдаёт наружу, сам не пересчитывает и не пишет:
        # раньше здесь был тест на живость Popen из памяти API — он и
        # создавал иллюзию, что статус «принадлежит» API-контейнеру.
        result.append(StreamResponse(
            id=s.id,
            name=s.name,
            rtsp_url=s.rtsp_url,
            status=s.status,
            fps=s.fps,
            detections_count=count,
        ))
    return result


@router.post("", response_model=StreamResponse, status_code=201)
def create_stream(data: StreamCreate, db: Session = Depends(get_db)):
    stream = Stream(
        name=data.name,
        rtsp_url=data.rtsp_url,
        fps=data.fps,
        status="inactive",
    )
    db.add(stream)
    db.commit()
    db.refresh(stream)
    return StreamResponse(
        id=stream.id,
        name=stream.name,
        rtsp_url=stream.rtsp_url,
        status=stream.status,
        fps=stream.fps,
        detections_count=0,
    )


@router.delete("/{stream_id}")
def delete_stream(stream_id: int, db: Session = Depends(get_db)):
    stream = db.query(Stream).filter(Stream.id == stream_id).first()
    if not stream:
        raise HTTPException(status_code=404, detail="Stream not found")
    # Процесс здесь НЕ трогаем: как только строка Stream исчезнет из БД,
    # стрим перестанет входить в desired-множество reconciler'а, и тот на
    # ближайшем витке сам убьёт его producer'а (см. app/streaming/manager.py).
    # Удаляем кадры стрима из S3
    storage.delete_prefix(f"streams/{stream_id}/")

    db.delete(stream)
    db.commit()
    return {"deleted": True}


@router.post("/{stream_id}/start")
def start_stream(stream_id: int, db: Session = Depends(get_db)):
    """Запросить запуск стрима (пишет только НАМЕРЕНИЕ).

    Ничего не спавнит. Ставит desired_status='active' — «хочу, чтобы этот
    стрим работал». Кто реально поднимет producer'а и проследит, чтобы он
    оставался живым, — stream_manager (reconciler). Идемпотентно: повторный
    start ничего плохого не делает (просто ещё раз пишет 'active').
    """
    stream = db.query(Stream).filter(Stream.id == stream_id).first()
    if not stream:
        raise HTTPException(status_code=404, detail="Stream not found")

    stream.desired_status = "active"
    db.commit()

    return {
        "desired_status": "active",
        "stream_id": stream_id,
        "note": "reconciler will spawn the producer on its next pass",
    }


@router.post("/{stream_id}/stop")
def stop_stream(stream_id: int, db: Session = Depends(get_db)):
    """Запросить остановку стрима (пишет только НАМЕРЕНИЕ).

    desired_status='inactive' — «хочу, чтобы этот стрим НЕ работал».
    reconciler на ближайшем витке увидит расхождение (процесс жив, а desired
    inactive) и завершит producer'а. Идемпотентно: stop остановленного стрима
    — тоже 200 (API не знает и не должен знать, жив ли процесс).
    """
    stream = db.query(Stream).filter(Stream.id == stream_id).first()
    if not stream:
        raise HTTPException(status_code=404, detail="Stream not found")

    stream.desired_status = "inactive"
    db.commit()

    return {"desired_status": "inactive", "stream_id": stream_id}


@router.get("/{stream_id}/frames/{frame_index}")
def get_stream_frame(stream_id: int, frame_index: int):
    """
    Получить чистый кадр стрима из MinIO.

    YOLO Consumer сохраняет кадры (только с детекциями) по ключу:
    streams/{stream_id}/{frame_index}.jpg

    Фронт получает чистый кадр + боксы из /detections и рисует сам.
    """
    frame_key = f"streams/{stream_id}/{frame_index}.jpg"

    if not storage.file_exists(frame_key):
        raise HTTPException(status_code=404, detail="Frame not found in S3")

    try:
        url = storage.get_presigned_url(frame_key)
    except Exception:
        raise HTTPException(status_code=500, detail="Failed to generate frame URL")

    return {
        "stream_id": stream_id,
        "frame_index": frame_index,
        "frame_url": url,
    }


def _extract_frame_index(object_name: str) -> int | None:
    """
    Достаём целочисленный frame_index из object_name вида 'streams/1/42.jpg'.
    Возвращает None, если имя не соответствует шаблону.
    """
    match = re.search(r"(\d+)\.jpg$", object_name)
    return int(match.group(1)) if match else None


@router.get("/{stream_id}/latest-frame")
def get_latest_stream_frame(stream_id: int, db: Session = Depends(get_db)):
    """
    Найти последний сохранённый кадр стрима в MinIO.

    YOLO Consumer сохраняет кадры только при наличии детекций. Поэтому
    «последний кадр» — это косвенный индикатор «когда последний раз что-то
    детектировалось на камере».

    Возвращает:
      - frame_index: номер последнего кадра
      - frame_url: presigned URL на чистый кадр
      - detections_count: сколько детекций на этом кадре в БД

    Дашборд поллит этот эндпоинт каждые 2–3 секунды — так получается
    «слайдшоу»-превью живого стрима без непрерывного видео-туннеля.
    """
    stream = db.query(Stream).filter(Stream.id == stream_id).first()
    if not stream:
        raise HTTPException(status_code=404, detail="Stream not found")

    # Список объектов с префиксом streams/{stream_id}/
    object_names = storage.list_objects(f"streams/{stream_id}/")
    if not object_names:
        raise HTTPException(
            status_code=404,
            detail="No frames stored for this stream yet "
                   "(frames are saved only when detections occur)",
        )

    # Находим максимальный frame_index по числовой части имени файла
    frame_indices = [
        idx for idx in (_extract_frame_index(n) for n in object_names)
        if idx is not None
    ]
    if not frame_indices:
        raise HTTPException(status_code=500, detail="Failed to parse frame indices")

    latest_index = max(frame_indices)
    frame_key = f"streams/{stream_id}/{latest_index}.jpg"

    try:
        url = storage.get_presigned_url(frame_key)
    except Exception:
        raise HTTPException(status_code=500, detail="Failed to generate frame URL")

    # Сколько детекций на этом кадре — пригодится для бейджа на превью
    det_count = (
        db.query(Detection)
        .filter(Detection.stream_id == stream_id, Detection.frame_index == latest_index)
        .count()
    )

    return {
        "stream_id": stream_id,
        "frame_index": latest_index,
        "frame_url": url,
        "detections_count": det_count,
    }


@router.get("/{stream_id}/detections")
def stream_detections(
    stream_id: int,
    limit: int = 100,
    frame_index: int | None = Query(
        None,
        description="Фильтр по конкретному frame_index (для оверлея боксов)",
    ),
    db: Session = Depends(get_db),
):
    stream = db.query(Stream).filter(Stream.id == stream_id).first()
    if not stream:
        raise HTTPException(status_code=404, detail="Stream not found")

    query = db.query(Detection).filter(Detection.stream_id == stream_id)

    # Если указан frame_index — возвращаем боксы ТОЛЬКО для этого кадра.
    # Это и есть «поднимаем детекции для последнего frame_index из БД».
    if frame_index is not None:
        query = query.filter(Detection.frame_index == frame_index)
        # Лимит не нужен: на один кадр обычно десятки боксов, не тысячи
        detections = query.all()
    else:
        detections = (
            query
            .order_by(Detection.detected_at.desc())
            .limit(limit)
            .all()
        )

    return [
        {
            "id": d.id,
            "frame_index": d.frame_index,
            "class_name": d.class_name,
            "confidence": d.confidence,
            "stream_id": d.stream_id,
            "bbox": [d.bbox_x1, d.bbox_y1, d.bbox_x2, d.bbox_y2],
            "detected_at": str(d.detected_at),
        }
        for d in detections
    ]
