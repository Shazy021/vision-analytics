"""API endpoints для управления RTSP стримами

Заметка про БД: ВСЕ эндпоинты используют Depends(get_db), а не SessionLocal().
Это сделано сознательно — только так FastAPI dependency_overrides в тестах
(test_api.py, test_preview_api.py) подменяют прод-движок на in-memory sqlite.
SessionLocal() в обход Depends(get_db) ломает герметичность тестов.
Сессию закрывает сам get_db (yield + finally) — вручную db.close() не нужен.
"""
import logging
import re

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.core import storage
from app.core.database import get_db
from app.db.models import Stream, StreamDetection, Track
from app.schemas.stream import StreamCreate, StreamResponse

logger = logging.getLogger(__name__)

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
        count = db.query(StreamDetection).filter(StreamDetection.stream_id == s.id).count()
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


def _purge_stream_frames(stream_id: int) -> None:
    """Фоновая зачистка кадров стрима из S3 (после ответа клиенту).

    На стриме с детекциями это тысячи объектов: синхронный delete_prefix
    внутри запроса уводил DELETE за таймауты клиентов (у бенча — 30 с) и
    ронял зачистку стенда. Ошибку только логируем: строка Stream уже
    удалена, повторного триггера нет — осиротевшие объекты дороже не станут.
    """
    try:
        storage.delete_prefix(f"streams/{stream_id}/")
    except Exception:
        logger.exception("Фоновая чистка S3 для стрима %s не удалась", stream_id)


@router.delete("/{stream_id}")
def delete_stream(stream_id: int, background: BackgroundTasks, db: Session = Depends(get_db)):
    stream = db.query(Stream).filter(Stream.id == stream_id).first()
    if not stream:
        raise HTTPException(status_code=404, detail="Stream not found")
    # Процесс здесь НЕ трогаем: как только строка Stream исчезнет из БД,
    # стрим перестанет входить в desired-множество reconciler'а, и тот на
    # ближайшем витке сам убьёт его producer'а (см. app/streaming/manager.py).
    db.delete(stream)
    db.commit()
    # Кадры стрима из S3 вычищаем фоном ПОСЛЕ ответа: десятки тысяч объектов
    # под нагрузкой — минуты работы MinIO, клиенту ждать их незачем.
    background.add_task(_purge_stream_frames, stream_id)
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
        db.query(StreamDetection)
        .filter(StreamDetection.stream_id == stream_id, StreamDetection.frame_index == latest_index)
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

    query = db.query(StreamDetection).filter(StreamDetection.stream_id == stream_id)

    # Если указан frame_index — возвращаем боксы ТОЛЬКО для этого кадра.
    # Это и есть «поднимаем детекции для последнего frame_index из БД».
    if frame_index is not None:
        query = query.filter(StreamDetection.frame_index == frame_index)
        # Лимит не нужен: на один кадр обычно десятки боксов, не тысячи
        detections = query.all()
    else:
        detections = (
            query
            .order_by(StreamDetection.detected_at.desc())
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
            "track_id": d.track_id,
            "bbox": [d.bbox_x1, d.bbox_y1, d.bbox_x2, d.bbox_y2],
            "detected_at": str(d.detected_at),
        }
        for d in detections
    ]


@router.get("/{stream_id}/tracks")
def stream_tracks(
    stream_id: int,
    status: str | None = Query(
        None,
        description="Фильтр по статусу: active / closed. Пусто — все треки.",
    ),
    limit: int = 100,
    db: Session = Depends(get_db),
):
    """Треки стрима (трекинг v1, ByteTrack).

    Сортировка — по последней видимости (свежие первее): активные треки
    почти всегда и так оказываются наверху, а закрытые идут хвостом в
    порядке угасания. duration_sec считаем здесь, а не на клиенте:
    timestamps наивные UTC, и вычитать их «на глаз» из строки — верный
    способ получить рассинхрон с часами браузера.
    """
    stream = db.query(Stream).filter(Stream.id == stream_id).first()
    if not stream:
        raise HTTPException(status_code=404, detail="Stream not found")

    query = db.query(Track).filter(Track.stream_id == stream_id)
    if status:
        query = query.filter(Track.status == status)
    tracks = query.order_by(Track.last_seen_at.desc()).limit(limit).all()

    return [
        {
            "id": t.id,
            "class_name": t.class_name,
            "status": t.status,
            "closed_reason": t.closed_reason,
            "first_seen_at": str(t.first_seen_at),
            "last_seen_at": str(t.last_seen_at),
            "closed_at": str(t.closed_at) if t.closed_at else None,
            "first_frame_index": t.first_frame_index,
            "last_frame_index": t.last_frame_index,
            "num_frames": t.num_frames,
            "duration_sec": (t.last_seen_at - t.first_seen_at).total_seconds(),
            "bbox": [t.bbox_x1, t.bbox_y1, t.bbox_x2, t.bbox_y2]
            if t.bbox_x1 is not None else None,
        }
        for t in tracks
    ]
