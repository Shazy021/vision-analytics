"""API endpoints для управления RTSP стримами"""
import subprocess
import threading
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from app.core.database import SessionLocal
from app.core import storage
from app.db.models import Stream, Detection

router = APIRouter(prefix="/api/streams", tags=["streams"])

# Хранилище запущенных процессов producer
# {stream_id: subprocess.Popen}
_running_processes: dict[int, subprocess.Popen] = {}


class StreamCreate(BaseModel):
    name: str
    rtsp_url: str
    fps: int = 5


class StreamResponse(BaseModel):
    model_config = {"from_attributes": True}
    id: int
    name: str
    rtsp_url: str
    status: str
    fps: int
    detections_count: int = 0


@router.get("", response_model=list[StreamResponse])
def list_streams():
    db = SessionLocal()
    try:
        streams = db.query(Stream).all()
        result = []
        for s in streams:
            count = db.query(Detection).filter(Detection.stream_id == s.id).count()
            # Проверяем реально ли процесс жив
            actual_status = "active" if s.id in _running_processes and _running_processes[s.id].poll() is None else "inactive"
            if actual_status != s.status:
                s.status = actual_status
                db.commit()
            result.append(StreamResponse(
                id=s.id,
                name=s.name,
                rtsp_url=s.rtsp_url,
                status=actual_status,
                fps=s.fps,
                detections_count=count,
            ))
        return result
    finally:
        db.close()


@router.post("", response_model=StreamResponse, status_code=201)
def create_stream(data: StreamCreate):
    db = SessionLocal()
    try:
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
    finally:
        db.close()


@router.delete("/{stream_id}")
def delete_stream(stream_id: int):
    db = SessionLocal()
    try:
        stream = db.query(Stream).filter(Stream.id == stream_id).first()
        if not stream:
            raise HTTPException(status_code=404, detail="Stream not found")
        # Останавливаем если запущен
        if stream_id in _running_processes:
            _running_processes[stream_id].terminate()
            del _running_processes[stream_id]
        # Удаляем кадры стрима из S3
        storage.delete_prefix(f"streams/{stream_id}/")

        db.delete(stream)
        db.commit()
        return {"deleted": True}
    finally:
        db.close()


@router.post("/{stream_id}/start")
def start_stream(stream_id: int):
    """Запустить обработку стрима"""
    db = SessionLocal()
    try:
        stream = db.query(Stream).filter(Stream.id == stream_id).first()
        if not stream:
            raise HTTPException(status_code=404, detail="Stream not found")

        if stream_id in _running_processes and _running_processes[stream_id].poll() is None:
            raise HTTPException(status_code=400, detail="Stream already running")

        # Запускаем producer как subprocess
        process = subprocess.Popen(
            [
                "python", "-m", "app.streaming.producer",
                stream.rtsp_url,
                str(stream.fps),
                str(stream.id),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        _running_processes[stream_id] = process

        stream.status = "active"
        db.commit()

        return {"status": "active", "stream_id": stream_id, "pid": process.pid}
    finally:
        db.close()


@router.post("/{stream_id}/stop")
def stop_stream(stream_id: int):
    """Остановить обработку стрима"""
    db = SessionLocal()
    try:
        stream = db.query(Stream).filter(Stream.id == stream_id).first()
        if not stream:
            raise HTTPException(status_code=404, detail="Stream not found")

        if stream_id not in _running_processes:
            raise HTTPException(status_code=400, detail="Stream not running")

        _running_processes[stream_id].terminate()
        del _running_processes[stream_id]

        stream.status = "inactive"
        db.commit()

        return {"status": "inactive", "stream_id": stream_id}
    finally:
        db.close()




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

@router.get("/{stream_id}/detections")
def stream_detections(stream_id: int, limit: int = 100):
    db = SessionLocal()
    try:
        stream = db.query(Stream).filter(Stream.id == stream_id).first()
        if not stream:
            raise HTTPException(status_code=404, detail="Stream not found")

        detections = (
            db.query(Detection)
            .filter(Detection.stream_id == stream_id)
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
    finally:
        db.close()