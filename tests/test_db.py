"""
Тесты ORM-моделей Video, VideoDetection, Stream, StreamDetection.
SQLite in-memory — настоящая PostgreSQL не нужна.
"""
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.core.database import Base
from app.db.models import Stream, StreamDetection, Video, VideoDetection


@pytest.fixture
def db_session():
    """Чистая in-memory SQLite база для каждого теста."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
    )
    Base.metadata.create_all(bind=engine)
    session = Session(bind=engine)
    yield session
    session.close()
    Base.metadata.drop_all(bind=engine)


def test_create_video(db_session):
    video = Video(
        filename="test.mp4",
        duration=120.5,
        fps=30,
        resolution="1920x1080",
        status="pending",
    )
    db_session.add(video)
    db_session.commit()

    assert video.id is not None
    assert video.filename == "test.mp4"
    assert video.duration == 120.5


def test_video_default_status(db_session):
    """status не указывали при создании — должен быть дефолт "processing"."""
    video = Video(filename="test2.mp4", duration=60.0, fps=25)
    db_session.add(video)
    db_session.commit()

    assert video.status == "processing"


def test_create_detection(db_session):
    video = Video(filename="test4.mp4", duration=90.0, fps=30)
    db_session.add(video)
    db_session.commit()  # получаем video.id

    detection = VideoDetection(
        video_id=video.id,
        class_name="person",
        confidence=0.95,
        bbox_x1=100.0,
        bbox_y1=200.0,
        bbox_x2=300.0,
        bbox_y2=400.0,
        frame_index=42,
        timestamp=1.4,
    )
    db_session.add(detection)
    db_session.commit()

    assert detection.id is not None
    assert detection.video_id == video.id
    assert detection.class_name == "person"
    assert detection.confidence == 0.95


def test_video_detections_relationship(db_session):
    """video.detections — связанные VideoDetection, все с тем же video_id."""
    video = Video(filename="test5.mp4", duration=60.0, fps=30)
    db_session.add(video)
    db_session.commit()

    for i in range(3):
        db_session.add(VideoDetection(
            video_id=video.id,
            class_name="car",
            confidence=0.8 + i * 0.05,
            bbox_x1=10.0 + i,
            bbox_y1=20.0,
            bbox_x2=50.0,
            bbox_y2=40.0,
            frame_index=i,
            timestamp=i / 30.0,
        ))
    db_session.commit()

    assert len(video.detections) == 3
    assert all(d.video_id == video.id for d in video.detections)


def test_create_stream_detection(db_session):
    stream = Stream(name="cam1", rtsp_url="rtsp://x", status="active", fps=5)
    db_session.add(stream)
    db_session.commit()

    detection = StreamDetection(
        stream_id=stream.id,
        class_name="person",
        confidence=0.9,
        bbox_x1=10.0,
        bbox_y1=20.0,
        bbox_x2=50.0,
        bbox_y2=80.0,
        frame_index=7,
    )
    db_session.add(detection)
    db_session.commit()

    assert detection.id is not None
    assert detection.stream_id == stream.id
    # У стримовой детекции нет своей шкалы времени — только detected_at
    assert detection.detected_at is not None


def test_delete_video_cascades_to_video_detections(db_session):
    video = Video(filename="del.mp4", duration=30.0, fps=30)
    db_session.add(video)
    db_session.commit()
    db_session.add(VideoDetection(
        video_id=video.id,
        class_name="car",
        confidence=0.8,
        bbox_x1=1.0, bbox_y1=2.0, bbox_x2=3.0, bbox_y2=4.0,
        frame_index=0,
        timestamp=0.0,
    ))
    db_session.commit()

    db_session.delete(video)
    db_session.commit()

    assert db_session.query(VideoDetection).count() == 0


def test_delete_stream_cascades_to_stream_detections(db_session):
    stream = Stream(name="del_cam", rtsp_url="rtsp://x", status="active", fps=5)
    db_session.add(stream)
    db_session.commit()
    db_session.add(StreamDetection(
        stream_id=stream.id,
        class_name="car",
        confidence=0.8,
        bbox_x1=1.0, bbox_y1=2.0, bbox_x2=3.0, bbox_y2=4.0,
        frame_index=0,
    ))
    db_session.commit()

    db_session.delete(stream)
    db_session.commit()

    assert db_session.query(StreamDetection).count() == 0
