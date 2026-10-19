"""
Тесты для ORM-моделей Video и Detection.
Используем SQLite in-memory — настоящая PostgreSQL не нужна.
"""
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.core.database import Base
from app.db.models import Detection, Video


# ============================================================
# ФИКСТУРА: in-memory SQLite база для каждого теста
# ============================================================
@pytest.fixture
def db_session():
    """
    Создаём чистую in-memory SQLite базу для каждого теста.
    SQLite in-memory — база живёт только в RAM,
    когда соединение закрывается — база исчезает.
    """
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False}
    )

    # Создаём все таблицы по нашим моделям
    Base.metadata.create_all(bind=engine)

    # Создаём сессию
    session = Session(bind=engine)

    yield session  # отдаём тесту

    # После теста — чистим
    session.close()
    Base.metadata.drop_all(bind=engine)


# ============================================================
# ТЕСТЫ МОДЕЛИ VIDEO
# ============================================================
def test_create_video(db_session):
    """Можем создать Video и записать в БД"""
    video = Video(
        filename="test.mp4",
        duration=120.5,
        fps=30,
        resolution="1920x1080",
        status="pending",
    )
    db_session.add(video)
    db_session.commit()

    # После commit у video появляется id (автоинкремент)
    assert video.id is not None
    assert video.filename == "test.mp4"
    assert video.duration == 120.5


def test_video_default_status(db_session):
    """У Video есть дефолтный status='processing'"""
    video = Video(
        filename="test2.mp4",
        duration=60.0,
        fps=25,
    )
    db_session.add(video)
    db_session.commit()

    # status не указывали при создании — должен быть дефолт "processing"
    assert video.status == "processing"


# ============================================================
# ТЕСТЫ МОДЕЛИ DETECTION + СВЯЗЬ С VIDEO
# ============================================================
def test_create_detection(db_session):
    """Можем создать Detection привязанный к Video"""
    # Сначала создаём видео (Detection не может существовать без Video)
    video = Video(
        filename="test4.mp4",
        duration=90.0,
        fps=30,
    )
    db_session.add(video)
    db_session.commit()  # получаем video.id

    # Теперь создаём детекцию
    detection = Detection(
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
    """У Video есть .detections — список связанных Detection"""
    video = Video(
        filename="test5.mp4",
        duration=60.0,
        fps=30,
    )
    db_session.add(video)
    db_session.commit()

    # Добавляем 3 детекции к одному видео
    for i in range(3):
        det = Detection(
            video_id=video.id,
            class_name="car",
            confidence=0.8 + i * 0.05,
            bbox_x1=10.0 + i,
            bbox_y1=20.0,
            bbox_x2=50.0,
            bbox_y2=80.0,
            frame_index=i,
            timestamp=i / 30.0,
        )
        db_session.add(det)
    db_session.commit()

    # Проверяем связь: video.detections должен вернуть 3 объекта
    assert len(video.detections) == 3
    # Все детекции принадлежат этому видео
    assert all(d.video_id == video.id for d in video.detections)
