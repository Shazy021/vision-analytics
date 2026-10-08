"""
Тесты для API-эндпоинтов.

TestClient — это "фейковый клиент" который делает HTTP-запросы
без реального запуска сервера. Не нужен uvicorn, не нужен браузер.
"""
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from app.core.database import Base, get_db
from app.db.models import Detection, Video
from app.main import app


# ============================================================
# ФИКСТУРА: in-memory БД + TestClient
# ============================================================
@pytest.fixture
def client():
    """
    Создаём TestClient с подменённой БД.

    StaticPool — ВСЕ соединения используют ОДНУ и ту же связь с БД.
    Без него SQLite :memory: для каждого соединения создаёт
    новую пустую базу, и create_all() не помогает.
    """
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,  # ← ключевое отличие!
    )
    Base.metadata.create_all(bind=engine)

    def test_get_db():
        db = Session(bind=engine)
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = test_get_db

    with TestClient(app) as c:
        yield c

    app.dependency_overrides.clear()
    Base.metadata.drop_all(bind=engine)


@pytest.fixture
def client_with_data(client):
    """
    Тот же client, но с предзаполненными данными.
    Создаём 1 видео и 5 детекций для тестов аналитики.

    Важно: используем ТУТ ЖЕ engine что и в client,
    а не создаём новый. Иначе данные в одной БД, а запросы в другой.
    """
    # Берём движок из текущего override
    # TestClient уже запущен, dependency_overrides уже установлен
    # Нам нужно получить сессию из test_get_db чтобы вставить данные
    db = next(app.dependency_overrides[get_db]())

    video = Video(filename="test.mp4", duration=30.0, fps=30, status="completed")
    db.add(video)
    db.commit()

    # 3 person + 2 car = 5 детекций
    detections = [
        Detection(video_id=video.id, class_name="person", confidence=0.9,
                  bbox_x1=10, bbox_y1=20, bbox_x2=50, bbox_y2=80,
                  frame_index=0, timestamp=0.0),
        Detection(video_id=video.id, class_name="person", confidence=0.85,
                  bbox_x1=15, bbox_y1=25, bbox_x2=55, bbox_y2=85,
                  frame_index=15, timestamp=0.5),
        Detection(video_id=video.id, class_name="person", confidence=0.7,
                  bbox_x1=20, bbox_y1=30, bbox_x2=60, bbox_y2=90,
                  frame_index=30, timestamp=1.0),
        Detection(video_id=video.id, class_name="car", confidence=0.95,
                  bbox_x1=100, bbox_y1=200, bbox_x2=300, bbox_y2=400,
                  frame_index=0, timestamp=0.0),
        Detection(video_id=video.id, class_name="car", confidence=0.8,
                  bbox_x1=110, bbox_y1=210, bbox_x2=310, bbox_y2=410,
                  frame_index=45, timestamp=1.5),
    ]
    db.add_all(detections)
    db.commit()
    db.close()

    yield client  # отдаём тот же client, он уже настроен


# ============================================================
# ТЕСТЫ ВИДЕО-ЭНДПОИНТОВ
# ============================================================
def test_create_video(client):
    """POST /api/videos — создаём видео"""
    response = client.post(
        "/api/videos",
        json={"filename": "new_video.mp4"}
    )
    assert response.status_code == 201
    data = response.json()
    assert data["filename"] == "new_video.mp4"
    assert data["status"] == "processing"
    assert data["id"] is not None


def test_create_video_with_all_fields(client):
    """POST /api/videos — все поля"""
    response = client.post(
        "/api/videos",
        json={
            "filename": "full_video.mp4",
            "duration": 120.5,
            "fps": 30,
            "resolution": "1920x1080",
        }
    )
    assert response.status_code == 201
    data = response.json()
    assert data["duration"] == 120.5
    assert data["fps"] == 30
    assert data["resolution"] == "1920x1080"


def test_list_videos(client_with_data):
    """GET /api/videos — список видео"""
    response = client_with_data.get("/api/videos")
    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    assert data[0]["filename"] == "test.mp4"


def test_get_video(client_with_data):
    """GET /api/videos/1 — одно видео"""
    response = client_with_data.get("/api/videos/1")
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == 1
    assert data["detections_count"] == 5  # 3 person + 2 car


def test_get_video_not_found(client):
    """GET /api/videos/999 — видео не существует"""
    response = client.get("/api/videos/999")
    assert response.status_code == 404


# ============================================================
# ТЕСТЫ АНАЛИТИКИ
# ============================================================
def test_stats(client_with_data):
    """GET /api/analytics/stats — общая статистика"""
    response = client_with_data.get("/api/analytics/stats")
    assert response.status_code == 200
    data = response.json()
    assert data["total_videos"] >= 1
    assert data["total_detections"] == 5


def test_class_distribution(client_with_data):
    """GET /api/analytics/class-distribution — распределение по классам"""
    response = client_with_data.get("/api/analytics/class-distribution")
    assert response.status_code == 200
    data = response.json()
    # person = 3, car = 2 (отсортировано по убыванию)
    assert len(data) == 2
    assert data[0]["class_name"] == "person"
    assert data[0]["count"] == 3
    assert data[1]["class_name"] == "car"
    assert data[1]["count"] == 2


def test_class_distribution_with_video_filter(client_with_data):
    """GET /api/analytics/class-distribution?video_id=1 — фильтр по видео"""
    response = client_with_data.get(
        "/api/analytics/class-distribution?video_id=1"
    )
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2  # person и car


def test_timeline(client_with_data):
    """GET /api/analytics/timeline — временная линия"""
    response = client_with_data.get(
        "/api/analytics/timeline?bucket=1.0"
    )
    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    for item in data:
        assert "time_start" in item
        assert "count" in item
        assert item["count"] > 0
