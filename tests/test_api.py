"""
Тесты для API-эндпоинтов.

TestClient — это "фейковый клиент" который делает HTTP-запросы
без реального запуска сервера. Не нужен uvicorn, не нужен браузер.
"""
import json
from unittest.mock import MagicMock, patch

import pytest

from app.core.database import get_db
from app.core.topics import TOPIC_VIDEO_REQUESTS
from app.db.models import Detection, Stream, Video
from app.main import app


# ============================================================
# ФИКСТУРЫ: предзаполненные данные (client — общий, из conftest.py)
# ============================================================
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


def test_upload_marks_failed_when_storage_down(client):
    """MinIO упал после создания записи в БД → видео failed, а не вечный processing."""
    with patch("app.core.storage.upload_file", side_effect=Exception("MinIO down")):
        response = client.post(
            "/api/videos/upload",
            files={"file": ("clip.mp4", b"0" * 100, "video/mp4")},
        )
    assert response.status_code == 500

    db = _db_session()
    video = db.get(Video, 1)
    assert video.status == "failed"
    assert video.object_key is None
    db.close()


def test_upload_publishes_task_to_kafka(client):
    """Успешный upload публикует {video_id} в топик video_requests и делает flush."""
    with patch("app.core.storage.upload_file"), \
         patch("app.api.routes.videos._get_kafka_producer") as mock_get_producer:
        producer = MagicMock()
        mock_get_producer.return_value = producer
        response = client.post(
            "/api/videos/upload",
            files={"file": ("clip.mp4", b"0" * 100, "video/mp4")},
        )

    assert response.status_code == 201
    topic, kwargs = producer.produce.call_args[0], producer.produce.call_args[1]
    assert topic[0] == TOPIC_VIDEO_REQUESTS
    assert json.loads(kwargs["value"]) == {"video_id": 1}
    # Без flush сообщение осталось бы в локальном буфере продюсера
    producer.flush.assert_called_once()


def test_upload_long_video_publishes_chunk_tasks(client):
    """Длинное видео (проба: 300с) → пакет чанковых сообщений и
    счётчики в БД; короткое (тест выше) — одна классическая задача."""
    with patch("app.core.storage.upload_file"), \
         patch("app.api.routes.videos.probe_media", return_value=(300.0, 30.0)), \
         patch("app.api.routes.videos._get_kafka_producer") as mock_get_producer:
        producer = MagicMock()
        mock_get_producer.return_value = producer
        response = client.post(
            "/api/videos/upload",
            files={"file": ("long.mp4", b"0" * 100, "video/mp4")},
        )

    assert response.status_code == 201
    # 300с / 30с = 10 чанков
    assert producer.produce.call_count == 10
    first = json.loads(producer.produce.call_args_list[0][1]["value"])
    assert first == {"video_id": 1, "start_s": 0.0, "end_s": 30.0}
    last = json.loads(producer.produce.call_args_list[-1][1]["value"])
    assert last["start_s"] == 270.0
    assert last["end_s"] == 300.0
    producer.flush.assert_called_once()

    db = _db_session()
    video = db.get(Video, 1)
    assert video.chunks_total == 10
    assert video.chunks_done == 0
    assert video.duration == 300.0
    assert video.fps == 30
    db.close()


def test_upload_marks_failed_when_kafka_down(client):
    """Kafka недоступна → 500 и failed, а не вечный processing без задачи."""
    with patch("app.core.storage.upload_file"), \
         patch("app.api.routes.videos._enqueue_video_tasks",
                side_effect=Exception("Kafka down")):
        response = client.post(
            "/api/videos/upload",
            files={"file": ("clip.mp4", b"0" * 100, "video/mp4")},
        )
    assert response.status_code == 500

    db = _db_session()
    assert db.get(Video, 1).status == "failed"
    db.close()


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


# ============================================================
# ТЕСТЫ STREAM start/stop — API пишет ТОЛЬКО desired-состояние
#
# После перехода на паттерн desired-state + reconciler эндпоинты start/stop
# ничего не спавнят: они лишь меняют Stream.desired_status. Реальными
# процессами владеет stream_manager, поэтому здесь проверяем именно запись
# намерения в БД, а не заведённый процесс.
# ============================================================
def _db_session():
    """Сессия поверх того же движка, что использует TestClient (StaticPool)."""
    return next(app.dependency_overrides[get_db]())


def _create_stream(client):
    r = client.post("/api/streams", json={"name": "cam", "rtsp_url": "rtsp://x", "fps": 5})
    assert r.status_code == 201
    return r.json()["id"]


def test_start_stream_sets_desired_active(client):
    sid = _create_stream(client)
    r = client.post(f"/api/streams/{sid}/start")
    assert r.status_code == 200
    assert r.json()["desired_status"] == "active"

    db = _db_session()
    s = db.get(Stream, sid)
    assert s.desired_status == "active"   # намерение записано
    assert s.status == "inactive"         # API НЕ выставляет факт — это reconciler
    db.close()


def test_stop_stream_sets_desired_inactive(client):
    sid = _create_stream(client)
    client.post(f"/api/streams/{sid}/start")
    r = client.post(f"/api/streams/{sid}/stop")
    assert r.status_code == 200
    assert r.json()["desired_status"] == "inactive"

    db = _db_session()
    assert db.get(Stream, sid).desired_status == "inactive"
    db.close()


def test_start_stop_are_idempotent(client):
    """Повторный start/stop не должен ломаться: API не знает про живость процесса."""
    sid = _create_stream(client)
    assert client.post(f"/api/streams/{sid}/start").status_code == 200
    assert client.post(f"/api/streams/{sid}/start").status_code == 200
    assert client.post(f"/api/streams/{sid}/stop").status_code == 200
    assert client.post(f"/api/streams/{sid}/stop").status_code == 200


def test_start_stream_not_found(client):
    assert client.post("/api/streams/99999/start").status_code == 404


def test_list_streams_reports_stored_status(client):
    """list_streams отдаёт status как факт из БД, а не пересчитывает по Popen.

    Регрессия на старое поведение: раньше список перезаписывал статус по
    живости subprocess'а в памяти API. Теперь reconciler поставил 'error' —
    и API обязан показать 'error', даже если у него нет никакого процесса.
    """
    sid = _create_stream(client)
    db = _db_session()
    s = db.get(Stream, sid)
    s.status = "error"
    s.status_reason = "producer падал слишком быстро"
    db.commit()
    db.close()

    data = client.get("/api/streams").json()
    row = next(x for x in data if x["id"] == sid)
    assert row["status"] == "error"
