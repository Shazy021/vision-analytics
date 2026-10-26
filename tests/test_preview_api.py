"""
Тесты для новых превью-эндпоинтов:

- GET /api/videos/{id}/detections?start_ts=...&end_ts=...
- GET /api/streams/{id}/detections?frame_index=...
- GET /api/streams/{id}/latest-frame (с моком storage.list_objects)

Все тесты — герметичные: БД in-memory, MinIO/Kafka замоканы.
"""
from unittest.mock import patch

import pytest

from app.core.database import get_db
from app.db.models import Detection, Stream, Video
from app.main import app


# ============================================================
# ФИКСТУРЫ: предзаполненные данные (client — общий, из conftest.py)
# ============================================================
@pytest.fixture
def client_with_video_detections(client):
    """1 видео + 4 детекции в разных timestamp."""
    db = next(app.dependency_overrides[get_db]())

    video = Video(filename="test.mp4", duration=10.0, fps=30, status="completed",
                  object_key="videos/1.mp4")
    db.add(video)
    db.commit()

    detections = [
        Detection(video_id=video.id, class_name="person", confidence=0.9,
                  bbox_x1=10, bbox_y1=20, bbox_x2=50, bbox_y2=80,
                  frame_index=0, timestamp=1.0),
        Detection(video_id=video.id, class_name="person", confidence=0.85,
                  bbox_x1=15, bbox_y1=25, bbox_x2=55, bbox_y2=85,
                  frame_index=15, timestamp=1.5),
        Detection(video_id=video.id, class_name="car", confidence=0.95,
                  bbox_x1=100, bbox_y1=200, bbox_x2=300, bbox_y2=400,
                  frame_index=30, timestamp=2.0),
        Detection(video_id=video.id, class_name="car", confidence=0.8,
                  bbox_x1=110, bbox_y1=210, bbox_x2=310, bbox_y2=410,
                  frame_index=45, timestamp=5.0),
    ]
    db.add_all(detections)
    db.commit()
    db.close()
    yield client


@pytest.fixture
def client_with_stream_detections(client):
    """1 стрим + детекции на трёх кадрах (10, 11, 12)."""
    db = next(app.dependency_overrides[get_db]())

    stream = Stream(name="Test stream", rtsp_url="rtsp://x", status="active", fps=5)
    db.add(stream)
    db.commit()

    for frame_index in (10, 11, 12):
        db.add(Detection(
            stream_id=stream.id,
            class_name="person",
            confidence=0.9,
            bbox_x1=10, bbox_y1=20, bbox_x2=50, bbox_y2=80,
            frame_index=frame_index,
        ))
    db.commit()
    db.close()
    yield client


# ============================================================
# ТЕСТЫ: GET /api/videos/{id}/detections?start_ts=&end_ts=
# ============================================================
class TestVideoDetectionsTimeRange:
    def test_returns_all_without_filter(self, client_with_video_detections):
        r = client_with_video_detections.get("/api/videos/1/detections")
        assert r.status_code == 200
        assert len(r.json()) == 4

    def test_filters_by_time_range(self, client_with_video_detections):
        # окно 0.75..1.75 → попадают timestamps 1.0 и 1.5 (2 детекции)
        r = client_with_video_detections.get(
            "/api/videos/1/detections?start_ts=0.75&end_ts=1.75"
        )
        assert r.status_code == 200
        data = r.json()
        assert len(data) == 2
        # сортировка по возрастанию timestamp
        assert data[0]["timestamp"] == 1.0
        assert data[1]["timestamp"] == 1.5

    def test_returns_bbox_field(self, client_with_video_detections):
        r = client_with_video_detections.get(
            "/api/videos/1/detections?start_ts=0.0&end_ts=2.5"
        )
        data = r.json()
        assert "bbox" in data[0]
        assert len(data[0]["bbox"]) == 4

    def test_404_for_missing_video(self, client):
        r = client.get("/api/videos/999/detections")
        assert r.status_code == 404

    def test_empty_window_returns_empty_list(self, client_with_video_detections):
        r = client_with_video_detections.get(
            "/api/videos/1/detections?start_ts=100.0&end_ts=200.0"
        )
        assert r.status_code == 200
        assert r.json() == []

    def test_closest_ts_returns_single_frame_boxes(self, client_with_video_detections):
        """
        closest_ts=1.2 должен вернуть ТОЛЬКО боксы кадра с timestamp=1.0
        (ближайший обработанный <= 1.2). Не 1.5 — это уже «после».
        Раньше с range ±0.5с в окно попали бы 1.0 и 1.5 → каша из боксов.
        """
        r = client_with_video_detections.get(
            "/api/videos/1/detections?closest_ts=1.2"
        )
        assert r.status_code == 200
        data = r.json()
        # Только боксы с timestamp=1.0 (1 person на этом кадре)
        assert len(data) == 1
        timestamps = {d["timestamp"] for d in data}
        assert timestamps == {1.0}, f"Expected only t=1.0, got {timestamps}"
        assert data[0]["class_name"] == "person"

    def test_closest_ts_returns_closest_after_if_none_before(self, client_with_video_detections):
        """
        closest_ts=0.5 — детекций с timestamp <= 0.5 нет (минимальный = 1.0).
        Эндпоинт должен вернуть ближайший ПОСЛЕ (timestamp=1.0).
        """
        r = client_with_video_detections.get(
            "/api/videos/1/detections?closest_ts=0.5"
        )
        assert r.status_code == 200
        data = r.json()
        assert len(data) == 1
        assert all(d["timestamp"] == 1.0 for d in data)

    def test_closest_ts_exact_match(self, client_with_video_detections):
        """closest_ts точно совпадает с timestamp — возвращаются боксы этого кадра."""
        r = client_with_video_detections.get(
            "/api/videos/1/detections?closest_ts=2.0"
        )
        assert r.status_code == 200
        data = r.json()
        assert len(data) == 1  # 1 car на t=2.0
        assert data[0]["timestamp"] == 2.0
        assert data[0]["class_name"] == "car"

    def test_closest_ts_beyond_last_detection(self, client_with_video_detections):
        """closest_ts=999.0 — далеко за пределами — должен вернуть последний кадр."""
        r = client_with_video_detections.get(
            "/api/videos/1/detections?closest_ts=999.0"
        )
        assert r.status_code == 200
        data = r.json()
        # Последний по timestamp = 5.0 (1 car)
        assert len(data) == 1
        assert data[0]["timestamp"] == 5.0


# ============================================================
# ТЕСТЫ: GET /api/videos/{id}/frame-image?timestamp=X
# ============================================================
class TestVideoFrameImage:
    def test_404_if_video_missing(self, client):
        r = client.get("/api/videos/999/frame-image?timestamp=1.0")
        assert r.status_code == 404

    def test_404_if_no_object_key(self, client):
        # Создаём видео без object_key
        db = next(app.dependency_overrides[get_db]())
        db.add(Video(filename="x.mp4", status="completed"))
        db.commit()
        db.close()
        r = client.get("/api/videos/1/frame-image?timestamp=1.0")
        assert r.status_code == 404
        assert "not in S3" in r.json()["detail"]

    def test_returns_jpeg_bytes(self, client_with_video_detections):
        """Мокаем storage.get_presigned_url + cv2.VideoCapture."""
        with patch("app.api.routes.videos.storage.get_presigned_url",
                   return_value="http://fake/video.mp4"), \
             patch("cv2.VideoCapture") as mock_cap_cls:
            # Подделываем открытый cap и один прочитанный кадр
            import numpy as np
            fake_cap = mock_cap_cls.return_value
            fake_cap.isOpened.return_value = True
            fake_cap.read.return_value = (True, np.zeros((100, 100, 3), dtype=np.uint8))

            r = client_with_video_detections.get(
                "/api/videos/1/frame-image?timestamp=2.0"
            )
            assert r.status_code == 200
            assert r.headers["content-type"] == "image/jpeg"
            assert len(r.content) > 0

    def test_502_when_capture_fails(self, client_with_video_detections):
        with patch("app.api.routes.videos.storage.get_presigned_url",
                   return_value="http://fake/video.mp4"), \
             patch("cv2.VideoCapture") as mock_cap_cls:
            fake_cap = mock_cap_cls.return_value
            fake_cap.isOpened.return_value = False

            r = client_with_video_detections.get(
                "/api/videos/1/frame-image?timestamp=2.0"
            )
            assert r.status_code == 502


# ============================================================
# ТЕСТЫ: GET /api/streams/{id}/detections?frame_index=X
# ============================================================
class TestStreamDetectionsByFrame:
    def test_filter_by_frame_index(self, client_with_stream_detections):
        r = client_with_stream_detections.get(
            "/api/streams/1/detections?frame_index=11"
        )
        assert r.status_code == 200
        data = r.json()
        assert len(data) == 1
        assert data[0]["frame_index"] == 11
        assert data[0]["class_name"] == "person"

    def test_no_filter_returns_latest_first(self, client_with_stream_detections):
        r = client_with_stream_detections.get("/api/streams/1/detections?limit=10")
        assert r.status_code == 200
        assert len(r.json()) == 3

    def test_unknown_frame_returns_empty(self, client_with_stream_detections):
        r = client_with_stream_detections.get(
            "/api/streams/1/detections?frame_index=9999"
        )
        assert r.status_code == 200
        assert r.json() == []


# ============================================================
# ТЕСТЫ: GET /api/streams/{id}/latest-frame
# ============================================================
class TestLatestStreamFrame:
    def test_404_when_no_frames_in_minio(self, client_with_stream_detections):
        """Мок storage.list_objects → [] → 404 с понятным сообщением."""
        with patch("app.core.storage.list_objects", return_value=[]):
            r = client_with_stream_detections.get("/api/streams/1/latest-frame")
            assert r.status_code == 404
            assert "No frames" in r.json()["detail"]

    def test_returns_max_frame_index(self, client_with_stream_detections):
        """
        storage.list_objects → ["streams/1/10.jpg", "streams/1/11.jpg", "streams/1/12.jpg"]
        Ожидаем: frame_index=12 (макс), detections_count=1.
        """
        fake_keys = [
            f"streams/1/{i}.jpg" for i in (10, 11, 12)
        ]
        with patch("app.core.storage.list_objects", return_value=fake_keys), \
             patch("app.core.storage.get_presigned_url",
                   return_value="http://fake/minio/12.jpg"):
            r = client_with_stream_detections.get("/api/streams/1/latest-frame")
            assert r.status_code == 200
            data = r.json()
            assert data["frame_index"] == 12
            assert data["frame_url"] == "http://fake/minio/12.jpg"
            assert data["detections_count"] == 1

    def test_404_for_unknown_stream(self, client):
        with patch("app.core.storage.list_objects", return_value=[]):
            r = client.get("/api/streams/999/latest-frame")
            assert r.status_code == 404

    def test_skips_non_numeric_object_names(self, client_with_stream_detections):
        """Если MinIO вернёт мусор — не падаем, а ищем только числа."""
        keys = ["streams/1/abc.jpg", "streams/1/11.jpg", "streams/1/.DS_Store"]
        with patch("app.core.storage.list_objects", return_value=keys), \
             patch("app.core.storage.get_presigned_url",
                   return_value="http://fake/minio/11.jpg"):
            r = client_with_stream_detections.get("/api/streams/1/latest-frame")
            assert r.status_code == 200
            assert r.json()["frame_index"] == 11
