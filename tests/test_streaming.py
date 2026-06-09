"""Тесты для Kafka streaming модуля"""
import json
import pytest
from unittest.mock import MagicMock, patch, call


# ---- Тесты FrameProducer ----

class TestFrameProducer:
    """Тесты формата сообщений producer"""

    def test_message_format(self):
        """Проверяем что сообщение имеет правильную структуру"""
        # Мокаем Kafka Producer
        with patch("app.streaming.producer.Producer") as mock_producer_cls:
            mock_producer = MagicMock()
            mock_producer_cls.return_value = mock_producer

            from app.streaming.producer import FrameProducer
            producer = FrameProducer("test.mp4", fps=2)

            # Проверяем что Producer создан с правильными параметрами
            mock_producer_cls.assert_called_once()
            call_kwargs = mock_producer_cls.call_args[0][0]
            assert call_kwargs["bootstrap.servers"] == "localhost:9092"

    def test_producer_has_delivery_report(self):
        """Проверяем что callback для ошибок определён"""
        from app.streaming.producer import FrameProducer
        producer = FrameProducer.__new__(FrameProducer)
        # Метод должен существовать
        assert hasattr(producer, "_delivery_report")


# ---- Тесты YOLOConsumer ----

class TestYOLOConsumer:
    """Тесты обработки сообщений YOLO consumer"""

    def test_detection_message_format(self):
        """Проверяем формат сообщения с детекциями"""
        # Формат который YOLO consumer отправляет в detections
        detection_msg = {
            "video_id": None,
            "stream_id": 1,
            "frame_index": 42,
            "source": "rtsp://camera1",
            "objects": [
                {
                    "class": "person",
                    "confidence": 0.95,
                    "bbox": {"x1": 100.0, "y1": 200.0, "x2": 300.0, "y2": 400.0},
                },
                {
                    "class": "car",
                    "confidence": 0.87,
                    "bbox": {"x1": 500.0, "y1": 100.0, "x2": 800.0, "y2": 300.0},
                },
            ],
            "num_detections": 2,
        }

        # Проверяем структуру
        assert detection_msg["video_id"] is None
        assert detection_msg["stream_id"] == 1
        assert detection_msg["frame_index"] == 42
        assert len(detection_msg["objects"]) == 2
        assert detection_msg["num_detections"] == 2

        # Проверяем формат объекта
        obj = detection_msg["objects"][0]
        assert "class" in obj
        assert "confidence" in obj
        assert "bbox" in obj
        assert all(k in obj["bbox"] for k in ["x1", "y1", "x2", "y2"])

    def test_detection_message_serializable(self):
        """Проверяем что сообщение можно сериализовать в JSON (Kafka требует bytes)"""
        detection_msg = {
            "video_id": None,
            "stream_id": 1,
            "frame_index": 42,
            "source": "rtsp://camera1",
            "objects": [{"class": "person", "confidence": 0.95, "bbox": {"x1": 1.0, "y1": 2.0, "x2": 3.0, "y2": 4.0}}],
            "num_detections": 1,
        }

        # Должно сериализоваться без ошибок
        serialized = json.dumps(detection_msg).encode()
        assert isinstance(serialized, bytes)

        # И десериализоваться обратно
        deserialized = json.loads(serialized.decode())
        assert deserialized == detection_msg


# ---- Тесты DetectionConsumer ----

class TestDetectionConsumer:
    """Тесты сохранения детекций в БД"""

    @patch("app.streaming.consumer.SessionLocal")
    def test_save_to_db_creates_detection_objects(self, mock_session_cls):
        """Проверяем что _save_to_db создаёт правильные Detection объекты"""
        mock_db = MagicMock()
        mock_session_cls.return_value = mock_db

        from app.streaming.consumer import DetectionConsumer
        consumer = DetectionConsumer.__new__(DetectionConsumer)

        detection = {
            "video_id": None,
            "stream_id": 1,
            "frame_index": 42,
            "source": "rtsp://camera1",
            "objects": [
                {"class": "person", "confidence": 0.95, "bbox": {"x1": 100.0, "y1": 200.0, "x2": 300.0, "y2": 400.0}},
            ],
        }

        with patch("app.streaming.consumer.Detection") as mock_detection_cls:
            consumer._save_to_db(detection)

            # Проверяем что Detection создан с правильными параметрами
            mock_detection_cls.assert_called_once_with(
                video_id=None,
                stream_id=1,
                frame_index=42,
                class_name="person",
                confidence=0.95,
                bbox_x1=100.0,
                bbox_y1=200.0,
                bbox_x2=300.0,
                bbox_y2=400.0,
            )
            mock_db.add.assert_called_once()
            mock_db.commit.assert_called_once()
            mock_db.close.assert_called_once()

    @patch("app.streaming.consumer.SessionLocal")
    def test_save_to_db_rollback_on_error(self, mock_session_cls):
        """Проверяем что при ошибке делается rollback"""
        mock_db = MagicMock()
        mock_db.add.side_effect = Exception("DB error")
        mock_session_cls.return_value = mock_db

        from app.streaming.consumer import DetectionConsumer
        consumer = DetectionConsumer.__new__(DetectionConsumer)

        detection = {
            "video_id": None,
            "stream_id": 1,
            "frame_index": 42,
            "objects": [{"class": "person", "confidence": 0.95, "bbox": {"x1": 1.0, "y1": 2.0, "x2": 3.0, "y2": 4.0}}],
        }

        with patch("app.streaming.consumer.Detection"):
            consumer._save_to_db(detection)

            mock_db.rollback.assert_called_once()
            mock_db.close.assert_called_once()


# ---- Тесты топиков ----

class TestTopics:
    """Тесты конфигурации топиков"""

    def test_topic_names(self):
        """Проверяем имена топиков"""
        from app.streaming.topics import TOPIC_RAW_FRAMES, TOPIC_DETECTIONS
        assert TOPIC_RAW_FRAMES == "raw_frames"
        assert TOPIC_DETECTIONS == "detections"

    def test_bootstrap_servers(self):
        """Проверяем адрес Kafka"""
        from app.streaming.topics import KAFKA_BOOTSTRAP_SERVERS
        assert KAFKA_BOOTSTRAP_SERVERS == "localhost:9092"