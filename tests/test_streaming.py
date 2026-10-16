"""Тесты для Kafka streaming модуля"""
import json
from unittest.mock import MagicMock, patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.core.database import Base
from app.db.models import Stream
from app.streaming.manager import MAX_FAST_CRASHES, StreamManager
from app.streaming.yolo_consumer import YOLOConsumer

# ---- Тесты FrameProducer ----

class TestFrameProducer:
    """Тесты формата сообщений producer"""

    def test_message_format(self):
        """Проверяем что сообщение имеет правильную структуру"""
        # Мокаем Kafka Producer
        with patch("app.streaming.producer.Producer") as mock_producer_cls:
            mock_producer = MagicMock()
            mock_producer_cls.return_value = mock_producer

            from app.core.config import settings
            from app.streaming.producer import FrameProducer
            FrameProducer("test.mp4", fps=2)

            # Проверяем что Producer создан с правильными параметрами.
            # Сверяемся с settings, а не с хардкодом: .env для локального
            # запуска и для compose задают разные адреса Kafka
            # (localhost:9092 vs kafka:29092) — контракт в том, что адрес
            # ВСЕГДА берётся из настроек
            mock_producer_cls.assert_called_once()
            call_kwargs = mock_producer_cls.call_args[0][0]
            assert call_kwargs["bootstrap.servers"] == settings.kafka_bootstrap_servers

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

    def _make_consumer(self):
        """Consumer без __init__ — без Kafka, без YOLO, без сети.
        __new__ создаёт объект, не вызывая конструктор. Обычный трюк
        для тестирования классов с тяжёлой инициализацией."""
        c = YOLOConsumer.__new__(YOLOConsumer)
        c.model = MagicMock()
        c.device = "cpu"
        c.producer = MagicMock()
        c.consumer = MagicMock()
        return c

    def test_poison_message_is_skipped_not_fatal(self):
        consumer = self._make_consumer()

        bad_msg = MagicMock()
        bad_msg.value.return_value = b"not json at all"
        bad_msg.error.return_value = None                  # ВАЖНО: без этого
            # msg.error() вернёт MagicMock — а это truthy, и мы уйдём в ветку
            # "Kafka error: continue", не дойдя до _handle. Классическая ловушка.
        bad_msg.topic.return_value = "raw_frames"
        bad_msg.offset.return_value = 42

        def fake_poll(timeout):
            if not fake_poll.called:
                fake_poll.called = True
                return bad_msg
            raise KeyboardInterrupt

        fake_poll.called = False
        consumer.consumer.poll.side_effect = fake_poll

        consumer.run()

        assert consumer.poison_count == 1

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
        from app.streaming.topics import TOPIC_DETECTIONS, TOPIC_RAW_FRAMES
        assert TOPIC_RAW_FRAMES == "raw_frames"
        assert TOPIC_DETECTIONS == "detections"

    def test_bootstrap_servers(self):
        """Адрес Kafka приходит из настроек (KAFKA_BOOTSTRAP_SERVERS в .env/compose)"""
        from app.core.config import settings
        from app.streaming.topics import KAFKA_BOOTSTRAP_SERVERS
        assert KAFKA_BOOTSTRAP_SERVERS == settings.kafka_bootstrap_servers


# ---- Тесты StreamManager (reconciler) ----

class FakeProcess:
    """Замена subprocess.Popen для тестов reconciler'а.

    .alive управляет ответом poll(): None = жив, код = мёртв.
    terminate() помечает убитым. Никаких реальных процессов и сети.
    """
    def __init__(self, alive=True):
        self.alive = alive
        self.terminated = False
        self.pid = 4242

    def poll(self):
        return None if self.alive else 0

    def terminate(self):
        self.terminated = True
        self.alive = False


class TestStreamManager:
    """Reconciler: сверяет desired из БД с фактическими процессами."""

    def _fresh(self, desired="active"):
        """Чистая in-memory БД + один стрим + менеджер с мок-spawn.

        Возвращает (db, Session, stream, spawned) — spawned список фейковых
        процессов, которые вернула фабрика spawn.
        """
        engine = create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Base.metadata.create_all(bind=engine)
        Session = sessionmaker(bind=engine)
        db = Session()
        stream = Stream(
            name="cam", rtsp_url="rtsp://x", fps=5,
            status="inactive", desired_status=desired,
        )
        db.add(stream)
        db.commit()
        db.refresh(stream)

        spawned = []
        def spawn(rtsp, fps, sid, alive=True):
            p = FakeProcess(alive=alive)
            spawned.append(p)
            return p

        manager = StreamManager(spawn=spawn, session_factory=Session)
        return db, Session, stream, spawned, manager

    def test_spawns_missing_producer(self):
        """desired=active, процесса нет → reconciler поднимает и ставит status=active."""
        db, _, stream, spawned, mgr = self._fresh()
        mgr.reconcile_once(db)
        assert len(spawned) == 1
        assert db.get(Stream, stream.id).status == "active"
        db.close()

    def test_does_not_respawn_live_producer(self):
        """Идемпотентность: живой нужный процесс не трогаем на следующем витке."""
        db, _, stream, spawned, mgr = self._fresh()
        mgr.reconcile_once(db)
        mgr.reconcile_once(db)
        assert len(spawned) == 1  # второй виток ничего не породил

    def test_respawns_after_crash(self):
        """producer умер → на следующем витке поднялся новый."""
        db, _, stream, spawned, mgr = self._fresh()
        mgr.reconcile_once(db)
        assert len(spawned) == 1
        spawned[0].alive = False          # имитируем падение
        mgr.reconcile_once(db)
        assert len(spawned) == 2          # поднят второй
        assert db.get(Stream, stream.id).status == "active"
        db.close()

    def test_terminates_when_desired_inactive(self):
        """desired=inactive → живой процесс убит, status=inactive."""
        db, _, stream, spawned, mgr = self._fresh()
        mgr.reconcile_once(db)
        assert len(spawned) == 1
        db.get(Stream, stream.id).desired_status = "inactive"
        db.commit()
        mgr.reconcile_once(db)
        assert spawned[0].terminated is True
        assert db.get(Stream, stream.id).status == "inactive"
        db.close()

    def test_backoff_marks_error_and_stops(self):
        """Быстрые смерти подряд → status=error, спавнов больше нет (защита от шторма)."""
        db, _, stream, spawned, mgr = self._fresh()
        # spawn возвращает уже мёртвый процесс → uptime ~0 < окна = краш каждый виток
        def dead_spawn(rtsp, fps, sid):
            p = FakeProcess(alive=False)
            spawned.append(p)
            return p
        mgr.spawn = dead_spawn

        for _ in range(MAX_FAST_CRASHES + 3):
            mgr.reconcile_once(db)

        st = db.get(Stream, stream.id)
        assert st.status == "error"
        assert st.status_reason            # причина заполнена
        # породили ровно MAX_FAST_CRASHES процессов и остановились
        assert len(spawned) == MAX_FAST_CRASHES

    def test_retry_after_stop_start_clears_error(self):
        """stop→start снимает error и даёт свежую попытку запуска."""
        db, _, stream, spawned, mgr = self._fresh()
        def dead_spawn(rtsp, fps, sid):
            p = FakeProcess(alive=False)
            spawned.append(p)
            return p
        mgr.spawn = dead_spawn
        for _ in range(MAX_FAST_CRASHES + 1):
            mgr.reconcile_once(db)
        assert db.get(Stream, stream.id).status == "error"

        # stop: снимаем с desired → reconciler чистит errored/crashes
        db.get(Stream, stream.id).desired_status = "inactive"
        db.commit()
        mgr.reconcile_once(db)

        # start: снова desired=active, spawn теперь живой → свежая попытка
        db.get(Stream, stream.id).desired_status = "active"
        db.commit()
        def alive_spawn(rtsp, fps, sid):
            p = FakeProcess(alive=True)
            spawned.append(p)
            return p
        mgr.spawn = alive_spawn
        mgr.reconcile_once(db)
        assert db.get(Stream, stream.id).status == "active"
        db.close()
