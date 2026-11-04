"""Тесты для Kafka streaming модуля"""
import json
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.consumers.yolo_frames import YOLOConsumer
from app.core.database import Base
from app.core.frame_envelope import pack
from app.db.models import Stream, StreamDetection
from app.streams.manager import MAX_FAST_CRASHES, StreamManager

# ---- Тесты FrameProducer ----

class TestFrameProducer:
    """Тесты формата сообщений producer"""

    def test_message_format(self):
        """Проверяем что сообщение имеет правильную структуру"""
        # Мокаем Kafka Producer
        with patch("app.streams.producer.Producer") as mock_producer_cls:
            mock_producer = MagicMock()
            mock_producer_cls.return_value = mock_producer

            from app.core.config import settings
            from app.streams.producer import FrameProducer
            FrameProducer("test.mp4", fps=2)

            # Проверяем что Producer создан с правильными параметрами.
            # Сверяемся с settings, а не с хардкодом: .env для локального
            # запуска и для compose задают разные адреса Kafka
            # (localhost:9092 vs kafka:29092) — контракт в том, что адрес
            # ВСЕГДА берётся из настроек
            mock_producer_cls.assert_called_once()
            call_kwargs = mock_producer_cls.call_args[0][0]
            assert call_kwargs["bootstrap.servers"] == settings.kafka_bootstrap_servers

    # ---- Тесты YOLOConsumer ----

class TestYOLOConsumer:
    """Тесты обработки сообщений YOLO consumer (конвейер: декод → батч → watermark-коммит)"""

    def _make_consumer(self):
        """Consumer без __init__ — без Kafka, без YOLO, без сети.
        __new__ создаёт объект, не вызывая конструктор. Обычный трюк
        для тестирования классов с тяжёлой инициализацией."""
        import queue
        import threading
        from collections import defaultdict

        c = YOLOConsumer.__new__(YOLOConsumer)
        c.detector = MagicMock()
        c.producer = MagicMock()
        c.consumer = MagicMock()
        # Атрибуты, которые в бою создаёт __init__ (батчинг + пулы + конвейер)
        c.batch_max = 4
        c.grace_ms = 1
        c.pool = ThreadPoolExecutor(max_workers=2)
        c.io_pool = ThreadPoolExecutor(max_workers=2)
        c.poison_count = 0
        c.consecutive_failures = 0
        c._state_lock = threading.Lock()
        c.inflight_cap = 16
        c._slots = threading.BoundedSemaphore(16)
        c.decoded = queue.Queue(maxsize=16)
        c._inflight = defaultdict(set)
        c._last_seen = {}
        c._closing = False
        return c

    def test_poison_message_is_skipped_not_fatal(self):
        """Ядовитое сообщение (мусор в конверте): _decode_submit не эмитит,
        снимает in-flight, poison учтён, процесс не умирает."""
        consumer = self._make_consumer()

        bad_msg = MagicMock()
        bad_msg.value.return_value = b"not json at all"
        bad_msg.error.return_value = None                  # ВАЖНО: без этого
            # msg.error() вернёт MagicMock — а это truthy, и мы уйдём в ветку
            # "Kafka error: continue", не дойдя до обработки. Классическая ловушка.
        bad_msg.topic.return_value = "raw_frames"
        bad_msg.partition.return_value = 0
        bad_msg.offset.return_value = 42

        consumer._slots.acquire()  # как в run(): слот берётся до submit
        consumer._enter_inflight(bad_msg)
        consumer._decode_submit(bad_msg)

        assert consumer.poison_count == 1
        assert consumer.decoded.empty()          # tombstone'ов больше нет
        assert consumer._inflight[0] == set()    # яд не стопорит watermark

    def _make_bad_msg(self):
        bad_msg = MagicMock()
        bad_msg.value.return_value = pack(
            {"frame_index": 1, "source": "s", "stream_id": 1}, b"fake-jpeg"
        )
        bad_msg.error.return_value = None
        bad_msg.topic.return_value = "raw_frames"
        bad_msg.partition.return_value = 0                 # sorted() по ним
        bad_msg.offset.return_value = 7
        return bad_msg

    def test_sticky_cuda_error_exits_for_restart(self, monkeypatch):
        """Липкая CUDA-ошибка — не poison message: процесс обязан выйти,
        иначе зомби-консумер вечно сжигает offset'ы мёртвым CUDA-контекстом.
        Выход через os._exit (может быть в worker-потоке) — патчим его."""
        from app.consumers import yolo_frames as yf

        consumer = self._make_consumer()
        consumer.detector.detect_batch.side_effect = RuntimeError(
            "CUDA error: unknown error"
        )

        class _Exited(BaseException):
            pass

        monkeypatch.setattr(yf.os, "_exit", lambda code: (_ for _ in ()).throw(_Exited(code)))

        with patch("app.consumers.yolo_frames.cv2.imdecode", return_value=MagicMock()):
            msg = self._make_bad_msg()
            meta, frame = consumer._prepare(msg)
            with pytest.raises(_Exited):
                consumer._infer_and_publish([(msg, meta, frame)])

        # Фатальный выход случается ДО публикации: ничего не отправлено,
        # offset не закоммичен — кадры достанут заново после рестарта.
        assert consumer.poison_count == 0  # это НЕ ядовитое сообщение
        consumer.producer.flush.assert_not_called()
        consumer.consumer.commit.assert_not_called()

    def test_handle_passes_captured_at_through(self):
        """yolo-consumer обязан пронести captured_at сквозняком: инференс
        и очередь не должны сдвигать время детекции."""
        consumer = self._make_consumer()
        consumer.detector.detect_batch.return_value = [[]]

        msg = MagicMock()
        msg.value.return_value = pack({
            "frame_index": 7,
            "source": "rtsp://x",
            "stream_id": 3,
            "captured_at": "2026-10-28T15:00:00+00:00",
        }, b"fake-jpeg")
        msg.error.return_value = None
        msg.partition.return_value = 0
        msg.offset.return_value = 7

        with patch("app.consumers.yolo_frames.cv2.imdecode", return_value=MagicMock()):
            meta, frame = consumer._prepare(msg)
            consumer._infer_and_publish([(msg, meta, frame)])

        produced = json.loads(consumer.producer.produce.call_args.kwargs["value"])
        assert produced["captured_at"] == "2026-10-28T15:00:00+00:00"

    def test_consecutive_failures_trigger_restart(self, monkeypatch):
        """N подряд не-CUDA падений — тоже выход, а не бесконечный skip."""
        from app.consumers import yolo_frames as yf

        consumer = self._make_consumer()

        class _Exited(BaseException):
            pass

        monkeypatch.setattr(yf.os, "_exit", lambda code: (_ for _ in ()).throw(_Exited(code)))

        for i in range(yf.MAX_CONSECUTIVE_FAILURES - 1):
            consumer._mark_failure(ValueError("boom"), "raw_frames", i)

        with pytest.raises(_Exited):
            consumer._mark_failure(ValueError("boom"), "raw_frames", yf.MAX_CONSECUTIVE_FAILURES - 1)
        assert consumer.poison_count == yf.MAX_CONSECUTIVE_FAILURES

# ---- Тесты DetectionConsumer ----

class TestDetectionConsumer:
    """Тесты сохранения детекций в БД"""

    @patch("app.consumers.detections.SessionLocal")
    def test_save_to_db_creates_detection_objects(self, mock_session_cls):
        """Проверяем что _save_to_db создаёт правильные StreamDetection объекты"""
        mock_db = MagicMock()
        mock_session_cls.return_value = mock_db

        from app.consumers.detections import DetectionConsumer
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

        with patch("app.consumers.detections.StreamDetection") as mock_detection_cls:
            consumer._save_to_db(detection)

            # Проверяем что StreamDetection создан с правильными параметрами
            mock_detection_cls.assert_called_once_with(
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

    @patch("app.consumers.detections.SessionLocal")
    def test_save_to_db_uses_captured_at(self, mock_session_cls):
        """captured_at из сообщения становится detected_at (naive UTC),
        а не временем записи — иначе бэклог «омолаживает» старые кадры."""
        from datetime import datetime

        mock_db = MagicMock()
        mock_session_cls.return_value = mock_db

        from app.consumers.detections import DetectionConsumer
        consumer = DetectionConsumer.__new__(DetectionConsumer)

        detection = {
            "stream_id": 1,
            "frame_index": 42,
            "captured_at": "2026-10-28T12:00:00+00:00",
            "objects": [{"class": "person", "confidence": 0.9, "bbox": {"x1": 1.0, "y1": 2.0, "x2": 3.0, "y2": 4.0}}],
        }

        with patch("app.consumers.detections.StreamDetection") as mock_detection_cls:
            consumer._save_to_db(detection)
            kwargs = mock_detection_cls.call_args.kwargs
            assert kwargs["detected_at"] == datetime(2026, 10, 28, 12, 0, 0)

    @patch("app.consumers.detections.SessionLocal")
    def test_save_to_db_without_captured_at_omits_detected_at(self, mock_session_cls):
        """Нет captured_at (старые сообщения) — поле не передаём вовсе,
        чтобы не затереть server_default (now()) явным NULL."""
        mock_db = MagicMock()
        mock_session_cls.return_value = mock_db

        from app.consumers.detections import DetectionConsumer
        consumer = DetectionConsumer.__new__(DetectionConsumer)

        detection = {
            "stream_id": 1,
            "frame_index": 42,
            "objects": [{"class": "person", "confidence": 0.9, "bbox": {"x1": 1.0, "y1": 2.0, "x2": 3.0, "y2": 4.0}}],
        }

        with patch("app.consumers.detections.StreamDetection") as mock_detection_cls:
            consumer._save_to_db(detection)
            assert "detected_at" not in mock_detection_cls.call_args.kwargs

    def test_parse_captured_at_rejects_garbage(self):
        """Битый captured_at → None (fallback на now()), а не падение записи."""
        from app.consumers.detections import _parse_captured_at
        assert _parse_captured_at("не дата") is None
        assert _parse_captured_at(None) is None

    @patch("app.consumers.detections.SessionLocal")
    def test_save_to_db_rollback_on_error(self, mock_session_cls):
        """Проверяем что при ошибке делается rollback"""
        mock_db = MagicMock()
        mock_db.add.side_effect = Exception("DB error")
        mock_session_cls.return_value = mock_db

        from app.consumers.detections import DetectionConsumer
        consumer = DetectionConsumer.__new__(DetectionConsumer)

        detection = {
            "video_id": None,
            "stream_id": 1,
            "frame_index": 42,
            "objects": [{"class": "person", "confidence": 0.95, "bbox": {"x1": 1.0, "y1": 2.0, "x2": 3.0, "y2": 4.0}}],
        }

        with patch("app.consumers.detections.StreamDetection"):
            consumer._save_to_db(detection)

            mock_db.rollback.assert_called_once()
            mock_db.close.assert_called_once()


# ---- Тесты батчевой записи DetectionConsumer -----------------------------

class TestDetectionConsumerBatch:
    """Батчевая запись: динамический сбор, bulk INSERT, один коммит,
    деградация на IntegrityError (и отсутствие двойного учёта метрик)."""

    def _make_consumer(self, batch_max=4, grace_ms=1):
        from app.consumers.detections import DetectionConsumer
        c = DetectionConsumer.__new__(DetectionConsumer)
        c.batch_max = batch_max
        c.grace_ms = grace_ms
        c.consumer = MagicMock()
        c._last_orphan_warn = {}
        return c

    def _msg(self, partition: int, offset: int, payload):
        m = MagicMock()
        m.partition.return_value = partition
        m.offset.return_value = offset
        m.value.return_value = payload if isinstance(payload, bytes) else payload.encode()
        m.error.return_value = None
        m.timestamp.return_value = (0, -1)  # без CreateTime — queue-wait не меряем
        return m

    def _payload(self, stream_id=1, frame_index=0, n_objs=1,
                 captured_at="2026-11-02T12:00:00+00:00"):
        d = {
            "stream_id": stream_id,
            "frame_index": frame_index,
            "objects": [
                {"class": "person", "confidence": 0.9,
                 "bbox": {"x1": 1.0, "y1": 2.0, "x2": 3.0, "y2": 4.0}}
                for _ in range(n_objs)
            ],
        }
        if captured_at is not None:
            d["captured_at"] = captured_at
        return json.dumps(d)

    @staticmethod
    def _stored_offsets(consumer_mock) -> list[int]:
        return [call.kwargs["message"].offset()
                for call in consumer_mock.store_offsets.call_args_list]

    @staticmethod
    def _written_results(written_mock) -> list[str]:
        # result — kwarg ВЫЗОВА labels(); inc() несёт только величину
        return [call.kwargs["result"] for call in written_mock.labels.call_args_list]

    @patch("app.consumers.detections.SessionLocal")
    def test_write_batch_single_bulk_insert_single_commit(self, mock_session_cls):
        """Весь батч — ОДИН bulk INSERT и ОДИН commit; offset'ы — по
        последней message каждой партиции; метрики — только после коммита."""
        mock_db = MagicMock()
        mock_session_cls.return_value = mock_db
        c = self._make_consumer()

        m0 = self._msg(0, 0, self._payload(frame_index=10))
        m1 = self._msg(0, 1, self._payload(frame_index=11, n_objs=2))
        m2 = self._msg(1, 0, self._payload(stream_id=2, frame_index=20, captured_at=None))

        with patch("app.consumers.detections.frame_written_total") as written, \
             patch("app.consumers.detections.frame_e2e_latency_seconds") as e2e, \
             patch("app.consumers.detections.frame_queue_wait_seconds") as qw:
            c._write_batch({0: [m0, m1], 1: [m2]})

        # Разворот объектов в строки: 1 + 2 + 1 = 4, одним вызовом
        args = mock_db.bulk_insert_mappings.call_args.args
        assert args[0] is StreamDetection
        assert len(args[1]) == 4
        assert mock_db.commit.call_count == 1
        # Строка без captured_at не несёт колонку — не затираем server_default
        assert "detected_at" not in args[1][3]
        # Коммит: последние message обеих партиций, один раз
        assert sorted(self._stored_offsets(c.consumer)) == [0, 1]
        assert c.consumer.commit.call_count == 1
        # e2e — только по сообщениям с валидным captured_at (оба — стрим 1)
        assert e2e.labels.call_count == 2
        assert e2e.labels.call_args_list[0].kwargs == {"stream_id": "1"}
        assert sorted(self._written_results(written)) == ["missing_ts", "ok", "ok"]
        qw.assert_not_called()  # CreateTime в фейках отсутствует

    @patch("app.consumers.detections.SessionLocal")
    def test_write_batch_poison_and_skipped_move_offsets_but_not_rows(self, mock_session_cls):
        """Яд и сообщения без stream_id не попадают в INSERT, но их
        offset'ы двигаются — иначе вечный ределивер."""
        mock_db = MagicMock()
        mock_session_cls.return_value = mock_db
        c = self._make_consumer()

        poison = self._msg(0, 0, b"\xff\xfe garbage")
        skipped = self._msg(0, 1, json.dumps({"frame_index": 1}))
        clean = self._msg(0, 2, self._payload(frame_index=2))

        with patch("app.consumers.detections.StreamDetection"), \
             patch("app.consumers.detections.frame_written_total") as written:
            c._write_batch({0: [poison, skipped, clean]})

        rows = mock_db.bulk_insert_mappings.call_args.args[1]
        assert len(rows) == 1
        assert self._stored_offsets(c.consumer) == [2]
        assert sorted(self._written_results(written)) == ["error", "ok", "skipped"]

    @patch("app.consumers.detections.SessionLocal")
    def test_write_batch_integrity_error_degrades_clean_only(self, mock_session_cls, monkeypatch):
        """FK-рыв (стрим удалили) рвёт ВЕСЬ батч — деградация идёт
        по-построчно, но только по «чистым» пейлоадам: яд и skipped уже
        посчитаны на парсинге, второй прогон удвоил бы их метрики."""
        mock_db = MagicMock()
        mock_db.bulk_insert_mappings.side_effect = IntegrityError("stmt", {}, Exception("fk"))
        mock_session_cls.return_value = mock_db
        c = self._make_consumer()

        poison = self._msg(0, 0, b"garbage")
        skipped = self._msg(0, 1, json.dumps({"frame_index": 1}))
        clean_a = self._msg(0, 2, self._payload(frame_index=2))
        clean_b = self._msg(0, 3, self._payload(frame_index=3))

        spy = MagicMock(return_value="ok")
        monkeypatch.setattr(c, "_save_to_db", spy)

        with patch("app.consumers.detections.frame_written_total") as written:
            c._write_batch({0: [poison, skipped, clean_a, clean_b]})

        # ровно два чистых пейлоада, по порядку — без повторов
        assert spy.call_count == 2
        assert [call.args[0]["frame_index"] for call in spy.call_args_list] == [2, 3]
        mock_db.rollback.assert_called_once()
        assert self._stored_offsets(c.consumer) == [3]
        assert sorted(self._written_results(written)) == ["error", "skipped"]

    @patch("app.consumers.detections.SessionLocal")
    def test_write_batch_db_down_counts_errors_and_moves_offsets(self, mock_session_cls):
        """БД легла транзиентно: весь батч — error, но offset'ы двигаются
        (swallow-and-forward, те же семантики, что у построчного режима)."""
        mock_db = MagicMock()
        mock_db.commit.side_effect = RuntimeError("connection lost")
        mock_session_cls.return_value = mock_db
        c = self._make_consumer()

        m0 = self._msg(0, 0, self._payload(frame_index=1))
        m1 = self._msg(0, 1, self._payload(frame_index=2, captured_at=None))

        with patch("app.consumers.detections.frame_written_total") as written:
            c._write_batch({0: [m0, m1]})

        mock_db.rollback.assert_called_once()
        mock_db.close.assert_called_once()
        written.labels.assert_called_once_with(result="error")
        assert written.labels.return_value.inc.call_args.args == (2,)
        assert self._stored_offsets(c.consumer) == [1]

    def test_collect_batch_hot_until_cold_poll(self):
        """Горячая очередь (кэш librdkafka) — собираем батч; как только
        poll замедлился (очередь остыла) — прекращаем сбор и уходим
        обрабатывать то, что собрали."""
        c = self._make_consumer(batch_max=10, grace_ms=50)

        queue = [self._msg(0, i, self._payload(frame_index=i)) for i in range(3)]
        delays = {1: 0.05}  # второй poll «медленный» — очередь остылa
        calls = {"n": 0}

        def poll(timeout=None):
            i = calls["n"]
            calls["n"] += 1
            if not queue:
                return None
            if delays.get(i, 0.0):
                time.sleep(delays[i])
            return queue.pop(0)

        c.consumer.poll.side_effect = poll
        parts = c._collect_batch()
        assert [m.offset() for p in sorted(parts) for m in parts[p]] == [0, 1]
        assert len(queue) == 1  # третье сообщение дожидается следующего батча

    def test_collect_batch_empty_within_grace(self):
        """Пустая тема: не сидим дольше grace-окна."""
        c = self._make_consumer(grace_ms=5)
        c.consumer.poll.return_value = None
        started = time.monotonic()
        assert c._collect_batch() == {}
        assert time.monotonic() - started < 0.5


# ---- Тесты топиков ----

class TestTopics:
    """Тесты конфигурации топиков"""

    def test_topic_names(self):
        """Проверяем имена топиков"""
        from app.core.topics import TOPIC_DETECTIONS, TOPIC_RAW_FRAMES
        assert TOPIC_RAW_FRAMES == "raw_frames"
        assert TOPIC_DETECTIONS == "detections"

    def test_bootstrap_servers(self):
        """Адрес Kafka приходит из настроек (KAFKA_BOOTSTRAP_SERVERS в .env/compose)"""
        from app.core.config import settings
        from app.core.topics import KAFKA_BOOTSTRAP_SERVERS
        assert KAFKA_BOOTSTRAP_SERVERS == settings.kafka_bootstrap_servers


# ---- Тесты StreamManager (reconciler) ----

class FakeProcess:
    """Замена subprocess.Popen для тестов reconciler'а.

    .alive управляет ответом poll(): None = жив, код = мёртв (возвращается .code).
    terminate() помечает убитым. Никаких реальных процессов и сети.
    """
    def __init__(self, alive=True, code=0):
        self.alive = alive
        self.code = code
        self.terminated = False
        self.pid = 4242

    def poll(self):
        return None if self.alive else self.code

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
        """desired=active, процесса нет → reconciler поднимает, status=starting."""
        db, _, stream, spawned, mgr = self._fresh()
        mgr.reconcile_once(db)
        assert len(spawned) == 1
        # starting, а не active: процесс запущен, но ещё не подтверждён живым
        assert db.get(Stream, stream.id).status == "starting"
        db.close()

    def test_confirms_active_after_healthy(self):
        """Пережив порог здоровья, живой producer получает status=active."""
        import time as _time

        from app.streams.manager import HEALTHY_THRESHOLD
        db, _, stream, spawned, mgr = self._fresh()
        mgr.reconcile_once(db)
        assert db.get(Stream, stream.id).status == "starting"
        # перематываем время старта в прошлое — «producer живёт давно»
        mgr._children[stream.id].started_at = _time.monotonic() - (HEALTHY_THRESHOLD + 1)
        mgr.reconcile_once(db)
        assert db.get(Stream, stream.id).status == "active"
        db.close()

    def test_does_not_respawn_live_producer(self):
        """Идемпотентность: живой нужный процесс не трогаем на следующем витке."""
        db, _, stream, spawned, mgr = self._fresh()
        mgr.reconcile_once(db)
        mgr.reconcile_once(db)
        assert len(spawned) == 1  # второй виток ничего не породил

    def test_respawns_after_crash(self):
        """producer умер → на следующем витке поднялся новый (status=starting)."""
        db, _, stream, spawned, mgr = self._fresh()
        mgr.reconcile_once(db)
        assert len(spawned) == 1
        spawned[0].alive = False          # имитируем падение
        mgr.reconcile_once(db)
        assert len(spawned) == 2          # поднят второй
        assert db.get(Stream, stream.id).status == "starting"
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

    def test_clears_stale_status_after_manager_restart(self):
        """Регресс: рестарт менеджера не должен оставлять залипший active.

        Симулируем перезапуск: менеджер с пустой памятью (_children пуст —
        продюсеры умерли вместе со старым процессом), а в БД стрим помечен
        active при desired=inactive. Раньше шаг «убираем лишнее» обходил
        только in-memory множества, такой стрим не замечался, и статус жил
        зелёным навсегда — «стоп не работает» на дашборде.
        """
        db, _, stream, spawned, mgr = self._fresh(desired="inactive")
        db.get(Stream, stream.id).status = "active"  # наследие прошлой жизни
        db.commit()
        mgr.reconcile_once(db)
        assert db.get(Stream, stream.id).status == "inactive"
        assert spawned == []  # и поднимать ничего не должны
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
        # свежий запуск → starting (active подтвердится только пережив порог здоровья)
        assert db.get(Stream, stream.id).status == "starting"
        db.close()

    def test_open_failure_exit_code_backs_off(self):
        """Код выхода EXIT_OPEN_FAILED = краш даже при «длинной» жизни.

        Это чинит исходный баг: producer с недоступным RTSP живёт ~9-10с
        (3 попытки открытия), что раньше попадало в ветку «прожил достаточно» и
        обнуляло счётчик → бесконечный респавн. Теперь код 42 однозначно краш.
        """
        import time as _time

        from app.streams.manager import EXIT_OPEN_FAILED, HEALTHY_THRESHOLD
        db, _, stream, spawned, mgr = self._fresh()

        def open_fail_spawn(rtsp, fps, sid):
            p = FakeProcess(alive=False, code=EXIT_OPEN_FAILED)
            spawned.append(p)
            return p
        mgr.spawn = open_fail_spawn

        for _ in range(MAX_FAST_CRASHES + 2):
            # перематываем время: даже «прожив» дольше порога, смерть с кодом 42
            # обязана считаться крашем
            if stream.id in mgr._children:
                mgr._children[stream.id].started_at = _time.monotonic() - (HEALTHY_THRESHOLD + 5)
            mgr.reconcile_once(db)

        st = db.get(Stream, stream.id)
        assert st.status == "error"
        assert "не открывается" in (st.status_reason or "")
        assert len(spawned) == MAX_FAST_CRASHES
        db.close()
