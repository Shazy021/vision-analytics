"""Тесты observability-обвязки: парсинг временных меток сообщений,
queue-wait из broker timestamp, лейблы результатов и enqueued_at в payload.

Метрики — только счётчики в памяти, GPU/torch/Kafka не нужны.
"""
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from app.core.metrics import parse_epoch_seconds, queue_wait_seconds


class _Msg:
    """Миниатюрная замена confluent_kafka.Message: только timestamp()."""

    def __init__(self, ts):
        self._ts = ts

    def timestamp(self):
        return self._ts


class TestParseEpochSeconds:
    def test_utc_iso(self):
        expected = datetime(2026, 10, 28, 12, 0, tzinfo=timezone.utc).timestamp()
        assert parse_epoch_seconds("2026-10-28T12:00:00+00:00") == expected

    def test_offset_timezones_normalized(self):
        assert parse_epoch_seconds("2026-10-28T15:00:00+03:00") == \
            parse_epoch_seconds("2026-10-28T12:00:00+00:00")

    def test_naive_assumed_utc(self):
        expected = datetime(2026, 10, 28, 12, 0, tzinfo=timezone.utc).timestamp()
        assert parse_epoch_seconds("2026-10-28T12:00:00") == expected

    def test_garbage_and_empty_are_none(self):
        assert parse_epoch_seconds("не дата") is None
        assert parse_epoch_seconds(None) is None
        assert parse_epoch_seconds("") is None


class TestQueueWaitSeconds:
    def test_create_time_measured(self):
        msg = _Msg((1, int((datetime.now(timezone.utc) - timedelta(seconds=2)).timestamp() * 1000)))
        wait = queue_wait_seconds(msg)
        assert wait is not None
        assert 1.5 < wait < 3.0

    def test_no_timestamp_skipped(self):
        assert queue_wait_seconds(_Msg((0, -1))) is None

    def test_future_timestamp_skipped(self):
        """Перекос часов producer'а: отрицательное ожидание — мусор,
        в гистограмме он портит квантили."""
        future = int((datetime.now(timezone.utc) + timedelta(seconds=100)).timestamp() * 1000)
        assert queue_wait_seconds(_Msg((1, future))) is None

    def test_unmockable_msg_survives(self):
        """MagicMock из консьюмерных тестов: timestamp() нераспаковываем —
        helper обязан вернуть None, а не уронить консьюмер."""
        assert queue_wait_seconds(MagicMock()) is None


class TestYoloHandleMetrics:
    def _make_consumer(self):
        from app.consumers.yolo_frames import YOLOConsumer
        c = YOLOConsumer.__new__(YOLOConsumer)
        c.detector = MagicMock()
        c.detector.detect.return_value = []
        c.producer = MagicMock()
        c.consumer = MagicMock()
        return c

    def _make_msg(self, ts=(1, 1)):
        msg = MagicMock()
        msg.value.return_value = json.dumps({
            "frame_data": "e30=",  # валидный base64, imdecode замокан
            "frame_index": 1,
            "source": "rtsp://x",
            "stream_id": 3,
        }).encode()
        msg.timestamp.return_value = ts
        return msg

    def test_counters_and_queue_wait(self):
        c = self._make_consumer()
        ts_ms = int((datetime.now(timezone.utc) - timedelta(seconds=1)).timestamp() * 1000)
        with patch("app.consumers.yolo_frames.cv2.imdecode", return_value=MagicMock()), \
             patch("app.consumers.yolo_frames.frames_received_total") as received, \
             patch("app.consumers.yolo_frames.frame_processed_total") as processed, \
             patch("app.consumers.yolo_frames.frame_queue_wait_seconds") as queue:
            c._handle(self._make_msg(ts=(1, ts_ms)))

        received.inc.assert_called_once()
        processed.labels.assert_called_once_with(result="ok")
        queue.labels.assert_called_once_with(topic="raw_frames")
        observed = queue.labels.return_value.observe.call_args[0][0]
        assert 0.5 < observed < 2.5

    def test_broken_timestamp_does_not_break_handle(self):
        c = self._make_consumer()
        with patch("app.consumers.yolo_frames.cv2.imdecode", return_value=MagicMock()), \
             patch("app.consumers.yolo_frames.frame_queue_wait_seconds") as queue:
            c._handle(self._make_msg(ts=(0, -1)))
        queue.labels.assert_not_called()


class TestDetectionSaveMetrics:
    def _make_consumer(self):
        from app.consumers.detections import DetectionConsumer
        return DetectionConsumer.__new__(DetectionConsumer)

    def _detection(self, **overrides):
        base = {
            "stream_id": 3,
            "frame_index": 42,
            "captured_at": "2026-10-28T12:00:00+00:00",
            "objects": [{"class": "person", "confidence": 0.9,
                         "bbox": {"x1": 1.0, "y1": 2.0, "x2": 3.0, "y2": 4.0}}],
        }
        base.update(overrides)
        return base

    @patch("app.consumers.detections.SessionLocal")
    def test_ok_counts_and_e2e_observed(self, mock_session_cls):
        mock_session_cls.return_value = MagicMock()
        c = self._make_consumer()
        with patch("app.consumers.detections.StreamDetection"), \
             patch("app.consumers.detections.frame_written_total") as written, \
             patch("app.consumers.detections.frame_e2e_latency_seconds") as e2e:
            result = c._save_to_db(self._detection())

        assert result == "ok"
        written.labels.assert_called_once_with(result="ok")
        e2e.labels.assert_called_once_with(stream_id="3")
        # captured_at в прошлом → положительная задержка всей трассы
        assert e2e.labels.return_value.observe.call_args[0][0] > 0

    @patch("app.consumers.detections.SessionLocal")
    def test_missing_captured_at_counts_missing_ts(self, mock_session_cls):
        mock_session_cls.return_value = MagicMock()
        c = self._make_consumer()
        detection = self._detection()
        del detection["captured_at"]
        with patch("app.consumers.detections.StreamDetection"), \
             patch("app.consumers.detections.frame_written_total") as written, \
             patch("app.consumers.detections.frame_e2e_latency_seconds") as e2e:
            result = c._save_to_db(detection)

        assert result == "missing_ts"
        written.labels.assert_called_once_with(result="missing_ts")
        e2e.labels.assert_not_called()  # e2e без валидного времени не мерим

    @patch("app.consumers.detections.SessionLocal")
    def test_broken_captured_at_counts_missing_ts(self, mock_session_cls):
        mock_session_cls.return_value = MagicMock()
        c = self._make_consumer()
        with patch("app.consumers.detections.StreamDetection"), \
             patch("app.consumers.detections.frame_written_total") as written, \
             patch("app.consumers.detections.frame_e2e_latency_seconds") as e2e:
            result = c._save_to_db(self._detection(captured_at="не дата"))

        assert result == "missing_ts"
        written.labels.assert_called_once_with(result="missing_ts")
        e2e.labels.assert_not_called()

    @patch("app.consumers.detections.SessionLocal")
    def test_no_stream_id_skipped(self, mock_session_cls):
        c = self._make_consumer()
        with patch("app.consumers.detections.frame_written_total") as written:
            result = c._save_to_db(self._detection(stream_id=None))
        assert result == "skipped"
        written.labels.assert_called_once_with(result="skipped")

    @patch("app.consumers.detections.SessionLocal")
    def test_db_error_counts_error(self, mock_session_cls):
        mock_db = MagicMock()
        mock_db.add.side_effect = Exception("DB down")
        mock_session_cls.return_value = mock_db
        c = self._make_consumer()
        with patch("app.consumers.detections.StreamDetection"), \
             patch("app.consumers.detections.frame_written_total") as written:
            result = c._save_to_db(self._detection())
        assert result == "error"
        written.labels.assert_called_once_with(result="error")


class TestVideoQueueWait:
    def _make_consumer(self):
        from app.consumers.video import VideoConsumer
        c = VideoConsumer.__new__(VideoConsumer)
        c.consumer = MagicMock()
        return c

    def _poll_once_then_keyboard(self, msg):
        def fake_poll(timeout):
            if not fake_poll.called:
                fake_poll.called = True
                return msg
            raise KeyboardInterrupt

        fake_poll.called = False
        return fake_poll

    def _msg(self, payload):
        msg = MagicMock()
        msg.value.return_value = json.dumps(payload).encode()
        msg.error.return_value = None
        msg.offset.return_value = 1
        return msg

    def test_valid_enqueued_at_observed(self):
        c = self._make_consumer()
        enqueued = datetime.now(timezone.utc) - timedelta(seconds=2)
        with patch("app.consumers.video.frame_task_queue_wait_seconds") as wait:
            c._observe_queue_wait({"video_id": 5, "enqueued_at": enqueued.isoformat()})

        observed = wait.observe.call_args[0][0]
        assert 1.5 < observed < 3.0

    def test_missing_or_broken_enqueued_at_skipped(self):
        c = self._make_consumer()
        with patch("app.consumers.video.frame_task_queue_wait_seconds") as wait:
            c._observe_queue_wait({"video_id": 5})
            c._observe_queue_wait({"video_id": 5, "enqueued_at": "мусор"})
            c._observe_queue_wait({"video_id": 5, "enqueued_at": ""})
        wait.observe.assert_not_called()

    def test_completed_and_failed_counted(self):
        c = self._make_consumer()
        msg = self._msg({"video_id": 5})
        c.consumer.poll.side_effect = self._poll_once_then_keyboard(msg)
        with patch.object(c, "_handle"), \
             patch("app.consumers.video.frame_task_completed_total") as done:
            c.run()
        done.labels.assert_called_once_with(status="completed")

        c = self._make_consumer()
        msg = self._msg({"video_id": 5})
        c.consumer.poll.side_effect = self._poll_once_then_keyboard(msg)
        with patch.object(c, "_handle", side_effect=RuntimeError("corrupt")), \
             patch("app.consumers.video.frame_task_completed_total") as done:
            c.run()
        done.labels.assert_called_once_with(status="failed")

    def test_fatal_cuda_not_counted(self):
        """CUDA-выход — не терминальный failed: задача будет переобработана."""
        c = self._make_consumer()
        msg = self._msg({"video_id": 5})
        c.consumer.poll.side_effect = self._poll_once_then_keyboard(msg)
        with patch.object(c, "_handle", side_effect=RuntimeError("CUDA error: unknown")), \
             patch("app.consumers.video.frame_task_completed_total") as done:
            with pytest.raises(SystemExit):
                c.run()
        done.labels.assert_not_called()

    def test_task_duration_observed_on_success_and_failure(self):
        """Полная длительность задачи пишется и на успехе, и на падении —
        упавшая после 3 минут работы задача тоже стоит того в квантилях."""
        c = self._make_consumer()
        with patch("app.consumers.video.SessionLocal", return_value=MagicMock()), \
             patch("app.consumers.video.process_video"), \
             patch("app.consumers.video.frame_task_duration_seconds") as dur:
            c._handle(self._msg({"video_id": 5}))
        assert dur.observe.call_args[0][0] >= 0

        c = self._make_consumer()
        with patch("app.consumers.video.SessionLocal", return_value=MagicMock()), \
             patch("app.consumers.video.process_video", side_effect=RuntimeError("boom")), \
             patch("app.consumers.video.frame_task_duration_seconds") as dur:
            with pytest.raises(RuntimeError):
                c._handle(self._msg({"video_id": 5}))
        assert dur.observe.call_args[0][0] >= 0


class TestApiEnqueuedAt:
    def test_upload_payload_carries_valid_enqueued_at(self, client):
        with patch("app.core.storage.upload_file"), \
             patch("app.api.routes.videos._get_kafka_producer") as mock_get:
            producer = MagicMock()
            mock_get.return_value = producer
            response = client.post(
                "/api/videos/upload",
                files={"file": ("clip.mp4", b"0" * 100, "video/mp4")},
            )

        assert response.status_code == 201
        payload = json.loads(producer.produce.call_args[1]["value"])
        enqueued = datetime.fromisoformat(payload["enqueued_at"])
        assert enqueued.tzinfo is not None  # UTC ISO с явным смещением
        drift = abs((datetime.now(timezone.utc) - enqueued).total_seconds())
        assert drift < 60


class TestChunkTimings:
    def test_process_video_chunk_observes_decode_inference_write(self, tmp_path):
        """Чанк синтетического видео: decode/inference/write-гистограммы
        получают ненулевые наблюдения."""
        import cv2
        import numpy as np
        from sqlalchemy import create_engine
        from sqlalchemy.orm import Session
        from sqlalchemy.pool import StaticPool

        from app.core.database import Base
        from app.db.models import Video
        from app.processing import processor

        path = str(tmp_path / "two.mp4")
        writer = cv2.VideoWriter(path, cv2.VideoWriter_fourcc(*"mp4v"), 30.0, (64, 64))
        for _ in range(60):  # 2 секунды
            writer.write(np.zeros((64, 64, 3), dtype=np.uint8))
        writer.release()

        engine = create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Base.metadata.create_all(bind=engine)
        db = Session(bind=engine)
        video = Video(
            filename="two.mp4", filepath=path, status="processing",
            duration=2.0, fps=30, extract_fps=5, max_frames=0,
            chunks_total=1, chunks_done=0,
        )
        db.add(video)
        db.commit()

        detector = MagicMock()
        detector.detect.return_value = []
        try:
            with patch.object(processor, "YOLODetector", return_value=detector), \
                 patch("app.processing.processor.frame_chunk_decode_duration_seconds") as dec, \
                 patch("app.processing.processor.frame_chunk_inference_duration_seconds") as inf, \
                 patch("app.processing.processor.frame_chunk_write_duration_seconds") as wr:
                processor.process_video_chunk(video.id, 0.0, 2.0, db)

            assert dec.observe.call_args[0][0] > 0
            assert inf.observe.call_args[0][0] > 0
            assert wr.observe.call_args[0][0] >= 0
        finally:
            db.close()
            Base.metadata.drop_all(bind=engine)
