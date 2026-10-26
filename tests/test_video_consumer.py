"""
Тесты VideoConsumer — трасса video_requests → process_video.

Как у YOLOConsumer: объект создаём через __new__ без конструктора
(он поднимает Kafka-соединение), consumer подменяем MagicMock'ом.
"""
import json
from unittest.mock import MagicMock, patch

import pytest

from app.consumers.video import VideoConsumer


def _make_consumer():
    c = VideoConsumer.__new__(VideoConsumer)
    c.consumer = MagicMock()
    return c


def _make_msg(video_id=7):
    msg = MagicMock()
    msg.value.return_value = json.dumps({"video_id": video_id}).encode()
    msg.error.return_value = None
    msg.offset.return_value = 3
    return msg


def _poll_once_then_keyboard(msg):
    def fake_poll(timeout):
        if not fake_poll.called:
            fake_poll.called = True
            return msg
        raise KeyboardInterrupt

    fake_poll.called = False
    return fake_poll


class TestVideoConsumerHandle:
    def test_handle_calls_process_video(self):
        c = _make_consumer()
        mock_db = MagicMock()

        with patch("app.consumers.video.SessionLocal", return_value=mock_db), \
             patch("app.consumers.video.process_video") as mock_process:
            c._handle(_make_msg(video_id=42))

        mock_process.assert_called_once_with(42, mock_db)
        mock_db.close.assert_called_once()

    def test_handle_marks_failed_on_error(self):
        """process_video упал ДО пометки (например, видео нет в БД) —
        консьюмер сам ставит failed, иначе видео висело бы в processing."""
        c = _make_consumer()
        mock_db = MagicMock()

        with patch("app.consumers.video.SessionLocal", return_value=mock_db), \
             patch("app.consumers.video.process_video",
                   side_effect=ValueError("Video with id=7 not found")):
            with pytest.raises(ValueError):
                c._handle(_make_msg(video_id=7))

        mock_db.rollback.assert_called_once()
        mock_db.query.return_value.filter.return_value.update.assert_called_once_with(
            {"status": "failed"}, synchronize_session=False,
        )
        mock_db.close.assert_called_once()


class TestVideoConsumerOffsets:
    def test_success_commits_offset(self):
        """at-least-once: offset коммитится ПОСЛЕ успешной обработки."""
        c = _make_consumer()
        msg = _make_msg()
        c.consumer.poll.side_effect = _poll_once_then_keyboard(msg)

        with patch.object(c, "_handle") as mock_handle:
            c.run()

        mock_handle.assert_called_once_with(msg)
        c.consumer.commit.assert_called_once_with(message=msg, asynchronous=False)

    def test_permanent_failure_commits_offset(self):
        """Не-CUDA падение: видео помечен failed в БД, offset коммитим —
        повтор задачи не вылечит битый файл."""
        c = _make_consumer()
        msg = _make_msg()
        c.consumer.poll.side_effect = _poll_once_then_keyboard(msg)

        with patch.object(c, "_handle", side_effect=RuntimeError("corrupt video")):
            c.run()

        c.consumer.commit.assert_called_once_with(message=msg, asynchronous=False)

    def test_fatal_cuda_exits_without_commit(self):
        """Липкая CUDA: выход БЕЗ коммита — после рестарта Kafka доставит
        задачу заново в свежий CUDA-контекст."""
        c = _make_consumer()
        msg = _make_msg()
        c.consumer.poll.side_effect = _poll_once_then_keyboard(msg)

        with patch.object(c, "_handle", side_effect=RuntimeError("CUDA error: unknown")):
            with pytest.raises(SystemExit) as exc_info:
                c.run()

        assert exc_info.value.code == 1
        c.consumer.commit.assert_not_called()
        c.consumer.close.assert_called_once()

    def test_consumer_config_manual_commit(self):
        """Ручной коммит и увеличенный max.poll.interval.ms — контракт
        at-least-once для долгих задач (см. докстринг модуля)."""
        with patch("app.consumers.video.Consumer") as mock_cls:
            VideoConsumer()

        conf = mock_cls.call_args[0][0]
        assert conf["enable.auto.commit"] is False
        assert conf["max.poll.interval.ms"] > 300000
