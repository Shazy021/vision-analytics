"""
Тесты для Celery-задач.

Проблема: в тестах не нужен настоящий Redis и воркер.
Решение: CELERY_TASK_ALWAYS_EAGER = True — задачи выполняются
синхронно, в том же процессе, без брокера.
"""
from unittest.mock import MagicMock, patch

import pytest

from app.tasks.processing import process_video_task


@pytest.fixture(autouse=True)
def celery_eager():
    """
    FORCE eager mode — задачи выполняются синхронно в тестах.
    Не нужен Redis, не нужен воркер.
    """
    from app.core.celery_app import celery_app
    celery_app.conf.update(
        task_always_eager=True,
        task_eager_propagates=True,
    )
    yield
    celery_app.conf.update(
        task_always_eager=False,
        task_eager_propagates=False,
    )


@patch("app.pipeline.processor.process_video")
@patch("app.tasks.processing.SessionLocal")
def test_process_video_task_success(mock_session_local, mock_process_video):
    """Задача успешно обрабатывает видео"""
    mock_db = MagicMock()
    mock_session_local.return_value = mock_db

    result = process_video_task(video_id=1)

    assert result["status"] == "completed"
    assert result["video_id"] == 1
    mock_process_video.assert_called_once_with(1, mock_db)
    mock_db.close.assert_called_once()


@patch("app.pipeline.processor.process_video")
@patch("app.tasks.processing.SessionLocal")
def test_process_video_task_failure(mock_session_local, mock_process_video):
    """Задача помечает результат как failed при ошибке"""
    mock_db = MagicMock()
    mock_session_local.return_value = mock_db

    mock_process_video.side_effect = Exception("YOLO crashed")
    with pytest.raises(Exception):
        process_video_task(video_id=2)

    mock_db.close.assert_called_once()


def test_process_video_task_has_queue():
    """Задача привязана к очереди processing"""
    assert process_video_task.queue == "processing"
