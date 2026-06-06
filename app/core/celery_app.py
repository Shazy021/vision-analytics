"""
Celery-приложение — диспетчер фоновых задач.

Celery = Worker + Broker + Backend

- Broker (Redis) — очередь сообщений: "обработай видео #5"
- Backend (Redis) — хранилище результатов: "видео #5 обработано, 108 детекций"
- Worker — процесс, который берёт задачи из очереди и выполняет
"""
from celery import Celery
from app.core.config import settings

celery_app = Celery(
    "vision_analytics",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
)

celery_app.conf.update(
    task_serializer = "json",
    accept_content = ["json"],
    result_serializer = "json",
    timezone = "UTC",
    enable_utc=True,
)

@celery_app.task
def add(x, y):
    """Простая задача для проверки что Celery работает."""
    return x + y

import app.tasks.processing  # noqa: E402, F401