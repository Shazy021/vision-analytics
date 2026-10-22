"""
Celery-приложение — диспетчер фоновых задач.

Broker (Redis) хранит очередь задач, backend (Redis) — результаты,
воркеры берут задачи из очереди и выполняют.
"""
from celery import Celery

from app.core.config import settings

celery_app = Celery(
    "vision_analytics",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
    include=["app.tasks.processing"],
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    broker_connection_retry_on_startup=True,
)
