"""Общие фикстуры для всего тестового сьюта."""
import os
import sys
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture(autouse=True)
def _no_external_services():
    """
    Герметичность тестов: никакой сети.

    Lifespan FastAPI-приложения (app.main) при каждом старте TestClient
    обращается в MinIO (ensure_bucket + set_stream_frames_ttl). Если MinIO
    недоступен (а в тестах он и не нужен), minio-py выжидает urllib3-ретраи
    — порядка 6 секунд на вызов. Каждый test_api-тест платил эти 6 секунд
    за запуск lifespan: 9 тестов ≈ минута пустого ожидания, а на машине,
    где хост minio не резолвится в DNS (запуск без Docker), — ещё дольше.

    Фикстура autouse: применяется ко ВСЕМ тестам автоматически.
    Если добавишь в lifespan новые внешние вызовы (Kafka, Redis, ...)
    — замокай их здесь же, тесты обязаны оставаться офлайн-безопасными.
    """
    with patch("app.core.storage.ensure_bucket"), \
         patch("app.core.storage.set_stream_frames_ttl"):
        yield
