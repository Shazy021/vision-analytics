"""Общие фикстуры для всего тестового сьюта."""
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from app.core.database import Base, get_db
from app.main import app


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
    Если добавишь в lifespan новые внешние вызовы (Kafka, ...)
    — замокай их здесь же, тесты обязаны оставаться офлайн-безопасными.
    """
    with patch("app.core.storage.ensure_bucket"), \
         patch("app.core.storage.set_stream_frames_ttl"):
        yield


@pytest.fixture
def client():
    """
    TestClient с подменённой БД (общий для всех API-тестов).

    StaticPool — ВСЕ соединения используют ОДНУ и ту же связь с БД.
    Без него SQLite :memory: для каждого соединения создаёт
    новую пустую базу, и create_all() не помогает.
    """
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,  # ← ключевое отличие!
    )
    Base.metadata.create_all(bind=engine)

    def test_get_db():
        db = Session(bind=engine)
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = test_get_db

    with patch("app.main.init_db"), TestClient(app) as c:
        yield c

    app.dependency_overrides.clear()
    Base.metadata.drop_all(bind=engine)
