"""
Главный файл FastAPI-приложения.
"""
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.routes import analytics, streams, videos
from app.core.database import init_db

# Настройка логгера для этого модуля
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app):
    # Startup
    try:
        init_db()
    except Exception:
        # БД может быть ещё не готова (например, старт до миграций).
        # Не валим приложение, но громко логируем: таблицы могло не создать.
        logger.exception(
            "[Startup] init_db failed — таблицы не созданы. "
            "Проверь DATABASE_URL и выполни 'alembic upgrade head'."
        )

    # Создаём бакет в MinIO, если его ещё нет
    # + устанавливаем TTL для кадров стримов (автоудаление через N дней)
    try:
        from app.core import storage
        storage.ensure_bucket()
        # Кадры стримов старше 7 дней будут удалены автоматически.
        # Это спасает диск от переполнения: 1 стрим = ~24 GB/день.
        # Видео (videos/) НЕ затрагиваются — они загружены пользователем.
        storage.set_stream_frames_ttl(days=7)
        logger.info("[Startup] MinIO bucket ready + stream frames TTL=7d")
    except Exception as e:
        logger.warning(f"[Startup] MinIO not available yet: {e}")

    yield
    # Shutdown (если нужно — добавляем сюда)

app = FastAPI(
    title="Vision Analytics Platform",
    description="API для видеоаналитики — обнаружение, трекинг, статистика",
    version="0.1.0",
    lifespan=lifespan,
)

# Подключаем роутеры — все эндпоинты из videos.py и analytics.py
# становятся доступны в приложении
app.include_router(videos.router)
app.include_router(analytics.router)
app.include_router(streams.router)


@app.get("/health")
def health_check():
    """Проверка что сервер жив"""
    return {"status": "ok"}
