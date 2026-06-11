"""
Главный файл FastAPI-приложения.
"""
from fastapi import FastAPI
from app.api.routes import videos, analytics, streams
from app.core.database import init_db
from contextlib import asynccontextmanager


@asynccontextmanager
async def lifespan(app):
    # Startup
    try:
        init_db()
    except Exception:
        pass

    # Создаём бакет в MinIO, если его ещё нет
    # + устанавливаем TTL для кадров стримов (автоудаление через N дней)
    try:
        from app.core import storage
        storage.ensure_bucket()
        # Кадры стримов старше 7 дней будут удалены автоматически.
        # Это спасает диск от переполнения: 1 стрим = ~24 GB/день.
        # Видео (videos/) НЕ затрагиваются — они загружены пользователем.
        storage.set_stream_frames_ttl(days=7)
        print("[Startup] MinIO bucket ready + stream frames TTL=7d")
    except Exception as e:
        print(f"[Startup] MinIO not available yet: {e}")

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