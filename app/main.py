"""
Главный файл FastAPI-приложения.
"""
from fastapi import FastAPI
from app.api.routes import videos, analytics
from app.core.database import init_db
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app):
    # Startup
    try:
        init_db()
    except Exception:
        pass
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


@app.get("/health")
def health_check():
    """Проверка что сервер жив"""
    return {"status": "ok"}