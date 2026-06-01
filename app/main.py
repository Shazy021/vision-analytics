"""
Главный файл FastAPI-приложения.
"""
from fastapi import FastAPI
from app.api.routes import videos, analytics
from app.core.database import init_db

app = FastAPI(
    title="Vision Analytics Platform",
    description="API для видеоаналитики — обнаружение, трекинг, статистика",
    version="0.1.0",
)

init_db()

# Подключаем роутеры — все эндпоинты из videos.py и analytics.py
# становятся доступны в приложении
app.include_router(videos.router)
app.include_router(analytics.router)


@app.get("/health")
def health_check():
    """Проверка что сервер жив"""
    return {"status": "ok"}