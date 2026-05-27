from sqlalchemy import create_engine, text
from app.core.config import settings

# create_engine — создаёт "движок" подключения
# Он не подключается сразу, а ждёт первого запроса
# echo=True — показывает SQL-запросы в консоли (удобно для дебага, потом уберём)
engine = create_engine(settings.database_url, echo=False)

if __name__ == "__main__":
    # text() — оборачивает сырой SQL в объект, который понимает SQLAlchemy
    # Нельзя просто conn.execute("SELECT 1") — нужен text()

    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        print("DB connected:", result.scalar()) # scalar() = первое значение первой строки