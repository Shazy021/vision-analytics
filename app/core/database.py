import logging

from sqlalchemy import create_engine, text
from sqlalchemy.orm import declarative_base, sessionmaker

from app.core.config import settings

logger = logging.getLogger(__name__)

# echo=False — не показывать SQL в консоли (поставь True для дебага)
engine = create_engine(settings.database_url, echo=False)

# autoflush/autocommit выключены: когда данные реально записываются,
# контролируют бизнес-код (commit в роутах/задачах), а не ORM.
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

Base = declarative_base()


def get_db():
    """Зависимость для FastAPI: одна сессия на запрос, закрытие в любом случае."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    """Создаёт все таблицы по моделям (без Alembic)."""
    from app.db.models import Detection, Video  # noqa: F401
    Base.metadata.create_all(bind=engine)


# Для ручной проверки подключения
if __name__ == "__main__":
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        logger.info(f"DB connected: {result.scalar()}")
