from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base
from app.core.config import settings

# create_engine — "движок" подключения
# echo=False — не показывать SQL в консоли (поставь True для дебага)
engine = create_engine(settings.database_url, echo=False)

# sessionmaker — фабрика сессий
# Сессия = единица работы с БД: открыл → сделал запросы → закрыл (commit)
# autoflush=False, autocommit=False — контролируем когда данные реально записываются
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

# Base — базовый класс для всех моделей
# Все модели (Video, Detection) наследуются от него
# Он знает про engine и может создавать/удалять таблицы
Base = declarative_base()

# Зависимость для FastAPI — будет создавать сессию на каждый запрос
def get_db():
    """
    Каждому запросу к API — своя сессия с БД.
    
    with next(get_db()) as db:
        video = db.query(Video).first()
    
    После обработки запроса — сессия закрывается автоматически.
    """
    db = SessionLocal()
    try:
        yield db # Отдаём сессию, await не нужен — SQLAlchemy синхронный
    finally:
        db.close() # Закрываем в любом случае — даже если была ошибка


# Для ручной проверки подключения
if __name__ == "__main__":
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        print("DB connected:", result.scalar())