from fastapi import FastAPI

# Создаём экземпляр приложения
# title - 
app = FastAPI(title="Vision Analytics API")

# @app.get — декоратор, регистрирует маршрут
# "/health" — URL путь
# async def — функция-обработчик (handler)
# async значит "можно делать await внутри", но для простых эндпоинтов — не обязательно
@app.get("/health")
async def health():
    return {"status": "ok"}    # FastAPI сам конвертирует dict → JSON