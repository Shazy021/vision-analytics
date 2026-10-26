FROM python:3.12-slim AS builder

WORKDIR /app

# torch ставится ПЕРЕД проектом: ultralytics тянет torch как зависимость,
# но возьмёт уже установленный (с индекса cu132), а не CUDA-wheel с PyPI.
RUN pip install --no-cache-dir torch torchvision \
    --index-url https://download.pytorch.org/whl/cu132

# Кэш слоя: копируем ТОЛЬКО манифест зависимостей + заглушку пакета.
# pip install ".[gpu]" построит проект (пустой app) ради зависимостей;
# полный код прилетит ниже слоем COPY . . и затенит site-packages
# (контейнеры работают из /app, а не из site-packages).
# Пока manifest не меняется — слой с 10 ГБ зависимостей не пересобирается.
COPY pyproject.toml ./
COPY app/__init__.py app/
RUN pip install --no-cache-dir ".[gpu]" && \
    pip uninstall -y opencv-python opencv-python-headless && \
    pip install --no-cache-dir opencv-python-headless==4.13.0.92

FROM python:3.12-slim

WORKDIR /app

COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

COPY . .

RUN mkdir -p uploads

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
