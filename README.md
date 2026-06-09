# Vision Analytics Platform

Платформа видеоаналитики: загрузка видео / RTSP-стрим → YOLO-детекция → Kafka → PostgreSQL → REST API → Streamlit-дашборд.

## Стек

- **API**: FastAPI, Pydantic, SQLAlchemy
- **CV**: Ultralytics YOLOv8, OpenCV
- **БД**: PostgreSQL, Alembic (миграции)
- **Дашборд**: Streamlit, Plotly
- **Задачи**: Celery, Redis
- **Стриминг**: Kafka (KRaft), confluent-kafka
- **RTSP**: MediaMTX
- **Инфра**: Docker Compose, GPU passthrough, pytest

## Быстрый старт

### Docker

```bash
docker-compose up -d
```

Поднимутся 11 сервисов: db, redis, kafka, api, worker (x2), flower, dashboard, yolo-consumer, detection-consumer, mediamtx.

### Доступ

| Сервис | URL |
|--------|-----|
| API | http://localhost:8000 |
| Swagger | http://localhost:8000/docs |
| Dashboard | http://localhost:8501 |
| Flower (Celery) | http://localhost:5555 |

### Загрузка видео

Через Dashboard (страница 📹 Видео) или через API:

```bash
curl -X POST http://localhost:8000/api/videos/upload \
  -F "file=@video.mp4"
```

Видео обработается автоматически: Celery Worker → YOLO → PostgreSQL.

### RTSP-стриминг (симуляция камерой)

**Шаг 1.** Запустить FFmpeg — пушить видео в MediaMTX как RTSP-стрим:

```bash
ffmpeg -stream_loop -1 -re -i data/test_v.mp4 -c:v copy -an -f rtsp -rtsp_transport tcp rtsp://localhost:8554/mystream
```

- `-stream_loop -1` — зациклить видео
- `-re` — реальная скорость (не быстрее оригинала)
- `-c:v copy` — копировать видео без перекодирования
- `-an` — убрать аудио (YOLO работает с картинками)
- `-rtsp_transport tcp` — использовать TCP (стабильнее с MediaMTX)

**Шаг 2.** Добавить стрим через Dashboard (страница 📡 Стримы):

- Название: `Тестовая камера`
- URL: `rtsp://mediamtx:8554/mystream` (из Docker-сети)
- FPS: 5

**Шаг 3.** Нажать ▶️ Запустить — запустится Frame Producer как subprocess.

Пайплайн: Producer → Kafka (`raw_frames`) → YOLO Consumer → Kafka (`detections`) → Detection Consumer → PostgreSQL.

### Локально (без Docker)

```bash
# Установить зависимости
pip install -r requirements.txt

# Запустить PostgreSQL, Redis, Kafka
# Обновить DATABASE_URL, CELERY_BROKER_URL, KAFKA_BOOTSTRAP_SERVERS

# Миграции
alembic upgrade head

# API
uvicorn app.main:app --reload

# Dashboard
streamlit run dashboard/app.py

# Celery Worker
celery -A app.core.celery_app worker --loglevel=info --concurrency=2 -Q processing

# Kafka Consumers
python -m app.streaming.yolo_consumer yolov8n.pt 0
python -m app.streaming.consumer
```

### Тесты

```bash
pytest tests/ -v

# Только стриминг-тесты
pytest tests/test_streaming.py -v
```

## API Endpoints

### Видео

| Method | Path | Description |
|--------|------|-------------|
| POST | /api/videos/upload | Загрузить видео и запустить обработку |
| GET | /api/videos | Список всех видео |
| GET | /api/videos/{id} | Информация о видео |
| DELETE | /api/videos/{id} | Удалить видео |

### Стримы

| Method | Path | Description |
|--------|------|-------------|
| POST | /api/streams | Создать стрим |
| GET | /api/streams | Список стримов |
| DELETE | /api/streams/{id} | Удалить стрим |
| POST | /api/streams/{id}/start | Запустить обработку |
| POST | /api/streams/{id}/stop | Остановить обработку |
| GET | /api/streams/{id}/detections | Детекции стрима |

### Аналитика

| Method | Path | Description |
|--------|------|-------------|
| GET | /api/analytics/stats | Общая статистика |
| GET | /api/analytics/class-distribution?source=video\|stream | Распределение по классам |
| GET | /api/analytics/timeline?source=video | Детекции по времени видео |
| GET | /api/analytics/stream-activity?minutes=10 | Активность стримов по реальному времени |

## Docker Compose — сеть

| Сервис | Адрес внутри Docker | Адрес с хоста |
|--------|-------------------|---------------|
| PostgreSQL | `db:5432` | `localhost:5432` |
| Redis | `redis:6379` | `localhost:6379` |
| Kafka | `kafka:29092` | `localhost:9092` |
| MediaMTX | `mediamtx:8554` | `localhost:8554` |
| FastAPI | `api:8000` | `localhost:8000` |

## Kafka Topics

| Топик | Retention | Описание |
|-------|-----------|----------|
| `raw_frames` | 1 час | Кадры от Producer (base64 JPEG) |
| `detections` | 24 часа | YOLO-детекции (JSON) |

Kafka — транспорт, не хранилище. Все детекции сохраняются в PostgreSQL.

## Структура проекта

```
├── app/
│   ├── api/routes/          # API endpoints (videos, streams, analytics)
│   ├── core/                # Config, database, celery
│   ├── db/models.py         # SQLAlchemy models (Video, Stream, Detection)
│   ├── schemas/             # Pydantic schemas
│   ├── services/            # Celery tasks (video processing)
│   ├── streaming/           # Kafka pipeline
│   │   ├── topics.py        # Kafka config
│   │   ├── producer.py      # RTSP/video → Kafka
│   │   ├── yolo_consumer.py # Kafka → YOLO → Kafka
│   │   └── consumer.py      # Kafka → PostgreSQL
│   └── main.py              # FastAPI entry point
├── dashboard/
│   ├── app.py               # Streamlit dashboard
│   └── Dockerfile           # Dashboard container
├── alembic/                 # Database migrations
├── tests/                   # Test suite
├── docker-compose.yml       # 11 services
├── Dockerfile               # API + workers
└── requirements.txt
```

## Требования

- Docker + Docker Compose
- NVIDIA GPU + Docker GPU support (для YOLO-инференса)
- FFmpeg (для симуляции RTSP с видео-файла)