# 📊 Vision Analytics Platform — Карта проекта

## 🎯 Цель проекта
Система обнаружения объектов (YOLO) для:
- **Загружаемых видеофайлов** (MP4, AVI, MOV, MKV)
- **RTSP/IP стримов** в реальном времени

---

## 🏗️ Архитектура (Шаг по шагу)

### **1. Точка входа** → `app/main.py`
```python
FastAPI приложение с роутерами:
├── /api/videos      # CRUD для видеофайлов
├── /api/streams     # Управление RTSP стримами
└── /api/analytics   # Аналитика и статистика
```

### **2. Загрузка видео** → `/api/videos/upload`
```
Клиент (браузер) 
    ↓
FastAPI: upload_video()
    ↓
Создаётся запись Video в БД (статус: "processing")
    ↓
Файл сохраняется временно → загружается в MinIO через multipart upload
    ↓
Celery задача process_video_task.delay(video_id) отсылается клиенту (мгновенный ответ!)
```

### **3. Обработка видео (Celery воркер)**
```
process_video_task(video_id)
    ↓
app/pipeline/processor.process_video()
    ↓
- Читает видео НАПРЯМУЮ из MinIO по HTTP (OpenCV + FFmpeg стриминг)
- НЕ скачивает на диск!
    ↓
- YOLO детекция каждого N-го кадра (по config.extract_fps)
- app/detection/yolo.YOLODetector.detect()
    ↓
- Создание Detection записей в БД:
  - frame_index, timestamp, class_name, bbox, confidence
    ↓
- Сохранение видео в MinIO под ключ videos/{id}.mp4
- Обновление Video.status = "completed" или "partial"
```

### **4. Стримы (Kafka Pipeline)**
```
RTSP URL → app/streaming/producer.FrameProducer
    ↓
Читает RTSP поток OpenCV.VideoCapture
    ↓
Каждые N кадров:
  - Encodes в JPEG + base64
  - Публикует в Kafka TOPIC_RAW_FRAMES
        │
        ├─> app/streaming/yolo_consumer.YOLOConsumer (с YOLO)
        │      ↓
        │      - Детекция объектов
        │      - Сохраняет кадры с детекциями в MinIO: streams/{stream_id}/{index}.jpg
        │      - Публикует в TOPIC_DETECTIONS
        │
        └─> app/streaming/consumer (сохраняет чистые кадры)

TOPIC_DETECTIONS → запись в БД таблицы detections со stream_id
```

### **5. Хранение** → `app/core/storage` (MinIO/S3 клиент)
- **Videos**: `videos/{video_id}.{ext}` — целые видеофайлы
- **Stream frames**: `streams/{stream_id}/{frame_index}.jpg` — кадры стримов (только с детекциями!)
- **TTL**: Кадры старше 7 дней удаляются автоматически

### **6. Аналитика** → `/api/analytics/*`
```
GET /api/analytics/stats
└── Общее количество видео, детекций, средняя уверенность

GET /api/analytics/class-distribution?video_id=1&source=video
└── Пирог: сколько каждого класса (person, car, ...)

GET /api/analytics/timeline?video_id=1&bucket=5.0
└── График детекций по времени (каждые 5 сек)

GET /api/analytics/stream-activity?stream_id=1&minutes=10
└── Активность стрима за последние N минут
```

### **7. Дашборд** → `dashboard/app.py` (Streamlit)
- Страница "📹 Видео": загрузка, список, аналитика по видео
- Страница "📡 Стримы": добавление RTSP, управление стримами, графики
- Кеширование запросов к API (10 сек TTL)

---

## 🗂️ Структура проекта

```
app/
├── main.py                 # FastAPI приложение, точки входа роутеров
├── core/                   # Ядро: конфиг, БД, Celery, MinIO storage
│   ├── config.py          # settings (YOLO весов, устройства, TTL, ...)
│   ├── database.py        # SessionLocal, Base для SQLAlchemy
│   ├── celery_app.py      # Celery app для асинхронных задач
│   └── storage.py         # MinIO клиент: upload/download/presigned_url
├── api/                    # API роутеры
│   └── routes/
│       ├── videos.py      # CRUD видеофайлов (Upload → MinIO → Celery)
│       ├── streams.py     # RTSP стримы (Producer → Kafka → Consumer)
│       └── analytics.py   # Аналитика: stats, class-distribution, timeline
├── detection/
│   └── yolo.py            # YOLODetector обёртка Ultralytics YOLO
├── pipeline/
│   └── processor.py       # Главный процессор видео (читает из MinIO)
├── streaming/             # Kafka пайплайн
│   ├── producer.py        # Отправляет кадры в Kafka (base64 JPEG)
│   ├── consumer.py        # Consumer чистых кадров (опционально)
│   ├── yolo_consumer.py   # YOLO детекция + сохранение в MinIO
│   └── topics.py          # Kafka темы и bootstrap серверы
├── video/
│   └── extractor.py       # VideoCapture обёртка (OpenCV http/source)
├── tasks/
│   └── processing.py      # Celery задача process_video_task
└── db/                    # SQLAlchemy модели
    └── models.py          # Video, Detection, Stream таблицы

tests/                     # Тесты: API, Celery, БД, YOLO
dashboard/                 # Streamlit дашборд
alembic/                   # DB миграции
docker-compose.yml         # Docker сервисы (API + Celery воркеры + Kafka + MinIO)
pyproject.toml             # Зависимости (Poetry)
```

---

## 🔄 Полный поток данных

### **Вариант А: Загружаемое видео**
```
Клиент uploads → /api/videos/upload
    ↓
Запись в БД + файл → MinIO
    ↓
Celery worker запускает процессор
    ↓
Processor читает видео из MinIO (HTTP stream)
    ↓
YOLO детекция кадров
    ↓
Detecions пишутся в БД
    ↓
Video.status = "completed"
    ↓
Клиент видит статус через /api/videos/{id}/status
```

### **Вариант Б: RTSP стрим**
```
1. POST /api/streams → создаёт Stream запись (статус: inactive)
2. POST /api/streams/{id}/start
   ↓
Subprocess запускает producer.py с RTSP URL
   ↓
Producer отправляет кадры в Kafka raw_frames
   ↓
YOLOConsumer читает Kafka, детектирует, сохраняет в MinIO + пишет detections
   ↓
Детекции попадают в БД streams.{id}.detections
   ↓
GET /api/streams/{id}/detections — последний список детекций
```

---

## 🛠️ Ключевые технологии

- **Backend**: FastAPI (Python)
- **Database**: PostgreSQL (SQLAlchemy ORM)
- **Object Storage**: MinIO (S3-compatible API)
- **Message Queue**: Kafka (RawFrames → YOLO detections)
- **Async Tasks**: Celery + Redis/RabbitMQ
- **ML**: Ultralytics YOLOv8/v9/v10
- **Media processing**: OpenCV + FFmpeg
- **Dashboard**: Streamlit

---

## 🔑 Важные детали реализации

### **Почему читаем видео из MinIO через HTTP?**
- Открытые порты (7000) могут быть заблокированы фаерволом
- Celery воркеры в отдельных процессах не видят локальные файлы
- OpenCV поддерживает http:// и https:// источники напрямую

### **Почему кадры стримов НЕ всегда сохраняем?**
- Strим = ephemeral (проходит мимо)
- Храним только кадры с детекциями (не тратить место на пустые кадры)
- TTL=7 дней чтобы не забивать диск

### **Производительность**
- GPU обрабатывает батчи кадров эффективнее
- Celery воркеры forked для CPU/GPU балансировки
- Kafka batch send для скорости

---

## 📝 Примеры использования API

```bash
# Загрузить видео
curl -F "file=@video.mp4" http://localhost:8000/api/videos/upload

# Старт стрима
curl -X POST http://localhost:8000/api/streams/{id}/start \
  -H "rtsp-url: rtsp://cam:554/stream"

# Получить аналитику
curl http://localhost:8000/api/analytics/stats?source=video
```

---

## 🐳 Docker архитектура (docker-compose.yml)

- **api** — FastAPI сервер (порт 7000)
- **celery_worker** — воркеры Celery (можно несколько)
- **celery_flower** — веб UI для мониторинга задач
- **db** — PostgreSQL БД
- **redis** — брокер для Celery + cache
- **kafka** — message broker
- **minio** — object storage (videos + stream frames)
- **traefik** — reverse proxy (если нужно)

---

## ✅ Чеклист при запуске

1. DB миграции: `alembic upgrade head`
2. MinIO bucket создан (auto на старте)
3. Celery воркеры запущены
4. Kafka broker доступен
5. YOLO модель загружена (указана в config weights_path)

---

## 📞 Контактные точки

- **Точка входа**: `app/main.py`
- **Celery задачи**: `app/tasks/processing.py`
- **YOLO детекция**: `app/detection/yolo.py`
- **MinIO storage**: `app/core/storage.py`
- **DB модели**: `app/db/models.py`

---

*Документ актуален на коммит: 7d5f492*