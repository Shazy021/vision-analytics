# Vision Analytics Platform — Архитектура

## Цель

Система обнаружения объектов (YOLO) для:
- **Загружаемых видеофайлов** (MP4, AVI, MOV, MKV)
- **RTSP/IP стримов** в реальном времени

---

## Обзор архитектуры

### 1. Точка входа — `app/main.py`

```python
FastAPI приложение с роутерами:
├── /api/videos      # CRUD для видеофайлов
├── /api/streams     # Управление RTSP стримами
└── /api/analytics   # Агрегированная статистика
```

События жизненного цикла (lifespan):
- `init_db()` — создать таблицы, если отсутствуют (не критично; предпочтителен Alembic)
- `storage.ensure_bucket()` — создать бакет MinIO
- `storage.set_stream_frames_ttl(days=7)` — автоудаление кадров стримов старше 7 дней

---

### 2. Загрузка видео — `POST /api/videos/upload`

```
Клиент (браузер / дашборд)
    |
FastAPI: upload_video()
    |
1. Создаётся запись Video в БД (статус: "processing")
2. Файл сохраняется во временную папку -> загружается в MinIO через fput_object (multipart)
3. Временный файл удаляется
4. Отправляется Celery задача: process_video_task.delay(video_id)
5. Возврат 201 мгновенно (клиент опрашивает /api/videos/{id}/status)
```

---

### 3. Обработка видео (Celery Worker)

```
process_video_task(video_id)
    |
app.pipeline.processor.process_video()
    |
- Читает видео НАПРЯМУЮ из MinIO по HTTP (OpenCV + FFmpeg streaming)
- НЕ скачивает на диск
    |
- YOLO детекция каждого N-го кадра (управляется settings.extract_fps)
- app.detection.yolo.YOLODetector.detect()
    |
- Создаются записи Detection в БД:
  frame_index, timestamp, class_name, bbox, confidence
    |
- Обновляется Video.status = "completed" | "partial" | "failed"
- Обновляются Video.duration, Video.fps
```

Примечание: файл видео уже находится в MinIO со шага 2. Процессор НЕ
пере-загружает его. Он читает через presigned URL.

---

### 4. Стримы (Kafka Pipeline)

```
RTSP URL -> app.streaming.producer.FrameProducer (subprocess, запускается stream_manager)
    |
Читает RTSP через OpenCV.VideoCapture (принудительный TCP-транспорт в Docker)
    |
Каждые N кадров:
  - Кодирование в JPEG + base64
  - Публикация в Kafka топик "raw_frames"
        |
        +--> app.streaming.yolo_consumer.YOLOConsumer
        |      - Декодирует кадр, выполняет YOLO инференс
        |      - Сохраняет кадры С детекциями в MinIO: streams/{stream_id}/{index}.jpg
        |      - Публикует детекции в Kafka топик "detections"
        |
        +--> (второй consumer на raw_frames отсутствует)

Kafka топик "detections"
        |
        +--> app.streaming.consumer.DetectionConsumer
               - Читает детекции из Kafka
               - Записывает строки Detection в PostgreSQL (со stream_id)
```

Жизненный цикл стрима (паттерн desired-state + reconciler):
1. `POST /api/streams` — создать запись Stream (`desired_status`: inactive)
2. `POST /api/streams/{id}/start` — записать `desired_status=active` (API НИЧЕГО не спавнит)
3. `POST /api/streams/{id}/stop` — записать `desired_status=inactive`
4. `DELETE /api/streams/{id}` — удалить запись + удалить кадры из MinIO

API — только «заказчик»: пишет намерение (`desired_status`) в БД. Реальными
процессами producer'а владеет отдельный сервис `stream_manager`
(`app/streaming/manager.py`) — см. § «Stream supervision» ниже.

Нумерация кадров (`frame_index`) сквозная: при старте producer продолжает
счёт от `max(frame_index)` из БД (`FrameProducer._initial_frame_index()`).
Без этого рестарт стрима обнулял счётчик, новые кадры перезаписывали старые
ключи `streams/{id}/{index}.jpg`, и `/latest-frame` (берёт `max()` по ключам)
навсегда залипал на старом максимуме — превью «стояло на одном кадре».

### Stream supervision (desired-state + reconciler)

Раньше API сам спавнил producer'а и держал живые `Popen` в словаре в памяти
(`_running_processes`). Отсюда три бага: рестарт API убивал все стримы,
упавший producer не поднимался, а два экземпляра API дрались за один стрим.

Теперь разделили НАМЕРЕНИЕ и ФАКТ (как контроллеры Kubernetes):
- `Stream.desired_status` (active/inactive) — намерение, пишет только API;
- `Stream.status` (active/inactive/error) — факт, пишет только `stream_manager`;
- `Stream.status_reason` — диагноз при `status=error`.

`stream_manager` крутит reconcile loop: каждый виток читает desired из БД,
сравнивает со своими живыми дочерними процессами и сближает их — недостающих
поднимает, лишних убивает. Виток идемпотентен (живых не трогает), поэтому
рестарт API безразличен стримам, а упавший producer поднимается сам.

Backoff от spawn-шторма: если producer умер раньше `CRASH_WINDOW` (10с) после
старта, это краш; после `MAX_FAST_CRASHES` (3) быстрых падений подряд стрим
получает `status=error` с причиной и больше не поднимается, пока пользователь
не сделает `stop` → `start` (это снимает стрим с desired и очищает состояние).

---

### 5. Хранилище — `app/core/storage.py` (MinIO / S3)

| Префикс | Содержимое | TTL |
|---------|-----------|-----|
| `videos/{id}.{ext}` | Исходные загруженные видео | Отсутствует (управляется пользователем) |
| `streams/{id}/{frame}.jpg` | Кадры стримов с детекциями | 7 дней (lifecycle rule) |

Все параметры берутся из `app.core.config.settings` (переменные окружения / `.env`).

#### Presigned URL: внутренний и публичный endpoint

У presigned ссылок два потребителя с разной сетевой видимостью:

| Потребитель | Endpoint | Пример |
|-------------|----------|--------|
| Серверная часть (cv2-seek в api, дашборд из контейнера) | `S3_ENDPOINT` | `http://minio:9000/...` |
| Браузер (видео-плеер дашборда) | `S3_PUBLIC_ENDPOINT` | `http://localhost:9000/...` |

Браузер не резолвит docker-имена (`minio:9000`), поэтому ссылки для него
подписываются отдельным Minio-клиентом на публичном адресе. Хост участвует в
подписи SigV4 — подменить его в готовой ссылке нельзя, только подписать
заранее. Region задаётся явно (`us-east-1`): иначе minio-py перед presign'ом
ходит на `GetBucketLocation` к своему endpoint — а до `localhost:9000` из
контейнера доступа нет.

---

### 6. Аналитика — `app/api/routes/analytics.py`

| Эндпоинт | Описание |
|----------|----------|
| `GET /api/analytics/stats` | Общее число видео, детекций, средняя уверенность |
| `GET /api/analytics/class-distribution` | Данные для круговой диаграммы: количество по классам |
| `GET /api/analytics/timeline` | Детекции по времени (бакеты по секундам) |
| `GET /api/analytics/stream-activity` | Детекции в минуту для стримов |

Все поддерживают фильтр `source=video|stream` и фильтры `video_id` / `stream_id`.

---

### 7. Дашборд — `dashboard/app.py` (Streamlit)

- **Страница "Видео"**: форма загрузки, список видео, круговая диаграмма классов,
  столбчатый график по времени + **синхронный двойной плеер**: два окна
  (чистое видео / те же кадры с боксами) на одном общем таймлайне.
  Реализован JS-компонентом (`streamlit.components.v1.html`,
  `SYNC_PLAYER_TEMPLATE`): все детекции видео инжектятся в HTML как JSON,
  ближайший обработанный кадр ищётся бинарным поиском по `currentTime`,
  боксы рисуются на canvas-оверлее. Синхронизация плееров клиентская —
  Streamlit не может дёргать сервер на каждый кадр.
- **Страница "Стримы"**: добавление/запуск/остановка/удаление RTSP, live-превью
  (поллинг `/latest-frame` с настраиваемым интервалом 2–10 с), график активности,
  последние детекции.
- Запросы к API кэшируются с TTL 10 сек (`@st.cache_data`)
- Автообновление страницы стримов: `st.rerun()` — строго в конце страницы
  (rerun обрывает выполнение скрипта, и секции ниже него не отрисовываются)

---

## Модель данных

Три таблицы PostgreSQL (`app/db/models.py`), версии накатываются Alembic.

### `videos`
| Колонка | Тип | Примечание |
|---------|-----|-----------|
| `id` | int PK | |
| `filename` | varchar(255) | |
| `filepath` | varchar(512) | локальный путь (опц.) |
| `object_key` | varchar(512) | ключ в MinIO, напр. `videos/5.mp4` |
| `duration` | float | секунды |
| `fps` | int | |
| `resolution` | varchar(20) | напр. `1920x1080` |
| `uploaded_at` | datetime | server default `now()` |
| `status` | varchar(20) | `processing` → `completed` / `partial` / `failed` |
| `task_id` | varchar(255) | id Celery-задачи |

### `detections`
| Колонка | Тип | Примечание |
|---------|-----|-----------|
| `id` | int PK | |
| `video_id` | int FK → `videos.id` | NULL для детекций стрима |
| `stream_id` | int FK → `streams.id` | NULL для детекций видео |
| `frame_index` | int | ключ кадра в MinIO |
| `class_name` | varchar(50) | |
| `confidence` | float | |
| `bbox_x1/y1/x2/y2` | float | ограничивающий бокс |
| `timestamp` | float | секунда внутри видеофайла |
| `detected_at` | datetime | реальное время; валидно и для видео, и для стримов |

Детекция относится ИЛИ к видео, ИЛИ к стриму — второй FK равен NULL.
Индексы: `video_id`, `stream_id`, `class_name`, `detected_at`.

### `streams`
| Колонка | Тип | Примечание |
|---------|-----|-----------|
| `id` | int PK | |
| `name` | varchar(255) | |
| `rtsp_url` | varchar(512) | |
| `desired_status` | varchar(20) | **намерение** — `active`/`inactive`; пишет только API |
| `status` | varchar(20) | **факт** — `active`/`inactive`/`error`; пишет только `stream_manager` |
| `status_reason` | varchar(255) | диагноз при `status=error` |
| `fps` | int | частота захвата |
| `created_at` | datetime | server default `now()` |
| `pid` | int | легаси — pid producer'а; больше не поддерживается (см. § Stream supervision) |

Разделение `desired_status` (намерение) vs `status` (факт) — ядро паттерна
desired-state + reconciler: намерением владеет API, наблюдаемым фактом —
`stream_manager`. Оба поля лежат в БД, поэтому переживают рестарт и API, и менеджера.

---

## Структура проекта

```
app/
├── main.py                     # FastAPI приложение, lifespan, регистрация роутеров
├── core/
│   ├── config.py               # pydantic-settings: БД, Kafka, S3, YOLO, Celery
│   ├── database.py             # SQLAlchemy engine, SessionLocal, Base, get_db
│   ├── celery_app.py           # Celery приложение (broker=Redis, backend=Redis)
│   └── storage.py              # MinIO клиент: upload, download, presigned, TTL
├── api/
│   └── routes/
│       ├── videos.py           # CRUD: upload, list, get, status, frame, delete
│       ├── streams.py          # CRUD: create, start, stop, delete, detections, frames
│       └── analytics.py        # stats, class-distribution, timeline, stream-activity
├── schemas/
│   ├── video.py                # VideoBase, VideoCreate, VideoRead (Pydantic v2)
│   └── detection.py            # DetectionBase, DetectionRead
├── detection/
│   └── yolo.py                 # YOLODetector обёртка (Ultralytics YOLOv8)
├── pipeline/
│   └── processor.py            # process_video(): чтение из MinIO -> YOLO -> БД
├── video/
│   └── extractor.py            # VideoCapture context manager, Frame dataclass
├── streaming/
│   ├── topics.py               # Имена Kafka топиков, bootstrap серверы
│   ├── producer.py             # FrameProducer: RTSP/файл -> Kafka raw_frames
│   ├── yolo_consumer.py        # YOLOConsumer: raw_frames -> YOLO -> detections + MinIO
│   └── consumer.py             # DetectionConsumer: detections -> PostgreSQL
├── tasks/
│   └── processing.py           # Celery задача: process_video_task
└── db/
    └── models.py               # SQLAlchemy: Video, Detection, Stream

alembic/                        # Миграции БД (alembic upgrade head)
tests/                          # pytest: API, Celery, YOLO, extractor, streaming
dashboard/                      # Streamlit приложение
.github/workflows/              # CI (lint + test), Docker build
docker-compose.yml              # Все сервисы (режим CPU)
docker-compose.gpu.yml          # GPU overrides (NVIDIA)
Dockerfile                      # CUDA образ (worker, yolo-consumer)
Dockerfile.cpu                  # CPU образ (api, flower, detection-consumer, dashboard)
Makefile                        # make up / make down / make test / make lint
pyproject.toml                  # Конфигурация pytest, ruff, alembic
requirements.txt                # Полный набор зависимостей (torch, ultralytics, cv2, ...)
requirements.cpu.txt            # Зависимости только для CPU (без torch)
requirements-dev.txt            # pytest, ruff, pre-commit
```

---

## Поток данных

### Путь А: Загружаемое видео

```
Клиент -> POST /api/videos/upload
    |
    v
FastAPI: создать запись Video -> загрузить файл в MinIO -> отправить Celery задачу
    |
    v
Celery Worker: process_video()
    |  читает видео из MinIO по HTTP (presigned URL)
    |  YOLO детекция каждого N-го кадра
    |  запись строк Detection в PostgreSQL
    |  обновление Video.status
    v
Клиент опрашивает GET /api/videos/{id}/status -> "completed"
```

### Путь Б: RTSP стрим

```
1. POST /api/streams           -> создать запись Stream (desired inactive)
2. POST /api/streams/{id}/start -> desired_status=active; stream_manager поднимет subprocess
    |
    v
FrameProducer: читает RTSP -> кодирует JPEG -> публикует в Kafka "raw_frames"
    |
    v
YOLOConsumer: потребляет "raw_frames" -> YOLO -> публикует "detections"
    |                                  -> сохраняет кадр в MinIO (если есть детекции)
    v
DetectionConsumer: потребляет "detections" -> записывает строки Detection в PostgreSQL
    |
    v
Дашборд: GET /api/streams/{id}/detections + /api/analytics/stream-activity
```

---

## Ключевые технологии

| Слой | Технология |
|------|-----------|
| API | FastAPI (Python 3.12) |
| База данных | PostgreSQL 16 (SQLAlchemy ORM) |
| Миграции | Alembic |
| Объектное хранилище | MinIO (S3-совместимое) |
| Очередь сообщений | Kafka (режим KRaft, без ZooKeeper) |
| Асинхронные задачи | Celery + Redis (broker + result backend) |
| ML | Ultralytics YOLOv8 |
| Медиа | OpenCV + FFmpeg |
| Дашборд | Streamlit + Plotly |
| Линтинг | Ruff |
| CI | GitHub Actions |

---

## Docker архитектура

Два образа для минимизации потребления GPU-памяти:

| Образ | Dockerfile | Сервисы | Содержимое |
|-------|-----------|---------|-----------|
| `vap-cpu` | `Dockerfile.cpu` | api, flower, detection-consumer, dashboard | Без torch (~2 ГБ) |
| `vap-cuda` | `Dockerfile` | worker, yolo-consumer | torch + CUDA (~10-11 ГБ) |

Сервисы в `docker-compose.yml`:

| Сервис | Образ | Порт | Назначение |
|--------|-------|------|-----------|
| `db` | postgres:16-alpine | 5432 | База данных |
| `redis` | redis:7-alpine | 6379 | Celery broker + backend |
| `api` | vap-cpu | 8000 | FastAPI сервер |
| `worker` | vap-cuda | — | Celery worker (replicas: 2, pool: solo) |
| `flower` | vap-cpu | 5555 | Веб-интерфейс мониторинга Celery |
| `dashboard` | vap-cpu | 8501 | Streamlit дашборд |
| `kafka` | cp-kafka:7.6.0 | 9092 | Kafka брокер (KRaft) |
| `kafka-init` | cp-kafka:7.6.0 | — | Одноразовый: создание топиков |
| `yolo-consumer` | vap-cuda | — | Kafka -> YOLO -> детекции |
| `detection-consumer` | vap-cpu | — | Kafka -> PostgreSQL |
| `stream-manager` | vap-cpu | — | Reconciler: владеет процессами producer'ов |
| `mediamtx` | bluenviron/mediamtx | 8554 | RTSP сервер (симуляция камеры) |
| `minio` | minio/minio | 9000, 9001 | Объектное хранилище + веб-консоль |

GPU: используй override-файл `docker-compose.gpu.yml` или `make up`
(автоопределение через `nvidia-smi`).

Celery workers используют `--pool=solo`, потому что CUDA-контекст не переживает
`fork()`. Параллелизм достигается через `deploy.replicas: 2` (независимые контейнеры).

---

## Детали реализации

### Почему читаем видео из MinIO по HTTP?
- Celery workers работают в отдельных контейнерах — общая файловая система недоступна
- OpenCV (backend FFmpeg) поддерживает источники `http://` нативно
- Избегает скачивания 7 ГБ файлов на диск worker'а

### Почему сохраняем только кадры стримов с детекциями?
- Стримы эфемерны — пустые кадры не имеют ценности
- 1 стрим ≈ 24 ГБ/день при 5 FPS; фильтрация экономит ~80% хранилища
- Lifecycle rule автоматически удаляет кадры старше 7 дней

### Почему producer'ами владеет отдельный stream_manager (reconciler)?
- Producer — это бесконечный блокирующий цикл (читает RTSP постоянно), не может
  работать внутри async event loop FastAPI → отдельный процесс
- Владелец процессов вынесен из API: рестарт/масштабирование API не роняет и не
  дублирует стримы (раньше `Popen` жили в памяти api и умирали вместе с ним)
- Один владелец = одна истина: два экземпляра API не дерутся за один стрим
- reconcile loop поднимает упавшего producer'а сам; backoff (см. § Stream
  supervision) не даёт устроить spawn-шторм на битом RTSP URL
- Логи producer'а теперь в `docker compose logs stream-manager` (там, где
  владелец процесса), а не в логах api

### Почему producer продолжает frame_index из БД?
- `frame_index` — это ключ кадра в MinIO (`streams/{id}/{index}.jpg`),
  а `/latest-frame` выбирает последний кадр как `max()` по этим ключам
- Обнуление счётчика при рестарте стрима заставляло новые кадры перезаписывать
  старые ключи: `max()` залипал на значении первого запуска, превью в дашборде
  «стояло на одном кадре», хотя детекции в БД продолжали капать
- Старт с `max(frame_index)` из БД безопасен: кадры сохраняются только при
  наличии детекций, поэтому на каждый сохранённый кадр есть строка Detection

### Почему превью видео — JS-компонент, а не st.video + st.slider?
- Нужны ДВА синхронных плеера (чистый / с боксами) на одном таймлайне —
  Streamlit-виджеты общаются с сервером через rerun, на каждый кадр таймлайна
  это неприемлемая задержка
- Боксы рисуются canvas-оверлеем поверх второго плеера по `currentTime`
  из уже инжектнутого JSON — ноль запросов к серверу во время воспроизведения
- Синхронизация правого («ведомого») плеера — drift-коррекция раз в 0.5 с

### Примечания по производительности
- GPU обрабатывает батчи эффективнее, чем одиночные кадры
  (`detect_batch()` присутствует в коде, но пока не подключён к пайплайну)
- Kafka `message.max.bytes`: 5 МБ (JPEG кадры в base64)
- Kafka retention: `raw_frames` = 1 час, `detections` = 24 часа
- MinIO upload: `fput_object` (multipart) для видео, `put_object` для кадров

---

## Примеры API

```bash
# Загрузить видео
curl -F "file=@video.mp4" http://localhost:8000/api/videos/upload

# Проверить статус обработки
curl http://localhost:8000/api/videos/1/status

# Создать стрим
curl -X POST http://localhost:8000/api/streams \
  -H "Content-Type: application/json" \
  -d '{"name":"Camera 1","rtsp_url":"rtsp://mediamtx:8554/mystream","fps":5}'

# Запустить стрим
curl -X POST http://localhost:8000/api/streams/1/start

# Получить аналитику
curl "http://localhost:8000/api/analytics/stats?source=video"
curl "http://localhost:8000/api/analytics/class-distribution?video_id=1"
```

---

## Чеклист при запуске

1. `cp .env.example .env` — задать учётные данные при необходимости
2. `docker compose up -d` (или `make up` для автоопределения GPU)
3. `alembic upgrade head` — выполнить миграции (или положиться на `init_db()` при старте)
4. Проверить работоспособность: `curl http://localhost:8000/health`
5. Открыть дашборд: http://localhost:8501
6. Открыть консоль MinIO: http://localhost:9001 (minioadmin / minioadmin123)
7. Открыть Flower: http://localhost:5555

Видео-плеер в дашборде ходит в MinIO напрямую из браузера — проверь, что
`S3_PUBLIC_ENDPOINT` (дефолт `http://localhost:9000`) доступен с машины,
с которой открываешь дашборд.

---

## Ключевые точки доступа

| Задача | Файл |
|--------|------|
| Точка входа приложения | `app/main.py` |
| Celery задачи | `app/tasks/processing.py` |
| YOLO детекция | `app/detection/yolo.py` |
| Пайплайн видео | `app/pipeline/processor.py` |
| Хранилище MinIO | `app/core/storage.py` |
| Модели БД | `app/db/models.py` |
| Конфигурация / переменные окружения | `app/core/config.py` |
| Kafka топики | `app/streaming/topics.py` |
| Миграции БД | `alembic/versions/` |
```
