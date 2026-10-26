# Vision Analytics Platform

![CI](https://github.com/Shazy021/vision-analytics/actions/workflows/ci.yml/badge.svg)

Платформа видеоаналитики: загрузка видео / RTSP-стрим → YOLO-детекция → Kafka → PostgreSQL → REST API → Streamlit-дашборд.

## Демо

Живые записи работы дашборда — страница 📹 Видео (обработанный ролик с двойным плеером) и страница 📡 Стримы (поток в реальном времени):

<p align="center"><img src="demos/Videos.gif" alt="Страница Видео: двойной плеер с кадрами и детекциями"></p>

<p align="center"><img src="demos/Streams.gif" alt="Страница Стримы: последние кадры и детекции стрима"></p>

<p align="center"><img src="demos/analytics.png" alt="Страница Аналитика: распределение классов и детекции по времени"><br><em>📊 Аналитика — распределение детекций по классам и по времени.</em></p>

## Стек

- **API**: FastAPI, Pydantic, SQLAlchemy
- **CV**: Ultralytics YOLOv8, OpenCV
- **БД**: PostgreSQL, Alembic (миграции)
- **Дашборд**: Streamlit, Plotly
- **Очереди/события**: Kafka (KRaft), confluent-kafka
- **RTSP**: MediaMTX
- **Инфра**: Docker Compose, GPU passthrough, pytest

## Быстрый старт

### Docker

```bash
# 1. Создай .env из шаблона (секреты и хосты сервисов)
cp .env.example .env

# 2. Подними все сервисы
docker-compose up -d
```

Поднимутся 12 сервисов (13 контейнеров с учётом video-consumer ×2): db, minio, kafka, kafka-init (разовый — создаёт топики), api, dashboard, stream-manager, video-consumer (x2), yolo-consumer, detection-consumer, mediamtx.

<p align="center"><img src="demos/docker-compose-ps.png" alt="Docker Desktop: все сервисы стека подняты"><br><em><code>kafka-init</code> в сером статусе — это норма: разовый контейнер создал топики и штатно завершился.</em></p>

> 🔐 Пароли/ключи задаются переменными в `.env` (${VAR:-default} в compose).
> В репозитории только безопасные дев-дефолты — для прода переопредели их в `.env`.
>
> 🌐 `S3_PUBLIC_ENDPOINT` (дефолт `http://localhost:9000`) — адрес MinIO для
> presigned URL, которые открывает **браузер** (видео-плеер в дашборде).
> Браузер не резолвит docker-имена, поэтому ссылка должна быть по адресу
> с проброшенным портом. Внутренние запросы контейнеров идут на `S3_ENDPOINT`.

> 💻 **GPU определяется автоматически.** Скрипты старта проверяют наличие NVIDIA
> (nvidia-smi) и подключают GPU-резервы из `docker-compose.gpu.yml` только на
> GPU-машинах. Без NVIDIA тот же стек поднимется на CPU — без правок файлов.
>
> **Почему образ с CUDA, а GPU — через override?** Образ собирается с CUDA-сборкой
> torch, но это только ядра: чтобы контейнер реально увидел карту, Docker должен
> выдать ему устройство через `deploy.resources.reservations.devices` (nvidia).
> Вшивать резервы в базовый compose нельзя — тогда `docker compose up` падает
> с ошибкой "could not select device driver nvidia" на любой машине без GPU.
> Поэтому: базовый файл CPU-safe, а карта выдаётся только там, где она есть.
>
> | Окружение | Команда |
> |----------------------|---------------------------------------------|
> | Linux / macOS / WSL2 | `make up` |
> | Windows | `.\scripts\up.cmd` |
> | Вручную (CPU) | `docker compose up -d` |
> | Вручную (GPU) | `docker compose -f docker-compose.yml -f docker-compose.gpu.yml up -d` |
>
> **Важно про ручной GPU-запуск:** override подключается ВМЕСТЕ с базовым
> файлом — сначала `docker-compose.yml`, потом `docker-compose.gpu.yml`.
> Если передать только override, проект невалиден (сервисы описаны в базовом).
> Флаги `-f` ставятся ДО слова `up`.
>
> **Почему `.cmd`, а не `.ps1`:** PowerShell по умолчанию блокирует
> неподписанные скрипты (ошибка «не имеет цифровой подписи»). `up.cmd`
> обходит это сам. Если хочешь запускать `up.ps1` напрямую — разреши
> локальные скрипты один раз:
> `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`

### Доступ

| Сервис | URL |
|--------|-----|
| API | http://localhost:8000 |
| Swagger | http://localhost:8000/docs |
| Dashboard | http://localhost:8501 |

### Загрузка видео

Через Dashboard (страница 📹 Видео) или через API:

```bash
curl -X POST http://localhost:8000/api/videos/upload \
  -F "file=@video.mp4"
```

Видео обработается автоматически: Kafka (`video_requests`) → video-consumer → YOLO → PostgreSQL.
Готовое видео доступно в дашборде: **синхронный двойной плеер** — чистый кадр
и кадр с боксами бок-о-бок на одном общем таймлайне.

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

**Шаг 3.** Нажать ▶️ Запустить — API запишет намерение (`desired_status=active`), а сервис `stream_manager` поднимет Frame Producer subprocess'ом.

Пайплайн: Producer → Kafka (`raw_frames`) → YOLO Consumer → Kafka (`detections`) → Detection Consumer → PostgreSQL.

<p align="center"><img src="demos/detection-gallery.png" alt="📋 Галерея детекций стрима: кропы объектов с классом и уверенностью"><br><em>📋 Галерея детекций на странице Стримы — кропы объектов с классом и уверенностью.</em></p>

> 📡 **Публичные камеры без MediaMTX.** Producer открывает и HLS-ссылки
> (`https://.../playlist.m3u8`) напрямую — вставь такой URL веб-камеры прямо
> в форму стрима. Браузерный User-Agent отправляется автоматически: часть CDN
> (включая idnes.cz) отдаёт HLS только «браузерным» клиентам. Сетевой сбой HLS
> producer переживает реконректом на месте, не дожидаясь reconciler'а.

### Стримы не работают — чек-лист

Механика: кнопка ▶️ Запустить пишет намерение (`desired_status=active`) в БД, а **reconciler в контейнере `stream_manager`** поднимает producer subprocess'ом и следит, чтобы он оставался живым.
Если источник не открылся — процесс умирает, reconciler переподнимает его; после 3 быстрых падений подряд стрим получит статус `error` с причиной и остановится (защита от spawn-шторма).
Чтобы повторить попытку после `error` — нажми ⏹ Стоп, затем ▶️ Запустить. Повторный «Запустить» на живом стриме вернёт 200 (API не знает и не должен знать про живость процесса).

**1. Смотри логи producer'а** — владелец процесса теперь `stream-manager`, туда и пишет producer:

```bash
docker compose logs stream-manager --tail 50
```

Там будет либо причина падения с трейсбеком, либо прогресс «Отправлено кадров: N».

**2. Типовая причина №1 — адрес источника не виден из контейнера.**
Producer работает внутри `stream-manager`, и `localhost` внутри контейнера — это сам контейнер, а не твоя машина:

| Откуда источник | URL для стрима |
|---|---|
| Встроенный MediaMTX | `rtsp://mediamtx:8554/mystream` |
| Программа на Windows-хосте (FFmpeg/OBS/Larix) | `rtsp://host.docker.internal:8554/mystream` |
| IP-камера в LAN | `rtsp://admin:pass@192.168.1.100:554/stream` |

**3. Типовая причина №2 — в источник никто не публикует поток.**
MediaMTX — приёмник, он сам видео не генерирует. Проверка с хоста (нужен ffmpeg):

```bash
ffmpeg -stream_loop -1 -re -i data/test_v.mp4 -c:v copy -an -f rtsp -rtsp_transport tcp rtsp://localhost:8554/mystream
```

### Локально (без Docker)

```bash
# Установить зависимости
pip install -r requirements.txt

# Запустить PostgreSQL, Kafka
# Обновить DATABASE_URL, KAFKA_BOOTSTRAP_SERVERS

# Миграции
alembic upgrade head

# API
uvicorn app.main:app --reload

# Dashboard
streamlit run dashboard/app.py

# Kafka Consumers
python -m app.consumers.video             # видео: video_requests → YOLO → БД
python -m app.consumers.yolo_frames yolov8n.pt   # стримы: raw_frames → YOLO → detections (устройство — из DEVICE, auto → cpu без GPU)
python -m app.consumers.detections
```

### Тесты и линтер

```bash
# Тесты — полностью герметичны и не требуют Docker: sqlite in-memory,
# сеть не трогается (MinIO/Kafka замоканы в conftest),
# тестовое видео генерируется на месте. Весь сьют — несколько секунд.
pytest tests/ -v

# Только стриминг-тесты
pytest tests/test_streaming.py -v

# Линтер (тот же, что в CI)
ruff check app dashboard tests
```

### Pre-commit — автопроверки перед коммитом

Хуки запускают ruff и гигиену файлов (пробелы, конец файла, валидность YAML/TOML,
защита от крупных файлов и приватных ключей) на каждый `git commit`. Установка — один раз:

```bash
pip install -r requirements-dev.txt
pre-commit install
```

- Первый прогон по всему репозиторию: `pre-commit run --all-files`
- Если хук автоисправил файлы — просто `git add -u` и коммить снова
- Хуки живут в `.pre-commit-config.yaml`; версии синхронизированы с CI

## API Endpoints

<p align="center"><img src="demos/swagger.png" alt="Swagger UI: все эндпоинты API" width="760"></p>

### Видео

| Method | Path | Description |
|--------|------|-------------|
| POST | /api/videos/upload | Загрузить видео и запустить обработку |
| GET | /api/videos/params | Параметры разбора (частота выборки кадров, лимит) |
| GET | /api/videos | Список всех видео |
| GET | /api/videos/{id} | Информация о видео (+ video_url для браузера) |
| GET | /api/videos/{id}/status | Статус обработки |
| GET | /api/videos/{id}/frame-image?timestamp=N | Кадр по времени как JPEG |
| GET | /api/videos/{id}/detections?closest_ts=N | Детекции (диапазон / ближайший кадр) |
| DELETE | /api/videos/{id} | Удалить видео |

### Стримы

| Method | Path | Description |
|--------|------|-------------|
| POST | /api/streams | Создать стрим |
| GET | /api/streams | Список стримов |
| DELETE | /api/streams/{id} | Удалить стрим |
| POST | /api/streams/{id}/start | Запустить обработку |
| POST | /api/streams/{id}/stop | Остановить обработку |
| GET | /api/streams/{id}/latest-frame | Последний кадр стрима (превью) |
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
| Kafka | `kafka:29092` | `localhost:9092` |
| MediaMTX | `mediamtx:8554` | `localhost:8554` |
| FastAPI | `api:8000` | `localhost:8000` |
| MinIO (S3 API) | `minio:9000` | `localhost:9000` |

## Kafka Topics

| Топик | Retention | Описание |
|-------|-----------|----------|
| `raw_frames` | 1 час | Кадры от Producer (base64 JPEG) |
| `detections` | 24 часа | YOLO-детекции (JSON) |
| `video_requests` | 24 часа | Задачи на обработку загруженных видео (JSON) |

Kafka — транспорт, не хранилище. Все детекции сохраняются в PostgreSQL.

## Структура проекта

```
├── app/
│   ├── api/routes/          # API endpoints (videos, streams, analytics)
│   ├── core/                # Config, database, storage, Kafka topics, CUDA utils
│   ├── db/models.py         # SQLAlchemy models (Video, Stream, Detection)
│   ├── schemas/             # Pydantic schemas
│   ├── processing/          # Обработка видео: processor, detector, extractor
│   ├── streams/             # Стримы: reconciler (manager) + FrameProducer
│   ├── consumers/           # Kafka-консьюмеры (модуль = контейнер)
│   │   ├── video.py         # video_requests → processor → БД
│   │   ├── yolo_frames.py   # raw_frames → YOLO → detections + MinIO
│   │   └── detections.py    # detections → PostgreSQL
│   └── main.py              # FastAPI entry point
├── dashboard/
│   └── app.py               # Streamlit dashboard
├── alembic/                 # Database migrations
├── tests/                   # Test suite
├── docker-compose.yml       # 12 services (kafka-init — разовый)
├── Dockerfile               # CUDA-образ: video-consumer, yolo-consumer (инференс)
├── Dockerfile.cpu           # CPU-образ: api, detection-consumer, stream-manager, dashboard
├── requirements.txt         # зависимости CUDA-образа (torch/ultralytics)
└── requirements.cpu.txt     # зависимости CPU-образа (без torch)
```

## Требования

- Docker + Docker Compose
- NVIDIA GPU + Docker GPU support (для YOLO-инференса)
- FFmpeg (для симуляции RTSP с видео-файла)

> Без NVIDIA тоже запустится: `DEVICE=auto` в конфиге выбирает CPU автоматически
> (см. `app/core/config.py::_detect_device`). GPU-резервы Docker подключает
> только на NVIDIA-машинах — через `make up` или override-файл (см. выше).
