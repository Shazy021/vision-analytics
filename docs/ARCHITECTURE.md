# Vision Analytics Platform — Architecture

## Goal

Object detection system (YOLO) for:
- **Uploaded video files** (MP4, AVI, MOV, MKV)
- **RTSP/IP streams** in real time

---

## Architecture Overview

### 1. Entry Point — `app/main.py`

```python
FastAPI app with routers:
├── /api/videos      # CRUD for video files
├── /api/streams     # RTSP stream management
└── /api/analytics   # Aggregated statistics
```

Lifespan events:
- `init_db()` — create tables if missing (non-fatal; Alembic is preferred)
- `storage.ensure_bucket()` — create MinIO bucket
- `storage.set_stream_frames_ttl(days=7)` — auto-delete stream frames older than 7 days

---

### 2. Video Upload — `POST /api/videos/upload`

```
Client (browser / dashboard)
    |
FastAPI: upload_video()
    |
1. Create Video row in DB (status: "processing")
2. Save file to temp disk -> upload to MinIO via fput_object (multipart)
3. Delete temp file
4. Send Celery task: process_video_task.delay(video_id)
5. Return 201 immediately (client polls /api/videos/{id}/status)
```

---

### 3. Video Processing (Celery Worker)

```
process_video_task(video_id)
    |
app.pipeline.processor.process_video()
    |
- Reads video DIRECTLY from MinIO via HTTP (OpenCV + FFmpeg streaming)
- Does NOT download to disk
    |
- YOLO detection every Nth frame (controlled by settings.extract_fps)
- app.detection.yolo.YOLODetector.detect()
    |
- Creates Detection rows in DB:
  frame_index, timestamp, class_name, bbox, confidence
    |
- Updates Video.status = "completed" | "partial" | "failed"
- Updates Video.duration, Video.fps
```

Note: the video file is already in MinIO from step 2. The processor does NOT
re-upload it. It reads via presigned URL.

---

### 4. Streams (Kafka Pipeline)

```
RTSP URL -> app.streaming.producer.FrameProducer (subprocess, spawned by stream_manager)
    |
Reads RTSP via OpenCV.VideoCapture (forced TCP transport in Docker)
    |
Every Nth frame:
  - Encode JPEG + base64
  - Publish to Kafka topic "raw_frames"
        |
        +--> app.streaming.yolo_consumer.YOLOConsumer
        |      - Decode frame, run YOLO inference
        |      - Save frames WITH detections to MinIO: streams/{stream_id}/{index}.jpg
        |      - Publish detections to Kafka topic "detections"
        |
        +--> (no second consumer on raw_frames)

Kafka topic "detections"
        |
        +--> app.streaming.consumer.DetectionConsumer
               - Read detections from Kafka
               - Write Detection rows to PostgreSQL (with stream_id)
```

Stream lifecycle (desired-state + reconciler pattern):
1. `POST /api/streams` — create Stream row (`desired_status`: inactive)
2. `POST /api/streams/{id}/start` — write `desired_status=active` (the API spawns NOTHING)
3. `POST /api/streams/{id}/stop` — write `desired_status=inactive`
4. `DELETE /api/streams/{id}` — delete row + delete frames from MinIO

The API is only an "intent writer": it records `desired_status` in the DB. The
actual producer processes are owned by a separate service, `stream_manager`
(`app/streaming/manager.py`) — see the "Stream supervision" section below.

Frame numbering (`frame_index`) is continuous: on start, the producer resumes
counting from `max(frame_index)` in the DB (`FrameProducer._initial_frame_index()`).
Without this, restarting a stream reset the counter, new frames overwrote old
`streams/{id}/{index}.jpg` keys, and `/latest-frame` (which takes `max()` over
those keys) stuck on the first-run maximum forever — the dashboard preview
"froze on a single frame".

### Stream supervision (desired-state + reconciler)

Previously the API spawned the producer itself and kept live `Popen` handles in
an in-memory dict (`_running_processes`). That caused three bugs: an API restart
killed every stream, a crashed producer never came back, and two API instances
fought over the same stream.

We now split INTENT from FACT (like Kubernetes controllers):
- `Stream.desired_status` (active/inactive) — intent, written only by the API;
- `Stream.status` (active/inactive/error) — fact, written only by `stream_manager`;
- `Stream.status_reason` — diagnosis when `status=error`.

`stream_manager` runs a reconcile loop: each pass reads desired from the DB,
compares it against its live child processes and converges them — spawning
missing producers and killing ones no longer desired. A pass is idempotent
(live producers are left alone), so an API restart is irrelevant to streams and
a crashed producer is respawned automatically.

Backoff against a spawn storm: if a producer dies sooner than `CRASH_WINDOW`
(10s) after start, that counts as a crash; after `MAX_FAST_CRASHES` (3) rapid
crashes in a row the stream gets `status=error` with a reason and is no longer
respawned until the user does `stop` → `start` (which removes it from desired
and clears the state).

---

### 5. Storage — `app/core/storage.py` (MinIO / S3)

| Prefix | Content | TTL |
|--------|---------|-----|
| `videos/{id}.{ext}` | Original uploaded videos | None (user-managed) |
| `streams/{id}/{frame}.jpg` | Stream frames with detections | 7 days (lifecycle rule) |

All parameters come from `app.core.config.settings` (env vars / `.env`).

#### Presigned URLs: internal vs public endpoint

Presigned links have two consumers with different network visibility:

| Consumer | Endpoint | Example |
|----------|----------|---------|
| Server side (cv2 seek in api, dashboard from its container) | `S3_ENDPOINT` | `http://minio:9000/...` |
| Browser (dashboard video player) | `S3_PUBLIC_ENDPOINT` | `http://localhost:9000/...` |

Browsers cannot resolve docker hostnames (`minio:9000`), so browser-facing
links are signed by a separate Minio client on the public address. The host
is part of the SigV4 signature — it cannot be swapped in a finished URL,
only signed upfront. The region is set explicitly (`us-east-1`): otherwise
minio-py calls `GetBucketLocation` against its own endpoint before presigning,
and `localhost:9000` is unreachable from inside a container.

---

### 6. Analytics — `app/api/routes/analytics.py`

| Endpoint | Description |
|----------|-------------|
| `GET /api/analytics/stats` | Total videos, detections, avg confidence |
| `GET /api/analytics/class-distribution` | Pie chart data: count per class |
| `GET /api/analytics/timeline` | Detections over time (bucketed by seconds) |
| `GET /api/analytics/stream-activity` | Detections per minute for streams |

All support `source=video|stream` filter and `video_id` / `stream_id` filters.

---

### 7. Dashboard — `dashboard/app.py` (Streamlit)

- **"Videos" page**: upload form, video list, class-distribution pie, timeline bar,
  plus a **synchronized dual player**: two windows (clean video / same frames
  with boxes) on one shared timeline. Implemented as a JS component
  (`streamlit.components.v1.html`, `SYNC_PLAYER_TEMPLATE`): all detections of
  the video are injected into the HTML as JSON, the nearest processed frame is
  found via binary search on `currentTime`, boxes are drawn on a canvas
  overlay. Player sync is client-side — Streamlit cannot hit the server per frame.
- **"Streams" page**: add/start/stop/delete RTSP, live preview (polling
  `/latest-frame` at a configurable 2–10 s interval), activity chart,
  recent detections.
- API calls cached with 10s TTL (`@st.cache_data`)
- Streams-page auto-refresh: `st.rerun()` runs strictly at the end of the page
  (rerun aborts script execution, so any sections below it would never render)

---

## Data Model

Three PostgreSQL tables (`app/db/models.py`), versioned with Alembic.

### `videos`
| Column | Type | Notes |
|--------|------|-------|
| `id` | int PK | |
| `filename` | varchar(255) | |
| `filepath` | varchar(512) | local path (optional) |
| `object_key` | varchar(512) | MinIO key, e.g. `videos/5.mp4` |
| `duration` | float | seconds |
| `fps` | int | |
| `resolution` | varchar(20) | e.g. `1920x1080` |
| `uploaded_at` | datetime | server default `now()` |
| `status` | varchar(20) | `processing` → `completed` / `partial` / `failed` |
| `task_id` | varchar(255) | Celery task id |

### `detections`
| Column | Type | Notes |
|--------|------|-------|
| `id` | int PK | |
| `video_id` | int FK → `videos.id` | NULL for stream detections |
| `stream_id` | int FK → `streams.id` | NULL for video detections |
| `frame_index` | int | frame key in MinIO |
| `class_name` | varchar(50) | |
| `confidence` | float | |
| `bbox_x1/y1/x2/y2` | float | bounding box |
| `timestamp` | float | seconds within the video file |
| `detected_at` | datetime | wall-clock; valid for videos AND streams |

A detection belongs to EITHER a video OR a stream — the other FK is NULL.
Indexed on `video_id`, `stream_id`, `class_name`, `detected_at`.

### `streams`
| Column | Type | Notes |
|--------|------|-------|
| `id` | int PK | |
| `name` | varchar(255) | |
| `rtsp_url` | varchar(512) | |
| `desired_status` | varchar(20) | **intent** — `active`/`inactive`; written only by the API |
| `status` | varchar(20) | **fact** — `active`/`inactive`/`error`; written only by `stream_manager` |
| `status_reason` | varchar(255) | diagnosis when `status=error` |
| `fps` | int | capture rate |
| `created_at` | datetime | server default `now()` |
| `pid` | int | legacy — last producer pid; no longer maintained (see § Stream supervision) |

The `desired_status` (intent) vs `status` (fact) split is the core of the
desired-state + reconciler pattern: the API owns intent, `stream_manager` owns
the observed fact. Both live in the DB, so they survive an API or manager restart.

---

## Project Structure

```
app/
├── main.py                     # FastAPI app, lifespan, router registration
├── core/
│   ├── config.py               # pydantic-settings: DB, Kafka, S3, YOLO, Celery
│   ├── database.py             # SQLAlchemy engine, SessionLocal, Base, get_db
│   ├── celery_app.py           # Celery app (broker=Redis, backend=Redis)
│   └── storage.py              # MinIO client: upload, download, presigned, TTL
├── api/
│   └── routes/
│       ├── videos.py           # CRUD: upload, list, get, status, frame, delete
│       ├── streams.py          # CRUD: create, start, stop, delete, detections, frames
│       └── analytics.py        # stats, class-distribution, timeline, stream-activity
├── schemas/
│   ├── video.py                # VideoBase, VideoCreate, VideoRead (Pydantic v2)
│   └── detection.py            # DetectionBase, DetectionRead
├── detection/
│   └── yolo.py                 # YOLODetector wrapper (Ultralytics YOLOv8)
├── pipeline/
│   └── processor.py            # process_video(): read MinIO -> YOLO -> DB
├── video/
│   └── extractor.py            # VideoCapture context manager, Frame dataclass
├── streaming/
│   ├── topics.py               # Kafka topic names, bootstrap servers
│   ├── producer.py             # FrameProducer: RTSP/file -> Kafka raw_frames
│   ├── yolo_consumer.py        # YOLOConsumer: raw_frames -> YOLO -> detections + MinIO
│   └── consumer.py             # DetectionConsumer: detections -> PostgreSQL
├── tasks/
│   └── processing.py           # Celery task: process_video_task
└── db/
    └── models.py               # SQLAlchemy: Video, Detection, Stream

alembic/                        # DB migrations (alembic upgrade head)
tests/                          # pytest: API, Celery, YOLO, extractor, streaming
dashboard/                      # Streamlit app
.github/workflows/              # CI (lint + test), Docker build
docker-compose.yml              # All services (CPU mode)
docker-compose.gpu.yml          # GPU overrides (NVIDIA)
Dockerfile                      # CUDA image (worker, yolo-consumer)
Dockerfile.cpu                  # CPU image (api, flower, detection-consumer, dashboard)
Makefile                        # make up / make down / make test / make lint
pyproject.toml                  # pytest, ruff, alembic config
requirements.txt                # Full deps (torch, ultralytics, cv2, ...)
requirements.cpu.txt            # CPU-only deps (no torch)
requirements-dev.txt            # pytest, ruff, pre-commit
```

---

## Data Flow

### Path A: Uploaded Video

```
Client -> POST /api/videos/upload
    |
    v
FastAPI: create Video row -> upload file to MinIO -> dispatch Celery task
    |
    v
Celery Worker: process_video()
    |  read video from MinIO via HTTP (presigned URL)
    |  YOLO detect every Nth frame
    |  write Detection rows to PostgreSQL
    |  update Video.status
    v
Client polls GET /api/videos/{id}/status -> "completed"
```

### Path B: RTSP Stream

```
1. POST /api/streams           -> create Stream row (desired inactive)
2. POST /api/streams/{id}/start -> desired_status=active; stream_manager spawns the subprocess
    |
    v
FrameProducer: read RTSP -> encode JPEG -> publish Kafka "raw_frames"
    |
    v
YOLOConsumer: consume "raw_frames" -> YOLO -> publish "detections"
    |                                  -> save frame to MinIO (if detections > 0)
    v
DetectionConsumer: consume "detections" -> write Detection rows to PostgreSQL
    |
    v
Dashboard: GET /api/streams/{id}/detections + /api/analytics/stream-activity
```

---

## Key Technologies

| Layer | Technology |
|-------|-----------|
| API | FastAPI (Python 3.12) |
| Database | PostgreSQL 16 (SQLAlchemy ORM) |
| Migrations | Alembic |
| Object Storage | MinIO (S3-compatible) |
| Message Queue | Kafka (KRaft mode, no ZooKeeper) |
| Async Tasks | Celery + Redis (broker + result backend) |
| ML | Ultralytics YOLOv8 |
| Media | OpenCV + FFmpeg |
| Dashboard | Streamlit + Plotly |
| Linting | Ruff |
| CI | GitHub Actions |

---

## Docker Architecture

Two images to minimize GPU memory usage:

| Image | Dockerfile | Services | Contents |
|-------|-----------|----------|----------|
| `vap-cpu` | `Dockerfile.cpu` | api, flower, detection-consumer, dashboard | No torch (~2 GB) |
| `vap-cuda` | `Dockerfile` | worker, yolo-consumer | torch + CUDA (~10-11 GB) |

Services in `docker-compose.yml`:

| Service | Image | Port | Purpose |
|---------|-------|------|---------|
| `db` | postgres:16-alpine | 5432 | Database |
| `redis` | redis:7-alpine | 6379 | Celery broker + backend |
| `api` | vap-cpu | 8000 | FastAPI server |
| `worker` | vap-cuda | — | Celery worker (replicas: 2, pool: solo) |
| `flower` | vap-cpu | 5555 | Celery monitoring UI |
| `dashboard` | vap-cpu | 8501 | Streamlit dashboard |
| `kafka` | cp-kafka:7.6.0 | 9092 | Kafka broker (KRaft) |
| `kafka-init` | cp-kafka:7.6.0 | — | One-shot: create topics |
| `yolo-consumer` | vap-cuda | — | Kafka -> YOLO -> detections |
| `detection-consumer` | vap-cpu | — | Kafka -> PostgreSQL |
| `stream-manager` | vap-cpu | — | Reconciler: owns producer subprocesses |
| `mediamtx` | bluenviron/mediamtx | 8554 | RTSP server (camera simulation) |
| `minio` | minio/minio | 9000, 9001 | Object storage + web console |

GPU: use `docker-compose.gpu.yml` override or `make up` (auto-detects via `nvidia-smi`).

Celery workers use `--pool=solo` because CUDA context does not survive `fork()`.
Parallelism via `deploy.replicas: 2` (independent containers).

---

## Implementation Details

### Why read video from MinIO via HTTP?
- Celery workers run in separate containers — no shared filesystem
- OpenCV (FFmpeg backend) supports `http://` sources natively
- Avoids downloading 7 GB files to worker disk

### Why only save stream frames with detections?
- Streams are ephemeral — empty frames have no value
- 1 stream ≈ 24 GB/day at 5 FPS; filtering saves ~80% storage
- Lifecycle rule auto-deletes frames older than 7 days

### Why do producer processes belong to a separate stream_manager (reconciler)?
- A producer is a long-running blocking loop (reads RTSP forever) and cannot run
  inside FastAPI's async event loop → it must be a separate process
- Ownership is moved out of the API: restarting/scaling the API neither kills
  nor duplicates streams (previously `Popen` lived in api memory and died with it)
- One owner = one source of truth: two API instances don't fight over one stream
- The reconcile loop respawns a crashed producer on its own; backoff (see the
  Stream supervision section) prevents a spawn storm on a broken RTSP URL
- Producer logs now land in `docker compose logs stream-manager` (where the
  owner lives), not in the api logs

### Why does the producer resume frame_index from the DB?
- `frame_index` is the frame key in MinIO (`streams/{id}/{index}.jpg`), and
  `/latest-frame` picks the newest frame as `max()` over those keys
- Resetting the counter on stream restart made new frames overwrite old keys:
  `max()` stuck on the first-run value, the dashboard preview "froze on one
  frame" while detections kept flowing into the DB
- Starting from `max(frame_index)` in the DB is safe: frames are saved only
  when detections exist, so every saved frame has a Detection row

### Why is the video preview a JS component instead of st.video + st.slider?
- Two synchronized players (clean / with boxes) on one timeline — Streamlit
  widgets round-trip to the server via rerun, an unacceptable latency per frame
- Boxes are drawn on a canvas overlay over the second player using
  `currentTime` from pre-injected JSON — zero server requests during playback
- The right ("follower") player is kept in sync via drift correction every 0.5 s

### Performance notes
- GPU processes batches more efficiently than single frames
  (`detect_batch()` exists in code but is not yet wired into the pipeline)
- Kafka `message.max.bytes`: 5 MB (JPEG frames in base64)
- Kafka retention: `raw_frames` = 1 hour, `detections` = 24 hours
- MinIO upload: `fput_object` (multipart) for videos, `put_object` for frames

---

## API Examples

```bash
# Upload a video
curl -F "file=@video.mp4" http://localhost:8000/api/videos/upload

# Check processing status
curl http://localhost:8000/api/videos/1/status

# Create a stream
curl -X POST http://localhost:8000/api/streams \
  -H "Content-Type: application/json" \
  -d '{"name":"Camera 1","rtsp_url":"rtsp://mediamtx:8554/mystream","fps":5}'

# Start a stream
curl -X POST http://localhost:8000/api/streams/1/start

# Get analytics
curl "http://localhost:8000/api/analytics/stats?source=video"
curl "http://localhost:8000/api/analytics/class-distribution?video_id=1"
```

---

## Startup Checklist

1. `cp .env.example .env` — set credentials if needed
2. `docker compose up -d` (or `make up` for GPU auto-detect)
3. `alembic upgrade head` — run migrations (or rely on `init_db()` at startup)
4. Verify health: `curl http://localhost:8000/health`
5. Open dashboard: http://localhost:8501
6. Open MinIO console: http://localhost:9001 (minioadmin / minioadmin123)
7. Open Flower: http://localhost:5555

The dashboard video player reaches MinIO directly from the browser — make sure
`S3_PUBLIC_ENDPOINT` (default `http://localhost:9000`) is reachable from the
machine where the dashboard is opened.

---

## Key Contact Points

| Concern | File |
|---------|------|
| App entry | `app/main.py` |
| Celery tasks | `app/tasks/processing.py` |
| YOLO detection | `app/detection/yolo.py` |
| Video pipeline | `app/pipeline/processor.py` |
| MinIO storage | `app/core/storage.py` |
| DB models | `app/db/models.py` |
| Config / env vars | `app/core/config.py` |
| Kafka topics | `app/streaming/topics.py` |
| DB migrations | `alembic/versions/` |
