"""Prometheus-метрики пайплайна — единая точка определения.

Что и почему измеряем:
- Counter — сколько событий произошло (кадры, ошибки, лаг-события). Само по
  себе число малоинформативно (растёт монотонно); ценность даёт rate() в
  PromQL — скорость прироста за окно (frames/s, errors/s).
- Histogram — распределение длительностей (decode, inference, ожидание в
  очереди, e2e). Хранит счётчики по бакетам; histogram_quantile() по ним
  даёт p50/p95/p99. Среднее скрывает хвосты, а нас интересуют именно они.

Все метрики пайплайна живут в ОДНОМ модуле и дефолтном реестре
prometheus_client:
- переопределение метрики с тем же именем в одном реестре запрещено
  (ValueError: Duplicated timeseries), а frame_queue_wait_seconds нужна
  ДВУМ консьюмерам — raw_frames и detections различаются лейблом topic;
- тесты импортируют несколько консьюмерных модулей в один процесс,
  разнесение по модулям уперлось бы в конфликт имён.
Плата за это: каждый /metrics показывает нулями метрики чужих стадий —
rate() от нуля не отличается от отсутствия серии, графикам это не вредит.

Cardinality: лейбл stream_id — ТОЛЬКО на frame_e2e_latency_seconds и
frames/s-счётчиках. Лейбл с высокой кардинальностью (много стримов) на
каждой метрике раздувает TSDB Prometheus; остальным метрикам stream_id
не нужен — стабильность трассы смотрим суммарно, без разбивки по стримам.
"""
import logging
import os
import time
from datetime import datetime, timezone

from prometheus_client import Counter, Histogram, start_http_server

logger = logging.getLogger(__name__)

# Единые бакеты для всех гистограмм: покрывают от 10 мс (decode кадра)
# до 10 с (инференс на слабом CPU / большой батч). 11 бакетов × серия —
# разумный компромисс между точностью квантилей и размером TSDB.
DURATION_BUCKETS = (0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10)

# --- yolo-consumer (raw_frames → YOLO → detections), /metrics :9091 ---
frames_received_total = Counter(
    "frames_received_total",
    "Messages polled from raw_frames (ingress rate via rate())",
)
frame_processed_total = Counter(
    "frame_processed_total",
    "Frames handled by yolo-consumer by outcome",
    ["result"],  # ok | error
)
frame_decode_duration_seconds = Histogram(
    "frame_decode_duration_seconds",
    "JPEG imdecode of one frame",
    buckets=DURATION_BUCKETS,
)
frame_inference_duration_seconds = Histogram(
    "frame_inference_duration_seconds",
    "detector.detect() for one frame",
    buckets=DURATION_BUCKETS,
)
frame_queue_wait_seconds = Histogram(
    "frame_queue_wait_seconds",
    "Broker creation time to dequeue (backlog of the previous stage)",
    ["topic"],  # raw_frames | detections
    buckets=DURATION_BUCKETS,
)
poison_messages_total = Counter(
    "poison_messages_total",
    "Messages skipped after handler failure in yolo-consumer",
)

# --- detection-consumer (detections → PostgreSQL), /metrics :9092 ---
frame_written_total = Counter(
    "frame_written_total",
    "Frames handled by detection-consumer by outcome",
    ["result"],  # ok | skipped | error | missing_ts
)
frame_write_duration_seconds = Histogram(
    "frame_write_duration_seconds",
    "_save_to_db for one detection message (insert + commit)",
    buckets=DURATION_BUCKETS,
)
frame_e2e_latency_seconds = Histogram(
    "frame_e2e_latency_seconds",
    "Frame capture (captured_at) to DB commit — full stream pipeline",
    ["stream_id"],
    buckets=DURATION_BUCKETS,
)

# --- video-consumer (video_requests → processor → PostgreSQL), /metrics :9093 ---
# Бакеты видео-трассы длиннее стримовых [0.01..10]: задача/чанк живут минуты
# (чанк с 780 сэмплами дал inference ~24с — в «стримовые» бакеты он попал бы
# в +Inf, и квантили стали бы ложью). Хвост до 10 минут = верхняя оценка
# разумного времени чанка на наших исходниках.
VIDEO_DURATION_BUCKETS = (0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600)

frame_task_queue_wait_seconds = Histogram(
    "frame_task_queue_wait_seconds",
    "API enqueue (enqueued_at) to dequeue of a video task",
    buckets=VIDEO_DURATION_BUCKETS,
)
frame_task_duration_seconds = Histogram(
    "frame_task_duration_seconds",
    "Whole video task: DB lookup + model load + decode + inference + write",
    buckets=VIDEO_DURATION_BUCKETS,
)
frame_chunk_decode_duration_seconds = Histogram(
    "frame_chunk_decode_duration_seconds",
    "Sum of frame decode time per video chunk",
    buckets=VIDEO_DURATION_BUCKETS,
)
frame_chunk_inference_duration_seconds = Histogram(
    "frame_chunk_inference_duration_seconds",
    "Sum of detector.detect() time per video chunk",
    buckets=VIDEO_DURATION_BUCKETS,
)
frame_chunk_write_duration_seconds = Histogram(
    "frame_chunk_write_duration_seconds",
    "Chunk results write: delete-range + insert + commit",
    buckets=VIDEO_DURATION_BUCKETS,
)
frame_task_completed_total = Counter(
    "frame_task_completed_total",
    "Video tasks reached terminal state in video-consumer",
    ["status"],  # completed | failed
)


def parse_epoch_seconds(raw: str | None) -> float | None:
    """UTC ISO из сообщения → epoch seconds; None, если значения нет/оно битое.

    Наивные (без смещения) значения считаем UTC — та же конвенция, что у
    _parse_captured_at в detection-консьюмере. Битое значение — НЕ ошибка:
    сообщения старых версий producers просто не имеют поля, метрику
    пропускаем, а не роняем консьюмер.
    """
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def queue_wait_seconds(msg) -> float | None:
    """Сколько сообщение пролежало в топике: broker creation time → сейчас.

    confluent_kafka Message.timestamp() возвращает (type, ms): 1=CreateTime,
    2=LogAppendTime, 0=нет данных (ms=-1). Тип 0 и отрицательный результат
    (перекос часов producer'а/брокера) — пропускаем: histogram не должен
    впитывать мусор, иначе квантили врут.

    try/except не только для правды жизни: тесты подсовывают MagicMock,
    у которого timestamp() нераспаковываемый.
    """
    try:
        ts_type, ts_ms = msg.timestamp()
    except (TypeError, ValueError):
        return None
    if ts_type not in (1, 2) or ts_ms <= 0:
        return None
    wait = time.time() - ts_ms / 1000.0
    return wait if wait >= 0 else None


def start_metrics_server(default_port: int) -> None:
    """Поднять HTTP /metrics внутри процесса (daemon-поток из prometheus_client).

    Метрики живут в памяти консьюмера — поднять сервер больше негде.
    METRICS_PORT даёт сдвинуть порт, если дефолт занят (несколько процессов
    на одном хосте без Docker); в compose порт не переопределяем —
    prometheus.yml скрейпит дефолты (см. monitoring/prometheus/).
    """
    port = int(os.environ.get("METRICS_PORT", default_port))
    start_http_server(port)
    logger.info("[Metrics] serving http://0.0.0.0:%d/metrics", port)
