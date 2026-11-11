"""
Единая точка конфигурации всего приложения.

Все настройки читаются из окружения / .env через pydantic-settings.
Приоритет: os.environ (Docker, CI) > .env.local > .env > дефолты из класса.

Благодаря этому в проекте один источник правды о конфиге:
- API, Kafka-консьюмеры и тесты читают один и тот же Settings
- docker-compose передаёт значения через переменные окружения
- локально достаточно файла .env (см. .env.example)
"""
import logging

from pydantic import ConfigDict
from pydantic_settings import BaseSettings

logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    # --- База данных ---
    database_url: str = "postgresql://user:pass@localhost:5432/vision"

    # --- Детекция ---
    extract_fps: int = 5
    max_frames: int = 300  # 0 = без лимита, иначе обработать только первые N прочитанных кадров

    # --- Инференс: только Triton Inference Server ---
    # Модель (TensorRT engine с вшитым NMS) живёт в отдельном Triton-сервере,
    # консьюмерам нужны только numpy + лёгкий gRPC-клиент, GPU держит один
    # Triton на все трассы и реплики. Без поднятого Triton детекции нет ни у
    # видео, ни у стримов — это обязательная часть стека.
    # Адрес Triton для gRPC: в docker-сети это "triton:8001" (имя сервиса),
    # с хоста — "localhost:18001" (порт standalone-контейнера).
    triton_url: str = "triton:8001"
    triton_model_name: str = "yolo_detector"
    # JPEG-intake (стримовый путь): вместо клиентского imdecode+letterbox
    # консьюмер шлёт сырые JPEG-байты в ансамбль yolo_jpeg (DALI: декод
    # nvImageCodec + resize/центрирование на GPU, дрейф против cv2-пути
    # измерен: matched 0.908, IoU 0.962, count-drift 0.045 @ q80).
    # Экономия на консьюмере — весь CPU декода и letterbox'а; gRPC-запрос
    # ~100 КБ вместо 1.23 МБ. Видео-трасса (processor.py) остаётся на
    # yolo_detector: там кадры из cv2-декода, кодировать их в JPEG ради
    # ансамбля — терять качество и CPU.
    triton_jpeg_intake: bool = False
    triton_jpeg_model_name: str = "yolo_jpeg"
    # Выравнивать длину JPEG-запросов стримов по бакету этого размера в
    # байтах (0 = выключено). Динамический батчинг Triton'а склеивает только
    # запросы с одинаковыми шейпами входа, а у JPEG разных кадров длина
    # разная — из-за этого ступень dali_letterbox не батчилась
    # вообще (замер: батч 1.04 и 242 fps; после выравнивания — батч 15.9
    # и 424 fps). Декодер останавливается на EOI-маркере, хвост из нулей
    # ему безразличен. Бакет (64 КБ), а не одна фиксированная длина: весь
    # запрос копируется и сериализается в python под GIL'ом, поэтому
    # 90-килобайтный кадр до 512 КБ домножать дорого — кадры одного
    # бакета батчатся между собой, этого достаточно.
    triton_jpeg_pad: int = 0
    # Сколько независимых gRPC-каналов держит клиент Triton. Один канал
    # сериализует крупные запросы (~110 req/s потолок), потокам отправки
    # нужно несколько — см. комментарий в triton_detector.py.
    triton_channels: int = 4

    # --- Чанкование видео ---
    # Длинные видео режутся на чанки по времени, которые параллельно берут
    # разные реплики video-consumer (см. app/processing/planner.py).
    # Короткие не режутся: оверхед N открытий/seek'ов съест выигрыш.
    # Порог 45с, а не 60: длительность из метаданных jitter-ит (видео
    # «на 1 минуту» = 1500 кадров / 25.0004 fps = 59.999с < 60).
    video_chunk_sec: float = 30.0
    video_min_chunk_duration: float = 45.0

    # --- YOLO consumer: пропускная способность raw_frames ---
    # Стримовый инференс асинхронный (Triton): кадры НЕ лепятся в батч на
    # клиенте — каждый кадр сразу уходит отдельным async-запросом, батчи
    # склеивает динамический батчинг Triton'а (замер: ~210 fps против
    # ~61 fps у клиентских батчей синхронными запросами).
    # Сколько кадров держать «в полёте» к Triton: меньше — GPU голодает
    # в сетевых паузах, больше — латентность и RAM. Замер: 32–64 дают
    # потолок GPU (~150–210 fps на этом стенде).
    yolo_infer_window: int = 64
    # Потоки для CPU/сетевой работы (imdecode, letterbox, аплоад в MinIO):
    # эта работа не занимает GPU, поэтому идёт параллельно отправке/приёму.
    yolo_io_workers: int = 4

    # --- Detection consumer: пропускная способность detections → БД ---
    # Сколько сообщений собрать в одну транзакцию (bulk INSERT). Больше
    # батча = меньше транзакций на ту же нагрузку (транзакция — главный
    # расходник построчного режима); при низкой нагрузке grace-окно не
    # держит одиночные сообщения дольше yolo-side grace.
    detection_batch_size: int = 64
    # Верхняя граница (мс) ожидания ПЕРВОГО сообщения батча; дальше сборка
    # динамическая, пока очередь горячая (та же семантика, что у yolo).
    detection_batch_grace_ms: int = 50

    # --- Kafka ---
    kafka_bootstrap_servers: str = "localhost:9092"

    # --- S3 (MinIO) ---
    s3_endpoint: str = "http://localhost:9000"
    # Публично доступный адрес MinIO для presigned URL, которые открывает
    # БРАУЗЕР (например st.video в дашборде). Внутри docker S3_ENDPOINT —
    # это http://minio:9000, и браузер такой хост не резолвит → видео не
    # играется. Сюда пишут адрес, доступный с машины разработчика
    # (обычно http://localhost:9000). Пусто = использовать s3_endpoint.
    s3_public_endpoint: str = ""
    s3_access_key: str = "minioadmin"
    s3_secret_key: str = "minioadmin123"
    s3_bucket: str = "vision"

    # extra="ignore": .env может содержать переменные, нужные не приложению,
    # а docker-compose (например POSTGRES_USER для контейнера db, API_URL для
    # сервисов compose). По умолчанию pydantic-settings forbids такие ключи
    # и роняет Settings с ValidationError — игнорируем их вместо падения.
    #
    # env_file списком: .env.local перечитывается ПОСЛЕ .env и переопределяет
    # его — в нём держат локальные хосты (localhost вместо docker-имён db/kafka)
    # для запуска alembic/скриптов с хостовой машины. .env.local в .gitignore.
    model_config = ConfigDict(env_file=(".env", ".env.local"), extra="ignore")


settings = Settings()
logger.info(f"[Config] triton={settings.triton_url}, bucket={settings.s3_bucket}")
