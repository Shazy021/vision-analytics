"""
Единая точка конфигурации всего приложения.

Все настройки читаются из окружения / .env через pydantic-settings.
Приоритет: os.environ (Docker, CI) > .env > дефолты из класса.

Благодаря этому в проекте один источник правды о конфиге:
- API, Celery-воркеры, Kafka-консьюмеры и тесты читают один и тот же Settings
- docker-compose передаёт значения через переменные окружения
- локально достаточно файла .env (см. .env.example)
"""
import logging

from pydantic import ConfigDict, field_validator
from pydantic_settings import BaseSettings

logger = logging.getLogger(__name__)


def _detect_device() -> str:
    """
    Автоопределение устройства для инференса: cuda, если есть GPU, иначе cpu.

    Зачем: дефолт DEVICE=cuda валил проект на любой машине без NVIDIA
    (рекрутер клонировал репо — и ничего не запустилось). С "auto" проект
    заводится везде: на машине с NVIDIA автоматически берётся GPU,
    без неё — CPU. Никаких ручных правок конфига после клона.
    """
    try:
        import torch

        if torch.cuda.is_available():
            return "cuda"
    except Exception:  # torch не установлен или драйвер недоступен — это не ошибка
        pass
    return "cpu"


class Settings(BaseSettings):
    # --- База данных ---
    database_url: str = "postgresql://user:pass@localhost:5432/vision"

    # --- YOLO ---
    weights_path: str = "yolov8n.pt"  # n = nano, самая маленькая и быстрая модель
    device: str = "auto"  # auto | cpu | cuda | 0 | 1 — "auto" резолвится при старте
    extract_fps: int = 5
    max_frames: int = 300  # 0 = без лимита, иначе обработать только N кадров

    # --- Celery ---
    celery_broker_url: str = "redis://localhost:6379/0"
    celery_result_backend: str = "redis://localhost:6379/1"

    # --- Kafka ---
    kafka_bootstrap_servers: str = "localhost:9092"

    # --- S3 (MinIO) ---
    s3_endpoint: str = "http://localhost:9000"
    s3_access_key: str = "minioadmin"
    s3_secret_key: str = "minioadmin123"
    s3_bucket: str = "vision"

    # extra="ignore": .env может содержать переменные, нужные не приложению,
    # а docker-compose (например POSTGRES_USER для контейнера db, API_URL для
    # сервисов compose). По умолчанию pydantic-settings forbids такие ключи
    # и роняет Settings с ValidationError — игнорируем их вместо падения.
    model_config = ConfigDict(env_file=".env", extra="ignore")

    @field_validator("device", mode="before")
    @classmethod
    def _normalize_device(cls, v):
        v = str(v).lower()
        if v in ("auto", "", "none"):
            return _detect_device()
        if v == "gpu":
            return "cuda"
        if v in ("cpu", "cuda") or v.isdigit():
            return v
        raise ValueError(f"Invalid device: {v!r}. Use auto / cpu / cuda / 0 / 1")


settings = Settings()
logger.info(f"[Config] device={settings.device}, bucket={settings.s3_bucket}")
