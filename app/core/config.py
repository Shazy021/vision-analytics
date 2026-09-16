from pydantic_settings import BaseSettings
from pydantic import ConfigDict, field_validator

class Settings(BaseSettings):
    database_url: str = "postgresql://user:pass@localhost:5432/vision"
    weights_path: str = "yolov8n.pt"         # n = nano, самая маленькая и быстрая модель
    device: str = "cuda"                     # "cuda" если есть NVIDIA GPU
    extract_fps: int = 5   
    max_frames: int = 300 # 0 = без лимита, иначе обработать только N кадров

    model_config = ConfigDict(env_file=".env")

    # --- Celery ---
    celery_broker_url: str = "redis://localhost:6379/0"
    celery_result_backend: str = "redis://localhost:6379/1"

    @field_validator("device", mode="before")
    @classmethod
    def _normalize_device(cls, v):
        v = str(v).lower()
        if v == "gpu":
            return "cuda"
        if v in ("cpu", "cuda") or v.isdigit():
            return v
        raise ValueError(f"Invalid device: {v!r}. Use cpu / cuda / 0 / 1")

settings = Settings()