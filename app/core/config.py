from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    database_url: str = "postgresql://user:pass@localhost:5432/vision"
    weights_path: str = "yolov8n.pt"         # n = nano, самая маленькая и быстрая модель
    device: str = "gpu"                     # "cuda" если есть NVIDIA GPU
    extract_fps: int = 5   

    class Config:
        env_file = ".env"

settings = Settings()