"""
S3-совместимое хранилище файлов (MinIO).
"""
import os
import io
from urllib.parse import urlparse

from minio import Minio
from minio.error import S3Error

# Настройки из окружения
ENDPOINT = os.environ.get("S3_ENDPOINT", "localhost:9000")
ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.environ.get("S3_SECRET_KEY", "minioadmin123")
BUCKET = os.environ.get("S3_BUCKET", "vision")

# Убираем http:// из endpoint для Minio client
_endpoint_url = urlparse(f"http://{ENDPOINT}" if not ENDPOINT.startswith("http") else ENDPOINT)
_endpoint_host = _endpoint_url.hostname
_endpoint_port = _endpoint_url.port or 9000

print(f"[Storage] S3_ENDPOINT={ENDPOINT} → connecting to {_endpoint_host}:{_endpoint_port}")

client = Minio(
    f"{_endpoint_host}:{_endpoint_port}",
    access_key=ACCESS_KEY,
    secret_key=SECRET_KEY,
    secure=False,
)

def ensure_bucket():
    """Создать бакет если не существует."""
    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)

def upload_file(object_name: str, file_path: str, content_type: str = "video/mp4") -> str:
    """Загрузить файл с диска в S3. Возвращает object_name."""
    ensure_bucket()
    client.fput_object(
        BUCKET, object_name, file_path, content_type
    )
    return object_name

def upload_bytes(object_name: str, data: bytes, content_type: str = "image/jpeg") -> str:
    """Загрузить байты в S3. Возвращает object_name."""
    ensure_bucket()
    client.put_object(
        BUCKET, object_name, io.BytesIO(data), len(data),
        content_type=content_type,
    )
    return object_name

def download_file(object_name: str, file_path: str) -> str:
    """Скачать файл из S3 на диск. Возвращает путь к файлу."""
    client.fget_object(BUCKET, object_name, file_path)
    return file_path


def get_bytes(object_name: str) -> bytes:
    """Получить файл из S3 как байты."""
    response = client.get_object(BUCKET, object_name)
    try:
        return response.read()
    finally:
        response.close()
        response.release_conn()


def get_presigned_url(object_name: str, expires_hours: int = 1) -> str:
    """Получить временную ссылку на файл."""
    from datetime import timedelta
    return client.presigned_get_object(
        BUCKET, object_name, expires=timedelta(hours=expires_hours)
    )


def delete_file(object_name: str):
    """Удалить файл из S3."""
    client.remove_object(BUCKET, object_name)


def file_exists(object_name: str) -> bool:
    """Проверить, существует ли файл в S3."""
    try:
        client.stat_object(BUCKET, object_name)
        return True
    except S3Error:
        return False


def delete_prefix(prefix: str):
    """Удалить все файлы с префиксом (например, frames/1/)."""
    objects = client.list_objects(BUCKET, prefix=prefix, recursive=True)
    for obj in objects:
        client.remove_object(BUCKET, obj.object_name)


def set_stream_frames_ttl(days: int = 7):
    """
    Установить TTL (автоудаление) для кадров стримов.

    MinIO Lifecycle Rule: все объекты с префиксом "streams/" старше N дней
    будут автоматически удалены. Это предотвращает заполнение диска.

    Почему только streams/?  Потому что видео (videos/) — это загруженные
    пользователем файлы, их нельзя удалять автоматически. А кадры стримов
    — это расходный материал, они теряют актуальность через несколько дней.
    """
    from minio.lifecycleconfig import LifecycleConfig, Rule, Expiration
    from minio.commonconfig import Filter

    config = LifecycleConfig([
        Rule(
            rule_id="stream-frames-ttl",
            status="Enabled",
            rule_filter=Filter(prefix="streams/"),
            expiration=Expiration(days=days),
        ),
    ])
    client.set_bucket_lifecycle(BUCKET, config)
    print(f"[Storage] TTL set: stream frames auto-delete after {days} days")