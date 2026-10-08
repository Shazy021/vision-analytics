"""
S3-совместимое хранилище файлов (MinIO).

Все параметры берутся из единого конфига (app.core.config.settings):
S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET.
"""
import io
import logging
from urllib.parse import urlparse

import urllib3
from minio import Minio
from minio.error import S3Error

from app.core.config import settings

logger = logging.getLogger(__name__)

BUCKET = settings.s3_bucket

# Убираем http:// из endpoint для Minio client
_endpoint_url = urlparse(
    settings.s3_endpoint
    if settings.s3_endpoint.startswith("http")
    else f"http://{settings.s3_endpoint}"
)
_endpoint_host = _endpoint_url.hostname
_endpoint_port = _endpoint_url.port or 9000

logger.info(f"[Storage] S3: {_endpoint_host}:{_endpoint_port}, bucket={BUCKET}")

# Явные таймауты и лимит ретраев: с http_client по умолчанию minio-py
# выжидает полный urllib3-бэкофф (~6+ сек на вызов), и старт приложения
# без MinIO подвисает на десятки секунд. 2 сек на подключение + 2 попытки
# — MinIO недоступен становится понятно сразу, lifespan это переживает.
_HTTP_CLIENT = urllib3.PoolManager(
    timeout=urllib3.Timeout(connect=2.0, read=30.0),
    retries=urllib3.Retry(
        total=2,
        backoff_factor=0.2,
        status_forcelist=[500, 502, 503, 504],
    ),
)

client = Minio(
    f"{_endpoint_host}:{_endpoint_port}",
    access_key=settings.s3_access_key,
    secret_key=settings.s3_secret_key,
    secure=False,
    http_client=_HTTP_CLIENT,
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
    from minio.commonconfig import Filter
    from minio.lifecycleconfig import Expiration, LifecycleConfig, Rule

    config = LifecycleConfig([
        Rule(
            rule_id="stream-frames-ttl",
            status="Enabled",
            rule_filter=Filter(prefix="streams/"),
            expiration=Expiration(days=days),
        ),
    ])
    client.set_bucket_lifecycle(BUCKET, config)
    logger.info(f"[Storage] TTL set: stream frames auto-delete after {days} days")
