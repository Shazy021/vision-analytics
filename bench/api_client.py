"""HTTP-клиент к API платформы — ровно те вызовы, что делает пользователь.

Бенчмарк НЕ лезет в БД/Kafka/MinIO напрямую: только публичный REST API.
Все ошибки переводятся в понятные исключения с русским текстом — упавший
стек должен диагностироваться за один взгляд на лог бенчмарка.
"""
from __future__ import annotations

import logging
import time
from typing import Any

import requests

logger = logging.getLogger(__name__)

# Таймауты: connect — коротко (стек локальный), read — с запасом на тяжёлые
# запросы (list_streams с подсчётом детекций под нагрузкой).
CONNECT_TIMEOUT_S = 10.0
DEFAULT_TIMEOUT_S = 30.0


class ApiError(RuntimeError):
    """API недоступен или отвечает не тем, что ожидается."""


class VisionApiClient:
    """Тонкая обёртка над REST API платформы (Streams + служебные проверки)."""

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self._session = requests.Session()
        self._session.headers["User-Agent"] = "vision-bench/1.0"

    # ------------------------------------------------------------------ base
    def _url(self, path: str) -> str:
        return f"{self.base_url}{path}"

    def _request(self, method: str, path: str, *, timeout: float = DEFAULT_TIMEOUT_S,
                 **kwargs) -> Any:
        url = self._url(path)
        try:
            resp = self._session.request(method, url, timeout=timeout, **kwargs)
        except requests.RequestException as exc:
            raise ApiError(f"{method} {url} недоступен: {exc}") from exc
        if resp.status_code >= 500:
            raise ApiError(f"{method} {url} -> HTTP {resp.status_code}: {resp.text[:300]}")
        return resp

    def ping(self) -> None:
        """Проверка живости API (лёгкий запрос, кидает ApiError при проблеме)."""
        try:
            resp = self._request("GET", "/health", timeout=10.0)
            resp.raise_for_status()
        except ApiError:
            raise
        except requests.RequestException as exc:
            raise ApiError(f"API {self.base_url} не отвечает: {exc}") from exc

    # ---------------------------------------------------------------- streams
    def create_stream(self, name: str, rtsp_url: str, fps: int) -> dict:
        resp = self._request(
            "POST", "/api/streams", json={"name": name, "rtsp_url": rtsp_url, "fps": fps}
        )
        if resp.status_code != 201:
            raise ApiError(f"create stream {name!r} -> HTTP {resp.status_code}: {resp.text[:300]}")
        return resp.json()

    def start_stream(self, stream_id: int) -> None:
        resp = self._request("POST", f"/api/streams/{stream_id}/start")
        if resp.status_code != 200:
            raise ApiError(f"start stream {stream_id} -> HTTP {resp.status_code}")

    def stop_stream(self, stream_id: int) -> None:
        resp = self._request("POST", f"/api/streams/{stream_id}/stop")
        if resp.status_code != 200:
            raise ApiError(f"stop stream {stream_id} -> HTTP {resp.status_code}")

    def delete_stream(self, stream_id: int) -> None:
        resp = self._request("DELETE", f"/api/streams/{stream_id}")
        if resp.status_code not in (200, 404):
            raise ApiError(f"DELETE stream {stream_id} -> HTTP {resp.status_code}")

    def list_streams(self) -> list[dict]:
        resp = self._request("GET", "/api/streams")
        resp.raise_for_status()
        return resp.json()

    def wait_stream_active(self, stream_id: int, timeout_s: float,
                           poll_s: float = 3.0) -> str:
        """Дождаться статуса active (или error) — reconcile-цикл платформы
        подтверждает active только после HEALTHY_THRESHOLD=30с жизни producer.
        Возвращает финальный статус; кидает ApiError при error/таймауте.
        """
        deadline = time.monotonic() + timeout_s
        last = "?"
        while time.monotonic() < deadline:
            for s in self.list_streams():
                if s["id"] == stream_id:
                    last = s["status"]
                    if last == "active":
                        return last
                    if last == "error":
                        raise ApiError(
                            f"стрим {stream_id} перешёл в error — проверь "
                            f"RTSP-источник и логи: docker compose logs stream-manager"
                        )
                    break
            time.sleep(poll_s)
        raise ApiError(
            f"стрим {stream_id} не стал active за {timeout_s:.0f}с (последний статус: {last}). "
            f"Reconciler поднимает producer ~30с; проверь docker compose ps stream-manager"
        )
