"""Управление стримовой нагрузкой через API — «добавляем камеры».

Два режима масштабирования (см. StreamProfile.scale):
  streams — держим N активных стримов, каждый читает один RTSP-путь
            (MediaMTX разводит один публикуемый поток на всех читателей);
  fps     — ОДИН стрим, на каждой ступени пересоздаём его с бо́льшим fps
            (fps — атрибут создания стрима, поменять на лету API не умеет).

Все созданные стримы именуются с префиксом BENCH_PREFIX — cleanup находит
свои артефакты по имени и никогда не трогает пользовательские камеры.
"""
from __future__ import annotations

import logging

from bench.api_client import ApiError, VisionApiClient

logger = logging.getLogger(__name__)

BENCH_PREFIX = "bench-cam-"


class StreamLoadError(RuntimeError):
    """Не удалось выставить требуемую нагрузку."""


class StreamLoadController:
    """Текущее состояние нагрузки + переходы между ступенями."""

    def __init__(self, api: VisionApiClient, rtsp_source_url: str, run_id: str):
        self.api = api
        self.rtsp_source_url = rtsp_source_url
        self.run_id = run_id
        # stream_id -> {"name", "fps"}: активные стримы этого прогона
        self._active: dict[int, dict] = {}

    # ------------------------------------------------------------------ info
    @property
    def active_count(self) -> int:
        return len(self._active)

    @property
    def current_fps(self) -> int | None:
        if not self._active:
            return None
        return next(iter(self._active.values()))["fps"]

    def offered_fps(self) -> float:
        return sum(s["fps"] for s in self._active.values())

    # --------------------------------------------------------------- actions
    def _create_and_start(self, fps: int, name: str, wait_active_timeout_s: float) -> int:
        stream = self.api.create_stream(name=name, rtsp_url=self.rtsp_source_url, fps=fps)
        sid = stream["id"]
        self.api.start_stream(sid)
        # starting → active занимает >=30с (HEALTHY_THRESHOLD платформы):
        # это честное время разворота нагрузки, а не бенчмарковский оверхед.
        self.api.wait_stream_active(sid, timeout_s=wait_active_timeout_s)
        self._active[sid] = {"name": name, "fps": fps}
        logger.info("Стрим %s (%s, %d fps) активен", sid, name, fps)
        return sid

    def set_streams_count(self, count: int, fps: int,
                          wait_active_timeout_s: float) -> None:
        """Режим streams: довести число активных стримов до count."""
        while self.active_count < count:
            idx = self.active_count + 1
            name = f"{BENCH_PREFIX}{self.run_id}-{idx:02d}"
            self._create_and_start(fps, name, wait_active_timeout_s)
        if self.active_count > count:
            raise StreamLoadError(
                f"снижение числа стримов в режиме streams не поддерживается "
                f"(active={self.active_count}, requested={count})"
            )

    def set_single_stream_fps(self, fps: int, wait_active_timeout_s: float) -> None:
        """Режим fps: пересоздать единственный стрим с новым fps."""
        self.stop_all()
        name = f"{BENCH_PREFIX}{self.run_id}-fps{fps}"
        self._create_and_start(fps, name, wait_active_timeout_s)

    # -------------------------------------------------------------- teardown
    def stop_all(self) -> None:
        for sid in list(self._active):
            try:
                self.api.stop_stream(sid)
            except ApiError:
                logger.warning("Не удалось остановить стрим %s — продолжаю", sid)
        self._active.clear()

    def delete_all(self) -> None:
        """Удалить ВСЕ свои стримы (по префиксу имени, включая брошенные)."""
        try:
            streams = self.api.list_streams()
        except ApiError:
            logger.warning("list_streams не удался — пропуск удаления")
            return
        victims = [
            s for s in streams
            if str(s.get("name", "")).startswith(BENCH_PREFIX)
            or s["id"] in self._active
        ]
        for s in victims:
            try:
                self.api.delete_stream(s["id"])
                logger.info("Стрим %s (%s) удалён", s["id"], s.get("name"))
            except ApiError:
                logger.warning("Не удалось удалить стрим %s — продолжаю", s["id"])
        self._active.clear()
