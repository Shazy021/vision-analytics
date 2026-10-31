"""Конфигурация бенчмарка: эндпоинты, критерии устойчивости, профили нагрузки.

Приоритет источника значения: CLI-флаг > переменная окружения > дефолт.
Адреса берутся из окружения, чтобы один и тот же инструмент работал и против
локального docker-стека (localhost), и против удалённого стенда.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = REPO_ROOT / "bench" / "results"
ASSETS_DIR = REPO_ROOT / "bench" / ".assets"


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default).rstrip("/")


# ============================================================================
# Эндпоинты тестируемой системы
# ============================================================================
@dataclass
class Endpoints:
    """Адреса тестируемого стенда — всё, что нужно бенчмарку снаружи."""

    api_url: str = field(
        default_factory=lambda: _env("BENCH_API_URL", "http://localhost:8000")
    )
    prometheus_url: str = field(
        default_factory=lambda: _env("BENCH_PROM_URL", "http://localhost:9090")
    )
    # Куда ffmpeg на ХОСТЕ пушит тестовый ролик (MediaMTX принимает RTSP).
    rtsp_publish_url: str = field(
        default_factory=lambda: _env("BENCH_RTSP_PUBLISH", "rtsp://localhost:8554/bench/src")
    )
    # Что читает producer ВНУТРИ docker-сети (адрес должен резолвиться из
    # контейнера stream-manager: mediamtx, не localhost — см. README платформы).
    rtsp_source_url: str = field(
        default_factory=lambda: _env("BENCH_RTSP_SOURCE", "rtsp://mediamtx:8554/bench/src")
    )


# ============================================================================
# Критерий «система держит нагрузку» на шаге
# ============================================================================
@dataclass
class Criteria:
    """Пороговые значения вердикта устойчивости (все — на окне измерения).

    Стримы: e2e p95 (захват кадра → коммит в БД) в бюджете И лаги стабильны
    И обработка догоняет подачу (rate_match по приём/обработка).
    """

    # Budget на frame_e2e_latency_seconds p95.
    stream_e2e_p95_budget_s: float = 2.0
    # Обработка догоняет подачу: rate(обработано) >= rate(получено) * min.
    rate_match_min: float = 0.95
    # Лаг Kafka не растёт быстрее, чем столько сообщений/с.
    lag_slope_max_per_s: float = 0.5


# ============================================================================
# Профили нагрузки
# ============================================================================
@dataclass
class StreamProfile:
    """Профиль стримового пайплайна.

    scale="streams": ступени — ЧИСЛО активных стримов, каждый с fps.
        Реалистичная модель «добавляем камеры»: offered = N x fps.
    scale="fps": ступени — FPS одного стрима (стрим пересоздаётся на каждую
        ступень). Чище изолирует yolo-consumer, но упирается в fps источника
        (producer не может сэмплировать чаще, чем кадры приходят).
    """

    scale: str = "streams"
    steps: list[int] = field(default_factory=lambda: [2, 4, 6, 8, 12, 16, 24])
    fps: int = 5
    # Ожидание статуса active у стримов ограничено отдельно: reconciler
    # платформы подтверждает active только через HEALTHY_THRESHOLD=30с.
    wait_active_timeout_s: float = 90.0
    # Стабилизация ПОСЛЕ active: producer'ы разогнались, транзиентные
    # очереди от старта рассосались.
    settle_s: float = 25.0
    # Окно измерения: по нему считаются rate'ы, квантили и тренды лага.
    measure_s: float = 90.0
    drain_timeout_s: float = 180.0


PROFILES: dict[str, dict[str, object]] = {
    "quick": {
        "stream": StreamProfile(
            steps=[1, 2, 4, 8], settle_s=20.0, measure_s=45.0, drain_timeout_s=120.0
        ),
    },
    "default": {
        "stream": StreamProfile(
            steps=[2, 4, 6, 8, 12, 16, 24], settle_s=25.0, measure_s=90.0
        ),
    },
    "thorough": {
        "stream": StreamProfile(
            steps=[1, 2, 3, 4, 6, 8, 12, 16, 24, 32, 48],
            settle_s=40.0,
            measure_s=180.0,
        ),
    },
}


@dataclass
class RunConfig:
    """Полная конфигурация одного прогона бенчмарка (стримовый конвейер)."""

    pipeline: str = "stream"
    profile_name: str = "default"
    label: str = ""
    notes: str = ""
    source_video: str = ""
    endpoints: Endpoints = field(default_factory=Endpoints)
    criteria: Criteria = field(default_factory=Criteria)
    stream: StreamProfile = field(default_factory=StreamProfile)
    # Интервал скрейпа Prometheus стенда (из prometheus.yml: 15s). Окна rate()
    # бенчмарк строит от него: rate-окно >= 4 скрейпа — иначе rate шумит.
    scrape_interval_s: float = 15.0
    # Само-тест/mock-режим: без MediaMTX/ffmpeg, против встроенного мока.
    mock: bool = False
    # Не удалять созданные стримы по завершении (для ручной диагностики).
    keep_artifacts: bool = False
    # Замерять фазу drain (сколько система разгребает бэклог после стопа).
    measure_drain: bool = True
    results_dir: Path = RESULTS_DIR

    def rate_window_s(self) -> float:
        """Окно для rate(): 4 скрейпа Prometheus, но не меньше 5с (selftest)."""
        return max(5.0, 4 * self.scrape_interval_s)

    def summary(self) -> dict:
        """Человекочитаемая сводка конфига для отчёта."""
        return {
            "pipeline": self.pipeline,
            "profile": self.profile_name,
            "label": self.label,
            "notes": self.notes,
            "source_video": self.source_video,
            "mock": self.mock,
            "keep_artifacts": self.keep_artifacts,
            "measure_drain": self.measure_drain,
            "scrape_interval_s": self.scrape_interval_s,
            "criteria": {
                "stream_e2e_p95_budget_s": self.criteria.stream_e2e_p95_budget_s,
                "rate_match_min": self.criteria.rate_match_min,
                "lag_slope_max_per_s": self.criteria.lag_slope_max_per_s,
            },
            "endpoints": {
                "api": self.endpoints.api_url,
                "prometheus": self.endpoints.prometheus_url,
                "rtsp_publish": self.endpoints.rtsp_publish_url,
                "rtsp_source": self.endpoints.rtsp_source_url,
            },
            "stream_profile": {
                "scale": self.stream.scale,
                "steps": self.stream.steps,
                "fps": self.stream.fps,
                "settle_s": self.stream.settle_s,
                "measure_s": self.stream.measure_s,
            },
        }
