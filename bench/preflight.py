"""Preflight: быстрые проверки стенда ДО прогона — fail fast с русским текстом.

Каждая проверка возвращает (имя, ok, деталь). CLI печатает таблицу и
возвращает ненулевой код, если что-то не поднято. Экономит полчаса
разбирательств «почему все шаги saturated» на лежащем Prometheus.
"""
from __future__ import annotations

import logging
import platform
import subprocess

from bench.api_client import ApiError, VisionApiClient
from bench.config import RunConfig
from bench.media import ffmpeg_available, probe
from bench.prom_client import PromError, PrometheusClient

logger = logging.getLogger(__name__)

# Джобы Prometheus, без которых измерения невозможны (метрики этих стадий
# живут в соответствующих процессах; см. monitoring/prometheus/prometheus.yml).
REQUIRED_PROM_JOBS = ("yolo-consumer", "detection-consumer", "kafka-exporter")


def run_preflight(cfg: RunConfig) -> list[tuple[str, bool, str]]:
    """Набор проверок стенда; все — снаружи, через публичные интерфейсы."""
    checks: list[tuple[str, bool, str]] = []
    api = VisionApiClient(cfg.endpoints.api_url)
    prom = PrometheusClient(cfg.endpoints.prometheus_url)

    # --- API ---------------------------------------------------------------
    try:
        api.ping()
        checks.append(("API доступен", True, cfg.endpoints.api_url))
    except (ApiError, Exception) as exc:  # noqa: BLE001 — текст проверки важнее типа
        checks.append(("API доступен", False, str(exc)))

    # --- Prometheus + таргеты ----------------------------------------------
    try:
        prom.ping()
        jobs = prom.up_jobs()
        missing = [j for j in REQUIRED_PROM_JOBS if jobs.get(j, 0) < 1]
        if missing:
            checks.append((
                "Prometheus таргеты",
                False,
                f"не скрейпятся: {', '.join(missing)} — проверь "
                f"docker compose ps и http://localhost:9090/targets",
            ))
        else:
            up_count = sum(1 for v in jobs.values() if v >= 1)
            checks.append(("Prometheus таргеты", True, f"все {up_count} джоб живы"))
    except PromError as exc:
        checks.append(("Prometheus таргеты", False, str(exc)))

    # --- ffmpeg / MediaMTX (только для реального прогона, не selftest) ------
    if not cfg.mock:
        checks.append((
            "ffmpeg на хосте",
            ffmpeg_available(),
            "нужен для публикации RTSP-источника (см. README платформы)",
        ))
        if cfg.source_video:
            try:
                meta = probe(cfg.source_video)
                checks.append((
                    "Источник-ролик читается",
                    True,
                    f"{meta.get('duration_s', 0):.0f}с, {meta.get('width')}x"
                    f"{meta.get('height')}, {meta.get('fps', 0):.0f} fps, "
                    f"{meta.get('codec')}",
                ))
            except Exception as exc:  # noqa: BLE001
                checks.append(("Источник-ролик читается", False, str(exc)))
        else:
            checks.append((
                "Источник-ролик задан",
                False,
                "укажи --video путь_к_ролику.mp4 (нужен один и тот же ролик "
                "для сравнимости прогонов)",
            ))

    # --- окружение -----------------------------------------------------------
    checks.append((
        "Окружение",
        True,
        f"{platform.system()} {platform.release()} | "
        f"CPU: {platform.machine()}, ядер: {_cpu_count()} | GPU: {_gpu_list() or 'нет/не виден'}",
    ))
    return checks


def _cpu_count() -> int:
    import os

    return os.cpu_count() or 1


def _gpu_list() -> str:
    """best-effort: модели GPU — сравнение прогонов на разных машинах.

    Только name, не `nvidia-smi -L`: последний печатает UUID устройства —
    serial-derived fingerprint, которому не место в публичном репо.
    """
    try:
        out = subprocess.run(
            ["nvidia-smi", "--query-gpu=name", "--format=csv,noheader"],
            capture_output=True, text=True, timeout=10,
        )
        if out.returncode == 0 and out.stdout.strip():
            return ", ".join(line.strip() for line in out.stdout.strip().splitlines())
    except (OSError, subprocess.SubprocessError):
        pass
    return ""
