"""Оркестратор прогона: ramp по ступеням, окна измерения, drain, teardown.

Жизненный цикл прогона:
    prepare → ( применить нагрузку → settle → окно измерения → анализ )×N
    → drain (снять нагрузку, посмотреть разгребание бэклога) → teardown.

Схема подачи: «камеры» добавляются ступенями (N x fps), подача постоянна —
один RTSP-источник, MediaMTX разводит его на всех потребителей.

Все измерения — ретроспективные запросы к Prometheus с явным моментом
`at` (конец окна): окно зафиксировано границами времени, а не моментом
запроса, поэтому анализ шага не соревнуется со следующей ступенью нагрузки.

Отказоустойчивость: сбой (API умер, RTSP упал, Ctrl+C) НЕ теряет прогон —
измеренные шаги попадают в result.json, summary.aborted объясняет причину,
teardown всё равно прибирает за собой.
"""
from __future__ import annotations

import logging
import os
import platform
import time
from pathlib import Path
from typing import Any

from bench.analysis import (
    GROUP_DETECTION,
    GROUP_YOLO,
    analyze_stream_step,
    summarize_run,
)
from bench.api_client import VisionApiClient
from bench.config import ASSETS_DIR, RunConfig
from bench.load_stream import StreamLoadController
from bench.media import MediaError, RtspPublisher, prepare_clip, probe
from bench.models import VERDICT_STABLE, RunResult
from bench.preflight import _gpu_list
from bench.prom_client import (
    PrometheusClient,
    epoch_to_iso,
    now_epoch,
)

logger = logging.getLogger(__name__)

# Клип для RTSP-публикатора: цикл 60с — «камера» с бесконечным контентом.
RTSP_CLIP_SECONDS = 60.0
# Лаг (сообщений), ниже которого очередь считается разгребённой в drain.
DRAIN_LAG_CLEAN = 5.0


# ============================================================================
# Хронология прогона
# ============================================================================
class Timeline:
    """События прогона с эпохальными метками — «как это выглядело» в отчёте."""

    def __init__(self) -> None:
        self._events: list[dict[str, Any]] = []

    def add(self, phase: str, detail: str = "", step: int | None = None) -> float:
        t = now_epoch()
        self._events.append(
            {"t": round(t, 3), "phase": phase, "step": step, "detail": detail}
        )
        suffix = f" (шаг {step})" if step is not None else ""
        logger.info("[%s]%s %s", phase, suffix, detail)
        return t

    @property
    def events(self) -> list[dict[str, Any]]:
        return list(self._events)


def make_run_id(pipeline: str) -> str:
    return f"{pipeline}-{time.strftime('%Y%m%d-%H%M%S')}"


def _sleep_logged(seconds: float, label: str) -> None:
    """Спать с heartbeat'ом: у thorough-профиля окна по 3 минуты."""
    deadline = time.monotonic() + seconds
    next_beat = time.monotonic() + 30.0
    while True:
        left = deadline - time.monotonic()
        if left <= 0:
            return
        time.sleep(min(left, 5.0))
        if time.monotonic() >= next_beat:
            logger.info("%s: осталось %.0fс", label, max(0.0, deadline - time.monotonic()))
            next_beat += 30.0


def _environment(wall_start: float) -> dict:
    # hostname сюда НЕ пишем: отчёты бенча версионируются в репо,
    # настоящий hostname линкует владельца машины. GPU — только модель
    # (без UUID, см. preflight._gpu_list): интерпретировать цифры нужно,
    # а идентификатор устройства в публичном репо не нужен.
    return {
        "system": f"{platform.system()} {platform.release()} ({platform.machine()})",
        "cpu_cores": os.cpu_count() or 1,
        "gpu": _gpu_list() or "не обнаружен (best-effort nvidia-smi)",
        "python": platform.python_version(),
        "wall_s": round(time.time() - wall_start, 1),
        "finished_at": epoch_to_iso(now_epoch()),
    }


def _estimate_wall_s(cfg: RunConfig) -> float:
    """Грубая оценка длительности прогона — чтобы планировал оператор."""
    p = cfg.stream
    # ~35с на развёртывание одного стрима (reconciler + HEALTHY_THRESHOLD).
    apply_s = sum(p.steps) * 35.0 if p.scale == "streams" else len(p.steps) * 40.0
    return apply_s + len(p.steps) * (p.settle_s + p.measure_s) + p.drain_timeout_s


def _fmt_duration(seconds: float) -> str:
    minutes = int(seconds // 60)
    return f"~{minutes} мин" if minutes >= 1 else f"~{seconds:.0f} с"


# ============================================================================
# Стримовый прогон
# ============================================================================
class _StreamRunner:
    """N «камер» с одного RTSP-источника: MediaMTX разводит поток читателям."""

    def __init__(self, cfg: RunConfig, api: VisionApiClient, prom: PrometheusClient,
                 result: RunResult, timeline: Timeline, run_id: str):
        self.cfg = cfg
        self.api = api
        self.prom = prom
        self.result = result
        self.timeline = timeline
        self.run_id = run_id
        self.controller = StreamLoadController(api, cfg.endpoints.rtsp_source_url, run_id)
        self.publisher: RtspPublisher | None = None

    # ------------------------------------------------------------- подготовка
    def prepare(self) -> None:
        if self.cfg.mock:
            self.result.source = {
                "mode": "mock",
                "detail": "синтетический стенд selftest (цифры не про реальную систему)",
            }
            return
        source = Path(self.cfg.source_video)
        if not source.is_file():
            raise MediaError(
                f"источник не найден или не файл: {source} "
                f"(укажи --video путь_к_ролику.mp4)"
            )
        clip = prepare_clip(source, RTSP_CLIP_SECONDS, ASSETS_DIR)
        meta = probe(str(clip))
        self.result.source = {
            "mode": "rtsp",
            "source_video": str(source),
            "clip": clip.name,
            "clip_duration_s": meta.get("duration_s"),
            "clip_fps": meta.get("fps"),
            "resolution": f"{meta.get('width')}x{meta.get('height')}",
            "codec": meta.get("codec"),
            "publish_url": self.cfg.endpoints.rtsp_publish_url,
            "platform_reads": self.cfg.endpoints.rtsp_source_url,
        }
        self.publisher = RtspPublisher(clip, self.cfg.endpoints.rtsp_publish_url)
        self.publisher.start()

    # ------------------------------------------------------------------- ramp
    def ramp(self) -> None:
        profile = self.cfg.stream
        for idx, value in enumerate(profile.steps, start=1):
            t_apply = self.timeline.add("load_apply", f"цель ступени: {value}", step=idx)
            if profile.scale == "streams":
                self.controller.set_streams_count(
                    value, profile.fps, profile.wait_active_timeout_s
                )
                offered = {
                    "streams": value,
                    "fps_per_stream": profile.fps,
                    "offered_fps": float(value * profile.fps),
                }
            else:
                self.controller.set_single_stream_fps(
                    value, profile.wait_active_timeout_s
                )
                offered = {
                    "streams": 1,
                    "fps_per_stream": value,
                    "offered_fps": float(value),
                }
            self.timeline.add(
                "load_applied",
                f"{self.controller.active_count} стрим(ов) активны, "
                f"подача {offered['offered_fps']:.0f} fps, "
                f"развёртывание {now_epoch() - t_apply:.0f}с",
                step=idx,
            )
            _sleep_logged(profile.settle_s, f"шаг {idx}: прогрев")
            t_start = now_epoch()
            self.timeline.add("measure_start", f"окно {profile.measure_s:.0f}с", step=idx)
            _sleep_logged(profile.measure_s, f"шаг {idx}: измерение")
            t_end = now_epoch()
            rec = analyze_stream_step(self.prom, self.cfg, idx, offered, t_start, t_end)
            self.result.steps.append(rec)
            self.timeline.add(
                "step_done",
                f"вердикт {rec.verdict}"
                + (f"; провалено: {', '.join(rec.failed_checks)}" if rec.failed_checks else ""),
                step=idx,
            )

    # ------------------------------------------------------------------ drain
    def drain(self) -> dict:
        profile = self.cfg.stream
        self.timeline.add("drain_start", "снимаю нагрузку, смотрю разгребание бэклога")
        t0 = now_epoch()
        self.controller.stop_all()
        groups = (GROUP_YOLO, GROUP_DETECTION)
        poll_s = max(2.0, self.cfg.scrape_interval_s)
        deadline = t0 + profile.drain_timeout_s
        final: dict[str, float] = {}
        while now_epoch() < deadline:
            final = {
                g: (self.prom.instant_value(
                    f'sum(kafka_consumergroup_lag{{consumergroup="{g}"}})'
                ) or 0.0)
                for g in groups
            }
            if final and all(v <= DRAIN_LAG_CLEAN for v in final.values()):
                break
            time.sleep(poll_s)
        duration = now_epoch() - t0
        cleared = bool(final) and all(v <= DRAIN_LAG_CLEAN for v in final.values())
        detail = (
            f"бэклог разгреблен за {duration:.0f}с" if cleared
            else f"бэклог НЕ разгреблен за {duration:.0f}с: {final}"
        )
        self.timeline.add("drain_end", detail)
        return {
            "description": "сколько система разгребала очереди после снятия нагрузки",
            "duration_s": round(duration, 1),
            "timeout_s": profile.drain_timeout_s,
            "timed_out": not cleared,
            "final_lag": {g: round(v, 1) for g, v in final.items()},
            "cleared": cleared,
        }

    # --------------------------------------------------------------- teardown
    def teardown(self, keep: bool) -> None:
        try:
            if keep:
                logger.warning(
                    "keep_artifacts: оставляю %d стрим(ов) с префиксом bench-cam- "
                    "созданными (статусы смотри в API)", self.controller.active_count,
                )
            else:
                self.controller.stop_all()
                self.controller.delete_all()
        except Exception as exc:  # noqa: BLE001 — зачистка не должна ронять итоги
            logger.error("Ошибка зачистки стримов: %s", exc)
        finally:
            if self.publisher is not None:
                try:
                    self.publisher.stop()
                except Exception as exc:  # noqa: BLE001
                    logger.error("Не удалось остановить RTSP-публикатор: %s", exc)

    def health(self) -> dict:
        error_frames = sum(
            (s.actual.get("error_fps") or 0.0) * float(s.window.get("measure_s") or 0)
            for s in self.result.steps
        )
        saturated = sum(1 for s in self.result.steps if s.verdict != VERDICT_STABLE)
        return {
            "error_frames_approx": round(error_frames),
            "steps_saturated": saturated,
        }


# ============================================================================
# Точка входа
# ============================================================================
def run_benchmark(cfg: RunConfig) -> RunResult:
    """Прогнать бенчмарк по конфигу и вернуть полный RunResult.

    Артефакты на диск пишет вызывающий (cli → reports.persist_run): оркестратор
    знает про нагрузку и измерения, но не про формат отчётов.
    """
    api = VisionApiClient(cfg.endpoints.api_url)
    prom = PrometheusClient(cfg.endpoints.prometheus_url)
    run_id = make_run_id(cfg.pipeline)

    result = RunResult(pipeline=cfg.pipeline, run_id=run_id, label=cfg.label)
    result.created_at = epoch_to_iso(now_epoch())
    result.config = cfg.summary()

    timeline = Timeline()
    wall_start = time.time()
    timeline.add(
        "run_start",
        f"run_id={run_id}, конвейер {cfg.pipeline}, профиль {cfg.profile_name}"
        + (", MOCK-стенд" if cfg.mock else ""),
    )
    logger.info("Оценка длительности прогона: %s", _fmt_duration(_estimate_wall_s(cfg)))

    impl = _StreamRunner(cfg, api, prom, result, timeline, run_id)

    try:
        impl.prepare()
        impl.ramp()
        if cfg.measure_drain:
            result.drain = impl.drain()
        timeline.add("run_end", "нагрузка снята, измерения завершены")
    except KeyboardInterrupt:
        result.summary["aborted"] = "прервано оператором (Ctrl+C)"
        timeline.add("abort", "KeyboardInterrupt")
        logger.error("Прогон прерван — сохраняю измеренные шаги")
    except Exception as exc:  # noqa: BLE001 — частичный результат ценнее стека
        result.summary["aborted"] = f"{type(exc).__name__}: {exc}"
        timeline.add("abort", result.summary["aborted"])
        logger.exception("Прогон упал — сохраняю измеренные шаги")
    finally:
        impl.teardown(cfg.keep_artifacts)
        result.timeline = timeline.events
        result.environment = _environment(wall_start)
        result.summary["health"] = impl.health()
        try:
            summarize_run(result, cfg)
        except Exception as exc:  # noqa: BLE001 — итоги не должны ронять прогон
            logger.error("Не удалось собрать summary: %s", exc)
            result.summary.setdefault("aborted", f"ошибка итогов: {exc}")
    return result
