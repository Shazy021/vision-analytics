"""Анализ: измерение шага, вердикт устойчивости, атрибуция узкого места.

Идея атрибуции — «бюджет времени ключевой латентности». У стримов это e2e
(захват кадра → коммит в БД); она раскладывается на стадии, чья доля
доминирует ИЛИ чья очередь растёт — тот и упирается. Все цифры берутся
из Prometheus окном за шаг — внешне, без единого изменения в коде платформы.

Все «скорости» считаются приростом счётчиков РОВНО за окно измерения
(increase за [начало, конец] на момент конца окна), а не rate-сериями со
скользящим окном: такое окно в первые секунды шага затаскивает хвост
предыдущей ступени нагрузки и занижает подачу.
"""
from __future__ import annotations

import logging

from bench.config import RunConfig
from bench.models import (
    VERDICT_SATURATED,
    VERDICT_STABLE,
    RunResult,
    StepRecord,
)
from bench.prom_client import (
    PrometheusClient,
    histogram_stats,
    safe_div,
    series_last,
    series_slope,
)

logger = logging.getLogger(__name__)

# Группы консьюмеров платформы (group.id в app/consumers/*.py).
GROUP_YOLO = "yolo-workers"
GROUP_DETECTION = "detection-writer"

# Красивые имена стадий для отчётов.
STAGE_NAMES = {
    "queue_raw_frames": "ожидание в raw_frames (бэклог до yolo-consumer)",
    "decode": "декодирование JPEG",
    "inference": "YOLO-инференс",
    "queue_detections": "ожидание в detections (бэклог до записи)",
    "write": "запись в PostgreSQL",
}


# ============================================================================
# Измерение шага
# ============================================================================
def _window_stats(prom: PrometheusClient, metric: str, window_s: float, at: float,
                  selector: str = "") -> dict | None:
    """Квантили/среднее гистограммы за окно; None, если стадия молчала."""
    buckets = prom.window_histogram(metric, window_s, at, selector)
    if buckets is None:
        return None
    stats = histogram_stats(buckets)
    return {k: (round(v, 4) if isinstance(v, float) else v) for k, v in stats.items()}


def _lag_metrics(prom: PrometheusClient, groups: list[str],
                 t_start: float, t_end: float, step_s: float) -> dict:
    """Тренд лага по каждой группе: slope (сообщ/с), конец и максимум окна.

    Лаг — гейдж (текущее значение), а не счётчик: окно не «приносится»
    скользящей функцией, поэтому [t_start, t_end] читается как есть.
    """
    out = {}
    for group in groups:
        series = prom.range_series(
            f'sum(kafka_consumergroup_lag{{consumergroup="{group}"}})',
            t_start, t_end, step_s,
        )
        slope = series_slope(series)
        out[group] = {
            "slope_per_s": round(slope, 3) if slope is not None else None,
            "end": series_last(series),
            "max": max((v for _, v in series), default=None),
        }
    return out


def analyze_stream_step(
    prom: PrometheusClient,
    cfg: RunConfig,
    index: int,
    offered: dict,
    t_start: float,
    t_end: float,
) -> StepRecord:
    """Измерить стримовый шаг на окне [t_start, t_end] (окно измерения)."""
    window_s = t_end - t_start
    step_q = max(1.0, cfg.scrape_interval_s)

    received_total = prom.counter_window_delta("frames_received_total", window_s, t_end)
    processed_total = prom.counter_window_delta(
        "frame_processed_total", window_s, t_end, selector='{result="ok"}'
    )
    errors_total = prom.counter_window_delta(
        "frame_processed_total", window_s, t_end, selector='{result="error"}'
    )
    received_m = safe_div(received_total, window_s)
    processed_m = safe_div(processed_total, window_s)
    errors_m = safe_div(errors_total, window_s)

    lag = _lag_metrics(prom, [GROUP_YOLO, GROUP_DETECTION], t_start, t_end, step_q)
    e2e = _window_stats(prom, "frame_e2e_latency_seconds", window_s, t_end)
    stages = {
        "queue_raw_frames": _window_stats(
            prom, "frame_queue_wait_seconds", window_s, t_end,
            selector='{topic="raw_frames"}',
        ),
        "decode": _window_stats(prom, "frame_decode_duration_seconds", window_s, t_end),
        "inference": _window_stats(prom, "frame_inference_duration_seconds", window_s, t_end),
        "queue_detections": _window_stats(
            prom, "frame_queue_wait_seconds", window_s, t_end,
            selector='{topic="detections"}',
        ),
        "write": _window_stats(prom, "frame_write_duration_seconds", window_s, t_end),
    }

    rec = StepRecord(index=index, offered=offered)
    rec.actual = {
        "received_fps": round(received_m, 2) if received_m is not None else None,
        "processed_fps": round(processed_m, 2) if processed_m is not None else None,
        "error_fps": round(errors_m, 2) if errors_m is not None else None,
    }
    rec.lag = lag
    rec.latency = {
        "metric": "frame_e2e_latency_seconds (захват кадра → коммит в БД)",
        "p50": (e2e or {}).get("p50"),
        "p95": (e2e or {}).get("p95"),
        "p99": (e2e or {}).get("p99"),
        "budget_s": cfg.criteria.stream_e2e_p95_budget_s,
    }
    rec.stages = stages
    rec.window = {"t_start": t_start, "t_end": t_end, "measure_s": round(window_s)}

    # --- квитанции критериев ---------------------------------------------
    rate_match = (
        received_m is not None and processed_m is not None
        and processed_m >= cfg.criteria.rate_match_min * received_m
    )
    lag_stable = all(
        (g["slope_per_s"] is not None and g["slope_per_s"] <= cfg.criteria.lag_slope_max_per_s)
        for g in lag.values()
    ) and bool(lag)
    p95 = (e2e or {}).get("p95")
    latency_ok = p95 is not None and p95 <= cfg.criteria.stream_e2e_p95_budget_s

    rec.checks = {
        "rate_match": rate_match,
        "lag_stable": lag_stable,
        "latency_budget": latency_ok,
    }
    rec.failed_checks = [k for k, v in rec.checks.items() if not v]
    rec.verdict = VERDICT_STABLE if not rec.failed_checks else VERDICT_SATURATED
    _log_step(rec)
    return rec


def _log_step(rec: StepRecord) -> None:
    lat = rec.latency
    logger.info(
        "Шаг %d [%s]: факт=%s, p95=%sс, лаги=%s → %s%s",
        rec.index,
        rec.verdict.upper(),
        rec.actual.get("received_fps"),
        lat.get("p95"),
        {g: v.get("slope_per_s") for g, v in rec.lag.items()},
        rec.verdict,
        f" (провалено: {', '.join(rec.failed_checks)})" if rec.failed_checks else "",
    )


# ============================================================================
# Итоги прогона: max sustainable, точка коллапса, узкое место
# ============================================================================
def summarize_run(result: RunResult, cfg: RunConfig) -> None:
    """Заполнить result.summary: устойчивый максимум, коллапс, bottleneck."""
    steps = result.steps
    if not steps:
        result.summary = {"error": "нет измеренных шагов"}
        return

    max_stable = result.max_stable_step
    collapse = result.collapse_step
    not_saturated = collapse is None

    # Ранний провал: нестабилен шаг, который ДОЛЖЕН быть лёгким, — аномалия
    # (обычно: стек не прогрелся или что-то упало). Прогон при этом всё равно
    # осмыслен, но предупреждаем.
    early_failure = None
    if collapse is not None and max_stable is not None and collapse.index < max_stable.index:
        early_failure = collapse.index

    summary: dict = {
        "max_sustainable": {
            "step": max_stable.index if max_stable else None,
            "offered": result.offered_units(max_stable) if max_stable else None,
            "units": "fps",
        },
        "collapse_point": {
            "step": collapse.index if collapse else None,
            "offered": result.offered_units(collapse) if collapse else None,
        },
        "not_saturated": not_saturated,
        "early_failure_step": early_failure,
    }
    summary["bottleneck"] = _bottleneck_stream(steps, collapse, max_stable, cfg)

    # Здоровье и причина прерывания — накоплены раннее, переносим в итоги.
    for key in ("health", "aborted"):
        if result.summary.get(key):
            summary[key] = result.summary[key]
    result.summary = summary


def _stage_mean(stages: dict, key: str) -> float | None:
    st = stages.get(key)
    if not st or st.get("mean") is None:
        return None
    return st["mean"]


def _budget_shares(budget: dict[str, float | None]) -> list[tuple[str, float]]:
    """Доли стадий в сумме бюджета; стадии без данных пропускаем."""
    total = sum(v for v in budget.values() if v is not None)
    if total <= 0:
        return []
    return sorted(
        ((k, v / total) for k, v in budget.items() if v is not None),
        key=lambda kv: kv[1],
        reverse=True,
    )


def _bottleneck_stream(steps: list[StepRecord], collapse: StepRecord | None,
                       max_stable: StepRecord | None, cfg: RunConfig) -> dict:
    """Узкое место стримового тракта.

    Логика: точка коллапса говорит ГДЕ сломалось (какая очередь выросла),
    бюджет e2e на устойчивом максимуме говорит ЧТО дорожает (какая стадия
    ест время). Вместе — ответ «где именно упирается».
    """
    ref = collapse or max_stable
    if ref is None:
        return {"stage": "unknown", "evidence": ["нет данных"]}

    evidence: list[str] = []
    stage = None
    detail = ""

    if collapse is not None:
        yolo_slope = (collapse.lag.get(GROUP_YOLO) or {}).get("slope_per_s")
        det_slope = (collapse.lag.get(GROUP_DETECTION) or {}).get("slope_per_s")
        if yolo_slope is not None and yolo_slope > cfg.criteria.lag_slope_max_per_s:
            stage = "yolo-consumer"
            q_share = _share(collapse.stages, "queue_raw_frames")
            dec = _stage_mean(collapse.stages, "decode")
            inf = _stage_mean(collapse.stages, "inference")
            if dec is not None and inf is not None:
                inner = "инференс" if inf >= dec else "декодирование"
                detail = (
                    f"внутри стадии: {inner} ({'inference' if inf >= dec else 'decode'} "
                    f"{max(inf, dec):.3f}с против {min(inf, dec):.3f}с на кадр)"
                )
            evidence.append(
                f"лаг {GROUP_YOLO} растёт на {yolo_slope:+.1f} сообщ/с — "
                f"консьюмер не успевает за подачей"
            )
            if q_share is not None:
                evidence.append(
                    f"бэклог до yolo-consumer: {q_share * 100:.0f}% бюджета e2e"
                )
        elif det_slope is not None and det_slope > cfg.criteria.lag_slope_max_per_s:
            stage = "detection-consumer (запись в PostgreSQL)"
            evidence.append(
                f"лаг {GROUP_DETECTION} растёт на {det_slope:+.1f} сообщ/с — "
                f"писатель не успевает"
            )
        else:
            stage = "латентность без роста лагов"
            evidence.append(
                "коллапс по бюджету латентности при стабильных лагах — "
                "проверь e2e-состав ниже"
            )

    if stage is None:
        stage = "главная статья бюджета e2e (система не насыщена)"

    # Бюджет e2e на опорном шаге — всегда в доказательствах.
    budget = {
        "queue_raw_frames": _stage_mean(ref.stages, "queue_raw_frames"),
        "decode": _stage_mean(ref.stages, "decode"),
        "inference": _stage_mean(ref.stages, "inference"),
        "queue_detections": _stage_mean(ref.stages, "queue_detections"),
        "write": _stage_mean(ref.stages, "write"),
    }
    shares = _budget_shares(budget)
    top = shares[0] if shares else None
    if top:
        evidence.append(
            f"на шаге {ref.index}: {STAGE_NAMES.get(top[0], top[0])} — "
            f"{top[1] * 100:.0f}% бюджета e2e"
        )
    if detail:
        evidence.append(detail)

    return {
        "stage": stage,
        "reference_step": ref.index,
        "e2e_budget_shares": {k: round(v, 3) for k, v in shares},
        "evidence": evidence,
    }


def _share(stages: dict, key: str) -> float | None:
    """Доля стадии в сумме известных средних (для одного шага)."""
    vals = [st.get("mean") for st in stages.values() if isinstance(st, dict)]
    total = sum(v for v in vals if v is not None)
    if total <= 0:
        return None
    mean = (stages.get(key) or {}).get("mean")
    return mean / total if mean is not None else None
