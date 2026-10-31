"""Selftest: полный прогон бенчмарка против синтетического стенда.

Мок (bench.mock_stack) имеет ИЗВЕСТНУЮ пропускную способность: стримы —
30 fps. Если бенчмарк находит именно эту границу (коллапс на 40 fps),
значит оркестратор, окна измерения, квантили, лаги, вердикты и артефакты
работают правильно. Это проверка самого инструмента без живого docker-стенда.

Запуск: python -m bench selftest   (~1 минута, ничего не поднимает, кроме
локального HTTP-мока на 127.0.0.1).

Артефакты прогона остаются в bench/results/selftest/<run_id>/ — как и у
реальных прогонов: result.json, report.md, load_curve.png.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

from bench.config import RESULTS_DIR, RunConfig, StreamProfile
from bench.mock_stack import MockVisionStack
from bench.models import VERDICT_SATURATED, VERDICT_STABLE
from bench.preflight import run_preflight
from bench.reports import persist_run
from bench.runner import run_benchmark

logger = logging.getLogger(__name__)


def run_selftest() -> int:
    stack = MockVisionStack()
    url = stack.start()
    checks: list[tuple[str, bool, str]] = []
    try:
        checks += _check_preflight(url)
        stream_result = run_benchmark(_stream_cfg(url))
        checks += _stream_checks(stream_result)
        checks += _artifact_checks(stream_result)
        checks += _compare_checks(stream_result)
    finally:
        stack.stop()
    _print_report(checks)
    return 0 if all(ok for _, ok, _ in checks) else 1


# ============================================================================
# Конфиг мок-прогона: короткие окна, емкость мока рассчитана на эти ступени
# ============================================================================
def _stream_cfg(url: str) -> RunConfig:
    cfg = RunConfig(profile_name="selftest", label="selftest", mock=True)
    cfg.endpoints.api_url = url
    cfg.endpoints.prometheus_url = url
    cfg.scrape_interval_s = 1.0
    cfg.results_dir = RESULTS_DIR / "selftest"
    # Ступени 1/2/4 стрима × 10 fps = 10/20/40 fps при ёмкости 30:
    # ожидаем stable, stable, saturated.
    cfg.stream = StreamProfile(
        scale="streams", steps=[1, 2, 4], fps=10,
        wait_active_timeout_s=20.0, settle_s=2.0, measure_s=6.0,
        drain_timeout_s=30.0,
    )
    return cfg


# ============================================================================
# Проверки
# ============================================================================
def _check_preflight(url: str) -> list[tuple[str, bool, str]]:
    try:
        results = run_preflight(_stream_cfg(url))
        failed = [name for name, ok, _ in results if not ok]
        ok = not failed
        detail = f"{len(results)} проверок" + (f", провалены: {', '.join(failed)}" if failed else "")
        return [("preflight против мока: всё зелёное", ok, detail)]
    except Exception as exc:  # noqa: BLE001
        return [("preflight против мока: всё зелёное", False, str(exc))]


def _stream_checks(result) -> list[tuple[str, bool, str]]:
    verdicts = [s.verdict for s in result.steps]
    summary = result.summary
    ms = summary.get("max_sustainable") or {}
    cp = summary.get("collapse_point") or {}
    bn = (summary.get("bottleneck") or {}).get("stage", "")
    drain = result.drain or {}
    return [
        (
            "стрим: вердикты [stable, stable, saturated]",
            verdicts == [VERDICT_STABLE, VERDICT_STABLE, VERDICT_SATURATED],
            str(verdicts),
        ),
        (
            "стрим: максимум = 20 fps (мок: 30 fps, шаг 4x10=40 — коллапс)",
            ms.get("offered") == 20,
            f"получено {ms.get('offered')}",
        ),
        (
            "стрим: коллапс = 40 fps",
            cp.get("offered") == 40,
            f"получено {cp.get('offered')}",
        ),
        ("стрим: узкое место = yolo-consumer", "yolo-consumer" in bn, bn),
        ("стрим: drain разгреб бэклог", bool(drain.get("cleared")),
         f"лаг после: {drain.get('final_lag')}"),
        ("стрим: прогон не прерван", not summary.get("aborted"),
         str(summary.get("aborted") or "ок")),
    ]


def _artifact_checks(result) -> list[tuple[str, bool, str]]:
    out_dir = RESULTS_DIR / "selftest" / result.run_id
    try:
        paths = persist_run(result, out_dir)
        data = json.loads(Path(paths["result_json"]).read_text(encoding="utf-8"))
        ok = (
            data.get("run_id") == result.run_id
            and len(data.get("steps") or []) == len(result.steps)
            and bool(data.get("timeline"))
        )
        checks = [(
            "артефакты: result.json перечитывается",
            ok,
            f"{len(data.get('steps') or [])} шагов, {len(data.get('timeline') or [])} событий",
        )]
        chart = paths.get("chart")
        chart_ok = bool(chart) and Path(chart).stat().st_size > 0
        checks.append(("артефакты: график load_curve.png построен", chart_ok,
                       str(Path(chart).name if chart else "нет")))
        md = Path(paths["report_md"]).read_text(encoding="utf-8")
        md_ok = "## Ступени нагрузки" in md and "## Где упирается" in md
        checks.append(("артефакты: report.md со всеми разделами", md_ok,
                       f"{len(md)} символов"))
    except Exception as exc:  # noqa: BLE001
        checks.append(("артефакты: не записались", False, str(exc)))
    return checks


def _compare_checks(result) -> list[tuple[str, bool, str]]:
    from bench.compare import compare_runs

    try:
        md = compare_runs(result.to_dict(), result.to_dict())
        ok = "## Пропускная способность" in md and "## Узкое место" in md
        return [("compare: отчёт «до → после» строится", ok, f"{len(md)} символов")]
    except Exception as exc:  # noqa: BLE001
        return [("compare: отчёт «до → после» строится", False, str(exc))]


def _print_report(checks: list[tuple[str, bool, str]]) -> None:
    print("\n" + "=" * 70)
    print("SELFTEST vision-bench (мок-стенд: стримы 30 fps)")
    print("=" * 70)
    for name, ok, detail in checks:
        mark = "✅" if ok else "❌"
        print(f"  {mark} {name}" + (f"  [{detail}]" if detail else ""))
    failed = sum(1 for _, ok, _ in checks if not ok)
    verdict = "ВСЕ ПРОВЕРКИ ПРОШЛИ" if not failed else f"ПРОВАЛЕНО: {failed}"
    print("=" * 70)
    print(verdict)
    print("=" * 70 + "\n")
