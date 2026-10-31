"""Артефакты прогона: result.json, report.md, график load_curve.png.

Отчёт собирается ТОЛЬКО из словаря result.json — поэтому его можно
перегенерировать для старых прогонов и работать без живого стенда.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from bench.analysis import STAGE_NAMES
from bench.models import VERDICT_STABLE, RunResult
from bench.prom_client import fmt_s

logger = logging.getLogger(__name__)

# ============================================================================
# Публичная точка входа
# ============================================================================
def persist_run(result: RunResult | dict, out_dir: Path) -> dict[str, Path | None]:
    """Записать все артефакты прогона в out_dir; вернуть пути.

    Порядок важен: result.json — первичен (даже без графика и отчёта прогон
    не теряется), график и report.md — обёрнуты в защиту от сбоев.
    """
    data = result.to_dict() if isinstance(result, RunResult) else dict(result)
    out_dir.mkdir(parents=True, exist_ok=True)

    json_path = out_dir / "result.json"
    json_path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    chart: Path | None = None
    try:
        chart = out_dir / "load_curve.png"
        _plot_load_curve(data, chart)
    except Exception as exc:  # noqa: BLE001 — отчёт без графика лучше, чем ничего
        logger.warning("График не построен (%s) — отчёт будет без него", exc)
        chart = None

    md_path = out_dir / "report.md"
    md_path.write_text(render_report(data), encoding="utf-8")
    logger.info("Артефакты прогона: %s", out_dir)
    return {"result_json": json_path, "report_md": md_path, "chart": chart}


# ============================================================================
# Markdown-отчёт
# ============================================================================
def render_report(d: dict) -> str:
    lines: list[str] = []
    add = lines.append
    steps: list[dict] = d.get("steps") or []
    cfg: dict = d.get("config") or {}
    summary: dict = d.get("summary") or {}
    env: dict = d.get("environment") or {}
    criteria: dict = cfg.get("criteria") or {}

    add("# Бенчмарк «Vision Analytics» — стримы")
    add("")
    add(f"- **run_id:** `{d.get('run_id', '')}`"
        + (f" · label: **{d['label']}**" if d.get("label") else ""))
    add(f"- **Профиль:** {cfg.get('profile', '')} · **Дата:** {d.get('created_at', '')}")
    if cfg.get("notes"):
        add(f"- **Заметки оператора:** {cfg['notes']}")
    if env:
        add(f"- **Стенд:** {env.get('system')} · "
            f"ядер {env.get('cpu_cores')} · GPU: {env.get('gpu')} · "
            f"длительность {env.get('wall_s', 0):.0f}с")
    if cfg.get("mock"):
        add("- ⚠️ **Режим MOCK**: синтетический стенд selftest — цифры не про "
            "реальную систему, они про корректность самого бенчмарка")
    if summary.get("aborted"):
        add(f"- ⚠️ **Прогон прерван:** {summary['aborted']} — результаты частичные")

    # ------------------------------------------------------------ главный ответ
    add("")
    add("## Главный ответ")
    add("")
    ms = summary.get("max_sustainable") or {}
    cp = summary.get("collapse_point") or {}
    max_rec = _step_by_index(steps, ms.get("step"))
    collapse_rec = _step_by_index(steps, cp.get("step"))

    if ms.get("offered") is not None:
        detail = ""
        if max_rec:
            o = max_rec.get("offered") or {}
            detail = f" — это {o.get('streams')} стрим(ов) × {o.get('fps_per_stream')} fps"
        add(f"- **Устойчивый максимум: {ms['offered']:g} fps** "
            f"(шаг {ms.get('step')}){detail}")
    else:
        add("- **Устойчивый максимум не найден** — не прошёл ни один шаг "
            "(стенд не прогрет или лежит? смотри preflight)")
    if cp.get("offered") is not None:
        failed = collapse_rec.get("failed_checks") if collapse_rec else None
        add(f"- **Точка коллапса: {cp['offered']:g} fps** (шаг {cp.get('step')})"
            + (f" — провалено: {', '.join(failed)}" if failed else ""))
    elif steps:
        top = steps[-1]
        add(f"- **Система НЕ насыщена** в проверенном диапазоне "
            f"(до {_offered(top):g} fps) — расширяй ступени")

    # -------------------------------------------------------------- узкое место
    bn = summary.get("bottleneck") or {}
    add("")
    add("## Где упирается")
    add("")
    stage = bn.get("stage", "—")
    ref = f" (по данным шага {bn.get('reference_step')})" if bn.get("reference_step") else ""
    add(f"**{stage}**{ref}")
    add("")
    for ev in bn.get("evidence") or []:
        add(f"- {ev}")
    shares = bn.get("e2e_budget_shares") or {}
    if shares:
        add("")
        add("Доли бюджета e2e на опорном шаге:")
        add("")
        add("| Стадия | Доля |")
        add("|---|---|")
        for key, share in shares.items():
            add(f"| {STAGE_NAMES.get(key, key)} | {share * 100:.0f}% |")

    # ---------------------------------------------------------------- ступени
    add("")
    add("## Ступени нагрузки")
    add("")
    add("| # | стримы | подача fps | приём fps | обработка fps | e2e p50 | "
    "e2e p95 | e2e p99 | лаг yolo / запись, сообщ/с | вердикт |")
    add("|---|---|---|---|---|---|---|---|---|---|")
    for st in steps:
        o, a, lat, lag = st["offered"], st["actual"], st["latency"], st["lag"]
        yolo = _lag_slope(lag, "yolo-workers")
        det = _lag_slope(lag, "detection-writer")
        add(
            f"| {st['index']} | {o.get('streams')} | {_offered(st):g} | "
            f"{_num(a.get('received_fps'))} | {_num(a.get('processed_fps'))} | "
            f"{fmt_s(lat.get('p50'))} | {fmt_s(lat.get('p95'))} | "
            f"{fmt_s(lat.get('p99'))} | {yolo} / {det} | "
            f"{_verdict(st)} |"
        )
    if steps:
        add("")
        add(f"Ключевая метрика: **{steps[-1]['latency'].get('metric', '')}**, "
            f"бюджет p95 = {criteria.get('stream_e2e_p95_budget_s')}с. "
            "Вердикт шага = все квитанции критериев пройдены.")

    # --------------------------------------------------------- состав латентности
    ref_idx = bn.get("reference_step")
    ref_rec = _step_by_index(steps, ref_idx) or (steps[-1] if steps else None)
    if ref_rec and ref_rec.get("stages"):
        add("")
        add(f"## Состав латентности на шаге {ref_rec['index']}")
        add("")
        add("| Стадия | mean | p95 | наблюдений |")
        add("|---|---|---|---|")
        for key, st in ref_rec["stages"].items():
            if not st:
                continue
            add(f"| {STAGE_NAMES.get(key, key)} | {fmt_s(st.get('mean'))} | "
                f"{fmt_s(st.get('p95'))} | {st.get('count', 0):.0f} |")

    # -------------------------------------------------------------------- drain
    drain = d.get("drain")
    if drain:
        add("")
        add("## Drain (разгребание после снятия нагрузки)")
        add("")
        add(f"- {drain.get('description', '')}")
        add(f"- Длительность: **{drain.get('duration_s', 0):.0f}с**"
            + (" — по таймауту, бэклог остался" if drain.get("timed_out") else ""))
        add(f"- Остаточный лаг: "
            f"{drain.get('final_lag')} (порог чистоты ≤ 5 сообщ.)")

    # --------------------------------------------------------------- хронология
    timeline: list[dict] = d.get("timeline") or []
    if timeline:
        add("")
        add("## Хронология")
        add("")
        add("| время | фаза | шаг | событие |")
        add("|---|---|---|---|")
        t0 = timeline[0].get("t", 0)
        for ev in timeline:
            rel = ev.get("t", 0) - t0
            add(f"| {int(rel // 60)}:{int(rel % 60):02d} | {ev.get('phase')} | "
                f"{ev.get('step') or ''} | {ev.get('detail') or ''} |")

    # --------------------------------------------------------------- методология
    add("")
    add("## Методология")
    add("")
    add("- Работа строго снаружи: HTTP API и Prometheus, код платформы не меняется.")
    add(f"- Окна измерения зафиксированы по времени ({_profile_str(cfg)}); "
        "квантили и приросты — `increase` ровно за окно, без хвоста прошлой ступени.")
    add(f"- Критерии: e2e p95 в бюджете {criteria.get('stream_e2e_p95_budget_s')}с, "
        f"лаг не растёт быстрее {criteria.get('lag_slope_max_per_s')} сообщ/с, "
        "обработка догоняет подачу (≥ "
        f"{criteria.get('rate_match_min', 0.95):.0%}).")
    add("- Лаг меряется линейным трендом (МНК) по окну — это СКОРОСТЬ роста "
        "бэклога, а не его разовый всплеск.")
    add(f"- Скрейп Prometheus стенда: {cfg.get('scrape_interval_s')}с — границы "
        "окна имеют точность ±одного скрейпа.")
    return "\n".join(lines) + "\n"


# ============================================================================
# График load_curve.png
# ============================================================================
def _plot_load_curve(d: dict, path: Path) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.font_manager as fm
    import matplotlib.pyplot as plt

    for font in ("/usr/share/fonts/truetype/chinese/NotoSansSC-Regular.ttf",):
        try:
            fm.fontManager.addfont(font)
        except Exception:  # noqa: BLE001 — фолбэк на DejaVu с кириллицей
            pass
    plt.rcParams["font.sans-serif"] = ["Noto Sans SC", "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False

    steps: list[dict] = d.get("steps") or []
    if not steps:
        path.write_bytes(b"")
        return
    cfg: dict = d.get("config") or {}
    criteria: dict = cfg.get("criteria") or {}
    summary: dict = d.get("summary") or {}

    pos = list(range(1, len(steps) + 1))
    xlabels = [f"{_offered(st):g}" for st in steps]

    def series(key: str) -> list[float]:
        out = []
        for st in steps:
            v = (st.get("latency") or {}).get(key)
            out.append(float(v) if v is not None else float("nan"))
        return out

    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(9, 7.5), sharex=True, constrained_layout=True
    )
    run_title = f"{d.get('run_id', '')}" + (f" · {d.get('label')}" if d.get("label") else "")

    # --- верхняя панель: латентность vs нагрузка ---------------------------
    ax1.plot(pos, series("p50"), "--o", color="#4c78a8", label="p50", ms=5)
    ax1.plot(pos, series("p95"), "-s", color="#f58518", label="p95", ms=6)
    ax1.plot(pos, series("p99"), ":^", color="#54a24b", label="p99", ms=5)
    budget = criteria.get("stream_e2e_p95_budget_s", 2.0)
    ax1.axhline(budget, color="#e45756", ls="--", lw=1.5, label=f"бюджет p95 = {budget:g}с")
    for x, st in zip(pos, steps):
        if st.get("verdict") != VERDICT_STABLE:
            v = (st.get("latency") or {}).get("p95")
            if v is not None:
                ax1.scatter([x], [float(v)], marker="x", s=120, color="#e45756", zorder=5)
    ms = (summary.get("max_sustainable") or {}).get("step")
    cp = (summary.get("collapse_point") or {}).get("step")
    if cp:
        ax1.axvline(cp, color="#e45756", ls="--", lw=1)
        ax1.annotate("коллапс", (cp, ax1.get_ylim()[1]), ha="center", va="bottom",
                     color="#e45756", fontsize=9)
    if ms:
        ax1.axvline(ms, color="#54a24b", ls=":", lw=1.2)
    ax1.set_ylabel("e2e латентность, с")
    ax1.set_title(f"Кривая нагрузки: {run_title}")
    ax1.grid(alpha=0.3)
    ax1.legend(loc="upper left", fontsize=9)

    # --- нижняя панель: скорость роста лага ---------------------------------
    groups: list[str] = []
    for st in steps:
        for g in (st.get("lag") or {}):
            if g not in groups:
                groups.append(g)
    width = 0.8 / max(1, len(groups))
    colors = ["#4c78a8", "#f58518", "#54a24b"]
    for gi, group in enumerate(groups):
        vals = []
        for st in steps:
            slope = ((st.get("lag") or {}).get(group) or {}).get("slope_per_s")
            vals.append(float(slope) if slope is not None else 0.0)
        offset = (gi - (len(groups) - 1) / 2) * width
        ax2.bar([x + offset for x in pos], vals, width * 0.92, label=group,
                color=colors[gi % len(colors)])
    threshold = criteria.get("lag_slope_max_per_s", 0.5)
    ax2.axhline(threshold, color="#e45756", ls="--", lw=1.5,
                label=f"порог {threshold:g}/с")
    ax2.set_ylabel("рост лага, сообщ/с")
    ax2.set_xlabel("нагрузка, fps")
    ax2.set_xticks(pos)
    ax2.set_xticklabels(xlabels)
    ax2.grid(alpha=0.3, axis="y")
    ax2.legend(loc="upper left", fontsize=9)

    fig.savefig(path, dpi=150)
    plt.close(fig)


# ============================================================================
# Мелкие помощники отчёта
# ============================================================================
def _step_by_index(steps: list[dict], index: Any) -> dict | None:
    if index is None:
        return None
    for st in steps:
        if st.get("index") == index:
            return st
    return None


def _offered(step: dict) -> float:
    """Мера нагрузки шага: суммарный fps подачи."""
    o = step.get("offered") or {}
    return float(o.get("offered_fps", 0) or 0)


def _num(v: Any) -> str:
    if v is None:
        return "—"
    if isinstance(v, float):
        return f"{v:g}"
    return str(v)


def _verdict(st: dict) -> str:
    if st.get("verdict") == VERDICT_STABLE:
        return "✅ stable"
    failed = st.get("failed_checks") or []
    return f"🔴 saturated ({', '.join(failed)})" if failed else "🔴 saturated"


def _lag_slope(lag: dict, group: str) -> str:
    slope = ((lag.get(group) or {}).get("slope_per_s"))
    return "—" if slope is None else f"{slope:+g}"


def _profile_str(cfg: dict) -> str:
    p = cfg.get("stream_profile") or {}
    return (f"прогрев {p.get('settle_s')}с + окно {p.get('measure_s')}с, "
            f"масштаб {p.get('scale')}, {p.get('fps')} fps на стрим")
