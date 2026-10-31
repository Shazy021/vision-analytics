"""Сравнение двух прогонов — ответ на вопрос «что изменилось после оптимизации».

Вход — два result.json (до/после). Прогоны могут отличаться набором ступеней:
сравнение сводит их по единой мере нагрузки (суммарный fps), а не по номеру
шага. Выход — markdown-отчёт и график; оба собираются только из JSON, живой
стенд не нужен.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from bench.models import VERDICT_STABLE
from bench.prom_client import fmt_s

logger = logging.getLogger(__name__)


def load_result(path: str | Path) -> dict:
    """Прочитать result.json (путь к файлу или к каталогу прогона)."""
    p = Path(path)
    if p.is_dir():
        p = p / "result.json"
    if not p.is_file():
        raise FileNotFoundError(f"не найден result.json: {path}")
    return json.loads(p.read_text(encoding="utf-8"))


def compare_runs(before: dict, after: dict) -> str:
    """Markdown-отчёт «было → стало» (before = до оптимизации)."""
    lines: list[str] = []
    add = lines.append
    same_pipeline = before.get("pipeline") == after.get("pipeline")
    units = "fps"

    add("# Сравнение прогонов: до → после")
    add("")
    for tag, d in (("До", before), ("После", after)):
        label = f" · {d.get('label')}" if d.get("label") else ""
        notes = f" — «{d['config'].get('notes')}»" if (d.get("config") or {}).get("notes") else ""
        add(f"- **{tag}:** `{d.get('run_id')}`{label} · {d.get('created_at')}{notes}")
    if not same_pipeline:
        add("- ⚠️ Прогоны разных конвейеров — сравнение только по общим полям")

    # ------------------------------------------------------- пропускная способность
    add("")
    add("## Пропускная способность")
    add("")
    b_ms = (before.get("summary") or {}).get("max_sustainable") or {}
    a_ms = (after.get("summary") or {}).get("max_sustainable") or {}
    b_val, a_val = b_ms.get("offered"), a_ms.get("offered")
    if b_val is None and a_val is None:
        add("- Ни в одном прогоне нет устойчивого шага — сравнивать нечего")
    else:
        add(f"- Устойчивый максимум: **{_num(b_val)} → {_num(a_val)} {units}**"
            + _delta_str(b_val, a_val, fmt="g"))
    b_cp = (before.get("summary") or {}).get("collapse_point") or {}
    a_cp = (after.get("summary") or {}).get("collapse_point") or {}
    b_cp_off, a_cp_off = b_cp.get("offered"), a_cp.get("offered")
    add(f"- Точка коллапса: **{_num(b_cp_off)} → {_num(a_cp_off)} {units}**"
        + _delta_str(b_cp_off, a_cp_off, fmt="g"))

    # --------------------------------------------------------------- латентность
    add("")
    add("## Латентность по ступеням (было → стало)")
    add("")
    b_steps = _steps_by_load(before)
    a_steps = _steps_by_load(after)
    common = sorted(set(b_steps) & set(a_steps))
    if common:
        add(f"| нагрузка, {units} | p95 до | p95 после | Δ p95 | лаг до | лаг после | вердикт |")
        add("|---|---|---|---|---|---|---|")
        for load in common:
            b, a = b_steps[load], a_steps[load]
            add(f"| {load:g} | {fmt_s(_p95(b))} | {fmt_s(_p95(a))} | "
                f"{_delta_str(_p95(b), _p95(a), fmt='.2f', invert=True)} | "
                f"{_lag(b)} | {_lag(a)} | {_v(b)} → {_v(a)} |")
        top = common[-1]
        b, a = b_steps[top], a_steps[top]
        add("")
        add(f"На максимальной общей ступени ({top:g} {units}): "
            f"p95 {fmt_s(_p95(b))} → {fmt_s(_p95(a))}, "
            f"вердикт {_v(b)} → {_v(a)}.")
    else:
        add("Общих ступеней нагрузки нет — прогоны не сопоставимы по шагам.")

    # --------------------------------------------------------------- узкое место
    add("")
    add("## Узкое место")
    add("")
    b_bn = ((before.get("summary") or {}).get("bottleneck") or {}).get("stage")
    a_bn = ((after.get("summary") or {}).get("bottleneck") or {}).get("stage")
    add(f"- Было: **{b_bn or '—'}**")
    add(f"- Стало: **{a_bn or '—'}**")
    if b_bn and a_bn:
        if b_bn == a_bn:
            add("- Узкое место не сместилось: оптимизация не сняла ограничение "
                "целиком — смотри, выросла ли пропускная способность выше")
        else:
            add(f"- Узкое место сместилось: «{b_bn}» → «{a_bn}» — "
                "ограничение переехало на следующую стадию")

    # -------------------------------------------------------------------- drain
    b_dr, a_dr = before.get("drain"), after.get("drain")
    if b_dr and a_dr:
        add("")
        add("## Drain")
        add("")
        add(f"- Разгребание бэклога: {b_dr.get('duration_s', 0):.0f}с → "
            f"{a_dr.get('duration_s', 0):.0f}с"
            + (" (после — чище)" if (a_dr.get("cleared", True)
                                     and not b_dr.get("cleared", True)) else ""))

    # -------------------------------------------------------------------- вывод
    add("")
    add("## Вывод")
    add("")
    add(_conclusion(before, after, b_val, a_val, b_steps, a_steps, common))

    b_aborted = (before.get("summary") or {}).get("aborted")
    a_aborted = (after.get("summary") or {}).get("aborted")
    if b_aborted or a_aborted:
        add("")
        add("⚠️ Один из прогонов прерван — сравнение по частичным данным "
            f"(до: {b_aborted or 'ок'}; после: {a_aborted or 'ок'}).")
    return "\n".join(lines) + "\n"


def _conclusion(before: dict, after: dict, b_val: Any, a_val: Any,
                b_steps: dict, a_steps: dict, common: list[float]) -> str:
    if b_val is not None and a_val is not None:
        if a_val > b_val:
            return (f"**Улучшение**: устойчивая подача выросла "
                    f"на {a_val - b_val:g} ({_pct(b_val, a_val)}). "
                    "Сравни латентность на общих ступенях — не куплено ли "
                    "это ростом p95.")
        if a_val < b_val:
            return (f"**Регрессия**: устойчивая подача упала "
                    f"на {b_val - a_val:g} ({_pct(a_val, b_val)}). "
                    "Смотри, какая квитанция валится на прежнем максимуме.")
    if common:
        top = common[-1]
        b, a = b_steps[top], a_steps[top]
        bp, ap = _p95(b), _p95(a)
        if bp is not None and ap is not None:
            if ap < 0.8 * bp:
                return (f"**Улучшение латентности**: при той же пропускной "
                        f"способности p95 на ступени {top:g} упал "
                        f"{fmt_s(bp)} → {fmt_s(ap)} "
                        f"({(1 - ap / bp) * 100:.0f}%).")
            if ap > 1.2 * bp:
                return (f"**Регрессия латентности**: p95 на ступени {top:g} "
                        f"вырос {fmt_s(bp)} → {fmt_s(ap)} "
                        f"({(ap / bp - 1) * 100:.0f}%).")
    return ("**Без существенных изменений** в рамках порогов: пропускная "
            "способность и латентности на общих ступенях сопоставимы.")


def plot_compare(before: dict, after: dict, path: Path) -> None:
    """p95 и рост лага по нагрузке: две кривые «до/после» на панель."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.font_manager as fm
    import matplotlib.pyplot as plt

    for font in ("/usr/share/fonts/truetype/chinese/NotoSansSC-Regular.ttf",):
        try:
            fm.fontManager.addfont(font)
        except Exception:  # noqa: BLE001
            pass
    plt.rcParams["font.sans-serif"] = ["Noto Sans SC", "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(9, 7.5), constrained_layout=True)

    for d, color, tag in ((before, "#e45756", "до"), (after, "#4c78a8", "после")):
        steps = d.get("steps") or []
        xs = [_offered(s) for s in steps]
        p95 = []
        for s in steps:
            v = (s.get("latency") or {}).get("p95")
            p95.append(float(v) if v is not None else float("nan"))
        ax1.plot(xs, p95, "-s", color=color, label=f"p95 ({tag}) "
                f"{d.get('run_id', '')}", ms=6)
        lag = [_lag_num(s) for s in steps]
        ax2.plot(xs, lag, "-o", color=color, label=f"рост лага ({tag})", ms=5)

    criteria = ((after.get("config") or {}).get("criteria")) or {}
    budget = criteria.get("stream_e2e_p95_budget_s", 2.0)
    lag_thr = criteria.get("lag_slope_max_per_s", 0.5)
    ax1.axhline(budget, color="#888", ls="--", lw=1.2, label=f"бюджет p95 = {budget:g}с")
    ax2.axhline(lag_thr, color="#888", ls="--", lw=1.2, label=f"порог лага {lag_thr:g}/с")

    ax1.set_ylabel("e2e латентность p95, с")
    ax1.set_title("До → после")
    ax1.grid(alpha=0.3)
    ax1.legend(loc="upper left", fontsize=9)
    ax2.set_ylabel("рост лага, сообщ/с")
    ax2.set_xlabel("нагрузка, fps")
    ax2.grid(alpha=0.3)
    ax2.legend(loc="upper left", fontsize=9)
    fig.savefig(path, dpi=150)
    plt.close(fig)


# ============================================================================
# Помощники
# ============================================================================
def _steps_by_load(d: dict) -> dict[float, dict]:
    out: dict[float, dict] = {}
    for st in d.get("steps") or []:
        out[_offered(st)] = st
    return out


def _offered(step: dict) -> float:
    o = step.get("offered") or {}
    return float(o.get("offered_fps", 0) or 0)


def _p95(step: dict) -> float | None:
    v = (step.get("latency") or {}).get("p95")
    return float(v) if v is not None else None


def _lag_num(step: dict) -> float:
    lag = step.get("lag") or {}
    vals = [(g or {}).get("slope_per_s") for g in lag.values()]
    nums = [v for v in vals if v is not None]
    return float(max(nums)) if nums else 0.0


def _lag(step: dict) -> str:
    v = _lag_num(step)
    return f"{v:+g}"


def _v(step: dict) -> str:
    return "stable" if step.get("verdict") == VERDICT_STABLE else "saturated"


def _num(v: Any) -> str:
    return "—" if v is None else f"{v:g}"


def _delta_str(before: Any, after: Any, fmt: str = ".2f", invert: bool = False) -> str:
    if before is None or after is None or not before:
        return ""
    delta = after - before
    pct = delta / abs(before) * 100
    good = delta > 0 if not invert else delta < 0
    arrow = "↑" if delta > 0 else ("↓" if delta < 0 else "→")
    mark = "✅" if good and delta != 0 else ("🔴" if not good and delta != 0 else "")
    return f" {mark} {arrow}{abs(delta):{fmt}} ({pct:+.0f}%)".replace("  ", " ")


def _pct(old: float, new: float) -> str:
    if not old:
        return "n/a"
    return f"{(new - old) / old * 100:+.0f}%"
