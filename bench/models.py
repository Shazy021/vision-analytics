"""Модели данных прогона: шаги нагрузки, таймлайн, итоги.

Всё, что попадает в result.json, — plain-dict-совместимые dataclass'ы:
одна схема и для отчётов, и для compare, и для будущего CI.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

VERDICT_STABLE = "stable"
VERDICT_SATURATED = "saturated"


@dataclass
class StepRecord:
    """Один шаг ramp'а: нагрузка, измерения, вердикт устойчивости."""

    index: int
    # Номинал: что хотели подать (единицы зависят от пайплайна).
    offered: dict[str, Any] = field(default_factory=dict)
    # Факт: что реально прошло через систему (по Prometheus и клиенту).
    actual: dict[str, Any] = field(default_factory=dict)
    # Ключевая латентность шага: {p50, p95, p99, budget_s, metric}.
    latency: dict[str, Any] = field(default_factory=dict)
    # Лаги Kafka: {группа: {slope_per_s, end, max}}.
    lag: dict[str, Any] = field(default_factory=dict)
    # Стадии: {имя: {mean, p95, count}} — None = стадия не работала.
    stages: dict[str, Any] = field(default_factory=dict)
    # Квитанции критериев: rate_match / lag_stable / latency_budget.
    checks: dict[str, bool] = field(default_factory=dict)
    verdict: str = VERDICT_STABLE
    failed_checks: list[str] = field(default_factory=list)
    # Границы окна измерения (epoch, UTC) и длительности фаз.
    window: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return dict(self.__dict__)


@dataclass
class RunResult:
    """Полный итог прогона — то, что сериализуется в result.json."""

    schema_version: int = 1
    tool: str = "vision-bench"
    pipeline: str = ""
    run_id: str = ""
    label: str = ""
    created_at: str = ""
    environment: dict[str, Any] = field(default_factory=dict)
    config: dict[str, Any] = field(default_factory=dict)
    source: dict[str, Any] = field(default_factory=dict)
    steps: list[StepRecord] = field(default_factory=list)
    timeline: list[dict[str, Any]] = field(default_factory=list)
    drain: dict[str, Any] | None = None
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "schema_version": self.schema_version,
            "tool": self.tool,
            "pipeline": self.pipeline,
            "run_id": self.run_id,
            "label": self.label,
            "created_at": self.created_at,
            "environment": self.environment,
            "config": self.config,
            "source": self.source,
            "steps": [s.to_dict() for s in self.steps],
            "timeline": self.timeline,
            "drain": self.drain,
            "summary": self.summary,
        }

    # -------------------------------------------------------- ключевые цифры
    @property
    def max_stable_step(self) -> StepRecord | None:
        stable = [s for s in self.steps if s.verdict == VERDICT_STABLE]
        return max(stable, key=lambda s: s.index) if stable else None

    @property
    def collapse_step(self) -> StepRecord | None:
        saturated = [s for s in self.steps if s.verdict == VERDICT_SATURATED]
        return min(saturated, key=lambda s: s.index) if saturated else None

    def offered_units(self, step: StepRecord) -> float | None:
        """Мера нагрузки шага: суммарный fps подачи."""
        return step.offered.get("offered_fps")
