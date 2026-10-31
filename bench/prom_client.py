"""Клиент Prometheus HTTP API + разбор ответов в питоньи числа.

Почему через Prometheus, а не напрямую по /metrics консьюмеров:
  1. Порты :9091/:9092/:9093 НЕ проброшены на хост — они живут только
     в docker-сети, снаружи их не видно. Prometheus — единственный внешний
     наблюдатель, и это ровно то, что видит «пользователь» системы.
  2. У Prometheus есть история (retention 7d): можно спросить окно задним
     числом уже ПОСЛЕ шага нагрузки — бенчмарку не нужно скрейпить самому
     и гоняться с рассинхроном выборок.

Ключевая техника: квантили по ОКНУ считаются через increase(bucket[W]) —
инкремент счётчиков бакетов ровно за окно, а не через rate + скользящее
окно, которое затаскивает в результат хвост предыдущего шага нагрузки.
"""
from __future__ import annotations

import logging
import math
import time
from typing import Any

import requests

logger = logging.getLogger(__name__)


class PromError(RuntimeError):
    """Prometheus недоступен или запрос не удался."""


class PrometheusClient:
    """Тонкая обёртка над /api/v1/query и /api/v1/query_range."""

    def __init__(self, base_url: str, timeout_s: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self.query_url = f"{self.base_url}/api/v1/query"
        self.range_url = f"{self.base_url}/api/v1/query_range"
        self.timeout_s = timeout_s
        self._session = requests.Session()

    # ------------------------------------------------------------------ core
    def ping(self) -> None:
        try:
            resp = self._session.get(f"{self.base_url}/-/healthy", timeout=5.0)
            if resp.status_code != 200:
                raise PromError(f"Prometheus /-/healthy -> HTTP {resp.status_code}")
        except requests.RequestException as exc:
            raise PromError(f"Prometheus {self.base_url} недоступен: {exc}") from exc

    def _get(self, url: str, params: dict) -> dict:
        try:
            resp = self._session.get(url, params=params, timeout=self.timeout_s)
        except requests.RequestException as exc:
            raise PromError(f"запрос {url} не удался: {exc}") from exc
        if resp.status_code != 200:
            raise PromError(f"{url} -> HTTP {resp.status_code}: {resp.text[:300]}")
        body = resp.json()
        if body.get("status") != "success":
            raise PromError(f"Prometheus вернул error: {body}")
        return body["data"]

    # --------------------------------------------------------------- queries
    def query_instant(self, expr: str, at: float | None = None) -> list[dict]:
        """Мгновенный запрос: [{metric: {...}, value: [ts, "v"]}]."""
        params: dict[str, Any] = {"query": expr}
        if at is not None:
            params["time"] = at
        data = self._get(self.query_url, params)
        return data.get("result", [])

    def query_range(self, expr: str, start: float, end: float,
                    step_s: float) -> list[dict]:
        """Диапазонный запрос: [{metric: {...}, values: [[ts, "v"], ...]}]."""
        if end <= start:
            raise ValueError(f"пустой диапазон: start={start} end={end}")
        data = self._get(
            self.range_url,
            {"query": expr, "start": start, "end": end, "step": step_s},
        )
        return data.get("result", [])

    # ------------------------------------------------------------- shortcuts
    def instant_value(self, expr: str, at: float | None = None) -> float | None:
        """Единственное значение мгновенного запроса (None, если серии нет)."""
        result = self.query_instant(expr, at=at)
        if not result:
            return None
        total = 0.0
        for item in result:
            total += float(item["value"][1])
        return total

    def range_series(self, expr: str, start: float, end: float,
                     step_s: float) -> list[tuple[float, float]]:
        """Одна серия (или сумма нескольких) как [(ts, value), ...]."""
        result = self.query_range(expr, start, end, step_s)
        if not result:
            return []
        by_ts: dict[float, float] = {}
        for item in result:
            for ts, raw in item.get("values", []):
                by_ts.setdefault(ts, 0.0)
                by_ts[ts] += float(raw)
        return sorted(by_ts.items())

    def up_jobs(self) -> dict[str, float]:
        """Статус таргетов: {job=1.0|0.0} — preflight-проверка скрейпа."""
        result = self.query_instant("up")
        jobs: dict[str, float] = {}
        for item in result:
            job = item["metric"].get("job", "?")
            jobs.setdefault(job, 0.0)
            jobs[job] = max(jobs[job], float(item["value"][1]))
        return jobs

    # ----------------------------------------------------------- histograms
    def window_histogram(self, metric: str, window_s: float, at: float,
                         selector: str = "") -> dict[str, float] | None:
        """Распределение длительностей РОВНО за окно: {le: increase}.

        Запрос: sum by (le) (increase(<metric>_bucket[<window>])) на момент
        `at` покрывает интервал (at-window, at] — это и есть окно измерения.
        Селектор добавляется к имени метрики: '{topic="raw_frames"}'.
        Возвращает None, если метрика не существует (стадия не работала).
        ВАЖНО: значения кумулятивны (бакет le=0.05 включает всё ≤ 0.05с),
        как и у настоящих _bucket-серий Prometheus — см. histogram_stats.
        """
        expr = f'sum by (le) (increase({metric}_bucket{selector}[{window_s:.0f}s]))'
        result = self.query_instant(expr, at=at)
        if not result:
            return None
        buckets: dict[str, float] = {}
        for item in result:
            le = item["metric"].get("le", "+Inf")
            buckets[le] = buckets.get(le, 0.0) + float(item["value"][1])
        # Нет ни одного наблюдения (все increase == 0) — считаем стадию молчащей.
        if not buckets or sum(buckets.values()) <= 0:
            return None
        return buckets

    def counter_window_delta(self, metric: str, window_s: float, at: float,
                             selector: str = "") -> float | None:
        """Прирост счётчика за окно (increase), None если метрики нет."""
        expr = f'increase({metric}{selector}[{window_s:.0f}s])'
        result = self.query_instant(expr, at=at)
        if not result:
            return None
        return sum(float(item["value"][1]) for item in result)


# ============================================================================
# Математика по гистограмме и сериям — без numpy, формулы короткие
# ============================================================================
def histogram_stats(buckets: dict[str, float]) -> dict[str, float | None]:
    """Квантили (p50/p95/p99) и приближённое среднее из бакетов гистограммы.

    ВАЖНО: счётчики _bucket у Prometheus КУМУЛЯТИВНЫЕ — бакет le=0.05 уже
    включает все наблюдения ≤ 0.05с из младших бакетов. Сначала переводим
    их в плотности (прирост на бакет), иначе квантили и счётчик задваиваются.

    Квантиль — линейная интерполяция внутри бакета (тот же алгоритм, что у
    Prometheus histogram_quantile: равномерное распределение внутри бакета).
    Среднее — массы по серединам бакетов; хвост за последней конечной
    границей (+Inf) оцениваем как 1.5 x границу — оценка снизу.
    """
    finite = sorted(
        (float(le), cnt) for le, cnt in buckets.items() if le != "+Inf"
    )
    if not finite:
        return {"count": 0, "p50": None, "p95": None, "p99": None, "mean": None}

    # Кумулятивные границы → плотности; total = счётчик всех наблюдений.
    total = max(buckets.get("+Inf", 0.0), finite[-1][1])
    if total <= 0:
        return {"count": 0, "p50": None, "p95": None, "p99": None, "mean": None}
    dens: list[tuple[float, float]] = []
    prev_cum = 0.0
    for bound, cum_cnt in finite:
        dens.append((bound, cum_cnt - prev_cum))
        prev_cum = cum_cnt
    tail = max(0.0, total - prev_cum)  # наблюдения дороже последней границы

    def quantile(q: float) -> float | None:
        rank = q * total
        acc = 0.0
        prev_bound = 0.0
        for bound, cnt in dens:
            acc += cnt
            if rank <= acc:
                width = bound - prev_bound
                if cnt <= 0 or width <= 0:
                    return bound
                frac = (rank - (acc - cnt)) / cnt
                return prev_bound + frac * width
            prev_bound = bound
        # Ранг в хвосте за последней конечной границей: знаем только «больше».
        return dens[-1][0]

    mean = 0.0
    prev_bound = 0.0
    for bound, cnt in dens:
        mean += (prev_bound + bound) / 2.0 * cnt
        prev_bound = bound
    if tail > 0:
        mean += prev_bound * 1.5 * tail
    return {
        "count": total,
        "p50": quantile(0.50),
        "p95": quantile(0.95),
        "p99": quantile(0.99),
        "mean": mean / total,
    }


def series_slope(series: list[tuple[float, float]]) -> float | None:
    """Наклон линейного тренда (value/сек) по МНК; None при <2 точках.

    Для лага Kafka это «скорость роста бэклога»: +10 сообщ/с = консьюмер
    отстаёт от подачи на 10 сообщ/с — главный признак насыщения.
    """
    n = len(series)
    if n < 2:
        return None
    t0 = series[0][0]
    xs = [t - t0 for t, _ in series]
    ys = [v for _, v in series]
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    denom = sum((x - mean_x) ** 2 for x in xs)
    if denom == 0:
        return None
    return sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys)) / denom


def series_mean(series: list[tuple[float, float]]) -> float | None:
    if not series:
        return None
    return sum(v for _, v in series) / len(series)


def series_last(series: list[tuple[float, float]]) -> float | None:
    return series[-1][1] if series else None


def fmt_s(value: float | None, digits: int = 2) -> str:
    """Секунды в человекочитаемый вид: 0.85с / 12.4с / 2м03с."""
    if value is None:
        return "—"
    if value < 10:
        return f"{value:.{digits}f}с"
    if value < 60:
        return f"{value:.1f}с"
    return f"{int(value // 60)}м{int(value % 60):02d}с"


def now_epoch() -> float:
    return time.time()


def epoch_to_iso(epoch: float) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(epoch)) + " UTC"


def safe_div(a: float | None, b: float | None) -> float | None:
    if a is None or b is None or b == 0 or (isinstance(a, float) and math.isnan(a)):
        return None
    return a / b
