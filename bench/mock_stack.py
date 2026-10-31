"""Синтетический стенд для selftest: API платформы + Prometheus в одном сервере.

Зачем: бенчмарк обязан доказать, что он измеряет правильно, ДО того как его
посадят на живой docker-стенд. Мок повторяет НАРУЖНОЕ поведение платформы
(REST API + PromQL) с ИЗВЕСТНОЙ пропускной способностью:

  стримы: MOCK_STREAM_CAPACITY_FPS = 30 fps суммарного потока кадров.

Selftest ждёт коллапс стримов на 40 fps (4 стрима x 10): если бенчмарк
находит именно эту границу, значит оркестратор, окна, квантили, лаги и
вердикты работают как задумано.

Модель «жидкостная» (fluid): прибытия — поток с кусочно-постоянной
скоростью, обслуживание — постоянная пропускная способность. Лаг = бэклог,
латентность = база + бэклог/пропускная (Little's law). Всё вычисляется
аналитически на любой момент времени, поэтому ретроспективные запросы
Prometheus окном назад от `at` работают точно, как настоящая история метрик.
"""
from __future__ import annotations

import json
import logging
import re
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable
from urllib.parse import parse_qs, urlparse

logger = logging.getLogger(__name__)

MOCK_STREAM_CAPACITY_FPS = 30.0
MOCK_ACTIVE_DELAY_S = 1.5
MOCK_PROM_JOBS = ("yolo-consumer", "detection-consumer", "kafka-exporter")

# Лестница бакетов гистограмм (кумулятивные le, как у Prometheus).
BUCKET_LADDER = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0]

# Золотое сечение: детерминированная квазиравномерная решётка множителей
# латентности — прогоны selftest воспроизводимы без ГСЧ.
_GOLDEN = 0.6180339887498949

_R_UP = re.compile(r"^up$")
_R_LAG = re.compile(r'^sum\(kafka_consumergroup_lag\{consumergroup="([\w-]+)"\}\)$')
_R_INC_FRAMES = re.compile(r"^increase\(frames_received_total\[([\d.]+)s\]\)$")
_R_RATE_FRAMES = re.compile(r"^sum\(rate\(frames_received_total\[([\d.]+)s\]\)\)$")
_R_INC_PROC = re.compile(
    r'^increase\(frame_processed_total\{result="(ok|error)"\}\[([\d.]+)s\]\)$'
)
_R_RATE_PROC = re.compile(
    r'^sum\(rate\(frame_processed_total\{result="(ok|error)"\}\[([\d.]+)s\]\)\)$'
)
_R_HIST = re.compile(
    r"^sum by \(le\) \(increase\((\w+)_bucket(\{[^}]*\})?\[([\d.]+)s\]\)\)$"
)

_FRAME_METRICS = {
    "frame_e2e_latency_seconds",
    "frame_queue_wait_seconds",
    "frame_decode_duration_seconds",
    "frame_inference_duration_seconds",
    "frame_write_duration_seconds",
}


class UnknownResource(KeyError):
    """Ресурс не найден — мок отвечает 404."""


# ============================================================================
# Жидкостная модель очереди
# ============================================================================
class FluidServer:
    """Очередь: кусочно-постоянная скорость прибытий + константный сервис.

    Прибытия задаются rate_delta(t, d) — сменой скорости потока кадров
    (+/- fps камеры). served(t) — точный кусочно-линейный интеграл
    обслуживания, backlog(t) = arrived(t) - served(t). Всё аналитично:
    любое окно в прошлое — точно.
    """

    def __init__(self, capacity_per_s: float) -> None:
        self.capacity = capacity_per_s
        self._rate_deltas: list[tuple[float, float]] = []

    def rate_delta(self, t: float, delta: float) -> None:
        self._rate_deltas.append((t, delta))

    # ---------------------------------------------------------------- arrived
    def _sorted_rate(self) -> list[tuple[float, float]]:
        return sorted(self._rate_deltas)

    def _rate_at(self, t: float) -> float:
        return sum(d for bt, d in self._rate_deltas if bt <= t)

    def arrived(self, t: float) -> float:
        total = 0.0
        prev_t = 0.0
        rate = 0.0
        for bt, delta in self._sorted_rate():
            if bt > t:
                break
            total += rate * (bt - prev_t)
            prev_t = bt
            rate += delta
        total += rate * (t - prev_t)
        return total

    # ---------------------------------------------------------------- served
    def served(self, t: float) -> float:
        served = 0.0
        backlog = 0.0
        events = sorted(bt for bt, _ in self._rate_deltas)
        prev: float | None = None
        for bt in events:
            if bt > t:
                break
            if prev is not None:
                served, backlog = self._advance(served, backlog, prev, bt)
            prev = bt
        if prev is not None and t > prev:
            served, backlog = self._advance(served, backlog, prev, t)
        return served

    def _advance(self, served: float, backlog: float, t_from: float, t_to: float):
        """Сегмент [t_from, t_to): скорость прибытий внутри постоянна."""
        dt = t_to - t_from
        if dt <= 0:
            return served, backlog
        rate = self._rate_at(t_from)
        if backlog + rate * dt <= self.capacity * dt:
            # Бэклог иссяк внутри сегмента — дальше сервис идёт в темпе подач
            # (формула точна: cap*t_d + r*(dt-t_d) == backlog + r*dt).
            return served + backlog + rate * dt, 0.0
        return served + self.capacity * dt, backlog + (rate - self.capacity) * dt

    def backlog(self, t: float) -> float:
        return max(0.0, self.arrived(t) - self.served(t))


# ============================================================================
# Мок стенда: HTTP API + Prometheus на одном порту
# ============================================================================
class MockVisionStack:
    """Синтетический стенд с известной пропускной способностью."""

    def __init__(self, stream_capacity_fps: float = MOCK_STREAM_CAPACITY_FPS) -> None:
        self.lock = threading.RLock()
        # Стримы: кадры текут потоком rate = сумма fps активных стримов.
        self.frames_q = FluidServer(stream_capacity_fps)
        self.streams: dict[int, dict] = {}
        self._next_stream_id = 1
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    # ------------------------------------------------------------- lifecycle
    def start(self, port: int = 0) -> str:
        self._server = ThreadingHTTPServer(("127.0.0.1", port), _make_handler(self))
        self._server.daemon_threads = True
        self._thread = threading.Thread(
            target=self._server.serve_forever, name="mock-vision-stack", daemon=True
        )
        self._thread.start()
        url = f"http://127.0.0.1:{self._server.server_address[1]}"
        logger.info("Mock-стенд: %s (стримы %.0f fps)", url, self.frames_q.capacity)
        return url

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        logger.info("Mock-стенд остановлен")

    # ------------------------------------------------------------------ API
    def api_create_stream(self, name: str, rtsp_url: str, fps: int) -> dict:
        with self.lock:
            sid = self._next_stream_id
            self._next_stream_id += 1
            self.streams[sid] = {
                "id": sid, "name": name, "rtsp_url": rtsp_url, "fps": int(fps),
                "started_at": None, "stopped_at": None,
            }
            return dict(self.streams[sid], status="created")

    def api_stream_action(self, sid: int, action: str) -> dict:
        with self.lock:
            s = self.streams.get(sid)
            if s is None:
                raise UnknownResource(sid)
            if action == "start":
                s["started_at"] = time.time()
            else:
                if s["started_at"] is not None and s["stopped_at"] is None:
                    s["stopped_at"] = time.time()
            self._recompute_rate()
            return {"id": sid, "status": self._stream_status(s)}

    def api_delete_stream(self, sid: int) -> dict:
        with self.lock:
            s = self.streams.get(sid)
            if s is None:
                raise UnknownResource(sid)
            if s["started_at"] is not None and s["stopped_at"] is None:
                s["stopped_at"] = time.time()
            del self.streams[sid]
            self._recompute_rate()
            return {"detail": "deleted"}

    def api_list_streams(self) -> list[dict]:
        with self.lock:
            return [
                dict(s, status=self._stream_status(s)) for s in self.streams.values()
            ]

    def _stream_status(self, s: dict) -> str:
        if s["stopped_at"] is not None:
            return "stopped"
        if s["started_at"] is None:
            return "created"
        elapsed = time.time() - s["started_at"]
        return "active" if elapsed >= MOCK_ACTIVE_DELAY_S else "starting"

    def _recompute_rate(self) -> None:
        """Скорость потока кадров = сумма fps активных стримов (сейчас)."""
        now = time.time()
        active = sum(
            s["fps"] for s in self.streams.values()
            if s["started_at"] is not None and s["stopped_at"] is None
        )
        self.frames_q.rate_delta(now, float(active) - self.frames_q._rate_at(now))

    # ----------------------------------------------------------- Prometheus
    def prom_query_instant(self, expr: str, at: float | None = None) -> list[dict]:
        if at is None:
            at = time.time()
        at = float(at)
        expr = expr.strip()
        with self.lock:
            if _R_UP.match(expr):
                return [{"metric": {"job": j}, "value": [at, "1"]} for j in MOCK_PROM_JOBS]

            m = _R_LAG.match(expr)
            if m:
                group = m.group(1)
                val = self._lag_value(group, at)
                return [{"metric": {"consumergroup": group}, "value": [at, f"{val:.4f}"]}]

            m = _R_INC_FRAMES.match(expr) or _R_RATE_FRAMES.match(expr)
            if m:
                w = float(m.group(1))
                val = self._delta(self.frames_q.arrived, at, w)
                if expr.startswith("sum(rate"):
                    val /= w
                return [{"metric": {}, "value": [at, f"{val:.4f}"]}]

            m = _R_INC_PROC.match(expr) or _R_RATE_PROC.match(expr)
            if m:
                result, w = m.group(1), float(m.group(2))
                val = self._delta(self.frames_q.served, at, w) if result == "ok" else 0.0
                if expr.startswith("sum(rate"):
                    val /= w
                return [{"metric": {"result": result}, "value": [at, f"{val:.4f}"]}]

            m = _R_HIST.match(expr)
            if m:
                metric, selector, w = m.group(1), (m.group(2) or ""), float(m.group(3))
                return self._histogram(metric, selector, at, w)

        raise ValueError(f"mock-Prometheus не понимает запрос: {expr!r}")

    def prom_query_range(self, expr: str, start: float, end: float,
                         step_s: float) -> list[dict]:
        if step_s <= 0:
            step_s = 1.0
        count = int((end - start) / step_s) + 1
        matrix: dict[str, dict] = {}
        for i in range(max(0, count)):
            t = round(start + i * step_s, 3)
            if t > end:
                break
            for item in self.prom_query_instant(expr, t):
                key = json.dumps(item["metric"], sort_keys=True)
                matrix.setdefault(key, {"metric": item["metric"], "values": []})
                matrix[key]["values"].append([t, item["value"][1]])
        return list(matrix.values())

    # -------------------------------------------------------- модель метрик
    @staticmethod
    def _delta(fn: Callable[[float], float], at: float, w: float) -> float:
        return fn(at) - fn(at - w)

    def _lag_value(self, group: str, t: float) -> float:
        if group == "yolo-workers":
            return self.frames_q.backlog(t)
        return 0.0  # detection-writer: писатель не отстаёт никогда

    def _histogram(self, metric: str, selector: str, at: float, w: float) -> list[dict]:
        n, mean = self._latency_model(metric, selector, at, w)
        if not n or n <= 0 or mean is None:
            return []
        dens: dict[float, int] = {}
        for i in range(int(n)):
            frac = ((i + 0.5) * _GOLDEN) % 1.0
            value = mean * (0.2 + 1.6 * frac)  # p50 ≈ mean, p95 ≈ 1.7 x mean
            for bound in BUCKET_LADDER:
                if value <= bound:
                    dens[bound] = dens.get(bound, 0) + 1
                    break
        # _bucket-серии Prometheus КУМУЛЯТИВНЫ (le=0.05 = все ≤ 0.05):
        # накапливаем плотности, +Inf = все наблюдения окна.
        out = []
        cum = 0
        for bound in BUCKET_LADDER:
            cum += dens.get(bound, 0)
            out.append({"metric": {"le": f"{bound:g}"}, "value": [at, str(cum)]})
        out.append({"metric": {"le": "+Inf"}, "value": [at, str(int(n))]})
        return out

    def _latency_model(self, metric: str, selector: str,
                       at: float, w: float) -> tuple[float, float | None]:
        """(наблюдений в окне, среднее) для метрики-гистограммы."""
        if metric not in _FRAME_METRICS:
            return 0.0, None
        t_mid = at - w / 2.0
        n = self._delta(self.frames_q.served, at, w)
        backlog = self.frames_q.backlog(t_mid)
        if metric == "frame_e2e_latency_seconds":
            return n, 0.25 + backlog / self.frames_q.capacity
        if metric == "frame_queue_wait_seconds":
            if "raw_frames" in selector:
                return n, max(0.01, backlog / self.frames_q.capacity)
            return n, 0.005
        if metric == "frame_decode_duration_seconds":
            return n, 0.02
        if metric == "frame_inference_duration_seconds":
            return n, 0.08
        return n, 0.008  # write


# ============================================================================
# HTTP-обвязка
# ============================================================================
def _make_handler(owner: MockVisionStack) -> type[BaseHTTPRequestHandler]:
    class Handler(_HandlerBase):
        stack: MockVisionStack = owner

    return Handler


class _HandlerBase(BaseHTTPRequestHandler):
    stack: MockVisionStack
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt: str, *args: Any) -> None:  # тише в консоль
        logger.debug("mock %s " + fmt, self.command, *args)

    # ------------------------------------------------------------------ utils
    def _send_json(self, code: int, payload: Any) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _body(self) -> bytes:
        length = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(length) if length > 0 else b""

    # -------------------------------------------------------------------- GET
    def do_GET(self) -> None:  # noqa: N802 — имя метода http.server
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)
        try:
            if path == "/-/healthy":
                body = b"OK"
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if path == "/api/v1/query":
                expr = (query.get("query") or [""])[0]
                at = float(query["time"][0]) if "time" in query else None
                result = self.stack.prom_query_instant(expr, at)
                self._send_json(200, _vector(result))
                return
            if path == "/api/v1/query_range":
                expr = (query.get("query") or [""])[0]
                start = float(query["start"][0])
                end = float(query["end"][0])
                step = float(query["step"][0])
                result = self.stack.prom_query_range(expr, start, end, step)
                self._send_json(200, _matrix(result))
                return
            if path == "/health":
                self._send_json(200, {"status": "ok"})
                return
            if path == "/api/streams":
                self._send_json(200, self.stack.api_list_streams())
                return
            self._send_json(404, {"detail": f"mock: нет маршрута {path}"})
        except UnknownResource:
            self._send_json(404, {"detail": "not found"})
        except Exception as exc:  # noqa: BLE001 — мок не должен молча падать
            logger.exception("mock GET %s", path)
            self._send_json(500, {"detail": str(exc)})

    # ------------------------------------------------------------------- POST
    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        try:
            if path == "/api/streams":
                data = json.loads(self._body() or b"{}")
                self._send_json(
                    201,
                    self.stack.api_create_stream(
                        str(data.get("name", "")),
                        str(data.get("rtsp_url", "")),
                        int(data.get("fps", 5)),
                    ),
                )
                return
            m = re.match(r"^/api/streams/(\d+)/(start|stop)$", path)
            if m:
                self._send_json(200, self.stack.api_stream_action(int(m.group(1)), m.group(2)))
                return
            self._send_json(404, {"detail": f"mock: нет маршрута {path}"})
        except UnknownResource:
            self._send_json(404, {"detail": "not found"})
        except Exception as exc:  # noqa: BLE001
            logger.exception("mock POST %s", path)
            self._send_json(500, {"detail": str(exc)})

    # ----------------------------------------------------------------- DELETE
    def do_DELETE(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        try:
            m = re.match(r"^/api/streams/(\d+)$", path)
            if m:
                self._send_json(200, self.stack.api_delete_stream(int(m.group(1))))
                return
            self._send_json(404, {"detail": f"mock: нет маршрута {path}"})
        except UnknownResource:
            self._send_json(404, {"detail": "not found"})
        except Exception as exc:  # noqa: BLE001
            logger.exception("mock DELETE %s", path)
            self._send_json(500, {"detail": str(exc)})


def _vector(result: list[dict]) -> dict:
    return {"status": "success", "data": {"resultType": "vector", "result": result}}


def _matrix(result: list[dict]) -> dict:
    return {"status": "success", "data": {"resultType": "matrix", "result": result}}
