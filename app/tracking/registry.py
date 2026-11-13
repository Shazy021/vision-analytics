"""Реестр трекеров по стримам: дедупликация, такт времени, порядок подачи.

detection-консьюмер получает детекции БЕЗ гарантий порядка и не менее одного
раза. Порядок не гарантирован структурно: кадры одного стрима лежат на всех
партициях raw_frames и обрабатываются разными репликами yolo-consumer —
ответы возвращаются в порядке готовности, соседние кадры меняются местами.

Здесь сознательно НЕТ reorder buffer: удержание кадра до прихода следующего
стоит ровно один межкадровый интервал латентности, а на медленных источниках
(HLS-камеры, доли кадра в секунду) это секунды — неприемлемо против e2e в
сотни миллисекунд. Поздний кадр идёт трекеру напрямую: перестановка соседних
кадров для ByteTrack безвредна (трек находится по месту, а не по порядку, а
IoU соседних позиций высок), KF получает dt, ограниченный снизу, и слегка
шумящую скорость — это честная цена за латентность.

- дедупликация: frame_index монотонен в рамках стрима, повтор — это
  redelivery, в трекер не идёт (дубль, попавший в KF, сдвигает скорость);
- dt для KF считаем из captured_at (реальное время захвата кадра); без него
  — default_dt. Обратный ход времени (поздний кадр) не откатывает такт
  стрима: last_captured идёт только вперёд, позднему кадру — минимальный dt.

Трекер одного стрима живёт, пока стрим активен; простоявший idle_sec
закрывается evict_idle() с закрытием его треков (иначе трек и строка tracks
в БД висели бы «активными» до рестарта консьюмера).
"""
import logging
import time
from collections import OrderedDict
from dataclasses import dataclass, field

import numpy as np

from app.core.metrics import track_frames_total
from app.tracking.byte_tracker import ByteTracker, TrackEvent

logger = logging.getLogger(__name__)


@dataclass
class TrackedFrame:
    """Кадр, прогнанный через трекер: объектам проставлен track_id
    (ЛОКАЛЬНЫЙ id трекера — в id строки tracks его переводит консьюмер)."""
    stream_id: int
    frame_index: int
    captured_at: str | None
    captured_epoch: float | None
    objects: list[dict]
    opened: list[TrackEvent] = field(default_factory=list)
    closed: list[TrackEvent] = field(default_factory=list)
    touched: list[tuple[int, str, tuple[float, float, float, float]]] = \
        field(default_factory=list)
    active: int = 0


class _StreamState:
    """Всё per-stream: трекер, дедуп, такт времени."""

    def __init__(self, stream_id: int, tracker: ByteTracker):
        self.stream_id = stream_id
        self.tracker = tracker
        self.last_released: int | None = None
        self.last_captured: float | None = None
        self.seen: OrderedDict[int, None] = OrderedDict()
        self.last_activity = time.monotonic()


class StreamTrackers:
    """Трекер на стрим + слой порядка. Один экземпляр — в одном потоке
    (detection-консьюмер), без блокировок."""

    def __init__(self, *, high_thresh: float = 0.6, low_thresh: float = 0.25,
                 max_age_sec: float = 3.0, min_hits: int = 2,
                 default_dt: float = 0.2, dedup_window: int = 1024,
                 idle_evict_sec: float = 300.0):
        self.default_dt = default_dt
        self.dedup_window = dedup_window
        self.idle_evict_sec = idle_evict_sec
        self.max_age_sec = max_age_sec
        self._tracker_kwargs = dict(
            high_thresh=high_thresh, low_thresh=low_thresh, min_hits=min_hits,
        )
        self._streams: dict[int, _StreamState] = {}

    def _budget_frames(self, fps: int | None) -> int:
        """Бюджет жизни потерянного трека в кадрах для потока с частотой fps.

        Время жизни физическое (max_age_sec), а стареет трек по кадрам —
        поэтому бюджет = сек × fps потока: 15 кадров при 5fps, 45 при 15fps,
        90 при 30fps. Без fps из метаданных стрима считаем по default_dt."""
        rate = fps if fps and fps > 0 else 1.0 / self.default_dt
        return max(1, round(self.max_age_sec * rate))

    @property
    def active_total(self) -> int:
        return sum(len(s.tracker.tracks) for s in self._streams.values())

    def submit(self, stream_id: int, frame_index: int,
               captured_at: str | None, captured_epoch: float | None,
               objects: list[dict], fps: int | None = None,
               ) -> tuple[list[TrackedFrame], bool]:
        """Принять кадр стрима. Возвращает (кадры в трекер — всегда ровно
        этот кадр, либо ничего для дубля; был ли кадр дублем).

        fps — номинальная частота потока (из streams.fps): нужен один раз,
        при создании трекера, чтобы перевести физическое время жизни трека
        в бюджет кадров этого конкретного потока."""
        st = self._streams.get(stream_id)
        if st is None:
            st = self._streams[stream_id] = _StreamState(
                stream_id,
                ByteTracker(max_age_frames=self._budget_frames(fps),
                            **self._tracker_kwargs))
        st.last_activity = time.monotonic()

        if frame_index in st.seen:
            track_frames_total.labels(result="duplicate").inc()
            return [], True
        st.seen[frame_index] = None
        while len(st.seen) > self.dedup_window:
            st.seen.popitem(last=False)

        if st.last_released is not None and frame_index <= st.last_released:
            track_frames_total.labels(result="late").inc()

        frame = {"frame_index": frame_index, "captured_at": captured_at,
                 "captured_epoch": captured_epoch, "objects": objects}
        return [self._track_frame(st, frame)], False

    def evict_idle(self) -> list[TrackedFrame]:
        """Свернуть трекеры стримов, молчащих дольше idle_evict_sec:
        подтверждённые треки закрываются событием closed (иначе в БД они
        остались бы «активными» навсегда)."""
        now = time.monotonic()
        closed_frames: list[TrackedFrame] = []
        for sid in [s for s, st in self._streams.items()
                    if now - st.last_activity > self.idle_evict_sec]:
            st = self._streams.pop(sid)
            events = [TrackEvent("closed", t.id, t.class_name, t.last_bbox)
                      for t in st.tracker.tracks if t.confirmed]
            if events:
                closed_frames.append(TrackedFrame(
                    stream_id=sid,
                    frame_index=st.last_released or 0,
                    captured_at=None, captured_epoch=None,
                    objects=[], closed=events,
                ))
            logger.info("Stream %s idle > %.0fs — tracker evicted, %d tracks closed",
                        sid, self.idle_evict_sec, len(events))
        return closed_frames

    # ------------------------------------------------------------------

    def _track_frame(self, st: _StreamState, frame: dict) -> TrackedFrame:
        fi = frame["frame_index"]
        st.last_released = fi if st.last_released is None else max(st.last_released, fi)

        # Такт стрима идёт только вперёд: поздний кадр не откатывает
        # last_captured, иначе следующий за ним кадр получил бы раздутый dt.
        dt = self.default_dt
        epoch = frame["captured_epoch"]
        if epoch is not None:
            if st.last_captured is None or epoch > st.last_captured:
                if st.last_captured is not None:
                    dt = epoch - st.last_captured
                st.last_captured = epoch
            else:
                dt = 1e-3  # поздний кадр: KF стоит на месте, не смотрит назад

        objects = frame["objects"]
        if objects:
            boxes = np.array([[o["bbox"]["x1"], o["bbox"]["y1"],
                               o["bbox"]["x2"], o["bbox"]["y2"]] for o in objects])
            scores = np.array([o["confidence"] for o in objects])
            classes = [o["class"] for o in objects]
        else:
            boxes = np.zeros((0, 4))
            scores = np.zeros(0)
            classes = []

        res = st.tracker.update(boxes, scores, classes, dt)
        track_frames_total.labels(result="ok").inc()
        for i, obj in enumerate(objects):
            obj["track_id"] = res.assignments[i]
        return TrackedFrame(
            stream_id=st.stream_id,
            frame_index=fi,
            captured_at=frame["captured_at"],
            captured_epoch=epoch,
            objects=objects,
            opened=res.opened, closed=res.closed,
            touched=res.touched, active=res.active,
        )
