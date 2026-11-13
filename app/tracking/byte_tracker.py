"""ByteTrack: двухстадийная ассоциация детекций в треки.

Суть метода (arXiv:2110.06864): детекции делятся на уверенные (score >=
high_thresh) и слабые (low_thresh <= score < high_thresh). Уверенные
ассоциируются с треками первыми; то, что не нашло пару, вторым заходом
пробуется пристроить к трекам, оставшимся ни с чем, за счёт слабых детекций.
Так объект в окклюзии (детектор даёт слабый бокс или не даёт вовсе) не
теряет идентификатор, а мусорные слабые детекции не рождают новых треков.

Трек подтверждается после min_hits попаданий: одиночный фантом детектора
треком не считается. Событие opened (строка tracks в БД) отправляется на
подтверждении, а не на рождении: смерть неподтверждённого фантома тихая
(closed у него не бывает), и opened-на-рождении оставлял бы в БД вечно
висевшие «active»-строки призраков. Потерянный трек живёт max_age_frames
кадров подачи (KF продолжает предсказывать позицию по dt), потом
закрывается. Кадр, а не секунда — единица жизни: трекер стареет только
когда стрим реально подаёт кадры, пауза источника или инференса трек
не убивает, а лимит сам масштабируется под fps потока.

Ассоциация — по IoU к предсказанной позиции KF и СТРОГО внутри класса:
переход person→car физически невозможен, а без этого ограничения два
пересекающихся объекта разных классов могли бы обменяться ID. Гейта
Махаланобиса нет намеренно: после серии точных обновлений ковариация
сжимается, и гейт начинает отвергать СОИ детекции, если геометрия объекта
резко меняется (растущий при приближении бокс, обрезка краем кадра). Трек
терял id, та же детекция через кадр рождала двойника — и id прыгал между
близнецами. Ограничение по движению остаётся: матчится IoU с ПРЕДСКАЗАНИЕМ
KF, чужая детекция рядом с улетевшим предсказанием не окажется.

Локальные track_id счётчика — временные, уникальны в рамках одного трекера
(одного стрима). В БД их заменяет id строки tracks — см. detections-консьюмер.
"""
from dataclasses import dataclass, field

import numpy as np

from app.tracking.association import assign, iou_matrix
from app.tracking.kalman import KalmanBox

# Порог IoU для пары «трек-детекция»: ниже — считаем, что это разные объекты.
_MIN_MATCH_IOU = 0.2
# Пород дубля при рождении: уверенная детекция без пары, чей бокс лежит
# поверх трека (тот же класс, IoU >= 0.5), — второй бокс на том же объекте,
# а не новый объект. Без этой проверки двойники рождаются всегда:
# венгерский алгоритм ставит пару «один трек — одна детекция», и второй бокс
# гарантированно остаётся без пары. Двойник получает id объекта, а не свой
# трек, — иначе близнецы начинают перетягивать детекции и id прыгает.
_DUP_SPAWN_IOU = 0.5


@dataclass(frozen=True)
class TrackEvent:
    """Событие жизненного цикла трека для потребителя (вставки в tracks)."""
    kind: str  # 'opened' | 'closed'
    track_id: int  # локальный id трекера
    class_name: str
    bbox: tuple[float, float, float, float]  # tlbr на момент события
    # Сколько попаданий собрал трек к моменту события (для opened: строка
    # tracks рождается на подтверждении и сразу честна по num_frames).
    hits: int = 1


@dataclass
class TrackResult:
    """Итог одного кадра.

    assignments[i] — локальный track_id i-й детекции, None если детекция
    не подтверждена; opened/closed — события жизненного цикла; touched —
    треки, обновлённые в этом кадре (для last_seen/num_frames в БД);
    active — сколько подтверждённых треков живо после кадра.
    """
    assignments: list[int | None] = field(default_factory=list)
    opened: list[TrackEvent] = field(default_factory=list)
    closed: list[TrackEvent] = field(default_factory=list)
    touched: list[tuple[int, str, tuple[float, float, float, float]]] = \
        field(default_factory=list)  # (local_id, class_name, bbox)
    active: int = 0


class _Track:
    """Один трек: KF-состояние плюс счётчики жизненного цикла."""

    def __init__(self, track_id: int, class_name: str, bbox: np.ndarray, score: float):
        self.id = track_id
        self.class_name = class_name
        self.kf = KalmanBox(bbox)
        self.hits = 1
        self.confirmed = False
        # Сколько КАДРОВ подряд трекер не видел этого объект (0 — видели в
        # этом кадре). Единица — кадр стрима, а не секунда: пауза источника
        # или инференса счётчик не двигает (кадров нет — трек ждёт), а
        # скорость счёта автоматически следует за fps потока.
        self.frames_since_update = 0
        self.last_bbox = tuple(float(v) for v in bbox)
        self.last_score = score

    def predict(self, dt: float) -> None:
        self.kf.predict(dt)

    def update(self, bbox: np.ndarray, score: float) -> None:
        self.kf.update(bbox)
        self.hits += 1
        self.frames_since_update = 0
        self.last_bbox = tuple(float(v) for v in bbox)
        self.last_score = score


class ByteTracker:
    """ТРЕКЕР ОДНОГО СТРИМА. Не потокобезопасен: один трекер — один поток
    (detection-консьюмер однопоточный по построению)."""

    def __init__(self, *, high_thresh: float = 0.6, low_thresh: float = 0.25,
                 max_age_frames: int = 25, min_hits: int = 2):
        # low_thresh не может быть выше high_thresh — иначе «слабых» детекций
        # не бывает вовсе и второй заход мёртвый код.
        self.high_thresh = high_thresh
        self.low_thresh = min(low_thresh, high_thresh)
        self.max_age_frames = max(1, max_age_frames)
        self.min_hits = max(1, min_hits)
        self._tracks: list[_Track] = []
        self._next_id = 1

    def update(self, boxes: np.ndarray, scores: np.ndarray,
               classes: list[str], dt: float) -> TrackResult:
        """Один кадр: [N,4] tlbr, [N] confidence, [N] имён классов, dt —
        секунды от предыдущего кадра этого стрима."""
        boxes = np.asarray(boxes, dtype=np.float64).reshape(-1, 4)
        scores = np.asarray(scores, dtype=np.float64)
        result = TrackResult()

        for track in self._tracks:
            track.predict(dt)

        high = np.nonzero(scores >= self.high_thresh)[0]
        low = np.nonzero((scores >= self.low_thresh) & (scores < self.high_thresh))[0]

        # det_idx → трек: итог обоих заходов ассоциации, нужен для выдачи.
        matched_det: dict[int, _Track] = {}
        matched_track_ids: set[int] = set()

        # Первый заход: уверенные детекции ко всем трекам. Без треков
        # ассоциировать не с чем — все уверенные детекции свободны и родят
        # новые треки.
        unmatched_high: list[int] = list(range(len(high)))
        if self._tracks and len(high):
            pairs, _, unmatched_d = self._match(self._tracks, boxes, high, classes)
            for t_idx, d_pos in pairs:
                track = self._tracks[t_idx]
                track.update(boxes[high[d_pos]], scores[high[d_pos]])
                matched_det[int(high[d_pos])] = track
                matched_track_ids.add(track.id)
            unmatched_high = unmatched_d

        # Второй заход: слабые детекции к трекам, оставшимся ни с чем
        # (включая потерянные ранее).
        rest = [t for t in self._tracks if t.id not in matched_track_ids]
        unmatched_rest_ids: set[int] = {t.id for t in rest}
        if rest and len(low):
            pairs, unmatched_t, _ = self._match(rest, boxes, low, classes)
            for t_pos, d_pos in pairs:
                track = rest[t_pos]
                track.update(boxes[low[d_pos]], scores[low[d_pos]])
                matched_det[int(low[d_pos])] = track
                unmatched_rest_ids.discard(track.id)

        # Рождение с подавлением дублей. Оставшиеся после двух заходов
        # уверенные детекции (по убыванию confidence) меряем чистым IoU ко
        # всем трекам: лёг поверх трека (тот же класс, IoU >= _DUP_SPAWN_IOU)
        # — это второй бокс на том же объекте, получает его id (KF не
        # трогаем: трек уже обновлён лучшим боксом в этом кадре). Иначе —
        # действительно новый объект: рождаем. Событие opened здесь НЕ
        # отправляется — строка tracks создаётся только на подтверждении
        # (см. ниже), иначе тихая смерть неподтверждённого фантома
        # оставляла бы в БД вечный «active».
        left_high = sorted((int(high[p]) for p in unmatched_high),
                           key=lambda d: -scores[d])
        if left_high:
            refs = [t.kf.tlbr for t in self._tracks]
            for d_idx in left_high:
                best_track, best_iou = None, 0.0
                for t_pos, track in enumerate(self._tracks):
                    if track.class_name != classes[d_idx]:
                        continue
                    ov = float(iou_matrix(refs[t_pos][None, :],
                                          boxes[d_idx][None, :])[0, 0])
                    if ov > best_iou:
                        best_track, best_iou = track, ov
                if best_track is not None and best_iou >= _DUP_SPAWN_IOU:
                    matched_det[d_idx] = best_track
                    continue
                new = _Track(self._next_id, classes[d_idx], boxes[d_idx], scores[d_idx])
                self._next_id += 1
                self._tracks.append(new)
                refs.append(new.kf.tlbr)

        # Смерть: потерянные дольше max_age_frames кадров. Подтверждённые —
        # событие closed; неподтверждённые фантомы тихо удаляются (в БД их
        # строк нет — opened до подтверждения не доходило).
        survivors: list[_Track] = []
        for track in self._tracks:
            if track.id in unmatched_rest_ids:
                track.frames_since_update += 1
            if track.frames_since_update > self.max_age_frames:
                if track.confirmed:
                    result.closed.append(
                        TrackEvent("closed", track.id, track.class_name, track.last_bbox))
                continue
            survivors.append(track)
        self._tracks = survivors

        # Подтверждение и выдача.
        for track in self._tracks:
            if not track.confirmed and track.hits >= self.min_hits:
                track.confirmed = True
                # opened — точка, в которой трек становится «настоящим» и
                # получает строку в БД. hits передаём сюда: минимальный
                # подтверждённый трек уже жил min_hits кадров, строка
                # рождается с этой цифрой, а не с единицей.
                result.opened.append(
                    TrackEvent("opened", track.id, track.class_name,
                               track.last_bbox, hits=track.hits))
        result.assignments = [
            matched_det[i].id if i in matched_det and matched_det[i].confirmed else None
            for i in range(len(boxes))
        ]
        result.touched = [(t.id, t.class_name, t.last_bbox)
                          for t in self._tracks if t.frames_since_update == 0]
        result.active = sum(1 for t in self._tracks if t.confirmed)
        return result

    # ------------------------------------------------------------------

    @property
    def tracks(self) -> list[_Track]:
        """Живые треки (для реестра/метрик — не трогать содержимое)."""
        return self._tracks

    def _match(self, tracks: list[_Track], boxes: np.ndarray,
               det_idx: np.ndarray,
               classes: list[str]) -> tuple[list[tuple[int, int]], list[int], list[int]]:
        """Ассоциация набора треков с подмножеством детекций det_idx.
        Возвращает пары (индекс в tracks, позиция в det_idx).

        Стоимость — IoU предсказанного бокса KF с детекцией; ограничение по
        движению задаёт само предсказание (с ним чужая быстрая детекция не
        пересечётся), отдельного гейта нет. Класс проверяем векторно — он
        у пары грубый."""
        pred = np.array([t.kf.tlbr for t in tracks])
        det_boxes = boxes[det_idx]
        ious = iou_matrix(pred, det_boxes)
        cost = 1.0 - ious
        track_cls = np.array([t.class_name for t in tracks])[:, None]
        det_cls = np.array([classes[int(d)] for d in det_idx])[None, :]
        valid = track_cls == det_cls
        return assign(cost, threshold=1.0 - _MIN_MATCH_IOU, valid=valid)
