"""Unit-тесты ядра трекинга: KF, ассоциация, ByteTrack, реестр стримов.

Без Kafka/БД — чистые численные сценарии: трекер должен держать ID на
прямолинейном движении, переживать короткую окклюзию, не склеивать
пересекающиеся объекты и корректно рождать/закрывать треки.
"""
import numpy as np

from app.tracking.association import assign, iou_matrix
from app.tracking.byte_tracker import ByteTracker
from app.tracking.kalman import KalmanBox, tlbr_to_z, z_to_tlbr
from app.tracking.registry import StreamTrackers


def feed(tracker: ByteTracker, boxes, scores, classes, dt=0.2):
    return tracker.update(np.array(boxes, dtype=float).reshape(-1, 4),
                          np.array(scores, dtype=float), list(classes), dt)


# ---------------------------------------------------------------- Kalman

class TestKalman:
    def test_tlbr_roundtrip(self):
        bbox = np.array([10.0, 20.0, 50.0, 90.0])
        back = z_to_tlbr(tlbr_to_z(bbox))
        np.testing.assert_allclose(back, bbox, atol=1e-9)

    def test_predict_moves_by_velocity(self):
        kf = KalmanBox(np.array([0.0, 0.0, 100.0, 100.0]))
        # Циклы predict/update с шагом 0.5с задают скорость ~ (+100, 0) px/s
        for x in (10.0, 20.0):
            kf.predict(0.5)
            kf.update(np.array([x, 0.0, 100.0 + x, 100.0]))
        pred = kf.predict(0.5)
        # Предсказание ушло вправо от последней позиции
        assert pred[0] > 20.0


# ---------------------------------------------------------------- association

class TestAssociation:
    def test_iou_values(self):
        a = np.array([[0.0, 0.0, 10.0, 10.0]])
        b = np.array([[0.0, 0.0, 10.0, 10.0],   # идентичный → 1.0
                      [5.0, 0.0, 15.0, 10.0],   # половина → 1/3
                      [20.0, 0.0, 30.0, 10.0]])  # нет пересечения → 0
        m = iou_matrix(a, b)
        np.testing.assert_allclose(m[0], [1.0, 1 / 3, 0.0], atol=1e-9)

    def test_assign_minimizes_cost(self):
        cost = np.array([[0.1, 0.9], [0.8, 0.2]])
        pairs, ua, ub = assign(cost, threshold=1.0)
        assert sorted(pairs) == [(0, 0), (1, 1)]
        assert ua == [] and ub == []

    def test_assign_drops_over_threshold(self):
        cost = np.array([[0.9]])
        pairs, ua, ub = assign(cost, threshold=0.5)
        assert pairs == [] and ua == [0] and ub == [0]

    def test_assign_respects_valid_mask(self):
        # Дешёвая пара (0,0) запрещена маской — решатель перестраивается
        # на валидные (0,1) и (1,0), хотя (1,0) и дороже запрещённой (1,1).
        cost = np.array([[0.1, 0.2], [0.3, 0.4]])
        valid = np.array([[False, True], [True, False]])
        pairs, ua, ub = assign(cost, threshold=0.5, valid=valid)
        assert sorted(pairs) == [(0, 1), (1, 0)]
        assert ua == [] and ub == []

    def test_assign_mask_forces_miss(self):
        # Если все валидные пары дороже порога — назначение срывается,
        # запрещённая дешёвая пара НЕ должна просочиться.
        cost = np.array([[0.1, 0.9]])
        valid = np.array([[False, True]])
        pairs, ua, ub = assign(cost, threshold=0.5, valid=valid)
        assert pairs == [] and ua == [0] and ub == [0, 1]


# ---------------------------------------------------------------- ByteTrack

class TestByteTracker:
    def test_straight_motion_keeps_one_id(self):
        tr = ByteTracker(min_hits=2)
        ids = []
        for i in range(6):
            x = 10.0 + i * 20.0
            res = feed(tr, [[x, 50.0, x + 40.0, 130.0]], [0.9], ["person"])
            ids.append(res.assignments[0])
        # Первый кадр рождает неподтверждённый трек (None), дальше — один ID
        assert ids[0] is None
        assert len(set(ids[1:])) == 1 and ids[1] is not None

    def test_occlusion_within_max_age_keeps_id(self):
        tr = ByteTracker(min_hits=2, max_age_frames=3)
        for i in range(3):
            x = 10.0 + i * 10.0
            feed(tr, [[x, 50.0, x + 40.0, 130.0]], [0.9], ["person"])
        # Две «пустые» паузы по 0.2с — объект в окклюзии, трек жив (KF)
        for _ in range(2):
            feed(tr, [], [], [])
        x = 70.0
        res = feed(tr, [[x, 50.0, x + 40.0, 130.0]], [0.9], ["person"])
        assert res.assignments[0] is not None

    def test_lost_beyond_max_age_closes(self):
        tr = ByteTracker(min_hits=2, max_age_frames=3)
        for i in range(3):
            x = 10.0 + i * 10.0
            feed(tr, [[x, 50.0, x + 40.0, 130.0]], [0.9], ["person"])
        closed = []
        for _ in range(5):  # 4-й пустой кадр: frames_since_update=4 > 3
            res = feed(tr, [], [], [])
            closed.extend(res.closed)
        assert len(closed) == 1 and closed[0].kind == "closed"
        assert len(tr.tracks) == 0

    def test_source_stall_does_not_kill_track(self):
        """Жизнь трека — в кадрах, а не в секундах: если источник молчал
        30 секунд и прислал один пустой кадр, счётчик сдвинулся на единицу —
        трек жив. Убивать треки паузами инференса/источника — значит
        рождать новые id на ровном месте. Медленный в пикселях объект
        (удалённый самолёт) после паузы опознаётся заново: скорость KF
        ~нулевая, предсказание остаётся на месте, ковариация за паузу
        раздулась и гейт пропускает."""
        tr = ByteTracker(min_hits=2, max_age_frames=3)
        for _ in range(3):
            feed(tr, [[25.0, 50.0, 65.0, 130.0]], [0.9], ["person"])
        # Кадр через 30 секунд молчания (dt огромный, но это ОДИН кадр)
        feed(tr, [], [], [], dt=30.0)
        res = feed(tr, [[25.0, 50.0, 65.0, 130.0]], [0.9], ["person"], dt=30.0)
        assert res.closed == []
        assert res.assignments[0] is not None  # трек узнал объект заново

    def test_crossing_objects_keep_distinct_ids(self):
        tr = ByteTracker(min_hits=2)
        # Два объекта навстречу (шаг 12px при боксе 40px — реалистичный
        # темп для 5 fps), пересекаются в центре кадра
        ids_a, ids_b = [], []
        for i in range(10):
            xa = 100.0 + i * 12.0
            xb = 260.0 - i * 12.0
            res = feed(tr, [[xa, 50.0, xa + 40.0, 130.0],
                            [xb, 55.0, xb + 40.0, 135.0]],
                       [0.9, 0.9], ["person", "person"])
            ids_a.append(res.assignments[0])
            ids_b.append(res.assignments[1])
        confirmed = [(a, b) for a, b in zip(ids_a, ids_b) if a and b]
        assert all(a != b for a, b in confirmed)
        # Каждый объект держит свой ID после пересечения
        a_ids = {a for a, _ in confirmed}
        b_ids = {b for _, b in confirmed}
        assert len(a_ids) == 1 and len(b_ids) == 1

    def test_duplicate_boxes_spawn_single_track(self):
        """Два пересекающихся бокса на одном объекте не плодят два трека:
        второй получает id первого (проверка при рождении), opened — ровно
        один. Иначе близнецы конкурируют за детекции и id прыгает."""
        tr = ByteTracker(min_hits=2)
        boxes = [[0.0, 0.0, 100.0, 100.0], [5.0, 0.0, 105.0, 100.0]]
        res1 = feed(tr, boxes, [0.9, 0.85], ["person", "person"])
        assert len(tr.tracks) == 1
        assert res1.opened == []  # ещё не подтверждён
        res2 = feed(tr, boxes, [0.9, 0.85], ["person", "person"])
        assert len(tr.tracks) == 1
        assert len(res2.opened) == 1
        # обе детекции одного объекта получили один id
        assert res2.assignments[0] is not None
        assert res2.assignments[0] == res2.assignments[1]

    def test_far_apart_detections_still_spawn_separate_tracks(self):
        """Подавление дублей не склеивает разнесённые объекты: два бокса
        без пересечения — два трека."""
        tr = ByteTracker(min_hits=2)
        feed(tr, [[0.0, 0.0, 50.0, 50.0], [500.0, 500.0, 550.0, 550.0]],
             [0.9, 0.9], ["person", "person"])
        assert len(tr.tracks) == 2

    def test_different_classes_do_not_swap_ids(self):
        tr = ByteTracker(min_hits=2)
        for i in range(3):
            x = 100.0 + i * 5.0
            feed(tr, [[x, 50.0, x + 40.0, 130.0]], [0.9], ["person"])
        # В тот же бокс прилетает car: person-трек не должен его унаследовать
        res = feed(tr, [[110.0, 50.0, 150.0, 130.0]], [0.9], ["car"])
        assert res.assignments[0] is None  # новый car-трек ещё не подтверждён

    def test_weak_detection_revives_lost_track(self):
        """Суть ByteTrack: слабая детекция в окклюзии сохраняет ID."""
        tr = ByteTracker(high_thresh=0.6, low_thresh=0.25, min_hits=2,
                         max_age_frames=5)
        for i in range(3):
            x = 10.0 + i * 5.0
            feed(tr, [[x, 50.0, x + 40.0, 130.0]], [0.9], ["person"])
        # Кадр со СЛАБОЙ детекцией на прежнем месте
        res = feed(tr, [[20.0, 50.0, 60.0, 130.0]], [0.3], ["person"])
        assert res.assignments[0] is not None

    def test_single_phantom_is_not_a_track(self):
        tr = ByteTracker(min_hits=2)
        res = feed(tr, [[0.0, 0.0, 50.0, 50.0]], [0.9], ["person"])
        assert res.assignments == [None]
        # Фантом исчез, не успев подтвердиться, — событий closed нет
        res = feed(tr, [], [], [])
        res = feed(tr, [], [], [], dt=1.0)
        assert res.closed == []

    def test_opened_fires_on_confirmation_not_birth(self):
        """opened = рождение строки tracks в БД. На рождении (hits=1) его
        быть не должно: смерть неподтверждённого фантома тихая, и opened-на-
        рождении оставлял бы в БД вечный «active»-призрак."""
        tr = ByteTracker(min_hits=2)
        res1 = feed(tr, [[0.0, 0.0, 50.0, 50.0]], [0.9], ["person"])
        assert res1.opened == []
        res2 = feed(tr, [[5.0, 0.0, 55.0, 50.0]], [0.9], ["person"])
        assert len(res2.opened) == 1
        assert res2.opened[0].kind == "opened"
        # Строка рождается подтверждённой: два кадра уже набрано
        assert res2.opened[0].hits == 2

    def test_phantom_death_leaves_no_events_at_all(self):
        """Полный жизненный цикл фантома: ни opened, ни closed — в БД не
        попадает ни одной строки, висящей «на сцене»."""
        tr = ByteTracker(min_hits=2, max_age_frames=1)
        res = feed(tr, [[0.0, 0.0, 50.0, 50.0]], [0.9], ["person"])
        assert res.opened == []
        res = feed(tr, [], [], [])   # frames_since_update=1, ещё жив
        assert res.closed == []
        res = feed(tr, [], [], [])   # 2 > 1 — фантом тихо умер
        assert res.opened == [] and res.closed == []
        assert tr.tracks == []


# ---------------------------------------------------------------- registry

class TestStreamTrackers:
    def make(self, **kw):
        defaults = dict(default_dt=0.2, idle_evict_sec=300.0)
        defaults.update(kw)
        return StreamTrackers(**defaults)

    def test_track_budget_scales_with_stream_fps(self):
        """Бюджет жизни потерянного трека — физические секунды, пересчитанные
        в кадры под fps потока: 3с при 15fps = 45 кадров; без fps — по
        default_dt (3с / 0.2с = 15 кадров)."""
        reg = self.make(max_age_sec=3.0)
        objs = [{"class": "person", "confidence": 0.9,
                 "bbox": {"x1": 0.0, "y1": 0.0, "x2": 50.0, "y2": 50.0}}]
        reg.submit(1, 100, None, None, objs, fps=15)
        assert reg._streams[1].tracker.max_age_frames == 45
        reg.submit(2, 100, None, None, objs)
        assert reg._streams[2].tracker.max_age_frames == 15

    def test_in_order_frames_released_immediately(self):
        """Каждый кадр уходит в трекер своим submit — без удержания
        (удержание стоило бы межкадровый интервал латентности)."""
        reg = self.make()
        objs = lambda x: [{"class": "person", "confidence": 0.9,  # noqa: E731
                           "bbox": {"x1": x, "y1": 0.0, "x2": x + 50.0, "y2": 50.0}}]
        released, dup = reg.submit(1, 100, None, None, objs(0.0))
        assert [f.frame_index for f in released] == [100] and not dup
        released, _ = reg.submit(1, 101, None, None, objs(10.0))
        assert [f.frame_index for f in released] == [101]

    def test_duplicate_frame_dropped(self):
        reg = self.make()
        objs = [{"class": "person", "confidence": 0.9,
                 "bbox": {"x1": 0.0, "y1": 0.0, "x2": 50.0, "y2": 50.0}}]
        reg.submit(1, 100, None, None, objs)
        released, dup = reg.submit(1, 100, None, None, objs)
        assert dup and released == []

    def test_late_frame_does_not_rewind_clock(self):
        """Поздний кадр не откатывает такт стрима: last_captured идёт только
        вперёд, иначе следующий кадр получил бы раздутый dt для KF."""
        reg = self.make()
        objs = [{"class": "person", "confidence": 0.9,
                 "bbox": {"x1": 0.0, "y1": 0.0, "x2": 50.0, "y2": 50.0}}]
        reg.submit(1, 100, None, 10.0, objs)
        reg.submit(1, 102, None, 10.4, objs)
        reg.submit(1, 101, None, 10.2, objs)  # поздний
        assert reg._streams[1].last_captured == 10.4

    def test_late_frame_tracked_directly(self):
        reg = self.make()
        objs = lambda x: [{"class": "person", "confidence": 0.9,  # noqa: E731
                           "bbox": {"x1": x, "y1": 0.0, "x2": x + 50.0, "y2": 50.0}}]
        reg.submit(1, 100, None, None, objs(0.0))
        reg.submit(1, 101, None, None, objs(10.0))
        # 99 припозднился — идёт в трекер сразу, латентность не растёт
        released, _ = reg.submit(1, 99, None, None, objs(-10.0))
        assert [f.frame_index for f in released] == [99]

    def test_stable_id_through_registry(self):
        reg = self.make()
        ids = []
        for i in range(6):
            x = float(i * 20)
            objs = [{"class": "person", "confidence": 0.9,
                     "bbox": {"x1": x, "y1": 0.0, "x2": x + 50.0, "y2": 50.0}}]
            released, _ = reg.submit(1, 100 + i, None, None, objs)
            for f in released:
                ids.append(f.objects[0]["track_id"])
        confirmed = [i for i in ids if i is not None]
        assert confirmed and len(set(confirmed)) == 1

    def test_streams_are_isolated(self):
        reg = self.make()
        objs = lambda x: [{"class": "person", "confidence": 0.9,  # noqa: E731
                           "bbox": {"x1": x, "y1": 0.0, "x2": x + 50.0, "y2": 50.0}}]
        for i in range(4):
            reg.submit(1, 100 + i, None, None, objs(float(i * 20)))
            reg.submit(2, 500 + i, None, None, objs(float(i * 20)))
        # Один и тот же frame_index в разных стримах — не дубль
        released, dup = reg.submit(2, 100, None, None, objs(0.0))
        assert not dup

    def test_evict_idle_closes_tracks(self):
        reg = self.make(idle_evict_sec=0.01)
        objs = [{"class": "person", "confidence": 0.9,
                 "bbox": {"x1": 0.0, "y1": 0.0, "x2": 50.0, "y2": 50.0}}]
        for i in range(3):
            reg.submit(1, 100 + i, None, None, objs)
        import time
        time.sleep(0.02)
        closed = [e for f in reg.evict_idle() for e in f.closed]
        assert len(closed) == 1 and closed[0].class_name == "person"
