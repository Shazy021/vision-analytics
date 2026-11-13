"""Калмановский фильтр с моделью равномерного движения для трекера.

Состояние — как в SORT/ByteTrack: (cx, cy, a, h) + скорости, где cx/cy —
центр бокса, a = w/h (aspect ratio), h — высота. Работаем в этом пространстве
(а не в x1y1x2y2), потому что предсказание масштаба объекта линейно: объект
едет к камере — h растёт с примерно постоянной скоростью, а x1/x2 нелинейны.

Фильтр пишется руками на numpy (без filterpy): матрицы 8x8, операции —
предсказание и обновление; весь смысл SORT-фильтрации в этих двух формулах.
dt передаётся на каждый шаг (у стримов интервал между кадрами неровный —
async-ответы Triton), поэтому F и Q строятся под конкретный dt.

Пространство — пиксели оригинального кадра; каждый трекер живёт в масштабе
своего стрима, нормализация не нужна.
"""
import numpy as np

# Число размерностей наблюдения (cx, cy, a, h) и состояния (они + скорости)
_Z_DIM = 4
_X_DIM = 8

# Шум измерения. Центр бокса — в пикселях, допуск ~10 px; aspect ratio и
# высота — безразмерные/пиксельные величины с другой природой, для них
# допуски свои. Значения — из SORT (std_weight_position = 1/20 от h), здесь
# зафиксированы константами: h в знаменателе шумов делал бы фильтр
# неустойчивым на мелких объектах, а разница масштабов стримов нивелируется
# тем, что метчинг решает IoU, фильтр лишь предсказывает позицию.
_STD_POS = 10.0
_STD_ASPECT = 0.1
_STD_H = 10.0

# Шум процесса — ускорение: как быстро фильтр разрешает объекту «сменить
# курс». 10 px/s² — типичная динамика пешехода в кадре 1080p; aspect ratio
# меняется заметно медленнее формы-силуэта.
_STD_ACC = 10.0
_STD_ACC_ASPECT = 0.01


def tlbr_to_z(bbox: np.ndarray) -> np.ndarray:
    """[x1, y1, x2, y2] → [cx, cy, a, h] (наблюдаемое пространство)."""
    w = bbox[2] - bbox[0]
    h = bbox[3] - bbox[1]
    return np.array([bbox[0] + w / 2.0, bbox[1] + h / 2.0,
                     w / h if h > 0 else 1.0, h], dtype=np.float64)


def z_to_tlbr(z: np.ndarray) -> np.ndarray:
    """[cx, cy, a, h] → [x1, y1, x2, y2]."""
    w = z[2] * z[3]
    return np.array([z[0] - w / 2.0, z[1] - z[3] / 2.0,
                     z[0] + w / 2.0, z[1] + z[3] / 2.0], dtype=np.float64)


class KalmanBox:
    """KF равномерного движения одного бокса.

    mean/cov живут между predict() и update(); predict(dt) сдвигает позицию
    по скорости и раздувает ковариацию, update(z) притягивает к измерению.
    """

    def __init__(self, bbox: np.ndarray):
        # Скорости на старте нулевые: направление движения объекта трекер
        # оценит сам за первые кадры; до этого предсказание = «стоит».
        self.mean = np.zeros(_X_DIM, dtype=np.float64)
        self.mean[:_Z_DIM] = tlbr_to_z(np.asarray(bbox, dtype=np.float64))
        # Дисперсии стартовой ковариации: по позициям — квадрат допуска
        # измерения, по скоростям — заведомо крупно (скорость неизвестна),
        # фильтр сойдётся за 2-3 обновления.
        self.cov = np.diag([
            _STD_POS ** 2, _STD_POS ** 2, _STD_ASPECT ** 2, _STD_H ** 2,
            100.0 ** 2, 100.0 ** 2, 0.1 ** 2, 100.0 ** 2,
        ])
        # H — наблюдатель: измеряем только позиции, скорости нет.
        self._h = np.zeros((_Z_DIM, _X_DIM), dtype=np.float64)
        self._h[:_Z_DIM, :_Z_DIM] = np.eye(_Z_DIM)
        self._std = np.array([_STD_POS, _STD_POS, _STD_ASPECT, _STD_H])

    def predict(self, dt: float) -> np.ndarray:
        """Сдвинуть состояние на dt секунд; вернуть предсказанный бокс (tlbr)."""
        dt = max(dt, 1e-6)
        f = np.eye(_X_DIM)
        f[:_Z_DIM, _Z_DIM:] = np.eye(_Z_DIM) * dt
        self.mean = f @ self.mean
        self.cov = f @ self.cov @ f.T + self._process_noise(dt)
        return z_to_tlbr(self.mean[:_Z_DIM])

    def update(self, bbox: np.ndarray) -> None:
        """Притянуть состояние к измерению [x1, y1, x2, y2]."""
        z = tlbr_to_z(np.asarray(bbox, dtype=np.float64))
        # Шум измерения масштабируется высотой объекта: для крупного плана
        # ошибка детектора в пикселях больше (классический трюк SORT).
        std = self._std * max(z[3], 1.0) / 100.0
        r = np.diag(np.maximum(std, 1e-6) ** 2)
        y = z - self.mean[:_Z_DIM]
        s = self.cov[:_Z_DIM, :_Z_DIM] + r
        k = self.cov @ self._h.T @ np.linalg.inv(s)
        self.mean = self.mean + k @ y
        ident = np.eye(_X_DIM)
        self.cov = (ident - k @ self._h) @ self.cov

    @property
    def tlbr(self) -> np.ndarray:
        """Текущее состояние как [x1, y1, x2, y2]."""
        return z_to_tlbr(self.mean[:_Z_DIM])

    def _process_noise(self, dt: float) -> np.ndarray:
        """Q(dt) для модели постоянного ускорения-шума: позиция набирает
        неопределённость как ½·a·dt², скорость — как a·dt."""
        acc = np.array([_STD_ACC, _STD_ACC, _STD_ACC_ASPECT, _STD_ACC])
        q_pos = (0.5 * acc * dt ** 2) ** 2
        q_vel = (acc * dt) ** 2
        return np.diag(np.concatenate([q_pos, q_vel]))
