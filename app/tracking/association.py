"""Ассоциация «трек ↔ детекция»: IoU-стоимость + венгерский алгоритм.

Венгерский алгоритм (линейное назначение, минимум суммы стоимости) —
scipy.optimize.linear_sum_assignment. scipy — единственная тяжёлая зависимость
трекинга (CPU-wheel ~40 МБ), своя реализация O(n³) на наших матрицах
(~30×30) не окупает ни строк кода, ни рисков.
"""
import numpy as np
from scipy.optimize import linear_sum_assignment


def iou_matrix(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """Матрица IoU двух наборов боксов [N,4] и [M,4] (tlbr, пиксели)."""
    a = np.atleast_2d(a)
    b = np.atleast_2d(b)
    if len(a) == 0 or len(b) == 0:
        return np.zeros((len(a), len(b)), dtype=np.float64)
    # Пересечение: минимум правых минус максимум левых, по каждой оси.
    inter_w = np.clip(np.minimum(a[:, 2:3], b[None, :, 2])
                      - np.maximum(a[:, 0:1], b[None, :, 0]), 0, None)
    inter_h = np.clip(np.minimum(a[:, 3:4], b[None, :, 3])
                      - np.maximum(a[:, 1:2], b[None, :, 1]), 0, None)
    inter = inter_w * inter_h
    area_a = ((a[:, 2] - a[:, 0]) * (a[:, 3] - a[:, 1]))[:, None]
    area_b = ((b[:, 2] - b[:, 0]) * (b[:, 3] - b[:, 1]))[None, :]
    union = area_a + area_b - inter
    return np.divide(inter, union, out=np.zeros_like(inter), where=union > 0)


def assign(cost: np.ndarray, threshold: float,
           valid: np.ndarray | None = None) -> tuple[list[tuple[int, int]], list[int], list[int]]:
    """Линейное назначение по стоимости (меньше — лучше).

    cost — [N, M]; пары с cost > threshold отбрасываются (назначение через
    порог — «этот трек к этой детекции не привязываем вообще»). valid —
    булева маска [N, M]: False-пары исключаются до решения (несовпадение
    классов) — вместо бесконечных стоимостей,
    которые linear_sum_assignment переваривает плохо.

    Возвращает (пары, индексы строк без пары, индексы столбцов без пары).
    """
    n, m = cost.shape
    if n == 0 or m == 0:
        return [], list(range(n)), list(range(m))
    c = cost.copy()
    if valid is not None:
        # Невалидным парам — заведомо худшая стоимость: решатель их не
        # возьмёт, пока есть хоть один валидный вариант в строке/столбце.
        c[~valid] = threshold + 1.0
    rows, cols = linear_sum_assignment(c)
    pairs = [(int(i), int(j)) for i, j in zip(rows, cols)
             if c[i, j] <= threshold]
    matched_r = {i for i, _ in pairs}
    matched_c = {j for _, j in pairs}
    return (pairs,
            [i for i in range(n) if i not in matched_r],
            [j for j in range(m) if j not in matched_c])
