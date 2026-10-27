"""Планирование чанков обработки видео.

Длинное видео режется на временные чанки [start_s, end_s), которые
параллельно обрабатывают разные реплики video-consumer. Каждое сообщение
в video_requests несёт свой диапазон; владение кадрами — по ВРЕМЕННОЙ
сетке (чанк владеет отметками k/extract_fps в своём диапазоне, сэмпл —
первый кадр >= отметки). Измерено на CFR- и VFR-роликах: индексная сетка
(idx = round((pts-pts0)/period)) на VFR даёт пропуски и двойные сэмплы,
временная — нет; там же подтверждён выбор cv2-seek вместо линейного
полного чтения (~1.6x выигрыша).

Короткие видео (меньше video_min_chunk_duration) не режутся: оверхед
N открытий потока и seek'ов на коротком ролике превышает выигрыш от
параллельности.
"""
import logging

import cv2

logger = logging.getLogger(__name__)


def probe_media(source: str) -> tuple[float, float] | None:
    """Проба метаданных БЕЗ декода: (duration_s, fps).

    Читает только заголовок (moov) — миллисекунды даже по HTTP.
    None, если файл не открывается или метаданные пусты/врουν (<= 0):
    вызывающий обязан деградировать в одиночный проход, а не падать.
    """
    cap = cv2.VideoCapture(source)
    if not cap.isOpened():
        return None
    try:
        fps = cap.get(cv2.CAP_PROP_FPS)
        total = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    finally:
        cap.release()
    if fps <= 0 or total <= 0:
        return None
    return total / fps, fps


def should_chunk(duration_s: float, min_duration_s: float) -> bool:
    """Гейт: чанкуем только видео длиннее порога."""
    return duration_s >= min_duration_s


def plan_chunks(duration_s: float, chunk_sec: float) -> list[tuple[float, float]]:
    """Границы чанков [start_s, end_s): без дыр и перекрытий.

    Последний чанк подрезается под остаток. Детерминирован: одинаковые
    входные данные дают одинаковый план — воркер восстанавливает свой
    бюджет из (duration, chunk_sec) без дополнительных полей в сообщении.
    """
    if duration_s <= 0 or chunk_sec <= 0:
        return []
    chunks: list[tuple[float, float]] = []
    start = 0.0
    while start < duration_s - 1e-9:
        end = min(start + chunk_sec, duration_s)
        chunks.append((start, end))
        start = end
    return chunks


def allocate_sample_budgets(num_chunks: int, total_budget: int) -> list[int]:
    """Распределить ГЛОБАЛЬНЫЙ бюджет сэмплов (max_frames // step) по чанкам.

    total_budget <= 0 — лимита нет (пустой список). Бюджет делится
    пропорционально числу чанков с остатком первым чанкам; если бюджета
    меньше, чем чанков, — round-robin (часть чанков получит 0 сэмплов).
    """
    if num_chunks <= 0 or total_budget <= 0:
        return []
    if total_budget >= num_chunks:
        base, rem = divmod(total_budget, num_chunks)
        return [base + (1 if i < rem else 0) for i in range(num_chunks)]
    budgets = [0] * num_chunks
    for i in range(total_budget):
        budgets[i % num_chunks] += 1
    return budgets


def chunk_slot(chunks: list[tuple[float, float]], start_s: float) -> int:
    """Индекс чанка с данной левой границей (-1, если не найден)."""
    for i, (s, _e) in enumerate(chunks):
        if abs(s - start_s) < 1e-6:
            return i
    return -1
