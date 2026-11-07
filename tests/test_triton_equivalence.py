"""
Эквивалентность ultralytics vs Triton на реальных кадрах (интеграционный).

Прогоняет одинаковые кадры через оба бэкенда и требует совпадения:
числа детекций, классов и геометрии рамок (IoU > 0.9). Ловит FP16-дрейф
и ошибки препроцессинга/обратного хода — то, что юниты с моками не видят.

Пропускается, если Triton не поднят или нет тестового видео:
    docker compose up -d triton   # или standalone-контейнер на localhost:18001
    $env:RUN_TRITON_TESTS='1'; pytest tests/test_triton_equivalence.py -v
Переменные: TRITON_TEST_URL (default localhost:18001 — standalone-порт),
WEIGHTS_PATH (torch-веса для эталона; ultralytics ставится вручную).
"""
import os
from pathlib import Path

import numpy as np
import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_TRITON_TESTS", "") != "1",
    reason="нужен поднятый Triton: RUN_TRITON_TESTS=1 (см. докстринг)",
)

VIDEO = Path("data/Praha_air.mp4")
FRAME_IDS = [804, 816, 828]  # кадры с сильными raw-ответами (см. equiv_check)
CONF, IOU = 0.25, 0.45


def _iou(a, b):
    iw = max(min(a[2], b[2]) - max(a[0], b[0]), 0)
    ih = max(min(a[3], b[3]) - max(a[1], b[1]), 0)
    inter = iw * ih
    union = (a[2] - a[0]) * (a[3] - a[1]) + (b[2] - b[0]) * (b[3] - b[1]) - inter
    return inter / union if union else 0.0


@pytest.fixture(scope="module")
def frames():
    cv2 = pytest.importorskip("cv2")
    if not VIDEO.exists():
        pytest.skip(f"нет тестового видео: {VIDEO}")
    cap = cv2.VideoCapture(str(VIDEO))
    out = []
    for fid in FRAME_IDS:
        cap.set(cv2.CAP_PROP_POS_FRAMES, fid)
        ok, frame = cap.read()
        assert ok, f"не прочитан кадр {fid}"
        out.append((fid, frame))
    cap.release()
    return out


@pytest.fixture(scope="module")
def triton_detector():
    from app.processing.triton_detector import TritonDetector

    url = os.environ.get("TRITON_TEST_URL", "localhost:18001")
    return TritonDetector(url=url)


@pytest.fixture(scope="module")
def reference():
    """Эталон на torch-весах: ultralytics больше не зависимость проекта,
    ставится вручную только для проверки пересобранных engine'ов."""
    ultralytics = pytest.importorskip("ultralytics")

    weights = os.environ.get("WEIGHTS_PATH", "models_cache/yolov8l.pt")
    return ultralytics.YOLO(weights)


def test_backend_agreement(frames, triton_detector, reference):
    """Один и тот же кадр: оба бэкенда дают одни рамки и классы."""
    for fid, frame in frames:
        ref = reference(frame, conf=CONF, iou=IOU, max_det=300,
                        verbose=False)[0].boxes
        ref_xyxy = ref.xyxy.cpu().numpy() if len(ref) else np.zeros((0, 4))
        ref_cls = ref.cls.cpu().numpy().astype(int) if len(ref) else np.zeros(0, int)

        got = triton_detector.detect(frame, frame_index=fid, conf=CONF)

        assert len(got) == len(ref_xyxy), \
            f"кадр {fid}: ultralytics={len(ref_xyxy)}, triton={len(got)}"
        for det, want_cls in zip(got, ref_cls):
            assert det.class_name == reference.names[int(want_cls)], \
                f"кадр {fid}: класс разъехался"
        if len(ref_xyxy):
            for det in got:
                best = max(_iou(det.bbox, r) for r in ref_xyxy)
                assert best > 0.9, \
                    f"кадр {fid}: рамка {det.bbox} не совпала (лучший IoU {best:.2f})"
