"""
Тесты TritonDetector.

Настоящий Triton не нужен: подставляем фейковый gRPC-клиент и проверяем
НАШУ часть работы — letterbox, обратный ход боксов из padded-полотна,
порог confidence, нарезку батча под лимон движка, разбор выходов.
Сам инференс — ответственность Triton/TensorRT.
"""
import cv2
import numpy as np
import pytest

from app.processing.triton_detector import (
    ENGINE_MAX_BATCH,
    INPUT_SIZE,
    TritonDetector,
    letterbox_geom,
)


class FakeResult:
    def __init__(self, arrays):
        self._arrays = arrays

    def as_numpy(self, name):
        return self._arrays[name]


class FakeClient:
    """Заглушка tritonclient.grpc.InferenceServerClient.

    На каждый infer возвращает пустые детекции для фактического размера
    батча (его читаем из формы входа) и складывает вызовы в .calls.
    """

    def __init__(self, ready=True, responder=None):
        self.ready = ready
        self.calls = []
        self.responder = responder

    def is_model_ready(self, name):
        return self.ready

    def infer(self, model_name, inputs, outputs):
        shape = inputs[0].shape()
        assert list(shape[1:]) == [3, INPUT_SIZE, INPUT_SIZE], \
            f"ожидался вход [B,3,{INPUT_SIZE},{INPUT_SIZE}], пришёл {shape}"
        b = shape[0]
        self.calls.append(b)
        if self.responder:
            return FakeResult(self.responder(b))
        return FakeResult({
            "num_detections": np.zeros((b, 1), dtype=np.int32),
            "boxes": np.zeros((b, 300, 4), dtype=np.float32),
            "scores": np.zeros((b, 300), dtype=np.float32),
            "classes": np.zeros((b, 300), dtype=np.int32),
        })


def _detector(client=None, **kw):
    return TritonDetector(url="fake:8001", client=client or FakeClient(), **kw)


def _frame(h=720, w=1280):
    return np.full((h, w, 3), 127, dtype=np.uint8)


class _Ref:
    def __init__(self, image, index, timestamp=0.0):
        self.image = image
        self.index = index
        self.timestamp = timestamp


# ---------------------------------------------------------------------------
# Препроцессинг
# ---------------------------------------------------------------------------

def test_preprocess_shape_and_geometry():
    """1280x720 -> полотно 640x640, scale 0.5, pad_y 140 (как у ultralytics).

    Вход engine — UINT8, поэтому на выходе сырые пиксели 0..255, без /255."""
    det = _detector()
    tensor, (pad_x, pad_y, scale, oh, ow) = det._preprocess(_frame())

    assert tensor.shape == (3, INPUT_SIZE, INPUT_SIZE)
    assert tensor.dtype == np.uint8
    assert 0 <= tensor.min() and tensor.max() <= 255
    assert (pad_x, pad_y, scale, oh, ow) == (0, 140, 0.5, 720, 1280)


def test_preprocess_rgb_and_contiguous():
    """BGR на входе -> RGB на выходе (первый канал синего кадра — не синий)."""
    det = _detector()
    blue = np.zeros((720, 1280, 3), dtype=np.uint8)
    blue[:, :] = (255, 0, 0)  # BGR: синий
    tensor, _ = det._preprocess(blue)
    # в RGB синий = (0,0,255): канал 2 заполнен, канал 0 пуст (кроме пад-области)
    assert tensor[0, 200:400, 200:400].max() == 0
    assert tensor[2, 200:400, 200:400].mean() == pytest.approx(255.0)
    assert tensor.flags["C_CONTIGUOUS"]


def _numpy_reference(frame):
    """Честная numpy-цепочка letterbox (view BGR->RGB + transpose): эталон,
    с которым сверяется cvtColor+ascontiguousarray. Интерполяция та же
    (INTER_LINEAR) — сравниваем цепочку конвертации, а не ресайз."""
    h, w = frame.shape[:2]
    scale = min(INPUT_SIZE / w, INPUT_SIZE / h)
    nw, nh = int(w * scale), int(h * scale)
    resized = cv2.resize(frame, (nw, nh), interpolation=cv2.INTER_LINEAR)
    pad_x, pad_y = (INPUT_SIZE - nw) // 2, (INPUT_SIZE - nh) // 2
    canvas = np.full((INPUT_SIZE, INPUT_SIZE, 3), 114, dtype=np.uint8)
    canvas[pad_y:pad_y + nh, pad_x:pad_x + nw] = resized
    tensor = np.ascontiguousarray(canvas[:, :, ::-1].transpose(2, 0, 1))
    return tensor, (pad_x, pad_y, scale, h, w)


def test_preprocess_matches_numpy_reference():
    """cvtColor(BGR->RGB)+transpose обязан совпадать с честной numpy-цепочкой
    побайтово — иначе рефакторинг предобработки молча сдвинул бы цвета."""
    rng = np.random.default_rng(7)
    # Нечётный размер: пад по x и y разный, кривые границы тоже проверяются.
    frame = rng.integers(0, 256, (480, 853, 3), dtype=np.uint8)
    det = _detector()

    tensor, geom = det._preprocess(frame)
    ref, ref_geom = _numpy_reference(frame)

    assert geom == ref_geom
    assert tensor.shape == ref.shape
    assert tensor.dtype == ref.dtype
    assert np.array_equal(tensor, ref)


# ---------------------------------------------------------------------------
# Обратный ход боксов
# ---------------------------------------------------------------------------

def test_box_inverse_roundtrip():
    """Бокс из пикселей кадра → padded-полотно → обратно ≈ оригинал."""
    det = _detector()
    # исходный бокс в пикселях 1280x720
    x1, y1, x2, y2 = 200.0, 300.0, 400.0, 500.0
    pad_x, pad_y, scale = 0.0, 140.0, 0.5
    padded = [x1 * scale + pad_x, y1 * scale + pad_y,
              x2 * scale + pad_x, y2 * scale + pad_y]

    boxes = np.array([padded], dtype=np.float32)
    scores = np.array([0.9], dtype=np.float32)
    classes = np.array([4], dtype=np.int32)  # airplane

    dets = det._to_detections(boxes, scores, classes, 1,
                              (pad_x, pad_y, scale, 720, 1280),
                              frame_index=7, timestamp=1.25, conf=0.25)

    assert len(dets) == 1
    assert dets[0].class_name == "airplane"
    assert dets[0].bbox == pytest.approx([x1, y1, x2, y2], abs=1.0)
    assert dets[0].frame_index == 7
    assert dets[0].timestamp == 1.25


def test_boxes_clipped_to_frame():
    """Рамки не вылезают за границы кадра после обратного хода."""
    det = _detector()
    boxes = np.array([[-50.0, 100.0, 700.0, 500.0]], dtype=np.float32)
    scores = np.array([0.8], dtype=np.float32)
    classes = np.array([0], dtype=np.int32)

    dets = det._to_detections(boxes, scores, classes, 1,
                              (0.0, 140.0, 0.5, 720, 1280), 0, 0.0, 0.25)
    x1, y1, x2, y2 = dets[0].bbox
    assert x1 >= 0 and y1 >= 0 and x2 <= 1280 and y2 <= 720


def test_prepare_geometry_describes_full_frame():
    """geom из prepare() описывает путь холст→КАДР 1:1 (без урезанного
    декода): полный кадр 1920x1080 → scale 640/1920, pad_y=140.
    Кольцо: бокс в пикселях кадра → прямой ход → _to_detections → обратно."""
    det = _detector()
    frame = np.full((1080, 1920, 3), 127, dtype=np.uint8)

    _tensor, geom = det.prepare(frame)
    pad_x, pad_y, scale, oh, ow = geom
    assert (oh, ow) == (1080, 1920)
    assert scale == pytest.approx(640 / 1920)
    assert (pad_x, pad_y) == (0, 140)

    # Кольцо координат: кадр → холст → кадр.
    x1, y1, x2, y2 = 400.0, 300.0, 800.0, 700.0
    boxes = np.array([[x1 * scale + pad_x, y1 * scale + pad_y,
                       x2 * scale + pad_x, y2 * scale + pad_y]],
                     dtype=np.float32)
    scores = np.array([0.9], dtype=np.float32)
    classes = np.array([2], dtype=np.int32)  # car

    dets = det._to_detections(boxes, scores, classes, 1, geom, 0, 0.0, 0.25)
    assert dets[0].bbox == pytest.approx([x1, y1, x2, y2], abs=1.0)


def test_empty_num_detections():
    det = _detector()
    assert det._to_detections(np.zeros((0, 4)), np.zeros(0), np.zeros(0, int),
                              0, (0, 140, 0.5, 720, 1280), 0, 0.0, 0.25) == []


# ---------------------------------------------------------------------------
# Пороги confidence
# ---------------------------------------------------------------------------

def test_conf_below_engine_floor_raises():
    """conf ниже вшитого порога движка — явная ошибка, а не тихая потеря."""
    det = _detector()
    with pytest.raises(ValueError, match="ниже порога движка"):
        det.detect_batch([_Ref(_frame(), 0)], conf=0.1)


def test_conf_above_floor_filters():
    """conf выше порога движка отсекает лишние scores на клиенте."""
    def responder(b):
        scores = np.zeros((b, 300), dtype=np.float32)
        scores[0, 0], scores[0, 1] = 0.9, 0.5
        boxes = np.zeros((b, 300, 4), dtype=np.float32)
        boxes[0, :2] = [100, 200, 300, 400]
        classes = np.zeros((b, 300), dtype=np.int32)
        num = np.array([[2]], dtype=np.int32)
        return {"num_detections": num, "boxes": boxes,
                "scores": scores, "classes": classes}

    det = _detector(client=FakeClient(responder=responder))
    dets = det.detect_batch([_Ref(_frame(), 0)], conf=0.7)
    assert len(dets[0]) == 1
    assert dets[0][0].confidence == pytest.approx(0.9)


# ---------------------------------------------------------------------------
# Батчинг
# ---------------------------------------------------------------------------

def test_long_batch_chunked_to_engine_limit():
    """20 кадров уходят двумя вызовами: 16 + 4 (лимит движка)."""
    client = FakeClient()
    det = _detector(client=client)
    frames = [_Ref(_frame(), i) for i in range(ENGINE_MAX_BATCH + 4)]

    results = det.detect_batch(frames)

    assert len(results) == 20
    assert client.calls == [ENGINE_MAX_BATCH, 4]


def test_detect_single_wraps_into_batch():
    client = FakeClient()
    det = _detector(client=client)
    dets = det.detect(_frame(), frame_index=3, timestamp=0.5)
    assert dets == []
    assert client.calls == [1]


# ---------------------------------------------------------------------------
# Подключение
# ---------------------------------------------------------------------------

def test_not_ready_raises():
    with pytest.raises(RuntimeError, match="not ready"):
        TritonDetector(url="fake:8001", client=FakeClient(ready=False))


# ---------------------------------------------------------------------------
# JPEG-intake (ансамбль yolo_jpeg: предобработка в DALI на GPU)
# ---------------------------------------------------------------------------

def test_letterbox_geom_matches_preprocess():
    """letterbox_geom(w,h) обязан совпадать с геометрией _preprocess — иначе
    jpeg-путь (обратный ход из размеров) разъедется с cv2-путём (из кадра)."""
    for (h, w) in [(720, 1280), (1080, 1920), (480, 853), (640, 640)]:
        det = _detector()
        _tensor, geom = det._preprocess(_frame(h=h, w=w))
        assert geom == letterbox_geom(w, h), f"{w}x{h}"


def test_prepare_jpeg_returns_bytes_and_geom(monkeypatch):
    from app.core.config import settings
    monkeypatch.setattr(settings, "triton_jpeg_pad", 0)
    det = _detector(jpeg_intake=True)
    jb = b"\xff\xd8\xff\xd9"  # условные JPEG-байты
    payload, geom = det.prepare_jpeg(jb, 1280, 720)
    assert payload == jb
    assert geom == letterbox_geom(1280, 720)


def test_prepare_jpeg_pads_to_bucket(monkeypatch):
    """TRITON_JPEG_PAD: длина округляется вверх до кратного бакету —
    иначе динамический батчинг Triton'а не склеивает кадры разной длины."""
    from app.core.config import settings
    monkeypatch.setattr(settings, "triton_jpeg_pad", 1024)
    det = _detector(jpeg_intake=True)
    jb = b"\xff\xd8" + b"\x01" * 100
    payload, _geom = det.prepare_jpeg(jb, 1280, 720)
    assert len(payload) == 1024
    assert payload.startswith(jb)
    assert payload[len(jb):] == bytes(1024 - len(jb))


def test_prepare_jpeg_bucket_exact_multiple_untouched(monkeypatch):
    """Длина кратна бакету — байты не трогаем вообще."""
    from app.core.config import settings
    monkeypatch.setattr(settings, "triton_jpeg_pad", 64)
    det = _detector(jpeg_intake=True)
    jb = b"\xff\xd8" + b"\x01" * 62  # ровно 64 байта
    payload, _geom = det.prepare_jpeg(jb, 1280, 720)
    assert payload == jb


class FakeAsyncClient:
    """Заглушка для async_infer: ловит вход (имя/форму) в .sent."""

    def __init__(self):
        self.sent = []

    def is_model_ready(self, name):
        return True

    def async_infer(self, model_name, inputs, outputs, callback, request_id):
        self.sent.append((model_name, inputs[0].name(),
                          list(inputs[0].shape())))
        callback(None, None)  # без детекций: as_numpy не дёргается


def test_submit_async_jpeg_sends_jpeg_input(monkeypatch):
    """jpeg-режим: вход называется «jpeg», форма [1, N] (batch-мер обязателен
    при max_batch_size>1), модель — ансамбль yolo_jpeg."""
    from app.core.config import settings
    monkeypatch.setattr(settings, "triton_jpeg_pad", 0)
    client = FakeAsyncClient()
    det = TritonDetector(url="fake:8001", client=client, jpeg_intake=True)
    jb = bytes(range(256)) * 40
    det.submit_async(1, det.prepare_jpeg(jb, 1280, 720), frame_index=0)
    model, name, shape = client.sent[0]
    assert model == "yolo_jpeg"
    assert name == "jpeg"
    assert shape == [1, len(jb)]


def test_submit_async_cv2_sends_images_input():
    """cv2-режим (по умолчанию): вход «images» [1,3,640,640], модель —
    yolo_detector. Регрессия на то, что jpeg-ветка не тронула старый путь."""
    client = FakeAsyncClient()
    det = TritonDetector(url="fake:8001", client=client)
    tensor, geom = det.prepare(_frame())
    det.submit_async(1, (tensor, geom), frame_index=0)
    model, name, shape = client.sent[0]
    assert model == "yolo_detector"
    assert name == "images"
    assert shape == [1, 3, INPUT_SIZE, INPUT_SIZE]
