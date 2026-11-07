"""TritonDetector: инференс через Triton Inference Server (TensorRT engine).

Единственный бэкенд детекции платформы: detect() / detect_batch() возвращают
списки Detection — этот контракт используют processor.py (видео) и
yolo_frames.py (стримы). Модель живёт не в процессе, а в Triton: сюда
приезжают пиксели, обратно уходят готовые боксы (NMS вшит в граф
экспорта, см. внешнюю репу yolo-trt-nms).

Зачем: torch-модель в каждом консьюмере стоит 2.5 ГБ VRAM и греет CPU при
старте; с Triton GPU держит ОДИН сервер на все реплики, консьюмерам нужны
только numpy + лёгкий gRPC-клиент (консьюмеры живут на CPU-образе без torch).

Формат выхода движка (проверено тестом эквивалентности на реальных кадрах):
  boxes   — xyxy в ПИКСЕЛЯХ padded-полотна 640 (не нормализованные yxyx:
            экспорт из ultralytics 8.x отдаёт сырые боксы в пикселях,
            EfficientNMS v2 сохраняет единицы входа)
  scores  — уверенность после NMS, по убыванию
  classes — индексы COCO
Обратный ход letterbox — единственный матимпортный момент: отнять pad,
разделить на scale, обрезать по краям кадра.

Порог confidence вшит в движок (0.25). Вызвать detect с conf НИЖЕ нельзя —
движок такие scores не отдаёт, вернём явную ошибку вместо тихой потери.
"""
from __future__ import annotations

import itertools
import logging
import queue
from dataclasses import dataclass

import cv2
import numpy as np

from app.core.config import settings
from app.processing.base import Detection

logger = logging.getLogger(__name__)

# Должно совпадать с собранным engine (export.py --mode triton).
INPUT_SIZE = 640
ENGINE_SCORE_FLOOR = 0.25   # score_threshold, вшитый в EfficientNMS
ENGINE_MAX_BATCH = 16       # maxShapes при сборке engine
# Вход engine — UINT8 (export.py --input-dtype uint8): сырые пиксели холста
# 0..255, нормализация /255 живёт первым слоем графа. Смысл: gRPC-запрос
# 1.23 МБ вместо 4.9 МБ — ровно вчетверо меньше сериализации и копирований
# на потоке отправки (её потолок ~110 req/s на канал был мерян fp32-входом).
# Точность не теряется: холст и так uint8, fp32-преобразование было
# бесполезным раздуванием байт перед отправкой.
INPUT_DTYPE = "UINT8"
INPUT_NP_DTYPE = np.uint8

# Имена COCO-классов по индексу — тот же порядок, что у ultralytics.
# Держим локально: этот модуль обязан работать без ultralytics/torch.
COCO_NAMES = (
    "person", "bicycle", "car", "motorcycle", "airplane", "bus", "train",
    "truck", "boat", "traffic light", "fire hydrant", "stop sign",
    "parking meter", "bench", "bird", "cat", "dog", "horse", "sheep", "cow",
    "elephant", "bear", "zebra", "giraffe", "backpack", "umbrella", "handbag",
    "tie", "suitcase", "frisbee", "skis", "snowboard", "sports ball", "kite",
    "baseball bat", "baseball glove", "skateboard", "surfboard",
    "tennis racket", "bottle", "wine glass", "cup", "fork", "knife", "spoon",
    "bowl", "banana", "apple", "sandwich", "orange", "broccoli", "carrot",
    "hot dog", "pizza", "donut", "cake", "chair", "couch", "potted plant",
    "bed", "dining table", "toilet", "tv", "laptop", "mouse", "remote",
    "keyboard", "cell phone", "microwave", "oven", "toaster", "sink",
    "refrigerator", "book", "clock", "vase", "scissors", "teddy bear",
    "hair drier", "toothbrush",
)


@dataclass
class _FrameLike:
    """Унификация входа detect()/detect_batch(): у обоих есть .image/.index/.timestamp."""

    image: np.ndarray
    index: int
    timestamp: float = 0.0


class TritonDetector:
    """Клиент Triton: синхронный контракт detect()/detect_batch() для видео
    + асинхронный поток кадров (prepare/submit_async/next_result) для
    стрим-конвейера.

    client — для тестов: можно подставить заглушку вместо gRPC.
    """

    def __init__(self, url: str | None = None, model_name: str | None = None,
                 client=None, channels: int | None = None):
        self.url = url or settings.triton_url
        self.model_name = model_name or settings.triton_model_name

        if client is not None:
            # Заглушка для тестов: один канал.
            self._clients = [client]
        else:
            # Ленивый импорт: модуль импортируют тесты и CPU-образы,
            # где tritonclient может не стоять до первого конструктора.
            import tritonclient.grpc as grpcclient
            # Несколько НЕЗАВИСИМЫХ gRPC-каналов: один канал сериализует
            # крупные запросы (один сериализатор + flow control), и потоки
            # отправки толкаются на нём. Замер: 1 канал ~110 req/s,
            # 4 канала — до предела GPU.
            n = max(1, channels or settings.triton_channels)
            self._clients = [grpcclient.InferenceServerClient(url=self.url)
                             for _ in range(n)]
        self._client = self._clients[0]
        self._client_rr = itertools.cycle(range(len(self._clients)))

        # Состояние асинхронного потока: очередь готовых результатов
        # (пишет completion-поток клиента, читает потребитель).
        self._async_results: queue.Queue = queue.Queue()

        # Проверка на старте: лучше упасть сразу с понятым текстом, чем
        # молча сыпать InferenceServerException на каждом батче.
        if not self._client.is_model_ready(self.model_name):
            raise RuntimeError(
                f"Triton at {self.url}: model '{self.model_name}' is not ready")
        logger.info("[TritonDetector] connected to %s, model=%s, channels=%d",
                    self.url, self.model_name, len(self._clients))

    # ------------------------------------------------------------------
    # Публичный контракт (использует processor.py)
    # ------------------------------------------------------------------
    def detect(self, frame, frame_index: int = 0, timestamp: float = 0.0,
               conf: float = 0.25) -> list[Detection]:
        return self.detect_batch(
            [_FrameLike(image=frame, index=frame_index, timestamp=timestamp)],
            conf=conf)[0]

    def detect_batch(self, frames: list, conf: float = 0.25) -> list[list[Detection]]:
        """Инференс батчем. frames — объекты с .image/.index/.timestamp."""
        if conf < ENGINE_SCORE_FLOOR - 1e-9:
            raise ValueError(
                f"conf={conf} ниже порога движка ({ENGINE_SCORE_FLOOR}): "
                "NMS с таким порогом вшит в engine, недостающие scores не "
                "вернутся. Пересоберите engine с меньшим --score или "
                "поднимите conf.")

        prepared = [self._preprocess(f.image) for f in frames]
        results: list[list[Detection]] = [[] for _ in frames]

        # Engine собран с max_batch ENGINE_MAX_BATCH — режем длинный батч
        # на куски, чтобы вызов не упал на валидации формы.
        for start in range(0, len(prepared), ENGINE_MAX_BATCH):
            chunk = prepared[start:start + ENGINE_MAX_BATCH]
            refs = frames[start:start + ENGINE_MAX_BATCH]
            num, boxes, scores, classes = self._infer_chunk([t for t, _ in chunk])
            for i, ref in enumerate(refs):
                results[start + i] = self._to_detections(
                    boxes[i], scores[i], classes[i], int(num[i, 0]),
                    chunk[i][1], ref.index, ref.timestamp, conf)
        return results

    # ------------------------------------------------------------------
    # Асинхронный поток кадров (стрим-конвейер)
    # ------------------------------------------------------------------
    #
    # Синхронный infer() неудобен для НЕПРЕРЫВНОГО потока: отправитель ждёт
    # ответ и не может держать в сети много работы, а жирные клиентские батчи
    # Triton ещё и не склеивает (preferred_batch_size уже достигнут одним
    # запросом). Замер на живом сервере (RTX 5080, yolov8l engine):
    #   батчи по 8 синхронными запросами — 61 fps, GPU занят ~10%;
    #   поток одиночных async-запросов  — 210 fps, GPU занят ~100%.
    # Поэтому стрим ходит сюда: submit_async() возвращает мгновенно,
    # результат приезжает в колбэк completion-потока и ложится в очередь;
    # конкурентные запросы склеивает динамический батчинг Triton'а.

    supports_async = True

    def prepare(self, frame):
        """Letterbox кадра (дорогая CPU-часть). Вызывать из decode-воркера,
        чтобы потоку отправки оставался только сетевой вызов.

        Кадр должен быть ПОЛНОГО размера оригинала: движок отдаёт боксы в
        пикселях холста 640, а потребитель ждёт их в пикселях оригинала —
        геометрия из _preprocess описывает ровно этот путь.
        """
        return self._preprocess(frame)

    def submit_async(self, corr: int, prepared, frame_index: int = 0,
                     timestamp: float = 0.0, conf: float = 0.25) -> None:
        """Один подготовленный кадр → async_infer, мгновенный возврат.

        corr — correlation id ОТ ПОТРЕБИТЕЛЯ: он обязан зарегистрировать
        кадр в своих pending ДО вызова — ответ может прийти быстрее, чем
        вернётся этот метод (gRPC дёргает done-callback немедленно для
        уже завершённого RPC). Результат приедет в next_result() как
        (corr, list[Detection]) либо (corr, Exception). Ошибка НЕ бросается
        здесь: она приедет в паре, чтобы один кривой кадр не уронил
        отправку остальных.
        """
        if conf < ENGINE_SCORE_FLOOR - 1e-9:
            raise ValueError(
                f"conf={conf} ниже порога движка ({ENGINE_SCORE_FLOOR}): "
                "NMS с таким порогом вшит в engine, недостающие scores не "
                "вернутся.")

        import tritonclient.grpc as grpcclient

        tensor, geom = prepared
        batched = tensor[np.newaxis, ...]
        inp = grpcclient.InferInput("images", list(batched.shape), INPUT_DTYPE)
        inp.set_data_from_numpy(batched)
        requested = [grpcclient.InferRequestedOutput(n) for n in
                     ("num_detections", "boxes", "scores", "classes")]

        def _on_done(result, error):
            # Нить — completion-поток gRPC-клиента: только разобрать и в
            # очередь; тяжёлую обработку делает потребитель результатов.
            if error is not None:
                self._async_results.put((corr, error))
                return
            try:
                num = result.as_numpy("num_detections")
                dets = self._to_detections(
                    result.as_numpy("boxes")[0],
                    result.as_numpy("scores")[0],
                    result.as_numpy("classes")[0],
                    int(num.reshape(-1)[0]),
                    geom, frame_index, timestamp, conf)
                self._async_results.put((corr, dets))
            except Exception as exc:  # кривой ответ движка — как ошибка кадра
                self._async_results.put((corr, exc))

        # Канал выбираем кругом: потоки отправки получают свои соединения,
        # а не дерутся за один сериализатор.
        client = self._clients[next(self._client_rr)]
        client.async_infer(model_name=self.model_name, inputs=[inp],
                           outputs=requested, callback=_on_done,
                           request_id=str(corr))

    def next_result(self, timeout: float | None = None):
        """Ждущий результат (corr, dets | Exception); queue.Empty по таймауту."""
        return self._async_results.get(timeout=timeout)

    # ------------------------------------------------------------------
    # Внутренности
    # ------------------------------------------------------------------
    def _preprocess(self, frame: np.ndarray):
        """Letterbox 640 (как ultralytics) -> (CHW uint8 [0..255], геометрия).

        Геометрия (pad_x, pad_y, scale, orig_h, orig_w) нужна обратному ходу
        боксов: движок оперирует пикселями padded-полотна.

        На выходе — СЫРЫЕ пиксели uint8 (вход engine — TYPE_UINT8, /255
        делает первый слой графа): 1.23 МБ вместо 4.9 МБ на gRPC-запрос.
        Собрано из OpenCV-примитивов, а не numpy-цепочек, и вот почему:
        цепочка canvas[::-1].transpose().astype(float32)/255 делала 3-4 копии
        4.9-мегабайтного тензора и ДЕРЖАЛА GIL всё это время — decode-воркеры
        выстраивались в очередь на GIL и мешали потокам отправки. Здесь:
          • cv2.resize пишет сразу в холст (dst=ROI) — без лишнего буфера;
          • cvtColor(BGR->RGB) — один проход на C++ с отпусканием GIL;
          • transpose+ascontiguousarray — ровно одна копия 1.2 МБ в CHW,
            без fp32-промежутка вообще.
        INTER_LINEAR вместо INTER_AREA: при 1080p->640 на детекции разницы
        нет, а на большом коэффициенте убавки INTER_AREA заметно дороже.
        """
        h, w = frame.shape[:2]
        scale = min(INPUT_SIZE / w, INPUT_SIZE / h)
        nw, nh = int(w * scale), int(h * scale)

        pad_x, pad_y = (INPUT_SIZE - nw) // 2, (INPUT_SIZE - nh) // 2
        canvas = np.full((INPUT_SIZE, INPUT_SIZE, 3), 114, dtype=np.uint8)
        cv2.resize(frame, (nw, nh),
                   dst=canvas[pad_y:pad_y + nh, pad_x:pad_x + nw],
                   interpolation=cv2.INTER_LINEAR)

        rgb = cv2.cvtColor(canvas, cv2.COLOR_BGR2RGB)
        tensor = np.ascontiguousarray(rgb.transpose(2, 0, 1))
        return tensor, (pad_x, pad_y, scale, h, w)

    def _infer_chunk(self, tensors: list[np.ndarray]):
        """Один gRPC-вызов на кусок батча. Возвращает (num, boxes, scores, classes)."""
        import tritonclient.grpc as grpcclient

        batch = np.stack(tensors)
        inp = grpcclient.InferInput(
            "images", list(batch.shape), INPUT_DTYPE)
        inp.set_data_from_numpy(batch)
        requested = [grpcclient.InferRequestedOutput(n) for n in
                     ("num_detections", "boxes", "scores", "classes")]
        res = self._client.infer(
            model_name=self.model_name, inputs=[inp], outputs=requested)
        return (res.as_numpy("num_detections"), res.as_numpy("boxes"),
                res.as_numpy("scores"), res.as_numpy("classes"))

    def _to_detections(self, boxes, scores, classes, n: int, geom,
                       frame_index: int, timestamp: float,
                       conf: float) -> list[Detection]:
        """Боксы движка (xyxy, пиксели padded 640) -> пиксели кадра + Detection."""
        if n <= 0:
            return []
        pad_x, pad_y, scale, oh, ow = geom

        xyxy = boxes[:n].astype(np.float32)
        xyxy[:, [0, 2]] = (xyxy[:, [0, 2]] - pad_x) / scale
        xyxy[:, [1, 3]] = (xyxy[:, [1, 3]] - pad_y) / scale
        xyxy[:, [0, 2]] = np.clip(xyxy[:, [0, 2]], 0, ow)
        xyxy[:, [1, 3]] = np.clip(xyxy[:, [1, 3]], 0, oh)

        # Порог движка уже отсёк мусор; conf выше порога фильтруем здесь.
        keep = scores[:n] >= conf
        return [
            Detection(
                class_name=COCO_NAMES[int(c)],
                confidence=float(s),
                bbox=[float(v) for v in box],
                frame_index=frame_index,
                timestamp=timestamp,
            )
            for c, s, box in zip(classes[:n][keep], scores[:n][keep], xyxy[keep])
        ]
