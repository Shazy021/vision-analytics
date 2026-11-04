"""YOLO Consumer: читает кадры из Kafka, прогоняет через YOLO, отдаёт детекции.

Путь кадра:
    raw_frames (Kafka) → декод → YOLO (батчами) → detections (Kafka) + кадр в MinIO

Кадры (чистые, без рамок) сохраняем в MinIO только если на них есть детекции —
пустые кадры хранить незачем. Ключ: streams/<stream_id>/<frame_index>.jpg.
Стрим эфемерен (прошёл и исчез) — без сохранения показать потом нечего.

АРХИТЕКТУРА: конвейер из трёх ролей, чтобы CPU, GPU и сеть не ждали друг друга.

  main (поток run)      Забирает сообщения из Kafka и раздаёт на декод.
                        Сам ничего тяжёлого не делает.
  decode (пул, 4 потока) Превращает JPEG в массив пикселей. cv2 на время работы
                        отпускает GIL, поэтому потоки реально параллельны.
  gpu (1 поток)         Лепит кадры в батч, один вызов YOLO на батч, отправка
                        результатов, перенос границы обработки в Kafka.

Почему конвейер: наивный цикл poll → decode → YOLO → отправка дал только ~20 fps
— GPU ждал декод, декод ждал сеть. В конвейере все три стадии работают одновременно.

КАК МЫ ЗАПОМИНАЕМ, ЧТО ОБРАБОТАНО (это заменило старый «секвенсор»).
Kafka хранит на партицию одну границу: «обработано всё до offset N». Сказать
«1,2,4 готовы, а 3 ещё нет» — нельзя. Значит границу можно поднимать только до
самого старого НЕобработанного сообщения. Мы держим множество таких должников
(self._inflight): забрал кадр из Kafka — добавь в должники; полностью обработал
или признал битым — убери. Граница = min(должников) − 1.
Благодаря этому кадры можно обрабатывать в ЛЮБОМ порядке (в каком декод
закончил) — ничего не надо переупорядочивать, а граница всё равно честная.
Продюсер кладёт кадры в партиции по кругу (без key) — партиции равные,
ни один стрим не отстаёт от других.

ГАРАНТИЯ: at-least-once. Границу сдвигаем только после producer.flush() —
когда детекции точно долетели до брокера. При краше необработанное прилетит
заново: дубли дешевле потерь.

ОШИБКИ:
  • Битый кадр — пропускаем, считаем в poison_messages_total, границу не держит.
  • «Заразная» CUDA-ошибка (см. _die_fatal) — GPU-контекст мёртв, процесс
    убивает себя сам (os._exit), Docker поднимает новый с живым контекстом.
  • 10 подряд любых падений — тоже уходим: рестарт надёжнее вечного зомби.
"""
import json
import logging
import os
import queue
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

import cv2
import numpy as np
from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition

from app.core.config import settings
from app.core.cuda import is_fatal_cuda_error
from app.core.frame_envelope import unpack
from app.core.metrics import (
    frame_decode_duration_seconds,
    frame_inference_duration_seconds,
    frame_processed_total,
    frame_queue_wait_seconds,
    frame_save_duration_seconds,
    frames_received_total,
    poison_messages_total,
    queue_wait_seconds,
    start_metrics_server,
)
from app.core.topics import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_DETECTIONS,
    TOPIC_RAW_FRAMES,
)

logger = logging.getLogger(__name__)

# Сколько падений подряд терпим, прежде чем уйти на рестарт.
# Одно-два — случайность (битый JPEG), десять подряд — уже болезнь процесса.
MAX_CONSECUTIVE_FAILURES = 10


def _is_fatal_cuda_error(exc: BaseException) -> bool:
    """«Заразная» (sticky) CUDA-ошибка: после неё контекст GPU мёртв.

    Отличается от обычной тем, что каждый СЛЕДУЮЩИЙ вызов падает тоже —
    починиться изнутри процесса нельзя. OOM ('CUDA out of memory') сюда НЕ
    попадает: это не заразно, следующий вызов может пройти.

    Проверяем по имени класса, а не isinstance(torch.AcceleratorError),
    чтобы не тянуть torch в импорт модуля (он нужен только в рантайме YOLO).
    """
    if type(exc).__name__ == "AcceleratorError":
        return True
    text = str(exc)
    return "CUDA error" in text or "cuCtxSynchronize" in text

# Импортируем storage один раз при загрузке модуля.
# Если MinIO недоступен — упадёт сразу с понятной ошибкой,
# а не будет молча глотать ошибки на каждом кадре.
try:
    from app.core import storage
    MINIO_AVAILABLE = True
    logger.info("[YOLO Consumer] MinIO storage connected")
except Exception as e:
    storage = None
    MINIO_AVAILABLE = False
    logger.error(f"[YOLO Consumer] WARNING: MinIO not available: {e}")
    logger.error("[YOLO Consumer] Stream frames will NOT be saved!")


@dataclass
class _FrameRef:
    """Адаптер под YOLODetector.detect_batch (ждёт атрибуты .image/.index/.timestamp)."""

    image: np.ndarray
    index: int
    timestamp: float = 0.0


class YOLOConsumer:
    """Конвейер: Kafka → параллельный декод → батч в порядке готовности → YOLO → detections + MinIO"""

    def __init__(
        self,
        model_path: str | None = None,
        device: str | None = None,
        *,
        batch_max: int | None = None,
        grace_ms: int | None = None,
        io_workers: int | None = None,
    ):
        # Ленивый импорт: YOLODetector тянет ultralytics/torch, а модуль
        # импортируется и там, где инференс не нужен (тесты, линт).
        # Тот же принцип, что с проверкой AcceleratorError по имени класса.
        from app.processing.detector import YOLODetector
        # Одна точка входа в инференс на всю платформу: видео-трасса
        # (processor.py) и стримовая используют один и тот же YOLODetector —
        # пороги и разбор боксов не разъедутся.
        self.detector = YOLODetector(weights_path=model_path, device=device)

        # Параметры батчинга: аргументы (тесты) > settings (env) > дефолты.
        self.batch_max = batch_max or settings.yolo_batch_size
        self.grace_ms = grace_ms if grace_ms is not None else settings.yolo_batch_grace_ms
        workers = io_workers or settings.yolo_io_workers
        # ДВА разных пула — это принципиально, а не перестраховка.
        # GPU-поток ждёт аплоады из io_pool, а decode-воркеры могут зависнуть
        # на put() в полную decoded-очередь, которую опустошает тот же
        # GPU-поток. Один пул на всё = каждый ждёт каждого = deadlock.
        self.pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="yolo-decode")
        self.io_pool = ThreadPoolExecutor(
            max_workers=max(2, workers // 2), thread_name_prefix="yolo-upload"
        )

        self.consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "yolo-workers",
            "auto.offset.reset": "earliest",
            # Границу «обработано до N» двигаем ТОЛЬКО сами, после полной
            # обработки батча и flush (at-least-once, см. _commit_watermarks).
            "enable.auto.commit": False,
            # Техническое требование confluent-kafka: при ручном store_offsets
            # автозапись offset'а на poll тоже нужно выключить, иначе библиотека
            # бросает _INVALID_ARG.
            "enable.auto.offset.store": False,
            "fetch.max.bytes": 5 * 1024 * 1024,
        })
        self.consumer.subscribe([TOPIC_RAW_FRAMES])

        self.producer = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "message.max.bytes": 5 * 1024 * 1024,
        })

        # --- Оборудование конвейера --------------------------------------
        # Сколько кадров может быть «в работе» одновременно (забрали из Kafka,
        # ещё не обработали до конца). 8×batch — это крышка памяти: декоднутый
        # 1080p-кадр весит ~6 МБ, без лимита они копились в очередях консьюмера
        # минутами (замер: e2e p95 24с при ПУСТОМ Kafka). Работает цепочкой:
        # decoded полна → _emit ждёт → жетоны не возвращаются → poll встал.
        # Лишнее остаётся в Kafka: там бэклог дешёвый (диск брокера) и
        # заметный (метрика queue_wait, lag).
        self.inflight_cap = self.batch_max * 8
        # Жетоны: перед тем взять кадр из Kafka, поток обязан занять жетон,
        # вернуть — после полной обработки. Все заняты → poll ждёт.
        self._slots = threading.BoundedSemaphore(self.inflight_cap)
        # Труба между декодерами и GPU-потоком: (msg, meta, frame).
        self.decoded: queue.Queue = queue.Queue(maxsize=self.inflight_cap)
        # Список должников для границы обработки: партиция → {offsets}.
        # Здесь лежат offset'ы, забранные из Kafka, но ещё НЕ обработанные
        # до конца. Границу «обработано до N» нельзя поднимать выше самого
        # старого должника. _last_seen — самый дальний забранный offset
        # (когда должников нет, граница = он).
        self._inflight: dict[int, set[int]] = defaultdict(set)
        self._last_seen: dict[int, int] = {}
        # В эти словари и счётчики пишут три вида потоков (main, decode, gpu)
        # — все изменения только под этим замком.
        self._state_lock = threading.Lock()
        self.poison_count = 0
        self.consecutive_failures = 0
        self._closing = False

    # ------------------------------------------------------------------
    # Список должников (граница «обработано до N»)
    # ------------------------------------------------------------------

    def _enter_inflight(self, msg) -> None:
        """Забрали кадр из Kafka → он должник: граница обязана остаться ниже."""
        p = msg.partition()
        with self._state_lock:
            self._inflight[p].add(msg.offset())
            if msg.offset() > self._last_seen.get(p, -1):
                self._last_seen[p] = msg.offset()

    def _leave_inflight(self, msg) -> None:
        """Кадр закрыт (обработан или признан битым) → убираем из должников."""
        with self._state_lock:
            self._inflight[msg.partition()].discard(msg.offset())

    # ------------------------------------------------------------------
    # Стадия 1: main-поток — забор кадров из Kafka
    # ------------------------------------------------------------------

    def _emit(self, item: tuple) -> None:
        """Положить кадр в decoded; очередь полна — ЖДАТЬ, не выбрасывать.

        Ждать безопасно: общее число кадров уже ограничено жетонами, лишнего
        в памяти не будет. Выброс — только при остановке (_closing): offset
        этого кадра не закоммичен, Kafka отдаст его после рестарта.
        """
        while True:
            try:
                self.decoded.put(item, timeout=1)
                return
            except queue.Full:
                if self._closing:
                    logger.warning(
                        "Decoded queue full during shutdown — frame dropped, will be redelivered"
                    )
                    return

    def _decode_submit(self, msg):
        """Задача decode-воркера: распаковать, декодировать, положить в decoded.

        Если кадр битый — он НЕ идёт в GPU, но обязательно убирается из
        должников: иначе его offset вечно держал бы границу обработки, и
        Kafka заново слала бы этот кадр по кругу.
        """
        try:
            try:
                meta, frame = self._prepare(msg)
            except Exception as exc:
                self._mark_failure(exc, TOPIC_RAW_FRAMES, msg.offset())
                self._leave_inflight(msg)
                return
            self._emit((msg, meta, frame))
        finally:
            self._slots.release()  # жетон возвращается при ЛЮБОМ исходе

    # ------------------------------------------------------------------
    # Стадия 2: декод одного кадра (вызывается в потоке пула)
    # ------------------------------------------------------------------

    def _prepare(self, msg):
        """Разобрать конверт и декодировать кадр (в потоке пула).

        Счётчик «кадр долетел до нас» стоит ЗДЕСЬ, до проверки на битость:
        сравнение rate(frames_received_total) с rate(frame_processed_total)
        показывает, едим ли мы свой вход или отстаём.
        """
        frames_received_total.inc()
        wait = queue_wait_seconds(msg)
        if wait is not None:
            frame_queue_wait_seconds.labels(topic=TOPIC_RAW_FRAMES).observe(wait)

        data, frame_bytes = unpack(msg.value())

        # Декодируем кадр
        started = time.perf_counter()
        frame = cv2.imdecode(
            np.frombuffer(frame_bytes, dtype=np.uint8),
            cv2.IMREAD_COLOR,
        )
        frame_decode_duration_seconds.observe(time.perf_counter() - started)
        if frame is None:
            raise ValueError("кадр не декодировался (битый JPEG?)")

        meta = {
            "frame_index": data["frame_index"],
            "stream_id": data.get("stream_id"),
            "source": data["source"],
            # Время ЗАХВАТА кадра тащим через весь конвейер: если консьюмер
            # отставал на минуту, детекция должна датироваться минутой назад,
            # а не «сейчас» — writer возьмёт его отсюда, не из now().
            "captured_at": data.get("captured_at"),
            # Сырой JPEG пригодится для MinIO — сохраняем без перекодирования.
            "frame_bytes": frame_bytes,
        }
        return meta, frame

    # ------------------------------------------------------------------
    # Стадия 3: GPU-поток — батч, YOLO, отправка, сдвиг границы
    # ------------------------------------------------------------------

    def _collect_decoded(self):
        """Собрать батч из декоднутых кадров — в том порядке, в каком декод закончил.

        Две фазы, чтобы не ждать впустую:
          фаза 1 (батч пуст): спим до grace_ms в ожидании ПЕРВОГО кадра.
            При низкой нагрузке кадр уходит сразу, не дожидаясь соседей.
          фаза 2 (батч не пуст): дреним очередь не засыпая, пока в ней есть
            кадры, но не больше batch_max. Под нагрузкой батч набирается
            за миллисекунды.
        Возврат: список кадров | [] — ждать нечего | None — сигнал остановки.
        """
        deadline = time.monotonic() + self.grace_ms / 1000.0
        batch: list = []
        while len(batch) < self.batch_max:
            if not batch:
                remain = deadline - time.monotonic()
                if remain <= 0:
                    return []
                try:
                    item = self.decoded.get(timeout=remain)
                except queue.Empty:
                    return []
            else:
                try:
                    item = self.decoded.get_nowait()
                except queue.Empty:
                    break
            if item is None:
                # None — не кадр, а «сигнал остановки» от run(): закругляемся.
                # Собранный сбрасываем: offset'ы не закоммичены → Kafka отдаст.
                self._closing = True
                if batch:
                    logger.warning(
                        "Sentinel при сборке батча — %d кадров сброшено, будут доставлены заново",
                        len(batch),
                    )
                return None
            batch.append(item)
        return batch

    def _commit_watermarks(self) -> None:
        """Сообщить Kafka границу «обработано до N» по каждой партиции.

        Граница = min(должников) − 1, а если должников нет — последний
        забранный offset. Ниже неё нет ни одного необработанного сообщения,
        поэтому при краше Kafka отдаст заново только то, что выше границы:
        дубли, но не потери.

        KafkaException означает: партиция уехала к другому консьюмеру прямо
        во время обработки (rebalance) — теперь её коммитит новый владелец,
        а нам достанутся дубли. Это не авария, а нормальная цена гарантии.
        """
        with self._state_lock:
            offsets = []
            for p in sorted(self._last_seen):
                inflight = self._inflight.get(p)
                watermark = (min(inflight) - 1) if inflight else self._last_seen[p]
                if watermark >= 0:
                    # +1: брокер хранит не «обработано до», а «следующий нужный»
                    offsets.append(TopicPartition(TOPIC_RAW_FRAMES, p, watermark + 1))
        if not offsets:
            return
        try:
            self.consumer.store_offsets(offsets=offsets)
            self.consumer.commit(asynchronous=False)
        except KafkaException as exc:
            logger.warning("Commit skipped (partition rebalanced?): %s", exc)

    def _gpu_thread(self) -> None:
        while True:
            batch = self._collect_decoded()
            if batch is None:  # сигнал остановки
                return
            if batch:
                self._infer_and_publish(batch)

    def _infer_and_publish(self, pairs: list) -> None:
        """Один батч от начала до конца: YOLO → отправка → подтверждение → граница.

        pairs — [(msg, meta, frame)] в порядке готовности декода.
        """
        failures = 0

        # 1) YOLO: один вызов на весь батч. Упал — пробуем по одному.
        results: list[list | None] = [None] * len(pairs)
        started = time.perf_counter()
        try:
            batch_results = self.detector.detect_batch(
                [_FrameRef(image=f, index=m["frame_index"]) for _, m, f in pairs]
            )
            for i, det in enumerate(batch_results):
                results[i] = det
        except Exception as exc:
            if is_fatal_cuda_error(exc):
                self._die_fatal(exc, f"batch of {len(pairs)} frames")
            # Батч упал целиком (не CUDA) — возможно, виноват один кадр.
            # Даём каждому шанс поодиночке: здоровые не должны страдать от чужих.
            logger.exception("Batch inference failed — degrading to per-frame")
            for i, (msg, meta, frame) in enumerate(pairs):
                try:
                    results[i] = self.detector.detect(frame, frame_index=meta["frame_index"])
                except Exception as exc2:
                    failures += 1
                    self._mark_failure(exc2, TOPIC_RAW_FRAMES, msg.offset())
        # Метрика «время инференса» на кадр: делим время батча поровну —
        # измерить, кто из кадров занял больше, физически невозможно.
        per_frame = (time.perf_counter() - started) / len(pairs)
        for _ in pairs:
            frame_inference_duration_seconds.observe(per_frame)

        # 2) Отправка: детекции в Kafka, кадры в MinIO (аплоад — параллельно)
        upload_futs = []
        for (msg, meta, _frame), det in zip(pairs, results):
            if det is None:
                continue  # кадр умер в деградации — уже учтён в _mark_failure
            objects = [
                {
                    "class": d.class_name,
                    "confidence": d.confidence,
                    "bbox": {"x1": d.bbox[0], "y1": d.bbox[1], "x2": d.bbox[2], "y2": d.bbox[3]},
                }
                for d in det
            ]
            detection_msg = json.dumps({
                "video_id": None,
                "stream_id": meta["stream_id"],
                "frame_index": meta["frame_index"],
                "source": meta["source"],
                "captured_at": meta["captured_at"],
                "objects": objects,
                "num_detections": len(objects),
            }).encode()
            self.producer.produce(TOPIC_DETECTIONS, value=detection_msg)
            frame_processed_total.labels(result="ok").inc()
            # Чистый кадр в MinIO — ТОЛЬКО если на нём есть детекции
            if meta["stream_id"] and objects:
                upload_futs.append(self.io_pool.submit(
                    self._save_frame_to_minio,
                    meta["stream_id"],
                    meta["frame_index"],
                    meta["frame_bytes"],
                ))
        # flush() ждёт, пока detекции реально примут в брокере. Именно с этого
        # момента кадр считается обработанным — поэтому flush обязан быть ДО
        # сдвига границы, иначе при краше мы потеряли бы кадры (at-least-once).
        self.producer.flush()
        for fut in upload_futs:
            fut.result()  # ждём аплоады; _save_frame_to_minio сам глотает ошибки

        # 3) Кадров из батча больше нет среди должников → сдвигаем границу.
        # Она накроет и битые кадры (их убрали из должников ещё в
        # _decode_submit), и кадры, упавшие в деградации: их offset тоже
        # «обработан», второй раз он не нужен.
        partitions = sorted({msg.partition() for msg, _meta, _frame in pairs})
        for msg, _meta, _frame in pairs:
            self._leave_inflight(msg)
        self._commit_watermarks()

        with self._state_lock:
            if failures == 0:
                self.consecutive_failures = 0
        logger.debug(
            "Batch: %d кадров обработано, %d ядовитых (партиции %s)",
            len(pairs), failures, partitions,
        )

    # ------------------------------------------------------------------
    # Общие: MinIO, сбои, жизненный цикл
    # ------------------------------------------------------------------

    def _save_frame_to_minio(self, stream_id: int, frame_index: int, frame_data: bytes):
        """Сохранить чистый кадр (JPEG) в MinIO. Ключ: streams/{stream_id}/{frame_index}.jpg"""
        if not MINIO_AVAILABLE:
            return
        try:
            frame_key = f"streams/{stream_id}/{frame_index}.jpg"
            started = time.perf_counter()
            storage.upload_bytes(frame_key, frame_data, content_type="image/jpeg")
            frame_save_duration_seconds.observe(time.perf_counter() - started)
            return True
        except Exception:
            # Логируем но не падаем — кадры не критичны
            logger.exception("[YOLO Consumer] Failed to save frame to MinIO")
            # MinIO мог отвалиться — пробуем реконнект при следующем кадре
            return False

    def _die_fatal(self, exc: BaseException, ctx: str) -> None:
        """Заразная CUDA-ошибка: процесс обязан убить себя сам.

        Починиться изнутри нельзя (каждый следующий вызов CUDA падает),
        поэтому os._exit, а не raise: мы можем сидеть в worker-потоке, где
        исключение убило бы только поток, а процесс-зомби продолжал бы
        жрать кадры мёртвым GPU. Код 1 → Docker перезапускает контейнер
        с чистого листа.
        """
        logger.critical(
            f"CUDA context is dead ({ctx}): {exc}. "
            "Sticky CUDA errors are unrecoverable in-process — exiting, "
            "the supervisor will restart the consumer.",
        )
        os._exit(1)

    def _mark_failure(self, exc: BaseException, topic: str, offset: int) -> None:
        """Единая обработка «кадр не получился»: счётчики, лог, решение о смерти.

        Вызывается из decode-воркеров и GPU-потока — счётчики под замком.
        """
        with self._state_lock:
            self.poison_count += 1
            self.consecutive_failures += 1
        frame_processed_total.labels(result="error").inc()
        poison_messages_total.inc()
        if is_fatal_cuda_error(exc):
            self._die_fatal(exc, f"topic={topic} offset={offset}")
        logger.exception(
            f"Poison message skipped: topic={topic} offset={offset} (total {self.poison_count})",
        )
        with self._state_lock:
            giving_up = self.consecutive_failures >= MAX_CONSECUTIVE_FAILURES
        if giving_up:
            # Не CUDA, но 10 падений подряд — тоже болезнь (перманентный OOM
            # и т.п.). Рестарт надёжнее, чем молча сжигать каждый кадр.
            logger.critical(
                f"{self.consecutive_failures} consecutive failures — exiting for restart.",
            )
            os._exit(1)

    def run(self):
        logger.info(
            "YOLO consumer listening to '%s'... (batch=%d, grace=%dms, decode_threads=%d, inflight<=%d)",
            TOPIC_RAW_FRAMES, self.batch_max, self.grace_ms,
            self.pool._max_workers, self.inflight_cap,
        )
        if MINIO_AVAILABLE:
            logger.info("[YOLO Consumer] Stream frames WILL be saved to MinIO")
        else:
            logger.info("[YOLO Consumer] Stream frames will NOT be saved (MinIO unavailable)")

        with self._state_lock:
            self.poison_count = 0
            self.consecutive_failures = 0
        self._closing = False

        gpu = threading.Thread(target=self._gpu_thread, name="yolo-gpu", daemon=True)
        gpu.start()
        try:
            while not self._closing:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    continue
                # Нет свободного жетона (конвейер полон) — стоим здесь, а не
                # тащим кадры в никуда: лишние 6-мегабайтные массивы в RAM.
                self._slots.acquire()
                self._enter_inflight(msg)  # с этого момента кадр — должник
                self.pool.submit(self._decode_submit, msg)
        except KeyboardInterrupt:
            logger.info("Stopping...")
        finally:
            # Остановка: даём GPU-потоку доработать и уйти, дожимаем очереди.
            self._closing = True
            try:
                # Кладём в трубу None — «сигнал остановки». GPU-поток спит в
                # get(timeout=...) и проснётся, только когда что-то прилетит.
                # timeout=5: если GPU-поток уже мёртв, вешаться нельзя.
                self.decoded.put(None, timeout=5)
            except queue.Full:
                pass
            gpu.join(timeout=5)
            self.pool.shutdown(wait=True)   # долить все декоды
            self.io_pool.shutdown(wait=True)  # долить все аплоады
            self.consumer.close()           # выйти из группы — ребаланс без нас
            self.producer.flush()           # долить detections в брокер


if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # Порт метрик поднимаем ПЕРВЫМ: /metrics должен отвечать, пока YOLO ещё
    # грузится в VRAM (иначе Prometheus считает консьюмер мёртвым на старте).
    start_metrics_server(9091)

    # Веса модели: аргумент CLI > WEIGHTS_PATH из настроек. Ничего не
    # подставляем здесь — захардкоженный "yolov8n.pt" сделал бы модель
    # nano-размера вечной, как бы её ни настраивали через env.
    model = sys.argv[1] if len(sys.argv) > 1 else None
    # Устройство: аргумент CLI > DEVICE (auto → cuda при наличии GPU,
    # иначе cpu — проект работает и без NVIDIA)
    device = sys.argv[2] if len(sys.argv) > 2 else None
    consumer = YOLOConsumer(model_path=model, device=device)
    consumer.run()
