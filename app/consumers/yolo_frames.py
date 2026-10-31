"""YOLO Consumer — читает кадры из Kafka, обрабатывает YOLO, отправляет детекции

Phase 4: Сохраняем ЧИСТЫЕ кадры со стримов в MinIO.
Стрим — ephemeral (прошёл и исчез), поэтому кадры нужно сохранить,
чтобы потом показать их с наложенными детекциями из БД.
Кадры сохраняются ТОЛЬКО если на них есть детекции —
нет смысла хранить пустые кадры.

ПРОПУСКНАЯ СПОСОБНОСТЬ (почему потребитель устроен именно так):
последовательный цикл poll → decode → YOLO(bs=1) → MinIO → produce ограничил
потолок ~20 fps: GPU простаивал, пока один поток делал imdecode и ждал
MinIO, а бенчмарк показывал 100% e2e-бюджета в ожидании raw_frames.
Теперь потребитель работает микро-батчами, и батч собирается ДИНАМИЧЕСКИ:
  1. собирает до YOLO_BATCH_SIZE сообщений с партиции: окно
     YOLO_BATCH_GRACE_MS ограничивает ожидание ПЕРВОГО сообщения, дальше
     батч расширяется, только пока очередь горячая (сообщения приходят из
     локального кэша librdkafka почти мгновенно). Под бэклогом батч
     набирается за миллисекунды, при низкой нагрузке одиночный кадр
     обрабатывается без ожидания несуществующих партнёров;
  2. декодит (JSON/base64/imdecode) в пуле потоков — параллельно;
  3. инференс БАТЧОМ (model(batch)) — GPU нагружается нормально;
  4. produce в detections + аплоад в MinIO (тот же пул, параллельно);
  5. offset коммитится РУЧНОЙ, только когда батч полностью обработан и
     flushed в detections — at-least-once, как и раньше, но крах теряет
     ни кадры, ни неотправленные детекции.
Порядок ВНУТРИ партиции сохраняется: батч берётся и обрабатывается в
порядке поступления. Вместе с key=stream_id у producer'а порядок кадров
стрима держится сквозняком.
"""
import base64
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

import cv2
import numpy as np
from confluent_kafka import Consumer, KafkaException, Producer

from app.core.config import settings
from app.core.cuda import is_fatal_cuda_error
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

# CUDA sticky error (CUDA_ERROR_UNKNOWN, illegal memory access, ECC):
# контекст мёртв, и КАЖДЫЙ последующий вызов будет падать — восстановление
# внутри процесса невозможно. Единственный правильный вариант — выйти с
# ненулевым кодом, чтобы супервизор (docker restart) поднял свежий процесс
# со свежим CUDA-контекстом.
MAX_CONSECUTIVE_FAILURES = 10

# Динамический батч: если poll вернул сообщение почти мгновенно, оно лежало
# в локальном кэше librdkafka — очередь горячая, продолжаем собирать батч.
# Если приходилось ЖДАТЬ (poll занял больше порога), сообщение «догнало» нас
# само — скорее всего, оно единственное, и тратить латентность на
# воображаемых партнёров по батчу бессмысленно.
HOT_POLL_SECONDS = 0.01


def _is_fatal_cuda_error(exc: BaseException) -> bool:
    """Sticky CUDA error: torch.AcceleratorError или сообщение 'CUDA error'.

    Проверяем по имени класса, а не через isinstance(torch.AcceleratorError),
    чтобы не тянуть torch в импорт модуля (он нужен только в рантайме YOLO).
    OOM ('CUDA out of memory') сюда НЕ попадает — он не липкий.
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
    """Читает raw_frames → YOLO (батчами) → detections + MinIO (параллельно)"""

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
        # Один пул на обе CPU/сетевые стадии: decode и MinIO-аплоуд никогда
        # не вкладываются друг в друга, поэтому deadlock невозможен.
        self.pool = ThreadPoolExecutor(
            max_workers=io_workers or settings.yolo_io_workers,
            thread_name_prefix="yolo-io",
        )

        self.consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "yolo-workers",
            "auto.offset.reset": "earliest",
            # Ручные коммиты: offset фиксируем только после полной обработки
            # батча и flush в detections (at-least-once, см. модуль).
            "enable.auto.commit": False,
            # confluent-kafka требует отключить авто-стор при ручном
            # store_offsets(): иначе он бросает _INVALID_ARG (offset уже
            # записан автоматически на poll).
            "enable.auto.offset.store": False,
            "fetch.max.bytes": 5 * 1024 * 1024,
        })
        self.consumer.subscribe([TOPIC_RAW_FRAMES])

        self.producer = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "message.max.bytes": 5 * 1024 * 1024,
        })

        # Счётчики зомби-режима: живут на объекте, run() их обнуляет при
        # старте (после рестарта процесса история падений не учитывается).
        self.poison_count = 0
        self.consecutive_failures = 0

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

    def _prepare(self, msg):
        """Разобрать и декодировать одно сообщение (вызывается в потоке пула).

        Ингресс и ожидание в очереди считаем ЗДЕСЬ: сообщение прибыло, даже
        если окажется ядовитым — rate(frames_received_total) vs
        rate(frame_processed_total) и есть «едим ли мы свой вход».
        """
        frames_received_total.inc()
        wait = queue_wait_seconds(msg)
        if wait is not None:
            frame_queue_wait_seconds.labels(topic=TOPIC_RAW_FRAMES).observe(wait)

        data = json.loads(msg.value().decode())

        # Декодируем кадр
        frame_bytes = base64.b64decode(data["frame_data"])
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
            # Время захвата кадра сквозняком: инференс и очередь не должны
            # сдвигать detected_at — writer поставит из него, не от now().
            "captured_at": data.get("captured_at"),
            "frame_bytes": frame_bytes,
        }
        return meta, frame

    def _mark_failure(self, exc: BaseException, topic: str, offset: int):
        """Единая обработка упавшего кадра: счётчики, poison, выход из зомби-режима."""
        self.poison_count += 1
        self.consecutive_failures += 1
        frame_processed_total.labels(result="error").inc()
        poison_messages_total.inc()
        if is_fatal_cuda_error(exc):
            # Липкая CUDA-ошибка: следующий кадр упадёт так же, и следующий,
            # и дальше вечно — «пропуск» тут только сжигает offset'ы. Выходим,
            # контейнер рестартует (restart-политика) с чистым CUDA-контекстом.
            logger.critical(
                f"CUDA context is dead (topic={topic} offset={offset}): {exc}. "
                "Sticky CUDA errors are unrecoverable in-process — "
                "exiting, the supervisor will restart the consumer.",
            )
            raise SystemExit(1)
        logger.exception(
            f"Poison message skipped: topic={topic} offset={offset} (total {self.poison_count})",
        )
        if self.consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
            # Не CUDA, но тоже зомби-режим (например, перманентный OOM):
            # N подряд падений — надёжнее рестартнуться, чем молча сжигать кадры.
            logger.critical(
                f"{self.consecutive_failures} consecutive failures — "
                "exiting for restart.",
            )
            raise SystemExit(1)

    def _collect_batch(self) -> dict[int, list]:
        """Собрать сообщения в батчи по партициям (ДИНАМИЧЕСКИЙ батч).

        Весь батч НЕ ждём: окно grace ограничивает ожидание ПЕРВОГО
        сообщения, дальнейшая сборка идёт, только пока очередь горячая
        (poll отдаёт сообщения из локального кэша почти мгновенно).
        Под бэклогом батч набирается до batch_max за миллисекунды; при
        низкой нагрузке одиночный кадр уходит без потерь латентности.
        Возвращает {partition: [сообщения]} в порядке поступления —
        порядок внутри партиции далее сохраняется.
        """
        parts: dict[int, list] = {}
        total = 0
        deadline = time.monotonic() + self.grace_ms / 1000.0
        while total < self.batch_max:
            remain = deadline - time.monotonic()
            if remain <= 0:
                break
            t0 = time.monotonic()
            msg = self.consumer.poll(remain)
            if msg is None:
                break
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue
            parts.setdefault(msg.partition(), []).append(msg)
            total += 1
            # Приходилось ждать сообщение — очередь остыла, батч таков,
            # каким является: дальше обрабатываем, а не ждём.
            if time.monotonic() - t0 >= HOT_POLL_SECONDS:
                break
        return parts

    def _process_partition(self, partition: int, msgs: list):
        """Обработать срез одной партиции: decode → батч-инференс → produce/MinIO → коммит."""
        failures = 0

        # 1) Декод параллельно (пул), порядок сохраняем
        pairs = []  # (msg, meta, frame)
        for msg, fut in ((m, self.pool.submit(self._prepare, m)) for m in msgs):
            try:
                meta, frame = fut.result()
                pairs.append((msg, meta, frame))
            except SystemExit:
                raise
            except Exception as exc:
                failures += 1
                self._mark_failure(exc, TOPIC_RAW_FRAMES, msg.offset())

        # 2) Инференс: батчом; если батч упал — деградация до по-кадрового
        results: list[list | None] = [None] * len(pairs)
        if pairs:
            started = time.perf_counter()
            try:
                batch_results = self.detector.detect_batch(
                    [_FrameRef(image=f, index=m["frame_index"]) for _, m, f in pairs]
                )
                for i, det in enumerate(batch_results):
                    results[i] = det
            except SystemExit:
                raise
            except Exception as exc:
                if is_fatal_cuda_error(exc):
                    logger.critical(
                        f"CUDA context is dead (partition={partition}): {exc}. "
                        "Exiting, the supervisor will restart the consumer.",
                    )
                    raise SystemExit(1)
                # Батч не прошёл целиком (сбой GPU и т.п.):
                # пробуем по одному — здоровые кадры не должны страдать
                # от чужих.
                logger.exception("Batch inference failed — degrading to per-frame")
                for i, (msg, meta, frame) in enumerate(pairs):
                    try:
                        results[i] = self.detector.detect(
                            frame, frame_index=meta["frame_index"]
                        )
                    except SystemExit:
                        raise
                    except Exception as exc2:
                        failures += 1
                        self._mark_failure(exc2, TOPIC_RAW_FRAMES, msg.offset())
            # Атрибуция на кадр: время батча делим равномерно — внутри
            # батча физически не разделить, кто сколько занял.
            per_frame = (time.perf_counter() - started) / len(pairs)
            for _ in pairs:
                frame_inference_duration_seconds.observe(per_frame)

        # 3) Produce в detections + аплоад в MinIO (пул, параллельно)
        upload_futs = []
        for (msg, meta, _frame), det in zip(pairs, results):
            if det is None:
                continue  # ядовитый кадр — уже учтён в _mark_failure
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
            # Сохраняем чистый кадр в MinIO (ТОЛЬКО если есть детекции)
            if meta["stream_id"] and objects:
                upload_futs.append(self.pool.submit(
                    self._save_frame_to_minio,
                    meta["stream_id"],
                    meta["frame_index"],
                    meta["frame_bytes"],
                ))
        # Flush ДО коммита: детекции долетели до брокера — тогда и offset
        # считается обработанным (at-least-once).
        self.producer.flush()
        for fut in upload_futs:
            fut.result()  # _save_frame_to_minio сам глотает исключения

        # 4) Ручной коммит: только после полного прохождения батча.
        # Ядовитые сообщения коммитятся тоже — иначе они бы зацикливались.
        # store_offsets(message=...) фиксирует offset+1 сообщения; внутри
        # партиции сообщения упорядочены, поэтому достаточно последнего.
        # KafkaException = партиция уехала к другому участнику прямо во
        # время обработки батча (rebalance): коммитит теперь её новый
        # владелец, а нам положены дубли — нормальная цена at-least-once.
        try:
            self.consumer.store_offsets(message=msgs[-1])
            self.consumer.commit(asynchronous=False)
        except KafkaException as exc:
            logger.warning(
                "Commit skipped (partition rebalanced?): partition=%d: %s",
                partition, exc,
            )

        if failures == 0:
            self.consecutive_failures = 0
        logger.debug(
            "Partition %d: %d кадров обработано, %d ядовитых",
            partition, len(pairs), failures,
        )

    def step(self):
        """Одна итерация цикла: сборка батчей + обработка по партициям.

        Вынесено из run() для тестов (run() — бесконечная обёртка).
        """
        parts = self._collect_batch()
        if not parts:
            return
        for partition in sorted(parts):
            self._process_partition(partition, parts[partition])

    def run(self):
        logger.info(
            "YOLO consumer listening to '%s'... (batch=%d, grace=%dms, io_threads=%d)",
            TOPIC_RAW_FRAMES, self.batch_max, self.grace_ms,
            self.pool._max_workers,
        )
        if MINIO_AVAILABLE:
            logger.info("[YOLO Consumer] Stream frames WILL be saved to MinIO")
        else:
            logger.info("[YOLO Consumer] Stream frames will NOT be saved (MinIO unavailable)")

        self.poison_count = 0
        self.consecutive_failures = 0

        try:
            while True:
                self.step()
        except KeyboardInterrupt:
            logger.info("Stopping...")
        finally:
            self.pool.shutdown(wait=True)
            self.consumer.close()
            self.producer.flush()


if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # До загрузки модели: /metrics должен отвечать, пока YOLO ещё грузится
    # в VRAM (путь к порту — METRICS_PORT, дефолт 9091, см. prometheus.yml).
    start_metrics_server(9091)

    # Веса: аргумент CLI > WEIGHTS_PATH (settings). Без аргумента — None,
    # чтобы YOLOConsumer взял путь из настроек; fallback "yolov8n.pt" здесь
    # захардкодил бы nano навсегда, как бы ни был озаглавлен compose-командой.
    model = sys.argv[1] if len(sys.argv) > 1 else None
    # Без явного аргумента устройство берётся из настроек:
    # DEVICE=auto → cuda при наличии GPU, иначе cpu (проект не падает без NVIDIA)
    device = sys.argv[2] if len(sys.argv) > 2 else None
    consumer = YOLOConsumer(model_path=model, device=device)
    consumer.run()
