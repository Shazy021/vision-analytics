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

Теперь это конвейер из трёх ролей, чтобы стадии перекрывали друг друга:
  main    poll Kafka → присваивает глобальный seq → пул декода.
          BoundedSemaphore (4×batch) — backpressure: при полном конвейере
          poll ПАУЗИРУЕТСЯ, а не кормит безлимитную очередь (декоднутый
          1080p-кадр весит ~6 МБ, память нельзя оставлять без крыши).
  decode  пул потоков: unpack + imdecode (cv2 отпускает GIL — параллельно
          по ядрам). Результат — в ограниченную очередь decoded.
  gpu     SEQUENCER + инференс: держит per-partition карту pending и
          выпускает только НЕНАПРЕРЫВНЫЙ префикс (seq == next_expected) —
          порядок внутри партиции (а значит и стрима) сохраняется, хоть
          декод завершился в любом порядке. Затем ОДИН detect_batch на
          весь выпуск (кросс-стримовый батч: key=stream_id приковал стрим
          к партиции, а батч не режется по партициям), produce, ОДИН
          flush, MinIO (отдельный пул — не пересекается с декодом,
          иначе deadlock: GPU ждёт future, воркер ждёт свободное место
          в decoded), ОДИН коммит по последней message каждой партиции.

Produce в GPU-потоке последовательный — и это бесплатно: librdkafka сама
держит delivery-поток, produce() — неблокирующая постановка в её очередь;
единственный блокирующий flush() должен завершиться до коммита, так что
параллелить его не с чем.

Батч собирается ДИНАМИЧЕСКИ, как и раньше: окно YOLO_BATCH_GRACE_MS
ограничивает ожидание ПЕРВОГО кадра, дальше батч расширяется, только пока
в decoded есть накопленное. Под бэклогом набирается за миллисекунды, при
низкой нагрузке одиночный кадр уходит без ожидания несуществующих
партнёров (и без секундных простоев — grace, а не фиксированный таймаут).

at-least-once: offset коммитится РУЧНОЙ, только когда батч полностью
обработан и flushed в detections; коммит — непрерывный префикс каждой
партиции, поэтому краш не даст ни потери, ни «дыр» в offset'ах.
Липкая CUDA-ошибка в любом потоке — os._exit(1): SystemExit в worker-
потоке умер бы молча, а процесс обязан умереть громко, чтобы супервизор
поднял его со свежим CUDA-контекстом.
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
from confluent_kafka import Consumer, KafkaException, Producer

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

# CUDA sticky error (CUDA_ERROR_UNKNOWN, illegal memory access, ECC):
# контекст мёртв, и КАЖДЫЙ последующий вызов будет падать — восстановление
# внутри процесса невозможно. Единственный правильный вариант — уйти из
# процесса с ненулевым кодом, чтобы супервизор (docker restart) поднял
# свежий процесс со свежим CUDA-контекстом.
MAX_CONSECUTIVE_FAILURES = 10


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
    """Конвейер: Kafka → параллельный декод → sequenced кросс-стримовый батч → YOLO → detections + MinIO"""

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
        # ДВА пула, и это принципиально: decode-пул и upload-пул не могут
        # быть одним. GPU-поток ждёт futures аплоуда, а decode-воркеры могут
        # заблокироваться на put() в полную decoded-очередь, которую дренирует
        # тот же GPU-поток — общий пул = взаимное ожидание = deadlock.
        self.pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="yolo-decode")
        self.io_pool = ThreadPoolExecutor(
            max_workers=max(2, workers // 2), thread_name_prefix="yolo-upload"
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

        # --- Plumbing конвейера -----------------------------------------
        # Потолок кадров «в полёте» (поданных на декод, ещё не выпущенных
        # в инференс). 8×batch принципиален: pending-потолок обязан быть не
        # меньше n_partitions × batch_max (6 партиций × 16 = 96) — иначе при
        # дырке в одной партиции pending забивается невыпускаемыми кадрами,
        # release недобирает батч, GPU голодает на неполных батчах
        # (замерено: 4×batch дал 99 fps против 110 без потолка).
        # Плюс запас 2 батча; не даёт скопить сотни декоднутых 1080p-кадров
        # (~6 МБ каждый) в памяти.
        self.inflight_cap = self.batch_max * 8
        self._slots = threading.BoundedSemaphore(self.inflight_cap)
        self.decoded: queue.Queue = queue.Queue(maxsize=self.inflight_cap)
        # PER-PARTITION счётчики seq (main-поток — единственный писатель):
        # sequencer выпускает непрерывный префикс СВОЕЙ партиции, поэтому
        # нумерация начинается с нуля на каждую партицию.
        self._part_seq: dict[int, int] = defaultdict(int)
        # Счётчики зомби-режима пишут и decode-воркеры, и GPU-поток.
        self._state_lock = threading.Lock()
        self.poison_count = 0
        self.consecutive_failures = 0
        # Sequencer: partition -> {seq: (msg, meta, frame)}, и следующий
        # ожидаемый seq на партицию. Выпускается только непрерывный префикс.
        self._pending: dict[int, dict[int, tuple]] = defaultdict(dict)
        self._next_seq: dict[int, int] = defaultdict(int)
        self._closing = False

    # ------------------------------------------------------------------
    # Стадия 1: main-поток — poll и подача на декод
    # ------------------------------------------------------------------

    def _emit(self, item: tuple) -> None:
        """Положить кадр в decoded — при полной очереди ЖДАТЬ, не дропать.

        Потеря seq фатальна: sequencer ждёт НЕПРЕРЫВНЫЙ префикс партиции,
        пропущенный seq навсегда стопорит и выпуск, и коммит (lag растёт,
        пока процесс не перезапущен — это ловили на догоне бэклога после
        рестарта). Backpressure — законное состояние конвейера: слоты
        (BoundedSemaphore) уже ограничивают inflight, ждать очереди
        безопасно. Drop — только при завершении (_closing): offset кадра
        не закоммичен, брокер отдаст его после рестарта (at-least-once).
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

    def _decode_submit(self, seq: int, msg):
        """Worker-задача: распаковать и декодировать, положить в decoded.

        Любой сбой — tombstone (meta=None): sequencer обязан пройти мимо
        этого seq, иначе партиция «застрянет» на нём навсегда.
        """
        try:
            meta, frame = self._prepare(msg)
            self._emit((seq, msg, meta, frame))
        except Exception as exc:
            self._mark_failure(exc, TOPIC_RAW_FRAMES, msg.offset())
            self._emit((seq, msg, None, None))
        finally:
            self._slots.release()

    # ------------------------------------------------------------------
    # Стадия 2: декод одного сообщения (вызывается в потоке пула)
    # ------------------------------------------------------------------

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
            # Время захвата кадра сквозняком: инференс и очередь не должны
            # сдвигать detected_at — writer поставит из него, не от now().
            "captured_at": data.get("captured_at"),
            "frame_bytes": frame_bytes,
        }
        return meta, frame

    # ------------------------------------------------------------------
    # Стадия 3: GPU-поток — sequencer, батч, инференс, publish, коммит
    # ------------------------------------------------------------------

    def _accept(self, seq: int, msg, meta, frame) -> None:
        """Положить декоднутый кадр (или tombstone) в pending его партиции."""
        self._pending[msg.partition()][seq] = (msg, meta, frame)

    def _prefix_len(self, p: int) -> int:
        """Длина непрерывного префикса, готового к выпуску, у партиции p."""
        pend = self._pending[p]
        nxt = self._next_seq[p]
        n = 0
        while nxt + n in pend:
            n += 1
        return n

    def _release_ready(self) -> list:
        """Выбрать непрерывный префикс каждой партиции, не больше batch_max.

        Кадры с seq БОЛЬШИМ, чем ближайший недостающий, остаются ждать —
        пусть декод финишировал раньше, публиковать их раньше младших
        нельзя: коммит должен оставаться непрерывным префиксом.

        Потолок batch_max принципиален: без него выпуск дорастает до
        inflight_cap (4×batch) — один detect_batch на десятки 1080p-кадров
        это гигабайтные пики RAM (float32-тензоры препроцессинга), которые
        аллокатор torch не отдаёт ОС.

        Квота делится между партициями ПРОПОРЦИОНАЛЬНО длине их префиксов
        (largest remainder), а не поровну: партиции получают 2–5 стримов
        (хеш key=stream_id), и равная дрень при разной подаче тонет
        неравномерно — жирная партиция копит латентность быстрее тощей.
        Пропорциональная дрень даёт одинаковый темп роста лага всем,
        «равномерно тонем» вместо «кто в какой партиции родилось».
        """
        prefixes = {p: n for p in sorted(self._pending) if (n := self._prefix_len(p)) > 0}
        if not prefixes:
            return []
        total = sum(prefixes.values())
        quota = {p: self.batch_max * n // total for p, n in prefixes.items()}
        # Дробостатки: кому большая остаточная доля — тот получает лишний кадр
        for p in sorted(prefixes, key=lambda p: -(self.batch_max * prefixes[p] % total))[: self.batch_max - sum(quota.values())]:
            quota[p] += 1
        # При total < batch_max пропорция может превысить сам префикс —
        # больше, чем есть непрерывного, партиция отдать не может.
        quota = {p: min(q, prefixes[p]) for p, q in quota.items()}

        batch: list = []
        for p in sorted(prefixes):
            pend = self._pending[p]
            nxt = self._next_seq[p]
            for _ in range(quota[p]):
                msg, meta, frame = pend.pop(nxt)
                nxt += 1
                if meta is not None:
                    batch.append((msg, meta, frame))
            self._next_seq[p] = nxt
        return batch

    def _pending_count(self) -> int:
        return sum(len(p) for p in self._pending.values())

    def _absorb_buffered(self) -> bool:
        """Переложить decoded в pending — пока pending не заполнится.
        False — получен sentinel.

        Потолок inflight_cap обязателен: release отдаёт не больше batch_max
        за цикл, а absorb без потолка забирал весь decoded — под перегрузкой
        pending рос бесконечно, кадры (msg + ~6 МБ каждый) сидели в нём
        минутами: e2e p95 24с при ПУСТОМ Kafka (queue_wait того не видит —
        бэклог прятался в приватных словарях консьюмера). С потолком
        цепочка замыкается: полный pending → полный decoded → блокируется
        _emit → слоты держатся → poll встаёт. Бэклог живёт в Kafka, где он
        честный (виден в queue_wait) и дешёвый (диск брокера, не RAM).
        """
        while self._pending_count() < self.inflight_cap:
            try:
                item = self.decoded.get_nowait()
            except queue.Empty:
                return True
            if item is None:
                self._closing = True
                return False
            self._accept(*item)
        return True

    def _collect_decoded(self):
        """Собрать батч из декоднутых кадров (динамика как у старого сбора).

        Окно grace ограничивает ожидание ПЕРВОГО кадра; дальше батч
        расширяется, только пока в decoded есть накопленное (горячая
        очередь). None — sentinel остановки, [] — окно вышло пустым.
        """
        deadline = time.monotonic() + self.grace_ms / 1000.0
        while True:
            if not self._absorb_buffered():
                return None
            batch = self._release_ready()
            if batch:
                return batch
            remain = deadline - time.monotonic()
            if remain <= 0:
                return []
            if self._pending_count() >= self.inflight_cap:
                # Места нет, а выпускать нечего (непрерывный префикс пуст) —
                # подождать, а не крутить busy-loop до дедлайна.
                time.sleep(min(remain, 0.05))
                continue
            try:
                item = self.decoded.get(timeout=remain)
            except queue.Empty:
                return []
            if item is None:
                return None
            self._accept(*item)

    def _gpu_thread(self) -> None:
        while True:
            batch = self._collect_decoded()
            if batch is None:
                return
            if batch:
                self._infer_and_publish(batch)

    def _infer_and_publish(self, pairs: list) -> None:
        """Один батч: инференс → produce/MinIO → flush → коммит.

        pairs — [(msg, meta, frame)] в выпуске sequencer'а: порядок внутри
        партиции сохранён, партиции сортированы по номеру.
        """
        failures = 0

        # 1) Инференс: одним батчом; если упал — деградация до по-кадрового
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
            # Батч не прошёл целиком (сбой GPU и т.п.):
            # пробуем по одному — здоровые кадры не должны страдать от чужих.
            logger.exception("Batch inference failed — degrading to per-frame")
            for i, (msg, meta, frame) in enumerate(pairs):
                try:
                    results[i] = self.detector.detect(frame, frame_index=meta["frame_index"])
                except Exception as exc2:
                    failures += 1
                    self._mark_failure(exc2, TOPIC_RAW_FRAMES, msg.offset())
        # Атрибуция на кадр: время батча делим равномерно — внутри батча
        # физически не разделить, кто сколько занял.
        per_frame = (time.perf_counter() - started) / len(pairs)
        for _ in pairs:
            frame_inference_duration_seconds.observe(per_frame)

        # 2) Produce в detections + аплоад в MinIO (upload-пул, параллельно)
        upload_futs = []
        for (msg, meta, _frame), det in zip(pairs, results):
            if det is None:
                continue  # кадр, упавший в деградации — уже учтён в _mark_failure
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
                upload_futs.append(self.io_pool.submit(
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

        # 3) Ручной коммит: ОДИН на батч, по последней message каждой
        # затронутой партиции. Ядовитые сообщения покрыты тем же коммитом
        # (их seq прошёл через sequencer) — иначе они бы зацикливались.
        # KafkaException = партиция уехала к другому участнику прямо во
        # время обработки (rebalance): коммитит теперь её новый владелец,
        # а нам положены дубли — нормальная цена at-least-once.
        last_by_partition: dict[int, object] = {}
        for msg, _meta, _frame in pairs:
            last_by_partition[msg.partition()] = msg
        try:
            for p in sorted(last_by_partition):
                self.consumer.store_offsets(message=last_by_partition[p])
            self.consumer.commit(asynchronous=False)
        except KafkaException as exc:
            logger.warning("Commit skipped (partition rebalanced?): %s", exc)

        with self._state_lock:
            if failures == 0:
                self.consecutive_failures = 0
        logger.debug(
            "Batch: %d кадров обработано, %d ядовитых (партиции %s)",
            len(pairs), failures, sorted(last_by_partition),
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
        """Липкая CUDA-ошибка: контекст мёртв, восстановление в процессе
        невозможно. os._exit, а не raise SystemExit: мы можем находиться
        в worker-потоке, где SystemExit умер бы молча, а процесс обязан
        умереть, чтобы супервизор поднял его со свежим CUDA-контекстом."""
        logger.critical(
            f"CUDA context is dead ({ctx}): {exc}. "
            "Sticky CUDA errors are unrecoverable in-process — exiting, "
            "the supervisor will restart the consumer.",
        )
        os._exit(1)

    def _mark_failure(self, exc: BaseException, topic: str, offset: int) -> None:
        """Единая обработка упавшего кадра: счётчики, poison, зомби-выход.

        Вызывается из decode-воркеров и GPU-потока — счётчики под lock.
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
            # Не CUDA, но тоже зомби-режим (например, перманентный OOM):
            # N подряд падений — надёжнее рестартнуться, чем молча сжигать кадры.
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
                # Backpressure: конвейер полон — паузим poll, а не колим
                # безлимитную очередь (декоднутые кадры весят ~6 МБ).
                self._slots.acquire()
                seq = self._part_seq[msg.partition()]
                self._part_seq[msg.partition()] = seq + 1
                self.pool.submit(self._decode_submit, seq, msg)
        except KeyboardInterrupt:
            logger.info("Stopping...")
        finally:
            self._closing = True
            try:
                # timeout: при живой, но забитой очереди кадр-drain сам
                # дойдёт до sentinel; при мёртвом GPU-потоке вешаться нельзя.
                self.decoded.put(None, timeout=5)  # sentinel для GPU-потока
            except queue.Full:
                pass
            gpu.join(timeout=5)
            self.pool.shutdown(wait=True)
            self.io_pool.shutdown(wait=True)
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
    # захардкодил бы nano навсегда, как бы ни было озаглавлено compose-командой.
    model = sys.argv[1] if len(sys.argv) > 1 else None
    # Без явного аргумента устройство берётся из настроек:
    # DEVICE=auto → cuda при наличии GPU, иначе cpu (проект не падает без NVIDIA)
    device = sys.argv[2] if len(sys.argv) > 2 else None
    consumer = YOLOConsumer(model_path=model, device=device)
    consumer.run()
