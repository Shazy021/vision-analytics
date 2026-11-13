"""YOLO Consumer: читает кадры из Kafka, прогоняет через Triton, отдаёт детекции.

Путь кадра:
    raw_frames (Kafka) → декод → Triton (поток async-запросов) → detections (Kafka) + кадр в MinIO

Кадры (чистые, без рамок) сохраняем в MinIO только если на них есть детекции —
пустые кадры хранить незачем. Ключ: streams/<stream_id>/<frame_index>.jpg.
Стрим эфемерен (прошёл и исчез) — без сохранения показать потом нечего.

АРХИТЕКТУРА: конвейер из четырёх ролей, чтобы CPU, GPU и сеть не ждали друг друга.

  main (поток run)      Забирает сообщения из Kafka и раздаёт на декод.
                        Сам ничего тяжёлого не делает.
  decode (пул)          Готовит кадр к отправке. В cv2-режиме: imdecode +
                        letterbox (cv2 отпускает GIL — потоки параллельны).
                        В jpeg-режиме (TRITON_JPEG_INTAKE) пикселей не
                        касается вовсе: байты идут в DALI-ансамбль yolo_jpeg
                        как есть, стадия сводится к распаковке конверта.
  send (N потоков)      Каждый кадр сразу улетает отдельным async-запросом по
                        своему gRPC-каналу. Один поток отправки — потолок ~110
                        req/s (сериализация 4.9 МБ ~9 мс), поэтому их несколько:
                        по одному на канал TritonDetector.
  диспетчер (1 поток)   Забирает ответы Triton'а, шлёт детекции в Kafka,
                        сдвигает границу обработки.

Почему конвейер: наивный цикл poll → decode → YOLO → отправка дал только ~20 fps
— GPU ждал декод, декод ждал сеть. В конвейере все стадии работают одновременно.

Почему кадры НЕ лепятся в батч на клиенте (замер на живом сервере, RTX 5080,
yolov8l engine): синхронные запросы с батчами по 8 кадров (39 МБ) дают потолок
~61 fps, GPU при этом занят ~10% — сервер получает работу «кусочками» и
исполняет каждый запрос отдельно (динамическому батчингу склеивать нечего:
preferred_batch_size достигнут уже в первом запросе). Непрерывный поток
одиночных async-запросов с окном 32–64 кадров в полёте: ~156 fps при GPU,
занятом ~100% — батчи в прогоны движка склеивает сам Triton.

КАК МЫ ЗАПОМИНАЕМ, ЧТО ОБРАБОТАНО (это заменило старый «секвенсор»).
Kafka хранит на партицию одну границу: «обработано всё до offset N». Сказать
«1,2,4 готовы, а 3 ещё нет» — нельзя. Значит границу можно поднимать только до
самого старого НЕобработанного сообщения. Мы держим множество таких должников
(self._inflight): забрал кадр из Kafka — добавь в должники; полностью обработал
или признал битым — убери. Граница = min(должников) − 1.
Благодаря этому кадры можно обрабатывать в ЛЮБОМ порядке (в каком Triton
ответил) — ничего не надо переупорядочивать, а граница всё равно честная.
Продюсер кладёт кадры в партиции по кругу (без key) — партиции равные,
ни один стрим не отстаёт от других.

ГАРАНТИЯ: at-least-once. Границу сдвигаем только после producer.flush() —
когда детекции точно долетели до брокера. При краше необработанное прилетит
заново: дубли дешевле потерь.

ОШИБКИ:
  • Битый кадр — пропускаем, считаем в poison_messages_total, границу не держит.
  • 10 подряд любых падений — уходим на рестарт (os._exit): надёжнее вечного
    зомби. «Заразных» CUDA-ошибок здесь больше нет: контекст CUDA живёт
    на стороне Triton-сервера, в этом процессе torch не грузится.
"""
import itertools
import json
import logging
import os
import queue
import threading
import time
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor

import cv2
import numpy as np
from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition

from app.core.config import settings
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

# У OpenCV есть собственный пул потоков (его крутят resize/blobFromImage из
# decode-воркеров). У нас параллелизм уже внешний — N decode-потоков, — и
# внутренние пулы только дерутся с ним за ядра (8 воркеров × N внутренних
# потоков = оверсабскрипш, переключения контекста вместо работы). Пусть
# каждый вызов OpenCV идёт в потоке-владельце, а масштабируем мы сами.
cv2.setNumThreads(1)

# Сколько падений подряд терпим, прежде чем уйти на рестарт.
# Одно-два — случайность (битый JPEG), десять подряд — уже болезнь процесса
# (Triton лёг, сеть легла, engine сломался).
MAX_CONSECUTIVE_FAILURES = 10

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


class YOLOConsumer:
    """Конвейер: Kafka → параллельный декод → async-поток в Triton → detections + MinIO"""

    def __init__(
        self,
        *,
        io_workers: int | None = None,
        window: int | None = None,
        channels: int | None = None,
    ):
        # Инференс живёт в Triton-сервере: в этом процессе только лёгкий
        # gRPC-клиент (ни torch, ни GPU не нужны) — как и в видео-трассе
        # (processor.py), трассы ходят в один и тот же Triton.
        # Стримовый путь после замеров оставили только асинхронным.
        # Ленивый импорт: tritonclient тянется здесь, а не при импорте модуля.
        from app.processing.triton_detector import TritonDetector

        # Параметры: аргументы (тесты) > settings (env) > дефолты.
        self.window = window or settings.yolo_infer_window
        self.channels = max(1, channels or settings.triton_channels)
        # jpeg_intake: предобработка (декод JPEG, resize, letterbox) живёт в
        # DALI-ансамбле yolo_jpeg на GPU Triton'а; консьюмер шлёт байты как
        # есть. Ставится через TRITON_JPEG_INTAKE (см. config.py).
        self.detector = TritonDetector(channels=self.channels,
                                       jpeg_intake=settings.triton_jpeg_intake)

        workers = io_workers or settings.yolo_io_workers
        self.pool = ThreadPoolExecutor(
            max_workers=workers, thread_name_prefix="yolo-decode"
        )
        # MinIO-аплоады — отдельный пул: ожидание сети хранилища не должно
        # затыкать декод, а декод не должен задерживать отправку в Triton.
        self.io_pool = ThreadPoolExecutor(
            max_workers=max(2, workers // 2), thread_name_prefix="yolo-minio"
        )

        # Потоков отправки — по числу gRPC-каналов: один канал сериализует
        # крупные запросы (~110 req/s потолок), каждому потоку нужно своё
        # соединение, иначе они просто выстроятся в очередь к сериализатору.
        self.send_workers = self.channels

        # Сколько кадров может быть «в работе» одновременно (забрали из Kafka,
        # ещё не обработали до конца). window — те, что уже в полёте к Triton,
        # ещё столько же — максимум на стадии декода/в трубе. Это крышка
        # памяти: декоднутый 1080p-кадр весит ~6 МБ, без лимита они копились
        # в очередях консьюмера минутами (замер: e2e p95 24с при ПУСТОМ Kafka).
        # Работает цепочкой: decoded полна → декод-воркер ждёт put() → жетоны
        # не возвращаются → poll встал. Лишнее остаётся в Kafka: там бэклог
        # дешёвый (диск брокера) и заметный (метрика queue_wait, lag).
        self.inflight_cap = self.window * 2
        # Жетоны: перед тем взять кадр из Kafka, поток обязан занять жетон,
        # вернуть — после полной обработки. Все заняты → poll ждёт.
        self._slots = threading.BoundedSemaphore(self.inflight_cap)
        # Труба между декодерами и потоками отправки: (msg, meta, prepared).
        self.decoded: queue.Queue = queue.Queue(maxsize=self.inflight_cap)
        # Окно кадра «в полёте» к Triton: жетон берёт поток отправки перед
        # submit, возвращает диспетчер вместе с результатом. Меньше окна —
        # GPU голодает в сетевых паузах; больше — латентность и RAM.
        self._win_slots = threading.BoundedSemaphore(self.window)

        # Список должников для границы обработки: партиция → {offsets}.
        # Здесь лежат offset'ы, забранные из Kafka, но ещё НЕ обработанные
        # до конца. Границу «обработано до N» нельзя поднимать выше самого
        # старого должника. _last_seen — самый дальний забранный offset
        # (когда должников нет, граница = он).
        self._inflight: dict[int, set[int]] = defaultdict(set)
        self._last_seen: dict[int, int] = {}
        # Куда по каждой партиции реально доехал последний успешный коммит.
        # Без этой проверки каждый цикл публикации хранил бы тот же offset
        # и звал бы commit() «впустую»: librdkafka отвечает _NO_OFFSET,
        # лог засирается, а при окне в 64 кадров водораздел между циклами
        # чаще всего НЕ двигается — кадры долетают не по порядку.
        self._committed: dict[int, int] = {}
        # corr → (msg, meta, ts_submit) для кадров в полёте к Triton.
        # corr выдаёт КОНСЬЮМЕР и регистрирует кадр здесь ДО отправки:
        # ответ может приехать быстрее, чем вернётся submit_async
        # (gRPC дёргает done-callback мгновенно для уже завершённого RPC) —
        # если corr ещё не тут, _handle_result не найдёт кадр, и он навсегда
        # останется должником: watermark замрёт за этим призраком
        # (так падал бенчмарк 2026-11-05 17:32).
        self._pending: dict[int, tuple] = {}
        self._pending_lock = threading.Lock()
        self._corr_seq = itertools.count(1)
        # Разобранные результаты, ждущие отправки в Kafka (только диспетчер).
        self._ready: list[tuple] = []
        # Хвост MinIO-аплоадов (их НЕ ждём inline, см. _publish_ready).
        self._uploads: deque = deque()
        # В эти словари и счётчики пишут три вида потоков (main, decode,
        # диспетчер) — все изменения только под этим замком.
        self._state_lock = threading.Lock()
        self.poison_count = 0
        self.consecutive_failures = 0
        self._closing = False
        self._send_threads: list[threading.Thread] = []

        self.consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "yolo-workers",
            "auto.offset.reset": "earliest",
            # Границу «обработано до N» двигаем ТОЛЬКО сами, после полной
            # обработки и flush (at-least-once, см. _commit_watermarks).
            "enable.auto.commit": False,
            # Техническое требование confluent-kafka: при ручном store_offsets
            # автозапись offset'а на poll тоже нужно выключить, иначе библиотека
            # бросает _INVALID_ARG.
            "enable.auto.offset.store": False,
            "fetch.max.bytes": 5 * 1024 * 1024,
        })
        self.consumer.subscribe([TOPIC_RAW_FRAMES], on_revoke=self._on_revoke)

        self.producer = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "message.max.bytes": 5 * 1024 * 1024,
        })

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

    def _on_revoke(self, consumer, partitions) -> None:
        """Партиции отзывают (rebalance) — забыть всё состояние по ним.

        Должники отозванной партиции — «призраки»: мы их больше не
        заберём, а новый владелец начнёт с последней закоммиченной границы
        (at-least-once: часть кадров он обработает заново — это цена
        гарантии). Если состояние НЕ чистить, min(должников) навсегда
        замирает за самым старым призраком: кадры идут, обрабатываются,
        детекции летят — а committed offset стоит, лаг растёт со скоростью
        подачи. Именно это и поймал бенчмарк после ребаланса 2026-11-05.
        """
        with self._state_lock:
            for tp in partitions:
                self._inflight.pop(tp.partition, None)
                self._last_seen.pop(tp.partition, None)
                self._committed.pop(tp.partition, None)
        logger.info("Partitions revoked, watermark state dropped: %s",
                    [tp.partition for tp in partitions])

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

        Если кадр битый — он НЕ идёт в Triton, но обязательно убирается из
        должников: иначе его offset вечно держал бы границу обработки, и
        Kafka заново слала бы этот кадр по кругу.
        """
        try:
            try:
                meta, prepared = self._prepare(msg)
            except Exception as exc:
                self._mark_failure(exc, TOPIC_RAW_FRAMES, msg.offset())
                self._leave_inflight(msg)
                return
            self._emit((msg, meta, prepared))
        finally:
            self._slots.release()  # жетон возвращается при ЛЮБОМ исходе

    # ------------------------------------------------------------------
    # Стадия 2: декод одного кадра (вызывается в потоке пула)
    # ------------------------------------------------------------------

    def _prepare(self, msg):
        """Разобрать конверт и подготовить кадр к отправке (в потоке пула).

        Два режима (settings.triton_jpeg_intake):
          cv2  — декодировать кадр и сделать letterbox (дорогая CPU-часть);
          jpeg — байты уходят в Triton как есть, ВСЁ чтение пикселей делает
                 DALI внутри ансамбля yolo_jpeg (декод, resize, letterbox);
                 геометрия обратного хода считается из width/height в мете,
                 кадр здесь не декодируется ни при каких условиях.

        Декодируем ПОЛНЫМ размером: стримы приходят в разном разрешении,
        и урезанный декод (IMREAD_REDUCED_COLOR_2) одинаково режет детали
        и у 1080p (безвредно), и у 480p (убивает детекции) — различить их
        дешёвого способа нет, поэтому не экономим на качестве.
        MinIO это не касается вовсе: там лежит сырой JPEG без перекодирования.

        Счётчик «кадр долетел до нас» стоит ЗДЕСЬ, до проверки на битость:
        сравнение rate(frames_received_total) с rate(frame_processed_total)
        показывает, едим ли мы свой вход или отстаём.
        """
        frames_received_total.inc()
        wait = queue_wait_seconds(msg)
        if wait is not None:
            frame_queue_wait_seconds.labels(topic=TOPIC_RAW_FRAMES).observe(wait)

        data, frame_bytes = unpack(msg.value())

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

        if self.detector.jpeg_intake:
            # Битый JPEG здесь не поймается (пикселей не касаемся) —
            # разберётся DALI на сервере, ошибка приедет через _handle_result.
            w, h = data.get("width"), data.get("height")
            if not w or not h:
                # Сообщения от producer'а до появления полей width/height
                # (окно — retention топика после апгрейда). Травим, а не
                # декодируем ради размеров: декод в этом режиме — то, от
                # чего мы уходили.
                raise ValueError("нет width/height в мете (старый producer?)")
            return meta, self.detector.prepare_jpeg(frame_bytes, w, h)

        # Декодируем кадр
        started = time.perf_counter()
        buf = np.frombuffer(frame_bytes, dtype=np.uint8)
        frame = cv2.imdecode(buf, cv2.IMREAD_COLOR)
        frame_decode_duration_seconds.observe(time.perf_counter() - started)
        if frame is None:
            raise ValueError("кадр не декодировался (битый JPEG?)")

        prepared = self.detector.prepare(frame)
        del frame
        return meta, prepared

    # ------------------------------------------------------------------
    # Стадия 3: потоки отправки — выстреливают кадры в Triton
    # ------------------------------------------------------------------

    def _send_worker(self) -> None:
        """Поток отправки: забирает кадр из трубы и СТРЕЛЯЕТ в Triton.

        Отдельные потоки (по числу gRPC-каналов детектора), потому что
        сериализация запроса (~9 мс на 4.9 МБ) на одном потоке даёт потолок
        ~110 req/s — это был реальный предел прошлого прогона. Окно в
        полёте держит семафор: жетон берёт _submit_frame, возвращает
        диспетчер вместе с результатом.
        """
        while True:
            if self._closing:
                return
            try:
                item = self.decoded.get(timeout=0.2)
            except queue.Empty:
                continue
            if item is None:
                # «Сигнал остановки» из run(): новые кадры не шлём,
                # отправленные доработает диспетчер.
                self._closing = True
                return
            self._submit_frame(item)

    def _submit_frame(self, item: tuple) -> None:
        msg, meta, prepared = item
        # Жетон окна — здесь, а не у вызывающего: этот же метод возвращает
        # его при неудачной отправке, так границы владения не разъезжаются.
        self._win_slots.acquire()
        # Порядок СВЯЗАН: зарегистрировать pending ДО отправки. Ответ
        # может приехать быстрее, чем вернётся submit_async (gRPC дёргает
        # done-callback мгновенно для уже завершённого RPC) — если corr
        # ещё не в pending, _handle_result не найдёт кадр, и он навсегда
        # останется должником: watermark замрёт за этим призраком
        # (так падал бенчмарк 2026-11-05 17:32).
        corr = next(self._corr_seq)
        with self._pending_lock:
            self._pending[corr] = (msg, meta, time.perf_counter())
        try:
            self.detector.submit_async(
                corr, prepared, frame_index=meta["frame_index"])
        except Exception as exc:
            # Отправка не состоялась (канал gRPC лёг и т.п.) — кадр закрыт
            # как проваленный; хроническая болезнь убьёт процесс в _mark_failure.
            with self._pending_lock:
                self._pending.pop(corr, None)
            self._win_slots.release()
            self._mark_failure(exc, TOPIC_RAW_FRAMES, msg.offset())
            self._leave_inflight(msg)

    # ------------------------------------------------------------------
    # Стадия 4: диспетчер — ответы Triton'а, детекции, граница
    # ------------------------------------------------------------------

    def _dispatch_thread(self) -> None:
        """Диспетчер: принимает ответы и шлёт детекции в Kafka.

        Кадр уходит в Triton СРАЗУ по готовности (см. докстринг модуля:
        поток одиночных async-запросов даёт ~156 fps против ~61 fps у
        клиентских батчей синхронными запросами). Порядок возврата
        результатов произволен — граница «обработано до N» считается от
        должников, ей порядок не важен.
        """
        while True:
            self._drain_results()
            with self._pending_lock:
                has_pending = bool(self._pending)
            # На остановке выходим, только когда приняты ВСЕ ответы и
            # потоки отправки точно ничего не собираются отправить:
            # иначе кадр, зарегистрированный в последнюю миллисекунду,
            # остался бы неопубликованным (дублирует Kafka — но зачем).
            senders_alive = any(t.is_alive() for t in self._send_threads)
            if self._closing and not has_pending and not senders_alive:
                break
            # Ждать результат с таймаутом нужно ВСЕГДА, даже когда pending
            # пуст: цикл без сна (busy-spin) крутит миллионы итераций, а
            # is_alive() выше — это ctypes-вызовы, держащие GIL. Такой спин
            # голодал decode-воркеры и poll: в бенче 20261105-221647 замерные
            # decode и queue_raw раздуло с <10 мс до 10–50 мс на ЛЮБОЙ
            # нагрузке — это был этаж contention, а не реальные пиксели.
            try:
                self._handle_result(*self.detector.next_result(timeout=0.05))
            except queue.Empty:
                pass

        # Все ответы приняты (pending пуст — результат приезжает ровно для
        # одного из pending), осталась недосланная пачка детекций.
        self._publish_ready()

    def _drain_results(self) -> None:
        """Забрать всё готовое из очереди ответов и отправить пачкой."""
        while True:
            try:
                corr, payload = self.detector.next_result(timeout=0)
            except queue.Empty:
                break
            self._handle_result(corr, payload)
        self._publish_ready()
        # Яд сдвигает границу (снимает должника), но детекций не порождает,
        # а _publish_ready при пустом _ready выходит сразу — без этого
        # коммита watermark догонял бы яд только со следующим успешным
        # кадром, а на паузе в трафике — никогда. Без изменений коммит
        # дешёв: _committed-гард возвращает пустой список до всякого Kafka.
        self._commit_watermarks()

    def _handle_result(self, corr: int, payload) -> None:
        with self._pending_lock:
            entry = self._pending.pop(corr, None)
        if entry is None:
            # Результат без запроса — так быть не должно: corr выдаём мы и
            # только мы. Молча проглотить — значит потерять кадр-призрак
            # (должник останется навсегда, watermark встанет). Кричим.
            logger.error(
                "Результат с неизвестным corr=%s — кадр потерян, "
                "watermark может встать (баг регистрации?)", corr,
            )
            return
        msg, meta, ts = entry
        self._win_slots.release()
        if isinstance(payload, BaseException):
            # Кадр не получился: детекций нет, но границу держать он
            # больше не должен — иначе Kafka зациклит его по кругу.
            self._mark_failure(payload, TOPIC_RAW_FRAMES, msg.offset())
            self._leave_inflight(msg)
            return
        frame_inference_duration_seconds.observe(time.perf_counter() - ts)
        self._ready.append((msg, meta, payload))

    def _publish_ready(self) -> None:
        """Готовые детекции в Kafka, кадры в MinIO, сдвиг границы.

        Порядок гарантии: produce → flush (детекции приняты брокером) →
        сдвиг границы. MinIO-аплоады НЕ ждутcя inline — доставка детекций
        в Kafka и есть гарантия at-least-once, а ожидание сети MinIO серило
        бы диспетчер.
        """
        if not self._ready:
            return
        for msg, meta, det in self._ready:
            # Ключ — stream_id: все кадры стрима падают в одну партицию
            # detections (hash(stream_id) % N), а значит достаются одному
            # detection-консьюмеру. Трекинг живёт в памяти консьюмера, и
            # расширение detections без ключа размазало бы один стрим по
            # нескольким трекерам — один самолёт получил бы несколько
            # track_id, каждый консьюмер видел бы его через кадр. С ключом
            # партиций можно делать сколько угодно — стрим всегда целиком у
            # одного владельца.
            self.producer.produce(TOPIC_DETECTIONS,
                                  key=str(meta["stream_id"]),
                                  value=self._detection_message(meta, det))
            frame_processed_total.labels(result="ok").inc()
            # Чистый кадр в MinIO — ТОЛЬКО если на нём есть детекции
            if meta["stream_id"] and det:
                self._uploads.append(self.io_pool.submit(
                    self._save_frame_to_minio,
                    meta["stream_id"],
                    meta["frame_index"],
                    meta["frame_bytes"],
                ))
        self._prune_uploads()
        # flush() ждёт, пока детекции реально примут в брокере. Именно с этого
        # момента кадр считается обработанным — поэтому flush обязан быть ДО
        # сдвига границы, иначе при краше мы потеряли бы кадры (at-least-once).
        self.producer.flush()
        # Кадров больше нет среди должников → сдвигаем границу. Она накроет
        # и битые кадры (их убрали из должников ещё в _decode_submit), и
        # проваленные в инференсе: их offset тоже «обработан», второй раз он не нужен.
        for msg, _meta, _det in self._ready:
            self._leave_inflight(msg)
        self._commit_watermarks()
        self._ready.clear()
        with self._state_lock:
            self.consecutive_failures = 0

    def _prune_uploads(self) -> None:
        """Не копить хвост MinIO-аплоадов: готовые снимаем, хвост ждём.

        Если MinIO лёг, аплоады копятся в памяти пула — при переполнении
        подождать самый старый: лучше притормозить конвейер, чем сожрать RAM.
        """
        while self._uploads and self._uploads[0].done():
            self._uploads.popleft()
        if len(self._uploads) > 2 * self.window:
            self._uploads.popleft().result()

    @staticmethod
    def _detection_message(meta: dict, det: list) -> bytes:
        """JSON детекции одного кадра — формат, который ждёт detections-консьюмер."""
        objects = [
            {
                "class": d.class_name,
                "confidence": d.confidence,
                "bbox": {"x1": d.bbox[0], "y1": d.bbox[1], "x2": d.bbox[2], "y2": d.bbox[3]},
            }
            for d in det
        ]
        return json.dumps({
            "video_id": None,
            "stream_id": meta["stream_id"],
            "frame_index": meta["frame_index"],
            "source": meta["source"],
            "captured_at": meta["captured_at"],
            "objects": objects,
            "num_detections": len(objects),
        }).encode()

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

    def _mark_failure(self, exc: BaseException, topic: str, offset: int) -> None:
        """Единая обработка «кадр не получился»: счётчики, лог, решение о смерти.

        Вызывается из decode-воркеров, потоков отправки и диспетчера —
        счётчики под замком.
        """
        with self._state_lock:
            self.poison_count += 1
            self.consecutive_failures += 1
        frame_processed_total.labels(result="error").inc()
        poison_messages_total.inc()
        logger.exception(
            f"Poison message skipped: topic={topic} offset={offset} (total {self.poison_count})",
        )
        with self._state_lock:
            giving_up = self.consecutive_failures >= MAX_CONSECUTIVE_FAILURES
        if giving_up:
            # 10 падений подряд — не случайность, а болезнь (Triton лёг,
            # сеть легла, перманентная ошибка движка). Рестарт надёжнее,
            # чем молча сжигать каждый кадр. os._exit, а не raise: мы можем
            # сидеть в worker-потоке, где исключение убило бы только поток.
            logger.critical(
                f"{self.consecutive_failures} consecutive failures — exiting for restart.",
            )
            os._exit(1)

    def _commit_watermarks(self) -> None:
        """Сообщить Kafka границу «обработано до N» по каждой партиции.

        Граница = min(должников) − 1, а если должников нет — последний
        забранный offset. Ниже неё нет ни одного необработанного сообщения,
        поэтому при краше Kafka отдаст заново только то, что выше границы:
        дубли, но не потери.

        Партиции с недвигнувшейся границей не храним и не коммитим
        (_committed-гард): повторный commit того же offset — это локальная
        ошибка _NO_OFFSET и спам в лог, а не авария.

        KafkaException означает: партиция уехала к другому консьюмеру прямо
        во время обработки (rebalance) — теперь её коммитит новый владелец,
        а нам достанутся дубли. Это не авария, а нормальная цена гарантии.
        """
        with self._state_lock:
            offsets = []
            for p in sorted(self._last_seen):
                inflight = self._inflight.get(p)
                watermark = (min(inflight) - 1) if inflight else self._last_seen[p]
                # +1: брокер хранит не «обработано до», а «следующий нужный»
                next_offset = watermark + 1
                if next_offset > self._committed.get(p, 0):
                    offsets.append(TopicPartition(TOPIC_RAW_FRAMES, p, next_offset))
        if not offsets:
            return
        try:
            self.consumer.store_offsets(offsets=offsets)
            self.consumer.commit(asynchronous=False)
        except KafkaException as exc:
            logger.warning("Commit skipped (partition rebalanced?): %s", exc)
        else:
            for tp in offsets:
                self._committed[tp.partition] = tp.offset

    def run(self):
        logger.info(
            "YOLO consumer listening to '%s'... (async stream, window=%d, "
            "senders=%d, decode_threads=%d, inflight<=%d)",
            TOPIC_RAW_FRAMES, self.window, self.send_workers,
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

        # Сначала потоки отправки (они кормят Triton), затем диспетчер
        # (он забирает ответы и шлёт детекции).
        self._send_threads = [
            threading.Thread(target=self._send_worker,
                             name=f"yolo-send-{i}", daemon=True)
            for i in range(self.send_workers)
        ]
        for t in self._send_threads:
            t.start()
        dispatcher = threading.Thread(target=self._dispatch_thread,
                                      name="yolo-dispatch", daemon=True)
        dispatcher.start()
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
            # Остановка: даём диспетчеру доработать и уйти, дожимаем очереди.
            self._closing = True
            try:
                # Кладём в трубу None — «сигнал остановки». Отправляющий
                # поток спит в get(timeout=...) и проснётся, только когда
                # что-то прилетит; остальные выйдут по флагу _closing.
                # timeout=5: если потоки уже мертвы, вешаться нельзя.
                self.decoded.put(None, timeout=5)
            except queue.Full:
                pass
            dispatcher.join(timeout=10)
            for t in self._send_threads:
                t.join(timeout=2)
            self.pool.shutdown(wait=True)     # долить все декоды
            self.io_pool.shutdown(wait=True)  # долить все аплоады
            self.consumer.close()             # выйти из группы — ребаланс без нас
            self.producer.flush()             # долить detections в брокер


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # Порт метрик поднимаем ПЕРВЫМ: /metrics должен отвечать, пока консьюмер
    # ещё подключается к Triton (иначе Prometheus считает его мёртвым на старте).
    start_metrics_server(9091)
    consumer = YOLOConsumer()
    consumer.run()
