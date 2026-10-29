"""Video Consumer — читает задачи из video_requests, обрабатывает видео YOLO.

Замена Celery-воркера: вместо брокера Redis — тот же Kafka, что у стримов,
вместо очереди задач Celery — обычный консьюмер. Обработка НЕ дублируется:
вызываем app.processing.processor.process_video — тот же код, что выполнял
Celery-воркер.

Гарантии доставки — at-least-once:
  enable.auto.commit=false, offset коммитим ПОСЛЕ обработки. Процесс упал
  посреди видео → сообщение не закоммичено → после рестарта будет
  доставлено заново. process_video идемпотентен (в начале удаляет старые
  детекции видео), поэтому повторная обработка безопасна.

Обработка ОДНОГО видео занимает минуты (инференс по кадрам), поэтому
max.poll.interval.ms поднят до часа: по дефолту (5 минут) Kafka считает
консьюмера умершим прямо посреди обработки и устроит бесконечные ребалансы.
"""
import json
import logging
import time

from confluent_kafka import Consumer

from app.core.cuda import is_fatal_cuda_error
from app.core.database import SessionLocal
from app.core.metrics import (
    frame_task_completed_total,
    frame_task_duration_seconds,
    frame_task_queue_wait_seconds,
    parse_epoch_seconds,
    start_metrics_server,
)
from app.core.topics import KAFKA_BOOTSTRAP_SERVERS, TOPIC_VIDEO_REQUESTS
from app.db.models import Video
from app.processing.processor import process_video, process_video_chunk

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class VideoConsumer:
    """Читает video_requests → process_video → коммит offset после успеха."""

    def __init__(self):
        self.consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "video-workers",
            "auto.offset.reset": "earliest",
            # Ручной коммит — ядро at-least-once (см. докстринг модуля)
            "enable.auto.commit": False,
            # Соседние партиции раздаются консьюмерам по очереди: пакет
            # чанков одного видео (API раскладывает их ротацией по
            # партициям) доставляется разным репликам, а не сваливается
            # одной реплике. Дефолтный range отдал бы p0-p3 одной реплике.
            "partition.assignment.strategy": "roundrobin",
            # Обработка одного видео — минуты; не даём группе нас выгнать
            "max.poll.interval.ms": 3600000,
        })
        self.consumer.subscribe([TOPIC_VIDEO_REQUESTS])

    def run(self):
        logger.info(f"Video consumer listening to '{TOPIC_VIDEO_REQUESTS}'...")
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    continue

                try:
                    self._handle(msg)
                    frame_task_completed_total.labels(status="completed").inc()
                except SystemExit:
                    raise
                except Exception as exc:
                    if is_fatal_cuda_error(exc):
                        # Липкая CUDA-ошибка: контекст мёртв, следующий
                        # запуск так же упадёт в этом процессе. Выходим БЕЗ
                        # коммита — супервизор рестартует контейнер, Kafka
                        # доставит сообщение заново в свежий контекст.
                        # Не считаем в failed: задача не терминальная,
                        # будет переобработана после рестарта.
                        logger.critical(
                            f"Fatal CUDA error on video task (offset={msg.offset()}): {exc}. "
                            "Exiting without commit; the message will be redelivered.",
                        )
                        raise SystemExit(1)
                    # process_video сам ставит status='failed' и перекидывает
                    # исключение — задача в терминальном состоянии, повтор её
                    # не вылечит (битый файл, недоступный MinIO и т.п.).
                    # Коммитим, чтобы не крутить ядовитое сообщение вечно.
                    logger.exception(
                        f"Video task failed permanently (offset={msg.offset()}) — "
                        "marked failed in DB, committing offset.",
                    )
                    frame_task_completed_total.labels(status="failed").inc()
                self.consumer.commit(message=msg, asynchronous=False)
        except KeyboardInterrupt:
            logger.info("Stopping...")
        finally:
            self.consumer.close()

    def _observe_queue_wait(self, payload: dict) -> None:
        """Очередь задач: dequeue − enqueued_at (UTC ISO от API при enqueue).

        Считаем время с момента ОТДАЧИ задачи API, а не с записи в брокер
        (msg.timestamp()): сюда входит и сам produce — «задача застряла в API»
        тоже задержка трассы. Сообщения старых версий API поля не имеют —
        просто не меряем.
        """
        enqueued = parse_epoch_seconds(payload.get("enqueued_at"))
        if enqueued is None:
            return
        wait = time.time() - enqueued
        if wait >= 0:  # перекос часов — мусор, портящий квантили
            frame_task_queue_wait_seconds.observe(wait)

    def _handle(self, msg):
        payload = json.loads(msg.value().decode())
        self._observe_queue_wait(payload)
        started = time.perf_counter()
        try:
            self._process(payload)
        finally:
            # Полная длительность задачи от выдачи из очереди до конца. Разница
            # frame_task_duration − (decode+inference+write чанка) = накладные
            # расходы: presigned URL, подключение к MinIO/БД, seek. Загрузка
            # модели сюда попадает только у первой задачи процесса (синглтон
            # в processor) — горб на этой панели = регресс, вернули тяжёлую
            # инициализацию на задачу.
            frame_task_duration_seconds.observe(time.perf_counter() - started)

    def _process(self, payload: dict):
        video_id = payload["video_id"]

        db = SessionLocal()
        try:
            if "start_s" in payload and "end_s" in payload:
                # Чанковая задача: срез [start_s, end_s) большого видео.
                # Такие задачи приходят пакетами (по одной на чанк) и
                # обрабатываются разными репликами параллельно.
                logger.info(
                    f"Processing video {video_id} chunk "
                    f"[{payload['start_s']:.1f},{payload['end_s']:.1f})..."
                )
                process_video_chunk(video_id, payload["start_s"], payload["end_s"], db)
            else:
                # Классическая задача: всё видео одним проходом
                # (короткие ролики и сообщения старых версий API).
                logger.info(f"Processing video {video_id} (task received)...")
                process_video(video_id, db)
            logger.info(f"Video {video_id} task done")
        except Exception:
            # process_video/process_video_chunk ставят failed сами, но только
            # после того, как нашли строку видео. Ошибки ДО этого (нет записи,
            # нет источника) — помечаем здесь, иначе видео навечно останется
            # в 'processing'. Условие status=='processing': не затыкать
            # 'completed' поздней ошибкой другого чанка того же видео.
            db.rollback()
            db.query(Video).filter(
                Video.id == video_id, Video.status == "processing",
            ).update(
                {"status": "failed"}, synchronize_session=False,
            )
            db.commit()
            raise
        finally:
            db.close()


if __name__ == "__main__":
    # Порт до коннекта к Kafka: /metrics должен жить даже в ожидании брокера
    start_metrics_server(9093)
    consumer = VideoConsumer()
    consumer.run()
