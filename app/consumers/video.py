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

from confluent_kafka import Consumer

from app.core.cuda import is_fatal_cuda_error
from app.core.database import SessionLocal
from app.core.topics import KAFKA_BOOTSTRAP_SERVERS, TOPIC_VIDEO_REQUESTS
from app.db.models import Video
from app.processing.processor import process_video

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
                except SystemExit:
                    raise
                except Exception as exc:
                    if is_fatal_cuda_error(exc):
                        # Липкая CUDA-ошибка: контекст мёртв, следующий
                        # запуск так же упадёт в этом процессе. Выходим БЕЗ
                        # коммита — супервизор рестартует контейнер, Kafka
                        # доставит сообщение заново в свежий контекст.
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
                self.consumer.commit(message=msg, asynchronous=False)
        except KeyboardInterrupt:
            logger.info("Stopping...")
        finally:
            self.consumer.close()

    def _handle(self, msg):
        payload = json.loads(msg.value().decode())
        video_id = payload["video_id"]

        db = SessionLocal()
        try:
            logger.info(f"Processing video {video_id} (task received)...")
            process_video(video_id, db)
            logger.info(f"Video {video_id} processed")
        except Exception:
            # process_video ставит failed сам, но только после того, как нашёл
            # строку видео. Ошибки ДО этого (нет записи, нет источника) —
            # помечаем здесь, иначе видео навечно останется в 'processing'.
            db.rollback()
            db.query(Video).filter(Video.id == video_id).update(
                {"status": "failed"}, synchronize_session=False,
            )
            db.commit()
            raise
        finally:
            db.close()


if __name__ == "__main__":
    consumer = VideoConsumer()
    consumer.run()
