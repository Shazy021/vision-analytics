"""Kafka Producer — читает источник (RTSP / HLS / файл) и шлёт кадры в топик raw_frames

Запускается как subprocess сервисом stream_manager
(app/streaming/manager.py — reconcile loop), поэтому:
- логи идут в stdout/stderr процесса и видны в `docker compose logs stream-manager`;
- адрес источника должен открываться С ТОЧКИ ЗРЕНИЯ КОНТЕЙНЕРА stream-manager:
  * встроенный MediaMTX:          rtsp://mediamtx:8554/<path>
  * источник на хосте (Windows):  rtsp://host.docker.internal:<port>/<path>
  * localhost изнутри контейнера — это сам контейнер, а НЕ машина хоста.
"""
import base64
import json
import logging
import os
import time

import cv2
from confluent_kafka import Producer

from app.core.topics import KAFKA_BOOTSTRAP_SERVERS, TOPIC_RAW_FRAMES

logger = logging.getLogger(__name__)

# MediaMTX в docker-compose принимает RTSP только по TCP (MTX_RTSP_TRANSPORTS: tcp),
# а OpenCV по умолчанию тянет RTSP по UDP — в Docker/WSL2 UDP-пакеты часто
# теряются, и VideoCapture не открывается вовсе. Форсим TCP для RTSP-захвата.
# setdefault — чтобы значение можно было переопределить снаружи (compose/.env).
RTSP_CAPTURE_ENV = "OPENCV_FFMPEG_CAPTURE_OPTIONS"
RTSP_CAPTURE_OPTS = "rtsp_transport;tcp"

# HLS (.m3u8 по http/https) — тоже live-источник: кадры прибывают в реальном
# темпе, как с RTSP-камеры, а не читаются «на всю скорость диска» из файла.
# Многие публичные HLS-плейлисты (напр. веб-камеры) отдают 403/пустой ответ
# дефолтному юзер-агенту FFmpeg, поэтому для http(s) шлём браузерный UA.
HTTP_UA_OPTS = (
    "user_agent;Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0 Safari/537.36"
)

OPEN_ATTEMPTS = 3      # сколько раз пробуем открыть источник
OPEN_RETRY_DELAY = 3   # пауза между попытками, сек

# Код выхода при «источник вообще не открылся» (битый/недоступный RTSP).
# Отдельный от общей ошибки (1), чтобы stream_manager отличал «источник
# недоступен» от «упал в рантайме» и гарантированно засчитывал это как краш,
# НЕ завися от того, сколько секунд заняли попытки открытия. Синхронизировано
# с EXIT_OPEN_FAILED в app/streaming/manager.py.
EXIT_OPEN_FAILED = 42


def _is_rtsp(source: object) -> bool:
    return str(source).lower().startswith("rtsp://")


def _is_http(source: object) -> bool:
    return str(source).lower().startswith(("http://", "https://"))


def _is_hls(source: object) -> bool:
    """HLS-плейлист (.m3u8) — live-поток по http. Прогрессивный mp4 по http
    (.mp4) — это «файл по сети»: читается быстро, требует эмуляции реального
    времени, как локальный файл. Различаем по расширению плейлиста."""
    return _is_http(source) and ".m3u8" in str(source).lower()


class FrameProducer:
    """Читает видео/RTSP поток и отправляет кадры в Kafka"""

    def __init__(self, source: str, fps: int = 5, stream_id: int | None = None):
        self.source = source
        self.fps = fps
        self.stream_id = stream_id
        self.producer = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "message.max.bytes": 5 * 1024 * 1024,
        })

    def _delivery_report(self, err, msg):
        if err:
            logger.error(f"Delivery error: {err}")

    def _open_capture(self) -> cv2.VideoCapture:
        """Открыть источник с ретраями и понятной диагностикой.

        Раньше провал открытия ронял процесс молча (stderr глотался роутом),
        и «стрим не робил» без единой строчки в логах — теперь причина видна
        в `docker compose logs stream-manager`.
        """
        os.environ.setdefault(
            RTSP_CAPTURE_ENV,
            HTTP_UA_OPTS if _is_http(self.source) else RTSP_CAPTURE_OPTS,
        )
        is_rtsp = _is_rtsp(self.source)

        for attempt in range(1, OPEN_ATTEMPTS + 1):
            cap = cv2.VideoCapture(self.source)
            if cap.isOpened():
                return cap
            cap.release()
            logger.warning(
                "Попытка %d/%d: не удалось открыть источник %r",
                attempt, OPEN_ATTEMPTS, self.source,
            )
            time.sleep(OPEN_RETRY_DELAY)

        if is_rtsp:
            hint = (
                "Адрес должен открываться ИЗ контейнера stream-manager: localhost внутри "
                "контейнера — это сам контейнер. Для встроенного MediaMTX — "
                "rtsp://mediamtx:8554/<path>, для источника на хосте — "
                "rtsp://host.docker.internal:<port>/<path>. Убедись, что в источник "
                "кто-то ПУБЛИКУЕТ поток (MediaMTX сам видео не генерирует)."
            )
        elif _is_hls(self.source):
            hint = (
                "HLS-плейлист должен открываться ИЗ контейнера stream-manager "
                "(нужен исходящий доступ в интернет). Проверить из контейнера: "
                f"docker compose exec stream-manager python -c \"import cv2; "
                f"print(cv2.VideoCapture({str(self.source)!r}).isOpened())\""
            )
        else:
            hint = (
                "Проверь, что файл существует ВНУТРИ контейнера api "
                "(./uploads на хосте = /app/uploads в контейнере)."
            )
        raise ConnectionError(
            f"Не удалось открыть источник {self.source!r} за {OPEN_ATTEMPTS} попыток. {hint}"
        )

    def _initial_frame_index(self) -> int:
        """
        Продолжить нумерацию кадров после рестарта стрима.

        frame_index — это ключ кадра в MinIO (streams/{id}/{idx}.jpg), а
        дашборд в /latest-frame берёт max() по этим ключам. Если после
        каждого рестарта начинать с 0, новые кадры перезаписывают старые
        ключи, max() навсегда залипает на значении из первого запуска —
        и превью «стоит на одном кадре», хотя детекции в БД капают.

        Стартуем с max(frame_index) из БД: кадры в MinIO сохраняются только
        при наличии детекций, поэтому в БД есть запись на каждый сохранённый
        кадр и этот максимум гарантированно >= любого существующего ключа.
        """
        if self.stream_id is None:
            return 0
        try:
            from sqlalchemy import func

            from app.core.database import SessionLocal
            from app.db.models import Detection

            with SessionLocal() as db:
                max_idx = (
                    db.query(func.max(Detection.frame_index))
                    .filter(Detection.stream_id == self.stream_id)
                    .scalar()
                )
            if max_idx:
                logger.info("Продолжаю нумерацию кадров с %d (было в БД)", max_idx + 1)
                return max_idx + 1
        except Exception:
            logger.exception("Не удалось прочитать max frame_index — стартую с 0")
        return 0

    def run(self):
        cap = self._open_capture()

        # Файл в роли «стрима» зацикливаем: стрим бесконечен по определению,
        # а одиночный проход файла завершает процесс через N секунд
        # и стрим сам собой «умирает» — сбивает с толку.
        # Для HLS этот же путь работает как реконнект: сетевой сбой читается
        # как EOF, переподключение на месте дешевле, чем смерть процесса
        # (3 быстрых падения подряд уводят стрим в error у reconciler'а).
        is_rtsp = _is_rtsp(self.source)
        is_hls = _is_hls(self.source)
        reopen_on_end = not is_rtsp and self.stream_id is not None

        src_fps = cap.get(cv2.CAP_PROP_FPS) or 30
        frame_interval = max(1, int(src_fps / self.fps))
        # сквозной счётчик прочитанных кадров (ключ кадра в MinIO);
        # продолжается с прошлого максимума — см. _initial_frame_index()
        frame_index = self._initial_frame_index()
        sent_count = 0

        # Для файлов эмулируем реальное время: RTSP и HLS и так идут в реальном
        # темпе (кадры прибывают с камеры), а файл читается быстрее некуда — без
        # паузы он завалит Kafka кадрами со скоростью диска вместо заявленных fps.
        is_file = not is_rtsp and not is_hls
        frame_period = 1.0 / src_fps
        next_read = time.monotonic()

        logger.info(
            "Streaming → Kafka '%s' at %d fps (source=%s, stream_id=%s%s)",
            TOPIC_RAW_FRAMES, self.fps, self.source, self.stream_id,
            ", loop" if reopen_on_end else "",
        )

        try:
            while True:
                ret, frame = cap.read()
                if not ret:
                    if reopen_on_end:
                        cap.release()
                        time.sleep(OPEN_RETRY_DELAY)
                        cap = cv2.VideoCapture(self.source)
                        if not cap.isOpened():
                            logger.error("Источник перестал открываться — останавливаюсь")
                            break
                        logger.info(
                            "Файл закончился — начинаю заново (live-симуляция)" if is_file
                            else "Чтение HLS прервалось — реконнект"
                        )
                        continue
                    logger.info(
                        "Источник закончился. Кадров прочитано: %d, отправлено: %d",
                        frame_index, sent_count,
                    )
                    break

                if is_file:
                    now = time.monotonic()
                    if next_read > now:
                        time.sleep(next_read - now)
                    next_read = time.monotonic() + frame_period

                if frame_index % frame_interval == 0:
                    _, buffer = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 80])
                    frame_b64 = base64.b64encode(buffer).decode()

                    message = json.dumps({
                        "frame_data": frame_b64,
                        "frame_index": frame_index,
                        "source": self.source,
                        "stream_id": self.stream_id,
                    }).encode()

                    self.producer.produce(
                        TOPIC_RAW_FRAMES,
                        value=message,
                        callback=self._delivery_report,
                    )
                    self.producer.poll(0)

                    sent_count += 1
                    # Не спамим в общий лог api: каждый кадр — DEBUG,
                    # прогресс раз в 25 кадров (и первый) — INFO
                    if sent_count == 1 or sent_count % 25 == 0:
                        logger.info("Отправлено кадров: %d (frame_index=%d)", sent_count, frame_index)
                    else:
                        logger.debug("Отправлен кадр #%d", frame_index)

                frame_index += 1

        except KeyboardInterrupt:
            logger.info("Остановлено вручную. Отправлено кадров: %d", sent_count)
        finally:
            cap.release()
            self.producer.flush()


if __name__ == "__main__":
    import sys

    # basicConfig нужен: это отдельный процесс, без него logging по умолчанию
    # печатает только WARNING+. Логи видны через `docker compose logs stream-manager` —
    # сюда же попадают падения producer'а (раньше они глотались DEVNULL).
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    source = sys.argv[1] if len(sys.argv) > 1 else "test_video.mp4"
    fps = int(sys.argv[2]) if len(sys.argv) > 2 else 2
    stream_id = int(sys.argv[3]) if len(sys.argv) > 3 else None

    producer = FrameProducer(source, fps=fps, stream_id=stream_id)
    try:
        producer.run()
    except ConnectionError:
        # Источник не открылся вообще — это не «рантайм-сбой», а битый/недоступный
        # адрес. Отдельный код выхода: reconciler по нему помечает стрим error,
        # даже если попытки открытия длились дольше порога здоровья.
        logger.exception("Источник не открылся (source=%r, stream_id=%s)", source, stream_id)
        raise SystemExit(EXIT_OPEN_FAILED)
    except Exception:
        logger.exception("Producer упал (source=%r, stream_id=%s)", source, stream_id)
        raise SystemExit(1)
