"""Kafka Producer: читает источник (RTSP / HLS / файл) и шлёт кадры в raw_frames.

Запускается отдельным процессом (subprocess) сервиса stream-manager, поэтому:
- логи видны в `docker compose logs stream-manager`;
- адрес источника должен открываться ИЗ КОНТЕЙНЕРА stream-manager:
  * встроенный MediaMTX:          rtsp://mediamtx:8554/<path>
  * источник на хосте (Windows):  rtsp://host.docker.internal:<port>/<path>
  * localhost внутри контейнера — это сам контейнер, а НЕ машина хоста.
"""
import logging
import os
import time
from datetime import datetime, timezone

import cv2
from confluent_kafka import Producer

from app.core.frame_envelope import pack
from app.core.topics import KAFKA_BOOTSTRAP_SERVERS, TOPIC_RAW_FRAMES

logger = logging.getLogger(__name__)

# MediaMTX принимает RTSP только по TCP, а OpenCV по умолчанию просит UDP —
# в Docker/WSL2 UDP-пакеты теряются и источник не открывается вовсе.
# Поэтому для RTSP форсим TCP. setdefault — чтобы значение можно было
# переопределить снаружи (compose/.env), если понадобится.
RTSP_CAPTURE_ENV = "OPENCV_FFMPEG_CAPTURE_OPTIONS"
RTSP_CAPTURE_OPTS = "rtsp_transport;tcp"

# HLS (.m3u8) — живой поток: кадры приходят в реальном темпе, как с камеры.
# Многие публичные HLS-плейлисты (веб-камеры) отбивают дефолтный юзер-агент
# FFmpeg (403/пусто), поэтому для http(s) притворяемся браузером.
HTTP_UA_OPTS = (
    "user_agent;Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0 Safari/537.36"
)

OPEN_ATTEMPTS = 3      # сколько раз пробуем открыть источник
OPEN_RETRY_DELAY = 3   # пауза между попытками, сек

# Отдельный код выхода для «источник вообще не открылся». Нужен, чтобы
# stream-manager различал «адрес битый» (не надо перезапускать вечно) и
# «упал в рантайме» (надо), не гадая по таймингам.
# Значение согласовано с EXIT_OPEN_FAILED в app/streams/manager.py.
EXIT_OPEN_FAILED = 42


def _is_rtsp(source: object) -> bool:
    return str(source).lower().startswith("rtsp://")


def _is_http(source: object) -> bool:
    return str(source).lower().startswith(("http://", "https://"))


def _is_hls(source: object) -> bool:
    """HLS (.m3u8) — живой поток. А mp4 по http — это «файл по сети»:
    читается на всю скорость, темп приходится подделывать, как у файла.
    Отличаем по расширению плейлиста."""
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

        frame_index — ключ кадра в MinIO (streams/{id}/{idx}.jpg), и превью
        на дашборде берёт самый большой ключ. Если после каждого рестарта
        начинать с нуля, новые кадры будут перезаписывать старые файлы,
        максимум залипнет навечно — и превью «стоит на одном кадре», хотя
        детекции в базу капают.

        Стартуем с max(frame_index) из БД: в MinIO попадают только кадры
        с детекциями, а в БД есть запись про каждый такой кадр — значит
        максимум из БД не меньше любого существующего ключа.
        """
        if self.stream_id is None:
            return 0
        try:
            from sqlalchemy import func

            from app.core.database import SessionLocal
            from app.db.models import StreamDetection

            with SessionLocal() as db:
                max_idx = (
                    db.query(func.max(StreamDetection.frame_index))
                    .filter(StreamDetection.stream_id == self.stream_id)
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

        # Файл в роли стрима зацикливаем: стрим по определению бесконечен,
        # а одиночный проход файла «убил» бы стрим через N секунд — это
        # выглядит как поломка. Для HLS тот же путь служит реконректом:
        # сетевой сбой выглядит как конец файла, переподключиться на месте
        # дешевле, чем упасть (3 падения подряд помечают стрим error).
        is_rtsp = _is_rtsp(self.source)
        is_hls = _is_hls(self.source)
        reopen_on_end = not is_rtsp and self.stream_id is not None

        src_fps = cap.get(cv2.CAP_PROP_FPS) or 30
        target_period = 1.0 / self.fps
        # Сквозной номер кадра (ключ в MinIO); продолжается с прошлого
        # максимума — см. _initial_frame_index().
        frame_index = self._initial_frame_index()
        sent_count = 0

        # Файл читаем с поддельным темпом: RTSP и HLS прибывают с камеры
        # сами, а файл открывается «быстрее некуда» — без пауз он завалил
        # бы Kafka со скоростью диска вместо заявленных fps.
        is_file = not is_rtsp and not is_hls
        frame_period = 1.0 / src_fps
        next_read = time.monotonic()

        # Отбор кадров по НАСТОЯЩЕМУ ВРЕМЕНИ, а не целым шагом по номеру
        # кадра. Раньше брали каждый int(src_fps/fps)-й кадр: на источнике
        # 25 fps при fps=10 это int(2.5)=2 → каждый 2-й → 12.5 fps вместо
        # заказанных 10 — целочисленный skip умеет только делители частоты
        # источника. Теперь кадр уходит, когда с прошлой отправки прошло
        # >= 1/fps: частота держится на любом src_fps. Если источник сам
        # медленнее заказанного fps, шлём каждый кадр — быстрее источника
        # не выжать.
        next_send = time.monotonic()

        logger.info(
            "Streaming → Kafka '%s' at %d fps (source=%s, stream_id=%s%s)",
            TOPIC_RAW_FRAMES, self.fps, self.source, self.stream_id,
            ", loop" if reopen_on_end else "",
        )

        try:
            while True:
                # grab() читает кадр, но не делает цветовой перевод и копию
                # в Mat — их (retrieve) делаем только для кадра, который
                # реально уходит в Kafka. Сам H.264-декод идёт на КАЖДОМ
                # кадре источника: P/B-кадры опираются на предыдущие, GOP
                # нельзя раскодировать «с пропуском». Поэтому экономия тут
                # на постобработке выброшенных кадров, а не на декоде.
                grabbed = cap.grab()
                if not grabbed:
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

                now = time.monotonic()
                if now >= next_send:
                    # План следующей отправки; если отстали (источник встал
                    # или просадка часов) — не догоняем пачкой, а
                    # пересинхронизируемся на «сейчас», иначе после stalls
                    # частота на время улетела бы выше заказа.
                    next_send += target_period
                    if next_send < now:
                        next_send = now

                    ret, frame = cap.retrieve()
                    if not ret:
                        logger.warning("Кадр не раскодировался — пропущен (frame_index=%d)", frame_index)
                        frame_index += 1
                        continue

                    ok, buffer = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 80])
                    if not ok:
                        logger.warning("Кадр не закодировался в JPEG — пропущен (frame_index=%d)", frame_index)
                        frame_index += 1
                        continue

                    message = pack({
                        "frame_index": frame_index,
                        "source": self.source,
                        "stream_id": self.stream_id,
                        # Размер кадра нужен yolo-консьюмеру в jpeg-режиме:
                        # обратный ход боксов (scale/pad) считается из него, а
                        # декодировать кадр ради двух чисел — смысл потерянного
                        # пути. В cv2-режиме поля просто не читаются.
                        "width": frame.shape[1],
                        "height": frame.shape[0],
                        # Время захвата кадра. detected_at в БД берётся отсюда,
                        # а не из момента записи: если консьюмер отставал, старые
                        # кадры останутся старыми, и график активности стрима
                        # покажет правду, а не «всё произошло сейчас».
                        "captured_at": datetime.now(timezone.utc).isoformat(),
                    }, buffer.tobytes())

                    # Ключ не ставим намеренно: без него Kafka раздаёт сообщения
                    # по партициям по кругу — нагрузка ровная, ни один стрим не
                    # отстаёт от других. Порядок внутри стрима нигде не нужен:
                    # в detections есть frame_index/captured_at, файл в MinIO
                    # ищется по frame_index, а yolo-consumer считает границу
                    # обработки по «должникам», а не по порядку кадров.
                    self.producer.produce(
                        TOPIC_RAW_FRAMES,
                        value=message,
                        callback=self._delivery_report,
                    )
                    self.producer.poll(0)

                    sent_count += 1
                    # Не засоряем общий лог: каждый кадр — DEBUG, первый и
                    # каждый 25-й — INFO, чтобы прогресс был виден глазами.
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

    # Это отдельный процесс: без basicConfig logging печатает только WARNING+,
    # и падение producer'а осталось бы невидимым. Логи видны через
    # `docker compose logs stream-manager`.
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
        # Источник не открылся ни разу — это битый адрес, а не сбой в работе.
        # Отдельный код выхода: stream-manager по нему сразу помечает стрим
        # error, не дожидаясь, пока долгие попытки открытия «замаскируют» его
        # под живой процесс.
        logger.exception("Источник не открылся (source=%r, stream_id=%s)", source, stream_id)
        raise SystemExit(EXIT_OPEN_FAILED)
    except Exception:
        logger.exception("Producer упал (source=%r, stream_id=%s)", source, stream_id)
        raise SystemExit(1)
