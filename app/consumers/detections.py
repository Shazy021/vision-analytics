"""Kafka Consumer: читает детекции из топика detections и пишет в PostgreSQL.

БАТЧЕВАЯ ЗАПИСЬ (почему именно так). Первый вариант писал по одному
сообщению: poll → объект ORM → commit. Под 100+ fps писатель не успевал за
yolo-consumer — слишком много транзакций, и очередь в detections росла.
Теперь на один батч:
  1. собираем сообщения динамически (как в yolo-consumer: ждём первое до
     grace_ms, дальше берём пока очередь «горячая»);
  2. bulk_insert_mappings — один INSERT на батч, без создания ORM-объектов;
  3. один commit в БД;
  4. границу «обработано до N» в Kafka сдвигаем только ПОСЛЕ записи:
     в неё входят ВСЕ сообщения батча, даже битые и пропущенные — иначе
     их бы приносило заново бесконечно.

Особый случай: стрим удалили из БД, а его кадры уже ехали по топикам.
Одна такая строка рвёт FOREIGN KEY и откатывает ВЕСЬ батч — поэтому при
IntegrityError переписываем батч по одному сообщению: «сирота» отбраковывается,
остальные строки записываются.
"""
import json
import logging
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaException
from sqlalchemy.exc import IntegrityError

from app.core.config import settings
from app.core.database import SessionLocal
from app.core.metrics import (
    frame_e2e_latency_seconds,
    frame_queue_wait_seconds,
    frame_write_duration_seconds,
    frame_written_total,
    parse_epoch_seconds,
    queue_wait_seconds,
    start_metrics_server,
)
from app.core.topics import KAFKA_BOOTSTRAP_SERVERS, TOPIC_DETECTIONS
from app.db.models import StreamDetection

# Настройка логгера (после импортов — E402)
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# «Горячая» очередь: poll вернул сообщение быстрее 5 мс — значит оно лежало
# в локальном кэше librdkafka, то есть бэклог есть и батч стоит наращивать.
# Медленнее — очередь опустела, батч пора закрывать.
HOT_POLL_SECONDS = 0.005


def _parse_captured_at(raw: str | None) -> datetime | None:
    """captured_at из сообщения → datetime без таймзоны (в UTC, как now() в колонке).

    None (нет поля / битое) — тогда detected_at подставит база сама (now()),
    как было до появления captured_at: старые сообщения не должны ломать запись.
    """
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        logger.warning("Некорректный captured_at %r — detected_at возьмём из now()", raw)
        return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


class DetectionConsumer:
    """Читает детекции из Kafka и пишет в PostgreSQL батчами"""

    # Кадры удалённого стрима могут идти сотнями — не тонем в логе:
    # предупреждаем об одном осиротевшем stream_id не чаще раза в минуту.
    _ORPHAN_WARN_INTERVAL_S = 60.0

    def __init__(self, *, batch_max: int | None = None, grace_ms: int | None = None):
        # Параметры батчинга: аргументы (тесты) > settings (env) > дефолты.
        self.batch_max = batch_max or settings.detection_batch_size
        self.grace_ms = grace_ms if grace_ms is not None else settings.detection_batch_grace_ms
        self.consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "detection-writer",
            "auto.offset.reset": "earliest",
            # Границу «обработано до N» двигаем сами, только после записи
            # батча в БД. Второе переключение — техническое требование
            # confluent-kafka: при ручном store_offsets автозапись должна
            # быть выключена, иначе библиотека бросит _INVALID_ARG.
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        })
        self.consumer.subscribe([TOPIC_DETECTIONS])
        self._last_orphan_warn: dict[int, float] = {}

    def _collect_batch(self) -> dict[int, list]:
        """Собрать сообщения в батч, раскладывая их по партициям.

        Две фазы, как в yolo-consumer: ждём ПЕРВОЕ сообщение до grace_ms,
        дальше наращиваем батч, пока poll отвечает мгновенно (очередь
        горячая). Под нагрузкой батч набирается за миллисекунды, при
        низкой нагрузке одиночное сообщение не ждёт соседей.
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
            if time.monotonic() - t0 >= HOT_POLL_SECONDS:
                break
        return parts

    def _write_batch(self, parts: dict[int, list]) -> None:
        """Один батч от начала до конца: разбор → INSERT → commit в БД → граница Kafka.

        Метрики frame_written_total считаем ОДИН раз, когда известен итог:
        error/skipped — ещё на разборе (эти строки не пишутся), ok/missing_ts
        — после успешного commit, error — если запись упала.
        """
        started = time.perf_counter()
        msgs = [m for p in sorted(parts) for m in parts[p]]
        # В границу «обработано до N» входят ВСЕ сообщения батча, даже битые
        # и пропущенные: их тоже не надо приносить заново бесконечно.
        last_by_partition = {p: parts[p][-1] for p in parts}

        mappings: list[dict] = []
        # Сообщения, которые прошли разбор и проверку stream_id — именно они
        # пойдут в построчную перезапись при IntegrityError (их метрики ещё
        # не посчитаны; битые и пропущенные учтены отдельно, повторно их
        # считать нельзя — будет двойка).
        clean_payloads: list[dict] = []
        # По одному флагу на сообщение с stream_id: есть ли валидный
        # captured_at (для метрики ok vs missing_ts). Порядок = порядок в msgs.
        frame_has_ts: list[bool] = []
        e2e_samples: list[tuple[str, float]] = []  # (stream_id, время захвата)

        for msg in msgs:
            wait = queue_wait_seconds(msg)
            if wait is not None:
                frame_queue_wait_seconds.labels(topic=TOPIC_DETECTIONS).observe(wait)

            try:
                detection = json.loads(msg.value().decode())
            except (UnicodeDecodeError, json.JSONDecodeError):
                # Битое сообщение: записываем в лог и счётчик, идём дальше —
                # из-за одного мусора весь батч ронять нельзя.
                logger.exception("Poison detection message skipped (offset=%s)", msg.offset())
                frame_written_total.labels(result="error").inc()
                continue

            stream_id = detection.get("stream_id")
            if stream_id is None:
                # detections — только стримовый топик (загруженные видео
                # пишутся в БД напрямую из video-consumer), stream_id обязателен.
                logger.warning("Skipping detection without stream_id: %s", detection)
                frame_written_total.labels(result="skipped").inc()
                continue

            clean_payloads.append(detection)
            captured_epoch = parse_epoch_seconds(detection.get("captured_at"))
            captured_at = _parse_captured_at(detection.get("captured_at"))
            for obj in detection.get("objects", []):
                fields = {
                    "stream_id": stream_id,
                    "frame_index": detection["frame_index"],
                    "class_name": obj["class"],
                    "confidence": obj["confidence"],
                    "bbox_x1": obj["bbox"]["x1"],
                    "bbox_y1": obj["bbox"]["y1"],
                    "bbox_x2": obj["bbox"]["x2"],
                    "bbox_y2": obj["bbox"]["y2"],
                }
                # Временем захвата кадра делимся аккуратно: явный None убил бы
                # серверный default (now()) и получил бы нарушение NOT NULL —
                # поэтому поле добавляем только когда время реально есть.
                if captured_at is not None:
                    fields["detected_at"] = captured_at
                mappings.append(fields)
            frame_has_ts.append(captured_epoch is not None)
            if captured_epoch is not None:
                e2e_samples.append((str(stream_id), captured_epoch))

        if mappings:
            db = SessionLocal()
            try:
                db.bulk_insert_mappings(StreamDetection, mappings)
                db.commit()
            except IntegrityError:
                # Одна из причин: стрим удалили из БД, пока его кадры ехали
                # по топикам. Ссылка на несуществующий стрим откатывает ВЕСЬ
                # батч — переписываем его по одному сообщению, чтобы
                # отбраковать только «сироту», а не хорошие строки.
                db.rollback()
                logger.warning(
                    "IntegrityError in batch (%d rows) — degrading to per-message",
                    len(mappings),
                )
                # Только прошедшие разбор сообщения: битые и пропущенные уже
                # посчитаны на разборе, второй прогон удвоил бы их метрики.
                for detection in clean_payloads:
                    self._save_to_db(detection)
            except Exception:
                db.rollback()
                logger.exception("Error saving batch to DB")
                # Не записана ни одна строка — весь батч в error.
                frame_written_total.labels(result="error").inc(len(frame_has_ts))
            else:
                # Метрики — только после успешного commit. e2e = захват кадра
                # → КОММИТ в БД, поэтому время берём сейчас: сама транзакция
                # тоже входит в задержку, которую видит пользователь.
                now = time.time()
                for stream_id, captured_epoch in e2e_samples:
                    latency = now - captured_epoch
                    if latency >= 0:
                        frame_e2e_latency_seconds.labels(stream_id=stream_id).observe(latency)
                for has_ts in frame_has_ts:
                    frame_written_total.labels(result="ok" if has_ts else "missing_ts").inc()
            finally:
                db.close()

        # Граница «обработано до N» — только теперь, когда строки в БД
        # (или принято решение пропустить). KafkaException = партиция уехала
        # к другому консьюмеру (rebalance): коммитит её новый владелец,
        # а нам достанутся дубли — это нормальная цена гарантии.
        try:
            for p in sorted(last_by_partition):
                self.consumer.store_offsets(message=last_by_partition[p])
            self.consumer.commit(asynchronous=False)
        except KafkaException as exc:
            logger.warning("Commit skipped (partition rebalanced?): %s", exc)

        frame_write_duration_seconds.observe(time.perf_counter() - started)
        logger.debug("Batch: %d сообщений, %d строк", len(msgs), len(mappings))

    def _save_to_db(self, detection: dict) -> str:
        """Запись одного сообщения (запасной путь, когда батч упал целиком).

        Возвращает итог для метрик — одно из значений лейбла frame_written_total:
        ok (записан, время кадра есть) | missing_ts (записан, но e2e не
        померить) | skipped (нет stream_id) | orphan (стрим удалён, кадр не
        нужен) | error (база отвалилась).
        """
        started = time.perf_counter()
        stream_id = detection.get("stream_id")
        if stream_id is None:
            logger.warning("Skipping detection without stream_id: %s", detection)
            frame_written_total.labels(result="skipped").inc()
            return "skipped"

        # Здесь нужен именно epoch-секунды (для e2e-задержки), а не datetime
        # из _parse_captured_at — там формат для колонки. Битое/нет — None:
        # e2e не мерим, но пишем как раньше (время подставит база).
        captured_epoch = parse_epoch_seconds(detection.get("captured_at"))

        db = SessionLocal()
        try:
            captured_at = _parse_captured_at(detection.get("captured_at"))
            for obj in detection.get("objects", []):
                fields = dict(
                    stream_id=stream_id,
                    frame_index=detection["frame_index"],
                    class_name=obj["class"],
                    confidence=obj["confidence"],
                    bbox_x1=obj["bbox"]["x1"],
                    bbox_y1=obj["bbox"]["y1"],
                    bbox_x2=obj["bbox"]["x2"],
                    bbox_y2=obj["bbox"]["y2"],
                )
                if captured_at is not None:
                    fields["detected_at"] = captured_at
                db_obj = StreamDetection(**fields)
                db.add(db_obj)
            db.commit()
            logger.debug("Saved %d detections for frame #%s", len(detection.get("objects", [])), detection["frame_index"])
        except IntegrityError:
            # Стрим удалили из БД, а его кадры были в пути. Запись не нужна
            # (стрима больше нет) — выбрасываем кадр, а не крутим его по кругу.
            db.rollback()
            self._warn_orphan(stream_id, detection.get("frame_index"))
            frame_written_total.labels(result="orphan").inc()
            return "orphan"
        except Exception:
            db.rollback()
            logger.exception("Error saving to DB")
            frame_written_total.labels(result="error").inc()
            return "error"
        else:
            result = "ok" if captured_epoch is not None else "missing_ts"
            frame_written_total.labels(result=result).inc()
            if captured_epoch is not None:
                # e2e: от захвата кадра до коммита в БД. Отрицательное значение
                # физически невозможно — это часы producer'а спешат; такое
                # в гистограмму класть нельзя, испортит квантили.
                latency = time.time() - captured_epoch
                if latency >= 0:
                    frame_e2e_latency_seconds.labels(stream_id=str(stream_id)).observe(latency)
            return result
        finally:
            db.close()
            frame_write_duration_seconds.observe(time.perf_counter() - started)

    def _warn_orphan(self, stream_id: int, frame_index: int | None) -> None:
        """Предупредить об осиротевшем стриме, но не чаще раза в минуту на id."""
        now = time.monotonic()
        if now - self._last_orphan_warn.get(stream_id, 0.0) >= self._ORPHAN_WARN_INTERVAL_S:
            self._last_orphan_warn[stream_id] = now
            logger.warning(
                "Выбрасываю детекции удалённого стрима %s (кадр %s): "
                "кадры ушли в топик до удаления стрима",
                stream_id, frame_index,
            )

    def run(self):
        logger.info(
            "Listening to Kafka topic '%s'... (batch=%d, grace=%dms)",
            TOPIC_DETECTIONS, self.batch_max, self.grace_ms,
        )
        try:
            while True:
                parts = self._collect_batch()
                if not parts:
                    continue
                self._write_batch(parts)
        except KeyboardInterrupt:
            logger.info("\nStopping...")
        finally:
            self.consumer.close()


if __name__ == "__main__":
    # Порт метрик поднимаем ДО подключения к Kafka: /metrics должен отвечать,
    # пока консьюмер ждёт брокера (иначе Prometheus считает его мёртвым).
    start_metrics_server(9092)
    consumer = DetectionConsumer()
    consumer.run()
