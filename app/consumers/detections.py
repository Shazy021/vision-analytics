"""Kafka Consumer — читает детекции и сохраняет в БД

БАТЧЕВАЯ ЗАПИСЬ (почему потребитель устроен именно так):
построчный режим (poll → SessionLocal → ORM-объекты → commit на каждое
сообщение) уперся в количество транзакций: под 100+ fps writer отстаёт от
yolo-consumer, и лаг копится в топике detections. Теперь:
  1. динамический сбор батча (та же семантика, что у yolo-consumer: окно
     grace ограничивает ожидание ПЕРВОГО сообщения, дальше — пока очередь
     горячая);
  2. bulk_insert_mappings: один INSERT на батч, без ORM-объектов в памяти;
  3. ОДИН commit на батч;
  4. ручной коммит offset'ов Kafka ТОЛЬКО после записи (at-least-once):
     last_by_partition строится из ВСЕХ сообщений батча — включая ядовитые
     и skipped, иначе они ределиверились бы вечно.
IntegrityError (стрим удалили, пока его кадры шли по топикам) рвёт весь
батч — деградация до построчного _save_to_db: сирота изолируется,
здоровые кадры проходят. Построчный путь остался и как тестируемая
единица (см. тесты).
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

# Горячая очередь: poll вернул сообщение почти мгновенно — оно было в
# локальном кэше librdkafka, бэклог есть, продолжаем собирать батч.
HOT_POLL_SECONDS = 0.005


def _parse_captured_at(raw: str | None) -> datetime | None:
    """captured_at из сообщения → naive UTC (тот же стиль, что у now() в колонке).

    None при отсутствии/битом значении — тогда detected_at поставит
    server_default (now()), как было до появления captured_at: старые
    сообщения в топике не должны ломать запись.
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

    # Как часто предупреждать о каждом «осиротевшем» stream_id: кадры
    # удалённого стрима могут идти сотнями, лог не должен тонуть в них.
    _ORPHAN_WARN_INTERVAL_S = 60.0

    def __init__(self, *, batch_max: int | None = None, grace_ms: int | None = None):
        # Параметры батчинга: аргументы (тесты) > settings (env) > дефолты.
        self.batch_max = batch_max or settings.detection_batch_size
        self.grace_ms = grace_ms if grace_ms is not None else settings.detection_batch_grace_ms
        self.consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "detection-writer",
            "auto.offset.reset": "earliest",
            # Ручные коммиты: offset фиксируем только после записи батча
            # (at-least-once). auto.offset.store выключен — иначе confluent
            # бросит _INVALID_ARG на store_offsets(message=...).
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        })
        self.consumer.subscribe([TOPIC_DETECTIONS])
        self._last_orphan_warn: dict[int, float] = {}

    def _collect_batch(self) -> dict[int, list]:
        """Собрать сообщения в батч по партициям (ДИНАМИЧЕСКИЙ батч).

        Окно grace ограничивает ожидание ПЕРВОГО сообщения; дальше батч
        расширяется, только пока очередь горячая (poll отдаёт из локального
        кэша почти мгновенно). Под бэклогом набирается за миллисекунды,
        при низкой нагрузке одиночное сообщение уходит без ожидания.
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
        """Один батч: парсинг → bulk insert → commit → offset'ы Kafka.

        Метрики frame_written_total считаются ОДИН раз, когда известен
        исход: skipped/poison — на парсинге (они не пишутся), ok/missing_ts
        — после успешного коммита, error — при падении записи.
        """
        started = time.perf_counter()
        msgs = [m for p in sorted(parts) for m in parts[p]]
        # ВСЕ сообщения батча участвуют в коммите — включая ядовитые и
        # skipped: их offset тоже должен двигаться, иначе вечный ределивер.
        last_by_partition = {p: parts[p][-1] for p in parts}

        mappings: list[dict] = []
        # Сообщения, прошедшие парсинг И проверку stream_id: именно они
        # попадают в деградацию при IntegrityError (по ним метрики ещё не
        # начислены — яд/skipped учтены отдельно и повторно не пишутся).
        clean_payloads: list[dict] = []
        # По одному флагу на сообщение с stream_id: True — captured_at
        # валиден (res=ok), False — missing_ts. Индекс соответствует
        # порядку в msgs (для сообщений без stream_id флага нет).
        frame_has_ts: list[bool] = []
        e2e_samples: list[tuple[str, float]] = []  # (stream_id, captured_epoch)

        for msg in msgs:
            wait = queue_wait_seconds(msg)
            if wait is not None:
                frame_queue_wait_seconds.labels(topic=TOPIC_DETECTIONS).observe(wait)

            try:
                detection = json.loads(msg.value().decode())
            except (UnicodeDecodeError, json.JSONDecodeError):
                # Яд: логируем, считаем, идём дальше — батч не роняем.
                logger.exception("Poison detection message skipped (offset=%s)", msg.offset())
                frame_written_total.labels(result="error").inc()
                continue

            stream_id = detection.get("stream_id")
            if stream_id is None:
                # Топик detections — исключительно стримовый (видео пишут в
                # БД напрямую из video-consumer), stream_id обязателен.
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
                # Явный None перезаписал бы server_default (now()) на NULL
                # и получил бы NOT NULL violation — поэтому поле добавляем
                # только когда время кадра действительно есть.
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
                # Стрим удалили, пока его кадры шли по топикам (raw_frames →
                # yolo → detections): FK на streams.id рвётся, и падает ВЕСЬ
                # батч. Деградация до построчного _save_to_db: сирота
                # изолируется, здоровые кадры проходят.
                db.rollback()
                logger.warning(
                    "IntegrityError in batch (%d rows) — degrading to per-message",
                    len(mappings),
                )
                # Только «чистые» сообщения: яд и skipped уже посчитаны на
                # парсинге, повторный прогон удвоил бы их метрики.
                for detection in clean_payloads:
                    self._save_to_db(detection)
            except Exception:
                db.rollback()
                logger.exception("Error saving batch to DB")
                # Ни одна строка не записана — все кадры батча: error.
                frame_written_total.labels(result="error").inc(len(frame_has_ts))
            else:
                # Метрики ТОЛЬКО после успешного коммита. e2e = захват →
                # КОММИТ, поэтому часы берём сейчас, а не в момент парсинга:
                # время самой транзакции входит в задержку.
                now = time.time()
                for stream_id, captured_epoch in e2e_samples:
                    latency = now - captured_epoch
                    if latency >= 0:
                        frame_e2e_latency_seconds.labels(stream_id=stream_id).observe(latency)
                for has_ts in frame_has_ts:
                    frame_written_total.labels(result="ok" if has_ts else "missing_ts").inc()
            finally:
                db.close()

        # Фиксация offset'ов: после записи (или решения пропустить).
        # KafkaException = партиция уехала к другому участнику (rebalance):
        # коммитит её новый владелец, дубли — цена at-least-once.
        try:
            for p in sorted(last_by_partition):
                self.consumer.store_offsets(message=last_by_partition[p])
            self.consumer.commit(asynchronous=False)
        except KafkaException as exc:
            logger.warning("Commit skipped (partition rebalanced?): %s", exc)

        frame_write_duration_seconds.observe(time.perf_counter() - started)
        logger.debug("Batch: %d сообщений, %d строк", len(msgs), len(mappings))

    def _save_to_db(self, detection: dict) -> str:
        """Построчная запись (путь деградации батча); вернуть итог для метрик.

        Результат — значение лейбла frame_written_total:
        ok (записан, captured_at валиден) | missing_ts (записан, но e2e
        не измерить) | skipped (нет stream_id) | orphan (стрим удалён,
        кадр выброшен) | error (БД отвалилась).
        """
        started = time.perf_counter()
        stream_id = detection.get("stream_id")
        if stream_id is None:
            logger.warning("Skipping detection without stream_id: %s", detection)
            frame_written_total.labels(result="skipped").inc()
            return "skipped"

        # Отдельно от _parse_captured_at (там naive datetime для колонки):
        # epoch нужен для e2e-задержки; битое/отсутствующее значение — None,
        # e2e не меряем, но пишем как раньше (fallback на now()).
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
            # Стрим удалили, пока его кадры ещё шли по топикам: FK рвётся.
            # Такой кадр бесполезен (стрима больше нет) — выбрасываем, а не
            # крутим сообщение в цикле ошибок.
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
                # e2e: захват кадра → коммит. Отрицательное значение — перекос
                # часов producer'а, мусор в гистограмме портит квантили.
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
    # Порт до коннекта к Kafka: /metrics должен жить даже в ожидании брокера
    start_metrics_server(9092)
    consumer = DetectionConsumer()
    consumer.run()
