"""Kafka Consumer: читает детекции из топика detections и пишет в PostgreSQL.

БАТЧЕВАЯ ЗАПИСЬ (почему именно так). Первый вариант писал по одному
сообщению: poll → объект ORM → commit. Под 100+ fps писатель не успевал за
yolo-consumer — слишком много транзакций, и очередь в detections росла.
Теперь на один батч:
  1. собираем сообщения динамически (как в yolo-consumer: ждём первое до
     grace_ms, дальше берём пока очередь «горячая»);
  2. прогоняем через трекер (трекинг стримов, см. app/tracking) — он
    расставляет объектам track_id и порождает события жизненного цикла;
  3. tracks: отдельной транзакцией — три пакетных запроса на весь батч
     (INSERT новых треков, UPDATE затронутых и UPDATE закрытых через
     VALUES), затем bulk_insert_mappings — один INSERT детекций на батч;
  4. один commit в БД на каждую транзакцию;
  5. границу «обработано до N» в Kafka сдвигаем только ПОСЛЕ записи.

ТРЕКИНГ И ПОРЯДОК СООБЩЕНИЙ. Кадр проходит трекер и записывается в этом же
батче — граница «обработано до N» двигается по последнему сообщению каждой
партиции, как до трекинга. Перестановку соседних кадров (разные реплики
yolo-consumer отвечают в порядке готовности) переваривает реестр трекеров:
поздний кадр идёт в KF напрямую, латентность пайплайна не растёт. Дубли
redelivery в трекер не идут, но в БД пишутся как писались — дедуп записей
здесь никогда не было.

Особый случай: стрим удалили из БД, а его кадры уже ехали по топикам.
Одна такая строка рвёт FOREIGN KEY и откатывает ВЕСЬ батч — поэтому при
IntegrityError переписываем батч по одному сообщению: «сирота» отбраковывается,
остальные строки записываются. Треки осиротевшего стрима при этом теряются
(их строки тоже падают по FK) — редкая и короткая деградация, детекции
главное не потерять.
"""
import json
import logging
import time
from datetime import datetime, timedelta, timezone

from confluent_kafka import Consumer, KafkaException
from sqlalchemy import DateTime, Float, Integer, column, update, values
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
    track_active_tracks,
    track_events_total,
)
from app.core.topics import KAFKA_BOOTSTRAP_SERVERS, TOPIC_DETECTIONS
from app.db.models import Stream, StreamDetection, Track
from app.tracking import StreamTrackers, TrackedFrame

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


def _values_alias(rows: list[tuple], schema: tuple[tuple[str, type], ...]):
    """Инлайн-таблица VALUES для одного UPDATE ... FROM (VALUES ...).

    SQLAlchemy собирает её ОДИН раз и отправляет одним запросом с
    параметрами: сотни поштучных UPDATE затронутых треков (главный расход
    старого цикла — по round-trip'у на трек) превращаются в один
    round-trip на батч.
    """
    return values(
        *[column(name, type_) for name, type_ in schema]
    ).data(rows).alias("v")


def _utcnow_naive() -> datetime:
    """Now в той же конвенции, что колонка detected_at (naive UTC)."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


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
    """Читает детекции из Kafka, трекает объекты стримов и пишет в PostgreSQL батчами"""

    # Кадры удалённого стрима могут идти сотнями — не тонем в логе:
    # предупреждаем об одном осиротевшем stream_id не чаще раза в минуту.
    _ORPHAN_WARN_INTERVAL_S = 60.0

    def __init__(self, *, batch_max: int | None = None, grace_ms: int | None = None,
                 trackers: StreamTrackers | None = ...):
        # Параметры батчинга: аргументы (тесты) > settings (env) > дефолты.
        self.batch_max = batch_max or settings.detection_batch_size
        self.grace_ms = grace_ms if grace_ms is not None else settings.detection_batch_grace_ms
        # Трекинг: по умолчанию по флагу settings; тесты могут передать
        # готовый реестр или None (выключить).
        if trackers is ...:
            trackers = self._build_trackers() if settings.tracking_enabled else None
        self.trackers = trackers
        # (stream_id, локальный id трекера) → id строки tracks. Живёт, пока
        # трекер стрима жив: при закрытии трека запись удаляем.
        self._track_ids: dict[tuple[int, int], int] = {}
        # stream_id → streams.fps: нужен один раз на стрим, при создании
        # трекера (перевести физическое время жизни трека в бюджет кадров).
        self._stream_fps: dict[int, int | None] = {}
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

    @staticmethod
    def _build_trackers() -> StreamTrackers:
        return StreamTrackers(
            high_thresh=settings.track_high_thresh,
            low_thresh=settings.track_low_thresh,
            max_age_sec=settings.track_max_age_sec,
            min_hits=settings.track_min_hits,
            default_dt=settings.track_default_dt_sec,
            idle_evict_sec=settings.track_idle_evict_sec,
        )

    def _fps_of(self, stream_id: int) -> int | None:
        """Номинальный fps стрима — один запрос на стрим за жизнь консьюмера.

        Нужен трекеру для бюджета жизни трека (сек × fps). Нет строки /
        нет значения — None, реестр посчитает бюджет по default_dt.

        Первый sighting стрима в этом процессе — заодно момент закрыть его
        активные треки, оставшиеся от прежнего владельца (рестарт этого
        консьюмера или переезд партиции на другую реплику): детекции ключены
        stream_id, значит стрим целиком принадлежит тому, кто видит его
        сообщения, и его «активные» строки из прошлой жизни — сироты.
        Закрывать на старте «все треки сразу» при нескольких репликах нельзя
        — убили бы чужие живые треки."""
        if stream_id in self._stream_fps:
            return self._stream_fps[stream_id]
        fps = None
        db = SessionLocal()
        try:
            fps = db.query(Stream.fps).filter(Stream.id == stream_id).scalar()
            n = db.query(Track).filter(Track.status == "active",
                                       Track.stream_id == stream_id).update(
                {Track.status: "closed", Track.closed_reason: "restart",
                 Track.closed_at: _utcnow_naive()},
                synchronize_session=False,
            )
            db.commit()
            if n:
                logger.info("Стрим %s: закрыто %d треков-сирот прежнего владельца",
                            stream_id, n)
        except Exception:
            db.rollback()
            logger.exception("Не удалось прочитать fps / закрыть сирот стрима %s",
                             stream_id)
        finally:
            db.close()
        self._stream_fps[stream_id] = fps
        return fps

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

    # ------------------------------------------------------------------
    # Трекинг: жизненный цикл треков в БД
    # ------------------------------------------------------------------

    def _close_stale_tracks(self) -> None:
        """Старт консьюмера: закрыть «active» треки молчавших стримов.

        Живые реплики (а их несколько) держат свои треки свежими — их не
        трогаем: сирот стрима закроет тот, кто получит его сообщения
        (_fps_of, первый sighting). Здесь подбираем только протухшие: стрим
        больше ничего не присылал дольше idle_evict, его трекер свернут или
        мёртв, и иначе строки висели бы «активными» до следующего рестарта."""
        db = SessionLocal()
        try:
            cutoff = _utcnow_naive() - timedelta(seconds=settings.track_idle_evict_sec)
            n = db.query(Track).filter(Track.status == "active",
                                       Track.last_seen_at < cutoff).update(
                {Track.status: "closed", Track.closed_reason: "restart",
                 Track.closed_at: _utcnow_naive()},
                synchronize_session=False,
            )
            db.commit()
            if n:
                logger.info("Закрыто %d «активных» треков-сирот (молчат > %.0fс)",
                            n, settings.track_idle_evict_sec)
        except Exception:
            db.rollback()
            logger.exception("Не удалось закрыть треки-сироты — рестарт обойдётся без уборки")
        finally:
            db.close()

    def _apply_track_events(self, frames: list[TrackedFrame]) -> None:
        """Открыть/обновить/закрыть треки одной транзакцией и заменить
        ЛОКАЛЬНЫЕ id трекера в объектах кадров на id строк tracks.

        Три пакетных запроса на весь батч (INSERT новых, UPDATE затронутых,
        UPDATE закрытых) вместо поштучных: раньше каждый затронутый трек
        получал свой UPDATE и каждый открытый — свой flush, по сетевому
        round-trip на трек. На 240 fps это 60–100 round-trip'ов на батч,
        цикл записи раздувался до 0.26с и ограничивал пропускную способность
        ~250 msg/s (бенч 2026-11-12: 98% бюджета e2e — бэклог detections).

        При любой ошибке транзакции (обычно FK осиротевшего стрима) детекции
        этого батча пишутся без track_id — потеря идентификаторов лучше
        потери кадров. Локальные id при этом не запоминаем, чтобы не
        ссылаться на несуществующие строки в следующих батчах.
        """
        if not frames:
            return
        opened = closed = 0
        opened_ids: set[tuple[int, int]] = set()
        new_rows: list[tuple[tuple[int, int], Track]] = []
        # db_id → [last_seen_at, last_frame_index, x1, y1, x2, y2, hits]:
        # трек может быть затронут несколько раз за батч (несколько кадров),
        # берём последние значения и число касаний — ровно то же делает
        # серия поштучных UPDATE, одним запросом.
        touched: dict[int, list] = {}
        # db_id → [closed_at, x1, y1, x2, y2]
        closing: dict[int, list] = {}
        db = SessionLocal()
        ok = True
        try:
            for f in frames:
                ts = _parse_captured_at(f.captured_at) or _utcnow_naive()
                for ev in f.opened:
                    # opened приходит на ПОДТВЕРЖДЕНИИ трека (min_hits
                    # попаданий), поэтому first_seen — кадр подтверждения,
                    # а num_frames сразу столько, сколько трек уже набил.
                    row = Track(
                        stream_id=f.stream_id,
                        class_name=ev.class_name,
                        status="active",
                        first_seen_at=ts, last_seen_at=ts,
                        first_frame_index=f.frame_index, last_frame_index=f.frame_index,
                        num_frames=ev.hits,
                        bbox_x1=ev.bbox[0], bbox_y1=ev.bbox[1],
                        bbox_x2=ev.bbox[2], bbox_y2=ev.bbox[3],
                    )
                    new_rows.append(((f.stream_id, ev.track_id), row))
                    opened_ids.add((f.stream_id, ev.track_id))
                    opened += 1
                for lid, _cls, bbox in f.touched:
                    key = (f.stream_id, lid)
                    db_id = self._track_ids.get(key)
                    if db_id is None or key in opened_ids:
                        continue  # рождён в этом же батче — строка уже свежая
                    agg = touched.get(db_id)
                    if agg is None:
                        touched[db_id] = [ts, f.frame_index, *bbox, 1]
                    else:
                        agg[:6] = [ts, f.frame_index, *bbox]  # последнее касание важнее
                        agg[6] += 1
                for ev in f.closed:
                    db_id = self._track_ids.pop((f.stream_id, ev.track_id), None)
                    if db_id is None:
                        continue
                    # last_seen_at/last_frame_index НЕ трогаем: их держит
                    # в курсе touched на каждом подтверждении. Переписать их
                    # временем закрытия — значит соврать о том, когда объект
                    # видели в последний раз: closed приезжает на max_age
                    # позже (а при evict_idle — через минуты) реального
                    # исчезновения, и «призраки» выглядят свежими.
                    closing[db_id] = [ts, *ev.bbox]
                    closed += 1

            if new_rows:
                # Один INSERT на все строки батча (insertmanyvalues) вместо
                # flush'а на каждый открытый трек; id приходят обратно.
                db.add_all([row for _, row in new_rows])
                db.flush()
                for key, row in new_rows:
                    self._track_ids[key] = row.id
            if touched:
                v = _values_alias(
                    [(db_id, *agg) for db_id, agg in touched.items()],
                    (("id", Integer), ("ts", DateTime), ("fi", Integer),
                     ("x1", Float), ("y1", Float), ("x2", Float), ("y2", Float),
                     ("hits", Integer)),
                )
                db.execute(
                    update(Track)
                    .values(
                        last_seen_at=v.c.ts,
                        last_frame_index=v.c.fi,
                        num_frames=Track.num_frames + v.c.hits,
                        bbox_x1=v.c.x1, bbox_y1=v.c.y1,
                        bbox_x2=v.c.x2, bbox_y2=v.c.y2,
                    )
                    .where(Track.id == v.c.id)
                )
            if closing:
                v = _values_alias(
                    [(db_id, *row) for db_id, row in closing.items()],
                    (("id", Integer), ("ts", DateTime),
                     ("x1", Float), ("y1", Float), ("x2", Float), ("y2", Float)),
                )
                db.execute(
                    update(Track)
                    .values(
                        status="closed", closed_reason="lost",
                        closed_at=v.c.ts,
                        bbox_x1=v.c.x1, bbox_y1=v.c.y1,
                        bbox_x2=v.c.x2, bbox_y2=v.c.y2,
                    )
                    .where(Track.id == v.c.id)
                )
            db.commit()
        except IntegrityError:
            db.rollback()
            ok = False
            logger.warning("IntegrityError в событиях треков — батч пишется без track_id")
        except Exception:
            db.rollback()
            ok = False
            logger.exception("Ошибка записи треков — батч пишется без track_id")
        finally:
            db.close()

        if not ok:
            # Отображения этого батча ненадёжны: убираем их, иначе следующий
            # батч проставит детекциям id несуществующих строк.
            for f in frames:
                for lid in {t for t, *_ in f.touched} | {e.track_id for e in f.opened}:
                    self._track_ids.pop((f.stream_id, lid), None)

        for f in frames:
            for obj in f.objects:
                lid = obj.get("track_id")
                obj["track_id"] = (self._track_ids.get((f.stream_id, lid))
                                   if lid is not None else None)

        if opened:
            track_events_total.labels(event="opened").inc(opened)
        if closed:
            track_events_total.labels(event="closed").inc(closed)

    def _sync_track_gauges(self) -> None:
        if self.trackers is not None:
            track_active_tracks.set(self.trackers.active_total)

    # ------------------------------------------------------------------
    # Основной путь батча
    # ------------------------------------------------------------------

    def _write_batch(self, parts: dict[int, list]) -> None:
        """Один батч от начала до конца: разбор → трекер → треки → INSERT →
        commit в БД → граница Kafka.

        Метрики frame_written_total считаем ОДИН раз, когда известен итог:
        error/skipped — ещё на разборе (эти строки не пишутся), ok/missing_ts
        — после успешного commit, error — если запись упала.
        """
        started = time.perf_counter()
        n_msgs = sum(len(v) for v in parts.values())

        released: list[TrackedFrame] = []
        # Кадры, идущие в обход трекера: дубли redelivery и всё при
        # выключенном трекинге. Пишутся как раньше, без track_id.
        untracked: list[dict] = []
        # В границу «обработано до N» входят ВСЕ сообщения батча, даже битые
        # и пропущенные: их тоже не надо приносить заново бесконечно.
        last_by_partition = {p: parts[p][-1] for p in parts}

        for p in sorted(parts):
            for msg in parts[p]:
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

                if self.trackers is None:
                    untracked.append(detection)
                    continue

                frames, dup = self.trackers.submit(
                    stream_id, detection["frame_index"],
                    detection.get("captured_at"),
                    parse_epoch_seconds(detection.get("captured_at")),
                    detection.get("objects", []),
                    fps=self._fps_of(stream_id),
                )
                if dup:
                    # Redelivery: трекер его не видел (и не должен — дубль
                    # сдвинул бы скорость в KF), но в БД строка пишется как
                    # писалась: дедуп записей здесь никогда не было.
                    untracked.append(detection)
                released.extend(frames)

        if released:
            self._apply_track_events(released)
            self._sync_track_gauges()

        self._store_frames(released, untracked)

        # Граница «обработано до N» — только теперь, когда строки в БД
        # (или принято решение пропустить). Каждый кадр прошёл трекер и
        # записан своим батчем, удержанных кадров нет — коммитим последний
        # message каждой партиции.
        try:
            for p in sorted(last_by_partition):
                self.consumer.store_offsets(message=last_by_partition[p])
            # asynchronous=True: round-trip к брокеру больше не часть цикла
            # батча. Гарантии это не касается — at-least-once держится на
            # порядке «сначала строки в БД, потом коммит», а потерянный
            # коммит стоит только дублями при рестарте. Колбэка об ошибке
            # здесь нет: в confluent-kafka 2.4 у commit() его не бывает
            # (on_commit появился позже), но исход ошибки без него тот же —
            # тишина и дубли, не потеря.
            self.consumer.commit(asynchronous=True)
        except KafkaException as exc:
            logger.warning("Commit skipped (partition rebalanced?): %s", exc)

        frame_write_duration_seconds.observe(time.perf_counter() - started)
        logger.debug("Batch: %d сообщений, %d кадров в трекер, %d в обход",
                     n_msgs, len(released), len(untracked))

    def _flush_buffers(self) -> None:
        """Кафка молчит: сворачиваем трекеры замолчавших стримов (их треки
        закрываются, иначе висели бы «активными» до рестарта)."""
        if self.trackers is None:
            return
        frames = self.trackers.evict_idle()
        if frames:
            self._apply_track_events(frames)
            self._store_frames(frames, [])
        self._sync_track_gauges()

    def _store_frames(self, released: list[TrackedFrame], untracked: list[dict]) -> None:
        """Запись кадров в stream_detections (объект = строка).

        released — кадры из трекера (объекты уже с DB track_id), untracked —
        сырые сообщения в обход трекера. Пустые кадры (без объектов) строк
        не дают и метрик frame_written_total не считают — как раньше.
        """
        payloads: list[dict] = [
            {"stream_id": f.stream_id, "frame_index": f.frame_index,
             "captured_at": f.captured_at, "objects": f.objects}
            for f in released
        ]
        payloads.extend(untracked)
        if not payloads:
            return

        mappings: list[dict] = []
        # Метрика на КАЖДЫЙ кадр: есть ли валидный captured_at
        # (ok vs missing_ts). Порядок = порядок payloads. Пустые кадры
        # строк не пишут, но как обработанные считаются — та же семантика
        # frame_written_total, что была до трекинга.
        frame_has_ts: list[bool] = []
        e2e_samples: list[tuple[str, float]] = []  # (stream_id, время захвата)
        written_payloads: list[dict] = []

        for payload in payloads:
            captured_epoch = parse_epoch_seconds(payload.get("captured_at"))
            captured_at = _parse_captured_at(payload.get("captured_at"))
            for obj in payload.get("objects", []):
                fields = {
                    "stream_id": payload["stream_id"],
                    "frame_index": payload["frame_index"],
                    "class_name": obj["class"],
                    "confidence": obj["confidence"],
                    "bbox_x1": obj["bbox"]["x1"],
                    "bbox_y1": obj["bbox"]["y1"],
                    "bbox_x2": obj["bbox"]["x2"],
                    "bbox_y2": obj["bbox"]["y2"],
                    "track_id": obj.get("track_id"),
                }
                # Временем захвата кадра делимся аккуратно: явный None убил бы
                # серверный default (now()) и получил бы нарушение NOT NULL —
                # поэтому поле добавляем только когда время реально есть.
                if captured_at is not None:
                    fields["detected_at"] = captured_at
                mappings.append(fields)
            written_payloads.append(payload)
            frame_has_ts.append(captured_epoch is not None)
            if captured_epoch is not None:
                e2e_samples.append((str(payload["stream_id"]), captured_epoch))

        if not mappings:
            # Ни одного объекта на всех кадрах — писать нечего, но кадры
            # обработаны: метрика обязана считать и их.
            for has_ts in frame_has_ts:
                frame_written_total.labels(result="ok" if has_ts else "missing_ts").inc()
            return

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
            for payload in written_payloads:
                self._save_to_db(payload)
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
                    track_id=obj.get("track_id"),
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
        if self.trackers is not None:
            self._close_stale_tracks()
        logger.info(
            "Listening to Kafka topic '%s'... (batch=%d, grace=%dms, tracking=%s)",
            TOPIC_DETECTIONS, self.batch_max, self.grace_ms, self.trackers is not None,
        )
        try:
            while True:
                parts = self._collect_batch()
                if not parts:
                    self._flush_buffers()
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
