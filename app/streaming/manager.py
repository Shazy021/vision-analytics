"""stream_manager — единственный владелец процессов producer'а.

Паттерн desired-state + reconciler (та же модель, что у контроллеров
Kubernetes и у systemd/Docker restart-политик):

  - API пишет в БД только НАМЕРЕНИЕ (Stream.desired_status) и ничего не
    порождает (см. app/api/routes/streams.py);
  - этот сервис крутит бесконечный reconcile loop: читает desired из БД,
    сравнивает со своими живыми дочерними процессами и сближает факт с
    желанием — недостающих поднимает, лишних убивает.

Почему отдельный процесс, а не поток внутри API:
  - владение процессами должно переживать рестарт API и не дублироваться,
    когда API запущен в нескольких экземплярах (один владелец — одна истина);
  - reconcile loop блокирующий (спит между витками) — ему не место в async
    event loop FastAPI.

Что это чинит из «известного ограничения» §4 ARCHITECTURE:
  - рестарт api больше не убивает стримы (процессы живут здесь, не в api);
  - упавший producer поднимается на ближайшем витке цикла;
  - два экземпляра API не дерутся за один стрим (они лишь пишут desired).
"""
import logging
import subprocess
import sys
import time
from dataclasses import dataclass

from sqlalchemy.orm import Session

from app.core.database import SessionLocal
from app.db.models import Stream

logger = logging.getLogger(__name__)

# --- Настройки reconcile loop'а -------------------------------------------
# Как часто сверять desired и fact. Меньше — быстрее подхватываем падения,
# больше — чаще дёргаем БД. 2с: компромисс для учебного проекта.
RECONCILE_INTERVAL = 2.0
# producer, проживший меньше этого времени, считается «упавшим сразу».
# Порог заведомо больше времени старта (импорт torch/cv2, коннект к Kafka),
# но меньше «нормальной» жизни рабочего стрима.
CRASH_WINDOW = 10.0
# Сколько быстрых падений подряд терпим, прежде чем сдаться и пометить error.
# 3 даёт один шанс на транзиентную проблему, но отсекает spawn-шторм на
# заведомо битом источнике (падение каждые <10с → шторм процессоров/CPU).
MAX_FAST_CRASHES = 3


def default_spawn(rtsp_url: str, fps: int, stream_id: int) -> subprocess.Popen:
    """Поднять producer'а как дочерний процесс.

    sys.executable — тот же интерпретатор, что крутит manager (в контейнере
    это /usr/local/bin/python), а не «python» из PATH хоста.
    stdout/stderr НЕ перенаправляем: диагностика producer'а (ретраи RTSP,
    трейсбеки) попадает в лог контейнера stream_manager — там, где теперь
    живёт владелец процесса, а не api.
    """
    return subprocess.Popen(
        [
            sys.executable, "-m", "app.streaming.producer",
            rtsp_url, str(fps), str(stream_id),
        ],
    )


@dataclass
class ChildRecord:
    """Процесс producer'а, за которым следит manager, плюс время его старта.

    started_at нужен для backoff: чтобы понять, умер ли процесс «сразу»
    (краш) или отжил своё (штатное завершение), сравниваем uptime с CRASH_WINDOW.
    """
    stream_id: int
    process: subprocess.Popen
    started_at: float


class StreamManager:
    """Reconciler жизненного цикла producer'ов стримов.

    Всё состояние (живые дети, счётчики крашей, множество error) живёт в
    памяти процесса manager'а. Это осознанно: менеджер — единственный и
    долгоживущий владелец процессов. При рестарте менеджера его дочерние
    процессы всё равно умирают (они его дети), а на первом же витке цикла
    всёdesired из БД будет поднят заново — состояние процесса восстанавливается
    из БД, а не из памяти.
    """

    def __init__(
        self,
        spawn=default_spawn,
        session_factory=SessionLocal,
        poll_interval: float = RECONCILE_INTERVAL,
    ):
        # spawn вынесен параметром ради тестов: в тестах подменяем мок-фабрикой,
        # чтобы не плодить реальные subprocess'ы. В проде — default_spawn.
        self.spawn = spawn
        self.session_factory = session_factory
        self.poll_interval = poll_interval
        # stream_id -> живой/мёртвый ChildRecord
        self._children: dict[int, ChildRecord] = {}
        # stream_id -> число быстрых падений подряд (сбрасывается при успехе/stop)
        self._crashes: dict[int, int] = {}
        # stream_id, которым мы «сдались» (error) — их НЕ переподнимаем,
        # пока пользователь не сделает stop -> start (снимет с desired).
        self._errored: set[int] = set()

    # --- примитивы действий ------------------------------------------------
    def _set_status(self, db: Session, stream_id: int, status: str, reason: str | None) -> None:
        """Единственная точка, где manager пишет ФАКТ (status/status_reason).

        db.get вернёт None, если строку уже удалили (DELETE стрима) — тогда
        просто ничего не делаем: писать статус удалённой записи незачем.
        """
        stream = db.get(Stream, stream_id)
        if stream is None:
            return
        stream.status = status
        stream.status_reason = reason

    def _spawn(self, db: Session, stream: Stream) -> None:
        logger.info(
            "stream %s: поднимаю producer (source=%r, fps=%s)",
            stream.id, stream.rtsp_url, stream.fps,
        )
        process = self.spawn(stream.rtsp_url, stream.fps, stream.id)
        self._children[stream.id] = ChildRecord(
            stream_id=stream.id,
            process=process,
            started_at=time.monotonic(),
        )
        # Логируем pid: это точка, где процесс вообще рождается, — по pid удобно
        # вручную убить producer'а в проверке «kill → reconciler поднимает».
        logger.info(
            "stream %s: producer поднят (pid=%s)",
            stream.id, getattr(process, "pid", "?"),
        )
        self._set_status(db, stream.id, "active", None)

    def _stop_child(self, stream_id: int) -> None:
        """Завершить дочерний процесс (если он есть) и забыть его."""
        child = self._children.pop(stream_id, None)
        if child is None:
            return
        logger.info(
            "stream %s: завершаю producer (pid=%s)",
            stream_id, getattr(child.process, "pid", "?"),
        )
        try:
            child.process.terminate()
        except Exception:
            # Процесс мог уже умереть между poll() и terminate() — это не беда.
            logger.exception("stream %s: terminate() бросил (возможно, уже мёртв)", stream_id)

    def _on_child_died(self, db: Session, stream: Stream, child: ChildRecord) -> bool:
        """Обработать обнаруженную смерть ребёнка.

        Возвращает True, если мы СДАЛИСЬ (пометили error) и поднимать нельзя,
        и False, если можно переподнимать.
        """
        sid = stream.id
        uptime = time.monotonic() - child.started_at
        # Мёртвый ребёнок больше не «живой» — убираем из трекинга в любом случае,
        # чтобы следующий виток не считал его живым и мог пересоздать.
        self._children.pop(sid, None)

        if uptime >= CRASH_WINDOW:
            # Прожил «достаточно» — это штатное завершение (например, камера
            # отвалилась через час), а не spawn-шторм. Обнуляем счётчик и
            # переподнимаем.
            self._crashes[sid] = 0
            logger.info(
                "stream %s: producer отжил %.0fс (>= окна %.0fс) — переподнимаю",
                sid, uptime, CRASH_WINDOW,
            )
            return False

        self._crashes[sid] = self._crashes.get(sid, 0) + 1
        n = self._crashes[sid]

        if n >= MAX_FAST_CRASHES:
            reason = (
                f"producer падал {n} раз(а) менее чем за {CRASH_WINDOW:.0f}с после "
                f"старта — слишком частые рестарты (проверь RTSP URL/источник). "
                f"Дальнейшие попытки остановлены; нажми стоп, затем старт, чтобы повторить."
            )
            logger.error("stream %s: %s", sid, reason)
            self._errored.add(sid)
            self._set_status(db, sid, "error", reason)
            return True

        logger.warning(
            "stream %s: producer умер через %.0fс (< окна %.0fс) — рестарт %d/%d",
            sid, uptime, CRASH_WINDOW, n, MAX_FAST_CRASHES,
        )
        return False

    # --- виток сверки ------------------------------------------------------
    def reconcile_once(self, db: Session) -> None:
        """Один виток reconcile: сблизить факт (наши процессы) с желанием (БД).

        Идемпотентен: живые нужные стримы не трогает, поэтому можно звать
        хоть каждую секунду. Отдельный метод (не инлайн в while) — чтобы
        его можно было протестировать без реального subprocess и без цикла.
        """
        streams = db.query(Stream).all()
        by_id = {s.id: s for s in streams}
        desired = {s.id for s in streams if s.desired_status == "active"}

        # 1) Убираем всё, что больше НЕ desired: лишние процессы + память об
        #    ошибках/крашах. Обходим и детей, и errored (у errored процесса
        #    уже нет, но состояние сбрасывать надо).
        for sid in set(self._children) | set(self._errored):
            if sid in desired:
                continue
            self._stop_child(sid)
            self._errored.discard(sid)
            self._crashes.pop(sid, None)
            # статус inactive — только если строка ещё жива (не удалена);
            # удалённый стрим просто перестаёт существовать, писать некому.
            self._set_status(db, sid, "inactive", None)

        # 2) Поднимаем недостающих и лечим умерших.
        for sid in desired:
            if sid in self._errored:
                continue  # сдались — ждём явного stop->start
            stream = by_id[sid]
            child = self._children.get(sid)
            if child is not None and child.process.poll() is None:
                continue  # жив — ничего не делаем (идемпотентность)
            if child is not None:
                # был и умер — решить, поднимать ли снова
                if self._on_child_died(db, stream, child):
                    continue  # пометили error — не поднимаем
            self._spawn(db, stream)

        db.commit()

    def run_forever(self) -> None:
        """Бесконечный reconcile loop. Ошибки одного витка не роняют сервис."""
        logger.info(
            "stream_manager запущен (interval=%.1fs, crash_window=%.0fs, max_fast_crashes=%d)",
            self.poll_interval, CRASH_WINDOW, MAX_FAST_CRASHES,
        )
        while True:
            try:
                with self.session_factory() as db:
                    self.reconcile_once(db)
            except Exception:
                # Не выходим: БД могла отвалиться на секунду (старт/рестарт
                # postgres). Проглотируем виток и попробуем через интервал.
                logger.exception("reconcile iteration failed — продолжу после паузы")
            time.sleep(self.poll_interval)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    StreamManager().run_forever()
