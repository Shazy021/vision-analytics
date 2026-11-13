"""autovacuum tuning: tracks и stream_detections

Бенч 2026-11-12 (stream-20261113-005615): detection-writer лаг рос на
+1.9 сообщ/с при том, что консьюмеры записывали ровно столько же, сколько
приходило (323.7 против 323.6 msg/s). Разбор: серия лага плоская (~15
сообщений) с всплесками 200–500 по всем партициям сразу — глобальные
остановки записи на 1–5 с. Виновник — autovacuum: за 19 минут прогона
tracks (185 живых строк!) был вычищен/проанализирован 148 раз,
stream_detections — 195. UPDATE tracks на каждом батче мгновенно пробивает
штатный порог (50 + 20% живых = 87), а на медленном диске Docker Desktop
каждый запуск автовакуума — I/O-шторм, в котором встают коммиты всех трёх
реплик.

Пороги поднимаем так, чтобы вакуум приходил раз в минуту-две, а не раз в
несколько секунд:
  • tracks — крошечная, но update-горячая: временный раздув на тысячи
    мёртвых копий — мегабайты, HOT-чистка внутри страниц плюс редкий
    полный вакуум достаточны;
  • stream_detections — insert-only, мёртвые строки только от раскатов
    упавших батчей; главный расход — autoanalyze на миллионах строк,
    его режем сильнее всего.

Revision ID: b4d92c1f7a35
Revises: 7d3a9c5e21f4
Create Date: 2026-11-13 01:30:00.000000

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = 'b4d92c1f7a35'
down_revision: Union[str, None] = '7d3a9c5e21f4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE tracks SET ("
        "autovacuum_vacuum_threshold = 5000,"
        "autovacuum_analyze_threshold = 5000,"
        "autovacuum_vacuum_scale_factor = 0.5,"
        "autovacuum_analyze_scale_factor = 0.5)"
    )
    op.execute(
        "ALTER TABLE stream_detections SET ("
        "autovacuum_vacuum_threshold = 100000,"
        "autovacuum_analyze_threshold = 100000,"
        "autovacuum_vacuum_scale_factor = 0.5,"
        "autovacuum_analyze_scale_factor = 0.5)"
    )


def downgrade() -> None:
    op.execute(
        "ALTER TABLE stream_detections RESET ("
        "autovacuum_vacuum_threshold, autovacuum_analyze_threshold,"
        "autovacuum_vacuum_scale_factor, autovacuum_analyze_scale_factor)"
    )
    op.execute(
        "ALTER TABLE tracks RESET ("
        "autovacuum_vacuum_threshold, autovacuum_analyze_threshold,"
        "autovacuum_vacuum_scale_factor, autovacuum_analyze_scale_factor)"
    )
