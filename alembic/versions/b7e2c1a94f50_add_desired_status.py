"""add desired_status and status_reason to streams

Revision ID: b7e2c1a94f50
Revises: 6bb33b8d5309
Create Date: 2026-10-16 17:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b7e2c1a94f50'
down_revision: Union[str, None] = '6bb33b8d5309'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # desired_status — НАМЕРЕНИЕ пользователя (active/inactive), пишет API.
    # NOT NULL требует server_default: на момент миграции в таблице уже лежат
    # строки, и PostgreSQL обязан чем-то их заполнить при добавлении колонки.
    # Дефолт 'inactive' корректен: ни один существующий стрим нельзя считать
    # «заказанным на запуск», пока пользователь явно не нажал start.
    op.add_column(
        'streams',
        sa.Column('desired_status', sa.String(length=20), nullable=False,
                  server_default='inactive'),
    )
    # status_reason — диагноз при status='error' (текст от reconciler'а).
    # nullable=True: причины нет у 99% строк большую часть времени.
    op.add_column(
        'streams',
        sa.Column('status_reason', sa.String(length=255), nullable=True),
    )


def downgrade() -> None:
    op.drop_column('streams', 'status_reason')
    op.drop_column('streams', 'desired_status')
