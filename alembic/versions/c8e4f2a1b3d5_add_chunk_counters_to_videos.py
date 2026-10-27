"""add chunk counters to videos

Revision ID: c8e4f2a1b3d5
Revises: e4a7f0b31c68
Create Date: 2026-10-27 14:50:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c8e4f2a1b3d5'
down_revision: Union[str, None] = 'e4a7f0b31c68'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Счётчики чанковой обработки (см. Video в app/db/models.py).
    # nullable=True: NULL означает «видео не чанковалось» (одиночная
    # трасса), существующие строки заполнять нечем.
    op.add_column(
        'videos',
        sa.Column('chunks_total', sa.Integer(), nullable=True),
    )
    op.add_column(
        'videos',
        sa.Column('chunks_done', sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column('videos', 'chunks_done')
    op.drop_column('videos', 'chunks_total')