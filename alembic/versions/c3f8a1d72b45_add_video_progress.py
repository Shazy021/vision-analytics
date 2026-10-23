"""add processing progress to videos

Revision ID: c3f8a1d72b45
Revises: b7e2c1a94f50
Create Date: 2026-10-23 14:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c3f8a1d72b45'
down_revision: Union[str, None] = 'b7e2c1a94f50'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Прогресс обработки видео (см. Video в app/db/models.py).
    # nullable=True: до момента, когда processor открыл файл и посчитал
    # выборку, значения неизвестны; существующие строки заполнять нечем.
    op.add_column(
        'videos',
        sa.Column('frames_to_process', sa.Integer(), nullable=True),
    )
    op.add_column(
        'videos',
        sa.Column('processed_frames', sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column('videos', 'processed_frames')
    op.drop_column('videos', 'frames_to_process')
