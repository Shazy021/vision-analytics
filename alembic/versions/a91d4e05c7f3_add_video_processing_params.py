"""per-video processing params (extract_fps, max_frames)

Revision ID: a91d4e05c7f3
Revises: c3f8a1d72b45
Create Date: 2026-10-23 15:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a91d4e05c7f3'
down_revision: Union[str, None] = 'c3f8a1d72b45'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Оверрайд параметров разбора на уровне видео (см. Video в models.py).
    # nullable=True = «дефолт платформы»: существующие видео обрабатывались
    # глобальными настройками, для них ничего не меняется.
    op.add_column(
        'videos',
        sa.Column('extract_fps', sa.Integer(), nullable=True),
    )
    op.add_column(
        'videos',
        sa.Column('max_frames', sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column('videos', 'max_frames')
    op.drop_column('videos', 'extract_fps')
