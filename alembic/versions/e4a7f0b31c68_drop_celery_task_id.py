"""drop celery task_id from videos

Revision ID: e4a7f0b31c68
Revises: a91d4e05c7f3
Create Date: 2026-10-26 07:40:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e4a7f0b31c68'
down_revision: Union[str, None] = 'a91d4e05c7f3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Celery выведен из проекта: задачи на обработку видео идут через
    # Kafka (топик video_requests), статус и прогресс — в БД (см. Video).
    # ID задачи Celery хранить больше негде и незачем.
    op.drop_column('videos', 'task_id')


def downgrade() -> None:
    op.add_column(
        'videos',
        sa.Column('task_id', sa.String(length=255), nullable=True),
    )
