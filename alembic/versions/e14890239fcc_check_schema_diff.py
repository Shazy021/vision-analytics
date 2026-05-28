"""check schema diff

Revision ID: e14890239fcc
Revises: f5d6ad946d0d
Create Date: 2026-05-29 01:40:35.693553

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e14890239fcc'
down_revision: Union[str, None] = 'f5d6ad946d0d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Переименовываем колонку confidance → confidence
    # op.alter_column НЕ удаляет данные — просто меняет имя
    op.alter_column(
        'detections', 'confidance',
        new_column_name='confidence',
    )


def downgrade() -> None:
    # Откат: confidence → confidance
    op.alter_column(
        'detections', 'confidence',
        new_column_name='confidance',
    )