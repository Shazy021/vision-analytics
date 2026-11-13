"""tracks table + track_id on stream_detections

Трекинг v1 (ByteTrack в detection-консьюмере): жизненный цикл отслеженного
объекта становится первой сущностью. Таблица tracks хранит «кто, когда и
сколько пробыл на сцене»; stream_detections.track_id связывает детекцию с
треком (NULL у исторических строк и на видео-трассе).

Revision ID: 7d3a9c5e21f4
Revises: c9f4a2d71e58
Create Date: 2026-11-12 13:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '7d3a9c5e21f4'
down_revision: Union[str, None] = 'c9f4a2d71e58'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table('tracks',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('stream_id', sa.Integer(), nullable=False),
    sa.Column('class_name', sa.String(length=50), nullable=False),
    sa.Column('status', sa.String(length=20), nullable=False),
    sa.Column('closed_reason', sa.String(length=20), nullable=True),
    sa.Column('first_seen_at', sa.DateTime(), nullable=False),
    sa.Column('last_seen_at', sa.DateTime(), nullable=False),
    sa.Column('closed_at', sa.DateTime(), nullable=True),
    sa.Column('first_frame_index', sa.Integer(), nullable=False),
    sa.Column('last_frame_index', sa.Integer(), nullable=False),
    sa.Column('num_frames', sa.Integer(), nullable=False),
    sa.Column('bbox_x1', sa.Float(), nullable=True),
    sa.Column('bbox_y1', sa.Float(), nullable=True),
    sa.Column('bbox_x2', sa.Float(), nullable=True),
    sa.Column('bbox_y2', sa.Float(), nullable=True),
    sa.ForeignKeyConstraint(['stream_id'], ['streams.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_tracks_stream_status', 'tracks', ['stream_id', 'status'], unique=False)
    op.create_index('idx_tracks_stream_last_seen', 'tracks', ['stream_id', 'last_seen_at'],
                    unique=False)

    op.add_column('stream_detections',
                  sa.Column('track_id', sa.Integer(), nullable=True))
    # Удаление трека не должно уносить детекции — они первичные данные.
    op.create_foreign_key('fk_stream_detections_track', 'stream_detections', 'tracks',
                          ['track_id'], ['id'], ondelete='SET NULL')


def downgrade() -> None:
    op.drop_constraint('fk_stream_detections_track', 'stream_detections', type_='foreignkey')
    op.drop_column('stream_detections', 'track_id')
    op.drop_index('idx_tracks_stream_last_seen', table_name='tracks')
    op.drop_index('idx_tracks_stream_status', table_name='tracks')
    op.drop_table('tracks')
