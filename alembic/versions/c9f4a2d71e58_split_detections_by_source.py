"""split detections into video_detections and stream_detections

Единая таблица detections с nullable-парой FK (video_id XOR stream_id)
разделяется на две: инвариант «ровно один владелец» становится NOT NULL
в самой схеме, а жизненные циклы (файл vs бесконечный стрим) больше не
смешаны в одном retention-плане.

Revision ID: c9f4a2d71e58
Revises: c8e4f2a1b3d5
Create Date: 2026-10-28 13:50:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c9f4a2d71e58'
down_revision: Union[str, None] = 'c8e4f2a1b3d5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_COMMON_COLUMNS = [
    sa.Column('frame_index', sa.Integer(), nullable=False),
    sa.Column('class_name', sa.String(length=50), nullable=False),
    sa.Column('confidence', sa.Float(), nullable=False),
    sa.Column('bbox_x1', sa.Float(), nullable=False),
    sa.Column('bbox_y1', sa.Float(), nullable=False),
    sa.Column('bbox_x2', sa.Float(), nullable=False),
    sa.Column('bbox_y2', sa.Float(), nullable=False),
    sa.Column('detected_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
]


def upgrade() -> None:
    op.create_table('video_detections',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('video_id', sa.Integer(), nullable=False),
    *_COMMON_COLUMNS,
    sa.Column('timestamp', sa.Float(), nullable=False),
    sa.ForeignKeyConstraint(['video_id'], ['videos.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_video_detections_video_class', 'video_detections', ['video_id', 'class_name'], unique=False)
    op.create_index('idx_video_detections_video_ts', 'video_detections', ['video_id', 'timestamp'], unique=False)

    op.create_table('stream_detections',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('stream_id', sa.Integer(), nullable=False),
    *_COMMON_COLUMNS,
    sa.ForeignKeyConstraint(['stream_id'], ['streams.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_stream_detections_stream_class', 'stream_detections', ['stream_id', 'class_name'], unique=False)
    op.create_index('idx_stream_detections_stream_at', 'stream_detections', ['stream_id', 'detected_at'], unique=False)

    # Перенос данных: id сохраняем (внешние ссылки на детекции остаются стабильными).
    # timestamp у старых видео-строк обязан быть (писатель всегда его ставил),
    # COALESCE — страховка от ручных вставок.
    op.execute('''
        INSERT INTO video_detections
            (id, video_id, frame_index, class_name, confidence,
             bbox_x1, bbox_y1, bbox_x2, bbox_y2, timestamp, detected_at)
        SELECT id, video_id, frame_index, class_name, confidence,
               bbox_x1, bbox_y1, bbox_x2, bbox_y2, COALESCE(timestamp, 0), detected_at
        FROM detections
        WHERE video_id IS NOT NULL
    ''')
    op.execute('''
        INSERT INTO stream_detections
            (id, stream_id, frame_index, class_name, confidence,
             bbox_x1, bbox_y1, bbox_x2, bbox_y2, detected_at)
        SELECT id, stream_id, frame_index, class_name, confidence,
               bbox_x1, bbox_y1, bbox_x2, bbox_y2, detected_at
        FROM detections
        WHERE stream_id IS NOT NULL
    ''')

    # id перенесены явными, а последовательности остались на 1 —
    # без setval первая же вставка упёрлась бы в дубликат первичного ключа.
    op.execute(
        "SELECT setval(pg_get_serial_sequence('video_detections', 'id'), "
        "COALESCE(MAX(id), 1)) FROM video_detections"
    )
    op.execute(
        "SELECT setval(pg_get_serial_sequence('stream_detections', 'id'), "
        "COALESCE(MAX(id), 1)) FROM stream_detections"
    )

    op.drop_index('idx_detections_video_id', table_name='detections')
    op.drop_index('idx_detections_video_class', table_name='detections')
    op.drop_index('idx_detections_stream_id', table_name='detections')
    op.drop_index('idx_detections_stream_class', table_name='detections')
    op.drop_index('idx_detections_detected_at', table_name='detections')
    op.drop_index('idx_detections_class', table_name='detections')
    op.drop_table('detections')


def downgrade() -> None:
    op.create_table('detections',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('video_id', sa.Integer(), nullable=True),
    sa.Column('stream_id', sa.Integer(), nullable=True),
    sa.Column('frame_index', sa.Integer(), nullable=False),
    sa.Column('class_name', sa.String(length=50), nullable=False),
    sa.Column('confidence', sa.Float(), nullable=False),
    sa.Column('bbox_x1', sa.Float(), nullable=False),
    sa.Column('bbox_y1', sa.Float(), nullable=False),
    sa.Column('bbox_x2', sa.Float(), nullable=False),
    sa.Column('bbox_y2', sa.Float(), nullable=False),
    sa.Column('timestamp', sa.Float(), nullable=True),
    sa.Column('detected_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
    sa.ForeignKeyConstraint(['stream_id'], ['streams.id'], ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['video_id'], ['videos.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id')
    )
    op.execute('''
        INSERT INTO detections
            (id, video_id, stream_id, frame_index, class_name, confidence,
             bbox_x1, bbox_y1, bbox_x2, bbox_y2, timestamp, detected_at)
        SELECT id, video_id, NULL, frame_index, class_name, confidence,
               bbox_x1, bbox_y1, bbox_x2, bbox_y2, timestamp, detected_at
        FROM video_detections
    ''')
    op.execute('''
        INSERT INTO detections
            (id, video_id, stream_id, frame_index, class_name, confidence,
             bbox_x1, bbox_y1, bbox_x2, bbox_y2, timestamp, detected_at)
        SELECT id, NULL, stream_id, frame_index, class_name, confidence,
               bbox_x1, bbox_y1, bbox_x2, bbox_y2, NULL, detected_at
        FROM stream_detections
    ''')
    op.execute(
        "SELECT setval(pg_get_serial_sequence('detections', 'id'), "
        "COALESCE(MAX(id), 1)) FROM detections"
    )
    op.create_index('idx_detections_class', 'detections', ['class_name'], unique=False)
    op.create_index('idx_detections_detected_at', 'detections', ['detected_at'], unique=False)
    op.create_index('idx_detections_stream_class', 'detections', ['stream_id', 'class_name'], unique=False)
    op.create_index('idx_detections_stream_id', 'detections', ['stream_id'], unique=False)
    op.create_index('idx_detections_video_class', 'detections', ['video_id', 'class_name'], unique=False)
    op.create_index('idx_detections_video_id', 'detections', ['video_id'], unique=False)
    op.drop_index('idx_stream_detections_stream_at', table_name='stream_detections')
    op.drop_index('idx_stream_detections_stream_class', table_name='stream_detections')
    op.drop_table('stream_detections')
    op.drop_index('idx_video_detections_video_ts', table_name='video_detections')
    op.drop_index('idx_video_detections_video_class', table_name='video_detections')
    op.drop_table('video_detections')