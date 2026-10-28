"""Тесты process_video_chunk: time-grid сэмплирование, идемпотентность,
финализация, защита от обрыва. Синтетика 6с/30fps + мок YOLODetector —
без GPU и сети.
"""
from unittest.mock import MagicMock, patch

import cv2
import numpy as np
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from app.core.database import Base
from app.db.models import Video, VideoDetection
from app.processing import processor
from app.processing.processor import _decode_chunk, process_video_chunk


@pytest.fixture(scope="module")
def six_second_video(tmp_path_factory):
    """6 секунд, 30 fps, 180 кадров."""
    path = str(tmp_path_factory.mktemp("chunks") / "six.mp4")
    writer = cv2.VideoWriter(path, cv2.VideoWriter_fourcc(*"mp4v"), 30.0, (64, 64))
    for _ in range(180):
        writer.write(np.zeros((64, 64, 3), dtype=np.uint8))
    writer.release()
    return path


@pytest.fixture()
def db():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    session = Session(bind=engine)
    yield session
    session.close()
    Base.metadata.drop_all(bind=engine)


def _mock_detector():
    """(мок-инстанс, журнал): detect() логирует (frame_index, timestamp),
    детекций не возвращает — тестируем механику, не YOLO."""
    record = []

    def detect(frame, frame_index=0, timestamp=0.0, conf=0.25):
        record.append((frame_index, timestamp))
        return []

    instance = MagicMock()
    instance.detect.side_effect = detect
    return instance, record


def _make_video(db, path, **overrides):
    kwargs = dict(
        filename="six.mp4", filepath=path, status="processing",
        duration=6.0, fps=30, extract_fps=5, max_frames=0,
        chunks_total=2, chunks_done=0,
    )
    kwargs.update(overrides)
    v = Video(**kwargs)
    db.add(v)
    db.commit()
    db.refresh(v)
    return v


class TestProcessVideoChunk:
    def test_two_chunks_cover_grid_without_duplicates(self, db, six_second_video):
        """Сетка 0.2с: 15 отметок на чанк, объединение — 30 уникальных,
        без пересечения. Финализация — по последнему чанку."""
        video = _make_video(db, six_second_video)
        detector, record = _mock_detector()
        with patch.object(processor, "YOLODetector", return_value=detector):
            process_video_chunk(video.id, 0.0, 3.0, db)
            process_video_chunk(video.id, 3.0, 6.0, db)

        stamps = [t for _fi, t in record]
        assert len(stamps) == 30
        assert len({round(s, 3) for s in stamps}) == 30
        assert all(0.0 <= s < 3.0 for s in stamps[:15])
        assert all(3.0 <= s < 6.0 for s in stamps[15:])

        db.expire_all()
        video = db.get(Video, video.id)
        assert video.processed_frames == 30
        assert video.chunks_done == 2
        assert video.status == "completed"  # max_frames=0 → без лимита

    def test_idempotent_redelivery(self, db, six_second_video):
        """Redelivery чанка: мусор в собственном диапазоне стирается,
        дублей нет."""
        video = _make_video(db, six_second_video)
        db.add(VideoDetection(
            video_id=video.id, class_name="junk", confidence=0.5,
            bbox_x1=0, bbox_y1=0, bbox_x2=1, bbox_y2=1,
            frame_index=10, timestamp=1.0,
        ))
        db.commit()
        detector, _record = _mock_detector()
        with patch.object(processor, "YOLODetector", return_value=detector):
            process_video_chunk(video.id, 0.0, 3.0, db)
            process_video_chunk(video.id, 0.0, 3.0, db)  # redelivery

        count = db.query(VideoDetection).filter(
            VideoDetection.video_id == video.id,
            VideoDetection.timestamp >= 0.0,
            VideoDetection.timestamp < 3.0,
        ).count()
        assert count == 0  # мусор удалён, мок детекций не добавляет

    def test_incremental_progress_no_double_count(self, db, six_second_video, monkeypatch):
        """Репорты внутри чанка + финальный остаток = точный итог,
        без двойного счёта."""
        monkeypatch.setattr(processor, "PROGRESS_REPORT_EVERY", 10)
        video = _make_video(db, six_second_video)
        detector, _record = _mock_detector()
        with patch.object(processor, "YOLODetector", return_value=detector):
            process_video_chunk(video.id, 0.0, 3.0, db)
        db.expire_all()
        video = db.get(Video, video.id)
        # 15 сэмплов: 10 зарепортано на 10-м, 5 — финальным дельтой
        assert video.processed_frames == 15

    def test_max_frames_budget_applied_per_chunk(self, db, six_second_video, monkeypatch):
        """max_frames=30, step=int(30/5)=6 → общий бюджет 5 сэмплов →
        распределение [3, 2]: первый чанк останавливается на 3.
        chunk_sec=3, чтобы план 6с-видео состоял из двух чанков."""
        from app.core.config import settings
        monkeypatch.setattr(settings, "video_chunk_sec", 3.0)
        video = _make_video(db, six_second_video, max_frames=30)
        detector, record = _mock_detector()
        with patch.object(processor, "YOLODetector", return_value=detector):
            process_video_chunk(video.id, 0.0, 3.0, db)
        assert len(record) == 3

    def test_missing_video_raises(self, db):
        with pytest.raises(ValueError, match="not found"):
            process_video_chunk(999, 0.0, 3.0, db)


class TestDecodeChunkGuard:
    def test_early_eof_raises(self, six_second_video):
        """Чанк заявлен до 10с, а файл 6с — обрыв, а не тишина."""
        detector, _ = _mock_detector()
        with pytest.raises(RuntimeError, match="ended early"):
            _decode_chunk(six_second_video, 0.0, 10.0, 0.2, 0, detector)

    def test_normal_completion(self, six_second_video):
        detector, _ = _mock_detector()
        detections, sampled, hit = _decode_chunk(
            six_second_video, 0.0, 6.0, 0.2, 0, detector
        )
        assert sampled == 30
        assert hit is False
        assert detections == []

    def test_budget_stops_decoding(self, six_second_video):
        detector, _ = _mock_detector()
        _d, sampled, hit = _decode_chunk(six_second_video, 0.0, 6.0, 0.2, 5, detector)
        assert sampled == 5
        assert hit is True
