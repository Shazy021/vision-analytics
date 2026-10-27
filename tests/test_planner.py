"""Юнит-тесты планировщика чанков (app/processing/planner.py)."""
import cv2
import numpy as np
import pytest

from app.processing.planner import (
    allocate_sample_budgets,
    chunk_slot,
    plan_chunks,
    probe_media,
    should_chunk,
)


class TestPlanChunks:
    def test_exact_fit(self):
        assert plan_chunks(60.0, 30.0) == [(0.0, 30.0), (30.0, 60.0)]

    def test_tail_trimmed(self):
        assert plan_chunks(75.0, 30.0) == [(0.0, 30.0), (30.0, 60.0), (60.0, 75.0)]

    def test_shorter_than_chunk(self):
        assert plan_chunks(0.5, 30.0) == [(0.0, 0.5)]

    def test_empty(self):
        assert plan_chunks(0.0, 30.0) == []
        assert plan_chunks(-5.0, 30.0) == []

    def test_no_gaps_no_overlaps(self):
        chunks = plan_chunks(358.5, 30.0)
        assert chunks[0][0] == 0.0
        assert chunks[-1][1] == pytest.approx(358.5)
        for (_s1, e1), (s2, e2) in zip(chunks, chunks[1:]):
            assert e1 == pytest.approx(s2)
            assert e2 > e1

    def test_bad_chunk_sec(self):
        assert plan_chunks(60.0, 0) == []


class TestShouldChunk:
    def test_below_threshold(self):
        assert should_chunk(119.9, 120.0) is False

    def test_at_threshold(self):
        assert should_chunk(120.0, 120.0) is True

    def test_above(self):
        assert should_chunk(600.0, 120.0) is True


class TestAllocateSampleBudgets:
    def test_even_split(self):
        assert allocate_sample_budgets(4, 100) == [25, 25, 25, 25]

    def test_remainder_to_front(self):
        assert allocate_sample_budgets(3, 10) == [4, 3, 3]

    def test_smaller_than_chunks_round_robin(self):
        assert allocate_sample_budgets(4, 3) == [1, 1, 1, 0]

    def test_disabled(self):
        assert allocate_sample_budgets(4, 0) == []
        assert allocate_sample_budgets(0, 10) == []


class TestChunkSlot:
    def test_finds_slot(self):
        chunks = plan_chunks(90.0, 30.0)
        assert chunk_slot(chunks, 30.0) == 1

    def test_float_tolerance(self):
        chunks = plan_chunks(90.0, 30.0)
        assert chunk_slot(chunks, 30.0000001) == 1

    def test_absent(self):
        assert chunk_slot([(0.0, 30.0)], 45.0) == -1


class TestProbeMedia:
    def test_synthetic_video(self, tmp_path):
        path = str(tmp_path / "probe.mp4")
        writer = cv2.VideoWriter(path, cv2.VideoWriter_fourcc(*"mp4v"), 30.0, (64, 64))
        for _ in range(90):
            writer.write(np.zeros((64, 64, 3), dtype=np.uint8))
        writer.release()
        duration, fps = probe_media(path)
        assert duration == pytest.approx(3.0, abs=0.2)
        assert fps == pytest.approx(30.0, abs=1.0)

    def test_garbage_returns_none(self, tmp_path):
        p = tmp_path / "junk.mp4"
        p.write_bytes(b"0" * 100)
        assert probe_media(str(p)) is None
