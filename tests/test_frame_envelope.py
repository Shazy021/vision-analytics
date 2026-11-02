"""Тесты бинарного конверта raw_frames (app.core.frame_envelope)."""
import pytest

from app.core.frame_envelope import pack, unpack


def test_roundtrip():
    meta = {
        "frame_index": 42,
        "source": "rtsp://mediamtx:8554/cam",
        "stream_id": 3,
        "captured_at": "2026-11-02T10:00:00+00:00",
    }
    jpeg = b"\xff\xd8\xff\xe0" + b"jpeg-payload" * 100
    meta_out, jpeg_out = unpack(pack(meta, jpeg))
    assert meta_out == meta
    assert jpeg_out == jpeg


def test_empty_jpeg_and_unicode_source_survive():
    meta = {"frame_index": 0, "source": "источник/камера-1", "stream_id": None}
    meta_out, jpeg_out = unpack(pack(meta, b""))
    assert meta_out == meta
    assert jpeg_out == b""


@pytest.mark.parametrize("garbage", [
    b"",
    b"not json at all",
    b"\x00\x00\x00\x10short",
    b"{}" + b"no-meta-len",
])
def test_garbage_raises(garbage):
    """Мусор обязан бросать — consumer трактует это как poison message."""
    with pytest.raises(Exception):
        unpack(garbage)
