"""Трекинг объектов (v1): ByteTrack поверх детекций стримов.

Ядро (kalman/association/byte_tracker) не знает ни про Kafka, ни про БД —
принимает боксы, отдаёт track_id. Реестр трекеров по стримам — registry.
"""
from app.tracking.byte_tracker import ByteTracker, TrackEvent
from app.tracking.registry import StreamTrackers, TrackedFrame

__all__ = ["ByteTracker", "TrackEvent", "StreamTrackers", "TrackedFrame"]
