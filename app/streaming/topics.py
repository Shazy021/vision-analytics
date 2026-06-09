"""Kafka топики и конфигурация"""
import os

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

TOPIC_RAW_FRAMES = "raw_frames"
TOPIC_DETECTIONS = "detections"

TOPIC_CONFIG = {
    "num_partitions": 3,
    "replication_factor": 1,
    "retention_ms": 3600000,
}

DETECTIONS_TOPIC_CONFIG = {
    "num_partitions": 3,
    "replication_factor": 1,
    "retention_ms": 86400000,
}