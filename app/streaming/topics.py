"""Kafka топики и конфигурация"""
from app.core.config import settings

# Единый конфиг: переопределяется через KAFKA_BOOTSTRAP_SERVERS (.env / compose)
KAFKA_BOOTSTRAP_SERVERS = settings.kafka_bootstrap_servers

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
