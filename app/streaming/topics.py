"""Kafka топики и конфигурация"""
from app.core.config import settings

# Единый конфиг: переопределяется через KAFKA_BOOTSTRAP_SERVERS (.env / compose)
KAFKA_BOOTSTRAP_SERVERS = settings.kafka_bootstrap_servers

TOPIC_RAW_FRAMES = "raw_frames"
TOPIC_DETECTIONS = "detections"

# Параметры самих топиков (partition/retention) задаёт сервис kafka-init
# в docker-compose.yml — единственное место, где они реально применяются.
