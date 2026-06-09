"""YOLO Consumer — читает кадры из Kafka, обрабатывает YOLO, отправляет детекции"""
import json
import base64
import numpy as np
import cv2
from confluent_kafka import Consumer, Producer
from app.streaming.topics import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_RAW_FRAMES,
    TOPIC_DETECTIONS,
)


class YOLOConsumer:
    """Читает raw_frames → YOLO → пишет в detections"""

    def __init__(self, model_path: str = "yolov8n.pt", device: str = "0"):
        from ultralytics import YOLO
        self.model = YOLO(model_path)
        self.device = device

        self.consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "yolo-workers",
            "auto.offset.reset": "earliest",
            "fetch.max.bytes": 5 * 1024 * 1024,
        })
        self.consumer.subscribe([TOPIC_RAW_FRAMES])

        self.producer = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "message.max.bytes": 5 * 1024 * 1024,
        })

    def run(self):
        print(f"YOLO consumer listening to '{TOPIC_RAW_FRAMES}'...")

        try:
            while True:
                msg = self.consumer.poll(1.0)  # таймаут 1 сек
                if msg is None:
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue

                data = json.loads(msg.value().decode())

                # Декодируем кадр
                frame_bytes = base64.b64decode(data["frame_data"])
                frame = cv2.imdecode(
                    np.frombuffer(frame_bytes, dtype=np.uint8),
                    cv2.IMREAD_COLOR,
                )

                # YOLO
                results = self.model(frame, device=self.device, verbose=False)[0]

                objects = []
                for box in results.boxes:
                    obj = {
                        "class": self.model.names[int(box.cls)],
                        "confidence": float(box.conf),
                        "bbox": {
                            "x1": float(box.xyxy[0][0]),
                            "y1": float(box.xyxy[0][1]),
                            "x2": float(box.xyxy[0][2]),
                            "y2": float(box.xyxy[0][3]),
                        },
                    }
                    objects.append(obj)

                detection_msg = json.dumps({
                    "video_id": None,
                    "stream_id": data.get("stream_id"),
                    "frame_index": data["frame_index"],
                    "source": data["source"],
                    "objects": objects,
                    "num_detections": len(objects),
                }).encode()

                self.producer.produce(TOPIC_DETECTIONS, value=detection_msg)
                self.producer.poll(0)
                print(f"Frame #{data['frame_index']}: {len(objects)} detections")

        except KeyboardInterrupt:
            print("\nStopping...")
        finally:
            self.consumer.close()
            self.producer.flush()


if __name__ == "__main__":
    import sys
    model = sys.argv[1] if len(sys.argv) > 1 else "yolov8n.pt"
    device = sys.argv[2] if len(sys.argv) > 2 else "0"
    consumer = YOLOConsumer(model_path=model, device=device)
    consumer.run()