"""Kafka Producer — отправка кадров в топик raw_frames"""
import cv2
import base64
import json
from confluent_kafka import Producer
from app.streaming.topics import KAFKA_BOOTSTRAP_SERVERS, TOPIC_RAW_FRAMES


class FrameProducer:
    """Читает видео/RTSP поток и отправляет кадры в Kafka"""

    def __init__(self, source: str, fps: int = 5, stream_id: int | None = None):
        self.source = source
        self.fps = fps
        self.stream_id = stream_id
        self.producer = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "message.max.bytes": 5 * 1024 * 1024,
        })

    def _delivery_report(self, err, msg):
        if err:
            print(f"\nSEND ERROR: {err}")

    def run(self):
        cap = cv2.VideoCapture(self.source)
        if not cap.isOpened():
            raise ConnectionError(f"Cannot open: {self.source}")

        src_fps = cap.get(cv2.CAP_PROP_FPS) or 30
        frame_interval = max(1, int(src_fps / self.fps))
        frame_count = 0
        sent_count = 0

        print(f"Streaming → Kafka '{TOPIC_RAW_FRAMES}' at {self.fps} fps" +
              (f" (stream_id={self.stream_id})" if self.stream_id else ""))

        try:
            while cap.isOpened():
                ret, frame = cap.read()
                if not ret:
                    print(f"\nEnd of video. Total frames: {frame_count}, sent: {sent_count}")
                    break

                if frame_count % frame_interval == 0:
                    _, buffer = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 80])
                    frame_b64 = base64.b64encode(buffer).decode()

                    message = json.dumps({
                        "frame_data": frame_b64,
                        "frame_index": frame_count,
                        "source": self.source,
                        "stream_id": self.stream_id,
                    }).encode()

                    self.producer.produce(
                        TOPIC_RAW_FRAMES,
                        value=message,
                        callback=self._delivery_report,
                    )
                    self.producer.poll(0)

                    sent_count += 1
                    print(f"Sent frame #{frame_count} ({sent_count} total)")

                frame_count += 1

        except KeyboardInterrupt:
            print(f"\nStopped. Sent {sent_count} frames")
        finally:
            cap.release()
            self.producer.flush()


if __name__ == "__main__":
    import sys
    source = sys.argv[1] if len(sys.argv) > 1 else "test_video.mp4"
    fps = int(sys.argv[2]) if len(sys.argv) > 2 else 2
    stream_id = int(sys.argv[3]) if len(sys.argv) > 3 else None
    producer = FrameProducer(source, fps=fps, stream_id=stream_id)
    producer.run()