"""
Celery-задача для обработки видео.

Вместо BackgroundTasks (блокирует event loop FastAPI)
используем Celery — отдельный воркер в отдельном процессе.
"""
import logging
from app.core.celery_app import celery_app
from app.core.database import SessionLocal
from app.pipeline.processor import process_video

logger = logging.getLogger(__name__)

@celery_app.task(bind=True, queue="processing")
def process_video_task(self, video_id: int) -> dict:
    """
    Задача: обработать видео через YOLO.
    
    bind=True — даёт доступ к self (объект задачи),
    можно обновлять статус: self.update_state()
    """
    db = SessionLocal()
    try:
        process_video(video_id, db)
        return {"status": "completed", "video_id": video_id}
    except Exception:
        logger.exception(f"process_video_task video_id=P{video_id} failed")
        raise
    finally:
        db.close()