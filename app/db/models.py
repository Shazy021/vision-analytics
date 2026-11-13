from sqlalchemy import Column, DateTime, Float, ForeignKey, Index, Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.core.database import Base


class Video(Base):
    """Загруженные видео."""
    __tablename__ = "videos"

    id = Column(Integer, primary_key=True, autoincrement=True)
    filename = Column(String(255), nullable=False)
    filepath = Column(String(512), nullable=True)
    object_key = Column(String(512), nullable=True)  # ключ в MinIO, напр. "videos/5.mp4"

    duration = Column(Float, nullable=True)
    fps = Column(Integer, nullable=True)
    resolution = Column(String(20), nullable=True)
    uploaded_at = Column(DateTime, server_default=func.now())

    # Статус обработки: processing -> completed / partial / failed
    status = Column(String(20), nullable=False, default="processing")

    # Прогресс обработки: факт, который пишет worker по ходу дела,
    # читает дашборд. Тот же принцип, что у desired/status у стримов:
    # прогресс живёт в БД, а не в памяти воркера, — переживает рестарты
    # и виден любому клиенту. nullable, пока процессор не открыл файл
    # и не посчитал, сколько кадров вообще придётся разобрать.
    frames_to_process = Column(Integer, nullable=True)
    processed_frames = Column(Integer, nullable=True)

    # Параметры разбора для КОНКРЕТНОГО видео (выбираются при загрузке).
    # None = использовать платформенные дефолты (settings.extract_fps /
    # settings.max_frames). Короткие ролики можно молотить чаще длинных,
    # и наоборот — глобальная настройка на это не тянет.
    extract_fps = Column(Integer, nullable=True)
    max_frames = Column(Integer, nullable=True)

    # Чанкование: planner на этапе загрузки пишет chunks_total (сколько
    # чанковых задач опубликовано в Kafka), processor атомарно инкрементит
    # chunks_done после каждого чанка. Кто довёл done до total — тот ставит
    # терминальный статус. NULL = видео не чанковалось (одиночная трасса).
    chunks_total = Column(Integer, nullable=True)
    chunks_done = Column(Integer, nullable=True)

    # cascade: удаление Video автоматически удаляет его детекции
    detections = relationship("VideoDetection", back_populates="video", cascade="all, delete-orphan")


class _DetectionMixin:
    """Колонки, общие для обоих видов детекций (видео/стрим)."""
    frame_index = Column(Integer, nullable=False)
    class_name = Column(String(50), nullable=False)
    confidence = Column(Float, nullable=False)
    bbox_x1 = Column(Float, nullable=False)
    bbox_y1 = Column(Float, nullable=False)
    bbox_x2 = Column(Float, nullable=False)
    bbox_y2 = Column(Float, nullable=False)
    # Реальное время детекции — работает и для файлов, и для стримов
    detected_at = Column(DateTime, server_default=func.now(), nullable=False)


class VideoDetection(_DetectionMixin, Base):
    """Детекции загруженного видео.

    Отдельная таблица (была общая detections с nullable-парой FK):
    жизненный цикл привязан к файлу (умирает вместе с видео), а система
    времени своя — timestamp в секундах от начала файла. NOT NULL-FOREIGN KEY
    исключает «детекцию без владельца», которую допускала общая схема.
    """
    __tablename__ = "video_detections"

    id = Column(Integer, primary_key=True, autoincrement=True)
    video_id = Column(Integer, ForeignKey("videos.id", ondelete="CASCADE"), nullable=False)
    # Секунда от начала файла (для стримов бессмысленна — там только detected_at)
    timestamp = Column(Float, nullable=False)

    video = relationship("Video", back_populates="detections")

    __table_args__ = (
        Index("idx_video_detections_video_class", "video_id", "class_name"),
        Index("idx_video_detections_video_ts", "video_id", "timestamp"),
    )


class StreamDetection(_DetectionMixin, Base):
    """Детекции RTSP-стрима.

    Копятся бесконечно (камера — постоянный объект, в отличие от файла),
    поэтому retention планируется по возрасту, а не по жизни источника.
    """
    __tablename__ = "stream_detections"

    id = Column(Integer, primary_key=True, autoincrement=True)
    stream_id = Column(Integer, ForeignKey("streams.id", ondelete="CASCADE"), nullable=False)
    # Трек, к которому относится детекция (трекинг v1, см. app/tracking).
    # NULL у детекций до включения трекинга и на видео-трассе.
    track_id = Column(Integer, ForeignKey("tracks.id", ondelete="SET NULL"), nullable=True)

    stream = relationship("Stream", back_populates="detections")

    __table_args__ = (
        Index("idx_stream_detections_stream_class", "stream_id", "class_name"),
        Index("idx_stream_detections_stream_at", "stream_id", "detected_at"),
    )


class Track(Base):
    """Жизненный цикл отслеженного объекта стрима (трекинг v1).

    Строка открывается, когда ByteTrack подтверждает новый трек, обновляется
    на каждом кадре с этим объектом и закрывается при потере (объект не
    подтверждался дольше track_max_age_sec секунд, отсчитанных в кадрах
    подачи этого стрима) или при рестарте
    консьюмера (состояние трекеров
    живёт в памяти и рестарт не переживает). Это фундамент для будущих
    зон/dwell-time: «кто, где и сколько пробыл» читается отсюда, без
    перегруппировки сырых детекций.

    id здесь же внешний ключ для stream_detections.track_id — детекции
    переживают закрытие трека (ON DELETE SET NULL).
    """
    __tablename__ = "tracks"

    id = Column(Integer, primary_key=True, autoincrement=True)
    stream_id = Column(Integer, ForeignKey("streams.id", ondelete="CASCADE"), nullable=False)
    class_name = Column(String(50), nullable=False)

    # active — объект на сцене; closed — трек завершён (причина в closed_reason).
    status = Column(String(20), nullable=False, default="active")
    # lost — объект пропал из кадра; restart — консьюмер перезапустился,
    # состояние трекеров в памяти потеряно, «висящие» active закрыты при старте.
    closed_reason = Column(String(20), nullable=True)

    first_seen_at = Column(DateTime, nullable=False)
    last_seen_at = Column(DateTime, nullable=False)
    closed_at = Column(DateTime, nullable=True)
    first_frame_index = Column(Integer, nullable=False)
    last_frame_index = Column(Integer, nullable=False)
    # Сколько кадров трек был подтверждён (не сколько шёл по предсказанию KF)
    num_frames = Column(Integer, nullable=False, default=1)

    # Последний наблюдённый бокс — для отладки и дашборда без join с детекциями
    bbox_x1 = Column(Float, nullable=True)
    bbox_y1 = Column(Float, nullable=True)
    bbox_x2 = Column(Float, nullable=True)
    bbox_y2 = Column(Float, nullable=True)

    stream = relationship("Stream")

    __table_args__ = (
        Index("idx_tracks_stream_status", "stream_id", "status"),
        Index("idx_tracks_stream_last_seen", "stream_id", "last_seen_at"),
    )


class Stream(Base):
    """RTSP/IP камеры."""
    __tablename__ = "streams"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)
    rtsp_url = Column(String(512), nullable=False)

    # ДВА разных статуса — это ядро паттерна desired-state + reconciler.
    #
    # desired_status = НАМЕРЕНИЕ («что пользователь хочет»). Пишет только API
    # (start/stop). Это «заказ», а не факт: стрим мог быть заказан, но producer
    # ещё не поднят, упал или упирается в backoff.
    #
    # status = ФАКТ («что есть на самом деле»). Пишет только stream_manager
    # (reconciler): active / inactive / error. API этот флаг НЕ выставляет —
    # он лишь читает и отдаёт наружу.
    #
    # Раньше оба смысла были слиты в одном status, и выставлял его API через
    # живость своего Popen. Из-за этого статус жил в памяти API-контейнера и
    # умирал вместе с ним. Теперь намерение и факт переживают рестарт API,
    # потому что оба лежат в БД, а факт обновляет независимый сервис.
    desired_status = Column(String(20), nullable=False, default="inactive")  # active/inactive
    status = Column(String(20), nullable=False, default="inactive")          # active/inactive/error
    # Причина перехода status в "error" (например, «producer умер через 3с,
    # превышен лимит рестартов»). Пусто, когда статуса ошибки нет.
    status_reason = Column(String(255), nullable=True)

    fps = Column(Integer, nullable=True, default=5)
    created_at = Column(DateTime, server_default=func.now())
    pid = Column(Integer, nullable=True)

    detections = relationship("StreamDetection", back_populates="stream", cascade="all, delete-orphan")
