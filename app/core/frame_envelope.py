"""Конвертер сообщения raw_frames: [4 байта BE длина меты][JSON-мета][сырой JPEG].

Kafka value бинарный сам по себе, а кадр раньше ехал внутри JSON как base64:
+33% к размеру сообщения и два лишних прохода кодирования на кадр
(encode у producer'а, decode у consumer'а). Мета (frame_index, stream_id,
source, captured_at) — маленькая, ей JSON и остаться; пиксели едут сырыми
байтами JPEG без обёрток.

Старый формат (JSON+base64) НЕ читается: при переходе топик raw_frames
чистится (retention и так 1 час, кадры — ephemeral).
"""
import json
import struct


def pack(meta: dict, jpeg: bytes) -> bytes:
    meta_bytes = json.dumps(meta).encode()
    return struct.pack(">I", len(meta_bytes)) + meta_bytes + jpeg


def unpack(value: bytes) -> tuple[dict, bytes]:
    """Развернуть сообщение в (мета, JPEG-байты).

    Мусор на входе — исключение на выходе: вызывающий трактует его как
    ядовитое сообщение (poison), как и раньше.
    """
    (meta_len,) = struct.unpack_from(">I", value, 0)
    meta = json.loads(value[4:4 + meta_len])
    return meta, value[4 + meta_len:]
