"""Подготовка медиа: детерминированный клип + RTSP-публикатор в MediaMTX.

Зачем свой клип, а не ролик как есть:
  1. Длина контролируется профилем (30с/60с) — frames_to_process известен
     заранее, отчёты по шагам сопоставимы между прогонами.
  2. Ре-кодирование в H.264 veryfast/CRF23 делает размер и качество кадра
     одинаковыми от прогона к прогону: меняется только код системы, а не вход.
  3. faststart + pix_fmt yuv420p — клип читается ffmpeg-публикатором
     без сюрпризов (seek по циклу, копирование кодека).

Публикатор: один ffmpeg льёт клип в MediaMTX бесконечным циклом (-stream_loop
-1 -re). MediaMTX сам разводит поток всем N читателям: 24 «камеры» = 24
producer'а платформы, читающих один и тот же RTSP-путь — ровно та топология,
которуюдает реальный набор камер.
"""
from __future__ import annotations

import hashlib
import json
import logging
import subprocess
import time
from pathlib import Path

logger = logging.getLogger(__name__)


class MediaError(RuntimeError):
    """ffmpeg/ffprobe не смогли подготовить или опубликовать медиа."""


def probe(path_or_url: str, timeout_s: float = 15.0) -> dict:
    """Метаданные источника: duration, fps, width, height, codec.

    Работает и с файлом, и с rtsp:// (для проверки живости публикатора).
    Бросает MediaError при нечитаемом источнике.
    """
    cmd = [
        "ffprobe", "-v", "error",
        "-rtsp_transport", "tcp",
        "-select_streams", "v:0",
        "-show_entries",
        "stream=codec_name,width,height,avg_frame_rate:format=duration",
        "-of", "json", path_or_url,
    ]
    try:
        out = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout_s, check=True
        ).stdout
    except subprocess.TimeoutExpired as exc:
        raise MediaError(f"ffprobe завис на {path_or_url}") from exc
    except subprocess.CalledProcessError as exc:
        raise MediaError(
            f"ffprobe не смог прочитать {path_or_url}: {exc.stderr.strip()[:200]}"
        ) from exc
    data = json.loads(out)
    streams = data.get("streams") or [{}]
    fmt = data.get("format") or {}
    st = streams[0]

    def parse_fps(raw: str | None) -> float | None:
        if not raw or "/" not in raw:
            return None
        num, den = raw.split("/", 1)
        try:
            num_f, den_f = float(num), float(den)
        except ValueError:
            return None
        return num_f / den_f if den_f else None

    duration = fmt.get("duration")
    return {
        "duration_s": float(duration) if duration else None,
        "fps": parse_fps(st.get("avg_frame_rate")),
        "width": st.get("width"),
        "height": st.get("height"),
        "codec": st.get("codec_name"),
    }


def ffmpeg_available() -> bool:
    try:
        subprocess.run(
            ["ffmpeg", "-version"], capture_output=True, timeout=10, check=True
        )
        return True
    except (subprocess.SubprocessError, FileNotFoundError, OSError):
        return False


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def prepare_clip(source: Path, seconds: float, assets_dir: Path) -> Path:
    """Нарезать детерминированный клип заданной длины (с кэшем по хэшу).

    Короткий источник зацикливается (-stream_loop -1 -t N): контент тот же,
    длительность — сколько профилю нужно.
    """
    assets_dir.mkdir(parents=True, exist_ok=True)
    src_sha = file_sha256(source)
    clip = assets_dir / f"clip_{src_sha[:12]}_{int(seconds)}s.mp4"
    if clip.exists():
        meta = probe(str(clip))
        if meta["duration_s"] and abs(meta["duration_s"] - seconds) < 1.0:
            logger.info("Клип уже готов (кэш): %s", clip)
            return clip
        logger.warning("Кэш-клип битый (duration=%s) — пересобираю", meta["duration_s"])
        clip.unlink()

    logger.info("Готовлю клип %.0fс из %s ...", seconds, source)
    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
        "-stream_loop", "-1",
        "-i", str(source),
        "-t", f"{seconds:.3f}",
        "-an",
        "-c:v", "libx264", "-preset", "veryfast", "-crf", "23",
        "-pix_fmt", "yuv420p", "-movflags", "+faststart",
        str(clip),
    ]
    try:
        subprocess.run(cmd, check=True, capture_output=True, timeout=600)
    except subprocess.CalledProcessError as exc:
        raise MediaError(f"ffmpeg не смог нарезать клип: {exc.stderr.decode()[:300]}") from exc
    meta = probe(str(clip))
    logger.info(
        "Клип готов: %.1fс, %s, %sx%s, %s fps",
        meta["duration_s"] or 0, meta["codec"], meta["width"], meta["height"],
        (meta["fps"] or 0),
    )
    return clip


class RtspPublisher:
    """ffmpeg-публикатор клипа в MediaMTX — «камера» для N стримов.

    -re — темп реального времени: кадры прибывают как с живой камеры, а не
    со скоростью диска. -c:v copy: клип уже H.264, транскод не нужен.
    """

    def __init__(self, clip: Path, publish_url: str):
        self.clip = clip
        self.publish_url = publish_url
        self._proc: subprocess.Popen | None = None

    def start(self, wait_alive_s: float = 30.0) -> None:
        cmd = [
            "ffmpeg", "-hide_banner", "-loglevel", "error",
            "-stream_loop", "-1", "-re",
            "-i", str(self.clip),
            "-c:v", "copy", "-an",
            "-f", "rtsp", "-rtsp_transport", "tcp",
            self.publish_url,
        ]
        logger.info("Публикатор: %s → %s", self.clip.name, self.publish_url)
        self._proc = subprocess.Popen(
            cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE
        )
        # Ждём, пока путь станет читаемым: MediaMTX поднимает поток не мгновенно.
        deadline = time.monotonic() + wait_alive_s
        while time.monotonic() < deadline:
            if self._proc.poll() is not None:
                err = (self._proc.stderr.read() or b"").decode(errors="replace")[:300]
                raise MediaError(f"ffmpeg-публикатор упал сразу: {err}")
            try:
                probe(self.publish_url, timeout_s=5.0)
                logger.info("RTSP-путь жив: %s", self.publish_url)
                return
            except MediaError:
                time.sleep(1.5)
        self.stop()
        raise MediaError(
            f"RTSP-путь {self.publish_url} не ожил за {wait_alive_s:.0f}с. "
            f"MediaMTX поднят? Проверь: docker compose ps mediamtx"
        )

    def stop(self) -> None:
        if self._proc is None:
            return
        if self._proc.poll() is None:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self._proc.kill()
                self._proc.wait(timeout=5)
        self._proc = None
        logger.info("Публикатор остановлен")

    def __enter__(self) -> "RtspPublisher":
        return self

    def __exit__(self, *exc) -> None:
        self.stop()
