"""
Streamlit-дашборд для Vision Analytics Platform.

Страницы:
  📹 Видео   — список + аналитика + синхронный двойной плеер (чистый / с боксами)
               на одном общем таймлайне (JS-компонент, canvas-оверлей)
  📡 Стримы  — список + аналитика + два окна превью (чистый/с детекциями)

Превью-рендер: HTML+CSS-overlay (не matplotlib). Причины:
  - работает в st.columns() без поломки layout (два окна бок-о-бок)
  - боксы на любой ширине контейнера точно на объектах (% координаты)
  - быстрее matplotlib (нет encode/decode растра)
"""
import base64
import io
import json
import os
import time

import plotly.express as px
import requests
import streamlit as st
import streamlit.components.v1 as stc
from PIL import Image

# ============================================================
# НАСТРОЙКА СТРАНИЦЫ
# ============================================================
st.set_page_config(
    page_title="Vision Analytics",
    page_icon="🔍",
    layout="wide",
)

API_URL = os.environ.get("API_URL", "http://localhost:8000")


# ============================================================
# КЕШИРОВАНИЕ
# ============================================================
@st.cache_data(ttl=10)
def get_stats(source=None):
    try:
        params = {}
        if source:
            params["source"] = source
        r = requests.get(f"{API_URL}/api/analytics/stats", params=params, timeout=5)
        return r.json() if r.status_code == 200 else None
    except Exception:
        return None


@st.cache_data(ttl=10)
def get_class_distribution(video_id=None, stream_id=None, source=None):
    try:
        params = {}
        if video_id:
            params["video_id"] = video_id
        if stream_id:
            params["stream_id"] = stream_id
        if source:
            params["source"] = source
        r = requests.get(f"{API_URL}/api/analytics/class-distribution", params=params, timeout=5)
        return r.json() if r.status_code == 200 else []
    except Exception:
        return []


@st.cache_data(ttl=10)
def get_timeline(video_id=None, source=None, bucket=5.0):
    try:
        params = {"bucket": bucket}
        if video_id:
            params["video_id"] = video_id
        if source:
            params["source"] = source
        r = requests.get(f"{API_URL}/api/analytics/timeline", params=params, timeout=5)
        return r.json() if r.status_code == 200 else []
    except Exception:
        return []


@st.cache_data(ttl=10)
def get_stream_activity(stream_id=None, minutes=10):
    try:
        params = {"minutes": minutes}
        if stream_id:
            params["stream_id"] = stream_id
        r = requests.get(f"{API_URL}/api/analytics/stream-activity", params=params, timeout=5)
        return r.json() if r.status_code == 200 else []
    except Exception:
        return []


@st.cache_data(ttl=10)
def get_videos():
    try:
        r = requests.get(f"{API_URL}/api/videos", timeout=5)
        return r.json() if r.status_code == 200 else []
    except Exception:
        return []


@st.cache_data(ttl=10)
def get_streams():
    try:
        r = requests.get(f"{API_URL}/api/streams", timeout=5)
        return r.json() if r.status_code == 200 else []
    except Exception:
        return []


@st.cache_data(ttl=10)
def get_stream_detections(stream_id, limit=100, frame_index=None):
    try:
        params = {"limit": limit}
        if frame_index is not None:
            params["frame_index"] = frame_index
        r = requests.get(
            f"{API_URL}/api/streams/{stream_id}/detections",
            params=params,
            timeout=5,
        )
        return r.json() if r.status_code == 200 else []
    except Exception:
        return []


# ---- Превью-эндпоинты: НЕ кэшируем (хотим свежий кадр каждый запрос) ----

def get_video_all_detections(video_id, limit=100000):
    """
    ВСЕ детекции видео для синхронного плеера: JSON инжектится прямо в
    JS-компонент, и клиент рисует боксы по currentTime без единого
    запроса к серверу на кадр (Streamlit-виджет так не умеет).
    """
    try:
        r = requests.get(
            f"{API_URL}/api/videos/{video_id}/detections",
            params={"limit": limit},
            timeout=15,
        )
        return r.json() if r.status_code == 200 else []
    except Exception:
        return []


def get_video_data(video_id):
    """
    Полные данные видео: duration, fps, status, video_url (presigned URL
    на весь MP4 в MinIO). video_url нужен для HTML5 <video>-плеера.
    """
    try:
        r = requests.get(
            f"{API_URL}/api/videos/{video_id}",
            timeout=5,
        )
        return r.json() if r.status_code == 200 else None
    except Exception:
        return None


def get_latest_stream_frame(stream_id):
    """
    Получить последний кадр стрима из MinIO.
    Возвращает dict: {frame_index, frame_url, detections_count}
    или None, если кадров ещё нет.
    """
    try:
        r = requests.get(
            f"{API_URL}/api/streams/{stream_id}/latest-frame",
            timeout=5,
        )
        if r.status_code == 200:
            return r.json()
        return None
    except Exception:
        return None


def fetch_image_bytes(url, timeout=5):
    """Скачать байты картинки по presigned URL."""
    try:
        r = requests.get(url, timeout=timeout)
        if r.status_code == 200:
            return r.content
        return None
    except Exception:
        return None


# ============================================================
# РИСОВАНИЕ БОКСОВ
# ============================================================
# Цвета по классам (COCO). Если класс не из списка — берём серый.
# Взято из стандартной палитры YOLO, чтобы визуально совпадало.
CLASS_COLORS = {
    "person": "#FF3838",
    "bicycle": "#FF9D97",
    "car": "#FF701F",
    "motorcycle": "#FFB21D",
    "airplane": "#CFD011",
    "bus": "#208FF2",
    "train": "#FFED13",
    "truck": "#79E323",
    "boat": "#B5E431",
    "traffic light": "#FF3838",
    "fire hydrant": "#FF9D97",
    "stop sign": "#FF701F",
    "parking meter": "#FFB21D",
    "bench": "#CFD011",
    "bird": "#208FF2",
    "cat": "#FFED13",
    "dog": "#79E323",
    "horse": "#B5E431",
}
DEFAULT_COLOR = "#1f77b4"


def render_frame_with_boxes_html(image_bytes, detections):
    """
    Наложить боксы детекций на картинку через CSS-overlay.

    Принимает:
      image_bytes: JPEG/PNG байты чистого кадра
      detections: список dicts с ключами bbox=[x1,y1,x2,y2], class_name, confidence

    Возвращает:
      HTML-строку для st.markdown(html, unsafe_allow_html=True).

    Почему HTML+CSS, а не matplotlib?
      1. Идеально работает в st.columns() — два окна бок-о-бок без проблем.
         matplotlib figures в колонках часто ломают layout (растягиваются,
         выталкивают друг друга).
      2. Лучше выглядит: подписи классов читаемые, цвета боксов насыщенные.
      3. Легче: matplotlib требует encode/decode растрового холста на каждый
         кадр, HTML — просто строка.
      4. Это то, что пользователь изначально предлагал: «CSS absolute-positioned
         div'ы поверх изображения, или canvas».

    Концепция:
      - <img> с width:100% занимает всю ширину контейнера.
      - Каждый бокс — position:absolute с координатами в % от размера <img>.
        % рассчитываем по PIL-размерам исходной картинки: bbox в пикселях →
        bbox в процентах. CSS-проценты масштабируются вместе с <img>, поэтому
        боксы всегда точно на объектах, при любой ширине контейнера.
    """
    if not image_bytes:
        return None

    try:
        img = Image.open(io.BytesIO(image_bytes))
        img_w, img_h = img.size
    except Exception:
        return None

    # Картинку в base64 для встраивания прямо в HTML (data: URL).
    # presigned URL не подходит — он разный каждый раз и не даст overlay:
    # <img> должен быть в одном DOM-дереве с боксовыми <div>'ами.
    img_b64 = base64.b64encode(image_bytes).decode("utf-8")

    # Шаблон одного бокса
    box_template = (
        '<div style="position:absolute;'
        'left:{x1:.2f}%;top:{y1:.2f}%;'
        'width:{w:.2f}%;height:{h:.2f}%;'
        'border:3px solid {color};box-sizing:border-box;">'
        '<span style="position:absolute;top:-22px;left:-3px;'
        'background:{color};color:white;padding:2px 6px;'
        'font-size:12px;font-weight:bold;white-space:nowrap;'
        'font-family:Arial,sans-serif;border-radius:3px 3px 0 0;'
        'line-height:1.4;">{label}</span>'
        '</div>'
    )
    boxes_html_parts = []
    for det in detections:
        bbox = det.get("bbox") or []
        if len(bbox) != 4:
            continue
        x1, y1, x2, y2 = bbox
        cls = det.get("class_name", "?")
        conf = det.get("confidence", 0.0)
        color = CLASS_COLORS.get(cls, DEFAULT_COLOR)
        label = f"{cls} {conf:.0%}"

        # bbox в пикселях → в % от размера изображения
        boxes_html_parts.append(box_template.format(
            x1=(x1 / img_w) * 100.0 if img_w else 0,
            y1=(y1 / img_h) * 100.0 if img_h else 0,
            w=((x2 - x1) / img_w) * 100.0 if img_w else 0,
            h=((y2 - y1) / img_h) * 100.0 if img_h else 0,
            color=color,
            label=label,
        ))

    return (
        '<div style="position:relative;width:100%;'
        'line-height:0;">'
        f'<img src="data:image/jpeg;base64,{img_b64}" '
        'style="width:100%;display:block;" alt="frame" />'
        + "".join(boxes_html_parts)
        + '</div>'
    )


# ============================================================
# СИНХРОННЫЙ ДВОЙНОЙ ПЛЕЕР (видео)
# ============================================================
# Два <video> с одним и тем же presigned URL: левое — чистое, правое — то же
# видео, но поверх canvas с боксами. Общий play/pause + скраббер. Вся
# синхронизация и отрисовка — клиентским JS: Streamlit не может дёргать
# сервер на каждый кадр, поэтому st.slider/st.video для этой задачи не годятся.
# Данные боксов инжектятся в HTML как JSON (ts → список боксов).
SYNC_PLAYER_TEMPLATE = """
<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><style>
  body{margin:0;font-family:Arial,sans-serif;}
  .row{display:flex;gap:10px;}
  .cell{flex:1;min-width:0;}
  .cap{font-weight:bold;font-size:14px;margin:0 0 6px 2px;}
  .wrap{position:relative;line-height:0;}
  video{width:100%;display:block;background:#000;border-radius:6px;}
  canvas{position:absolute;left:0;top:0;width:100%;height:100%;}
  .ctrl{display:flex;align-items:center;gap:10px;margin-top:10px;flex-wrap:wrap;}
  #play{padding:6px 14px;font-size:14px;cursor:pointer;border-radius:6px;
        border:1px solid #bbb;background:#f0f2f6;}
  #scrub{flex:1;min-width:120px;}
  #time{font-variant-numeric:tabular-nums;font-size:13px;white-space:nowrap;}
  label{font-size:13px;white-space:nowrap;}
</style></head>
<body>
<div class="row">
  <div class="cell"><div class="cap">Чистое видео</div>
    <div class="wrap"><video id="vraw" preload="auto" playsinline></video></div></div>
  <div class="cell"><div class="cap">С детекциями</div>
    <div class="wrap"><video id="vbox" preload="auto" playsinline></video>
    <canvas id="overlay"></canvas></div></div>
</div>
<div class="ctrl">
  <button id="play">▶ Играть</button>
  <input id="scrub" type="range" min="0" max="0" step="0.05" value="0">
  <span id="time">0:00 / 0:00</span>
  <label><input type="checkbox" id="boxes" checked> боксы</label>
</div>
<script>
const VIDEO_URL = __URL_JSON__;
const DETS = __DETS_JSON__;   // {"<ts>": [[x1,y1,x2,y2,"class",conf], ...]}
const COLORS = __COLORS_JSON__;
const DEFAULT_COLOR = "#1f77b4";
const v1 = document.getElementById("vraw");
const v2 = document.getElementById("vbox");
const canvas = document.getElementById("overlay");
const ctx = canvas.getContext("2d");
const playBtn = document.getElementById("play");
const scrub = document.getElementById("scrub");
const timeLbl = document.getElementById("time");
const boxesCb = document.getElementById("boxes");
v1.src = VIDEO_URL;
v2.src = VIDEO_URL;
v2.muted = true;  // чтобы не было эха — аудио ведёт левое окно
const TS = Object.keys(DETS).map(Number).sort((a, b) => a - b);
let showBoxes = true;
let scrubbing = false;

// Ближайший обработанный кадр: max ts <= t (как closest_ts в API),
// до первого обработанного кадра показываем его боксы.
function nearestDets(t) {
  if (!TS.length) return [];
  let lo = 0, hi = TS.length - 1, idx = -1;
  while (lo <= hi) {
    const m = (lo + hi) >> 1;
    if (TS[m] <= t + 0.05) { idx = m; lo = m + 1; } else { hi = m - 1; }
  }
  if (idx < 0) idx = 0;
  return DETS[TS[idx]] || [];
}
function resizeCanvas() {
  const w = v2.clientWidth, h = v2.clientHeight;
  if (canvas.width !== w || canvas.height !== h) { canvas.width = w; canvas.height = h; }
}
function draw() {
  resizeCanvas();
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  if (!showBoxes || !v2.videoWidth) return;
  const sx = canvas.width / v2.videoWidth, sy = canvas.height / v2.videoHeight;
  ctx.lineWidth = 2;
  ctx.font = "bold 12px Arial";
  for (const d of nearestDets(v2.currentTime)) {
    const x1 = d[0]*sx, y1 = d[1]*sy, w = (d[2]-d[0])*sx, h = (d[3]-d[1])*sy;
    const color = COLORS[d[4]] || DEFAULT_COLOR;
    ctx.strokeStyle = color;
    ctx.strokeRect(x1, y1, w, h);
    const label = d[4] + " " + Math.round(d[5]*100) + "%";
    const tw = ctx.measureText(label).width + 8;
    const ty = y1 > 18 ? y1 - 18 : y1;
    ctx.fillStyle = color;
    ctx.fillRect(x1, ty, tw, 16);
    ctx.fillStyle = "#fff";
    ctx.fillText(label, x1 + 4, ty + 12);
  }
}
function fmt(t) {
  t = Math.max(0, t | 0);
  const m = Math.floor(t/60), s = t % 60;
  return m + ":" + (s < 10 ? "0" : "") + s;
}
function syncUI() {
  if (!scrubbing) scrub.value = v1.currentTime;
  timeLbl.textContent = fmt(v1.currentTime) + " / " + fmt(v1.duration || 0);
}
v1.addEventListener("loadedmetadata", () => { scrub.max = v1.duration || 0; syncUI(); });
v1.addEventListener("timeupdate", syncUI);
v1.addEventListener("play", () => { playBtn.textContent = "⏸ Пауза"; });
v1.addEventListener("pause", () => { playBtn.textContent = "▶ Играть"; });
playBtn.addEventListener("click", () => {
  if (v1.paused) {
    if (Math.abs(v2.currentTime - v1.currentTime) > 0.05) v2.currentTime = v1.currentTime;
    v1.play(); v2.play();
  } else { v1.pause(); v2.pause(); }
});
scrub.addEventListener("input", () => {
  scrubbing = true;
  v1.currentTime = +scrub.value; v2.currentTime = +scrub.value;
  syncUI();
});
scrub.addEventListener("change", () => { scrubbing = false; });
boxesCb.addEventListener("change", () => { showBoxes = boxesCb.checked; });
// Правое окно — ведомое: раз в 0.5с догоняет левое при рассинхроне
setInterval(() => {
  if (!v1.paused && Math.abs(v2.currentTime - v1.currentTime) > 0.3) {
    v2.currentTime = v1.currentTime;
  }
}, 500);
(function loop() { draw(); requestAnimationFrame(loop); })();
</script>
</body>
</html>
"""


def build_sync_player_html(video_url, detections_by_ts):
    """
    Собрать HTML синхронного плеера (два окна + общий таймлайн).

    detections_by_ts: {timestamp: [[x1, y1, x2, y2, class_name, confidence], ...]}
    """
    return (
        SYNC_PLAYER_TEMPLATE
        .replace("__URL_JSON__", json.dumps(video_url))
        .replace("__DETS_JSON__", json.dumps(detections_by_ts, separators=(",", ":")))
        .replace("__COLORS_JSON__", json.dumps(CLASS_COLORS))
    )


# ============================================================
# БОКОВОЕ МЕНЮ
# ============================================================
page = st.sidebar.selectbox("Навигация", ["📹 Видео", "📡 Стримы"])

# ============================================================
# ЗАГОЛОВОК
# ============================================================
st.title("🔍 Vision Analytics Platform")
st.markdown("---")

# ============================================================
# СТРАНИЦА: ВИДЕО
# ============================================================
if page == "📹 Видео":
    stats = get_stats(source="video")
    if stats:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("📹 Всего видео", stats["total_videos"])
        with col2:
            st.metric("🎯 Всего детекций", stats["total_detections"])
        with col3:
            st.metric("📊 Средняя уверенность", f"{stats['avg_confidence']:.1%}")
    else:
        st.warning("⚠️ Не удалось подключиться к API")

    st.markdown("---")

    # Фильтр по видео
    videos = get_videos()
    selected_video_id = None
    selected_video_obj = None

    if videos:
        video_options = {"Все видео": None}
        for v in videos:
            icon = "✅" if v["status"] == "completed" else "⏳" if v["status"] == "processing" else "❌"
            label = f"{icon} #{v['id']} — {v['filename']} ({v['detections_count']} дет.)"
            video_options[label] = v["id"]

        selected = st.selectbox("Выберите видео:", list(video_options.keys()))
        selected_video_id = video_options[selected]

        # Найдём объект выбранного видео — нужен для duration/fps в превью
        for v in videos:
            if v["id"] == selected_video_id:
                selected_video_obj = v
                break

    st.markdown("---")

    # Графики — ТОЛЬКО видео-детекции (source="video")
    st.subheader("📊 Аналитика")

    dist = get_class_distribution(video_id=selected_video_id, source="video")
    timeline = get_timeline(video_id=selected_video_id, source="video", bucket=5.0)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Распределение по классам**")
        if dist:
            fig = px.pie(dist, values="count", names="class_name", hole=0.4)
            fig.update_layout(margin=dict(l=0, r=0, t=0, b=0))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Нет данных")

    with col2:
        st.markdown("**Детекции по времени**")
        if timeline:
            fig = px.bar(timeline, x="time_start", y="count",
                        labels={"time_start": "Время (сек)", "count": "Детекции"})
            fig.update_layout(margin=dict(l=0, r=0, t=0, b=0))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Нет данных")

    st.markdown("---")

    # ========================================================
    # ПРЕВЮ ВИДЕО — HTML5-плеер + таймлайн + два окна
    # ========================================================
    st.subheader("🎬 Превью видео")

    if selected_video_obj is None:
        st.info("Выберите видео выше, чтобы открыть превью 👆")
    elif selected_video_obj["status"] == "processing":
        st.warning("⏳ Видео ещё в обработке. Превью доступно после завершения.")
    elif selected_video_obj["status"] == "failed":
        st.error("❌ Обработка видео завершилась с ошибкой.")
    elif selected_video_obj["status"] not in ("completed", "partial"):
        st.warning(f"⚠️ Неизвестный статус видео: {selected_video_obj['status']}")
    else:
        # "completed" — обработано полностью, "partial" — уперлись в MAX_FRAMES,
        # но детекции ЕСТЬ и их можно показать. Длительность в обоих случаях =
        # длительность ВСЕГО файла (processor.py считает по total_frames),
        # так что слайдер скроллит весь таймлайн. За пределами обработанного
        # куска дашборд просто скажет "детекций нет".
        if selected_video_obj["status"] == "partial":
            st.info(
                "⚠️ Видео обработано частично (достигнут лимит `MAX_FRAMES`). "
                "Превью работает по всему таймлайну, но детекции есть только "
                "в первой части — на остальных моментах увидите «детекций нет»."
            )

        duration = selected_video_obj.get("duration") or 0
        if duration <= 0:
            st.error("Длительность видео неизвестна — превью недоступно.")
        else:
            # ---- Синхронный двойной плеер: 2 окна + общий таймлайн ----
            # Левое окно — чистое видео, правое — то же видео, но поверх него
            # canvas с боксами. Play/pause и скраббер ОДИН на оба: это один и
            # тот же presigned URL в двух <video>-элементах, синхронизацию
            # держит клиентский JS (см. SYNC_PLAYER_TEMPLATE).
            video_data = get_video_data(selected_video_id)
            video_url = (video_data or {}).get("video_url")
            if not video_url:
                st.warning(
                    "Не удалось получить URL видео. Возможно, MinIO недоступен "
                    "или video.object_key пустой."
                )
            else:
                # Боксы все разом: ts → список боксов. Инжект в HTML, дальше
                # JS сам находит ближайший обработанный кадр по currentTime.
                all_dets = get_video_all_detections(selected_video_id)
                by_ts = {}
                for d in all_dets:
                    bbox = d.get("bbox")
                    if not bbox or len(bbox) != 4 or d.get("timestamp") is None:
                        continue
                    by_ts.setdefault(round(float(d["timestamp"]), 3), []).append(
                        [
                            float(bbox[0]), float(bbox[1]),
                            float(bbox[2]), float(bbox[3]),
                            d.get("class_name", "?"),
                            float(d.get("confidence", 0.0)),
                        ]
                    )

                st.caption(
                    f"🎯 {len(all_dets)} детекций на {len(by_ts)} обработанных "
                    "кадрах. Оба окна — одно видео: слева чистое, справа с "
                    "боксами; таймлайн общий."
                )
                stc.html(
                    build_sync_player_html(video_url, by_ts),
                    height=560,
                    scrolling=True,
                )

    st.markdown("---")

    # Список видео
    st.subheader("📹 Все видео")

    if videos:
        for v in videos:
            icon = "✅" if v["status"] == "completed" else "⏳" if v["status"] == "processing" else "❌"
            with st.expander(f"{icon} #{v['id']} — {v['filename']}"):
                c1, c2, c3 = st.columns(3)
                with c1:
                    st.write(f"**Статус:** {v['status']}")
                    st.write(f"**FPS:** {v['fps'] or 'N/A'}")
                with c2:
                    st.write(f"**Длительность:** {v['duration']:.1f} сек" if v['duration'] else "**Длительность:** N/A")
                    st.write(f"**Детекций:** {v['detections_count']}")
                with c3:
                    st.write(f"**Загружено:** {v['uploaded_at'][:19] if v['uploaded_at'] else 'N/A'}")

                # Кнопка удаления
                if st.button("🗑 Удалить", key=f"del_video_{v['id']}"):
                    try:
                        r = requests.delete(f"{API_URL}/api/videos/{v['id']}", timeout=10)
                        if r.status_code == 200:
                            st.success(f"✅ Видео #{v['id']} удалено")
                            st.cache_data.clear()
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error(f"❌ Ошибка: {r.json().get('detail', 'Не удалось удалить')}")
                    except Exception as e:
                        st.error(f"❌ {e}")
    else:
        st.info("Пока нет загруженных видео. Загрузите через форму ниже 👇")

    st.markdown("---")

    # Загрузка видео
    st.subheader("📤 Загрузить видео")

    uploaded_file = st.file_uploader("Выберите видео-файл", type=["mp4", "avi", "mov", "mkv"])

    if uploaded_file is not None:
        if st.button("🚀 Загрузить и обработать"):
            files = {"file": (uploaded_file.name, uploaded_file, uploaded_file.type)}
            try:
                r = requests.post(f"{API_URL}/api/videos/upload", files=files, timeout=10)
                if r.status_code == 201:
                    data = r.json()
                    st.success(f"✅ Видео загружено! ID: {data['id']}")
                    st.info("⏳ Обработка запущена. Обновите страницу через минуту.")
                    st.cache_data.clear()
                    time.sleep(2)
                    st.rerun()
                else:
                    st.error(f"❌ Ошибка: {r.text}")
            except Exception as e:
                st.error(f"❌ Не удалось подключиться к API: {e}")


# ============================================================
# СТРАНИЦА: СТРИМЫ
# ============================================================
elif page == "📡 Стримы":
    # Статистика стримов
    stream_stats = get_stats(source="stream")
    if stream_stats:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("📡 Всего стримов", len(get_streams()))
        with col2:
            st.metric("🎯 Детекций со стримов", stream_stats["total_detections"])
        with col3:
            st.metric("📊 Средняя уверенность", f"{stream_stats['avg_confidence']:.1%}")

    st.markdown("---")

    # Автообновление каждые 5 сек — для аналитики (не превью!)
    if "last_refresh" not in st.session_state:
        st.session_state.last_refresh = time.time()

    if time.time() - st.session_state.last_refresh > 5:
        st.cache_data.clear()
        st.session_state.last_refresh = time.time()

    # Добавить стрим
    st.subheader("➕ Добавить RTSP стрим")

    with st.form("add_stream"):
        stream_name = st.text_input("Название", placeholder="Парковка камера 1")
        rtsp_url = st.text_input("RTSP URL", placeholder="rtsp://admin:pass@192.168.1.100:554/stream")
        stream_fps = st.slider("Частота захвата (FPS)", 1, 10, 5)
        submitted = st.form_submit_button("Добавить стрим")

        if submitted and stream_name and rtsp_url:
            try:
                r = requests.post(f"{API_URL}/api/streams", json={
                    "name": stream_name,
                    "rtsp_url": rtsp_url,
                    "fps": stream_fps,
                }, timeout=5)
                if r.status_code == 201:
                    st.success(f"✅ Стрим '{stream_name}' добавлен!")
                    st.cache_data.clear()
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error(f"❌ Ошибка: {r.text}")
            except Exception as e:
                st.error(f"❌ {e}")

    st.caption(
        "⚠️ Адрес источника должен открываться **изнутри контейнера `stream-manager`**: для встроенного "
        "MediaMTX — `rtsp://mediamtx:8554/<path>`; для источника на Windows-хосте — "
        "`rtsp://host.docker.internal:<port>/<path>`. `localhost` внутри контейнера — это сам контейнер. "
        "Если стрим не стартует — смотри `docker compose logs stream-manager --tail 50`: producer пишет туда причину."
    )

    st.markdown("---")

    # Список стримов с кнопками
    st.subheader("📡 Стримы")

    streams = get_streams()

    if streams:
        for s in streams:
            icon = "🟢" if s["status"] == "active" else "🔴"

            col_info, col_start, col_del = st.columns([5, 1, 1])

            with col_info:
                st.markdown(
                    f"{icon} **#{s['id']} — {s['name']}** "
                    f"({s['detections_count']} дет.) | "
                    f"`{s['rtsp_url']}` | FPS: {s['fps']} | {s['status']}"
                )

            with col_start:
                if s["status"] == "inactive":
                    if st.button("▶️", key=f"start_{s['id']}", help="Запустить"):
                        try:
                            r = requests.post(f"{API_URL}/api/streams/{s['id']}/start", timeout=5)
                            if r.status_code == 200:
                                st.success("✅ Стрим запущен!")
                                st.cache_data.clear()
                                time.sleep(1)
                                st.rerun()
                            else:
                                st.error(f"❌ {r.json().get('detail', 'Ошибка')}")
                        except Exception as e:
                            st.error(f"❌ {e}")
                else:
                    if st.button("⏹", key=f"stop_{s['id']}", help="Остановить"):
                        try:
                            r = requests.post(f"{API_URL}/api/streams/{s['id']}/stop", timeout=5)
                            if r.status_code == 200:
                                st.success("✅ Стрим остановлен")
                                st.cache_data.clear()
                                time.sleep(1)
                                st.rerun()
                            else:
                                st.error(f"❌ {r.json().get('detail', 'Ошибка')}")
                        except Exception as e:
                            st.error(f"❌ {e}")

            with col_del:
                if st.button("🗑", key=f"del_{s['id']}", help="Удалить"):
                    try:
                        r = requests.delete(f"{API_URL}/api/streams/{s['id']}", timeout=5)
                        if r.status_code == 200:
                            st.success("✅ Удалён")
                            st.cache_data.clear()
                            time.sleep(1)
                            st.rerun()
                    except Exception as e:
                        st.error(f"❌ {e}")

            st.markdown("---")

        # ========================================================
        # ПРЕВЮ СТРИМА — два окна: чистый кадр / с детекциями
        # ========================================================
        st.subheader("🖼 Превью стримов")

        stream_options = {"Все стримы": None}
        for s in streams:
            stream_options[f"#{s['id']} — {s['name']}"] = s["id"]

        prev_sel = st.selectbox(
            "Стрим для превью:",
            list(stream_options.keys()),
            key="stream_preview_select",
        )
        prev_stream_id = stream_options[prev_sel]

        # Слайдер частоты опроса — пользователь может сам решить, насколько
        # «живо» он хочет видеть картинку. 2 сек — нормальный компромисс:
        # плавность vs. нагрузка на api-контейнер и MinIO.
        poll_interval = st.slider(
            "Интервал авто-обновления (сек)",
            min_value=2,
            max_value=10,
            value=3,
            step=1,
            help="Дашборд будет запрашивать последний кадр из MinIO с этой частотой. "
                 "Меньше = плавнее, но больше нагрузки на API.",
        )

        # По умолчанию ВКЛ — пользователь expects живое превью, не стату.
        # Раньше default=False — пользователь не понимал почему кадр не
        # обновляется, ведь он не нажимал чекбокс.
        auto_refresh = st.checkbox(
            "🔄 Авто-обновление (живое превью)",
            value=True,
            help="Дашборд сам опрашивает /latest-frame каждые N секунд и "
                 "перерисовывает кадр. Снимите галочку, если хотите "
                 "вручную листать кнопкой ниже.",
        )

        if prev_stream_id is None:
            st.info("Выберите конкретный стрим выше, чтобы открыть превью 👆")
        else:
            # Запрос последнего кадра
            latest = get_latest_stream_frame(prev_stream_id)

            if latest is None:
                st.warning(
                    "Пока нет сохранённых кадров. Кадры стрима сохраняются в MinIO "
                    "ТОЛЬКО когда YOLO находит детекции (см. yolo_consumer.py:139 — "
                    "пустые кадры не пишутся, чтобы не занимать диск). Запустите "
                    "стрим и подождите, пока в кадре появится что-то детектируемое."
                )
            else:
                frame_url = latest["frame_url"]
                frame_index = latest["frame_index"]
                det_count_latest = latest["detections_count"]

                # Скачиваем байты картинки — st.image по URL кэширует,
                # а нам нужно свежее изображение каждые N сек.
                img_bytes = fetch_image_bytes(frame_url)

                # Боксы для этого кадра
                dets = get_stream_detections(
                    prev_stream_id,
                    frame_index=frame_index,
                )

                col_raw, col_boxed = st.columns(2)

                with col_raw:
                    st.markdown("**Стрим (чистый кадр)**")
                    if img_bytes:
                        html_raw = render_frame_with_boxes_html(img_bytes, [])
                        if html_raw:
                            st.markdown(html_raw, unsafe_allow_html=True)
                        else:
                            st.error("Не удалось отрисовать кадр.")
                    else:
                        st.error("Не удалось скачать кадр из MinIO.")
                    st.caption(
                        f"📷 Frame #{frame_index} | "
                        f"🎯 Детекций на кадре: **{det_count_latest}** | "
                        f" polled at {time.strftime('%H:%M:%S')}"
                    )

                with col_boxed:
                    st.markdown("**Стрим с детекциями**")
                    if img_bytes:
                        if not dets:
                            st.info("На этом кадре детекций нет (видимо, их и не было).")
                            html_no = render_frame_with_boxes_html(img_bytes, [])
                            if html_no:
                                st.markdown(html_no, unsafe_allow_html=True)
                        else:
                            html_boxed = render_frame_with_boxes_html(img_bytes, dets)
                            if html_boxed:
                                st.markdown(html_boxed, unsafe_allow_html=True)
                            else:
                                st.error("Не удалось отрисовать кадр.")
                    else:
                        st.warning("Сначала нужно получить чистый кадр.")

                # Подсказка про «почему кадр не обновляется»
                # frame_index хранится в session_state с прошлого poll'а.
                prev_idx_key = f"prev_frame_idx_{prev_stream_id}"
                prev_idx = st.session_state.get(prev_idx_key)
                if prev_idx is not None and prev_idx == frame_index:
                    # frame_index не поменялся с прошлого раза → новых детекций нет
                    st.caption(
                        "ℹ️ Кадр тот же, что и в прошлый poll — новых детекций "
                        "с прошлого раза не появилось. Превью обновится, когда "
                        "YOLO сохранит следующий кадр (если в кадре есть объекты)."
                    )
                st.session_state[prev_idx_key] = frame_index

                # САМО ПЕРЕЗАПУСКАЕМ СТРАНИЦУ ЗДЕСЬ НЕЛЬЗЯ: st.rerun() обрывает
                # выполнение скрипта, и секции ниже (аналитика, метрики,
                # последние детекции) никогда не отрисовываются. Блок
                # авто-обновления — в самом конце страницы стримов.

        st.markdown("---")

        # ========================================================
        # Аналитика стримов (как было раньше — без изменений)
        # ========================================================
        st.subheader("📊 Аналитика стримов")

        stream_options_an = {"Все стримы": None}
        for s in streams:
            stream_options_an[f"#{s['id']} — {s['name']}"] = s["id"]

        sel = st.selectbox("Стрим:", list(stream_options_an.keys()), key="stream_analytics")
        sel_stream_id = stream_options_an[sel]

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Распределение по классам**")
            dist = get_class_distribution(stream_id=sel_stream_id, source="stream")
            if dist:
                fig = px.pie(dist, values="count", names="class_name", hole=0.4)
                fig.update_layout(margin=dict(l=0, r=0, t=0, b=0))
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Нет данных")

        with col2:
            st.markdown("**Активность (детекции/мин)**")
            activity = get_stream_activity(stream_id=sel_stream_id, minutes=10)
            if activity:
                fig = px.bar(activity, x="time", y="count",
                            labels={"time": "Время", "count": "Детекции"})
                fig.update_layout(margin=dict(l=0, r=0, t=0, b=0))
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Нет данных за последние 10 минут")

        st.markdown("---")

        # Последние детекции
        st.subheader("📋 Последние детекции")

        if sel_stream_id:
            detections = get_stream_detections(sel_stream_id, limit=50)
        else:
            detections = []
            for s in streams:
                if s["detections_count"] > 0:
                    detections = get_stream_detections(s["id"], limit=20)
                    break

        if detections:
            for d in detections[:20]:
                st.write(
                    f"• **{d['class_name']}** ({d['confidence']:.0%}) "
                    f"— frame #{d['frame_index']} "
                    f"— {d['detected_at'][:19]}"
                )
        else:
            st.info("Нет детекций")

        # ========================================================
        # Авто-обновление — СТРОГО в конце страницы.
        # st.rerun() обрывает выполнение скрипта: если поставить его выше,
        # секции «Аналитика стримов» и «Последние детекции» не успевают
        # отрисоваться и метрики «не показываются».
        # Работает и когда кадров ещё нет (latest is None) — превью
        # «подхватит» первый кадр, как только YOLO его сохранит.
        # ========================================================
        if auto_refresh:
            # Прогресс-бар как индикатор, что не зависли
            progress = st.progress(0.0)
            for i in range(poll_interval * 10):
                progress.progress((i + 1) / (poll_interval * 10))
                time.sleep(0.1)
            progress.empty()
            st.cache_data.clear()  # сбрасываем кэш streams/аналитики
            st.rerun()

    else:
        st.info("Нет добавленных стримов. Добавьте через форму выше 👆")

    st.markdown("---")

    with st.expander("ℹ️ Стрим не работает — где смотреть причину"):
        st.code(
            "# Producer стрима — это subprocess внутри контейнера api,\n"
            "# его логи (и падения) прилетают сюда:\n"
            "docker compose logs api --tail 50\n\n"
            "# Инференс (YOLO) и запись в БД:\n"
            "docker compose logs yolo-consumer --tail 30\n"
            "docker compose logs detection-consumer --tail 30\n\n"
            "# Топики Kafka (создаются сервисом kafka-init):\n"
            "docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list\n\n"
            "# Ручной запуск producer (адрес — с точки зрения КОНТЕЙНЕРА!):\n"
            "docker compose exec api python -m app.streaming.producer rtsp://mediamtx:8554/mystream 5 1\n\n"
            "# Тест без камеры: положи .mp4 в ./uploads — в контейнере это /app/uploads/…\n"
            "docker compose exec api python -m app.streaming.producer /app/uploads/demo.mp4 5 1",
            language="bash",
        )
