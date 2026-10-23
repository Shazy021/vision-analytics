# -*- coding: utf-8 -*-
"""
Streamlit-дашборд для Vision Analytics Platform (v2 — визуальная прокачка).

Что нового относительно v1:
  - Единая тёмная тема: .streamlit/config.toml + CSS-карточки метрик
  - Секционные заголовки с акцентной полоской, бейджи статусов, тосты
  - Plotly-графики под тёмную тему (donut с числом в центре)
  - Sidebar: брендинг + панель здоровья системы
  - Живое превью и аналитика стримов через @st.fragment(run_every=...)
    (требуется Streamlit >= 1.37) — страница больше НЕ перерисовывается целиком

Страницы:
  Видео   — список + аналитика + синхронный двойной плеер (чистый / с боксами)
  Стримы  — список + аналитика + два окна превью (чистый/с детекциями)
"""
import base64
import io
import json
import os
import statistics
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
    initial_sidebar_state="expanded",
)

API_URL = os.environ.get("API_URL", "http://localhost:8000")

# ---- Палитра (должна совпадать с .streamlit/config.toml) ----
CARD = "#1A1F2B"
BORDER = "#2A3040"
ACCENT = "#a8bff5"
MUTED = "#8B93A1"
BG = "#0E1117"


# ============================================================
# CSS: карточки, заголовки, пустые состояния
# ============================================================
def load_css():
    st.markdown(
        """
        <style>
            /* Карточки метрик */
            div[data-testid="stMetric"] {
                background-color: #1A1F2B;
                border: 1px solid #2A3040;
                border-radius: 12px;
                padding: 18px 20px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.25);
            }
            div[data-testid="stMetricLabel"] p {
                font-size: 13px !important;
                color: #8B93A1 !important;
                text-transform: uppercase;
                letter-spacing: 0.5px;
            }
            div[data-testid="stMetricValue"] {
                font-size: 30px;
                font-weight: 700;
            }
            /* Кнопки — акцентная обводка при наведении */
            div[data-testid="stBaseButton-secondary"] {
                border-color: #2A3040;
            }
            /* Слегка сжать вертикальные отступы между блоками */
            div[data-testid="stMarkdownContainer"] hr { margin: 6px 0; }
        </style>
        """,
        unsafe_allow_html=True,
    )


load_css()


def section_header(text: str) -> str:
    """Заголовок секции с акцентной полоской слева."""
    return (
        f'<p style="border-left:4px solid {ACCENT};padding-left:10px;'
        f'font-size:1.25rem;font-weight:700;margin:8px 0 12px 0;">{text}</p>'
    )


def status_badge(status: str) -> str:
    """Цветной бейдж статуса стрима (active/inactive/...)."""
    colors = {
        "active": ("#1A7F37", "#DAFBE1"),
        "inactive": ("#57606A", "#E7EBEF"),
        "error": ("#B62324", "#FFD8D8"),
    }
    fg, bg = colors.get(status, ("#BF8700", "#FFF3D1"))
    return (
        f'<span style="background:{bg};color:{fg};padding:3px 10px;'
        f'border-radius:20px;font-size:12px;font-weight:600;'
        f'text-transform:uppercase;letter-spacing:0.4px;">{status}</span>'
    )


def empty_state(icon: str, title: str, hint: str = "") -> None:
    """Красивое пустое состояние вместо серого st.info."""
    st.markdown(
        f"""
        <div style="text-align:center;padding:30px;border:1px dashed {BORDER};
                    border-radius:12px;background:{CARD};">
          <div style="font-size:36px;">{icon}</div>
          <div style="font-weight:600;margin:8px 0 4px 0;">{title}</div>
          <div style="color:{MUTED};font-size:13px;">{hint}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def dark_fig(fig, height: int = 300):
    """Привести Plotly-фигуру к тёмной теме приложения."""
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=0, r=0, t=28, b=44),  # b=44 — место под легенду, иначе она на диаграмме
        height=height,
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="top", y=-0.06,
                    xanchor="center", x=0.5),
    )
    fig.update_xaxes(gridcolor=BORDER)
    fig.update_yaxes(gridcolor=BORDER)
    return fig


def collapse_small(dist, min_pct: float = 1.0):
    """Свернуть классы легче min_pct% в один серый «другое».

    15 тонких секторов donut не читаются: подписи мелких секторов Plotly
    выносит наружу, и они складываются в «гребёнку». Хвост оставляем
    одним сектором — сумма долей остаётся 100%. «Другое» не чёрный ящик:
    в него кладём members — человекочитаемый список свёрнутых классов,
    donut_chart показывает его в hover.
    """
    total = sum(d["count"] for d in dist) or 1
    big = [d for d in dist if d["count"] / total * 100 >= min_pct]
    rest = sorted((d for d in dist if d["count"] / total * 100 < min_pct),
                  key=lambda d: -d["count"])
    if rest:
        names = [f"{d['class_name']} ({d['count']})" for d in rest[:8]]
        if len(rest) > 8:
            names.append(f"и ещё {len(rest) - 8}")
        big = big + [{
            "class_name": "другое",
            "count": sum(d["count"] for d in rest),
            "members": ", ".join(names),
        }]
    return big or dist


def donut_chart(dist, height: int = 320):
    """Пончик распределения по классам.

    Проценты рисуются только ВНУТРИ секторов >= 3% (textinfo="text" +
    textposition="inside" — иначе Plotly выносит подписи мелких секторов
    наружу); мелочь свёрнута в «другое»; точные значения — в hover.
    """
    if not dist:
        return None
    dist = collapse_small(dist)
    total = sum(d["count"] for d in dist)
    colors = {d["class_name"]: CHART_PALETTE[i % len(CHART_PALETTE)]
              for i, d in enumerate(dist)}
    colors["другое"] = "#57606A"
    fig = px.pie(dist, values="count", names="class_name", hole=0.65,
                 color="class_name", color_discrete_map=colors)
    fig.add_annotation(text=f"<b>{total}</b>", showarrow=False,
                       font=dict(size=26, color="#E6EDF3"))
    # px.pie — ВСЕГДА один trace (color лишь красит сектора через
    # color_discrete_map), поэтому списки text/customdata корректно
    # сопоставляются с метками по индексу.
    percents = [d["count"] / total * 100 for d in dist]
    fig.update_traces(
        marker=dict(line=dict(color=BG, width=2)),
        text=[f"{p:.1f}%" if p >= 3 else "" for p in percents],
        textinfo="text",
        textposition="inside",
        insidetextorientation="horizontal",
        sort=True,
        customdata=[d.get("members", "") for d in dist],
        # У «другого» в customdata лежит список свёрнутых классов —
        # показываем его второй строкой hover; у обычных секторов строка
        # пустая. <extra></extra> глушит стандартный блок имени trace.
        hovertemplate="%{label}: %{value} дет. (%{percent})"
                      "<br>%{customdata}<extra></extra>",
    )
    return dark_fig(fig, height)


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
        r = requests.get(
            f"{API_URL}/api/analytics/class-distribution", params=params, timeout=5
        )
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
        r = requests.get(
            f"{API_URL}/api/analytics/timeline", params=params, timeout=5
        )
        return r.json() if r.status_code == 200 else []
    except Exception:
        return []


@st.cache_data(ttl=10)
def get_stream_activity(stream_id=None, minutes=10):
    try:
        params = {"minutes": minutes}
        if stream_id:
            params["stream_id"] = stream_id
        r = requests.get(
            f"{API_URL}/api/analytics/stream-activity", params=params, timeout=5
        )
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
    запроса к серверу на кадр.
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
    """Полные данные видео: duration, fps, status, video_url (presigned)."""
    try:
        r = requests.get(f"{API_URL}/api/videos/{video_id}", timeout=5)
        return r.json() if r.status_code == 200 else None
    except Exception:
        return None


@st.cache_data(ttl=300)
def get_processing_params():
    """Параметры разбора видео (extract_fps, max_frames) — конфигурация API."""
    try:
        r = requests.get(f"{API_URL}/api/videos/params", timeout=5)
        return r.json() if r.status_code == 200 else None
    except Exception:
        return None


def search_params_caption(extract_fps=None, max_frames=None) -> str:
    """Строка «где ищем детекции»: частота выборки кадров и потолок.

    extract_fps/max_frames — параметры конкретного видео (из ответа API);
    None — берём платформенные дефолты из /api/videos/params.
    """
    p = get_processing_params() or {}
    ef = extract_fps or p.get("extract_fps")
    mf = p.get("max_frames") if max_frames is None else max_frames
    if not ef:
        return ""
    limit = ("без лимита" if not mf
             else f"лимит {mf} прочитанных кадров")
    return f"🔎 Детекции ищем ~{ef} раз/с, {limit}."


def get_latest_stream_frame(stream_id):
    """Последний кадр стрима из MinIO: {frame_index, frame_url, detections_count}."""
    try:
        r = requests.get(f"{API_URL}/api/streams/{stream_id}/latest-frame", timeout=5)
        if r.status_code == 200:
            return r.json()
        return None
    except Exception:
        return None


def fetch_image_bytes(url, timeout=5):
    try:
        r = requests.get(url, timeout=timeout)
        if r.status_code == 200:
            return r.content
        return None
    except Exception:
        return None


# ============================================================
# РИСОВАНИЕ БОКСОВ (CSS-overlay)
# ============================================================
# Спокойная категорная палитра (AntV G2): равная «плотность» цветов,
# различимы на тёмном фоне без неона. Используется и для боксов, и для
# секторов диаграмм — дашборд выглядит цельно.
CHART_PALETTE = [
    "#5B8FF9", "#5AD8A6", "#F6BD16", "#E8684A", "#6DC8EC",
    "#9270CA", "#FF9D4D", "#269A99", "#5D7092", "#FF99C3",
]

CLASS_COLORS = {
    "person": "#5B8FF9",
    "bicycle": "#5AD8A6",
    "car": "#F6BD16",
    "motorcycle": "#E8684A",
    "airplane": "#6DC8EC",
    "bus": "#9270CA",
    "train": "#FF9D4D",
    "truck": "#269A99",
    "boat": "#5D7092",
    "traffic light": "#FF99C3",
    "fire hydrant": "#3D6FD9",
    "stop sign": "#3FA97A",
    "parking meter": "#C99A0F",
    "bench": "#B24A31",
    "bird": "#4A98BE",
    "cat": "#6F54A0",
    "dog": "#C97531",
    "horse": "#1D7A79",
}
DEFAULT_COLOR = "#8B93A1"


def render_frame_with_boxes_html(image_bytes, detections):
    """
    Наложить боксы детекций на картинку через CSS-overlay.

    Боксы позиционируются в процентах от размеров изображения, поэтому
    точно садятся на объекты при любой ширине контейнера.
    """
    if not image_bytes:
        return None

    try:
        img = Image.open(io.BytesIO(image_bytes))
        img_w, img_h = img.size
    except Exception:
        return None

    img_b64 = base64.b64encode(image_bytes).decode("utf-8")

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
        "</div>"
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

        boxes_html_parts.append(
            box_template.format(
                x1=(x1 / img_w) * 100.0 if img_w else 0,
                y1=(y1 / img_h) * 100.0 if img_h else 0,
                w=((x2 - x1) / img_w) * 100.0 if img_w else 0,
                h=((y2 - y1) / img_h) * 100.0 if img_h else 0,
                color=color,
                label=label,
            )
        )

    return (
        '<div style="position:relative;width:100%;'
        'line-height:0;">'
        f'<img src="data:image/jpeg;base64,{img_b64}" '
        'style="width:100%;display:block;" alt="frame" />'
        + "".join(boxes_html_parts)
        + "</div>"
    )


# ============================================================
# DETECTION GALLERY — карточки объектов (кроп bbox + подпись)
# ============================================================
CROP_PAD = 0.3          # запас вокруг бокса (доля от его стороны)
GALLERY_MAX_CARDS = 24  # жёсткий потолок: каждая карточка — кроп с MinIO-кадра


@st.cache_data(ttl=600, show_spinner=False)
def fetch_frame_bytes_by_index(stream_id, frame_index):
    """Чистый кадр по номеру: /frames/{idx} → presigned URL → байты.

    Ключ streams/{id}/{idx}.jpg неизменяем (кадры не перезаписываются),
    поэтому кеш по (stream_id, frame_index) корректен: каждый кадр
    скачивается из MinIO один раз за сессию. ttl — страховка на случай
    пересоздания стрима с тем же id (нумерация кадров тогда начнётся с 0).
    """
    try:
        r = requests.get(
            f"{API_URL}/api/streams/{stream_id}/frames/{frame_index}", timeout=5
        )
        if r.status_code != 200:
            return None
        return fetch_image_bytes(r.json()["frame_url"])
    except Exception:
        return None


def crop_object(img_bytes, bbox, pad=CROP_PAD):
    """Вырезать бокс с запасом pad; вернуть base64 JPEG для <img>."""
    try:
        img = Image.open(io.BytesIO(img_bytes)).convert("RGB")
        w, h = img.size
        x1, y1, x2, y2 = bbox
        pw, ph = (x2 - x1) * pad, (y2 - y1) * pad
        box = (
            max(0, int(x1 - pw)), max(0, int(y1 - ph)),
            min(w, int(x2 + pw)), min(h, int(y2 + ph)),
        )
        if box[2] - box[0] < 8 or box[3] - box[1] < 8:
            return None
        c = img.crop(box)
        c.thumbnail((320, 320))
        buf = io.BytesIO()
        c.save(buf, format="JPEG", quality=85)
        return base64.b64encode(buf.getvalue()).decode()
    except Exception:
        return None


def detections_gallery_html(stream_id, detections, cards=12):
    """Сетка карточек: дедуп по frame_index, каждый объект — отдельный кроп.

    Один и тот же автобус детектится каждые несколько кадров — голый список
    детекций превращается в простыню дублей. Поэтому группировка: берём
    первые `cards` УНИКАЛЬНЫХ кадров (detections отсортированы по свежести),
    внутри кадра — по карточке на объект.
    """
    by_frame = {}
    for d in detections:
        by_frame.setdefault(d["frame_index"], []).append(d)

    cards_html = []
    for fi, dets in list(by_frame.items())[:cards]:
        img_bytes = fetch_frame_bytes_by_index(stream_id, fi)
        if not img_bytes:
            continue
        for d in dets:
            b64 = crop_object(img_bytes, d["bbox"])
            if not b64:
                continue
            color = CLASS_COLORS.get(d["class_name"], DEFAULT_COLOR)
            ts = d["detected_at"][11:19] if len(d["detected_at"]) >= 19 else ""
            # Только inline-стили: st.markdown вырезает class у пользовательского
            # HTML (тот же приём, что в render_frame_with_boxes_html).
            cards_html.append(
                f'<div style="background:{CARD};border:1px solid {BORDER};'
                'border-radius:10px;overflow:hidden;">'
                f'<div style="position:relative;line-height:0;'
                f'border-bottom:2px solid {color};">'
                f'<img src="data:image/jpeg;base64,{b64}" alt="" '
                'style="width:100%;aspect-ratio:4/3;object-fit:cover;display:block;"/>'
                f'<span style="position:absolute;bottom:6px;right:6px;'
                f'background:{color};color:#fff;font-size:11px;font-weight:700;'
                'padding:2px 7px;border-radius:10px;line-height:1.4;">'
                f'{d["confidence"]:.0%}</span>'
                '</div>'
                '<div style="padding:7px 10px 9px 10px;font-size:12px;'
                'line-height:1.5;">'
                f'<b>{d["class_name"]}</b>'
                f'<span style="color:{MUTED};display:block;">'
                f'frame #{fi} · {ts}</span></div></div>'
            )
            if len(cards_html) >= GALLERY_MAX_CARDS:
                break
        if len(cards_html) >= GALLERY_MAX_CARDS:
            break

    if not cards_html:
        return None

    return (
        '<div style="display:grid;'
        'grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:10px;">'
        + "".join(cards_html)
        + "</div>"
    )


# ============================================================
# СИНХРОННЫЙ ДВОЙНОЙ ПЛЕЕР (видео)
# ============================================================
# Два <video> с одним и тем же presigned URL: левое — чистое, правое —
# поверх canvas с боксами. Общий play/pause + скраббер, синхронизация
# и отрисовка — клиентским JS. Данные боксов инжектятся как JSON.
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
const DETS = __DETS_JSON__;
const BOX_TTL = __TTL__;      // сек: боксы старше этого гасятся
const COLORS = __COLORS_JSON__;
const DEFAULT_COLOR = "#8B93A1";
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
v2.muted = true;
const TS = Object.keys(DETS).map(Number).sort((a, b) => a - b);
let showBoxes = true;
let scrubbing = false;

function nearestDets(t) {
  if (!TS.length) return null;
  let lo = 0, hi = TS.length - 1, idx = -1;
  while (lo <= hi) {
    const m = (lo + hi) >> 1;
    if (TS[m] <= t + 0.05) { idx = m; lo = m + 1; } else { hi = m - 1; }
  }
  if (idx < 0) return null;                  // до первого обработанного кадра — боксов нет
  if (t - TS[idx] > BOX_TTL) return null;    // кадр протух — гасим
  return DETS[TS[idx]];
}
function resizeCanvas() {
  const w = v2.clientWidth, h = v2.clientHeight;
  if (canvas.width !== w || canvas.height !== h) { canvas.width = w; canvas.height = h; }
}
function draw() {
  resizeCanvas();
  ctx.clearRect(0, 0, canvas.width, canvas.height);   // чистим всегда
  if (!showBoxes || !v2.videoWidth) return;
  const dets = nearestDets(v2.currentTime);
  if (!dets) return;                                   // нет свежих — экран чистый
  const sx = canvas.width / v2.videoWidth, sy = canvas.height / v2.videoHeight;
  ctx.lineWidth = 2;
  ctx.font = "bold 12px Arial";
  for (const d of dets) {                              // ← берём уже найденное
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
    # TTL боксов: если до ближайшего обработанного кадра дальше,
    # чем это значение — боксы не рисуем (объект ушёл, держать нечего).
    ts_sorted = sorted(detections_by_ts)
    diffs = [b - a for a, b in zip(ts_sorted, ts_sorted[1:])]
    if diffs:
        box_ttl = max(0.1, min(statistics.median(diffs) * 1.5, 1.0))
    else:
        box_ttl = 0.1

    return (
        SYNC_PLAYER_TEMPLATE.replace("__URL_JSON__", json.dumps(video_url))
        .replace("__DETS_JSON__", json.dumps(detections_by_ts, separators=(",", ":")))
        .replace("__COLORS_JSON__", json.dumps(CLASS_COLORS))
        .replace("__TTL__", f"{box_ttl:.3f}")
    )

# ============================================================
# SIDEBAR: брендинг + здоровье системы
# ============================================================
st.sidebar.markdown(
    f"""
    <div style="padding:6px 4px 14px 4px;">
      <div style="font-size:22px;font-weight:800;letter-spacing:0.3px;">
        🔍 Vision <span style="color:{ACCENT};">Analytics</span>
      </div>
      <div style="color:{MUTED};font-size:12px;margin-top:2px;">
        video &amp; stream detection platform
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

page = st.sidebar.selectbox("Навигация", ["📹 Видео", "📡 Стримы"])


def health_html():
    """Карточка здоровья системы: API, видео, стримы."""
    stats = get_stats()
    streams = get_streams()
    api_ok = stats is not None
    dot = f'<span style="color:{ACCENT};font-size:11px;">●</span>' if api_ok else \
          '<span style="color:#F85149;font-size:11px;">●</span>'
    n_streams = len(streams) if streams else 0
    n_active = sum(1 for s in (streams or []) if s.get("status") == "active")
    return f"""
    <div style="background:{CARD};border:1px solid {BORDER};border-radius:12px;
                padding:14px 16px;margin-top:10px;font-size:13px;">
      <div style="font-weight:700;margin-bottom:8px;text-transform:uppercase;
                  font-size:11px;letter-spacing:0.5px;color:{MUTED};">
        Состояние системы
      </div>
      <div style="margin-bottom:4px;">{dot} API: {'онлайн' if api_ok else 'недоступен'}</div>
      <div style="margin-bottom:4px;">📹 Видео: <b>{(stats or {}).get('total_videos', '—')}</b></div>
      <div style="margin-bottom:4px;">📡 Стримы: <b>{n_streams}</b> (активных: <b>{n_active}</b>)</div>
      <div>🤖 Модель: <b>YOLO</b></div>
    </div>
    """


st.sidebar.markdown(health_html(), unsafe_allow_html=True)

# ============================================================
# ЗАГОЛОВОК
# ============================================================
st.markdown(
    f"""
    <h1 style="margin-bottom:2px;">🔍 Vision Analytics</h1>
    <p style="color:{MUTED};margin-top:0;">детекции на видео и живых стримах</p>
    """,
    unsafe_allow_html=True,
)

# ============================================================
# СТРАНИЦА: ВИДЕО
# ============================================================
if page == "📹 Видео":
    stats = get_stats(source="video")
    if stats:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Всего видео", stats["total_videos"])
        with col2:
            st.metric("Всего детекций", stats["total_detections"])
        with col3:
            st.metric("Средняя уверенность", f"{stats['avg_confidence']:.1%}")
    else:
        st.warning("⚠️ Не удалось подключиться к API")

    videos = get_videos()
    selected_video_id = None
    selected_video_obj = None

    if videos:
        video_options = {"Все видео": None}
        for v in videos:
            icon = (
                "✅" if v["status"] == "completed"
                else "⏳" if v["status"] == "processing"
                else "❌"
            )
            label = f"{icon} #{v['id']} — {v['filename']} ({v['detections_count']} дет.)"
            video_options[label] = v["id"]

        selected = st.selectbox("Выберите видео:", list(video_options.keys()))
        selected_video_id = video_options[selected]

        for v in videos:
            if v["id"] == selected_video_id:
                selected_video_obj = v
                break
    else:
        empty_state("🎬", "Пока нет загруженных видео",
                    "Загрузите первый файл через форму ниже — и здесь появится аналитика.")

    # ---- Аналитика ----
    st.markdown(section_header("📊 Аналитика"), unsafe_allow_html=True)

    dist = get_class_distribution(video_id=selected_video_id, source="video")
    timeline = get_timeline(video_id=selected_video_id, source="video", bucket=5.0)

    col1, col2 = st.columns(2)

    with col1:
        if dist:
            st.plotly_chart(donut_chart(dist), use_container_width=True)
        else:
            empty_state("📊", "Нет данных по классам",
                        "Данные появятся после обработки видео.")

    with col2:
        if timeline:
            fig = px.bar(
                timeline, x="time_start", y="count",
                labels={"time_start": "Время (сек)", "count": "Детекции"},
                color_discrete_sequence=[ACCENT],
            )
            st.plotly_chart(dark_fig(fig, 320), use_container_width=True)
        else:
            empty_state("⏱", "Нет данных по времени",
                        "Таймлайн детекций появится после обработки.")

    # ---- Превью видео ----
    st.markdown(section_header("🎬 Превью видео"), unsafe_allow_html=True)

    if selected_video_obj is None:
        pass  # empty_state уже показан выше
    elif selected_video_obj["status"] == "processing":
        @st.fragment(run_every=2)
        def video_progress_fragment(video_id=None):
            data = get_video_data(video_id) or {}
            done = data.get("processed_frames")
            total = data.get("frames_to_process")
            if total:
                frac = min(1.0, (done or 0) / total)
                st.progress(frac, text=f"⏳ Обработка: {done or 0} / {total} кадров")
            else:
                st.progress(0.0, text="⏳ Воркер открывает видео…")
            st.caption(
                search_params_caption(
                    data.get("extract_fps"), data.get("max_frames")
                )
            )
            if data.get("status") != "processing":
                # Готово (completed/partial/failed) — сбрасываем кеш списков
                # и перерисовываем страницу уже с превью.
                st.cache_data.clear()
                st.rerun()

        video_progress_fragment(selected_video_id)
    elif selected_video_obj["status"] == "failed":
        st.error("❌ Обработка видео завершилась с ошибкой.")
    elif selected_video_obj["status"] not in ("completed", "partial"):
        st.warning(f"⚠️ Неизвестный статус видео: {selected_video_obj['status']}")
    else:
        if selected_video_obj["status"] == "partial":
            st.info(
                "⚠️ Видео обработано частично (лимит MAX_FRAMES). Превью работает "
                "по всему таймлайну, но детекции есть только в обработанной части."
            )

        duration = selected_video_obj.get("duration") or 0
        if duration <= 0:
            st.error("Длительность видео неизвестна — превью недоступно.")
        else:
            video_data = get_video_data(selected_video_id)
            video_url = (video_data or {}).get("video_url")
            if not video_url:
                st.warning(
                    "Не удалось получить URL видео. Возможно, MinIO недоступен "
                    "или video.object_key пустой."
                )
            else:
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
                    "кадрах. Оба окна — одно видео: слева чистое, справа с боксами."
                )
                stc.html(build_sync_player_html(video_url, by_ts), height=560,
                         scrolling=True)

    # ---- Список видео ----
    st.markdown(section_header("📹 Все видео"), unsafe_allow_html=True)

    if videos:
        for v in videos:
            icon = (
                "✅" if v["status"] == "completed"
                else "⏳" if v["status"] == "processing"
                else "❌"
            )
            with st.expander(f"{icon} #{v['id']} — {v['filename']}"):
                c1, c2, c3 = st.columns(3)
                with c1:
                    st.write(f"**Статус:** {v['status']}")
                    st.write(f"**FPS:** {v['fps'] or 'N/A'}")
                with c2:
                    st.write(
                        f"**Длительность:** {v['duration']:.1f} сек"
                        if v["duration"] else "**Длительность:** N/A"
                    )
                    st.write(f"**Детекций:** {v['detections_count']}")
                with c3:
                    st.write(
                        f"**Загружено:** {v['uploaded_at'][:19] if v['uploaded_at'] else 'N/A'}"
                    )

                if v["status"] == "processing" and v.get("frames_to_process"):
                    done = v.get("processed_frames") or 0
                    total = v["frames_to_process"]
                    st.progress(min(1.0, done / total),
                                text=f"⏳ {done} / {total} кадров")

                if st.button("🗑 Удалить", key=f"del_video_{v['id']}"):
                    try:
                        r = requests.delete(
                            f"{API_URL}/api/videos/{v['id']}", timeout=10
                        )
                        if r.status_code == 200:
                            st.toast(f"🗑 Видео #{v['id']} удалено")
                            st.cache_data.clear()
                            time.sleep(0.6)
                            st.rerun()
                        else:
                            st.error(
                                f"❌ Ошибка: {r.json().get('detail', 'Не удалось удалить')}"
                            )
                    except Exception as e:
                        st.error(f"❌ {e}")

    # ---- Загрузка видео ----
    st.markdown(section_header("📤 Загрузить видео"), unsafe_allow_html=True)

    uploaded_file = st.file_uploader(
        "Выберите видео-файл", type=["mp4", "avi", "mov", "mkv"]
    )

    # Контролы параметров появляются только когда файл выбран; после
    # успешной загрузки uploader очищается (st.rerun) и они исчезают.
    if uploaded_file is not None:
        p_defaults = get_processing_params() or {}
        uc1, uc2 = st.columns(2)
        with uc1:
            up_extract_fps = st.slider(
                "Частота выборки (кадров/с)", 1, 15,
                int(p_defaults.get("extract_fps") or 5),
                help="Как часто брать кадры из видео под YOLO. Чаще — мельче "
                     "сетка, но дольше обработка.",
            )
        with uc2:
            up_max_frames = st.number_input(
                "Лимит кадров (0 — без лимита)", min_value=0, max_value=100000,
                value=int(p_defaults.get("max_frames") or 0), step=50,
                help="Потолок ПРОЧИТАННЫХ кадров. Для длинных записей режет "
                     "время обработки: хватит первых N секунд.",
            )
        st.caption(search_params_caption(up_extract_fps, up_max_frames))

    if uploaded_file is not None:
        if st.button("🚀 Загрузить и обработать"):
            files = {"file": (uploaded_file.name, uploaded_file, uploaded_file.type)}
            try:
                r = requests.post(
                    f"{API_URL}/api/videos/upload", files=files,
                    data={"extract_fps": up_extract_fps,
                          "max_frames": up_max_frames},
                    timeout=10
                )
                if r.status_code == 201:
                    data = r.json()
                    st.toast(f"🚀 Видео загружено! ID: {data['id']}")
                    st.info("⏳ Обработка запущена. Обновите страницу через минуту.")
                    st.cache_data.clear()
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error(f"❌ Ошибка: {r.text}")
            except Exception as e:
                st.error(f"❌ Не удалось подключиться к API: {e}")


# ============================================================
# СТРАНИЦА: СТРИМЫ
# ============================================================
elif page == "📡 Стримы":
    # ---- Управление обновлением (ДО фрагментов: значения нужны при декорировании) ----
    poll_interval = st.slider(
        "Интервал авто-обновления (сек)",
        min_value=1,
        max_value=10,
        value=3,
        step=1,
        help="Частота обновления живого превью и аналитики стримов. "
             "Обновляется только блок ниже, а не вся страница.",
    )

    auto_refresh = st.checkbox(
        "🔄 Авто-обновление (живое превью)",
        value=True,
        help="Фрагменты превью и аналитики сами дергают API каждые N секунд "
             "(st.fragment/run_every, Streamlit >= 1.37).",
    )

    # ---- Метрики стримов: живой фрагмент ----
    @st.fragment(run_every=poll_interval if auto_refresh else None)
    def stream_metrics_fragment():
        stats = get_stats(source="stream")
        streams = get_streams()
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Стримов всего", len(streams) if streams else 0)
        with col2:
            st.metric(
                "Детекций со стримов",
                stats["total_detections"] if stats else 0,
            )
        with col3:
            st.metric(
                "Средняя уверенность",
                f"{stats['avg_confidence']:.1%}" if stats else "—",
            )

    stream_metrics_fragment()

    # ---- Добавить стрим ----
    st.markdown(section_header("➕ Добавить стрим"), unsafe_allow_html=True)

    with st.form("add_stream"):
        stream_name = st.text_input("Название", placeholder="Парковка камера 1")
        rtsp_url = st.text_input(
            "URL источника (RTSP / HLS)",
            placeholder="rtsp://... или https://example.com/cam/playlist.m3u8",
        )
        stream_fps = st.slider("Частота захвата (FPS)", 1, 10, 5)
        submitted = st.form_submit_button("Добавить стрим")

        if submitted and stream_name and rtsp_url:
            try:
                r = requests.post(
                    f"{API_URL}/api/streams",
                    json={"name": stream_name, "rtsp_url": rtsp_url, "fps": stream_fps},
                    timeout=5,
                )
                if r.status_code == 201:
                    st.toast(f"✅ Стрим '{stream_name}' добавлен!")
                    st.cache_data.clear()
                    time.sleep(0.6)
                    st.rerun()
                else:
                    st.error(f"❌ Ошибка: {r.text}")
            except Exception as e:
                st.error(f"❌ {e}")

    st.caption(
        "⚠️ Адрес источника должен открываться **изнутри контейнера `stream-manager`**: "
        "для встроенного MediaMTX — `rtsp://mediamtx:8554/<path>`; для источника на "
        "Windows-хосте — `rtsp://host.docker.internal:<port>/<path>`. `localhost` внутри "
        "контейнера — это сам контейнер. Если стрим не стартует — смотри "
        "`docker compose logs stream-manager --tail 50`."
    )

    streams = get_streams()

    # ---- Список стримов ----
    st.markdown(section_header("📡 Стримы"), unsafe_allow_html=True)

    if streams:
        for s in streams:
            col_info, col_start, col_del = st.columns([5, 1, 1])

            with col_info:
                icon = "🟢" if s["status"] == "active" else "🔴"
                st.markdown(
                    f"{icon} **#{s['id']} — {s['name']}** "
                    f"({s['detections_count']} дет.) &nbsp; {status_badge(s['status'])}\n\n"
                    f"`{s['rtsp_url']}` &nbsp;·&nbsp; FPS: {s['fps']}",
                    unsafe_allow_html=True,
                )

            with col_start:
                if s["status"] == "inactive":
                    if st.button("▶️", key=f"start_{s['id']}", help="Запустить"):
                        try:
                            r = requests.post(
                                f"{API_URL}/api/streams/{s['id']}/start", timeout=5
                            )
                            if r.status_code == 200:
                                st.toast("✅ Стрим запущен")
                                st.cache_data.clear()
                                time.sleep(0.6)
                                st.rerun()
                            else:
                                st.error(f"❌ {r.json().get('detail', 'Ошибка')}")
                        except Exception as e:
                            st.error(f"❌ {e}")
                else:
                    if st.button("⏹", key=f"stop_{s['id']}", help="Остановить"):
                        try:
                            r = requests.post(
                                f"{API_URL}/api/streams/{s['id']}/stop", timeout=5
                            )
                            if r.status_code == 200:
                                st.toast("⏹ Стрим остановлен")
                                st.cache_data.clear()
                                time.sleep(0.6)
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
                            st.toast("🗑 Удалён")
                            st.cache_data.clear()
                            time.sleep(0.6)
                            st.rerun()
                    except Exception as e:
                        st.error(f"❌ {e}")

            st.markdown("")

        # ============================================================
        # ПРЕВЮ СТРИМА — фрагмент: обновляется сам, без рерана страницы
        # ============================================================
        st.markdown(section_header("🖼 Превью стримов"), unsafe_allow_html=True)

        @st.fragment(run_every=poll_interval if auto_refresh else None)
        def stream_preview_fragment():
            stream_options = {"Все стримы": None}
            for s in streams:
                stream_options[f"#{s['id']} — {s['name']}"] = s["id"]

            prev_sel = st.selectbox(
                "Стрим для превью:",
                list(stream_options.keys()),
                key="stream_preview_select",
            )
            prev_stream_id = stream_options[prev_sel]

            if prev_stream_id is None:
                empty_state("🖼", "Выберите стрим",
                            "Выберите конкретный стрим в списке выше — здесь появится живое превью.")
                return

            latest = get_latest_stream_frame(prev_stream_id)

            if latest is None:
                empty_state("📷", "Пока нет сохранённых кадров",
                            "Кадры пишутся в MinIO только когда YOLO находит детекции. "
                            "Запустите стрим и подождите.")
                return

            frame_url = latest["frame_url"]
            frame_index = latest["frame_index"]
            det_count_latest = latest["detections_count"]

            img_bytes = fetch_image_bytes(frame_url)
            dets = get_stream_detections(prev_stream_id, frame_index=frame_index)

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
                    f"📷 Frame #{frame_index} | 🎯 Детекций на кадре: "
                    f"**{det_count_latest}** | polled at {time.strftime('%H:%M:%S')}"
                )

            with col_boxed:
                st.markdown("**Стрим с детекциями**")
                if img_bytes:
                    if not dets:
                        html_no = render_frame_with_boxes_html(img_bytes, [])
                        if html_no:
                            st.markdown(html_no, unsafe_allow_html=True)
                        st.caption("ℹ️ На этом кадре детекций нет.")
                    else:
                        html_boxed = render_frame_with_boxes_html(img_bytes, dets)
                        if html_boxed:
                            st.markdown(html_boxed, unsafe_allow_html=True)
                        else:
                            st.error("Не удалось отрисовать кадр.")
                else:
                    st.warning("Сначала нужно получить чистый кадр.")

            prev_idx_key = f"prev_frame_idx_{prev_stream_id}"
            if st.session_state.get(prev_idx_key) == frame_index:
                st.caption(
                    "ℹ️ Кадр тот же, что и в прошлый poll — новых детекций не появилось. "
                    "Превью обновится, когда YOLO сохранит следующий кадр."
                )
            st.session_state[prev_idx_key] = frame_index

        stream_preview_fragment()

        # ============================================================
        # АНАЛИТИКА СТРИМОВ — фрагмент
        # ============================================================
        st.markdown(section_header("📊 Аналитика стримов"), unsafe_allow_html=True)

        @st.fragment(run_every=poll_interval if auto_refresh else None)
        def stream_analytics_fragment():
            stream_options_an = {"Все стримы": None}
            for s in streams:
                stream_options_an[f"#{s['id']} — {s['name']}"] = s["id"]

            sel = st.selectbox(
                "Стрим:", list(stream_options_an.keys()), key="stream_analytics"
            )
            sel_stream_id = stream_options_an[sel]

            col1, col2 = st.columns(2)

            with col1:
                dist = get_class_distribution(
                    stream_id=sel_stream_id, source="stream"
                )
                if dist:
                    st.plotly_chart(donut_chart(dist), use_container_width=True)
                else:
                    empty_state("📊", "Нет данных", "Детекции появятся при работе стрима.")

            with col2:
                activity = get_stream_activity(stream_id=sel_stream_id, minutes=10)
                if activity:
                    fig = px.bar(
                        activity, x="time", y="count",
                        labels={"time": "Время", "count": "Детекции"},
                        color_discrete_sequence=[ACCENT],
                    )
                    st.plotly_chart(dark_fig(fig, 320), use_container_width=True)
                else:
                    empty_state("⏱", "Нет активности", "За последние 10 минут детекций не было.")

            st.markdown(section_header("📋 Галерея детекций"), unsafe_allow_html=True)

            # Для «Все стримы» галерея берёт первый стрим с детекциями —
            # кропы всё равно тянутся с конкретного MinIO-префикса.
            gallery_stream_id = sel_stream_id
            if gallery_stream_id is None:
                for s in streams:
                    if s["detections_count"] > 0:
                        gallery_stream_id = s["id"]
                        break

            n_cards = st.slider(
                "Кадров в галерее", 4, 24, 12,
                help="Карточка = объект с уникального кадра (дедуп по кадрам). "
                     "Кадры качаются из MinIO и кешируются: больше карточек — "
                     "дольше первый рендер, дальше почти без трафика.",
            )

            detections = (
                get_stream_detections(gallery_stream_id, limit=200)
                if gallery_stream_id else []
            )
            gallery = (
                detections_gallery_html(gallery_stream_id, detections, cards=n_cards)
                if detections else None
            )

            if gallery:
                st.markdown(gallery, unsafe_allow_html=True)
            elif detections:
                st.caption(
                    "ℹ️ Кадры с этими детекциями уже удалены из MinIO "
                    "(TTL 7 дней) или ещё не записаны."
                )
            else:
                empty_state("📋", "Нет детекций", "Как только YOLO найдёт объекты — они появятся здесь.")

        stream_analytics_fragment()

    else:
        empty_state("📡", "Нет добавленных стримов",
                    "Добавьте первый стрим через форму выше — формат rtsp:// или m3u8.")

    st.markdown("---")

    with st.expander("ℹ️ Стрим не работает — где смотреть причину"):
        st.code(
            "# Producer стрима — это subprocess внутри контейнера stream-manager,\n"
            "# его логи (и падения) прилетают сюда:\n"
            "docker compose logs stream-manager --tail 50\n\n"
            "# Инференс (YOLO) и запись в БД:\n"
            "docker compose logs yolo-consumer --tail 30\n"
            "docker compose logs detection-consumer --tail 30\n\n"
            "# Топики Kafka (создаются сервисом kafka-init):\n"
            "docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list\n\n"
            "# Ручной запуск producer (адрес — с точки зрения КОНТЕЙНЕРА!):\n"
            "docker compose exec stream-manager python -m app.streaming.producer "
            "rtsp://mediamtx:8554/mystream 5 1\n\n"
            "# Тест без камеры: положи .mp4 в ./uploads — в контейнере это /app/uploads/…\n"
            "docker compose exec stream-manager python -m app.streaming.producer "
            "/app/uploads/demo.mp4 5 1",
            language="bash",
        )
