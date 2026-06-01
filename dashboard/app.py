"""
Streamlit-дашборд для Vision Analytics Platform.
"""
import streamlit as st
import plotly.express as px
import requests
import time
import os

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
# КЕШИРОВАНИЕ — запросы к API кешируются на 10 секунд
# st.cache_data(ttl=10) — результат сохраняется на 10 сек,
# потом при следующем вызове — свежий запрос
# Это ускоряет дашборд: не бьём API на каждый клик
# ============================================================
@st.cache_data(ttl=10)
def get_stats():
    try:
        r = requests.get(f"{API_URL}/api/analytics/stats", timeout=5)
        return r.json() if r.status_code == 200 else None
    except:
        return None


@st.cache_data(ttl=10)
def get_class_distribution(video_id=None):
    try:
        params = {}
        if video_id:
            params["video_id"] = video_id
        r = requests.get(f"{API_URL}/api/analytics/class-distribution", params=params, timeout=5)
        return r.json() if r.status_code == 200 else []
    except:
        return []


@st.cache_data(ttl=10)
def get_timeline(video_id=None, bucket=5.0):
    try:
        params = {"bucket": bucket}
        if video_id:
            params["video_id"] = video_id
        r = requests.get(f"{API_URL}/api/analytics/timeline", params=params, timeout=5)
        return r.json() if r.status_code == 200 else []
    except:
        return []


@st.cache_data(ttl=10)
def get_videos():
    try:
        r = requests.get(f"{API_URL}/api/videos", timeout=5)
        return r.json() if r.status_code == 200 else []
    except:
        return []


# ============================================================
# ЗАГОЛОВОК
# ============================================================
st.title("🔍 Vision Analytics Platform")
st.markdown("---")

# ============================================================
# СЕКЦИЯ 1: ОБЩАЯ СТАТИСТИКА
# ============================================================
stats = get_stats()

if stats:
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("📹 Всего видео", stats["total_videos"])
    with col2:
        st.metric("🎯 Всего детекций", stats["total_detections"])
    with col3:
        st.metric("📊 Средняя уверенность", f"{stats['avg_confidence']:.1%}")
else:
    st.warning("⚠️ Не удалось подключиться к API. Запустите сервер: uvicorn app.main:app --reload")

st.markdown("---")

# ============================================================
# СЕКЦИЯ 2: ФИЛЬТР ПО ВИДЕО
# ============================================================
videos = get_videos()
selected_video_id = None

if videos:
    video_options = {"Все видео": None}
    for v in videos:
        status_icon = "✅" if v["status"] == "completed" else "⏳" if v["status"] == "processing" else "❌"
        label = f"{status_icon} #{v['id']} — {v['filename']} ({v['detections_count']} детекций)"
        video_options[label] = v["id"]

    selected = st.selectbox("Выберите видео для аналитики:", list(video_options.keys()))
    selected_video_id = video_options[selected]

st.markdown("---")

# ============================================================
# СЕКЦИЯ 3: ГРАФИКИ (фильтруются по выбранному видео)
# ============================================================
st.subheader("📊 Аналитика")

dist = get_class_distribution(video_id=selected_video_id)
timeline = get_timeline(video_id=selected_video_id, bucket=5.0)

col_chart1, col_chart2 = st.columns(2)

with col_chart1:
    st.markdown("**Распределение по классам**")
    if dist:
        fig = px.pie(
            dist,
            values="count",
            names="class_name",
            hole=0.4,
        )
        fig.update_layout(margin=dict(l=0, r=0, t=0, b=0))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Нет данных для графика")

with col_chart2:
    st.markdown("**Детекции по времени**")
    if timeline:
        fig = px.bar(
            timeline,
            x="time_start",
            y="count",
            labels={"time_start": "Время (сек)", "count": "Детекции"},
        )
        fig.update_layout(margin=dict(l=0, r=0, t=0, b=0))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Нет данных для графика")

st.markdown("---")

# ============================================================
# СЕКЦИЯ 4: СПИСОК ВСЕХ ВИДЕО (не фильтруется)
# ============================================================
st.subheader("📹 Все видео")

if videos:
    for v in videos:
        status_emoji = "✅" if v["status"] == "completed" else "⏳" if v["status"] == "processing" else "❌"
        with st.expander(f"{status_emoji} #{v['id']} — {v['filename']}"):
            col1, col2, col3 = st.columns(3)
            with col1:
                st.write(f"**Статус:** {v['status']}")
                st.write(f"**FPS:** {v['fps'] or 'N/A'}")
            with col2:
                st.write(f"**Длительность:** {v['duration']:.1f} сек" if v['duration'] else "**Длительность:** N/A")
                st.write(f"**Детекций:** {v['detections_count']}")
            with col3:
                st.write(f"**Загружено:** {v['uploaded_at'][:19] if v['uploaded_at'] else 'N/A'}")
else:
    st.info("Пока нет загруженных видео. Загрузите через форму ниже 👇")

st.markdown("---")

# ============================================================
# СЕКЦИЯ 5: ЗАГРУЗКА ВИДЕО
# ============================================================
st.subheader("📤 Загрузить видео")

uploaded_file = st.file_uploader(
    "Выберите видео-файл",
    type=["mp4", "avi", "mov", "mkv"],
)

if uploaded_file is not None:
    if st.button("🚀 Загрузить и обработать"):
        files = {"file": (uploaded_file.name, uploaded_file, uploaded_file.type)}
        try:
            r = requests.post(f"{API_URL}/api/videos/upload", files=files, timeout=10)
            if r.status_code == 201:
                data = r.json()
                st.success(f"✅ Видео загружено! ID: {data['id']}, статус: {data['status']}")
                st.info("⏳ Обработка запущена в фоне. Обновите страницу через минуту.")
                st.cache_data.clear()  # ← сбрасываем кеш чтобы увидеть новые данные
                time.sleep(2)
                st.rerun()
            else:
                st.error(f"❌ Ошибка: {r.text}")
        except Exception as e:
            st.error(f"❌ Не удалось подключиться к API: {e}")