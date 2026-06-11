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
    except:
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
    except:
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
    except:
        return []


@st.cache_data(ttl=10)
def get_stream_activity(stream_id=None, minutes=10):
    try:
        params = {"minutes": minutes}
        if stream_id:
            params["stream_id"] = stream_id
        r = requests.get(f"{API_URL}/api/analytics/stream-activity", params=params, timeout=5)
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


@st.cache_data(ttl=10)
def get_streams():
    try:
        r = requests.get(f"{API_URL}/api/streams", timeout=5)
        return r.json() if r.status_code == 200 else []
    except:
        return []


@st.cache_data(ttl=10)
def get_stream_detections(stream_id, limit=100):
    try:
        r = requests.get(f"{API_URL}/api/streams/{stream_id}/detections", params={"limit": limit}, timeout=5)
        return r.json() if r.status_code == 200 else []
    except:
        return []


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

    if videos:
        video_options = {"Все видео": None}
        for v in videos:
            icon = "✅" if v["status"] == "completed" else "⏳" if v["status"] == "processing" else "❌"
            label = f"{icon} #{v['id']} — {v['filename']} ({v['detections_count']} дет.)"
            video_options[label] = v["id"]

        selected = st.selectbox("Выберите видео:", list(video_options.keys()))
        selected_video_id = video_options[selected]

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

    # Автообновление каждые 5 сек
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

        # Графики — ТОЛЬКО стрим-детекции (source="stream")
        st.subheader("📊 Аналитика стримов")

        stream_options = {"Все стримы": None}
        for s in streams:
            stream_options[f"#{s['id']} — {s['name']}"] = s["id"]

        sel = st.selectbox("Стрим:", list(stream_options.keys()), key="stream_analytics")
        sel_stream_id = stream_options[sel]

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

    else:
        st.info("Нет добавленных стримов. Добавьте через форму выше 👆")

    st.markdown("---")

    with st.expander("ℹ️ Как запустить обработку стрима вручную"):
        st.code(
            "# 1. Запусти YOLO consumer\n"
            "python -m app.streaming.yolo_consumer yolov8n.pt 0\n\n"
            "# 2. Запусти DB consumer\n"
            "python -m app.streaming.consumer\n\n"
            "# 3. Запусти producer с RTSP URL\n"
            'python -m app.streaming.producer "rtsp://..." 5\n\n'
            "# Или с видео-файлом для теста\n"
            'python -m app.streaming.producer data/output.mp4 2 --loop',
            language="bash",
        )