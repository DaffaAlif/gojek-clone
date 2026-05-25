import json
import threading
import streamlit as st
import folium
from streamlit_folium import st_folium
from confluent_kafka import Consumer
from datetime import datetime
import time
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import KAFKA_CONFIG, TOPICS
import dashboard.state as state

# ─── Background threads ───────────────────────────────
def consume_locations():
    consumer = Consumer({
        **KAFKA_CONFIG,
        'group.id': 'dashboard-location-group',
        'auto.offset.reset': 'latest'
    })
    consumer.subscribe([TOPICS['location_updates']])
    while True:
        msg = consumer.poll(1.0)
        if not msg or msg.error():
            continue
        try:
            data = json.loads(msg.value().decode('utf-8'))
            state.drivers_data[data['driver_id']] = data
        except:
            continue

def consume_matches():
    consumer = Consumer({
        **KAFKA_CONFIG,
        'group.id': 'dashboard-match-group',
        'auto.offset.reset': 'latest'
    })
    consumer.subscribe([TOPICS['ride_matched']])
    while True:
        msg = consumer.poll(1.0)
        if not msg or msg.error():
            continue
        try:
            data = json.loads(msg.value().decode('utf-8'))
            state.matches_data.insert(0, data)
            state.matches_data = state.matches_data[:10]
        except:
            continue

# Jalankan thread sekali saja
if not state.thread_started:
    t1 = threading.Thread(target=consume_locations, daemon=True)
    t2 = threading.Thread(target=consume_matches,   daemon=True)
    t1.start()
    t2.start()
    state.thread_started = True

# ─── UI ───────────────────────────────────────────────
st.set_page_config(page_title="Ride-Hailing Dashboard", layout="wide")
st.title("🚗 Ride-Hailing Live Dashboard")

col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("🗺️ Peta Lokasi Driver")
    m = folium.Map(location=[-6.2088, 106.8456], zoom_start=12)

    for driver_id, d in state.drivers_data.items():
        color = "green"  if d['status'] == 'available' else \
                "orange" if d['status'] == 'on_trip'   else "red"
        folium.Marker(
            location=[d['lat'], d['lng']],
            popup=f"{d['name']} ({d['status']})",
            tooltip=d['name'],
            icon=folium.Icon(color=color, icon='car', prefix='fa')
        ).add_to(m)

    st_folium(m, width=700, height=450)
    st.markdown("🟢 **Available** &nbsp;&nbsp; 🟠 **On Trip** &nbsp;&nbsp; 🔴 **Offline**")

with col2:
    st.subheader("📊 Statistik")
    total     = len(state.drivers_data)
    available = sum(1 for d in state.drivers_data.values() if d['status'] == 'available')
    on_trip   = sum(1 for d in state.drivers_data.values() if d['status'] == 'on_trip')
    offline   = sum(1 for d in state.drivers_data.values() if d['status'] == 'offline')

    st.metric("Total Driver", total)
    st.metric("✅ Available", available)
    st.metric("🟠 On Trip",   on_trip)
    st.metric("🔴 Offline",   offline)

    st.divider()

    st.subheader("🔔 Match Terbaru")
    if not state.matches_data:
        st.info("Belum ada match...")
    else:
        for match in state.matches_data[:5]:
            st.success(
                f"**{match['rider_name']}** → **{match['driver_name']}** | "
                f"{match['distance_km']} km | ETA {match['eta_minutes']} menit"
            )

# ─── Auto refresh ─────────────────────────────────────
st.markdown(f"*Last update: {datetime.now().strftime('%H:%M:%S')}*")
time.sleep(3)
st.rerun()