import json
import os
import threading
import time
import sys

import streamlit as st
import streamlit.components.v1 as components
from confluent_kafka import Consumer
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import KAFKA_CONFIG, TOPICS
import dashboard.state as state

# ── Live data file (read by the Leaflet map via fetch) ────────────────────────

STATIC_DIR     = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static')
LIVE_DATA_PATH = os.path.join(STATIC_DIR, 'live_data.json')
_file_lock     = threading.Lock()


def _write_live_data():
    with state.lock:
        tracking = dict(state.tracking_data)
        match    = dict(state.active_match)
        eta      = dict(state.eta_data)
        status   = dict(state.ride_status)

    drivers = []
    for driver_id, t in tracking.items():
        m = match.get(driver_id, {})
        e = eta.get(driver_id, {})
        drivers.append({
            "driver_id":   driver_id,
            "driver_name": t.get('driver_name', driver_id),
            "rider_name":  t.get('rider_name', ''),
            "lat":         t['current_lat'],
            "lng":         t['current_lng'],
            "phase":       t.get('phase', ''),
            "phase_label": t.get('phase_label', ''),
            "distance_km": round(t.get('distance_km', 0), 2),
            "eta_minutes": e.get('eta_minutes', '-'),
            "status":      status.get(driver_id, 'on_trip'),
            "pickup":      m.get('pickup'),
            "destination": m.get('destination'),
        })

    payload = {"drivers": drivers, "updated_at": datetime.now().isoformat()}
    with _file_lock:
        with open(LIVE_DATA_PATH, 'w') as f:
            json.dump(payload, f)


# ── Consumer threads ──────────────────────────────────────────────────────────

def _make_consumer(group_id, topic_keys):
    c = Consumer({
        **KAFKA_CONFIG,
        'group.id': group_id,
        'auto.offset.reset': 'latest',
    })
    c.subscribe([TOPICS[k] for k in topic_keys])
    return c


def _consume_location():
    c = _make_consumer('dash-loc-grp', ['location_updates'])
    while True:
        msg = c.poll(1.0)
        if not msg or msg.error():
            continue
        try:
            data = json.loads(msg.value().decode())
            with state.lock:
                state.drivers_data[data['driver_id']] = data
                state.node_last_seen[msg.topic()] = time.time()
        except Exception:
            pass


def _consume_match():
    c = _make_consumer('dash-match-grp', ['ride_matched'])
    while True:
        msg = c.poll(1.0)
        if not msg or msg.error():
            continue
        try:
            data = json.loads(msg.value().decode())
            with state.lock:
                state.matches_data.insert(0, data)
                state.matches_data = state.matches_data[:10]
                state.active_match[data['driver_id']] = data
                state.node_last_seen[msg.topic()] = time.time()
            _write_live_data()
        except Exception:
            pass


def _consume_tracking():
    c = _make_consumer('dash-track-grp', ['ride_tracking'])
    while True:
        msg = c.poll(1.0)
        if not msg or msg.error():
            continue
        try:
            data = json.loads(msg.value().decode())
            driver_id = data['driver_id']
            with state.lock:
                state.tracking_data[driver_id] = data
                state.node_last_seen[msg.topic()] = time.time()
            _write_live_data()
        except Exception:
            pass


def _consume_status():
    c = _make_consumer('dash-status-grp', ['ride_status'])
    while True:
        msg = c.poll(1.0)
        if not msg or msg.error():
            continue
        try:
            data = json.loads(msg.value().decode())
            with state.lock:
                state.ride_status[data['driver_id']] = data['status']
                state.node_last_seen[msg.topic()] = time.time()
                if data['status'] == 'completed':
                    state.driver_trail.pop(data['driver_id'], None)
                    state.tracking_data.pop(data['driver_id'], None)
                    state.active_match.pop(data['driver_id'], None)
                    state.eta_data.pop(data['driver_id'], None)
            _write_live_data()
        except Exception:
            pass


def _consume_eta():
    c = _make_consumer('dash-eta-grp', ['eta_updates'])
    while True:
        msg = c.poll(1.0)
        if not msg or msg.error():
            continue
        try:
            data = json.loads(msg.value().decode())
            with state.lock:
                state.eta_data[data['driver_id']] = data
                state.node_last_seen[msg.topic()] = time.time()
        except Exception:
            pass


if not state.thread_started:
    for fn in [_consume_location, _consume_match, _consume_tracking,
               _consume_status, _consume_eta]:
        threading.Thread(target=fn, daemon=True).start()
    state.thread_started = True

# ── Leaflet map HTML (never reloads — markers update via JS fetch) ─────────────

MAP_HTML = """<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8"/>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body { font-family: sans-serif; }
    #map { width: 100%; height: 520px; }
    #infobox {
      position: absolute; top: 12px; right: 12px; z-index: 1000;
      background: rgba(255,255,255,0.95); padding: 10px 14px;
      border-radius: 8px; font-size: 13px; min-width: 190px;
      box-shadow: 0 2px 8px rgba(0,0,0,0.25); line-height: 1.7;
    }
    #infobox b { font-size: 14px; }
    #legend {
      position: absolute; bottom: 24px; left: 12px; z-index: 1000;
      background: rgba(255,255,255,0.92); padding: 8px 12px;
      border-radius: 6px; font-size: 12px;
      box-shadow: 0 1px 4px rgba(0,0,0,0.2); line-height: 1.8;
    }
    .dot { display:inline-block; width:11px; height:11px;
           border-radius:50%; margin-right:5px; vertical-align:middle; }
  </style>
</head>
<body>
  <div id="map"></div>
  <div id="infobox">Menunggu data...</div>
  <div id="legend">
    <span class="dot" style="background:#ff6d00"></span> Driver<br>
    <span class="dot" style="background:#2e7d32"></span> Pickup<br>
    <span class="dot" style="background:#c62828"></span> Tujuan
  </div>

  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <script>
    var map = L.map('map', {zoomControl: true}).setView([-6.2088, 106.8456], 13);
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: 'OpenStreetMap'
    }).addTo(map);

    function circleIcon(color, size) {
      size = size || 16;
      return L.divIcon({
        html: '<div style="background:' + color + ';width:' + size + 'px;height:' + size +
              'px;border-radius:50%;border:3px solid white;' +
              'box-shadow:0 1px 4px rgba(0,0,0,0.4)"></div>',
        iconSize:   [size + 6, size + 6],
        iconAnchor: [(size + 6) / 2, (size + 6) / 2],
        className: ''
      });
    }

    var driverMarker = null;
    var pickupMarker = null;
    var destMarker   = null;
    var currentRider = null;
    var firstLoad    = true;

    var DATA_URL = 'http://localhost:8501/app/static/live_data.json';

    function clearMarkers() {
      if (driverMarker) { map.removeLayer(driverMarker); driverMarker = null; }
      if (pickupMarker) { map.removeLayer(pickupMarker); pickupMarker = null; }
      if (destMarker)   { map.removeLayer(destMarker);   destMarker   = null; }
      currentRider = null;
      firstLoad    = true;
    }

    function update() {
      fetch(DATA_URL + '?t=' + Date.now())
        .then(function(r) { return r.json(); })
        .then(function(data) {
          var infobox = document.getElementById('infobox');

          if (!data.drivers || data.drivers.length === 0) {
            infobox.innerHTML = 'Tidak ada perjalanan aktif';
            clearMarkers();
            return;
          }

          var d = data.drivers[0];

          // Reset markers when a new ride starts
          if (currentRider !== d.rider_name) {
            clearMarkers();
            currentRider = d.rider_name;
          }

          // Driver marker
          if (!driverMarker) {
            driverMarker = L.marker([d.lat, d.lng], {icon: circleIcon('#ff6d00', 16)})
              .bindPopup('<b>' + d.driver_name + '</b><br>' + d.phase_label)
              .addTo(map);
          } else {
            driverMarker.setLatLng([d.lat, d.lng]);
          }

          // Pickup marker (created once)
          if (d.pickup && !pickupMarker) {
            pickupMarker = L.marker([d.pickup.lat, d.pickup.lng], {icon: circleIcon('#2e7d32', 13)})
              .bindPopup('Pickup: ' + d.rider_name)
              .addTo(map);
          }

          // Destination marker (created once)
          if (d.destination && !destMarker) {
            destMarker = L.marker([d.destination.lat, d.destination.lng], {icon: circleIcon('#c62828', 13)})
              .bindPopup('Tujuan: ' + d.rider_name)
              .addTo(map);
          }

          // Follow driver
          if (firstLoad) {
            map.setView([d.lat, d.lng], 15);
            firstLoad = false;
          } else {
            map.panTo([d.lat, d.lng], {animate: true, duration: 0.8});
          }

          // Update info box
          infobox.innerHTML =
            '<b>' + d.driver_name + '</b><br>' +
            '<span style="color:#555">' + d.rider_name + '</span><br>' +
            d.phase_label + '<br>' +
            'Sisa: <b>' + d.distance_km + ' km</b><br>' +
            'ETA: <b>' + d.eta_minutes + ' menit</b>';
        })
        .catch(function() {});
    }

    setInterval(update, 1000);
    update();
  </script>
</body>
</html>"""

# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(page_title="Ride-Hailing Live Dashboard", layout="wide")
st.title("Ride-Hailing Live Dashboard")

# ── Fragment: stats panel (refreshes text only) ───────────────────────────────

@st.fragment(run_every=1)
def render_stats():
    now = time.time()

    with state.lock:
        status_snap  = dict(state.ride_status)
        eta_snap     = dict(state.eta_data)
        matches_list = list(state.matches_data[:5])
        node_seen    = dict(state.node_last_seen)
        drivers_snap = dict(state.drivers_data)

    # Active ride
    st.subheader("Perjalanan Aktif")
    if eta_snap:
        for driver_id, eta in eta_snap.items():
            status = status_snap.get(driver_id, 'on_trip')
            st.markdown(f"""
**{eta.get('driver_name', driver_id)}** -> **{eta.get('rider_name', '?')}**

| | |
|---|---|
| Status | `{status}` |
| Phase | `{eta.get('phase_label', '-')}` |
| Sisa | `{eta.get('distance_km', 0):.2f} km` |
| ETA | `{eta.get('eta_minutes', '-')} menit` |
| Kendaraan | `{eta.get('vehicle', '-')}` |
""")
    else:
        st.info("Tidak ada perjalanan aktif.")

    st.divider()

    # Driver stats
    st.subheader("Driver")
    total     = len(drivers_snap)
    available = sum(1 for d in drivers_snap.values() if d['status'] == 'available')
    on_trip   = sum(1 for d in drivers_snap.values() if d['status'] == 'on_trip')
    c1, c2, c3 = st.columns(3)
    c1.metric("Total", total)
    c2.metric("Idle", available)
    c3.metric("On Trip", on_trip)

    st.divider()

    # Node status
    st.subheader("Node Status")
    nodes = [
        (TOPICS['location_updates'], "location-updates"),
        (TOPICS['ride_requests'],    "ride-requests"),
        (TOPICS['ride_matched'],     "ride-matched"),
        (TOPICS['ride_status'],      "ride-status"),
        (TOPICS['ride_tracking'],    "ride-tracking"),
        (TOPICS['eta_updates'],      "eta-updates"),
    ]
    for topic, label in nodes:
        age = now - node_seen.get(topic, 0)
        if age < 5:
            dot, note = "[ON] ", "aktif"
        elif age < 30:
            dot, note = "[~]  ", f"{int(age)}s lalu"
        else:
            dot, note = "[OFF]", "idle"
        st.markdown(f"`{dot}` `{label}` &nbsp; {note}")

    st.divider()

    # Recent matches
    st.subheader("Match Terbaru")
    if not matches_list:
        st.info("Belum ada match...")
    else:
        for match in matches_list:
            st.success(
                f"**{match['rider_name']}** -> **{match['driver_name']}** | "
                f"{match['distance_km']} km | ETA {match['eta_minutes']} menit"
            )

    st.caption(f"Updated: {datetime.now().strftime('%H:%M:%S')}")


# ── Layout ────────────────────────────────────────────────────────────────────

col_map, col_stat = st.columns([3, 1])

with col_map:
    st.subheader("Peta Real-Time")
    # Render sekali — tidak pernah di-reload, marker update via JS
    components.html(MAP_HTML, height=530)

with col_stat:
    render_stats()
