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
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.0/css/all.min.css"/>
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
      box-shadow: 0 1px 4px rgba(0,0,0,0.2); line-height: 2;
    }
    .fa-map-pin, .fa-car, .fa-motorcycle { margin-right: 6px; }
  </style>
</head>
<body>
  <div id="map"></div>
  <div id="infobox">Menunggu data...</div>
  <div id="legend">
    <i class="fa-solid fa-car" style="color:#ff6d00"></i> Driver<br>
    <i class="fa-solid fa-map-pin" style="color:#2e7d32"></i> Pickup<br>
    <i class="fa-solid fa-map-pin" style="color:#c62828"></i> Tujuan<br>
    <span style="display:inline-block;width:18px;height:4px;background:#1565C0;border-radius:2px;vertical-align:middle;margin-right:6px"></span>Menuju pickup<br>
    <span style="display:inline-block;width:18px;height:4px;background:#42A5F5;border-radius:2px;vertical-align:middle;margin-right:6px"></span>Menuju tujuan
  </div>

  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <script>
    var map = L.map('map', {zoomControl: true}).setView([-6.2088, 106.8456], 13);
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: 'OpenStreetMap'
    }).addTo(map);

    function pinIcon(color, faClass) {
      return L.divIcon({
        html: '<div style="position:relative;width:32px;height:42px;text-align:center;">' +
              '<i class="fa-solid ' + faClass + '" style="font-size:36px;color:' + color +
              ';filter:drop-shadow(0 2px 3px rgba(0,0,0,0.4));line-height:1"></i>' +
              '</div>',
        iconSize:   [32, 42],
        iconAnchor: [16, 42],
        popupAnchor:[0, -44],
        className: ''
      });
    }

    function driverIcon(vehicle) {
      var faClass = (vehicle === 'motorcycle') ? 'fa-motorcycle' : 'fa-car';
      return L.divIcon({
        html: '<div style="background:#ff6d00;border-radius:50%;width:36px;height:36px;' +
              'display:flex;align-items:center;justify-content:center;' +
              'border:3px solid white;box-shadow:0 2px 6px rgba(0,0,0,0.5);">' +
              '<i class="fa-solid ' + faClass + '" style="color:white;font-size:16px;"></i>' +
              '</div>',
        iconSize:   [36, 36],
        iconAnchor: [18, 18],
        popupAnchor:[0, -20],
        className: ''
      });
    }

    var driverMarker  = null;
    var pickupMarker  = null;
    var destMarker    = null;
    var routeToPickup = null;
    var routeToDest   = null;
    var currentRider  = null;
    var currentVehicle = null;
    var firstLoad     = true;

    // ── Smooth marker animation ───────────────────────────────────────────────
    var _animFrame   = null;
    var _animStart   = null;
    var _animFromLat = 0, _animFromLng = 0;
    var _animToLat   = 0, _animToLng   = 0;
    var ANIM_MS      = 900;  // durasi animasi (sedikit < interval update 1000ms)

    function _ease(t) {
      // ease-in-out cubic
      return t < 0.5 ? 4 * t * t * t : 1 - Math.pow(-2 * t + 2, 3) / 2;
    }

    function animateDriverTo(toLat, toLng) {
      if (!driverMarker) return;
      var from     = driverMarker.getLatLng();
      _animFromLat = from.lat;
      _animFromLng = from.lng;
      _animToLat   = toLat;
      _animToLng   = toLng;
      if (_animFrame) cancelAnimationFrame(_animFrame);
      _animStart = null;

      function tick(ts) {
        if (!_animStart) _animStart = ts;
        var t = Math.min((ts - _animStart) / ANIM_MS, 1);
        var e = _ease(t);
        driverMarker.setLatLng([
          _animFromLat + (_animToLat - _animFromLat) * e,
          _animFromLng + (_animToLng - _animFromLng) * e
        ]);
        if (t < 1) { _animFrame = requestAnimationFrame(tick); }
        else        { _animFrame = null; }
      }
      _animFrame = requestAnimationFrame(tick);
    }
    // ─────────────────────────────────────────────────────────────────────────

    var DATA_URL  = 'http://localhost:8502/live_data.json';
    var OSRM_BASE = 'https://router.project-osrm.org/route/v1/driving/';

    // Ambil polyline koordinat dari OSRM, panggil cb([latLng, ...])
    function fetchRoute(from, to, cb) {
      var url = OSRM_BASE +
        from.lng + ',' + from.lat + ';' +
        to.lng   + ',' + to.lat   +
        '?overview=full&geometries=geojson';
      fetch(url)
        .then(function(r) { return r.json(); })
        .then(function(data) {
          if (data.routes && data.routes.length > 0) {
            var coords = data.routes[0].geometry.coordinates.map(function(c) {
              return [c[1], c[0]];  // [lng,lat] → [lat,lng]
            });
            cb(coords);
          }
        })
        .catch(function() {});
    }

    // Gambar dua segmen rute saat ride pertama kali terdeteksi
    function drawRoutes(driverPos, pickup, destination) {
      // Segmen 1: driver → pickup (biru tua)
      fetchRoute(driverPos, pickup, function(coords) {
        if (routeToPickup) { map.removeLayer(routeToPickup); }
        routeToPickup = L.polyline(coords, {
          color: '#1565C0', weight: 5, opacity: 0.85
        }).addTo(map);
      });

      // Segmen 2: pickup → destination (biru muda)
      fetchRoute(pickup, destination, function(coords) {
        if (routeToDest) { map.removeLayer(routeToDest); }
        routeToDest = L.polyline(coords, {
          color: '#42A5F5', weight: 5, opacity: 0.85
        }).addTo(map);
      });
    }

    function clearMarkers() {
      if (_animFrame)    { cancelAnimationFrame(_animFrame); _animFrame = null; }
      if (driverMarker)  { map.removeLayer(driverMarker);  driverMarker  = null; }
      if (pickupMarker)  { map.removeLayer(pickupMarker);  pickupMarker  = null; }
      if (destMarker)    { map.removeLayer(destMarker);    destMarker    = null; }
      if (routeToPickup) { map.removeLayer(routeToPickup); routeToPickup = null; }
      if (routeToDest)   { map.removeLayer(routeToDest);   routeToDest   = null; }
      currentRider   = null;
      currentVehicle = null;
      firstLoad      = true;
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

          // Reset saat ride baru mulai
          if (currentRider !== d.rider_name) {
            clearMarkers();
            currentRider   = d.rider_name;
            currentVehicle = d.vehicle;
          }

          // Driver marker — car atau motorcycle
          if (!driverMarker) {
            driverMarker = L.marker([d.lat, d.lng], {icon: driverIcon(d.vehicle)})
              .bindPopup('<b>' + d.driver_name + '</b><br>' + d.phase_label)
              .addTo(map);
          } else {
            animateDriverTo(d.lat, d.lng);
          }

          // Pickup marker + gambar rute (dibuat sekali)
          if (d.pickup && !pickupMarker) {
            pickupMarker = L.marker([d.pickup.lat, d.pickup.lng],
              {icon: pinIcon('#2e7d32', 'fa-map-pin')})
              .bindPopup('<b>Pickup</b><br>' + d.rider_name)
              .addTo(map);

            if (d.destination) {
              drawRoutes(
                {lat: d.lat, lng: d.lng},
                d.pickup,
                d.destination
              );
            }
          }

          // Destination marker (dibuat sekali)
          if (d.destination && !destMarker) {
            destMarker = L.marker([d.destination.lat, d.destination.lng],
              {icon: pinIcon('#c62828', 'fa-map-pin')})
              .bindPopup('<b>Tujuan</b><br>' + d.rider_name)
              .addTo(map);
          }

          // Ikuti driver
          if (firstLoad) {
            map.setView([d.lat, d.lng], 14);
            firstLoad = false;
          } else {
            map.panTo([d.lat, d.lng], {animate: true, duration: 0.8});
          }

          // Info box
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
