"""
routing.py — OSRM road routing helpers.

Fungsi-fungsi ini dipakai oleh tracking_service.py dan simulate_one_ride.py
agar driver bergerak mengikuti jalan nyata, bukan garis lurus.
"""

import json
import math
import urllib.request

OSRM_BASE = "https://router.project-osrm.org/route/v1/driving/"


def fetch_osrm_route(from_lat, from_lng, to_lat, to_lng):
    """
    Ambil waypoint rute jalan dari OSRM public API.
    Return: list of (lat, lng) — kosong jika gagal.
    """
    url = (
        f"{OSRM_BASE}{from_lng},{from_lat};{to_lng},{to_lat}"
        "?overview=full&geometries=geojson"
    )
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "ride-hailing-sim/1.0"}
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
        if data.get("routes"):
            coords = data["routes"][0]["geometry"]["coordinates"]
            return [(c[1], c[0]) for c in coords]   # [lng,lat] → (lat,lng)
    except Exception as e:
        print(f"[OSRM] fetch gagal ({from_lat:.4f},{from_lng:.4f} → "
              f"{to_lat:.4f},{to_lng:.4f}): {e}")
    return []


def step_along_route(waypoints, route_idx, current_lat, current_lng, step):
    """
    Gerakkan posisi sejauh `step` coordinate-units mengikuti waypoints.

    Args:
        waypoints  : list of (lat, lng) dari fetch_osrm_route
        route_idx  : indeks waypoint yang sudah dicapai
        current_lat/lng: posisi driver saat ini
        step       : jarak per langkah dalam satuan koordinat

    Returns: (new_lat, new_lng, new_route_idx, arrived)
    """
    if not waypoints or route_idx >= len(waypoints) - 1:
        return current_lat, current_lng, route_idx, True

    lat, lng = current_lat, current_lng
    remaining = step
    idx = route_idx

    while remaining > 0 and idx < len(waypoints) - 1:
        t_lat, t_lng = waypoints[idx + 1]
        d_lat = t_lat - lat
        d_lng = t_lng - lng
        dist = math.sqrt(d_lat ** 2 + d_lng ** 2)

        if dist <= remaining:
            lat, lng = t_lat, t_lng
            remaining -= dist
            idx += 1
        else:
            ratio = remaining / dist
            lat += d_lat * ratio
            lng += d_lng * ratio
            remaining = 0

    arrived = idx >= len(waypoints) - 1
    return lat, lng, idx, arrived


def count_route_steps(waypoints, step):
    """Hitung berapa kali step_along_route diperlukan untuk menempuh seluruh route."""
    if not waypoints:
        return 0
    total_dist = sum(
        math.sqrt(
            (waypoints[i + 1][0] - waypoints[i][0]) ** 2 +
            (waypoints[i + 1][1] - waypoints[i][1]) ** 2
        )
        for i in range(len(waypoints) - 1)
    )
    return max(1, math.ceil(total_dist / step))
