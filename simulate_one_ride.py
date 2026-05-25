"""
simulate_one_ride.py

End-to-end simulation: 1 driver + 1 rider menyelesaikan 1 perjalanan penuh.
Menjalankan semua logika (producer, matching, tracking, ETA) dalam satu proses.
Exit code 0 = sukses, 1 = gagal/timeout.

Usage:
    python simulate_one_ride.py [--timeout 120]
"""
import json
import math
import time
import sys
import os
import threading
import argparse
from datetime import datetime

# Force UTF-8 output on Windows
if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if sys.stderr.encoding and sys.stderr.encoding.lower() != 'utf-8':
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from confluent_kafka import Producer, Consumer, KafkaError
from config import KAFKA_CONFIG, TOPICS

# ─── Konfigurasi simulasi ─────────────────────────────────────────────────────

DRIVER = {
    "driver_id": "SIM-DRV-001",
    "name": "Budi (Simulasi)",
    "vehicle": "motorcycle",
}

RIDER = {
    "rider_id": "SIM-RDR-001",
    "name": "Rina (Simulasi)",
}

DRIVER_START = {"lat": -6.2090, "lng": 106.8460}
PICKUP_LOC   = {"lat": -6.2150, "lng": 106.8400}
DEST_LOC     = {"lat": -6.2300, "lng": 106.8600}

STEP_SIZE = 0.001  # coordinate units per tracking step

# ─── Helper ───────────────────────────────────────────────────────────────────

def haversine(lat1, lng1, lat2, lng2):
    R = 6371
    d_lat = math.radians(lat2 - lat1)
    d_lng = math.radians(lng2 - lng1)
    a = (math.sin(d_lat / 2) ** 2 +
         math.cos(math.radians(lat1)) *
         math.cos(math.radians(lat2)) *
         math.sin(d_lng / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def move_towards(clat, clng, tlat, tlng, step=0.001):
    d_lat = tlat - clat
    d_lng = tlng - clng
    dist = math.sqrt(d_lat ** 2 + d_lng ** 2)
    if dist < step:
        return tlat, tlng, True
    ratio = step / dist
    return clat + d_lat * ratio, clng + d_lng * ratio, False


def count_steps(slat, slng, tlat, tlng, step=STEP_SIZE):
    lat, lng = slat, slng
    n = 0
    while True:
        d_lat = tlat - lat
        d_lng = tlng - lng
        dist = math.sqrt(d_lat ** 2 + d_lng ** 2)
        if dist < step:
            return n + 1
        ratio = step / dist
        lat += d_lat * ratio
        lng += d_lng * ratio
        n += 1


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(icon, msg):
    print(f"{icon}  [{now_str()}] {msg}", flush=True)


def make_consumer(group_id, topics):
    c = Consumer({
        **KAFKA_CONFIG,
        'group.id': group_id,
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True,
    })
    c.subscribe(topics)
    return c


def poll_once(consumer, timeout=2.0):
    msg = consumer.poll(timeout)
    if msg is None or msg.error():
        return None
    return json.loads(msg.value().decode('utf-8'))


# ─── Stage: Publish driver location ──────────────────────────────────────────

def publish_driver_location(producer, pos, status="available"):
    payload = {
        "driver_id": DRIVER['driver_id'],
        "name":      DRIVER['name'],
        "vehicle":   DRIVER['vehicle'],
        "lat":       round(pos['lat'], 6),
        "lng":       round(pos['lng'], 6),
        "status":    status,
        "timestamp": now_str(),
        "speed_kmh": 35.0,
    }
    producer.produce(
        topic=TOPICS['location_updates'],
        key=DRIVER['driver_id'],
        value=json.dumps(payload),
    )
    producer.flush()
    return payload


# ─── Stage: Publish rider request ────────────────────────────────────────────

def publish_rider_request(producer):
    payload = {
        "rider_id":    RIDER['rider_id'],
        "name":        RIDER['name'],
        "pickup":      PICKUP_LOC,
        "destination": DEST_LOC,
        "service":     "GoRide",
        "timestamp":   now_str(),
    }
    producer.produce(
        topic=TOPICS['ride_requests'],
        key=RIDER['rider_id'],
        value=json.dumps(payload),
    )
    producer.flush()
    return payload


# ─── Stage: Matching ──────────────────────────────────────────────────────────

def do_matching(producer, driver_pos, rider_request):
    pickup = rider_request['pickup']
    distance = haversine(
        pickup['lat'], pickup['lng'],
        driver_pos['lat'], driver_pos['lng']
    )
    eta = max(round((distance / 30) * 60), 1)

    match_payload = {
        "rider_id":    RIDER['rider_id'],
        "rider_name":  RIDER['name'],
        "driver_id":   DRIVER['driver_id'],
        "driver_name": DRIVER['name'],
        "vehicle":     DRIVER['vehicle'],
        "pickup":      pickup,
        "destination": rider_request['destination'],
        "distance_km": round(distance, 2),
        "eta_minutes": eta,
        "service":     rider_request['service'],
        "timestamp":   now_str(),
    }

    producer.produce(
        topic=TOPICS['ride_matched'],
        key=RIDER['rider_id'],
        value=json.dumps(match_payload),
    )
    producer.flush()
    return match_payload


# ─── Stage: Tracking (step-by-step movement) ─────────────────────────────────

def do_tracking(producer, match_info, sleep_secs=0.2):
    """Simulate driver moving to pickup then to destination. Returns True on success."""
    pos = dict(DRIVER_START)
    phase = "to_pickup"
    speed  = {"motorcycle": 30, "car": 25}
    vehicle = DRIVER['vehicle']

    # Publish accepted status
    producer.produce(
        topic=TOPICS['ride_status'],
        key=DRIVER['driver_id'],
        value=json.dumps({
            "driver_id": DRIVER['driver_id'],
            "rider_id":  RIDER['rider_id'],
            "status":    "accepted",
            "timestamp": now_str(),
        })
    )
    producer.flush()
    log("✅", f"Status: accepted")

    max_steps = 500
    step_count = 0

    while step_count < max_steps:
        step_count += 1

        if phase == "to_pickup":
            target = match_info['pickup']
        else:
            target = match_info['destination']

        new_lat, new_lng, arrived = move_towards(
            pos['lat'], pos['lng'],
            target['lat'], target['lng'],
            step=STEP_SIZE,
        )
        pos = {"lat": new_lat, "lng": new_lng}

        distance_to_target = haversine(new_lat, new_lng, target['lat'], target['lng'])
        spd = speed.get(vehicle, 25)
        eta = max(round((distance_to_target / spd) * 60), 1)

        phase_label = "Menuju pickup" if phase == "to_pickup" else "Menuju destinasi"

        # Publish tracking update
        tracking_payload = {
            "driver_id":   DRIVER['driver_id'],
            "rider_id":    RIDER['rider_id'],
            "driver_name": DRIVER['name'],
            "rider_name":  RIDER['name'],
            "phase":       phase,
            "phase_label": phase_label,
            "current_lat": round(new_lat, 6),
            "current_lng": round(new_lng, 6),
            "target_lat":  target['lat'],
            "target_lng":  target['lng'],
            "distance_km": round(distance_to_target, 2),
            "timestamp":   now_str(),
        }
        producer.produce(
            topic=TOPICS['ride_tracking'],
            key=DRIVER['driver_id'],
            value=json.dumps(tracking_payload),
        )

        # Publish ETA update
        eta_payload = {
            "driver_id":   DRIVER['driver_id'],
            "rider_id":    RIDER['rider_id'],
            "driver_name": DRIVER['name'],
            "rider_name":  RIDER['name'],
            "phase":       phase,
            "phase_label": phase_label,
            "distance_km": round(distance_to_target, 2),
            "speed_kmh":   spd,
            "eta_minutes": eta,
            "vehicle":     vehicle,
            "timestamp":   now_str(),
        }
        producer.produce(
            topic=TOPICS['eta_updates'],
            key=DRIVER['driver_id'],
            value=json.dumps(eta_payload),
        )
        producer.flush()

        log("📍", f"{DRIVER['name']} | {phase_label} | Sisa: {distance_to_target:.3f} km | ETA: {eta} menit")

        time.sleep(sleep_secs)

        if arrived:
            if phase == "to_pickup":
                # Publish picked_up status
                producer.produce(
                    topic=TOPICS['ride_status'],
                    key=DRIVER['driver_id'],
                    value=json.dumps({
                        "driver_id": DRIVER['driver_id'],
                        "rider_id":  RIDER['rider_id'],
                        "status":    "picked_up",
                        "timestamp": now_str(),
                    })
                )
                producer.flush()
                log("🙋", f"{DRIVER['name']} menjemput {RIDER['name']}!")
                phase = "to_destination"

            elif phase == "to_destination":
                # Publish completed status
                producer.produce(
                    topic=TOPICS['ride_status'],
                    key=DRIVER['driver_id'],
                    value=json.dumps({
                        "driver_id": DRIVER['driver_id'],
                        "rider_id":  RIDER['rider_id'],
                        "status":    "completed",
                        "timestamp": now_str(),
                    })
                )
                producer.flush()
                log("🏁", f"Perjalanan selesai! {DRIVER['name']} mengantar {RIDER['name']}")
                return True

    log("⚠️", "Batas langkah tercapai sebelum perjalanan selesai")
    return False


# ─── Main simulation ──────────────────────────────────────────────────────────

def run_simulation(timeout, duration=60):
    result = {"success": False}

    # Pre-calculate total tracking steps to distribute time evenly
    fixed_overhead = 3  # 3x time.sleep(1) before tracking starts
    steps_to_pickup = count_steps(
        DRIVER_START['lat'], DRIVER_START['lng'],
        PICKUP_LOC['lat'],   PICKUP_LOC['lng'],
    )
    steps_to_dest = count_steps(
        PICKUP_LOC['lat'],  PICKUP_LOC['lng'],
        DEST_LOC['lat'],    DEST_LOC['lng'],
    )
    total_steps = steps_to_pickup + steps_to_dest
    sleep_secs = max(0.05, (duration - fixed_overhead) / total_steps)

    def _run():
        try:
            producer = Producer(KAFKA_CONFIG)

            log("🚀", f"Simulasi dimulai: 1 Driver + 1 Rider (durasi ~{duration}s, {total_steps} langkah, {sleep_secs:.2f}s/langkah)")
            print("─" * 60, flush=True)

            # 1) Publish posisi awal driver
            log("🚗", f"Driver [{DRIVER['name']}] online di ({DRIVER_START['lat']}, {DRIVER_START['lng']})")
            driver_pos = publish_driver_location(producer, DRIVER_START, status="available")
            time.sleep(1)

            # 2) Publish ride request
            log("🙋", f"Rider [{RIDER['name']}] meminta perjalanan ke ({DEST_LOC['lat']}, {DEST_LOC['lng']})")
            rider_request = publish_rider_request(producer)
            time.sleep(1)

            # 3) Matching
            log("🔍", "Mencari driver terdekat...")
            match_info = do_matching(producer, DRIVER_START, rider_request)
            log("✅", (
                f"MATCHED! {RIDER['name']} → {DRIVER['name']} | "
                f"Jarak: {match_info['distance_km']} km | "
                f"ETA: {match_info['eta_minutes']} menit"
            ))
            print("─" * 60, flush=True)
            time.sleep(1)

            # 4) Tracking + status updates
            log("🛤️", "Tracking perjalanan dimulai...")
            success = do_tracking(producer, match_info, sleep_secs=sleep_secs)

            print("─" * 60, flush=True)
            if success:
                log("🎉", "SIMULASI BERHASIL! Perjalanan selesai dengan sukses.")
                result["success"] = True
            else:
                log("❌", "SIMULASI GAGAL: Perjalanan tidak selesai.")

        except Exception as e:
            log("❌", f"Exception: {e}")
            import traceback
            traceback.print_exc()

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    thread.join(timeout=timeout)

    if thread.is_alive():
        log("⏰", f"Timeout setelah {timeout} detik!")
        return False

    return result["success"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulasi 1 driver + 1 rider")
    parser.add_argument("--duration", "-d", type=int, default=60,
                        help="Durasi simulasi dalam detik (default: 60)")
    parser.add_argument("--timeout", "-t", type=int, default=None,
                        help="Timeout paksa dalam detik (default: duration + 30)")
    args = parser.parse_args()

    timeout = args.timeout if args.timeout else args.duration + 30
    success = run_simulation(timeout, duration=args.duration)
    sys.exit(0 if success else 1)
