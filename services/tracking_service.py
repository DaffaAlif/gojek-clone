import json
import math
import threading
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime
from confluent_kafka import Consumer, Producer
from config import KAFKA_CONFIG, TOPICS

# ─── State ────────────────────────────────────────────
active_rides = {}      # driver_id → ride info
driver_locations = {}  # driver_id → latest location
lock = threading.Lock()

def haversine(lat1, lng1, lat2, lng2):
    R = 6371
    d_lat = math.radians(lat2 - lat1)
    d_lng = math.radians(lng2 - lng1)
    a = (math.sin(d_lat / 2) ** 2 +
         math.cos(math.radians(lat1)) *
         math.cos(math.radians(lat2)) *
         math.sin(d_lng / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def move_towards(current_lat, current_lng, target_lat, target_lng, step=0.002):
    """Gerakkan posisi mendekati target"""
    d_lat = target_lat - current_lat
    d_lng = target_lng - current_lng
    distance = math.sqrt(d_lat**2 + d_lng**2)

    if distance < step:
        return target_lat, target_lng, True  # sudah sampai

    ratio = step / distance
    new_lat = current_lat + d_lat * ratio
    new_lng = current_lng + d_lng * ratio
    return new_lat, new_lng, False  # belum sampai

# ─── Thread 1: Pantau ride yang di-match ──────────────
def listen_matches():
    consumer = Consumer({
        **KAFKA_CONFIG,
        'group.id': 'tracking-match-group',
        'auto.offset.reset': 'latest'
    })
    consumer.subscribe([TOPICS['ride_matched']])
    producer = Producer(KAFKA_CONFIG)

    print("📡 Menunggu ride matched...")

    while True:
        msg = consumer.poll(1.0)
        if not msg or msg.error():
            continue
        try:
            data = json.loads(msg.value().decode('utf-8'))
            driver_id = data['driver_id']

            with lock:
                active_rides[driver_id] = {
                    "rider_id":    data['rider_id'],
                    "rider_name":  data['rider_name'],
                    "driver_id":   driver_id,
                    "driver_name": data['driver_name'],
                    "vehicle":     data['vehicle'],
                    "pickup":      data['pickup'],
                    "destination": data['destination'],
                    "phase":       "to_pickup",  # to_pickup → to_destination → completed
                    "service":     data['service'],
                }

            # Publish status: accepted
            status_payload = {
                "driver_id":  driver_id,
                "rider_id":   data['rider_id'],
                "status":     "accepted",
                "timestamp":  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            producer.produce(
                topic=TOPICS['ride_status'],
                key=driver_id,
                value=json.dumps(status_payload)
            )
            producer.flush()

            print(f"🆕 Tracking dimulai: {data['driver_name']} → {data['rider_name']}")

        except Exception as e:
            print(f"Error: {e}")

# ─── Thread 2: Pantau lokasi & update tracking ────────
def track_rides():
    import time
    producer = Producer(KAFKA_CONFIG)

    print("🔄 Tracking loop aktif...")

    while True:
        time.sleep(3)

        with lock:
            rides_copy = dict(active_rides)

        for driver_id, ride in rides_copy.items():
            # Ambil lokasi driver terakhir
            with lock:
                loc = driver_locations.get(driver_id)
            
            if not loc:
                continue

            current_lat = loc['lat']
            current_lng = loc['lng']

            if ride['phase'] == 'to_pickup':
                target = ride['pickup']
                new_lat, new_lng, arrived = move_towards(
                    current_lat, current_lng,
                    target['lat'], target['lng']
                )

                if arrived:
                    with lock:
                        active_rides[driver_id]['phase'] = 'to_destination'
                    
                    # Status: pickup
                    status = {
                        "driver_id":  driver_id,
                        "rider_id":   ride['rider_id'],
                        "status":     "picked_up",
                        "timestamp":  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                    producer.produce(
                        topic=TOPICS['ride_status'],
                        key=driver_id,
                        value=json.dumps(status)
                    )
                    print(f"🙋 {ride['driver_name']} menjemput {ride['rider_name']}!")

                phase_label = "Menuju pickup"

            elif ride['phase'] == 'to_destination':
                target = ride['destination']
                new_lat, new_lng, arrived = move_towards(
                    current_lat, current_lng,
                    target['lat'], target['lng']
                )

                if arrived:
                    # Status: completed
                    status = {
                        "driver_id":  driver_id,
                        "rider_id":   ride['rider_id'],
                        "status":     "completed",
                        "timestamp":  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                    producer.produce(
                        topic=TOPICS['ride_status'],
                        key=driver_id,
                        value=json.dumps(status)
                    )
                    print(f"✅ {ride['driver_name']} selesai antar {ride['rider_name']}!")

                    with lock:
                        del active_rides[driver_id]
                    producer.flush()
                    continue

                phase_label = "Menuju destinasi"
            else:
                continue

            # Update lokasi driver di memory
            with lock:
                driver_locations[driver_id] = {"lat": new_lat, "lng": new_lng}

            # Publish tracking update
            distance_to_target = haversine(
                new_lat, new_lng,
                target['lat'], target['lng']
            )

            tracking_payload = {
                "driver_id":    driver_id,
                "rider_id":     ride['rider_id'],
                "driver_name":  ride['driver_name'],
                "rider_name":   ride['rider_name'],
                "phase":        ride['phase'],
                "phase_label":  phase_label,
                "current_lat":  round(new_lat, 6),
                "current_lng":  round(new_lng, 6),
                "target_lat":   target['lat'],
                "target_lng":   target['lng'],
                "distance_km":  round(distance_to_target, 2),
                "timestamp":    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            producer.produce(
                topic=TOPICS['ride_tracking'],
                key=driver_id,
                value=json.dumps(tracking_payload)
            )
            producer.flush()

            print(
                f"📍 {ride['driver_name']} | {phase_label} | "
                f"Sisa: {distance_to_target:.2f} km"
            )

# ─── Thread 3: Consume lokasi driver ─────────────────
def listen_locations():
    consumer = Consumer({
        **KAFKA_CONFIG,
        'group.id': 'tracking-location-group',
        'auto.offset.reset': 'latest'
    })
    consumer.subscribe([TOPICS['location_updates']])

    while True:
        msg = consumer.poll(1.0)
        if not msg or msg.error():
            continue
        try:
            data = json.loads(msg.value().decode('utf-8'))
            with lock:
                driver_locations[data['driver_id']] = {
                    "lat": data['lat'],
                    "lng": data['lng']
                }
        except:
            continue

# ─── Main ─────────────────────────────────────────────
if __name__ == "__main__":
    print("🛤️  Tracking Service mulai...")
    print("─" * 60)

    t1 = threading.Thread(target=listen_matches,   daemon=True)
    t2 = threading.Thread(target=track_rides,       daemon=True)
    t3 = threading.Thread(target=listen_locations,  daemon=True)

    t1.start()
    t2.start()
    t3.start()

    try:
        while True:
            import time
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n⛔ Tracking Service dihentikan.")