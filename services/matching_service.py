import json
import math
import threading
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime
from confluent_kafka import Consumer, Producer
from config import KAFKA_CONFIG, TOPICS

# Simpan posisi driver terbaru di memory
driver_registry = {}
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

def listen_locations():
    consumer = Consumer({
        **KAFKA_CONFIG,
        'group.id': 'matching-location-group',
        'auto.offset.reset': 'latest'
    })
    consumer.subscribe([TOPICS['location_updates']])
    print("📡 Memantau lokasi driver...")

    while True:
        msg = consumer.poll(1.0)
        if not msg or msg.error():
            continue
        try:
            data = json.loads(msg.value().decode('utf-8'))
            # Simpan hanya driver yang available
            with lock:
                if data['status'] == 'available':
                    driver_registry[data['driver_id']] = data
                elif data['driver_id'] in driver_registry:
                    del driver_registry[data['driver_id']]
        except:
            continue

def listen_requests():
    consumer = Consumer({
        **KAFKA_CONFIG,
        'group.id': 'matching-request-group',
        'auto.offset.reset': 'latest'
    })
    producer = Producer(KAFKA_CONFIG)
    consumer.subscribe([TOPICS['ride_requests']])
    print("🔍 Menunggu request rider...")

    while True:
        msg = consumer.poll(1.0)
        if not msg or msg.error():
            continue
        try:
            request = json.loads(msg.value().decode('utf-8'))
            
            pickup  = request['pickup']

            with lock:
                drivers = list(driver_registry.values())

            if not drivers:
                print(f"⚠️  Tidak ada driver available untuk {request['name']}")
                continue

            nearest = min(drivers, key=lambda d: haversine(
                pickup['lat'], pickup['lng'],
                d['lat'], d['lng']
            ))

            distance = haversine(
                pickup['lat'], pickup['lng'],
                nearest['lat'], nearest['lng']
            )

            eta_minutes = round((distance / 30) * 60)

            result = {
                "rider_id":   request['rider_id'],
                "rider_name": request['name'],
                "driver_id":  nearest['driver_id'],
                "driver_name":nearest['name'],
                "vehicle":    nearest['vehicle'],
                "pickup":     pickup,
                "destination":request['destination'],
                "distance_km":round(distance, 2),
                "eta_minutes":eta_minutes,
                "service":    request['service'],
                "timestamp":  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }

            producer.produce(
                topic=TOPICS['ride_matched'],
                key=request['rider_id'],
                value=json.dumps(result)
            )
            producer.flush()

            print(
                f"✅ MATCH! {request['name']} → {nearest['name']} | "
                f"Jarak: {distance:.2f} km | ETA: {eta_minutes} menit"
            )
            print(f"🙋 [{request['timestamp']}] {request['name']} minta ojek")

        except KeyboardInterrupt:
            print("\n⛔ Notif Service dihentikan.")

if __name__ == "__main__":
    print("🚀 Matching Service mulai...")
    print("─" * 60)

    t1 = threading.Thread(target=listen_locations, daemon=True)
    t2 = threading.Thread(target=listen_requests,  daemon=True)

    t1.start()
    t2.start()

    try:
        t1.join()
        t2.join()
    except KeyboardInterrupt:
        print("\n⛔ Matching Service dihentikan.")