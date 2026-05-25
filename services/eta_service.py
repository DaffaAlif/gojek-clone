import json
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime
from confluent_kafka import Consumer, Producer
from config import KAFKA_CONFIG, TOPICS

# Kecepatan rata-rata per kendaraan (km/h)
SPEED = {
    "motorcycle": 30,
    "car":        25,
}

# Simpan info kendaraan dari ride-matched
vehicle_map = {}

def run():
    consumer_tracking = Consumer({
        **KAFKA_CONFIG,
        'group.id': 'eta-tracking-group',
        'auto.offset.reset': 'latest'
    })

    consumer_matched = Consumer({
        **KAFKA_CONFIG,
        'group.id': 'eta-matched-group',
        'auto.offset.reset': 'latest'
    })

    producer = Producer(KAFKA_CONFIG)

    consumer_tracking.subscribe([TOPICS['ride_tracking']])
    consumer_matched.subscribe([TOPICS['ride_matched']])

    print("⏱️  ETA Service mulai...")
    print("─" * 60)

    import threading

    # Thread untuk ambil info kendaraan
    def listen_matched():
        while True:
            msg = consumer_matched.poll(1.0)
            if not msg or msg.error():
                continue
            try:
                data = json.loads(msg.value().decode('utf-8'))
                vehicle_map[data['driver_id']] = data['vehicle']
            except:
                continue

    t = threading.Thread(target=listen_matched, daemon=True)
    t.start()

    # Main loop: consume tracking, hitung ETA
    try:
        while True:
            msg = consumer_tracking.poll(1.0)
            if not msg or msg.error():
                continue
            try:
                data = json.loads(msg.value().decode('utf-8'))
                driver_id = data['driver_id']
                distance  = data['distance_km']

                # Ambil kecepatan berdasarkan kendaraan
                vehicle = vehicle_map.get(driver_id, "motorcycle")
                speed   = SPEED.get(vehicle, 25)

                # Hitung ETA
                eta_minutes = round((distance / speed) * 60)
                if eta_minutes < 1:
                    eta_minutes = 1

                # Publish ETA
                eta_payload = {
                    "driver_id":   driver_id,
                    "rider_id":    data['rider_id'],
                    "driver_name": data['driver_name'],
                    "rider_name":  data['rider_name'],
                    "phase":       data['phase'],
                    "phase_label": data['phase_label'],
                    "distance_km": distance,
                    "speed_kmh":   speed,
                    "eta_minutes": eta_minutes,
                    "vehicle":     vehicle,
                    "timestamp":   datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }

                producer.produce(
                    topic=TOPICS['eta_updates'],
                    key=driver_id,
                    value=json.dumps(eta_payload)
                )
                producer.flush()

                # Emoji berdasarkan phase
                icon = "🏍️" if vehicle == "motorcycle" else "🚗"

                print(
                    f"{icon} {data['driver_name']} → {data['rider_name']} | "
                    f"{data['phase_label']} | "
                    f"Sisa: {distance} km | "
                    f"ETA: {eta_minutes} menit"
                )

            except Exception as e:
                print(f"Error: {e}")

    except KeyboardInterrupt:
        print("\n⛔ ETA Service dihentikan.")

if __name__ == "__main__":
    run()