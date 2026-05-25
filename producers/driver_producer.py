import json
import time
import random
import argparse
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime
from confluent_kafka import Producer
from config import KAFKA_CONFIG, TOPICS, JAKARTA_BOUNDS

ALL_DRIVERS = [
    {"driver_id": "DRV-001", "name": "Budi",  "vehicle": "motorcycle"},
    {"driver_id": "DRV-002", "name": "Andi",  "vehicle": "motorcycle"},
    {"driver_id": "DRV-003", "name": "Siti",  "vehicle": "car"},
    {"driver_id": "DRV-004", "name": "Rudi",  "vehicle": "motorcycle"},
    {"driver_id": "DRV-005", "name": "Dewi",  "vehicle": "car"},
]

STATUSES = ["available", "available", "available", "on_trip", "offline"]

driver_positions = {}

def init_driver_positions(drivers):
    for driver in drivers:
        driver_positions[driver["driver_id"]] = {
            "lat": random.uniform(JAKARTA_BOUNDS['lat_min'], JAKARTA_BOUNDS['lat_max']),
            "lng": random.uniform(JAKARTA_BOUNDS['lng_min'], JAKARTA_BOUNDS['lng_max'])
        }

def move_driver(driver_id):
    pos = driver_positions[driver_id]
    pos['lat'] += random.uniform(-0.001, 0.001)
    pos['lng'] += random.uniform(-0.001, 0.001)
    pos['lat'] = max(JAKARTA_BOUNDS['lat_min'], min(JAKARTA_BOUNDS['lat_max'], pos['lat']))
    pos['lng'] = max(JAKARTA_BOUNDS['lng_min'], min(JAKARTA_BOUNDS['lng_max'], pos['lng']))
    driver_positions[driver_id] = pos
    return pos

def delivery_report(err, msg):
    if err:
        print(f"❌ Gagal kirim: {err}")
    else:
        data = json.loads(msg.value().decode('utf-8'))
        print(
            f"📍 [{data['timestamp']}] "
            f"{data['name']} ({data['driver_id']}) | "
            f"Lat: {data['lat']:.4f}, Lng: {data['lng']:.4f} | "
            f"Status: {data['status']}"
        )

def parse_args():
    parser = argparse.ArgumentParser(description="Driver location producer")
    parser.add_argument(
        "--interval", "-i", type=float, default=3.0,
        help="Interval publish lokasi dalam detik (default: 3)"
    )
    parser.add_argument(
        "--count", "-c", type=int, default=len(ALL_DRIVERS),
        choices=range(1, len(ALL_DRIVERS) + 1),
        metavar=f"1-{len(ALL_DRIVERS)}",
        help=f"Jumlah driver aktif (default: {len(ALL_DRIVERS)})"
    )
    parser.add_argument(
        "--max-speed", type=float, default=60.0,
        help="Kecepatan maksimum driver km/h (default: 60)"
    )
    return parser.parse_args()

def run():
    args = parse_args()
    drivers = ALL_DRIVERS[:args.count]

    producer = Producer(KAFKA_CONFIG)
    print("🚗 Driver Producer mulai berjalan...")
    print(f"📡 Mengirim ke topic: [{TOPICS['location_updates']}]")
    print(f"⚙️  Driver aktif: {args.count} | Interval: {args.interval}s | Max speed: {args.max_speed} km/h")
    print("─" * 60)

    init_driver_positions(drivers)

    try:
        while True:
            for driver in drivers:
                pos = move_driver(driver["driver_id"])
                payload = {
                    "driver_id": driver["driver_id"],
                    "name":      driver["name"],
                    "vehicle":   driver["vehicle"],
                    "lat":       round(pos["lat"], 6),
                    "lng":       round(pos["lng"], 6),
                    "status":    random.choice(STATUSES),
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "speed_kmh": round(random.uniform(0, args.max_speed), 1)
                }
                producer.produce(
                    topic=TOPICS['location_updates'],
                    key=driver["driver_id"],
                    value=json.dumps(payload),
                    callback=delivery_report
                )
            producer.flush()
            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\n⛔ Driver Producer dihentikan.")

if __name__ == "__main__":
    run()