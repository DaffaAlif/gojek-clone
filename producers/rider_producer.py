import json
import time
import random
import argparse
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime
from confluent_kafka import Producer
from config import KAFKA_CONFIG, TOPICS, JAKARTA_BOUNDS

ALL_RIDERS = [
    {"rider_id": "RDR-001", "name": "Rina"},
    {"rider_id": "RDR-002", "name": "Doni"},
    {"rider_id": "RDR-003", "name": "Mega"},
]

ALL_SERVICES = ["GoRide", "GoCar"]

def random_location():
    return {
        "lat": round(random.uniform(JAKARTA_BOUNDS['lat_min'], JAKARTA_BOUNDS['lat_max']), 6),
        "lng": round(random.uniform(JAKARTA_BOUNDS['lng_min'], JAKARTA_BOUNDS['lng_max']), 6)
    }

def delivery_report(err, msg):
    if err:
        print(f"❌ Gagal: {err}")
    else:
        data = json.loads(msg.value().decode('utf-8'))
        print(
            f"🙋 [{data['timestamp']}] "
            f"{data['name']} minta ojek | "
            f"Pickup: ({data['pickup']['lat']:.4f}, {data['pickup']['lng']:.4f})"
        )

def parse_args():
    parser = argparse.ArgumentParser(description="Rider request producer")
    parser.add_argument(
        "--interval", "-i", type=float, default=8.0,
        help="Interval request dalam detik (default: 8)"
    )
    parser.add_argument(
        "--batch", "-b", type=int, default=2,
        choices=range(1, len(ALL_RIDERS) + 1),
        metavar=f"1-{len(ALL_RIDERS)}",
        help=f"Maks rider per batch (default: 2)"
    )
    parser.add_argument(
        "--service", "-s", choices=ALL_SERVICES + ["random"], default="random",
        help="Paksa layanan tertentu: GoRide, GoCar, atau random (default: random)"
    )
    parser.add_argument(
        "--count", "-c", type=int, default=len(ALL_RIDERS),
        choices=range(1, len(ALL_RIDERS) + 1),
        metavar=f"1-{len(ALL_RIDERS)}",
        help=f"Jumlah rider aktif (default: {len(ALL_RIDERS)})"
    )
    return parser.parse_args()

def run():
    args = parse_args()
    riders = ALL_RIDERS[:args.count]
    batch_max = min(args.batch, len(riders))

    producer = Producer(KAFKA_CONFIG)
    print("🙋 Rider Producer mulai...")
    print(f"⚙️  Rider aktif: {args.count} | Interval: {args.interval}s | Batch maks: {batch_max} | Layanan: {args.service}")
    print("─" * 60)

    try:
        while True:
            batch = random.sample(riders, k=random.randint(1, batch_max))
            for rider in batch:
                service = random.choice(ALL_SERVICES) if args.service == "random" else args.service
                payload = {
                    "rider_id":    rider["rider_id"],
                    "name":        rider["name"],
                    "pickup":      random_location(),
                    "destination": random_location(),
                    "service":     service,
                    "timestamp":   datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                producer.produce(
                    topic=TOPICS['ride_requests'],
                    key=rider["rider_id"],
                    value=json.dumps(payload),
                    callback=delivery_report
                )
            producer.flush()
            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\n⛔ Rider Producer dihentikan.")

if __name__ == "__main__":
    run()