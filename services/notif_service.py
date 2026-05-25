import json
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from confluent_kafka import Consumer
from config import KAFKA_CONFIG, TOPICS

def run():
    consumer = Consumer({
        **KAFKA_CONFIG,
        'group.id': 'notif-group',
        'auto.offset.reset': 'latest'
    })
    consumer.subscribe([TOPICS['ride_matched']])

    print("🔔 Notif Service mulai...")
    print("─" * 60)

    try:
        while True:
            msg = consumer.poll(1.0)
            if not msg or msg.error():
                continue
            try:
                data = json.loads(msg.value().decode('utf-8'))
                print(f"""
╔══════════════════════════════════════════╗
║           🚗 RIDE MATCHED!               ║
╠══════════════════════════════════════════╣
║ Rider  : {data['rider_name']:<32}║
║ Driver : {data['driver_name']:<32}║
║ Kendaraan: {data['vehicle']:<30}║
║ Layanan: {data['service']:<32}║
╠══════════════════════════════════════════╣
║ Jarak  : {str(data['distance_km']) + ' km':<32}║
║ ETA    : {str(data['eta_minutes']) + ' menit':<32}║
║ Waktu  : {data['timestamp']:<32}║
╚══════════════════════════════════════════╝
                """)
            except:
                continue
    except KeyboardInterrupt:
        print("\n⛔ Notif Service dihentikan.")

if __name__ == "__main__":
    run()