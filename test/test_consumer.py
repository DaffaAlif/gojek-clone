from confluent_kafka import Consumer
import json

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'test-group',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(['location-updates'])

print("🎧 Mendengarkan pesan...")

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(f"Error: {msg.error()}")
        continue

    raw = msg.value()
    if not raw:
        continue  

    try:
        data = json.loads(raw.decode('utf-8'))
        print(f"📍 Driver: {data['driver_id']} | Lokasi: {data['lat']}, {data['lng']}")
    except json.JSONDecodeError:
        print(f"⚠️ Pesan bukan JSON, skip: {raw}")
        continue