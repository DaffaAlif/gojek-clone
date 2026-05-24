from confluent_kafka import Producer
import json

producer = Producer({'bootstrap.servers': 'localhost:9092'})

def delivery_report(err, msg):
    if err:
        print(f'❌ Error: {err}')
    else:
        print(f'✅ Pesan terkirim ke topic [{msg.topic()}]')

# Kirim pesan
data = {
    "driver_id": "DRV-001",
    "lat": -6.2088,
    "lng": 106.8456,
    "status": "available"
}

producer.produce(
    topic='location-updates',
    value=json.dumps(data),
    callback=delivery_report
)

producer.flush()