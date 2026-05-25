"""
Integration tests for ride-hailing-kafka.
Requires a running Kafka broker at localhost:9092.
Run after docker-compose up -d.
"""
import json
import math
import time
import sys
import os
import pytest
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime
from confluent_kafka import Producer, Consumer, KafkaError
from config import KAFKA_CONFIG, TOPICS

TIMEOUT_SECONDS = 30

# ─── Helpers ─────────────────────────────────────────────────────────────────

def make_producer():
    return Producer(KAFKA_CONFIG)


def make_consumer(group_id, topics, offset='latest'):
    c = Consumer({
        **KAFKA_CONFIG,
        'group.id': group_id,
        'auto.offset.reset': offset,
    })
    c.subscribe(topics)
    return c


def haversine(lat1, lng1, lat2, lng2):
    R = 6371
    d_lat = math.radians(lat2 - lat1)
    d_lng = math.radians(lng2 - lng1)
    a = (math.sin(d_lat / 2) ** 2 +
         math.cos(math.radians(lat1)) *
         math.cos(math.radians(lat2)) *
         math.sin(d_lng / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def consume_one_message(consumer, timeout=TIMEOUT_SECONDS):
    deadline = time.time() + timeout
    while time.time() < deadline:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            raise RuntimeError(f"Kafka error: {msg.error()}")
        return json.loads(msg.value().decode('utf-8'))
    return None


# ─── Test: Kafka Connectivity ────────────────────────────────────────────────

class TestKafkaConnectivity:
    def test_producer_can_connect(self):
        producer = make_producer()
        assert producer is not None

    def test_consumer_can_subscribe(self):
        consumer = make_consumer('test-conn-group', [TOPICS['location_updates']])
        assert consumer is not None
        consumer.close()

    def test_produce_and_consume_location_update(self):
        unique_driver_id = f"TEST-DRV-{int(time.time())}"
        payload = {
            "driver_id": unique_driver_id,
            "name": "TestDriver",
            "vehicle": "motorcycle",
            "lat": -6.2088,
            "lng": 106.8456,
            "status": "available",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "speed_kmh": 30.0,
        }

        consumer = make_consumer(
            f'test-loc-group-{int(time.time())}',
            [TOPICS['location_updates']],
            offset='latest'
        )
        time.sleep(1)

        producer = make_producer()
        producer.produce(
            topic=TOPICS['location_updates'],
            key=unique_driver_id,
            value=json.dumps(payload),
        )
        producer.flush()

        received = consume_one_message(consumer)
        consumer.close()

        assert received is not None, "Pesan tidak diterima dalam batas waktu"
        assert received['driver_id'] == unique_driver_id


# ─── Test: Ride Request Flow ──────────────────────────────────────────────────

class TestRideRequestFlow:
    def test_produce_and_consume_ride_request(self):
        unique_rider_id = f"TEST-RDR-{int(time.time())}"
        payload = {
            "rider_id": unique_rider_id,
            "name": "TestRider",
            "pickup": {"lat": -6.2088, "lng": 106.8456},
            "destination": {"lat": -6.2300, "lng": 106.8600},
            "service": "GoRide",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        consumer = make_consumer(
            f'test-req-group-{int(time.time())}',
            [TOPICS['ride_requests']],
            offset='latest'
        )
        time.sleep(1)

        producer = make_producer()
        producer.produce(
            topic=TOPICS['ride_requests'],
            key=unique_rider_id,
            value=json.dumps(payload),
        )
        producer.flush()

        received = consume_one_message(consumer)
        consumer.close()

        assert received is not None, "Ride request tidak diterima"
        assert received['rider_id'] == unique_rider_id
        assert 'pickup' in received
        assert 'destination' in received


# ─── Test: Inline Matching Simulation ────────────────────────────────────────

class TestInlineMatchingSimulation:
    """Simulate matching logic inline without spawning service processes."""

    def test_match_driver_to_rider(self):
        driver_id = f"SIM-DRV-{int(time.time())}"
        rider_id = f"SIM-RDR-{int(time.time())}"

        driver_data = {
            "driver_id": driver_id,
            "name": "SimDriver",
            "vehicle": "motorcycle",
            "lat": -6.2090,
            "lng": 106.8460,
            "status": "available",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "speed_kmh": 35.0,
        }

        rider_request = {
            "rider_id": rider_id,
            "name": "SimRider",
            "pickup": {"lat": -6.2095, "lng": 106.8465},
            "destination": {"lat": -6.2300, "lng": 106.8700},
            "service": "GoRide",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        # ── Produce driver location ─────────────────────────
        loc_consumer = make_consumer(
            f'sim-loc-group-{int(time.time())}',
            [TOPICS['location_updates']],
            offset='latest'
        )
        match_consumer = make_consumer(
            f'sim-match-group-{int(time.time())}',
            [TOPICS['ride_matched']],
            offset='latest'
        )
        time.sleep(1)

        producer = make_producer()
        producer.produce(
            topic=TOPICS['location_updates'],
            key=driver_id,
            value=json.dumps(driver_data),
        )
        producer.flush()

        # ── Verify driver location was published ────────────
        loc_msg = consume_one_message(loc_consumer, timeout=15)
        loc_consumer.close()
        assert loc_msg is not None, "Lokasi driver tidak dipublikasikan"
        assert loc_msg['status'] == 'available'

        # ── Run inline matching ──────────────────────────────
        pickup = rider_request['pickup']
        distance = haversine(
            pickup['lat'], pickup['lng'],
            driver_data['lat'], driver_data['lng']
        )
        eta_minutes = max(round((distance / 30) * 60), 1)

        match_result = {
            "rider_id": rider_id,
            "rider_name": rider_request['name'],
            "driver_id": driver_id,
            "driver_name": driver_data['name'],
            "vehicle": driver_data['vehicle'],
            "pickup": rider_request['pickup'],
            "destination": rider_request['destination'],
            "distance_km": round(distance, 2),
            "eta_minutes": eta_minutes,
            "service": rider_request['service'],
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        producer.produce(
            topic=TOPICS['ride_matched'],
            key=rider_id,
            value=json.dumps(match_result),
        )
        producer.flush()

        # ── Verify match was published ───────────────────────
        matched = consume_one_message(match_consumer, timeout=15)
        match_consumer.close()

        assert matched is not None, "Match result tidak dipublikasikan"
        assert matched['rider_id'] == rider_id
        assert matched['driver_id'] == driver_id
        assert matched['distance_km'] >= 0
        assert matched['eta_minutes'] >= 1

    def test_ride_status_transitions(self):
        """Verifikasi status ride bisa dipublish: accepted → picked_up → completed"""
        driver_id = f"SIM-TRACK-DRV-{int(time.time())}"
        rider_id = f"SIM-TRACK-RDR-{int(time.time())}"

        status_consumer = make_consumer(
            f'sim-status-group-{int(time.time())}',
            [TOPICS['ride_status']],
            offset='latest'
        )
        time.sleep(1)

        producer = make_producer()
        statuses = ["accepted", "picked_up", "completed"]
        for status in statuses:
            payload = {
                "driver_id": driver_id,
                "rider_id": rider_id,
                "status": status,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
            producer.produce(
                topic=TOPICS['ride_status'],
                key=driver_id,
                value=json.dumps(payload),
            )
        producer.flush()

        received_statuses = []
        deadline = time.time() + 20
        while len(received_statuses) < 3 and time.time() < deadline:
            msg = status_consumer.poll(1.0)
            if msg and not msg.error():
                data = json.loads(msg.value().decode('utf-8'))
                if data['driver_id'] == driver_id:
                    received_statuses.append(data['status'])

        status_consumer.close()

        assert len(received_statuses) == 3, \
            f"Hanya {len(received_statuses)} status diterima, expected 3"
        assert received_statuses == ["accepted", "picked_up", "completed"]
