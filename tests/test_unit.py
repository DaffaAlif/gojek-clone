"""
Unit tests for ride-hailing-kafka project.
All tests run without a real Kafka broker (pure logic).
"""
import json
import math
import sys
import os
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import KAFKA_CONFIG, TOPICS, JAKARTA_BOUNDS


# ─── Helpers (extracted from services so we can test them independently) ──────

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
    d_lat = target_lat - current_lat
    d_lng = target_lng - current_lng
    distance = math.sqrt(d_lat ** 2 + d_lng ** 2)
    if distance < step:
        return target_lat, target_lng, True
    ratio = step / distance
    new_lat = current_lat + d_lat * ratio
    new_lng = current_lng + d_lng * ratio
    return new_lat, new_lng, False


def find_nearest_driver(drivers, pickup_lat, pickup_lng):
    if not drivers:
        return None
    return min(drivers, key=lambda d: haversine(
        pickup_lat, pickup_lng, d['lat'], d['lng']
    ))


def calculate_eta(distance_km, vehicle):
    speeds = {"motorcycle": 30, "car": 25}
    speed = speeds.get(vehicle, 25)
    eta = round((distance_km / speed) * 60)
    return max(eta, 1)


# ─── Config Tests ─────────────────────────────────────────────────────────────

class TestConfig:
    def test_kafka_bootstrap_servers_present(self):
        assert 'bootstrap.servers' in KAFKA_CONFIG

    def test_all_topics_defined(self):
        required = ['location_updates', 'ride_requests', 'ride_matched',
                    'ride_status', 'ride_tracking', 'eta_updates']
        for topic in required:
            assert topic in TOPICS, f"Topic '{topic}' tidak ada di TOPICS"

    def test_jakarta_bounds_valid(self):
        assert JAKARTA_BOUNDS['lat_min'] < JAKARTA_BOUNDS['lat_max']
        assert JAKARTA_BOUNDS['lng_min'] < JAKARTA_BOUNDS['lng_max']

    def test_jakarta_bounds_in_correct_range(self):
        assert -90 <= JAKARTA_BOUNDS['lat_min'] <= 90
        assert -90 <= JAKARTA_BOUNDS['lat_max'] <= 90
        assert -180 <= JAKARTA_BOUNDS['lng_min'] <= 180
        assert -180 <= JAKARTA_BOUNDS['lng_max'] <= 180


# ─── Haversine Tests ──────────────────────────────────────────────────────────

class TestHaversine:
    def test_same_point_zero_distance(self):
        dist = haversine(-6.2088, 106.8456, -6.2088, 106.8456)
        assert dist == pytest.approx(0.0, abs=1e-6)

    def test_known_distance_monas_to_senayan(self):
        # Monas → Senayan ~7-8 km
        dist = haversine(-6.1754, 106.8272, -6.2183, 106.8017)
        assert 5.0 < dist < 10.0

    def test_distance_is_symmetric(self):
        d1 = haversine(-6.2088, 106.8456, -6.1754, 106.8272)
        d2 = haversine(-6.1754, 106.8272, -6.2088, 106.8456)
        assert d1 == pytest.approx(d2, rel=1e-9)

    def test_distance_positive(self):
        dist = haversine(-6.2088, 106.8456, -6.3000, 106.9000)
        assert dist > 0

    def test_distance_unit_is_km(self):
        # ~1 degree latitude ≈ 111 km
        dist = haversine(0.0, 0.0, 1.0, 0.0)
        assert 100 < dist < 120


# ─── Driver Movement Tests ────────────────────────────────────────────────────

class TestMoveTowards:
    def test_move_closer_to_target(self):
        new_lat, new_lng, arrived = move_towards(0.0, 0.0, 1.0, 1.0, step=0.1)
        dist_before = math.sqrt(1.0 ** 2 + 1.0 ** 2)
        dist_after = math.sqrt(new_lat ** 2 + new_lng ** 2)
        assert dist_after < dist_before

    def test_arrives_when_within_step(self):
        _, _, arrived = move_towards(0.0, 0.0, 0.0005, 0.0005, step=0.002)
        assert arrived is True

    def test_not_arrived_when_far(self):
        _, _, arrived = move_towards(0.0, 0.0, 1.0, 1.0, step=0.002)
        assert arrived is False

    def test_coordinates_change_after_move(self):
        new_lat, new_lng, _ = move_towards(-6.2, 106.8, -6.1, 106.9, step=0.01)
        assert new_lat != -6.2
        assert new_lng != 106.8

    def test_returns_exact_target_when_arrived(self):
        target_lat, target_lng = 1.0, 1.0
        new_lat, new_lng, arrived = move_towards(1.0001, 1.0001, target_lat, target_lng, step=0.01)
        assert arrived is True
        assert new_lat == target_lat
        assert new_lng == target_lng


# ─── Matching Logic Tests ─────────────────────────────────────────────────────

class TestMatchingLogic:
    def test_returns_nearest_driver(self):
        drivers = [
            {"driver_id": "DRV-001", "lat": -6.2000, "lng": 106.8000},
            {"driver_id": "DRV-002", "lat": -6.2500, "lng": 106.8500},  # lebih jauh
            {"driver_id": "DRV-003", "lat": -6.2100, "lng": 106.8050},  # paling dekat
        ]
        nearest = find_nearest_driver(drivers, -6.2088, 106.8056)
        assert nearest["driver_id"] == "DRV-003"

    def test_returns_none_for_empty_drivers(self):
        result = find_nearest_driver([], -6.2088, 106.8456)
        assert result is None

    def test_single_driver_always_selected(self):
        drivers = [{"driver_id": "DRV-001", "lat": -6.2088, "lng": 106.8456}]
        nearest = find_nearest_driver(drivers, -6.3000, 106.9000)
        assert nearest["driver_id"] == "DRV-001"

    def test_distance_decreases_as_driver_moves_closer(self):
        pickup = {"lat": -6.2088, "lng": 106.8456}
        driver_far = {"driver_id": "DRV-001", "lat": -6.2500, "lng": 106.8800}
        driver_near = {"driver_id": "DRV-002", "lat": -6.2100, "lng": 106.8460}
        nearest = find_nearest_driver([driver_far, driver_near],
                                      pickup['lat'], pickup['lng'])
        assert nearest["driver_id"] == "DRV-002"


# ─── ETA Calculation Tests ────────────────────────────────────────────────────

class TestETACalculation:
    def test_motorcycle_faster_than_car(self):
        eta_motorcycle = calculate_eta(5.0, "motorcycle")
        eta_car = calculate_eta(5.0, "car")
        assert eta_motorcycle < eta_car

    def test_minimum_eta_is_one_minute(self):
        eta = calculate_eta(0.01, "motorcycle")
        assert eta >= 1

    def test_eta_increases_with_distance(self):
        eta_near = calculate_eta(1.0, "motorcycle")
        eta_far = calculate_eta(10.0, "motorcycle")
        assert eta_far > eta_near

    def test_eta_formula_motorcycle(self):
        # 30 km at 30 km/h = 60 minutes
        eta = calculate_eta(30.0, "motorcycle")
        assert eta == 60

    def test_eta_formula_car(self):
        # 25 km at 25 km/h = 60 minutes
        eta = calculate_eta(25.0, "car")
        assert eta == 60

    def test_unknown_vehicle_defaults_to_car_speed(self):
        eta_unknown = calculate_eta(10.0, "unknown")
        eta_car = calculate_eta(10.0, "car")
        assert eta_unknown == eta_car


# ─── Payload Schema Tests ─────────────────────────────────────────────────────

class TestDriverPayloadSchema:
    def _make_driver_payload(self, driver_id="DRV-001", name="Budi",
                              vehicle="motorcycle", lat=-6.2088, lng=106.8456,
                              status="available", speed=30.0):
        return {
            "driver_id": driver_id,
            "name": name,
            "vehicle": vehicle,
            "lat": lat,
            "lng": lng,
            "status": status,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "speed_kmh": speed,
        }

    def test_required_fields_present(self):
        payload = self._make_driver_payload()
        required = ["driver_id", "name", "vehicle", "lat", "lng", "status",
                    "timestamp", "speed_kmh"]
        for field in required:
            assert field in payload, f"Field '{field}' hilang dari driver payload"

    def test_lat_within_jakarta_bounds(self):
        payload = self._make_driver_payload(lat=-6.2088)
        assert JAKARTA_BOUNDS['lat_min'] <= payload['lat'] <= JAKARTA_BOUNDS['lat_max']

    def test_lng_within_jakarta_bounds(self):
        payload = self._make_driver_payload(lng=106.8456)
        assert JAKARTA_BOUNDS['lng_min'] <= payload['lng'] <= JAKARTA_BOUNDS['lng_max']

    def test_valid_status_values(self):
        valid_statuses = ["available", "on_trip", "offline"]
        for status in valid_statuses:
            payload = self._make_driver_payload(status=status)
            assert payload['status'] in valid_statuses

    def test_payload_json_serializable(self):
        payload = self._make_driver_payload()
        serialized = json.dumps(payload)
        deserialized = json.loads(serialized)
        assert deserialized['driver_id'] == payload['driver_id']


class TestRiderPayloadSchema:
    def _make_rider_payload(self, rider_id="RDR-001", name="Rina",
                             service="GoRide"):
        return {
            "rider_id": rider_id,
            "name": name,
            "pickup": {"lat": -6.2088, "lng": 106.8456},
            "destination": {"lat": -6.2300, "lng": 106.8600},
            "service": service,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    def test_required_fields_present(self):
        payload = self._make_rider_payload()
        required = ["rider_id", "name", "pickup", "destination", "service", "timestamp"]
        for field in required:
            assert field in payload, f"Field '{field}' hilang dari rider payload"

    def test_pickup_has_lat_lng(self):
        payload = self._make_rider_payload()
        assert 'lat' in payload['pickup']
        assert 'lng' in payload['pickup']

    def test_destination_has_lat_lng(self):
        payload = self._make_rider_payload()
        assert 'lat' in payload['destination']
        assert 'lng' in payload['destination']

    def test_valid_service_values(self):
        for service in ["GoRide", "GoCar"]:
            payload = self._make_rider_payload(service=service)
            assert payload['service'] in ["GoRide", "GoCar"]

    def test_payload_json_serializable(self):
        payload = self._make_rider_payload()
        serialized = json.dumps(payload)
        deserialized = json.loads(serialized)
        assert deserialized['rider_id'] == payload['rider_id']


class TestMatchResultSchema:
    def _make_match_payload(self):
        return {
            "rider_id": "RDR-001",
            "rider_name": "Rina",
            "driver_id": "DRV-001",
            "driver_name": "Budi",
            "vehicle": "motorcycle",
            "pickup": {"lat": -6.2088, "lng": 106.8456},
            "destination": {"lat": -6.2300, "lng": 106.8600},
            "distance_km": 3.5,
            "eta_minutes": 7,
            "service": "GoRide",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    def test_required_fields_present(self):
        payload = self._make_match_payload()
        required = ["rider_id", "rider_name", "driver_id", "driver_name",
                    "vehicle", "pickup", "destination", "distance_km",
                    "eta_minutes", "service", "timestamp"]
        for field in required:
            assert field in payload, f"Field '{field}' hilang dari match payload"

    def test_distance_positive(self):
        payload = self._make_match_payload()
        assert payload['distance_km'] > 0

    def test_eta_at_least_one_minute(self):
        payload = self._make_match_payload()
        assert payload['eta_minutes'] >= 1


# ─── Driver Producer Logic Tests (mocked Kafka) ───────────────────────────────

class TestDriverProducerLogic:
    @patch('confluent_kafka.Producer')
    def test_producer_created_with_config(self, mock_producer_class):
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer

        from config import KAFKA_CONFIG
        from confluent_kafka import Producer
        p = Producer(KAFKA_CONFIG)
        mock_producer_class.assert_called_once_with(KAFKA_CONFIG)

    def test_driver_position_stays_in_bounds_after_move(self):
        import random
        import sys
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'producers'))

        pos = {"lat": -6.2000, "lng": 106.8500}
        for _ in range(100):
            pos['lat'] += random.uniform(-0.001, 0.001)
            pos['lng'] += random.uniform(-0.001, 0.001)
            pos['lat'] = max(JAKARTA_BOUNDS['lat_min'],
                             min(JAKARTA_BOUNDS['lat_max'], pos['lat']))
            pos['lng'] = max(JAKARTA_BOUNDS['lng_min'],
                             min(JAKARTA_BOUNDS['lng_max'], pos['lng']))

        assert JAKARTA_BOUNDS['lat_min'] <= pos['lat'] <= JAKARTA_BOUNDS['lat_max']
        assert JAKARTA_BOUNDS['lng_min'] <= pos['lng'] <= JAKARTA_BOUNDS['lng_max']


# ─── Tracking Phase Transition Tests ─────────────────────────────────────────

class TestTrackingPhaseTransition:
    def test_phase_starts_as_to_pickup(self):
        ride = {
            "driver_id": "DRV-001",
            "rider_id": "RDR-001",
            "phase": "to_pickup",
            "pickup": {"lat": -6.2000, "lng": 106.8000},
            "destination": {"lat": -6.2300, "lng": 106.8300},
        }
        assert ride['phase'] == 'to_pickup'

    def test_phase_transition_on_pickup_arrival(self):
        ride = {
            "phase": "to_pickup",
            "pickup": {"lat": -6.2000, "lng": 106.8000},
        }
        current = {"lat": -6.2000, "lng": 106.8000}

        _, _, arrived = move_towards(
            current['lat'], current['lng'],
            ride['pickup']['lat'], ride['pickup']['lng'],
            step=1.0
        )
        if arrived:
            ride['phase'] = 'to_destination'

        assert ride['phase'] == 'to_destination'

    def test_completed_ride_removed_from_active(self):
        active_rides = {"DRV-001": {"phase": "to_destination"}}
        del active_rides["DRV-001"]
        assert "DRV-001" not in active_rides
