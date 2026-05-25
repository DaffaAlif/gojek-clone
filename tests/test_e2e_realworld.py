"""
test_e2e_realworld.py

End-to-end real-world test selama 60 DETIK dengan Kafka nyata.
Semua komponen berjalan paralel dalam satu proses:

  Producers  : 3 driver mobile (interval 1.5s) + 3 rider (interval 8s)
  Engines    : matching → tracking → ETA → notification (semua inline)
  Consumers  : status tracker + metrics collector

Kriteria LULUS:
  1. Minimal 2 ride berhasil di-match
  2. Minimal 1 ride mencapai status picked_up atau completed
  3. Nol critical exception di engine inti

Jalankan:
    python -m pytest tests/test_e2e_realworld.py -v -s
    python tests/test_e2e_realworld.py           (standalone)
"""

import json
import math
import time
import sys
import os
import threading
import uuid
from datetime import datetime
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from confluent_kafka import Producer, Consumer, KafkaError
from config import KAFKA_CONFIG, TOPICS, JAKARTA_BOUNDS

# ─── Konstanta simulasi ───────────────────────────────────────────────────────

TEST_DURATION_SEC   = 60
DRIVER_INTERVAL     = 1.5   # detik antar publish lokasi
RIDER_INTERVAL      = 8.0   # detik antar ride request
TRACKING_STEP_DELAY = 0.4   # detik antar langkah tracking
TRACKING_MOVE_STEP  = 0.008 # derajat per step pergerakan driver

RUN_ID = uuid.uuid4().hex[:8]  # unik per run agar group ID tidak bentrok

DRIVERS = [
    {"driver_id": f"E2E-DRV-001-{RUN_ID}", "name": "Budi",  "vehicle": "motorcycle"},
    {"driver_id": f"E2E-DRV-002-{RUN_ID}", "name": "Andi",  "vehicle": "motorcycle"},
    {"driver_id": f"E2E-DRV-003-{RUN_ID}", "name": "Dewi",  "vehicle": "car"},
]

RIDERS = [
    {"rider_id": f"E2E-RDR-001-{RUN_ID}", "name": "Rina"},
    {"rider_id": f"E2E-RDR-002-{RUN_ID}", "name": "Doni"},
    {"rider_id": f"E2E-RDR-003-{RUN_ID}", "name": "Mega"},
]

SPEED_KMH = {"motorcycle": 30, "car": 25}

# ─── Shared state ─────────────────────────────────────────────────────────────

lock = threading.Lock()

metrics = {
    "loc_published":    0,
    "requests_published": 0,
    "rides_matched":    [],   # list of match payloads
    "rides_accepted":   set(),
    "rides_picked_up":  set(),
    "rides_completed":  set(),
    "eta_published":    0,
    "errors":           [],
}

driver_positions = {}          # driver_id → {"lat": float, "lng": float}
available_drivers = {}         # driver_id → full driver data (status=available)
active_rides = {}              # driver_id → ride info dict
matched_driver_ids = set()     # driver_id yang sudah di-match (jangan double-book)

stop_event = threading.Event()

# ─── Helpers ─────────────────────────────────────────────────────────────────

def ts():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def log(tag, msg):
    print(f"  {tag} [{ts()}] {msg}", flush=True)

def rand_lat():
    import random
    return round(random.uniform(JAKARTA_BOUNDS['lat_min'], JAKARTA_BOUNDS['lat_max']), 6)

def rand_lng():
    import random
    return round(random.uniform(JAKARTA_BOUNDS['lng_min'], JAKARTA_BOUNDS['lng_max']), 6)

def haversine(lat1, lng1, lat2, lng2):
    R = 6371
    dl = math.radians(lat2 - lat1)
    dn = math.radians(lng2 - lng1)
    a  = math.sin(dl/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dn/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def move_towards(clat, clng, tlat, tlng, step=TRACKING_MOVE_STEP):
    dlat = tlat - clat
    dlng = tlng - clng
    dist = math.sqrt(dlat**2 + dlng**2)
    if dist < step:
        return tlat, tlng, True
    ratio = step / dist
    return clat + dlat * ratio, clng + dlng * ratio, False

def make_producer():
    return Producer(KAFKA_CONFIG)

def make_consumer(suffix, topics, offset="latest"):
    gid = f"e2e-{suffix}-{RUN_ID}"
    c = Consumer({**KAFKA_CONFIG, "group.id": gid, "auto.offset.reset": offset})
    c.subscribe(topics)
    return c

def record_error(context, exc):
    msg = f"{context}: {type(exc).__name__}: {exc}"
    with lock:
        metrics["errors"].append(msg)
    log("❌", msg)

# ─── Thread 1: Driver Location Producer ──────────────────────────────────────

def driver_location_producer():
    import random
    producer = make_producer()

    # inisialisasi posisi awal tiap driver
    for d in DRIVERS:
        with lock:
            driver_positions[d["driver_id"]] = {"lat": rand_lat(), "lng": rand_lng()}

    log("🚗", f"Driver Producer aktif — {len(DRIVERS)} driver, interval {DRIVER_INTERVAL}s")

    while not stop_event.is_set():
        for d in DRIVERS:
            with lock:
                pos = driver_positions[d["driver_id"]]
                # drift acak (driver yang tidak sedang tracking tetap bergerak)
                if d["driver_id"] not in active_rides:
                    pos["lat"] = max(JAKARTA_BOUNDS['lat_min'], min(JAKARTA_BOUNDS['lat_max'],
                                     pos["lat"] + random.uniform(-0.003, 0.003)))
                    pos["lng"] = max(JAKARTA_BOUNDS['lng_min'], min(JAKARTA_BOUNDS['lng_max'],
                                     pos["lng"] + random.uniform(-0.003, 0.003)))
                    driver_positions[d["driver_id"]] = pos

                is_matched = d["driver_id"] in matched_driver_ids
                status = "on_trip" if is_matched else "available"

            payload = {
                "driver_id": d["driver_id"],
                "name":      d["name"],
                "vehicle":   d["vehicle"],
                "lat":       round(pos["lat"], 6),
                "lng":       round(pos["lng"], 6),
                "status":    status,
                "timestamp": ts(),
                "speed_kmh": round(random.uniform(15, 50), 1),
            }
            producer.produce(topic=TOPICS['location_updates'],
                             key=d["driver_id"],
                             value=json.dumps(payload))

        producer.flush()
        with lock:
            metrics["loc_published"] += len(DRIVERS)

        stop_event.wait(DRIVER_INTERVAL)

    log("🚗", "Driver Producer berhenti")

# ─── Thread 2: Rider Request Producer ────────────────────────────────────────

def rider_request_producer():
    import random
    producer = make_producer()

    # tunggu driver sudah publish beberapa lokasi dulu
    stop_event.wait(3.0)

    log("🙋", f"Rider Producer aktif — {len(RIDERS)} rider, interval {RIDER_INTERVAL}s")

    while not stop_event.is_set():
        batch = random.sample(RIDERS, k=random.randint(1, min(2, len(RIDERS))))
        for r in batch:
            payload = {
                "rider_id":    r["rider_id"],
                "name":        r["name"],
                "pickup":      {"lat": rand_lat(), "lng": rand_lng()},
                "destination": {"lat": rand_lat(), "lng": rand_lng()},
                "service":     random.choice(["GoRide", "GoCar"]),
                "timestamp":   ts(),
            }
            producer.produce(topic=TOPICS['ride_requests'],
                             key=r["rider_id"],
                             value=json.dumps(payload))
            log("🙋", f"{r['name']} request {payload['service']} "
                      f"pickup ({payload['pickup']['lat']:.4f}, {payload['pickup']['lng']:.4f})")

        producer.flush()
        with lock:
            metrics["requests_published"] += len(batch)

        stop_event.wait(RIDER_INTERVAL)

    log("🙋", "Rider Producer berhenti")

# ─── Thread 3: Matching Engine ────────────────────────────────────────────────

def matching_engine():
    """
    Consume: location_updates (update registry driver available)
             ride_requests    (cari driver terdekat, publish match)
    Produce: ride_matched
    """
    loc_consumer = make_consumer("match-loc", [TOPICS['location_updates']])
    req_consumer = make_consumer("match-req", [TOPICS['ride_requests']])
    producer     = make_producer()

    log("🔍", "Matching Engine aktif")

    def _loc_loop():
        while not stop_event.is_set():
            msg = loc_consumer.poll(0.5)
            if not msg or msg.error():
                continue
            try:
                data = json.loads(msg.value().decode())
                did  = data["driver_id"]
                # hanya driver E2E run ini yang relevan
                if RUN_ID not in did:
                    continue
                with lock:
                    if data["status"] == "available" and did not in matched_driver_ids:
                        available_drivers[did] = data
                    elif did in available_drivers:
                        del available_drivers[did]
            except Exception as e:
                record_error("matching/loc", e)
        loc_consumer.close()

    threading.Thread(target=_loc_loop, daemon=True).start()

    while not stop_event.is_set():
        msg = req_consumer.poll(0.5)
        if not msg or msg.error():
            continue
        try:
            req    = json.loads(msg.value().decode())
            pickup = req["pickup"]

            with lock:
                avail = [d for d in available_drivers.values()
                         if d["driver_id"] not in matched_driver_ids]

            if not avail:
                log("⚠️ ", f"Tidak ada driver available untuk {req['name']}")
                continue

            nearest  = min(avail, key=lambda d: haversine(
                pickup["lat"], pickup["lng"], d["lat"], d["lng"]))
            dist     = haversine(pickup["lat"], pickup["lng"], nearest["lat"], nearest["lng"])
            eta      = max(round((dist / SPEED_KMH.get(nearest["vehicle"], 30)) * 60), 1)

            match = {
                "rider_id":    req["rider_id"],
                "rider_name":  req["name"],
                "driver_id":   nearest["driver_id"],
                "driver_name": nearest["name"],
                "vehicle":     nearest["vehicle"],
                "pickup":      pickup,
                "destination": req["destination"],
                "distance_km": round(dist, 2),
                "eta_minutes": eta,
                "service":     req["service"],
                "timestamp":   ts(),
            }

            with lock:
                matched_driver_ids.add(nearest["driver_id"])
                if nearest["driver_id"] in available_drivers:
                    del available_drivers[nearest["driver_id"]]
                metrics["rides_matched"].append(match)

            producer.produce(topic=TOPICS['ride_matched'],
                             key=req["rider_id"],
                             value=json.dumps(match))
            producer.flush()

            log("✅", f"MATCH! {req['name']} → {nearest['name']} | "
                      f"dist {dist:.2f}km | ETA {eta}min")

        except Exception as e:
            record_error("matching/req", e)

    req_consumer.close()
    log("🔍", "Matching Engine berhenti")

# ─── Thread 4: Tracking Engine ───────────────────────────────────────────────

def tracking_engine():
    """
    Consume: ride_matched  → daftarkan ride baru, publish status=accepted
    Loop tiap TRACKING_STEP_DELAY detik → gerakkan driver, publish ride_tracking + ride_status
    """
    match_consumer = make_consumer("track-match", [TOPICS['ride_matched']])
    producer       = make_producer()

    log("🛤️ ", "Tracking Engine aktif")

    def _listen_matches():
        while not stop_event.is_set():
            msg = match_consumer.poll(0.5)
            if not msg or msg.error():
                continue
            try:
                data = json.loads(msg.value().decode())
                did  = data["driver_id"]
                if RUN_ID not in did:
                    continue

                with lock:
                    pos = driver_positions.get(did, {"lat": rand_lat(), "lng": rand_lng()})
                    active_rides[did] = {
                        **data,
                        "phase":       "to_pickup",
                        "current_lat": pos["lat"],
                        "current_lng": pos["lng"],
                    }

                producer.produce(topic=TOPICS['ride_status'],
                                 key=did,
                                 value=json.dumps({
                                     "driver_id": did,
                                     "rider_id":  data["rider_id"],
                                     "status":    "accepted",
                                     "timestamp": ts(),
                                 }))
                producer.flush()
                with lock:
                    metrics["rides_accepted"].add(data["rider_id"])
                log("🆕", f"Tracking dimulai: {data['driver_name']} → {data['rider_name']}")
            except Exception as e:
                record_error("tracking/match", e)
        match_consumer.close()

    threading.Thread(target=_listen_matches, daemon=True).start()

    while not stop_event.is_set():
        stop_event.wait(TRACKING_STEP_DELAY)

        with lock:
            rides_snapshot = {k: dict(v) for k, v in active_rides.items()}

        for did, ride in rides_snapshot.items():
            try:
                clat = ride["current_lat"]
                clng = ride["current_lng"]

                if ride["phase"] == "to_pickup":
                    target     = ride["pickup"]
                    phase_lbl  = "Menuju pickup"
                elif ride["phase"] == "to_destination":
                    target     = ride["destination"]
                    phase_lbl  = "Menuju destinasi"
                else:
                    continue

                nlat, nlng, arrived = move_towards(clat, clng,
                                                   target["lat"], target["lng"])

                with lock:
                    if did in active_rides:
                        active_rides[did]["current_lat"] = nlat
                        active_rides[did]["current_lng"] = nlng
                    driver_positions[did] = {"lat": nlat, "lng": nlng}

                dist_to = haversine(nlat, nlng, target["lat"], target["lng"])

                tracking_payload = {
                    "driver_id":   did,
                    "rider_id":    ride["rider_id"],
                    "driver_name": ride["driver_name"],
                    "rider_name":  ride["rider_name"],
                    "phase":       ride["phase"],
                    "phase_label": phase_lbl,
                    "current_lat": round(nlat, 6),
                    "current_lng": round(nlng, 6),
                    "target_lat":  target["lat"],
                    "target_lng":  target["lng"],
                    "distance_km": round(dist_to, 2),
                    "vehicle":     ride["vehicle"],
                    "timestamp":   ts(),
                }
                producer.produce(topic=TOPICS['ride_tracking'],
                                 key=did,
                                 value=json.dumps(tracking_payload))

                if arrived:
                    if ride["phase"] == "to_pickup":
                        with lock:
                            if did in active_rides:
                                active_rides[did]["phase"] = "to_destination"
                            metrics["rides_picked_up"].add(ride["rider_id"])

                        producer.produce(topic=TOPICS['ride_status'],
                                         key=did,
                                         value=json.dumps({
                                             "driver_id": did,
                                             "rider_id":  ride["rider_id"],
                                             "status":    "picked_up",
                                             "timestamp": ts(),
                                         }))
                        log("🙋", f"{ride['driver_name']} menjemput {ride['rider_name']}")

                    elif ride["phase"] == "to_destination":
                        with lock:
                            active_rides.pop(did, None)
                            matched_driver_ids.discard(did)
                            metrics["rides_completed"].add(ride["rider_id"])

                        producer.produce(topic=TOPICS['ride_status'],
                                         key=did,
                                         value=json.dumps({
                                             "driver_id": did,
                                             "rider_id":  ride["rider_id"],
                                             "status":    "completed",
                                             "timestamp": ts(),
                                         }))
                        log("🏁", f"{ride['driver_name']} selesai antar {ride['rider_name']}")

                producer.flush()

            except Exception as e:
                record_error(f"tracking/step/{did}", e)

    log("🛤️ ", "Tracking Engine berhenti")

# ─── Thread 5: ETA Engine ────────────────────────────────────────────────────

def eta_engine():
    consumer = make_consumer("eta", [TOPICS['ride_tracking']])
    producer  = make_producer()

    log("⏱️ ", "ETA Engine aktif")

    while not stop_event.is_set():
        msg = consumer.poll(0.5)
        if not msg or msg.error():
            continue
        try:
            data    = json.loads(msg.value().decode())
            if RUN_ID not in data.get("driver_id", ""):
                continue
            dist    = data["distance_km"]
            vehicle = data.get("vehicle", "motorcycle")
            speed   = SPEED_KMH.get(vehicle, 30)
            eta     = max(round((dist / speed) * 60), 1)

            producer.produce(topic=TOPICS['eta_updates'],
                             key=data["driver_id"],
                             value=json.dumps({
                                 **data,
                                 "speed_kmh":   speed,
                                 "eta_minutes": eta,
                             }))
            producer.flush()

            with lock:
                metrics["eta_published"] += 1

        except Exception as e:
            record_error("eta", e)

    consumer.close()
    log("⏱️ ", "ETA Engine berhenti")

# ─── Thread 6: Status Consumer (metrics collector) ───────────────────────────

def status_consumer():
    consumer = make_consumer("status-collector", [TOPICS['ride_status']])

    log("📊", "Status Consumer aktif")

    while not stop_event.is_set():
        msg = consumer.poll(0.5)
        if not msg or msg.error():
            continue
        try:
            data = json.loads(msg.value().decode())
            if RUN_ID not in data.get("driver_id", ""):
                continue
            status = data.get("status")
            rid    = data.get("rider_id", "?")
            did    = data.get("driver_id", "?")
            log("📊", f"Status [{status.upper()}] rider={rid[-12:]} driver={did[-12:]}")
        except Exception as e:
            record_error("status_consumer", e)

    consumer.close()
    log("📊", "Status Consumer berhenti")

# ─── Progress reporter ────────────────────────────────────────────────────────

def progress_reporter(start_time):
    interval = 10  # laporan tiap 10 detik
    while not stop_event.is_set():
        stop_event.wait(interval)
        elapsed = time.time() - start_time
        remaining = max(0, TEST_DURATION_SEC - elapsed)
        with lock:
            m = metrics["rides_matched"]
            c = metrics["rides_completed"]
            p = metrics["rides_picked_up"]
            loc = metrics["loc_published"]
            req = metrics["requests_published"]
            eta = metrics["eta_published"]
            err = len(metrics["errors"])
        print(
            f"\n  ┌─ Progress [{elapsed:.0f}s / {TEST_DURATION_SEC}s | sisa {remaining:.0f}s] ─────────────\n"
            f"  │  Lokasi published : {loc:>4}    Requests : {req:>3}\n"
            f"  │  Matched          : {len(m):>4}    Picked up: {len(p):>3}    Completed: {len(c):>3}\n"
            f"  │  ETA updates      : {eta:>4}    Errors   : {err:>3}\n"
            f"  └──────────────────────────────────────────────────────\n",
            flush=True
        )

# ─── Main orchestrator ────────────────────────────────────────────────────────

def run_e2e_test():
    print("\n" + "═" * 62)
    print("  RIDE-HAILING KAFKA — END-TO-END REAL-WORLD TEST (60 DETIK)")
    print("═" * 62)
    print(f"  Run ID   : {RUN_ID}")
    print(f"  Durasi   : {TEST_DURATION_SEC} detik")
    print(f"  Driver   : {len(DRIVERS)}   Rider: {len(RIDERS)}")
    print(f"  Topics   : {', '.join(TOPICS.values())}")
    print("═" * 62 + "\n")

    threads = [
        threading.Thread(target=driver_location_producer, name="DriverProd",   daemon=True),
        threading.Thread(target=rider_request_producer,   name="RiderProd",    daemon=True),
        threading.Thread(target=matching_engine,          name="Matching",     daemon=True),
        threading.Thread(target=tracking_engine,          name="Tracking",     daemon=True),
        threading.Thread(target=eta_engine,               name="ETA",          daemon=True),
        threading.Thread(target=status_consumer,          name="StatusConsumer",daemon=True),
    ]

    start_time = time.time()
    reporter = threading.Thread(
        target=progress_reporter, args=(start_time,), daemon=True
    )

    for t in threads:
        t.start()
    reporter.start()

    # ── Jalankan selama TEST_DURATION_SEC detik ─────────────
    time.sleep(TEST_DURATION_SEC)
    stop_event.set()

    # Beri waktu thread selesai gracefully
    for t in threads:
        t.join(timeout=5)

    elapsed = time.time() - start_time

    # ─── Laporan akhir ────────────────────────────────────────
    with lock:
        matched   = len(metrics["rides_matched"])
        picked_up = len(metrics["rides_picked_up"])
        completed = len(metrics["rides_completed"])
        loc_pub   = metrics["loc_published"]
        req_pub   = metrics["requests_published"]
        eta_pub   = metrics["eta_published"]
        errors    = list(metrics["errors"])

    # Kriteria LULUS
    criteria = {
        "Minimal 2 ride matched":  matched   >= 2,
        "Minimal 1 ride picked_up atau completed": (picked_up + completed) >= 1,
        "Zero critical errors":    len(errors) == 0,
    }
    passed = all(criteria.values())

    print("\n" + "═" * 62)
    print("  LAPORAN AKHIR END-TO-END TEST")
    print("═" * 62)
    print(f"  Durasi nyata      : {elapsed:.1f} detik")
    print(f"  Lokasi published  : {loc_pub}")
    print(f"  Ride requests     : {req_pub}")
    print(f"  Rides matched     : {matched}")
    print(f"  Rides picked_up   : {picked_up}")
    print(f"  Rides completed   : {completed}")
    print(f"  ETA updates       : {eta_pub}")
    print(f"  Errors            : {len(errors)}")
    if errors:
        print("\n  Detail errors:")
        for e in errors[:5]:
            print(f"    • {e}")
    print("\n  Kriteria kelulusan:")
    for crit, ok in criteria.items():
        icon = "LULUS" if ok else "GAGAL"
        print(f"    [{icon}]  {crit}")
    print()
    status = "LULUS ✓" if passed else "GAGAL ✗"
    print(f"  HASIL KESELURUHAN: {status}")
    print("═" * 62 + "\n")

    return passed, {
        "duration_sec":   elapsed,
        "loc_published":  loc_pub,
        "req_published":  req_pub,
        "matched":        matched,
        "picked_up":      picked_up,
        "completed":      completed,
        "eta_published":  eta_pub,
        "errors":         errors,
    }

# ─── Pytest entry point ───────────────────────────────────────────────────────

def test_e2e_1_minute():
    """
    Pytest wrapper: 60-detik real-world end-to-end test.
    Requires Kafka running on localhost:9092.
    """
    passed, report = run_e2e_test()

    assert report["matched"]   >= 2, \
        f"Hanya {report['matched']} ride yang matched (minimal 2)"
    assert (report["picked_up"] + report["completed"]) >= 1, \
        f"Tidak ada ride yang mencapai picked_up atau completed"
    assert len(report["errors"]) == 0, \
        f"Ada {len(report['errors'])} error: {report['errors'][:3]}"


# ─── Standalone ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Force UTF-8 pada Windows
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")

    passed, _ = run_e2e_test()
    sys.exit(0 if passed else 1)
