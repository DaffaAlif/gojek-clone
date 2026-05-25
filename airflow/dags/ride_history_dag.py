"""
ride_history_dag.py

Konsumsi event ride-matched dan ride-status dari Kafka,
lalu simpan ke tabel ride_history di PostgreSQL (database: ridedata).

Schedule : setiap 5 menit
Consumer group : airflow-ride-history-matched  (offset di-track oleh Kafka)
"""

import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

log = logging.getLogger(__name__)

# ── Default args ──────────────────────────────────────────────────────────────

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# ── Helpers ───────────────────────────────────────────────────────────────────

KAFKA_SERVERS = "kafka:29092"
TOPIC_MATCHED = "ride-matched"
TOPIC_STATUS  = "ride-status"
GROUP_ID      = "airflow-ride-history-matched"
POSTGRES_CONN = "ride_data_supabase"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ride_history (
    id              SERIAL PRIMARY KEY,
    rider_id        VARCHAR(50)        NOT NULL,
    rider_name      VARCHAR(100),
    driver_id       VARCHAR(50)        NOT NULL,
    driver_name     VARCHAR(100),
    vehicle         VARCHAR(20),
    service         VARCHAR(20),
    pickup_lat      DOUBLE PRECISION,
    pickup_lng      DOUBLE PRECISION,
    destination_lat DOUBLE PRECISION,
    destination_lng DOUBLE PRECISION,
    distance_km     DOUBLE PRECISION,
    eta_minutes     INTEGER,
    status          VARCHAR(20)        DEFAULT 'matched',
    matched_at      TIMESTAMP,
    completed_at    TIMESTAMP,
    ingested_at     TIMESTAMP          DEFAULT NOW(),
    UNIQUE (rider_id, driver_id, matched_at)
);
"""


def _poll_kafka(bootstrap_servers, topic, group_id, max_messages=200, poll_timeout=5.0):
    """Return list of parsed JSON messages from `topic`, up to `max_messages`."""
    from confluent_kafka import Consumer, KafkaError

    conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "session.timeout.ms": 10000,
    }
    consumer = Consumer(conf)
    consumer.subscribe([topic])

    messages = []
    empty_polls = 0

    try:
        while len(messages) < max_messages and empty_polls < 3:
            msg = consumer.poll(poll_timeout)
            if msg is None:
                empty_polls += 1
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.warning("Kafka error: %s", msg.error())
                empty_polls += 1
                continue
            empty_polls = 0
            try:
                messages.append(json.loads(msg.value().decode("utf-8")))
            except Exception as exc:
                log.warning("Failed to parse message: %s", exc)

        consumer.commit()
    finally:
        consumer.close()

    log.info("Polled %d messages from topic %s", len(messages), topic)
    return messages


# ── Task functions ─────────────────────────────────────────────────────────────

def consume_and_insert(**context):
    """
    1. Konsumsi ride-matched  -> kumpulkan detail perjalanan (matched_at)
    2. Konsumsi ride-status   -> cari timestamp completed
    3. Upsert ke ride_history
    """
    # --- Ambil event matched ---
    matched_msgs = _poll_kafka(KAFKA_SERVERS, TOPIC_MATCHED, GROUP_ID)

    # Indeks: (rider_id, driver_id) -> matched record
    matched_index = {}
    for m in matched_msgs:
        key = (m.get("rider_id"), m.get("driver_id"))
        # Simpan record terbaru
        matched_index[key] = m

    if not matched_index:
        log.info("Tidak ada event ride-matched baru. Skip.")
        return

    # --- Ambil event status ---
    status_msgs = _poll_kafka(KAFKA_SERVERS, TOPIC_STATUS, GROUP_ID + "-status")

    # Indeks: (rider_id, driver_id) -> completed_at (str)
    completed_index = {}
    for s in status_msgs:
        if s.get("status") == "completed":
            key = (s.get("rider_id"), s.get("driver_id"))
            completed_index[key] = s.get("timestamp")

    # --- Upsert ke PostgreSQL ---
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN)
    conn = hook.get_conn()
    cursor = conn.cursor()

    upsert_sql = """
    INSERT INTO ride_history (
        rider_id, rider_name,
        driver_id, driver_name,
        vehicle, service,
        pickup_lat, pickup_lng,
        destination_lat, destination_lng,
        distance_km, eta_minutes,
        status, matched_at, completed_at
    ) VALUES (
        %(rider_id)s, %(rider_name)s,
        %(driver_id)s, %(driver_name)s,
        %(vehicle)s, %(service)s,
        %(pickup_lat)s, %(pickup_lng)s,
        %(destination_lat)s, %(destination_lng)s,
        %(distance_km)s, %(eta_minutes)s,
        %(status)s, %(matched_at)s, %(completed_at)s
    )
    ON CONFLICT (rider_id, driver_id, matched_at) DO UPDATE SET
        status       = EXCLUDED.status,
        completed_at = COALESCE(EXCLUDED.completed_at, ride_history.completed_at),
        ingested_at  = NOW();
    """

    inserted = 0
    for (rider_id, driver_id), m in matched_index.items():
        key = (rider_id, driver_id)

        pickup      = m.get("pickup", {}) or {}
        destination = m.get("destination", {}) or {}

        raw_ts = m.get("timestamp", "")
        try:
            matched_at = datetime.strptime(raw_ts, "%Y-%m-%d %H:%M:%S")
        except Exception:
            matched_at = None

        completed_raw = completed_index.get(key)
        try:
            completed_at = datetime.strptime(completed_raw, "%Y-%m-%d %H:%M:%S") if completed_raw else None
        except Exception:
            completed_at = None

        status = "completed" if completed_at else "matched"

        params = {
            "rider_id":        rider_id,
            "rider_name":      m.get("rider_name"),
            "driver_id":       driver_id,
            "driver_name":     m.get("driver_name"),
            "vehicle":         m.get("vehicle"),
            "service":         m.get("service"),
            "pickup_lat":      pickup.get("lat"),
            "pickup_lng":      pickup.get("lng"),
            "destination_lat": destination.get("lat"),
            "destination_lng": destination.get("lng"),
            "distance_km":     m.get("distance_km"),
            "eta_minutes":     m.get("eta_minutes"),
            "status":          status,
            "matched_at":      matched_at,
            "completed_at":    completed_at,
        }

        try:
            cursor.execute(upsert_sql, params)
            inserted += 1
        except Exception as exc:
            log.error("Upsert gagal untuk (%s, %s): %s", rider_id, driver_id, exc)
            conn.rollback()
            continue

    conn.commit()
    cursor.close()
    conn.close()
    log.info("Upserted %d ride_history records.", inserted)


# ── DAG definition ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="ride_history_ingestion",
    default_args=default_args,
    description="Ingest ride events from Kafka into PostgreSQL ride_history",
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ride-hailing", "kafka", "postgres"],
) as dag:

    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id=POSTGRES_CONN,
        sql=CREATE_TABLE_SQL,
    )

    consume_kafka = PythonOperator(
        task_id="consume_kafka_and_insert",
        python_callable=consume_and_insert,
    )

    create_table >> consume_kafka
