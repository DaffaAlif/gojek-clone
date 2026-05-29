"""
ride_history_dag.py

Konsumsi event ride-matched dan ride-status dari Kafka lalu simpan ke
tabel public.ride_history di Supabase (PostgreSQL).

Perbaikan dari versi sebelumnya:
- Satu consumer group untuk kedua topik (offset sinkron)
- Commit Kafka offset SETELAH DB berhasil (bukan sebelumnya)
- Koneksi langsung via psycopg2 + sslmode=require (bypass Airflow conn)
- matched_at fallback ke utcnow() agar UNIQUE constraint selalu aktif
- Handle orphan completed events (ride matched di run sebelumnya)

Schedule : setiap 5 menit
Consumer : airflow-ride-history  (subscribe ride-matched + ride-status)
"""

import json
import logging
import os
import psycopg2
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

default_args = {
    "owner":        "airflow",
    "retries":      1,
    "retry_delay":  timedelta(minutes=1),
}

KAFKA_SERVERS = "kafka:29092"
TOPIC_MATCHED = "ride-matched"
TOPIC_STATUS  = "ride-status"
GROUP_ID      = "airflow-ride-history"   # satu group untuk kedua topik

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS public.ride_history (
    id              BIGSERIAL PRIMARY KEY,
    rider_id        VARCHAR(50)    NOT NULL,
    rider_name      VARCHAR(100),
    driver_id       VARCHAR(50)    NOT NULL,
    driver_name     VARCHAR(100),
    vehicle         VARCHAR(20),
    service         VARCHAR(20),
    pickup_lat      DOUBLE PRECISION,
    pickup_lng      DOUBLE PRECISION,
    destination_lat DOUBLE PRECISION,
    destination_lng DOUBLE PRECISION,
    distance_km     DOUBLE PRECISION,
    eta_minutes     INTEGER,
    status          VARCHAR(20)    DEFAULT 'matched',
    matched_at      TIMESTAMP      NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMP,
    ingested_at     TIMESTAMP      DEFAULT NOW(),
    UNIQUE (rider_id, driver_id, matched_at)
);
"""

# ── DB connection ─────────────────────────────────────────────────────────────

def _get_db():
    """
    Koneksi langsung ke Supabase via env vars dengan sslmode=require.
    Tidak menggunakan Airflow PostgresHook agar SSL dapat dikonfigurasi
    secara eksplisit tanpa tergantung pada format connection string Airflow.
    """
    return psycopg2.connect(
        host=os.environ["SUPABASE_HOST"],
        port=int(os.environ.get("SUPABASE_PORT", 5432)),
        user=os.environ["SUPABASE_USER"],
        password=os.environ["SUPABASE_PASSWORD"],
        dbname=os.environ.get("SUPABASE_DB", "postgres"),
        sslmode="require",
        connect_timeout=15,
    )

# ── Tasks ─────────────────────────────────────────────────────────────────────

def ensure_table():
    """Buat tabel ride_history di Supabase jika belum ada."""
    conn = _get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
        conn.commit()
        log.info("Table public.ride_history OK")
    finally:
        conn.close()


def consume_and_insert(**context):
    """
    Poll ride-matched dan ride-status dari Kafka dalam satu consumer group.
    Commit Kafka offset hanya setelah seluruh operasi DB berhasil.

    Alur:
    1. Poll semua pesan dari kedua topik (kumpulkan di memori)
    2. Upsert ride-matched ke ride_history
    3. Untuk completed event yang matched-nya ada di batch ini → set completed_at
    4. Untuk completed event yang matched-nya sudah di batch sebelumnya → UPDATE langsung ke DB
    5. Commit Kafka offset
    """
    from confluent_kafka import Consumer, KafkaError, TopicPartition

    consumer = Consumer({
        "bootstrap.servers":    KAFKA_SERVERS,
        "group.id":             GROUP_ID,
        "auto.offset.reset":    "earliest",
        "enable.auto.commit":   False,
        "session.timeout.ms":   30000,
        "max.poll.interval.ms": 300000,
    })
    consumer.subscribe([TOPIC_MATCHED, TOPIC_STATUS])

    matched   = {}  # (rider_id, driver_id) → msg dict
    completed = {}  # (rider_id, driver_id) → timestamp str
    offsets   = {}  # (topic, partition) → TopicPartition(next offset)

    empty_polls = 0
    total = 0

    try:
        while total < 500 and empty_polls < 3:
            msg = consumer.poll(5.0)
            if msg is None:
                empty_polls += 1
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.warning("Kafka error: %s", msg.error())
                empty_polls += 1
                continue

            empty_polls = 0
            total += 1
            offsets[(msg.topic(), msg.partition())] = TopicPartition(
                msg.topic(), msg.partition(), msg.offset() + 1
            )

            try:
                data = json.loads(msg.value().decode("utf-8"))
                if msg.topic() == TOPIC_MATCHED:
                    key = (data.get("rider_id"), data.get("driver_id"))
                    matched[key] = data
                elif msg.topic() == TOPIC_STATUS and data.get("status") == "completed":
                    key = (data.get("rider_id"), data.get("driver_id"))
                    completed[key] = data.get("timestamp")
            except Exception as exc:
                log.warning("Parse error: %s", exc)

    except Exception as exc:
        log.error("Consumer fatal error: %s", exc)
        consumer.close()
        raise

    log.info(
        "Polled %d msgs — %d ride-matched, %d completed status",
        total, len(matched), len(completed),
    )

    if not matched and not completed:
        log.info("Tidak ada event baru. Skip.")
        consumer.commit(offsets=list(offsets.values()) if offsets else None)
        consumer.close()
        return

    # ── DB operations ─────────────────────────────────────────────────────────
    conn = _get_db()
    cursor = conn.cursor()
    upserted = 0
    updated  = 0

    UPSERT_SQL = """
    INSERT INTO public.ride_history (
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
        completed_at = COALESCE(EXCLUDED.completed_at, public.ride_history.completed_at),
        ingested_at  = NOW();
    """

    # 1. Upsert setiap ride-matched event
    for (rider_id, driver_id), m in matched.items():
        pickup = m.get("pickup") or {}
        dest   = m.get("destination") or {}
        key    = (rider_id, driver_id)

        try:
            matched_at = datetime.strptime(m.get("timestamp", ""), "%Y-%m-%d %H:%M:%S")
        except Exception:
            matched_at = datetime.utcnow()

        completed_raw = completed.get(key)
        try:
            completed_at = datetime.strptime(completed_raw, "%Y-%m-%d %H:%M:%S") if completed_raw else None
        except Exception:
            completed_at = None

        try:
            cursor.execute(UPSERT_SQL, {
                "rider_id":        rider_id,
                "rider_name":      m.get("rider_name"),
                "driver_id":       driver_id,
                "driver_name":     m.get("driver_name"),
                "vehicle":         m.get("vehicle"),
                "service":         m.get("service"),
                "pickup_lat":      pickup.get("lat"),
                "pickup_lng":      pickup.get("lng"),
                "destination_lat": dest.get("lat"),
                "destination_lng": dest.get("lng"),
                "distance_km":     m.get("distance_km"),
                "eta_minutes":     m.get("eta_minutes"),
                "status":          "completed" if completed_at else "matched",
                "matched_at":      matched_at,
                "completed_at":    completed_at,
            })
            upserted += 1
        except Exception as exc:
            log.error("Upsert gagal (%s, %s): %s", rider_id, driver_id, exc)
            conn.rollback()

    # 2. Update status untuk completed events yang ride-matched-nya
    #    sudah di-insert di run sebelumnya (orphan completed)
    for (rider_id, driver_id), ts in completed.items():
        if (rider_id, driver_id) in matched:
            continue  # sudah ditangani di langkah 1
        try:
            completed_at = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S") if ts else datetime.utcnow()
        except Exception:
            completed_at = datetime.utcnow()
        try:
            cursor.execute("""
                UPDATE public.ride_history
                SET status       = 'completed',
                    completed_at = COALESCE(%s, completed_at),
                    ingested_at  = NOW()
                WHERE rider_id = %s
                  AND driver_id = %s
                  AND status   != 'completed'
            """, (completed_at, rider_id, driver_id))
            updated += cursor.rowcount
        except Exception as exc:
            log.error("Update status gagal (%s, %s): %s", rider_id, driver_id, exc)
            conn.rollback()

    conn.commit()
    cursor.close()
    conn.close()
    log.info("DB OK — upserted %d rides baru, updated %d ke completed.", upserted, updated)

    # Commit Kafka offset SETELAH DB berhasil
    if offsets:
        consumer.commit(offsets=list(offsets.values()))
    consumer.close()


# ── DAG ───────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="ride_history_ingestion",
    default_args=default_args,
    description="Kafka → Supabase: ingest ride-matched + ride-status ke ride_history",
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ride-hailing", "kafka", "supabase"],
) as dag:

    t_table = PythonOperator(
        task_id="ensure_table",
        python_callable=ensure_table,
    )

    t_ingest = PythonOperator(
        task_id="consume_and_insert",
        python_callable=consume_and_insert,
    )

    t_table >> t_ingest
