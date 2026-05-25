-- Buat database ride history (terpisah dari database airflow metadata)
CREATE DATABASE ridedata;

-- Buat tabel di database ridedata
\connect ridedata

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
