-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Raw sensor readings
CREATE TABLE IF NOT EXISTS machine_sensors (
    ts          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    machine_id  TEXT NOT NULL,
    machine_type TEXT,
    location    TEXT,
    sensor_type TEXT NOT NULL,
    value       DOUBLE PRECISION,
    unit        TEXT
);

SELECT create_hypertable('machine_sensors', 'ts');
ALTER TABLE machine_sensors SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'machine_id,sensor_type'
);

-- Aggregates from Flink
CREATE TABLE IF NOT EXISTS sensor_aggregates (
    window_start TIMESTAMPTZ NOT NULL,
    window_end   TIMESTAMPTZ NOT NULL,
    machine_id   TEXT NOT NULL,
    sensor_type  TEXT NOT NULL,
    avg_val      DOUBLE PRECISION,
    min_val      DOUBLE PRECISION,
    max_val      DOUBLE PRECISION,
    cnt          BIGINT
);

SELECT create_hypertable('sensor_aggregates', 'window_start');

-- Indexes
CREATE INDEX idx_machine_sensors_machine ON machine_sensors (machine_id, ts DESC);
CREATE INDEX idx_agg_machine ON sensor_aggregates (machine_id, window_start DESC);

-- 90-day retention on both tables
SELECT add_retention_policy('machine_sensors', INTERVAL '90 days');
SELECT add_retention_policy('sensor_aggregates', INTERVAL '90 days');
