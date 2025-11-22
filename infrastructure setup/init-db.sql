-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- 1. Create machine_sensors table
-- Stores raw sensor data from the simulated machines
CREATE TABLE IF NOT EXISTS machine_sensors (
    timestamp       TIMESTAMPTZ NOT NULL,
    machine_id      TEXT NOT NULL,
    machine_type    TEXT,
    sensor_type     TEXT NOT NULL,
    value           DOUBLE PRECISION,
    location        TEXT
);

-- Convert to hypertable partitioned by timestamp
SELECT create_hypertable('machine_sensors', 'timestamp', if_not_exists => TRUE);

-- Add indexes for common query patterns (by machine and sensor type)
CREATE INDEX IF NOT EXISTS idx_machine_sensors_machine_id ON machine_sensors (machine_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_machine_sensors_type ON machine_sensors (sensor_type, timestamp DESC);

-- 2. Create sensor_aggregates table
-- Stores 1-minute windowed aggregations computed by Flink
CREATE TABLE IF NOT EXISTS sensor_aggregates (
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    machine_id      TEXT NOT NULL,
    sensor_type     TEXT NOT NULL,
    avg_value       DOUBLE PRECISION,
    min_value       DOUBLE PRECISION,
    max_value       DOUBLE PRECISION,
    count_readings  INTEGER
);

-- Convert to hypertable (using window_start as the time partitioning column)
SELECT create_hypertable('sensor_aggregates', 'window_start', if_not_exists => TRUE);

-- Add indexes for analytics
CREATE INDEX IF NOT EXISTS idx_sensor_aggregates_machine ON sensor_aggregates (machine_id, window_start DESC);

-- 3. Add Data Retention Policy (90 days)
-- Automatically drops chunks older than 90 days for both tables
SELECT add_retention_policy('machine_sensors', INTERVAL '90 days', if_not_exists => TRUE);
SELECT add_retention_policy('sensor_aggregates', INTERVAL '90 days', if_not_exists => TRUE);
