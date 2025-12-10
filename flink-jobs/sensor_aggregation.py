import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def main():
    # 1. Setup Environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # 2. Source: Redpanda
    # We read 'timestamp' as a STRING first to avoid parsing errors
    # Then we create 'event_time' by replacing 'T' with a space and casting
    t_env.execute_sql("""
        CREATE TABLE source_sensors (
            machine_id STRING,
            sensor_type STRING,
            `value` DOUBLE,
            `timestamp` STRING,
            `event_time` AS TO_TIMESTAMP(REPLACE(`timestamp`, 'T', ' ')),
            WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'machine-sensors',
            'properties.bootstrap.servers' = 'redpanda:9092',
            'properties.group.id' = 'flink-consumer-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    # 3. Sink: Aggregates
    t_env.execute_sql("""
        CREATE TABLE sink_sensor_aggregates (
            machine_id STRING,
            sensor_type STRING,
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            avg_value DOUBLE,
            min_value DOUBLE,
            max_value DOUBLE,
            count_readings BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://timescaledb:5432/iiot',
            'table-name' = 'sensor_aggregates',
            'username' = 'admin',
            'password' = 'admin'
        )
    """)

    # 4. Sink: Raw Data
    t_env.execute_sql("""
        CREATE TABLE sink_raw_sensors (
            machine_id STRING,
            sensor_type STRING,
            `value` DOUBLE,
            `timestamp` TIMESTAMP(3)
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://timescaledb:5432/iiot',
            'table-name' = 'machine_sensors',
            'username' = 'admin',
            'password' = 'admin'
        )
    """)

    # 5. Execute Logic
    statement_set = t_env.create_statement_set()
    
    # Query A: Pass-through raw data (Using calculated event_time)
    statement_set.add_insert_sql("""
        INSERT INTO sink_raw_sensors
        SELECT machine_id, sensor_type, `value`, `event_time`
        FROM source_sensors
    """)

    # Query B: Window Aggregation (Using calculated event_time)
    statement_set.add_insert_sql("""
        INSERT INTO sink_sensor_aggregates
        SELECT 
            machine_id, 
            sensor_type,
            window_start, 
            window_end,
            AVG(`value`) as avg_value,
            MIN(`value`) as min_value,
            MAX(`value`) as max_value,
            COUNT(*) as count_readings
        FROM TABLE(
            TUMBLE(TABLE source_sensors, DESCRIPTOR(`event_time`), INTERVAL '1' MINUTE)
        )
        GROUP BY window_start, window_end, machine_id, sensor_type
    """)

    print("Submitting Flink Jobs with Robust Timestamp Parsing...")
    statement_set.execute()

if __name__ == '__main__':
    main()