# Task 1 — Infrastructure setup


## Project Layout

Create the working directory and necessary subfolders:

```bash
mkdir iiot-lab
cd iiot-lab
mkdir flink-jobs spark-jobs prefect-flows
touch init-db.sql docker-compose.yml Dockerfile.flink
```

## Build and Run

Build the custom Flink Python image and start the services using Docker Compose:

```bash
docker build -f Dockerfile.flink -t flink-py:1.18 .
docker compose up -d --build
```

## Verify TimescaleDB

Open a `psql` shell inside the `timescaledb` service to inspect the created tables and hypertables:

```bash
docker compose exec timescaledb psql -U admin -d iiot
```

Expected output:

```text
psql (14.17)
iiot=# \dt
             List of relations
 Schema |       Name        | Type  | Owner 
--------+-------------------+-------+-------
 public | machine_sensors   | table | admin
 public | sensor_aggregates | table | admin
(2 rows)

iiot=# SELECT * FROM timescaledb_information.hypertables;
 hypertable_schema |  hypertable_name  | owner | num_dimensions | num_chunks | compression_enabled | tablespaces 
-------------------+-------------------+-------+----------------+------------+---------------------+-------------
 public            | machine_sensors   | admin |              1 |          0 | f                   | 
 public            | sensor_aggregates | admin |              1 |          0 | f                   | 
(2 rows)

iiot=#
```

## Accessing Services

The Debian container hosting Docker is reachable at `192.168.50.22`. The Web UIs for the various services are available at the following addresses:

| Service | URL | Credentials / Notes |
| :--- | :--- | :--- |
| **Portainer** | https://192.168.50.22:9443/ | User: `admin`, Pwd: `dataengineeringlabo` |
| **Redpanda Console** | http://192.168.50.22:8080 | |
| **Spark Master** | http://192.168.50.22:9090 | |
| **Apache Flink** | http://192.168.50.22:8081 | |

> **Note:** These URLs assume the host is reachable from your network (e.g., via Tailscale) and that the ports are correctly exposed in Docker.

# Task 2 — Data Ingestion Service

Create an `ingestion/` directory containing the following files:

*   `Dockerfile`: Container definition
*   `requirements.txt`: Python dependencies
*   `ingest_data.py`: Main ingestion script


## Implementation Details

The data ingestion service generates synthetic sensor data for industrial machines (CNC, Lathe, Welder).

  * **Historical Data:** On startup, it backfills 7 days of data to simulate a history.
  * **Real-time Data:** It enters a loop generating readings every 5 seconds.
  * **Data Quality:** Uses Gaussian distribution (`numpy.random.normal`) to create realistic variations in temperature, pressure, and vibration.

**Key Code Snippet (Producer Logic):**

```python
# From ingest_data.py
def produce_message(producer, message):
    message_json = json.dumps(message).encode('utf-8')
    machine_id = message["machine_id"]
    # Partition by machine_id to ensure ordering
    producer.produce(TOPIC_NAME, key=machine_id.encode('utf-8'), value=message_json)
```

## Problems and Fixes

1.  **Redpanda Connection Failure on Startup:**

      * *Problem:* The Python script attempted to connect immediately, but Redpanda wasn't ready, causing the container to crash.
      * *Fix:* Implemented a `while True` retry loop with a `try-except` block in the connection logic to wait until the broker is reachable.

2.  **Dependency Conflicts:**

      * *Problem:* Conflicting usage of `kafka-python` and `confluent-kafka`.
      * *Fix:* Standardized on `confluent-kafka` in `requirements.txt`.

-----

# Task 3 — Stream Processing with Apache Flink

## Deliverables

  * Custom Flink Docker image with necessary connectors (Kafka, JDBC, Postgres).
  * `sensor_aggregation.py` job performing tumbling window aggregations.
  * Real-time data flowing from Redpanda → Flink → TimescaleDB.

## Implementation Details

To enable Flink to talk to external systems, we built a custom Docker image rather than using the default one.

**Dockerfile.flink (Custom Build):**

```dockerfile
FROM flink:1.18.1-java11
# Install Python & Pip
RUN apt-get update -y && apt-get install -y python3 python3-pip python3-dev && ln -s /usr/bin/python3 /usr/bin/python
# Install Connectors (Verified Versions)
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar
RUN wget -P /opt/flink/lib/ https://jdbc.postgresql.org/download/postgresql-42.6.0.jar
```

**Aggregation Logic (SQL):**
The job uses Flink SQL to define a 1-minute tumbling window:

```sql
INSERT INTO sink_sensor_aggregates
SELECT 
    machine_id, sensor_type, window_start, window_end,
    AVG(`value`), MIN(`value`), MAX(`value`), COUNT(*)
FROM TABLE(
    TUMBLE(TABLE source_sensors, DESCRIPTOR(`event_time`), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end, machine_id, sensor_type
```

## Problems and Fixes

1.  **Missing Dependencies (ClassNotDef):**

      * *Problem:* Flink crashed because it couldn't find Kafka/JDBC classes.
      * *Fix:* Created the `flink-build` directory and custom Dockerfile to download specific JARs compatible with Flink 1.18.

2.  **Timestamp Parsing Error (NULL Violation):**

      * *Problem:* The database write failed with `ERROR: NULL value in column "timestamp"`. Flink's SQL parser could not automatically handle the ISO 8601 format containing the `T` separator (e.g., `2023-10-10T10:00:00`).
      * *Fix:* Modified the source table DDL to read the timestamp as a string first, then clean it manually using a computed column:
        ```sql
        `event_time` AS TO_TIMESTAMP(REPLACE(`timestamp`, 'T', ' '))
        ```

3.  **Schema Mismatch:**

      * *Problem:* The Flink sink definition used short names (`avg_val`), but the database expected full names (`avg_value`).
      * *Fix:* Aligned the Flink SQL `CREATE TABLE` statement to match the `init-db.sql` schema exactly.

-----

# Task 4 — Batch Processing with Apache Spark

## Deliverables

  * Spark ETL job (`timescale_to_deltalake.py`) reading from TimescaleDB.
  * Data successfully written to Azurite (Azure Blob Storage Emulator) in Delta Lake format.
  * Partitioned storage by Year/Month/Day.

## Implementation Details

The Spark job was executed using `spark-submit` inside the `spark-master` container. It required the `hadoop-azure` and `delta-core` packages to communicate with the storage emulator.

**Execution Command:**

```bash
docker compose exec -u 0 spark-master /opt/spark/bin/spark-submit \
  --packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-azure:3.3.4,org.postgresql:postgresql:42.6.0 \
  /opt/spark/jobs/timescale_to_deltalake.py
```

## Problems and Fixes

1.  **Permission Denied (.ivy2 cache):**

      * *Problem:* The default `spark` user could not write dependency cache files.
      * *Fix:* Ran the docker command with `-u 0` (root) to allow dependency downloads.

2.  **Job Hanging Indefinitely (Entropy Issue):**

      * *Problem:* The Spark job would start but hang forever during the connection phase. This is a common Docker issue where Java runs out of entropy for random number generation.
      * *Fix:* Added JVM flags to the Spark session config:
        ```python
        .config("spark.driver.extraJavaOptions", "-Djava.security.egd=file:/dev/./urandom")
        .config("spark.executor.extraJavaOptions", "-Djava.security.egd=file:/dev/./urandom")
        ```

3.  **Connection Refused (Port 80 vs 10000):**

      * *Problem:* The Hadoop-Azure WASB driver defaults to connecting on Port 80 (HTTP). However, the Azurite container was listening on Port 10000.
      * *Fix:* Reconfigured `docker-compose.yml` to make Azurite listen internally on Port 80 while still exposing Port 10000 to the host.
        ```yaml
        command: "azurite-blob --blobHost 0.0.0.0 --blobPort 80"
        ports:
          - "10000:80"
        ```

4.  **Authorization Failure (HTTPS vs HTTP):**

      * *Problem:* Spark tried to connect via HTTPS, but the local emulator uses HTTP.
      * *Fix:* Forced HTTP mode in the Spark config:
        ```python
        .config("fs.azure.always.use.https", "false")
        .config("fs.azure.secure.mode", "false")
        ```