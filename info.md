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

