# Task 1 — Infrastructure setup


## Project layout

Create the working directory and subfolders:

```bash
mkdir iiot-lab
cd iiot-lab
mkdir flink-jobs spark-jobs prefect-flows
touch init-db.sql docker-compose.yml Dockerfile.flink
```

## Build and run

Build the Flink Python image and start services with Docker Compose:

```bash
docker build -f Dockerfile.flink -t flink-py:1.18 .
docker compose up -d --build
```

## Verify TimescaleDB

Open a psql shell inside the `timescaledb` service and inspect tables and hypertables:

```bash
docker compose exec timescaledb psql -U admin -d iiot
```

Output:

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


## Accessing services

The Debian container with Docker is reachable at `192.168.50.22`. Web UIs are available at the following addresses:

- Portainer: https://192.168.50.22:9443/
    - admin, pwd: dataengineeringlabo
- Redpanda console: http://192.168.50.22:8080
- Spark Master:    http://192.168.50.22:9090
- Apache Flink:    http://192.168.50.22:8081

(Note: URLs assume the host is reachable from your network, via Tailscale, and ports are exposed in Docker.)
