# Task 1: Infrastructure Setup


mkdir iiot-lab
cd iiot-lab
mkdir flink-jobs spark-jobs prefect-flows
touch init-db.sql docker-compose.yml Dockerfile.flink

docker build -f Dockerfile.flink -t flink-py:1.18 .

docker compose up -d --build