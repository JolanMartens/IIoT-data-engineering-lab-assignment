import os
import time
import json
import logging
from datetime import datetime, timedelta
import random
import numpy as np
from confluent_kafka import Producer, KafkaException

# --- Configuration ---
REDPANDA_BROKER = os.environ.get('REDPANDA_BROKER', 'redpanda:9092')
TOPIC_NAME = 'machine-sensors'
SEND_INTERVAL_SECONDS = int(os.environ.get('SEND_INTERVAL_SECONDS', 5))
PAST_DATA_DAYS = 7

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Constants ---
MACHINES = [
    {"id": "CNC-001", "type": "CNC Mill", "location": "Assembly Line A"},
    {"id": "LATHE-002", "type": "Lathe", "location": "Tooling Bay 2"},
    {"id": "PRESS-003", "type": "Hydraulic Press", "location": "Stamping Area"},
    {"id": "WELD-004", "type": "Robotic Welder", "location": "Line B"}
]

SENSOR_TEMPLATES = {
    "temperature": (85.0, 5.0),
    "vibration": (1.5, 0.5),
    "pressure": (150.0, 10.0),
    "power_draw": (1200.0, 50.0)
}

# --- Helper Functions ---

def create_redpanda_producer():
    conf = {
        'bootstrap.servers': REDPANDA_BROKER,
        'acks': 'all',
        'delivery.report.only.error': False
    }
    
    # Retry loop instead of recursion
    while True:
        try:
            producer = Producer(conf)
            # Fast check: try to list topics to verify connection
            producer.list_topics(timeout=5)
            logging.info(f"Connected to Redpanda at {REDPANDA_BROKER}")
            return producer
        except KafkaException as e:
            logging.error(f"Failed to connect to Redpanda: {e}. Retrying in 5s...")
            time.sleep(5)
        except Exception as e:
            logging.error(f"Unexpected error: {e}. Retrying in 5s...")
            time.sleep(5)

def delivery_report(err, msg):
    if err is not None:
        logging.error(f"Message delivery failed: {err}")
    else:
        logging.debug(f"Message delivered to topic '{msg.topic()}' [{msg.partition()}]")

def generate_sensor_reading(machine_id, sensor_type, mean, std_dev, timestamp):
    value = np.random.normal(mean, std_dev)
    return {
        "timestamp": timestamp.isoformat(),
        "machine_id": machine_id,
        "sensor_type": sensor_type,
        "value": round(value, 2),
    }

def produce_message(producer, message):
    message_json = json.dumps(message).encode('utf-8')
    machine_id = message["machine_id"]
    
    try:
        producer.produce(
            TOPIC_NAME,
            key=machine_id.encode('utf-8'),
            value=message_json,
            callback=delivery_report
        )
        producer.poll(0)
    except BufferError:
        logging.warning("Local buffer full, waiting...")
        producer.poll(1) # Wait for buffer space
    except Exception as e:
        logging.error(f"Failed to produce message for {machine_id}: {e}")

def generate_and_produce_past_data(producer):
    logging.info(f"Generating {PAST_DATA_DAYS} days of historical data...")
    end_time = datetime.now()
    start_time = end_time - timedelta(days=PAST_DATA_DAYS)
    
    # Interval for historical data (sparse: every 1 hour to save generation time)
    # Adjust this if you want denser historical data
    interval_seconds = 3600 
    
    current_timestamp = start_time
    message_count = 0

    while current_timestamp < end_time:
        for machine in MACHINES:
            for sensor_type, (mean, std_dev) in SENSOR_TEMPLATES.items():
                message = generate_sensor_reading(
                    machine["id"], sensor_type, mean, std_dev, current_timestamp
                )
                produce_message(producer, message)
                message_count += 1
        
        current_timestamp += timedelta(seconds=interval_seconds)
        
        # Periodically flush to prevent buffer overflow during bulk load
        if message_count % 1000 == 0:
            producer.flush()

    producer.flush()
    logging.info(f"Successfully produced {message_count} historical messages.")

def main():
    producer = create_redpanda_producer()

    # 1. Historical Data
    generate_and_produce_past_data(producer)

    # 2. Real-time Stream
    logging.info(f"Starting real-time stream every {SEND_INTERVAL_SECONDS}s...")
    
    try:
        while True:
            start_time = time.time()
            now = datetime.now()

            for machine in MACHINES:
                for sensor_type, (mean, std_dev) in SENSOR_TEMPLATES.items():
                    message = generate_sensor_reading(
                        machine["id"], sensor_type, mean, std_dev, now
                    )
                    produce_message(producer, message)

            producer.flush(timeout=1.0)

            elapsed = time.time() - start_time
            sleep_time = SEND_INTERVAL_SECONDS - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)

    except KeyboardInterrupt:
        logging.info("Stopping producer...")
    finally:
        producer.flush()

if __name__ == '__main__':
    main()