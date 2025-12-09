import os
import time
import json
import logging
from datetime import datetime, timedelta
import random
import numpy as np

from confluent_kafka import Producer, KafkaException

# --- Configuration ---

# Environment variables for configuration, defaulting to service names in docker-compose
REDPANDA_BROKER = os.environ.get('REDPANDA_BROKER', 'redpanda:9092')
TOPIC_NAME = 'machine-sensors'
# Default to 5 seconds if not set
SEND_INTERVAL_SECONDS = int(os.environ.get('SEND_INTERVAL_SECONDS', 5))
# For generating past data, covering the last 7 days
PAST_DATA_DAYS = 7

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Machine and Sensor Definitions ---

# Initialize machines
MACHINES = [ # TODO: add other fields
    {"id": "CNC-001", "type": "CNC Mill", "location": "Assembly Line A"},
    {"id": "LATHE-002", "type": "Lathe", "location": "Tooling Bay 2"},
    {"id": "PRESS-003", "type": "Hydraulic Press", "location": "Stamping Area"},
    {"id": "WELD-004", "type": "Robotic Welder", "location": "Line B"}
]

# Simulated sensor types and their expected Gaussian distribution parameters
SENSOR_TEMPLATES = {
    "temperature": (85.0, 5.0),    # Celsius
    "vibration": (1.5, 0.5),       # mm/s^2
    "pressure": (150.0, 10.0),     # PSI
    "power_draw": (1200.0, 50.0)   # Watts
}

# --- Helper Functions ---

def create_redpanda_producer():
    conf = {
        # Connection settings
        'bootstrap.servers': REDPANDA_BROKER,
        # Delivery acknowledgement settings (QoS)
        'acks': 'all',  # Wait for all in-sync replicas to acknowledge
        # Required for asynchronous delivery reporting
        'delivery.report.only.error': False
    }
    try:
        producer = Producer(conf)
        logging.info(f"Redpanda Producer initialized for brokers: {REDPANDA_BROKER}")
        return producer
    except KafkaException as e:
        logging.error(f"Failed to create Producer: {e}")
        time.sleep(5)
        return create_redpanda_producer() # Attempt to reconnect

def delivery_report(err, msg):
    """Callback function called on delivery of a message to Redpanda."""
    if err is not None:
        logging.error(f"Message delivery failed: {err}")
    else:
        # Log success only at debug level for high-volume logs
        logging.debug(f"Message delivered to topic '{msg.topic()}' [{msg.partition()}]")

# generate sensor reading with realistic values using Gaussian distribution
def generate_sensor_reading(machine_id, sensor_type, mean, std_dev, timestamp):

    # Gaussian distribution
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
        # Asynchronously send the message
        # Use machine_id as the key for partitioning, ensuring all data for a machine
        # goes to the same partition, which is good for stream processing state.
        producer.produce(
            TOPIC_NAME,
            key=machine_id.encode('utf-8'),
            value=message_json,
            callback=delivery_report # The callback handles logging delivery status
        )
        # Poll to handle callback processing
        producer.poll(0)
    except Exception as e:
        logging.error(f"Failed to produce message for {machine_id}: {e}")


def generate_and_produce_past_data(producer):
    """Generates and produces a batch of historical data from the past week."""
    logging.info(f"Generating and producing {PAST_DATA_DAYS} days of historical data...")

    end_time = datetime.now()
    # Start time is 7 days ago
    start_time = end_time - timedelta(days=PAST_DATA_DAYS)
    time_delta = end_time - start_time

    # Generate 1 message per machine per sensor every 15 minutes for the past week
    interval_seconds = 15 * 60
    total_messages = (time_delta.total_seconds() / interval_seconds) * len(MACHINES) * len(SENSOR_TEMPLATES)

    current_timestamp = start_time
    message_count = 0

    while current_timestamp < end_time:
        for machine in MACHINES:
            for sensor_type, (mean, std_dev) in SENSOR_TEMPLATES.items():
                message = generate_sensor_reading(
                    machine["id"],
                    sensor_type,
                    mean,
                    std_dev,
                    current_timestamp
                )
                produce_message(producer, message)
                message_count += 1

        # Advance time by the interval
        current_timestamp += timedelta(seconds=interval_seconds)

    # Flush any remaining messages in the producer queue
    producer.flush()
    logging.info(f"Successfully produced {message_count} historical messages.")


def main():
    """Main loop for generating and producing real-time sensor data."""
    producer = create_redpanda_producer()

    # 1. Generate historical data on startup
    generate_and_produce_past_data(producer)

    # 2. Start real-time stream
    logging.info(f"Starting real-time data stream, sending every {SEND_INTERVAL_SECONDS} seconds...")

    try:
        while True:
            start_time = time.time()
            now = datetime.now()

            # Iterate through each machine and its sensors
            for machine in MACHINES:
                for sensor_type, (mean, std_dev) in SENSOR_TEMPLATES.items():
                    # Generate a new reading for the current time
                    message = generate_sensor_reading(
                        machine["id"],
                        sensor_type,
                        mean,
                        std_dev,
                        now
                    )
                    produce_message(producer, message)

            # ensure all messages are sent before waiting
            producer.flush(timeout=1.0) # Flush with a short timeout

            # calculate time to sleep
            elapsed_time = time.time() - start_time
            sleep_time = SEND_INTERVAL_SECONDS - elapsed_time

            if sleep_time > 0:
                time.sleep(sleep_time)
            else:
                logging.warning(f"Producer is running slow! Loop took {elapsed_time:.2f}s, exceeding interval of {SEND_INTERVAL_SECONDS}s.")

    except KeyboardInterrupt:
        logging.info("Shutting down producer...")
    finally:
        # perform final flush and close the producer
        producer.flush(timeout=30)
        logging.info("Producer shut down successfully.")

if __name__ == '__main__':
    main()