import json
import time

from confluent_kafka import Producer
from datasimulator import generate_iot_data

# Kafka configuration
BOOTSTRAP_SERVERS = "localhost:9092"  # Update with your Kafka server address


def get_kafka_config():
    return {
        # User-specific properties that you must set
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        # Fixed properties
        "acks": "all",
    }


# Kafka producer configuration
def create_kafka_producer():
    return Producer(get_kafka_config())


# Produce data to Kafka
def produce_to_kafka(producer: Producer, topic, interval=1):
    print(f"Producing data to Kafka topic: {topic}")
    try:
        while True:
            data: dict = generate_iot_data()
            json_data = json.dumps(data)
            print(f"Produced: {json_data}")
            producer.produce(topic, json_data)
            producer.flush()
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nStopping data production.")


# Main function
if __name__ == "__main__":

    TOPIC = "iot-sensor-data"

    # Create Kafka producer
    producer = create_kafka_producer()

    # Start producing data
    produce_to_kafka(producer, TOPIC, interval=2)
