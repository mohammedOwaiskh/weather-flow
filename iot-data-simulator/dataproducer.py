import json
import time

from confluent_kafka import Producer
from datasimulator import generate_iot_data

# Kafka configuration
BOOTSTRAP_SERVERS = "localhost:9092"  # Update with your Kafka server address


def get_kafka_config() -> dict:
    """Generates configuration settings for Kafka producer.

    Provides a dictionary of configuration parameters required for establishing a Kafka producer connection. Combines user-defined and fixed settings to ensure reliable message publishing.

    Returns:
        dict: A configuration dictionary with Kafka producer settings.
    """
    return {
        # User-specific properties that you must set
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        # Fixed properties
        "acks": "all",
    }


# Kafka producer configuration
def create_kafka_producer() -> Producer:
    """Creates a Kafka producer for sending IoT sensor data.

    Initializes a Kafka producer using predefined configuration settings. Prepares a communication channel for publishing simulated sensor measurements to a Kafka topic.

    Returns:
        Producer: A configured Kafka producer ready to send messages.
    """
    return Producer(get_kafka_config())


# Produce data to Kafka
def produce_to_kafka(producer: Producer, topic, interval=1):
    """Continuously produces simulated IoT sensor data to a Kafka topic.

    Generates and publishes sensor measurement data at regular intervals using a Kafka producer. Provides a persistent data streaming mechanism with configurable publishing frequency.

    Args:
        producer: Configured Kafka producer for sending messages.
        topic: Kafka topic to which sensor data will be published.
        interval: Time delay between data production cycles, defaults to 1 second.

    Raises:
        KeyboardInterrupt: Allows graceful termination of data production when interrupted.
    """
    print(f"Producing data to Kafka topic: {topic}")
    try:
        while True:
            # for _ in range(1):
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
