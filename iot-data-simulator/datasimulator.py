import datetime
from dateutil import parser
from faker import Faker
from confluent_kafka import Producer
import random
import json
import time

# Initialize Faker instance
fake = Faker()

# Kafka configuration
BOOTSTRAP_SERVERS = "localhost:9092"  # Update with your Kafka server address


def get_kafka_config():
    return {
        # User-specific properties that you must set
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        # Fixed properties
        "acks": "all",
    }


# Function to generate IoT sensor data
def generate_iot_data():
    datetime_this_decade_str = fake.date_time_this_year(before_now=True)
    timestamp = parser.parse(datetime_this_decade_str)
    return {
        "device_id": f"sensor_{random.randint(1, 100)}",
        "temperature": round(random.uniform(-10.0, 50.0), 2),  # Temperature in Celsius
        "humidity": random.randint(10, 100),  # Humidity in percentage
        "pressure": round(random.uniform(950, 1050), 2),  # Atmospheric pressure in hPa
        "wind_speed": round(random.uniform(0.0, 30.0), 2),  # Wind speed in m/s
        "rainfall": round(random.uniform(0.0, 200.0), 2),  # Rainfall in mm
        "location": {
            "latitude": round(random.uniform(-90.0, 90.0), 6),  # Latitude
            "longitude": round(random.uniform(-180.0, 180.0), 6),  # Longitude
        },
        "device_status": random.choice(
            ["active", "inactive", "maintenance"]
        ),  # Status of the IoT device
        "timestamp": timestamp.isoformat(),  # ISO 8601 formatted timestamp
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
            time.sleep(interval)  # Wait for the specified interval
    except KeyboardInterrupt:
        print("\nStopping data production.")


# Main function
if __name__ == "__main__":

    TOPIC = "iot-sensor-data"

    # Create Kafka producer
    producer = create_kafka_producer()

    # Start producing data
    produce_to_kafka(producer, TOPIC, interval=2)
