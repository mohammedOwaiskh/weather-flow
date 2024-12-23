from confluent_kafka import Consumer

from processor import process_data

BOOTSTRAP_SERVERS = "localhost:9092"


def get_kafka_config():
    return {
        # User-specific properties that you must set
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        # Fixed properties
        "group.id": "weather-flow-consumer-grp",
        "auto.offset.reset": "earliest",
    }


def create_kafka_consumer():
    return Consumer(get_kafka_config())


def consume(consumer: Consumer, topic: str, interval=2.0):
    consumer.subscribe([topic])
    try:
        while True:
            msg = consumer.poll(interval)
            if msg is None:
                # print("Waiting...")
                ...
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                process_data(msg.topic(), msg.value())
    except KeyboardInterrupt:
        print("Stopped Consuming")
    finally:
        consumer.close()


if __name__ == "__main__":
    TOPIC = "iot-sensor-data"

    consumer = create_kafka_consumer()

    consume(consumer, TOPIC)
