from pyspark.sql import SparkSession, DataFrame


def create_kafka_df(spark: SparkSession) -> DataFrame:
    """Creates a Kafka streaming DataFrame for consuming IoT sensor data.

    Establishes a Spark streaming connection to a Kafka topic for reading sensor measurements. Configures Kafka consumer settings to enable reliable data ingestion from the specified topic.

    Args:
        spark: Active Spark session used to create the streaming DataFrame.

    Returns:
        DataFrame: A Spark streaming DataFrame containing raw Kafka messages from the IoT sensor data topic.
    """
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "iot-sensor-data")
        .option("failOnDataLoss", False)
        .option("group.id", "weather-flow-01")
        .load()
    )
