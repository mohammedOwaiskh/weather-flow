from pyspark.sql import SparkSession, DataFrame


def create_kafka_df(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "iot-sensor-data")
        .option("failOnDataLoss", False)
        .option("group.id", "weather-flow-01")
        .load()
    )
