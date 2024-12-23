import json

from pyspark import SparkConf
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructType, StructField, DataType
from pyspark.sql.functions import from_json


def process_data(topic, data):
    data_dict: dict = json.loads(data)

    print(data_dict)


def get_spark_session():
    conf = SparkConf()
    conf.setMaster("local")
    conf.set("spark.streaming.stopGracefullyOnShutdown", True)
    conf.set("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0")
    conf.set("spark.sql.shuffle.partitions", 4)
    return SparkSession.builder.appName("WeatherFlow").config(conf=conf).getOrCreate()


def create_kafka_df(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "iot-sensor-data")
        .option("startingOffsets", "earliest")
        .load()
    )


def get_data_schema():
    # TODO
    return


if __name__ == "__main__":
    spark = get_spark_session()

    kafka_df = create_kafka_df(spark)

    kafka_df = kafka_df.withColumn("value", kafka_df.value.cast(StringType()))

    weather_df = kafka_df.select("value").withColumn("value", from_json())

    weather_df.writeStream.format("console").outputMode("append").trigger(
        processingTime="10 seconds"
    ).start().awaitTermination()
