from pyspark import SparkConf
from pyspark.sql import SparkSession


def get_spark_session() -> SparkSession:
    conf = SparkConf()
    conf.setMaster("local")
    conf.set("spark.streaming.stopGracefullyOnShutdown", True)
    conf.set(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws:2.7.2",
    )
    conf.set("spark.sql.shuffle.partitions", 4)
    return SparkSession.builder.appName("WeatherFlow").config(conf=conf).getOrCreate()
