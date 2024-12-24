from pyspark import SparkConf
from pyspark.sql import SparkSession


def get_spark_session() -> SparkSession:
    conf = SparkConf()
    conf.setMaster("local")
    conf.set("spark.streaming.stopGracefullyOnShutdown", True)
    conf.set("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0")
    conf.set("spark.sql.shuffle.partitions", 4)
    return SparkSession.builder.appName("WeatherFlow").config(conf=conf).getOrCreate()


if __name__ == "__main__":
    print("HI")
