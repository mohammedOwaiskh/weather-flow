import configparser

from pyspark import SparkConf


def get_spark_config() -> SparkConf:
    """
    Return a SparkConf object after reading 'spark.config'
    Returns:
        SparkConf: object with spark
    """
    spark_config = SparkConf()
    for k, v in read_application_config(section="SPARK_CONFIG"):
        spark_config.set(k, v)

    return spark_config


def read_application_config(
    key: str = None, section: str = "DEFAULT"
) -> str | list[tuple]:
    config = configparser.ConfigParser()
    config.read("data-processor\\config\\application.conf")
    return config.items(section) if key is None else config[section][key]


def get_aws_config(key: str) -> str:
    return read_application_config(key, section="AWS_CONFIG")
