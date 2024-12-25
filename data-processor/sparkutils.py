from pyspark import SparkConf
from pyspark.sql import SparkSession
from utils.configutils import get_aws_config, read_application_config

ACCESS_KEY_ID = get_aws_config("aws.access.key")
SECRET_ACCESS_KEY = get_aws_config("aws.secret.key")
ENV = read_application_config("app.env.profile")


def get_spark_config() -> SparkConf:
    """Generates a Spark configuration object from application settings.

    Reads Spark-related configuration parameters from the application configuration file and populates a SparkConf object. Provides a centralized way to configure Spark session parameters.

    Returns:
        SparkConf: A configured Spark configuration object with settings from the application configuration.
    """
    spark_config = SparkConf()
    for k, v in read_application_config(section="SPARK_CONFIG"):
        spark_config.set(k, v)

    return spark_config


def get_spark_session() -> SparkSession:
    """Creates and configures a Spark session for data processing.

    Initializes a Spark session with predefined configuration settings and sets up AWS credentials for S3 access when not in AWS environment. Ensures a consistent and configured Spark session is available for data processing tasks.

    Returns:
        SparkSession: A configured Spark session ready for data processing operations.
    """
    conf = get_spark_config()
    spark: SparkSession = SparkSession.builder.config(conf=conf).getOrCreate()
    if ENV != "aws":
        hadoopConfig = spark.sparkContext._jsc.hadoopConfiguration()
        hadoopConfig.set(
            "fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        hadoopConfig.set("fs.s3a.access.key", ACCESS_KEY_ID)
        hadoopConfig.set("fs.s3a.secret.key", SECRET_ACCESS_KEY)
    return spark
