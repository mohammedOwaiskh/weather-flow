from utils.configutils import get_aws_config, get_spark_config, read_application_config
from pyspark.sql import SparkSession


ACCESS_KEY_ID = get_aws_config("aws.access.key")
SECRET_ACCESS_KEY = get_aws_config("aws.secret.key")
ENV = read_application_config("app.env.profile", "APP_DEFAULT")


def get_spark_session() -> SparkSession:
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
