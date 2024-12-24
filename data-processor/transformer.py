import json

from dataconsumer import create_kafka_df
from writer import write_to_console, write_to_csv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from sparkutils import get_spark_session
import s3fs


def transform_kafka_data(kafka_df: DataFrame) -> DataFrame:
    raw_df = kafka_df.withColumn("value", kafka_df.value.cast(StringType())).select(
        "value"
    )

    data_schema = get_data_schema()
    iot_data_json_df = raw_df.withColumn("value", from_json("value", data_schema))
    iot_data_parsed = iot_data_json_df.select(col("value.*"))

    return (
        iot_data_parsed.withColumn("longitude", col("location.longitude"))
        .withColumn("latitude", col("location.latitude"))
        .drop("location")
    )


def get_data_schema():
    return StructType(
        [
            StructField("device_id", StringType()),
            StructField("temperature", DecimalType()),
            StructField("humidity", IntegerType()),
            StructField("pressure", DecimalType()),
            StructField("wind_speed", DecimalType()),
            StructField("rainfall", DecimalType()),
            StructField(
                "location",
                StructType(
                    [
                        StructField("latitude", DecimalType()),
                        StructField("longitude", DecimalType()),
                    ]
                ),
            ),
            StructField("device_status", StringType()),
            StructField("timestamp", TimestampType()),
        ]
    )


if __name__ == "__main__":

    try:
        spark = get_spark_session()

        kafka_df = create_kafka_df(spark)

        iot_data_df = transform_kafka_data(kafka_df)

        # write_to_console(iot_data_df)

        write_to_csv(iot_data_df, "s3:/weather-flow-output/output-files")
    except KeyboardInterrupt:
        print("\nStopped processing data")
