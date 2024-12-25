from dataconsumer import create_kafka_df
from pyspark.sql import DataFrame
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
from writer import write_to_csv


def transform_kafka_data(kafka_df: DataFrame) -> DataFrame:
    """Transforms raw Kafka streaming data into a structured DataFrame.

    Converts Kafka message values from JSON to a predefined schema and flattens the location data. Prepares the streaming data for further processing by standardizing its structure.

    Args:
        kafka_df: Incoming Spark DataFrame containing raw Kafka messages.

    Returns:
        DataFrame: A transformed DataFrame with parsed and flattened IoT sensor data.
    """
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


def get_data_schema() -> StructType:
    """Defines the schema for weather device data.

    Creates a structured schema representing the data model for weather device measurements. Provides a consistent and well-defined structure for parsing and validating incoming streaming data.

    Returns:
        StructType: A Spark DataFrame schema with fields for device measurements and metadata.
    """
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


def transform():
    """Orchestrates the end-to-end data processing workflow for IoT sensor data.

    Coordinates the ingestion, transformation, and writing of streaming sensor data from Kafka to CSV storage. Manages the entire data processing pipeline with error handling for graceful interruption.

    Raises:
        KeyboardInterrupt: Allows for clean termination of the data processing workflow.
    """
    try:
        spark = get_spark_session()

        kafka_df = create_kafka_df(spark)
        iot_data_df = transform_kafka_data(kafka_df)
        # write_to_console(iot_data_df)
        write_to_csv(iot_data_df, "s3a://weather-flow-output/output-files")
    except KeyboardInterrupt:
        print("\nStopped processing data")


if __name__ == "__main__":
    transform()
