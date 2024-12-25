from pyspark.sql import DataFrame


def write_to_console(df: DataFrame):
    df.writeStream.format("console").outputMode("append").trigger(
        processingTime="10 seconds"
    ).start().awaitTermination()


def write_to_csv(df: DataFrame, location: str, checkpoint: str = "checkpoint"):
    df.writeStream.format("csv").option("checkpointLocation", f"{checkpoint}/").option(
        "path", f"{location}/"
    ).option("header", True).outputMode("append").trigger(
        processingTime="10 seconds"
    ).start().awaitTermination()
