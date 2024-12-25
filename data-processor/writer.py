from pyspark.sql import DataFrame


def write_to_console(df: DataFrame):
    """Writes a Spark streaming DataFrame to console in a append mode.

    Outputs the DataFrame to the console using Spark's streaming capabilities with an append output mode and a 10-second processing interval. Provides a convenient method for debugging and monitoring streaming data.

    Args:
        df: Spark DataFrame to be written to console.
    """
    df.writeStream.format("console").outputMode("append").trigger(
        processingTime="10 seconds"
    ).start().awaitTermination()


def write_to_csv(df: DataFrame, location: str, checkpoint: str = "checkpoint"):
    """Writes a Spark streaming DataFrame to CSV files in a append mode.

    Streams the DataFrame to CSV files with configurable location and checkpoint settings. Provides a flexible method for persisting streaming data to CSV format with periodic processing.

    Args:
        df: Spark DataFrame to be written to CSV.
        location: Directory path where CSV files will be saved.
        checkpoint: Directory path for storing streaming checkpoints, defaults to "checkpoint".
    """
    df.writeStream.format("csv").option("checkpointLocation", f"{checkpoint}/").option(
        "path", f"{location}/"
    ).option("header", True).outputMode("append").trigger(
        processingTime="10 seconds"
    ).start().awaitTermination()
