"""Spark session factory shared by every batch and streaming job."""

import os

from pyspark.sql import SparkSession


def get_spark(app_name, extra_conf=None):
    """Return a SparkSession tuned for a single-node Docker container.

    Memory and parallelism are read from environment variables so the same
    job can be scaled up on a real cluster without code changes.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.driver.memory",
            os.environ.get("SPARK_DRIVER_MEMORY", "4g"),
        )
        .config(
            "spark.sql.shuffle.partitions",
            os.environ.get("SPARK_SHUFFLE_PARTITIONS", "64"),
        )
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
    )
    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    return spark
