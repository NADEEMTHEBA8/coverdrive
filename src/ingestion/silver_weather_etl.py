"""Silver ETL: Process Open-Meteo weather JSON into tabular Parquet using PySpark."""

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from structlog import get_logger

from src.common.utils import configure_logging

log = get_logger(__name__)


def process_weather(spark: SparkSession, bronze_path: str, silver_path: str) -> None:
    """Flatten Weather JSON into a tabular dataset."""
    log.info("weather_etl.start", bronze_path=bronze_path)

    # 1. Read Raw JSON
    raw_df = spark.read.option("multiline", "true").json(bronze_path)
    log.info("weather_etl.read", rows=raw_df.count())

    if raw_df.count() == 0:
        log.warning("weather_etl.empty_input")
        return

    # 2. Extract match-level weather (the daily arrays should only have 1 element)
    weather_df = raw_df.select(
        col("match_id"),
        col("daily.time").getItem(0).alias("date"),
        col("daily.temperature_2m_max").getItem(0).alias("temp_max_c"),
        col("daily.precipitation_sum").getItem(0).alias("precip_mm"),
        col("daily.rain_sum").getItem(0).alias("rain_mm"),
    )

    weather_df.write.mode("overwrite").parquet(silver_path)
    log.info("weather_etl.written", rows=weather_df.count(), path=silver_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Weather ETL")
    parser.add_argument(
        "--bronze-path",
        default="s3a://coverdrive/bronze/weather/*.json",
        help="Input JSON glob",
    )
    parser.add_argument(
        "--silver-path",
        default="s3a://coverdrive/silver/weather/",
        help="Output Parquet path",
    )
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("WeatherETL")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .getOrCreate()
    )
    endpoint = os.environ.get("COVERDRIVE_S3_ENDPOINT")
    if endpoint:
        sc = spark.sparkContext
        sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", endpoint)
        sc._jsc.hadoopConfiguration().set(
            "fs.s3a.access.key", os.environ.get("COVERDRIVE_S3_ACCESS_KEY", "minioadmin")
        )
        sc._jsc.hadoopConfiguration().set(
            "fs.s3a.secret.key", os.environ.get("COVERDRIVE_S3_SECRET_KEY", "minioadmin")
        )
        sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
        sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
        sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    process_weather(spark, args.bronze_path, args.silver_path)


if __name__ == "__main__":
    configure_logging()
    main()
