"""Silver → Gold enrichment: PySpark key-salted join of batting and bowling career stats.

PySpark is chosen over Pandas here specifically because the join between batting
(a wide fact table) and bowling (a player dimension) produces severe data skew —
a handful of all-rounders like Kohli appear in orders-of-magnitude more batting
rows than fringe players. At scale this pins a single Spark executor (OOM). The
key-salting technique in ``process_silver_to_gold`` distributes the skewed keys
evenly across the cluster without a broadcast join, which would fail once the
player registry exceeds Spark's broadcast threshold.

The module is designed as a single entrypoint (``__main__``) so Airflow's
BashOperator can call it directly:
    python -m coverdrive.processing.silver_pyspark_etl
"""

from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, floor, lit, lower, rand, regexp_replace, trim

from src.common.utils import configure_logging, get_logger, get_settings

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
configure_logging()
log = get_logger(__name__)
_SALT_BUCKETS: int = 10


def create_spark_session() -> SparkSession:
    """Build a SparkSession.

    Routes to MinIO when ``COVERDRIVE_S3_ENDPOINT`` is set, so the same
    entrypoint works locally and in EMR/Glue without code changes.
    """
    settings = get_settings()
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID", settings.coverdrive_s3_access_key)
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", settings.coverdrive_s3_secret_key)
    builder = (
        SparkSession.builder.appName("Coverdrive-Gold-ETL")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
    )
    s3_endpoint = os.getenv("COVERDRIVE_S3_ENDPOINT")
    if s3_endpoint:
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        )
    return builder.getOrCreate()


def process_silver_to_gold(spark: SparkSession, silver_path: str, gold_path: str) -> None:
    """Join Silver batting and bowling tables into a Gold player-stats table."""
    batting_path = f"{silver_path}batting/*/*.parquet"
    bowling_path = f"{silver_path}bowling/*/*.parquet"
    log.info("spark.read.batting", path=batting_path)
    try:
        batting_df = spark.read.parquet(batting_path)
    except Exception:
        log.exception("spark.read.batting.failed", path=batting_path)
        raise
    log.info("spark.read.bowling", path=bowling_path)
    try:
        bowling_df = spark.read.parquet(bowling_path)
    except Exception:
        log.exception("spark.read.bowling.failed", path=bowling_path)
        raise
    player_col_bat = "player" if "player" in batting_df.columns else "Player"
    player_col_bowl = "player" if "player" in bowling_df.columns else "Player"
    batting_df = batting_df.withColumn(
        "player_clean", lower(trim(regexp_replace(col(player_col_bat), "\\s*\\([^)]+\\)\\s*$", "")))
    )
    bowling_df = bowling_df.withColumn(
        "player_clean",
        lower(trim(regexp_replace(col(player_col_bowl), "\\s*\\([^)]+\\)\\s*$", ""))),
    )
    salts_df = spark.range(0, _SALT_BUCKETS).withColumnRenamed("id", "salt")
    salted_batting_df = batting_df.withColumn(
        "salted_key", concat(col("player_clean"), lit("_"), floor(rand() * _SALT_BUCKETS))
    )
    salted_bowling_df = bowling_df.crossJoin(salts_df).withColumn(
        "salted_key", concat(col("player_clean"), lit("_"), col("salt"))
    )
    for c in bowling_df.columns:
        if c != "player_clean":
            salted_bowling_df = salted_bowling_df.withColumnRenamed(c, f"bowl_{c}")
    joined_df = salted_batting_df.join(salted_bowling_df, on="salted_key", how="left").drop(
        "salted_key", "salt", "player_clean", "bowl_player_clean"
    )
    output_path = f"{gold_path}player_stats"
    log.info("spark.write.gold", path=output_path)
    joined_df.write.mode("overwrite").parquet(output_path)
    log.info("gold.etl.complete")


if __name__ == "__main__":
    spark_sess = create_spark_session()
    silver_s3 = os.environ["SILVER_S3_PATH"]
    gold_s3 = os.environ["GOLD_S3_PATH"]
    process_silver_to_gold(spark_sess, silver_s3, gold_s3)
    sys.exit(0)
