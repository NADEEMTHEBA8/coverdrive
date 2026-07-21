"""Silver ETL: Flatten nested Cricsheet JSON into tabular Parquet using PySpark."""

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, input_file_name, regexp_extract
from structlog import get_logger

from coverdrive.utils import configure_logging

log = get_logger(__name__)


def process_cricsheet(
    spark: SparkSession, bronze_path: str, silver_matches_path: str, silver_balls_path: str
) -> None:
    """Flatten Cricsheet JSON into matches and ball-by-ball datasets."""
    log.info("cricsheet_etl.start", bronze_path=bronze_path)

    # 1. Read Raw JSON (PySpark infers the complex nested schema)
    raw_df = spark.read.option("multiline", "true").json(bronze_path)

    # Extract match_id from filename (e.g., "1389389.json" -> "1389389")
    df = raw_df.withColumn("match_id", regexp_extract(input_file_name(), r"(\d+)\.json", 1))

    log.info("cricsheet_etl.read", rows=df.count())

    # 2. Extract Match-level metadata (Dimension-ready)
    # The info struct contains arrays and strings
    matches_df = df.select(
        col("match_id"),
        col("info.dates").getItem(0).alias("match_date"),
        col("info.venue").alias("venue"),
        col("info.city").alias("city"),
        col("info.teams").getItem(0).alias("team1"),
        col("info.teams").getItem(1).alias("team2"),
        col("info.match_type").alias("match_type"),
    )

    matches_df.write.mode("overwrite").parquet(silver_matches_path)
    log.info("cricsheet_etl.matches_written", rows=matches_df.count(), path=silver_matches_path)

    # 3. Flatten Ball-by-Ball data (Fact-ready)
    # Explode innings -> overs -> deliveries
    innings_df = df.select(col("match_id"), explode(col("innings")).alias("inning"))
    overs_df = innings_df.select(
        col("match_id"),
        col("inning.team").alias("batting_team"),
        explode(col("inning.overs")).alias("over"),
    )
    balls_df = overs_df.select(
        col("match_id"),
        col("batting_team"),
        col("over.over").alias("over_num"),
        explode(col("over.deliveries")).alias("ball"),
    )

    # Extract the delivery details
    flat_balls_df = balls_df.select(
        col("match_id"),
        col("batting_team"),
        col("over_num"),
        col("ball.batter").alias("batter"),
        col("ball.bowler").alias("bowler"),
        col("ball.runs.batter").alias("runs_batter"),
        col("ball.runs.extras").alias("runs_extras"),
        col("ball.runs.total").alias("runs_total"),
        # Extract wicket info (if any)
        col("ball.wickets").getItem(0).getField("kind").alias("wicket_kind"),
        col("ball.wickets").getItem(0).getField("player_out").alias("player_out"),
    )

    flat_balls_df.write.mode("overwrite").parquet(silver_balls_path)
    log.info("cricsheet_etl.balls_written", rows=flat_balls_df.count(), path=silver_balls_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Cricsheet ETL")
    parser.add_argument(
        "--bronze-path",
        default="s3a://coverdrive/bronze/cricsheet/*.json",
        help="Input JSON glob",
    )
    parser.add_argument(
        "--silver-matches",
        default="s3a://coverdrive/silver/cricsheet_matches/",
        help="Output Parquet path for matches",
    )
    parser.add_argument(
        "--silver-balls",
        default="s3a://coverdrive/silver/cricsheet_balls/",
        help="Output Parquet path for ball-by-ball",
    )
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("CricsheetETL")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .getOrCreate()
    )
    # Configure S3 endpoint for local MinIO if needed
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

    process_cricsheet(spark, args.bronze_path, args.silver_matches, args.silver_balls)


if __name__ == "__main__":
    configure_logging()
    main()
