"""Silver ETL: Flatten nested Cricsheet JSON into tabular Parquet using PySpark."""

import argparse
import io
import os
import sys
import zipfile
from collections.abc import Iterator

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, input_file_name, regexp_extract
from structlog import get_logger

from src.common.utils import configure_logging, get_s3_client, get_settings

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
log = get_logger(__name__)


def process_cricsheet(
    spark: SparkSession, bronze_path: str, silver_matches_path: str, silver_balls_path: str
) -> None:
    """Flatten Cricsheet JSON into matches and ball-by-ball datasets."""
    log.info("cricsheet_etl.start", bronze_path=bronze_path)
    settings = get_settings()
    is_zip = False
    try:
        s3_client = get_s3_client()
        s3_client.head_object(
            Bucket=settings.coverdrive_s3_bucket, Key="bronze/cricsheet/t20s_json.zip"
        )
        is_zip = True
    except Exception:
        is_zip = False
    if is_zip:
        log.info("cricsheet_etl.reading_direct_zip", bucket=settings.coverdrive_s3_bucket)
        s3_client = get_s3_client()
        resp = s3_client.get_object(
            Bucket=settings.coverdrive_s3_bucket, Key="bronze/cricsheet/t20s_json.zip"
        )
        zip_bytes = resp["Body"].read()

        def unzip_entries(zip_payload: bytes) -> Iterator[tuple[str, str]]:
            with zipfile.ZipFile(io.BytesIO(zip_payload)) as z:
                for fn in z.namelist():
                    if fn.endswith(".json"):
                        match_id = fn.split("/")[-1].replace(".json", "")
                        content = z.read(fn).decode("utf-8")
                        yield (match_id, content)

        sc = spark.sparkContext
        pairs_rdd = sc.parallelize([zip_bytes]).flatMap(unzip_entries)
        json_rdd = pairs_rdd.map(lambda x: x[1])
        raw_df = spark.read.option("multiline", "true").json(json_rdd)
        from pyspark.sql.functions import concat_ws, sha2

        df = raw_df.withColumn(
            "match_id",
            sha2(
                concat_ws(
                    "_",
                    col("info.teams").getItem(0),
                    col("info.teams").getItem(1),
                    col("info.dates").getItem(0),
                ),
                256,
            ),
        )
    else:
        raw_df = spark.read.option("multiline", "true").json(bronze_path)
        df = raw_df.withColumn("match_id", regexp_extract(input_file_name(), "(\\d+)\\.json", 1))
    log.info("cricsheet_etl.read", rows=df.count())
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
    flat_balls_df = balls_df.select(
        col("match_id"),
        col("batting_team"),
        col("over_num"),
        col("ball.batter").alias("batter"),
        col("ball.bowler").alias("bowler"),
        col("ball.runs.batter").alias("runs_batter"),
        col("ball.runs.extras").alias("runs_extras"),
        col("ball.runs.total").alias("runs_total"),
        col("ball.wickets").getItem(0).getField("kind").alias("wicket_kind"),
        col("ball.wickets").getItem(0).getField("player_out").alias("player_out"),
    )
    flat_balls_df.write.mode("overwrite").parquet(silver_balls_path)
    log.info("cricsheet_etl.balls_written", rows=flat_balls_df.count(), path=silver_balls_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Cricsheet ETL")
    parser.add_argument(
        "--bronze-path", default="s3a://coverdrive/bronze/cricsheet/*.json", help="Input JSON glob"
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
        .config("spark.driver.memory", "4g")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .getOrCreate()
    )
    from src.common.utils import get_settings

    settings = get_settings()
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", settings.coverdrive_s3_access_key)
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", settings.coverdrive_s3_secret_key)
    if settings.coverdrive_s3_endpoint:
        sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", settings.coverdrive_s3_endpoint)
        sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
        sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
    process_cricsheet(spark, args.bronze_path, args.silver_matches, args.silver_balls)


if __name__ == "__main__":
    configure_logging()
    main()
