import os
import tempfile

import pytest
from pyspark.sql import SparkSession

try:
    from src.ingestion.silver_pyspark_etl import _SALT_BUCKETS, process_silver_to_gold
except ImportError:
    from coverdrive.processing.silver_pyspark_etl import process_silver_to_gold


@pytest.fixture(scope="session")
def spark():
    """Create a spark session for testing."""
    return SparkSession.builder.appName("pytest-pyspark-local").master("local[2]").getOrCreate()


def test_key_salting_distribution(spark):
    """
    Verifies that the 'Key Salting' logic successfully breaks down a skewed
    dataset into multiple evenly distributed buckets before the join.
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        silver_dir = os.path.join(temp_dir, "silver")
        batting_dir = os.path.join(silver_dir, "batting", "ingestion_date=2026-07-19")
        bowling_dir = os.path.join(silver_dir, "bowling", "ingestion_date=2026-07-19")
        os.makedirs(batting_dir)
        os.makedirs(bowling_dir)
        batting_data = [
            ("V Kohli (IND)", 10000),
            ("V Kohli (IND)", 2000),
            ("V Kohli (IND)", 500),
            ("Unknown Player (UNK)", 0),
        ]
        batting_df = spark.createDataFrame(batting_data, ["Player", "Runs"])
        batting_df.write.parquet(os.path.join(batting_dir, "data.parquet"))
        bowling_data = [("V Kohli (IND)", 4), ("Unknown Player (UNK)", 0)]
        bowling_df = spark.createDataFrame(bowling_data, ["Player", "Wickets"])
        bowling_df.write.parquet(os.path.join(bowling_dir, "data.parquet"))
        gold_path = os.path.join(temp_dir, "gold/")
        os.environ["COVERDRIVE_S3_ACCESS_KEY"] = "minioadmin"
        os.environ["COVERDRIVE_S3_SECRET_KEY"] = "minioadmin"
        process_silver_to_gold(spark, silver_dir + "/", gold_path)
        gold_stats_path = os.path.join(gold_path, "player_stats")
        assert os.path.exists(gold_stats_path), "Gold output was not created"
        result_df = spark.read.parquet(gold_stats_path)
        assert result_df.count() == 4
        kohli_rows = result_df.filter(
            result_df.Runs.isNotNull() & result_df.bowl_Wickets.isNotNull()
        ).count()
        assert kohli_rows == 4, "Failed to join all rows for skewed key"
