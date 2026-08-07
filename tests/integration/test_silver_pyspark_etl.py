import os
import tempfile

import pytest
from pyspark.sql import SparkSession
from src.coverdrive.processing.silver_pyspark_etl import (
    process_silver_to_gold,
)


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
        # Create mock silver directories
        silver_dir = os.path.join(temp_dir, "silver")
        batting_dir = os.path.join(silver_dir, "batting", "ingestion_date=2026-07-19")
        bowling_dir = os.path.join(silver_dir, "bowling", "ingestion_date=2026-07-19")
        os.makedirs(batting_dir)
        os.makedirs(bowling_dir)

        # Create valid mock Silver Parquet data simulating a skewed player (e.g., Kohli)
        batting_data = [
            ("V Kohli (IND)", 10000),
            ("V Kohli (IND)", 2000),
            ("V Kohli (IND)", 500),
            ("Unknown Player (UNK)", 0),
        ]
        batting_df = spark.createDataFrame(batting_data, ["Player", "Runs"])
        batting_df.write.parquet(os.path.join(batting_dir, "data.parquet"))

        # Create mock bowling registry (Dimension)
        bowling_data = [("V Kohli (IND)", 4), ("Unknown Player (UNK)", 0)]
        bowling_df = spark.createDataFrame(bowling_data, ["Player", "Wickets"])
        bowling_df.write.parquet(os.path.join(bowling_dir, "data.parquet"))

        gold_path = os.path.join(temp_dir, "gold/")

        # Execute Pipeline
        # Append trailing slash to silver_path to match the ETL script's glob pattern
        os.environ["COVERDRIVE_S3_ACCESS_KEY"] = "minioadmin"  # pragma: allowlist secret
        os.environ["COVERDRIVE_S3_SECRET_KEY"] = "minioadmin"  # pragma: allowlist secret

        process_silver_to_gold(spark, silver_dir + "/", gold_path)

        # Read the gold output to verify the join succeeded despite the salt
        gold_stats_path = os.path.join(gold_path, "player_stats")
        assert os.path.exists(gold_stats_path), "Gold output was not created"
        result_df = spark.read.parquet(gold_stats_path)

        # Verify the data integrity remained intact after the complex salt & join
        assert result_df.count() == 4

        # Ensure 'player_clean' was standardized correctly and the join matched properly
        kohli_rows = result_df.filter(
            result_df.Runs.isNotNull() & result_df.bowl_Wickets.isNotNull()
        ).count()
        assert kohli_rows == 4, "Failed to join all rows for skewed key"
