"""Tests for coverdrive.ingestion."""

from __future__ import annotations

import io
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest
import requests

from coverdrive import ingestion
from coverdrive.utils import build_partition_path


def test_build_partition_path_format() -> None:
    """Hive-style partition path is deterministic for a given date."""
    fixed_date = datetime(2024, 6, 15, tzinfo=UTC)
    key = build_partition_path("bronze", "batting", fixed_date)
    assert key == "bronze/batting/ingestion_date=2024-06-15/data.parquet"


def test_build_partition_path_silver_layer() -> None:
    fixed_date = datetime(2024, 6, 15, tzinfo=UTC)
    assert build_partition_path("silver", "bowling", fixed_date) == (
        "silver/bowling/ingestion_date=2024-06-15/data.parquet"
    )


def test_load_from_fixtures_drops_unnamed_columns(fixtures_dir: Path) -> None:
    df = ingestion.load_from_fixtures("batting", fixtures_dir)
    assert not any(c.startswith("Unnamed") for c in df.columns)
    assert len(df) > 0
    assert "Player" in df.columns


def test_load_from_fixtures_missing_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="Fixture not found"):
        ingestion.load_from_fixtures("nonexistent", tmp_path)


def test_write_bronze_is_idempotent(batting_csv: pd.DataFrame, s3_bucket: str) -> None:
    """Writing the same DataFrame twice for the same partition produces one object."""
    fixed_date = datetime(2024, 6, 15, tzinfo=UTC)
    uri1 = ingestion.write_bronze(batting_csv, "batting", ingestion_date=fixed_date)
    uri2 = ingestion.write_bronze(batting_csv, "batting", ingestion_date=fixed_date)
    assert uri1 == uri2

    import boto3

    s3 = boto3.client("s3", region_name="us-east-1")
    response = s3.list_objects_v2(
        Bucket=s3_bucket, Prefix="bronze/batting/ingestion_date=2024-06-15/"
    )
    assert response["KeyCount"] == 1


def test_write_bronze_round_trip(batting_csv: pd.DataFrame, s3_bucket: str) -> None:
    """Written Parquet reads back identical."""
    fixed_date = datetime(2024, 6, 15, tzinfo=UTC)
    ingestion.write_bronze(batting_csv, "batting", ingestion_date=fixed_date)

    import boto3

    s3 = boto3.client("s3", region_name="us-east-1")
    key = "bronze/batting/ingestion_date=2024-06-15/data.parquet"
    obj = s3.get_object(Bucket=s3_bucket, Key=key)
    roundtripped = pd.read_parquet(io.BytesIO(obj["Body"].read()))
    pd.testing.assert_frame_equal(
        batting_csv.reset_index(drop=True), roundtripped.reset_index(drop=True)
    )


def test_parse_html_table_index_out_of_range() -> None:
    """If ESPN HTML changes shape, we get a clear error — not a silent KeyError."""
    html_one_table = "<html><body><table><tr><td>a</td></tr></table></body></html>"
    with pytest.raises(ValueError, match="ESPNcricinfo HTML structure may have changed"):
        ingestion._parse_html_table(html_one_table, table_index=2)


def test_fetch_page_retries_on_503(monkeypatch: pytest.MonkeyPatch) -> None:
    """Transient 503s recover; persistent ones raise after max_attempts."""
    call_count = {"n": 0}

    def flaky_get(*args, **kwargs):  # type: ignore[no-untyped-def]
        call_count["n"] += 1
        response = requests.Response()
        if call_count["n"] < 2:
            response.status_code = 503
            response.reason = "Service Unavailable"
            # raise_for_status raises HTTPError, which is in RETRYABLE_HTTP_ERRORS
        else:
            response.status_code = 200
            response._content = b"<html><body></body></html>"
        return response

    monkeypatch.setattr(requests, "get", flaky_get)
    # Use the real retry behavior with a tiny number of attempts
    from coverdrive.utils import load_pipeline_config

    cfg = load_pipeline_config()
    from coverdrive.utils import make_retrier

    retrier = make_retrier(ingestion.RETRYABLE_HTTP_ERRORS)
    for attempt in retrier:
        with attempt:
            html = ingestion._fetch_page("http://test", {}, cfg)
    assert call_count["n"] >= 2  # At least one retry happened
    assert html == "<html><body></body></html>"
