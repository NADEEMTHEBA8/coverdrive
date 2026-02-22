"""Shared test fixtures.

Uses `moto` to mock S3 so tests run hermetically — no MinIO container needed.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import boto3
import pandas as pd
import pytest
from moto import mock_aws


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Force test-time env values so we never accidentally hit real AWS / MinIO."""
    monkeypatch.setenv("COVERDRIVE_ENV", "ci")
    monkeypatch.setenv("COVERDRIVE_S3_ENDPOINT", "")  # moto handles routing
    monkeypatch.setenv("COVERDRIVE_S3_ACCESS_KEY", "testing")
    monkeypatch.setenv("COVERDRIVE_S3_SECRET_KEY", "testing")
    monkeypatch.setenv("COVERDRIVE_S3_BUCKET", "coverdrive-test")
    monkeypatch.setenv("COVERDRIVE_S3_USE_SSL", "false")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

    # Clear cached settings/config between tests.
    from coverdrive.utils import get_settings, load_pipeline_config

    get_settings.cache_clear()
    load_pipeline_config.cache_clear()


@pytest.fixture
def fixtures_dir() -> Path:
    """Path to the test fixtures directory."""
    return Path(__file__).parent / "fixtures"


@pytest.fixture
def batting_csv(fixtures_dir: Path) -> pd.DataFrame:
    """The raw batting CSV fixture, loaded as a DataFrame."""
    return pd.read_csv(fixtures_dir / "batting_sample.csv")


@pytest.fixture
def bowling_csv(fixtures_dir: Path) -> pd.DataFrame:
    """The raw bowling CSV fixture, loaded as a DataFrame."""
    return pd.read_csv(fixtures_dir / "bowling_sample.csv")


@pytest.fixture
def s3_bucket() -> Iterator[str]:
    """Provide a mocked S3 bucket via moto. Yields the bucket name."""
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        bucket = os.environ["COVERDRIVE_S3_BUCKET"]
        client.create_bucket(Bucket=bucket)
        yield bucket


@pytest.fixture
def pipeline_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Write a minimal pipeline.yaml into a temp dir and point the loader at it."""
    config_path = tmp_path / "pipeline.yaml"
    config_path.write_text(
        """
version: 1
sources:
  batting:
    base_url: "https://example.test/batting"
    params: {class: 2, type: batting}
    pages_to_fetch: 2
    target_table: batting
  bowling:
    base_url: "https://example.test/bowling"
    params: {class: 2, type: bowling}
    pages_to_fetch: 2
    target_table: bowling
storage:
  bronze_prefix: "bronze"
  silver_prefix: "silver"
  partition_key: "ingestion_date"
  compression: "snappy"
http:
  timeout_seconds: 5
  user_agent: "test"
  retry:
    max_attempts: 2
    initial_wait_seconds: 0.01
    max_wait_seconds: 0.05
    multiplier: 1
quality:
  batting:
    min_rows: 1
    max_null_ratio: 0.5
    runs_max: 30000
    strike_rate_max: 500
  bowling:
    min_rows: 1
    max_null_ratio: 0.5
    wickets_max: 1000
    economy_max: 20
"""
    )
    # Point the config loader at our temp file by monkeypatching its default arg.
    from coverdrive import utils

    monkeypatch.setattr(
        utils,
        "load_pipeline_config",
        lambda path=config_path: utils.PipelineConfig.model_validate(
            __import__("yaml").safe_load(config_path.read_text())
        ),
    )
    utils.load_pipeline_config.cache_clear() if hasattr(
        utils.load_pipeline_config, "cache_clear"
    ) else None
    return config_path
