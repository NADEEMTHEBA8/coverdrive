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
    monkeypatch.setenv("COVERDRIVE_S3_ENDPOINT", "")
    monkeypatch.setenv("COVERDRIVE_S3_ACCESS_KEY", "testing")
    monkeypatch.setenv("COVERDRIVE_S3_SECRET_KEY", "testing")
    monkeypatch.setenv("COVERDRIVE_S3_BUCKET", "coverdrive-test")
    monkeypatch.setenv("COVERDRIVE_S3_USE_SSL", "false")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    try:
        from src.common.utils import get_settings, load_pipeline_config
    except ImportError:
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
        '\nversion: 1\nsources:\n  batting:\n    base_url: "https://example.test/batting"\n    params: {class: 2, type: batting}\n    pages_to_fetch: 2\n    target_table: batting\n  bowling:\n    base_url: "https://example.test/bowling"\n    params: {class: 2, type: bowling}\n    pages_to_fetch: 2\n    target_table: bowling\nstorage:\n  bronze_prefix: "bronze"\n  silver_prefix: "silver"\n  partition_key: "ingestion_date"\n  compression: "snappy"\nhttp:\n  timeout_seconds: 5\n  user_agent: "test"\n  retry:\n    max_attempts: 2\n    initial_wait_seconds: 0.01\n    max_wait_seconds: 0.05\n    multiplier: 1\nquality:\n  batting:\n    min_rows: 1\n    max_null_ratio: 0.5\n    runs_max: 30000\n    strike_rate_max: 500\n  bowling:\n    min_rows: 1\n    max_null_ratio: 0.5\n    wickets_max: 1000\n    economy_max: 20\n'
    )
    try:
        from src.common import utils
    except ImportError:
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
