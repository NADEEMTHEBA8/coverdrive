"""Shared utilities: typed config, structured logging, retry helpers, S3 client.

Everything else in this package depends on these primitives. Keep this module
small, well-typed, and side-effect-free at import time.
"""

from __future__ import annotations

import logging
import sys
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal, cast

import boto3
import structlog
import yaml
from botocore.client import Config as BotoConfig
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from tenacity import (
    RetryCallState,
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

# ─── Config models ───────────────────────────────────────────────────────────
# Pipeline config from conf/pipeline.yaml. Strongly typed so a typo in YAML
# fails at startup, not at run time.


class _RetryConfig(BaseModel):
    max_attempts: int = Field(ge=1, le=10)
    initial_wait_seconds: float = Field(gt=0)
    max_wait_seconds: float = Field(gt=0)
    multiplier: float = Field(gt=0)


class _HttpConfig(BaseModel):
    timeout_seconds: int = Field(ge=1, le=300)
    user_agent: str
    retry: _RetryConfig


class _SourceConfig(BaseModel):
    base_url: str
    params: dict[str, Any]
    pages_to_fetch: int = Field(ge=1, le=100)
    target_table: str


class _StorageConfig(BaseModel):
    bronze_prefix: str
    silver_prefix: str
    partition_key: str
    compression: Literal["snappy", "gzip", "zstd"]


class _TableQualityConfig(BaseModel):
    min_rows: int
    max_null_ratio: float = Field(ge=0, le=1)
    runs_max: int | None = None
    strike_rate_max: float | None = None
    wickets_max: int | None = None
    economy_max: float | None = None


class _QualityConfig(BaseModel):
    batting: _TableQualityConfig
    bowling: _TableQualityConfig


class PipelineConfig(BaseModel):
    """Top-level pipeline config — loaded from conf/pipeline.yaml."""

    version: int
    sources: dict[str, _SourceConfig]
    storage: _StorageConfig
    http: _HttpConfig
    quality: _QualityConfig


# ─── Runtime settings (env-driven) ───────────────────────────────────────────
# Settings that vary per environment (local/ci/prod) come from env vars,
# not the YAML file. Twelve-factor app principle.


class Settings(BaseSettings):
    """Environment-driven settings. Defaults match docker-compose local stack."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    coverdrive_env: Literal["local", "ci", "prod"] = "local"
    log_level: str = "INFO"

    coverdrive_s3_endpoint: str = "http://localhost:9000"
    coverdrive_s3_access_key: str = "minioadmin"
    coverdrive_s3_secret_key: str = "minioadmin"
    coverdrive_s3_bucket: str = "coverdrive"
    coverdrive_s3_use_ssl: bool = False

    coverdrive_warehouse_path: str = "data/warehouse.duckdb"

    api_host: str = "0.0.0.0"  # noqa: S104  # binding all interfaces is intentional for container
    api_port: int = 8000

    slack_webhook_url: str | None = None


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Cached settings accessor. One instance per process."""
    return Settings()


@lru_cache(maxsize=1)
def load_pipeline_config(config_path: str | Path = "conf/pipeline.yaml") -> PipelineConfig:
    """Load and validate the pipeline config. Cached — file is read once per process."""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Pipeline config not found at {path.resolve()}")
    with path.open() as f:
        raw = yaml.safe_load(f)
    return PipelineConfig.model_validate(raw)


# ─── Logging ─────────────────────────────────────────────────────────────────
# structlog with JSON output in prod, human-readable console in local dev.
# Bound context (e.g. run_id, table) propagates through nested calls.


def configure_logging(level: str | None = None) -> None:
    """Configure structlog. Call once at process start (idempotent)."""
    settings = get_settings()
    log_level = (level or settings.log_level).upper()
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, log_level, logging.INFO),
    )

    processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]
    if settings.coverdrive_env == "local":
        processors.append(structlog.dev.ConsoleRenderer(colors=True))
    else:
        processors.append(structlog.processors.JSONRenderer())

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(getattr(logging, log_level)),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """Return a structured logger. Call after configure_logging."""
    return cast(structlog.stdlib.BoundLogger, structlog.get_logger(name))


# ─── Retry decorator ─────────────────────────────────────────────────────────
# Wraps any function with tenacity exponential backoff + structured logging
# of each retry. Used for HTTP calls, S3 writes, etc.


def _log_retry(retry_state: RetryCallState) -> None:
    """Log every retry attempt with full context."""
    log = get_logger("retry")
    fn_name = retry_state.fn.__name__ if retry_state.fn else "<unknown>"
    log.warning(
        "retry.attempt",
        function=fn_name,
        attempt=retry_state.attempt_number,
        wait_seconds=retry_state.next_action.sleep if retry_state.next_action else 0,
        exception=str(retry_state.outcome.exception()) if retry_state.outcome else None,
    )


def make_retrier(
    retryable_exceptions: tuple[type[BaseException], ...] = (Exception,),
) -> Retrying:
    """Build a Retrying object from pipeline config. Caller wraps their function."""
    cfg = load_pipeline_config().http.retry
    return Retrying(
        stop=stop_after_attempt(cfg.max_attempts),
        wait=wait_exponential(
            multiplier=cfg.multiplier,
            min=cfg.initial_wait_seconds,
            max=cfg.max_wait_seconds,
        ),
        retry=retry_if_exception_type(retryable_exceptions),
        before_sleep=_log_retry,
        reraise=True,
    )


# ─── S3 helpers ──────────────────────────────────────────────────────────────


def get_s3_client() -> Any:
    """Return a boto3 S3 client configured for MinIO/AWS via env settings.

    If `coverdrive_s3_endpoint` is empty, the kwarg is omitted entirely so
    boto3 uses its default AWS endpoint resolver. Tests rely on this:
    moto intercepts those default endpoints.
    """
    settings = get_settings()
    kwargs: dict[str, Any] = {
        "aws_access_key_id": settings.coverdrive_s3_access_key,
        "aws_secret_access_key": settings.coverdrive_s3_secret_key,
        "config": BotoConfig(signature_version="s3v4", s3={"addressing_style": "path"}),
        "use_ssl": settings.coverdrive_s3_use_ssl,
    }
    if settings.coverdrive_s3_endpoint:
        kwargs["endpoint_url"] = settings.coverdrive_s3_endpoint
    return boto3.client("s3", **kwargs)


def build_partition_path(
    layer: Literal["bronze", "silver"],
    table: str,
    ingestion_date: datetime | None = None,
) -> str:
    """Build the S3 key prefix for a partition.

    Returns Hive-style path: <layer>/<table>/ingestion_date=YYYY-MM-DD/data.parquet
    This format is recognized natively by DuckDB, dbt-duckdb, Athena, Spark.
    """
    cfg = load_pipeline_config().storage
    prefix = cfg.bronze_prefix if layer == "bronze" else cfg.silver_prefix
    date = (ingestion_date or datetime.now(UTC)).strftime("%Y-%m-%d")
    return f"{prefix}/{table}/{cfg.partition_key}={date}/data.parquet"


def s3_uri(key: str) -> str:
    """Convert a bucket-relative key into a full s3:// URI."""
    return f"s3://{get_settings().coverdrive_s3_bucket}/{key}"
