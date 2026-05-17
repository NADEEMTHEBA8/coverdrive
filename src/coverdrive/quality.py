"""Data quality gate between Silver and Gold.

Implemented with Pandera (DataFrameModel API) — chosen over Great Expectations
because Pandera schemas are typed, version-controlled Python, and unit-testable.
GE's JSON suite format is harder to refactor and harder to type-check.

The contract: this module either returns cleanly (Silver is publishable) or
raises a `QualityGateFailure`. Airflow's DAG halts on the exception — bad data
never reaches Gold.

Add a new check: declare a new field/validator on the appropriate schema.
Add a new table: add a new schema class and register it in `_SCHEMAS`.
"""

from __future__ import annotations

import io
import sys
from datetime import UTC, datetime
from typing import Final

import pandas as pd
import pandera.pandas as pa
from pandera.errors import SchemaError, SchemaErrors
from pandera.typing import Series

from coverdrive.utils import (
    build_partition_path,
    configure_logging,
    get_logger,
    get_s3_client,
    get_settings,
    load_pipeline_config,
)

log = get_logger(__name__)


class QualityGateFailure(RuntimeError):  # noqa: N818
    """Raised when one or more quality checks fail.

    Airflow's task-failure handler catches this and halts the DAG before
    Gold transformation runs. The message includes structured failure detail
    suitable for Slack/PagerDuty.
    """


# ─── Schemas ─────────────────────────────────────────────────────────────────
# Pandera DataFrameModels read like SQLAlchemy models. Each field carries its
# constraints; pa.dataframe_check decorates table-level invariants.
#
# Numeric thresholds come from pipeline.yaml so they're configurable per env
# (a CI smoke test might allow fewer rows than prod). We bind them at import
# via the helper functions below.


def _resolve_min_rows(production: int, fixtures: int | None) -> int:
    """Pick the threshold based on COVERDRIVE_USE_FIXTURES.

    Falls back to the production value when no fixtures override is set.
    This keeps the demo path (200-row sample) running without weakening
    the real production gates.
    """
    if get_settings().coverdrive_use_fixtures and fixtures is not None:
        return fixtures
    return production


def _batting_cfg() -> dict[str, float | int]:
    cfg = load_pipeline_config().quality.batting
    return {
        "runs_max": cfg.runs_max or 25000,
        "strike_rate_max": cfg.strike_rate_max or 500.0,
        "min_rows": _resolve_min_rows(cfg.min_rows, cfg.min_rows_fixtures),
        "max_null_ratio": cfg.max_null_ratio,
    }


def _bowling_cfg() -> dict[str, float | int]:
    cfg = load_pipeline_config().quality.bowling
    return {
        "wickets_max": cfg.wickets_max or 700,
        "economy_max": cfg.economy_max or 15.0,
        "min_rows": _resolve_min_rows(cfg.min_rows, cfg.min_rows_fixtures),
        "max_null_ratio": cfg.max_null_ratio,
    }


class BattingSilverSchema(pa.DataFrameModel):
    """Contract for Silver batting data — what Gold can rely on."""

    player: Series[str] = pa.Field(nullable=False, unique=False, str_length={"min_value": 1})
    country_tag: Series[str] = pa.Field(nullable=True)
    career_start_year: Series[pd.Int64Dtype] = pa.Field(
        ge=1860, le=datetime.now(UTC).year + 1, nullable=True
    )
    career_end_year: Series[pd.Int64Dtype] = pa.Field(
        ge=1860, le=datetime.now(UTC).year + 1, nullable=True
    )
    matches: Series[pd.Int64Dtype] = pa.Field(ge=0, nullable=True)
    innings: Series[pd.Int64Dtype] = pa.Field(ge=0, nullable=True)
    runs: Series[pd.Int64Dtype] = pa.Field(ge=0, le=_batting_cfg()["runs_max"], nullable=False)
    average: Series[pd.Float64Dtype] = pa.Field(ge=0, nullable=True)
    strike_rate: Series[pd.Float64Dtype] = pa.Field(
        ge=0, le=_batting_cfg()["strike_rate_max"], nullable=True
    )
    hundreds: Series[pd.Int64Dtype] = pa.Field(ge=0, nullable=True)
    fifties: Series[pd.Int64Dtype] = pa.Field(ge=0, nullable=True)

    class Config:
        strict = False  # Allow extra columns; we only enforce the contract.
        coerce = False  # Transform.py already casts. Re-coercing hides bugs.

    @pa.dataframe_check
    def career_span_valid(cls, df: pd.DataFrame) -> Series[bool]:  # type: ignore[misc]  # noqa: N805
        """end_year >= start_year when both are present."""
        if "career_start_year" not in df or "career_end_year" not in df:
            return pd.Series([True] * len(df))  # type: ignore[return-value]
        both = df["career_start_year"].notna() & df["career_end_year"].notna()
        valid = df.loc[both, "career_end_year"] >= df.loc[both, "career_start_year"]
        return pd.Series(True, index=df.index).where(~both, valid)  # type: ignore[return-value]


class BowlingSilverSchema(pa.DataFrameModel):
    """Contract for Silver bowling data."""

    player: Series[str] = pa.Field(nullable=False, str_length={"min_value": 1})
    country_tag: Series[str] = pa.Field(nullable=True)
    career_start_year: Series[pd.Int64Dtype] = pa.Field(
        ge=1860, le=datetime.now(UTC).year + 1, nullable=True
    )
    career_end_year: Series[pd.Int64Dtype] = pa.Field(
        ge=1860, le=datetime.now(UTC).year + 1, nullable=True
    )
    matches: Series[pd.Int64Dtype] = pa.Field(ge=0, nullable=True)
    innings: Series[pd.Int64Dtype] = pa.Field(ge=0, nullable=True)
    balls_bowled: Series[pd.Int64Dtype] = pa.Field(ge=0, nullable=True)
    runs_conceded: Series[pd.Int64Dtype] = pa.Field(ge=0, nullable=True)
    wickets: Series[pd.Int64Dtype] = pa.Field(
        gt=0, le=_bowling_cfg()["wickets_max"], nullable=False
    )
    bowling_average: Series[pd.Float64Dtype] = pa.Field(ge=0, nullable=True)
    economy_rate: Series[pd.Float64Dtype] = pa.Field(
        ge=0, le=_bowling_cfg()["economy_max"], nullable=True
    )
    bowling_strike_rate: Series[pd.Float64Dtype] = pa.Field(ge=0, nullable=True)

    class Config:
        strict = False
        coerce = False


# Registry: table name → (schema, config-fetcher)
_SCHEMAS: Final = {
    "batting": (BattingSilverSchema, _batting_cfg),
    "bowling": (BowlingSilverSchema, _bowling_cfg),
}


# ─── Volume / null checks (Pandera does column-level; these are table-level) ─


def _check_row_count(df: pd.DataFrame, table: str, min_rows: int) -> None:
    if len(df) < min_rows:
        raise QualityGateFailure(
            f"{table}: only {len(df)} rows, expected at least {min_rows}. "
            "Upstream scrape may have partially failed."
        )


# Columns that may be entirely absent from a given Bronze source (different
# ESPN pages return different stat shapes). When 100% null, the column is
# "optional" — exempt from the null-ratio gate. When partially null at a rate
# above the threshold, that's a real partial-scrape signal and we halt.
_OPTIONAL_WHEN_FULLY_NULL: Final = frozenset(
    {
        "fours",
        "sixes",
        "fours_is_lower_bound",
        "sixes_is_lower_bound",
        "high_score",
        "high_score_not_out",
        "country_tag",
        "career_start_year",
        "career_end_year",
    }
)


def _check_null_ratios(df: pd.DataFrame, table: str, max_ratio: float) -> None:
    null_ratios = df.isna().mean()
    failed = {}
    for col, ratio in null_ratios.items():
        if ratio <= max_ratio:
            continue
        # 100%-null in a known-optional column is allowed (column absent from source).
        if ratio == 1.0 and col in _OPTIONAL_WHEN_FULLY_NULL:
            continue
        failed[col] = ratio
    if failed:
        details = ", ".join(f"{col}={ratio:.1%}" for col, ratio in failed.items())
        raise QualityGateFailure(
            f"{table}: null ratio exceeds threshold {max_ratio:.1%} for columns: {details}"
        )


# ─── Driver ──────────────────────────────────────────────────────────────────


def validate_table(df: pd.DataFrame, table: str) -> None:
    """Validate one Silver table. Raises QualityGateFailure on any failure.

    Three layers of checks (cheapest first):
    1. Row count — quick volume sanity.
    2. Null ratios — column completeness.
    3. Pandera schema — type + per-row constraints, returns ALL violations.
    """
    if table not in _SCHEMAS:
        raise ValueError(f"No quality schema registered for table {table!r}")
    schema, get_cfg = _SCHEMAS[table]
    cfg = get_cfg()
    check_log = log.bind(table=table)

    check_log.info("quality.start", rows=len(df))
    _check_row_count(df, table, cfg["min_rows"])  # type: ignore[arg-type]
    _check_null_ratios(df, table, cfg["max_null_ratio"])

    try:
        schema.validate(df, lazy=True)  # lazy=True surfaces all failures, not first
    except (SchemaError, SchemaErrors) as e:
        check_log.error("quality.schema_failed", failure_cases=str(e))
        raise QualityGateFailure(f"{table}: schema validation failed:\n{e}") from e

    check_log.info("quality.passed", rows=len(df))


def _read_silver(table: str, ingestion_date: datetime | None) -> pd.DataFrame:
    settings = get_settings()
    key = build_partition_path("silver", table, ingestion_date)
    s3 = get_s3_client()
    response = s3.get_object(Bucket=settings.coverdrive_s3_bucket, Key=key)
    return pd.read_parquet(io.BytesIO(response["Body"].read()))


def run_quality_gate(ingestion_date: datetime | None = None) -> None:
    """Validate every Silver table for the given partition. Halts on failure."""
    ts = ingestion_date or datetime.now(UTC)
    failures: list[str] = []
    for table in _SCHEMAS:
        try:
            df = _read_silver(table, ingestion_date=ts)
            validate_table(df, table)
        except QualityGateFailure as e:
            failures.append(str(e))
            log.error("quality.gate.failure", table=table, error=str(e))

    if failures:
        joined = "\n---\n".join(failures)
        raise QualityGateFailure(f"Quality gate failed for {len(failures)} table(s):\n{joined}")
    log.info("quality.gate.passed_all", tables=list(_SCHEMAS))


def main() -> int:
    configure_logging()
    try:
        run_quality_gate()
    except QualityGateFailure:
        log.exception("quality.gate.halt_pipeline")
        return 2  # Distinguishable exit code for "data quality" vs "infra error"
    except Exception:
        log.exception("quality.unexpected_error")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
