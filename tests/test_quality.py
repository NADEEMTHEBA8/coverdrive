"""Tests for coverdrive.quality — Pandera schemas and gate semantics."""

from __future__ import annotations

import pandas as pd
import pytest

from coverdrive import quality, transform
from coverdrive.quality import (
    BattingSilverSchema,
    BowlingSilverSchema,
    QualityGateFailure,
    validate_table,
)


# ─── Happy path: real fixture data passes ─────────────────────────────────────


def test_validate_batting_passes_on_clean_fixture(batting_csv: pd.DataFrame) -> None:
    """A transformed fixture should clear the quality gate."""
    silver = transform.transform_batting(batting_csv)
    # Fixture is 100 rows; relax min_rows for the test by validating schema only.
    BattingSilverSchema.validate(silver, lazy=True)


def test_validate_bowling_passes_on_clean_fixture(bowling_csv: pd.DataFrame) -> None:
    silver = transform.transform_bowling(bowling_csv)
    BowlingSilverSchema.validate(silver, lazy=True)


# ─── Schema-level failures ────────────────────────────────────────────────────


def test_schema_rejects_negative_runs(batting_csv: pd.DataFrame) -> None:
    """Pandera's ge=0 constraint catches impossible values."""
    silver = transform.transform_batting(batting_csv)
    silver.loc[0, "runs"] = -100
    with pytest.raises(Exception, match="runs"):
        BattingSilverSchema.validate(silver, lazy=True)


def test_schema_rejects_runs_above_ceiling(batting_csv: pd.DataFrame) -> None:
    """A run total above the Tendulkar ceiling is a scrape error, not a record."""
    silver = transform.transform_batting(batting_csv)
    silver.loc[0, "runs"] = 99999
    with pytest.raises(Exception, match="runs"):
        BattingSilverSchema.validate(silver, lazy=True)


def test_schema_rejects_null_player(batting_csv: pd.DataFrame) -> None:
    """Player is the natural key — never nullable."""
    silver = transform.transform_batting(batting_csv)
    silver.loc[0, "player"] = None
    with pytest.raises(Exception):  # noqa: B017, PT011
        BattingSilverSchema.validate(silver, lazy=True)


def test_schema_rejects_invalid_career_span(batting_csv: pd.DataFrame) -> None:
    """end_year < start_year is impossible — catches data-entry errors at the source."""
    silver = transform.transform_batting(batting_csv).copy()
    silver["career_start_year"] = pd.array([2020] * len(silver), dtype="Int64")
    silver["career_end_year"] = pd.array([2000] * len(silver), dtype="Int64")
    with pytest.raises(Exception):  # noqa: B017, PT011
        BattingSilverSchema.validate(silver, lazy=True)


# ─── Table-level checks (row count, null ratio) ──────────────────────────────


def test_row_count_check_fails_below_threshold() -> None:
    """An empty DataFrame trips the min_rows check before schema runs."""
    df = pd.DataFrame({"player": [], "runs": []})
    with pytest.raises(QualityGateFailure, match="rows"):
        quality._check_row_count(df, "batting", min_rows=100)


def test_null_ratio_check_fails_above_threshold() -> None:
    """If >5% of values in a column are null, halt."""
    df = pd.DataFrame({"player": ["a", "b"] + [None] * 18, "runs": list(range(20))})
    with pytest.raises(QualityGateFailure, match="null ratio"):
        quality._check_null_ratios(df, "batting", max_ratio=0.05)


def test_null_ratio_check_passes_when_below_threshold() -> None:
    """A small amount of nullity is acceptable."""
    df = pd.DataFrame({"player": ["a"] * 95 + [None] * 5, "runs": [10] * 100})
    # Should not raise
    quality._check_null_ratios(df, "batting", max_ratio=0.10)


# ─── Exit code semantics ──────────────────────────────────────────────────────
# Distinct exit codes let the orchestrator distinguish "bad data" from
# "bug in code". DAG alerting routes them differently.


def test_quality_failure_exception_is_distinguishable() -> None:
    """QualityGateFailure must not be the same class as other RuntimeError types."""
    err = QualityGateFailure("test")
    assert isinstance(err, RuntimeError)  # for broad catches
    assert type(err) is QualityGateFailure  # but specifically catchable


def test_validate_table_unknown_table_raises() -> None:
    """A typo in the table name shouldn't silently skip validation."""
    df = pd.DataFrame()
    with pytest.raises(ValueError, match="No quality schema"):
        validate_table(df, "not_a_table")
