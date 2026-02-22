"""Tests for coverdrive.transform — pure-function unit tests, no S3 needed."""

from __future__ import annotations

import pandas as pd
import pytest

from coverdrive import transform


# ─── Generic helpers ─────────────────────────────────────────────────────────


def test_split_player_country() -> None:
    series = pd.Series(
        ["MS Dhoni (Asia/IND)", "SR Tendulkar (IND)", "Wasim Akram (PAK)"]
    )
    players, countries = transform._split_player_country(series)
    assert players.tolist() == ["ms dhoni", "sr tendulkar", "wasim akram"]
    assert countries.tolist() == ["asia/ind", "ind", "pak"]


def test_split_player_country_no_tag() -> None:
    series = pd.Series(["AB de Villiers", "Virat Kohli"])
    players, countries = transform._split_player_country(series)
    assert players.tolist() == ["ab de villiers", "virat kohli"]
    assert countries.isna().all()


def test_parse_span() -> None:
    series = pd.Series(["1996-2015", "2007-2022", "1989-2003"])
    start, end = transform._parse_span(series)
    assert start.tolist() == [1996, 2007, 1989]
    assert end.tolist() == [2015, 2022, 2003]


def test_parse_span_malformed_yields_nulls() -> None:
    series = pd.Series(["1996", "2007-", "bad"])
    start, end = transform._parse_span(series)
    assert start.isna().all()
    assert end.isna().all()


def test_strip_plus_suffix_flags_lower_bound() -> None:
    """ESPN uses '120+' to mean ≥120; we preserve as flag."""
    series = pd.Series(["120+", "85", "200+"])
    values, flags = transform._strip_plus_suffix(series)
    assert values.tolist() == [120, 85, 200]
    assert flags.tolist() == [True, False, True]


def test_strip_star_suffix_flags_not_out() -> None:
    """ESPN uses '264*' to mean 264 not out."""
    series = pd.Series(["264*", "200", "300*"])
    values, flags = transform._strip_star_suffix(series)
    assert values.tolist() == [264, 200, 300]
    assert flags.tolist() == [True, False, True]


# ─── End-to-end transform on fixture data ────────────────────────────────────


def test_transform_batting_produces_clean_schema(batting_csv: pd.DataFrame) -> None:
    out = transform.transform_batting(batting_csv)

    # Required columns present
    for col in ("player", "country_tag", "runs", "average", "matches"):
        assert col in out.columns

    # Player names lowercased
    assert out["player"].str.islower().all()

    # No nulls in critical fields
    assert out["player"].notna().all()
    assert out["runs"].notna().all()

    # Numeric types where expected
    assert pd.api.types.is_integer_dtype(out["runs"])
    assert pd.api.types.is_float_dtype(out["average"])


def test_transform_batting_dedupes_on_natural_key(batting_csv: pd.DataFrame) -> None:
    """Duplicating a row in input doesn't duplicate in output."""
    doubled = pd.concat([batting_csv, batting_csv], ignore_index=True)
    out = transform.transform_batting(doubled)
    # Composite key: player + career_start_year
    if "career_start_year" in out.columns:
        keys = list(zip(out["player"], out["career_start_year"], strict=True))
        assert len(keys) == len(set(keys))
    else:
        assert out["player"].is_unique


def test_transform_batting_idempotent(batting_csv: pd.DataFrame) -> None:
    """Running the transform twice yields identical output."""
    out1 = transform.transform_batting(batting_csv)
    out2 = transform.transform_batting(batting_csv)
    pd.testing.assert_frame_equal(out1, out2)


def test_transform_bowling_filters_zero_wickets(bowling_csv: pd.DataFrame) -> None:
    """Bowlers with 0 wickets are dropped — no signal for ranking."""
    out = transform.transform_bowling(bowling_csv)
    if "wickets" in out.columns:
        assert (out["wickets"] > 0).all()


def test_transform_bowling_extracts_country(bowling_csv: pd.DataFrame) -> None:
    """Country tag is extracted from the player string."""
    out = transform.transform_bowling(bowling_csv)
    # At least some of our sample rows have country tags
    assert out["country_tag"].notna().sum() > 0


def test_transform_handles_mixed_special_chars() -> None:
    """A synthetic row with both '+' and '*' edge cases passes through cleanly."""
    df = pd.DataFrame(
        {
            "Player": ["test player (test)"],
            "Span": ["2000-2020"],
            "Mat": [100],
            "Inns": [90],
            "NO": [5],
            "Runs": [4500],
            "HS": ["150*"],
            "Ave": [50.0],
            "BF": [5000],
            "SR": [90.0],
            "100": [10],
            "50": [25],
            "0": [3],
            "4s": ["400+"],
            "6s": ["80"],
        }
    )
    out = transform.transform_batting(df)
    assert out["high_score"].iloc[0] == 150
    assert bool(out["high_score_not_out"].iloc[0])
    assert out["fours"].iloc[0] == 400
    assert bool(out["fours_is_lower_bound"].iloc[0])
    assert out["sixes"].iloc[0] == 80
    assert not bool(out["sixes_is_lower_bound"].iloc[0])
