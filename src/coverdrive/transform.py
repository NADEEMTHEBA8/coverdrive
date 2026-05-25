"""Silver transformations: clean Bronze raw scrapes into a conformed schema.

Cleaning steps applied (in order):
1. Drop pandas index artifacts.
2. Lowercase strings for case-insensitive joining.
3. Strip the country tag from player names ("MS Dhoni (Asia/IND)" → "ms dhoni",
   "ind").
4. Strip special characters that block numeric casting:
   * `'+'` in 4s/6s columns (means "more than this many" — preserved as flag)
   * `'*'` in HS column (means "not out" — preserved as boolean column)
5. Cast numerics to int64 / float64.
6. Parse the Span column ("1996-2015") into start_year / end_year integers.
7. Drop rows with critical nulls (no player name, no runs).
8. Deduplicate on the natural key (player + span).
"""

from __future__ import annotations

import io
import re
import sys
from datetime import UTC, datetime

import pandas as pd

from coverdrive.utils import (
    build_partition_path,
    configure_logging,
    get_logger,
    get_s3_client,
    get_settings,
    load_pipeline_config,
)

log = get_logger(__name__)

# Regexes compiled once at module load — cheap, but explicit beats implicit.
_COUNTRY_TAG_PATTERN = re.compile(r"\s*\(([^)]+)\)\s*$")
_SPAN_PATTERN = re.compile(r"^(\d{4})-(\d{4})$")


# ─── Generic helpers ─────────────────────────────────────────────────────────


def _split_player_country(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Split 'Player (CountryTags)' into (player_clean, country_tag).

    The country column in raw ESPN data is embedded in the player string,
    e.g. 'MS Dhoni (Asia/IND)'. We extract the bracketed value verbatim;
    parsing 'Asia/IND' into country aliases is a Gold-layer concern.
    """
    extracted = series.astype("string").str.extract(_COUNTRY_TAG_PATTERN, expand=False)
    cleaned = series.astype("string").str.replace(_COUNTRY_TAG_PATTERN, "", regex=True).str.strip()
    return cleaned.str.lower(), extracted.str.lower()


def _parse_span(span: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Parse 'YYYY-YYYY' span strings into two Int64 columns."""
    match = span.astype("string").str.extract(_SPAN_PATTERN)
    return (
        pd.to_numeric(match[0], errors="coerce").astype("Int64"),
        pd.to_numeric(match[1], errors="coerce").astype("Int64"),
    )


def _strip_plus_suffix(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    """ESPN encodes "more than N fours/sixes" as e.g. '120+'.

    Returns (numeric_value, is_lower_bound_flag).
    """
    raw = series.astype("string")
    flag = raw.str.endswith("+", na=False)
    numeric = pd.to_numeric(raw.str.rstrip("+"), errors="coerce").astype("Int64")
    return numeric, flag


def _strip_star_suffix(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    """ESPN encodes "not out" in HighScore as e.g. '264*'.

    Returns (numeric_high_score, is_not_out_flag).
    """
    raw = series.astype("string")
    flag = raw.str.endswith("*", na=False)
    numeric = pd.to_numeric(raw.str.rstrip("*"), errors="coerce").astype("Int64")
    return numeric, flag


# ─── Per-source transformers ─────────────────────────────────────────────────
# Each builds a clean, typed DataFrame conforming to the Silver schema.
# The Pandera schemas in coverdrive.quality enforce the output contract.


def transform_batting(df: pd.DataFrame) -> pd.DataFrame:
    """Bronze batting → Silver batting. Idempotent on its input."""
    df = df.copy()
    # Drop index column variants
    df = df.loc[:, ~df.columns.astype(str).str.startswith("Unnamed")]

    # Handle whichever column-name convention this Bronze write used.
    # The dissertation has two — early scrapes used short ESPN names,
    # later runs renamed them. We normalize.
    rename_map = {
        "Mat": "matches",
        "Inns": "innings",
        "NO": "not_outs",
        "BF": "balls_faced",
        "Ave": "average",
        "SR": "strike_rate",
        "100": "hundreds",
        "50": "fifties",
        "0": "ducks",
        "4s": "fours_raw",
        "6s": "sixes_raw",
        "HS": "high_score_raw",
        "Player": "player_raw",
        "Span": "span_raw",
        "Runs": "runs",
        # Already-renamed inputs pass through:
        "Matches": "matches",
        "Innings": "innings",
        "NotOuts": "not_outs",
        "BallsFaced": "balls_faced",
        "Average": "average",
        "StrikeRate": "strike_rate",
        "Hundreds": "hundreds",
        "Fifties": "fifties",
        "Ducks": "ducks",
        # Pre-split career-span convention:
        "Start": "career_start_raw",
        "End": "career_end_raw",
        # Pre-split country convention (some scrapes embed it in Player; some don't):
        "Country": "country_explicit",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    out = pd.DataFrame()
    out["player"], parsed_country = (
        _split_player_country(df["player_raw"])
        if "player_raw" in df.columns
        else _split_player_country(df["Player"])
    )
    # Prefer an explicit Country column if the input has one; fall back to parsed.
    if "country_explicit" in df.columns:
        out["country_tag"] = df["country_explicit"].astype("string")
    else:
        out["country_tag"] = parsed_country

    # Career span: parse from "1989-2013" string if present, else use pre-split Start/End.
    if "span_raw" in df.columns:
        out["career_start_year"], out["career_end_year"] = _parse_span(df["span_raw"])
    else:
        out["career_start_year"] = (
            pd.to_numeric(df["career_start_raw"], errors="coerce").astype("Int64")
            if "career_start_raw" in df.columns
            else pd.Series([pd.NA] * len(df), dtype="Int64")
        )
        out["career_end_year"] = (
            pd.to_numeric(df["career_end_raw"], errors="coerce").astype("Int64")
            if "career_end_raw" in df.columns
            else pd.Series([pd.NA] * len(df), dtype="Int64")
        )

    # Numeric columns: prefer already-numeric over re-parsing
    for col in (
        "matches",
        "innings",
        "not_outs",
        "runs",
        "balls_faced",
        "hundreds",
        "fifties",
        "ducks",
    ):
        if col in df.columns:
            out[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for col in ("average", "strike_rate"):
        if col in df.columns:
            out[col] = pd.to_numeric(df[col], errors="coerce").astype("Float64")

    # Special-character columns
    if "fours_raw" in df.columns:
        out["fours"], out["fours_is_lower_bound"] = _strip_plus_suffix(df["fours_raw"])
    if "sixes_raw" in df.columns:
        out["sixes"], out["sixes_is_lower_bound"] = _strip_plus_suffix(df["sixes_raw"])
    if "high_score_raw" in df.columns:
        out["high_score"], out["high_score_not_out"] = _strip_star_suffix(df["high_score_raw"])

    # ─── Schema stabilization ──────────────────────────────────────────
    # Always emit the full Silver-batting contract, regardless of which Bronze
    # columns were available. Missing numeric columns become typed NA, missing
    # boolean flags become False. This makes the Silver schema input-independent
    # so downstream (dbt staging, Pandera gates) can rely on a stable column
    # set — see ADR-001's "data lineage" lesson.
    INT_COLS = (
        "matches",
        "innings",
        "not_outs",
        "runs",
        "balls_faced",
        "hundreds",
        "fifties",
        "ducks",
        "fours",
        "sixes",
        "high_score",
    )
    FLOAT_COLS = ("average", "strike_rate")
    BOOL_COLS = ("fours_is_lower_bound", "sixes_is_lower_bound", "high_score_not_out")
    STRING_COLS = ("country_tag",)

    for col in INT_COLS:
        if col not in out.columns:
            out[col] = pd.Series([pd.NA] * len(out), dtype="Int64")
    for col in FLOAT_COLS:
        if col not in out.columns:
            out[col] = pd.Series([pd.NA] * len(out), dtype="Float64")
    for col in BOOL_COLS:
        if col not in out.columns:
            out[col] = pd.Series([False] * len(out), dtype="boolean")
    for col in STRING_COLS:
        if col not in out.columns:
            out[col] = pd.Series([pd.NA] * len(out), dtype="string")

    # Quality filters: require player name and at least one run scored.
    # Players with zero recorded runs add no analytic value.
    valid_mask = out["player"].notna() & out["runs"].notna()
    dropped_rows = len(out) - valid_mask.sum()
    if dropped_rows > 0:
        log.warning("transform.batting.dropped_nulls", dropped_count=dropped_rows)
    out = out[valid_mask]

    # Natural-key dedup. Multiple ESPN scrapes on the same day can produce
    # duplicates if pagination overlapped — keep the row with most matches.
    out = (
        out.sort_values("matches", ascending=False, na_position="last")
        .drop_duplicates(subset=["player", "career_start_year"], keep="first")
        .reset_index(drop=True)
    )

    log.info("transform.batting.complete", rows_in=len(df), rows_out=len(out))
    return out


def transform_bowling(df: pd.DataFrame) -> pd.DataFrame:
    """Bronze bowling → Silver bowling. Idempotent on its input."""
    df = df.copy()
    df = df.loc[:, ~df.columns.astype(str).str.startswith("Unnamed")]

    rename_map = {
        "Mat": "matches",
        "Inns": "innings",
        "Balls": "balls_bowled",
        "Runs": "runs_conceded",
        "Wkts": "wickets",
        "Ave": "bowling_average",
        "Econ": "economy_rate",
        "SR": "bowling_strike_rate",
        "BBI": "best_bowling_innings",
        "Player": "player_raw",
        "Span": "span_raw",
        "4": "four_wicket_hauls",
        "5": "five_wicket_hauls",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    out = pd.DataFrame()
    out["player"], out["country_tag"] = _split_player_country(
        df["player_raw"] if "player_raw" in df.columns else df["Player"]
    )
    if "span_raw" in df.columns:
        out["career_start_year"], out["career_end_year"] = _parse_span(df["span_raw"])

    for col in (
        "matches",
        "innings",
        "balls_bowled",
        "runs_conceded",
        "wickets",
        "four_wicket_hauls",
        "five_wicket_hauls",
    ):
        if col in df.columns:
            out[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for col in ("bowling_average", "economy_rate", "bowling_strike_rate"):
        if col in df.columns:
            out[col] = pd.to_numeric(df[col], errors="coerce").astype("Float64")

    if "best_bowling_innings" in df.columns:
        out["best_bowling_innings"] = df["best_bowling_innings"].astype("string")

    # Bowlers with 0 wickets or 0 balls bowled contribute no signal.
    valid_mask = out["player"].notna() & out["wickets"].notna() & (out["wickets"] > 0)
    dropped_rows = len(out) - valid_mask.sum()
    if dropped_rows > 0:
        log.warning("transform.bowling.dropped_invalid", dropped_count=dropped_rows)
    out = out[valid_mask]

    out = (
        out.sort_values("matches", ascending=False, na_position="last")
        .drop_duplicates(subset=["player", "career_start_year"], keep="first")
        .reset_index(drop=True)
    )

    log.info("transform.bowling.complete", rows_in=len(df), rows_out=len(out))
    return out


# ─── Bronze read / Silver write ──────────────────────────────────────────────


def read_bronze(table: str, ingestion_date: datetime | None = None) -> pd.DataFrame:
    """Read a Bronze partition as a DataFrame."""
    settings = get_settings()
    key = build_partition_path("bronze", table, ingestion_date)
    s3 = get_s3_client()
    response = s3.get_object(Bucket=settings.coverdrive_s3_bucket, Key=key)
    df = pd.read_parquet(io.BytesIO(response["Body"].read()))
    log.info("bronze.read", table=table, key=key, rows=len(df))
    return df


def write_silver(df: pd.DataFrame, table: str, ingestion_date: datetime | None = None) -> str:
    """Write a transformed DataFrame to the Silver partition."""
    cfg = load_pipeline_config()
    settings = get_settings()
    key = build_partition_path("silver", table, ingestion_date)

    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", compression=cfg.storage.compression, index=False)
    buffer.seek(0)

    s3 = get_s3_client()
    s3.put_object(
        Bucket=settings.coverdrive_s3_bucket,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )

    uri = f"s3://{settings.coverdrive_s3_bucket}/{key}"
    log.info("silver.written", table=table, uri=uri, rows=len(df))
    return uri


# ─── Driver ──────────────────────────────────────────────────────────────────

# Dispatch table — adding a new source means one entry, not a new branch.
_TRANSFORMERS = {
    "batting": transform_batting,
    "bowling": transform_bowling,
}


def run_transform(ingestion_date: datetime | None = None) -> dict[str, str]:
    """Bronze → Silver for every configured source."""
    cfg = load_pipeline_config()
    written: dict[str, str] = {}
    ts = ingestion_date or datetime.now(UTC)

    for source_name in cfg.sources:
        if source_name not in _TRANSFORMERS:
            log.warning("transform.no_transformer", source=source_name)
            continue
        with_log = log.bind(source=source_name)
        with_log.info("transform.start")
        bronze_df = read_bronze(source_name, ingestion_date=ts)
        silver_df = _TRANSFORMERS[source_name](bronze_df)
        written[source_name] = write_silver(silver_df, source_name, ingestion_date=ts)
        with_log.info("transform.complete")

    return written


def main() -> int:
    configure_logging()
    try:
        run_transform()
    except Exception:
        log.exception("transform.fatal")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
