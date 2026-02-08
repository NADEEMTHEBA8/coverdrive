"""Bronze parquet -> Silver parquet. Pure functions, no I/O."""
from __future__ import annotations

import re

import pandas as pd


PLAYER_TAG_RE = re.compile(r"^(.*?)\s*\(([^)]+)\)\s*$")
SPAN_RE = re.compile(r"^(\d{4})-(\d{4})$")


def split_player_country(s: pd.Series) -> tuple[pd.Series, pd.Series]:
    extracted = s.fillna("").astype(str).str.extract(PLAYER_TAG_RE)
    players = extracted[0].str.strip().str.lower().replace("", pd.NA).astype("string")
    countries = extracted[1].str.strip().astype("string")
    no_tag = players.isna() & s.notna()
    players = players.where(~no_tag, s.astype(str).str.strip().str.lower())
    return players, countries


def parse_span(s: pd.Series) -> tuple[pd.Series, pd.Series]:
    parts = s.astype(str).str.extract(SPAN_RE)
    start = pd.to_numeric(parts[0], errors="coerce").astype("Int64")
    end = pd.to_numeric(parts[1], errors="coerce").astype("Int64")
    return start, end


def transform_batting(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df.loc[:, ~df.columns.astype(str).str.startswith("Unnamed")]
    rename_map = {
        "Mat": "matches", "Inns": "innings", "NO": "not_outs",
        "Runs": "runs", "Ave": "average", "SR": "strike_rate",
        "100": "hundreds", "50": "fifties", "0": "ducks",
        "Player": "player_raw", "Span": "span_raw",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
    out = pd.DataFrame()
    out["player"], out["country_tag"] = split_player_country(df["player_raw"])
    out["career_start_year"], out["career_end_year"] = parse_span(df["span_raw"])
    for col in ("matches", "innings", "not_outs", "runs", "hundreds", "fifties", "ducks"):
        if col in df.columns:
            out[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for col in ("average", "strike_rate"):
        if col in df.columns:
            out[col] = pd.to_numeric(df[col], errors="coerce").astype("Float64")
    return out.dropna(subset=["player", "runs"])


HS_STAR_RE = re.compile(r"^(\d+)(\*?)$")


def strip_hs_star(s: pd.Series) -> tuple[pd.Series, pd.Series]:
    """espn appends '*' to high scores made not-out. split into (value, flag)."""
    extracted = s.astype(str).str.extract(HS_STAR_RE)
    value = pd.to_numeric(extracted[0], errors="coerce").astype("Int64")
    not_out = extracted[1].fillna("").eq("*")
    return value, not_out
