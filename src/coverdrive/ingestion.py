"""Pull career stats from ESPNcricinfo and dump to parquet.

Prototype — no retries, no config. Just want to see if I can get the data.
"""
from __future__ import annotations

import io
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup


BATTING_URL = (
    "https://stats.espncricinfo.com/ci/engine/stats/index.html"
    "?class=2;type=batting;template=results;size=200"
)


def fetch_page(url: str) -> str:
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.text


def parse_table(html: str) -> pd.DataFrame:
    soup = BeautifulSoup(html, "lxml")
    tables = soup.find_all("table", class_="engineTable")
    return pd.read_html(io.StringIO(str(tables[2])))[0]


def main() -> None:
    out = Path("data/bronze/batting")
    out.mkdir(parents=True, exist_ok=True)
    df = parse_table(fetch_page(BATTING_URL))
    df.to_parquet(out / "data.parquet")
    print(f"wrote {len(df)} rows")


if __name__ == "__main__":
    main()
