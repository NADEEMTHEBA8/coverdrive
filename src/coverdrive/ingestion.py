"""Pull career stats from ESPNcricinfo, write to s3 as parquet."""
from __future__ import annotations

import io

import pandas as pd
import requests
from bs4 import BeautifulSoup

from coverdrive.utils import build_partition_path, get_logger, get_s3_client, get_settings

log = get_logger(__name__)


def fetch_page(url: str) -> str:
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.text


def parse_table(html: str, index: int = 2) -> pd.DataFrame:
    soup = BeautifulSoup(html, "lxml")
    tables = soup.find_all("table", class_="engineTable")
    return pd.read_html(io.StringIO(str(tables[index])))[0]


def write_bronze(df: pd.DataFrame, table: str) -> None:
    settings = get_settings()
    key = build_partition_path("bronze", table)
    buf = io.BytesIO()
    df.to_parquet(buf, compression="snappy")
    buf.seek(0)
    get_s3_client().put_object(
        Bucket=settings.coverdrive_s3_bucket, Key=key, Body=buf.getvalue()
    )
    log.info("bronze.written", table=table, rows=len(df), key=key)
