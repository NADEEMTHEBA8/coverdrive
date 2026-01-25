"""Pull career stats from ESPNcricinfo, write to s3 (minio locally) as parquet."""
from __future__ import annotations

import io
import os

import boto3
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


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["COVERDRIVE_S3_ENDPOINT"],
        aws_access_key_id=os.environ["COVERDRIVE_S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["COVERDRIVE_S3_SECRET_KEY"],
    )


def write_parquet(df: pd.DataFrame, key: str) -> None:
    buf = io.BytesIO()
    df.to_parquet(buf, compression="snappy")
    buf.seek(0)
    s3_client().put_object(
        Bucket=os.environ["COVERDRIVE_S3_BUCKET"], Key=key, Body=buf.getvalue()
    )


def main() -> None:
    df = parse_table(fetch_page(BATTING_URL))
    write_parquet(df, "bronze/batting/data.parquet")
    print(f"wrote {len(df)} rows to s3")


if __name__ == "__main__":
    main()
