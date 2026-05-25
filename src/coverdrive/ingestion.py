"""Bronze ingestion: scrape ESPNcricinfo HTML tables into partitioned Parquet on S3."""

from __future__ import annotations

import argparse
import io
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Final

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from coverdrive.utils import (
    PipelineConfig,
    build_partition_path,
    configure_logging,
    get_logger,
    get_s3_client,
    get_settings,
    load_pipeline_config,
    make_retrier,
)

log = get_logger(__name__)

# HTTP errors worth retrying. 4xx (except 429) won't change on retry.
RETRYABLE_HTTP_ERRORS: Final = (
    requests.ConnectionError,
    requests.Timeout,
    requests.HTTPError,
)

# pandas.read_html returns multiple tables per page; ESPN's results table is index 2.
# Documented here so a future upstream change is one line to fix.
ESPN_RESULTS_TABLE_INDEX: Final = 2


# ─── Scrape ──────────────────────────────────────────────────────────────────


def _fetch_page(url: str, params: dict[str, str | int], cfg: PipelineConfig) -> str:
    """Fetch a single HTML page. Single attempt — caller wraps with retry."""
    response = requests.get(
        url,
        params=params,
        headers={"User-Agent": cfg.http.user_agent},
        timeout=cfg.http.timeout_seconds,
    )
    response.raise_for_status()
    return response.text


def _parse_html_table(html: str, table_index: int = ESPN_RESULTS_TABLE_INDEX) -> pd.DataFrame:
    """Extract the cricket stats table from an ESPN results page."""
    tables = pd.read_html(io.StringIO(html), flavor="lxml")
    if len(tables) <= table_index:
        raise ValueError(
            f"Expected at least {table_index + 1} tables on page, got {len(tables)}. "
            "ESPNcricinfo HTML structure may have changed — review _parse_html_table."
        )
    return tables[table_index]


def scrape_table(source_name: str, cfg: PipelineConfig) -> pd.DataFrame:
    """Scrape a paginated ESPN source. Returns concatenated raw rows."""
    source = cfg.sources[source_name]
    retrier = make_retrier(RETRYABLE_HTTP_ERRORS)
    log.info("scrape.start", source=source_name, pages=source.pages_to_fetch)

    frames: list[pd.DataFrame] = []
    for page in range(1, source.pages_to_fetch + 1):
        # ESPN paginates via the `page` parameter; results are 200/page.
        params: dict[str, str | int] = {**source.params, "page": page}
        page_log = log.bind(source=source_name, page=page)

        # tenacity retries the whole closure on retryable failures
        for attempt in retrier:
            with attempt:
                html = _fetch_page(source.base_url, params, cfg)
                df = _parse_html_table(html)

        if df.empty:
            page_log.info("scrape.page_empty_stop")
            break
        frames.append(df)
        page_log.info("scrape.page_ok", rows=len(df))

        if not frames:
            raise RuntimeError(f"No rows scraped for source {source_name!r}")

    combined = pd.concat(frames, ignore_index=True)

    # Coerce all columns to numeric where possible; leave strings as-is
    for col in combined.columns:
        if combined[col].dtype == object:
            combined[col] = pd.to_numeric(
                combined[col],
                errors="coerce",
            )

    log.info("scrape.complete", source=source_name, total_rows=len(combined))
    return combined


# ─── Fixture mode ────────────────────────────────────────────────────────────


def load_from_fixtures(source_name: str, fixtures_dir: Path) -> pd.DataFrame:
    """Load a source from its CSV fixture. Used in CI and offline development."""
    fixture_path = fixtures_dir / f"{source_name}_sample.csv"
    if not fixture_path.exists():
        raise FileNotFoundError(f"Fixture not found: {fixture_path}")
    df = pd.read_csv(fixture_path)
    # Drop unnamed index columns commonly present in exported CSVs
    df = df.loc[:, ~df.columns.str.startswith("Unnamed")]
    log.info("fixtures.loaded", source=source_name, rows=len(df), path=str(fixture_path))
    return df


# ─── Bronze write ────────────────────────────────────────────────────────────


def write_bronze(
    df: pd.DataFrame,
    table: str,
    ingestion_date: datetime | None = None,
) -> str:
    """Write a DataFrame as Parquet to the Bronze partition for `table`.

    Returns the s3:// URI of the written object.
    Overwrites the partition — idempotent on the (table, ingestion_date) key.
    """
    cfg = load_pipeline_config()
    settings = get_settings()
    key = build_partition_path("bronze", table, ingestion_date)
    write_log = log.bind(table=table, key=key, rows=len(df))

    table_pa = pa.Table.from_pandas(df)

    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
        with pq.ParquetWriter(
            tmp.name, table_pa.schema, compression=cfg.storage.compression
        ) as writer:
            # Chunking simulates processing large data volumes
            for batch in table_pa.to_batches(max_chunksize=10000):
                writer.write_batch(batch)

        tmp.seek(0)
        s3 = get_s3_client()
        s3.upload_fileobj(
            Fileobj=tmp,
            Bucket=settings.coverdrive_s3_bucket,
            Key=key,
            ExtraArgs={"ContentType": "application/octet-stream"},
        )
        file_size = Path(tmp.name).stat().st_size

    uri = f"s3://{settings.coverdrive_s3_bucket}/{key}"
    write_log.info("bronze.written", uri=uri, bytes=file_size)
    return uri


# ─── Orchestration ───────────────────────────────────────────────────────────


def run_ingestion(
    mode: str,
    fixtures_dir: Path = Path("tests/fixtures"),
    ingestion_date: datetime | None = None,
) -> dict[str, str]:
    """Run ingestion for every configured source. Returns {table: written_uri}."""
    cfg = load_pipeline_config()
    ts = ingestion_date or datetime.now(UTC)
    written: dict[str, str] = {}

    for source_name in cfg.sources:
        source_log = log.bind(source=source_name, mode=mode)
        source_log.info("ingest.start")
        try:
            if mode == "scrape":
                df = scrape_table(source_name, cfg)
            elif mode == "fixtures":
                df = load_from_fixtures(source_name, fixtures_dir)
            else:
                raise ValueError(f"Unknown mode: {mode!r}")
            written[source_name] = write_bronze(df, source_name, ingestion_date=ts)
            source_log.info("ingest.success")
        except Exception:
            source_log.exception("ingest.failed")
            raise

    log.info("ingest.all_sources_complete", count=len(written))
    return written


def main() -> int:
    """CLI entrypoint: `python -m coverdrive.ingestion --mode=scrape`."""
    parser = argparse.ArgumentParser(description="Run Coverdrive Bronze ingestion")
    parser.add_argument(
        "--mode",
        choices=["scrape", "fixtures"],
        default="scrape",
        help="scrape: live HTTP from ESPNcricinfo. fixtures: load test CSVs.",
    )
    parser.add_argument(
        "--ingestion-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=UTC),
        default=None,
        help="Override the partition date (YYYY-MM-DD). Defaults to today UTC.",
    )
    args = parser.parse_args()

    configure_logging()
    try:
        run_ingestion(mode=args.mode, ingestion_date=args.ingestion_date)
    except Exception:
        log.exception("ingestion.fatal")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
