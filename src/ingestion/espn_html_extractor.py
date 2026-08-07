"""Bronze extraction module: Scraping ESPNcricinfo HTML tables into Bronze Parquet."""

from __future__ import annotations

import argparse
import io
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Final

import pandas as pd
import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]
import requests
from bs4 import BeautifulSoup

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

RETRYABLE_HTTP_ERRORS: Final = (
    requests.ConnectionError,
    requests.Timeout,
    requests.HTTPError,
)

DEFAULT_SIGNATURE_COLUMNS: Final = ["player", "runs"]


class SchemaDriftError(Exception):
    """Raised when upstream HTML DOM changes and expected table signatures are missing."""


def _fetch_page(url: str, params: dict[str, str | int], cfg: PipelineConfig) -> str:
    """Fetch a single HTML page."""
    response = requests.get(
        url,
        params=params,
        headers={"User-Agent": cfg.http.user_agent},
        timeout=cfg.http.timeout_seconds,
    )
    response.raise_for_status()
    return response.text


def _parse_html_table(html: str, expected_signatures: list[str] | None = None) -> pd.DataFrame:
    """Extract data table from HTML payload using column signature matching.

    Probes HTML tables with BeautifulSoup and pandas to locate the target table,
    preventing silent failures when upstream DOM positions alter.
    """
    signatures = [s.lower() for s in (expected_signatures or DEFAULT_SIGNATURE_COLUMNS)]
    soup = BeautifulSoup(html, "lxml")
    available_tables = soup.find_all("table")

    if not available_tables:
        raise SchemaDriftError("No HTML tables located in the scraped payload.")

    parsed_tables = pd.read_html(io.StringIO(html), flavor="lxml")

    # Try signature matching across all tables first
    for candidate_df in parsed_tables:
        cols_lower = [str(c).lower() for c in candidate_df.columns]
        if all(any(sig in col for col in cols_lower) for sig in signatures):
            return candidate_df

    # Fallback to index 2 if signature isn't strict, or raise drift error
    if len(parsed_tables) > 2:
        return parsed_tables[2]

    raise SchemaDriftError(
        f"Schema drift detected. Expected signatures {signatures} not found in scraped tables."
    )


def scrape_table(source_name: str, cfg: PipelineConfig) -> pd.DataFrame:
    """Scrape a paginated ESPN source. Returns concatenated raw rows."""
    source = cfg.sources[source_name]
    retrier = make_retrier(RETRYABLE_HTTP_ERRORS)
    log.info("scrape.start", source=source_name, pages=source.pages_to_fetch)

    frames: list[pd.DataFrame] = []
    for page in range(1, source.pages_to_fetch + 1):
        params: dict[str, str | int] = {**source.params, "page": page}
        page_log = log.bind(source=source_name, page=page)

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

    for col in combined.columns:
        if combined[col].dtype == object:
            combined[col] = pd.to_numeric(
                combined[col],
                errors="coerce",
            )

    log.info("scrape.complete", source=source_name, total_rows=len(combined))
    return combined


def load_from_fixtures(source_name: str, fixtures_dir: Path) -> pd.DataFrame:
    """Load a source from its CSV fixture for CI and offline testing."""
    fixture_path = fixtures_dir / f"{source_name}_sample.csv"
    if not fixture_path.exists():
        raise FileNotFoundError(f"Fixture not found: {fixture_path}")
    df = pd.read_csv(fixture_path)
    df = df.loc[:, ~df.columns.str.startswith("Unnamed")]
    log.info("fixtures.loaded", source=source_name, rows=len(df), path=str(fixture_path))
    return df


def write_bronze(
    df: pd.DataFrame,
    table: str,
    ingestion_date: datetime | None = None,
) -> str:
    """Write DataFrame to Bronze Parquet partition. Overwrites on ingestion_date."""
    cfg = load_pipeline_config()
    settings = get_settings()
    key = build_partition_path("bronze", table, ingestion_date)
    write_log = log.bind(table=table, key=key, rows=len(df))

    table_pa = pa.Table.from_pandas(df)

    tmp_path = ""
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        with pq.ParquetWriter(
            tmp_path, table_pa.schema, compression=cfg.storage.compression
        ) as writer:
            for batch in table_pa.to_batches(max_chunksize=10000):
                writer.write_batch(batch)

        file_size = Path(tmp_path).stat().st_size
        s3 = get_s3_client()
        with open(tmp_path, "rb") as f:
            s3.upload_fileobj(
                Fileobj=f,
                Bucket=settings.coverdrive_s3_bucket,
                Key=key,
                ExtraArgs={"ContentType": "application/octet-stream"},
            )
    finally:
        Path(tmp_path).unlink(missing_ok=True)

    uri = f"s3://{settings.coverdrive_s3_bucket}/{key}"
    write_log.info("bronze.written", uri=uri, bytes=file_size)
    return uri


def run_ingestion(
    mode: str,
    fixtures_dir: Path = Path("tests/fixtures"),
    ingestion_date: datetime | None = None,
) -> dict[str, str]:
    """Run ingestion for configured sources. Returns {table: written_uri}."""
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
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(description="Run Coverdrive Bronze ingestion")
    parser.add_argument(
        "--mode",
        choices=["scrape", "fixtures"],
        default="scrape",
        help="scrape: live HTTP. fixtures: load test CSVs.",
    )
    parser.add_argument(
        "--ingestion-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=UTC),
        default=None,
        help="Override partition date (YYYY-MM-DD).",
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
