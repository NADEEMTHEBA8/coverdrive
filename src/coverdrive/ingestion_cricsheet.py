"""Bronze ingestion: Download Cricsheet JSON data and push to S3."""

import argparse
import io
import zipfile
from typing import Final

import requests
from structlog import get_logger

from coverdrive.utils import (
    configure_logging,
    get_s3_client,
    get_settings,
)

log = get_logger(__name__)

CRICSHEET_URL: Final = "https://cricsheet.org/downloads/t20s_json.zip"
MAX_MATCHES: Final = 50  # Limit to 50 matches for the demo pipeline


def ingest_cricsheet() -> None:
    """Download Cricsheet T20s zip, extract a subset, and upload to S3."""
    log.info("ingest_cricsheet.start", url=CRICSHEET_URL)

    # 1. Download zip in memory
    resp = requests.get(CRICSHEET_URL, timeout=30)
    resp.raise_for_status()

    # 2. Extract and upload a subset
    s3 = get_s3_client()
    bucket = get_settings().coverdrive_s3_bucket
    prefix = "bronze/cricsheet"

    uploaded_count = 0
    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        # Filter to only JSON files (ignore READMEs)
        json_files = [f for f in z.namelist() if f.endswith(".json")]

        # Sort alphabetically (newest matches tend to have higher IDs, though not guaranteed)
        json_files.sort(reverse=True)

        for filename in json_files[:MAX_MATCHES]:
            file_data = z.read(filename)
            s3_key = f"{prefix}/{filename}"

            s3.put_object(
                Bucket=bucket,
                Key=s3_key,
                Body=file_data,
                ContentType="application/json",
            )
            uploaded_count += 1
            if uploaded_count % 10 == 0:
                log.info("ingest_cricsheet.progress", uploaded=uploaded_count)

    log.info("ingest_cricsheet.complete", total_uploaded=uploaded_count)


if __name__ == "__main__":
    configure_logging()
    parser = argparse.ArgumentParser(description="Ingest Cricsheet Data")
    parser.parse_args()
    ingest_cricsheet()
