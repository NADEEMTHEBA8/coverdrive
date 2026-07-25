"""Extraction module for historical T20 match archives via Cricsheet."""

from __future__ import annotations

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

CRICSHEET_ARCHIVE_URL: Final = "https://cricsheet.org/downloads/t20s_json.zip"
MAX_MATCHES_LIMIT: Final = 50


def extract_telemetry_archives(
    archive_url: str = CRICSHEET_ARCHIVE_URL, upload_limit: int = MAX_MATCHES_LIMIT
) -> None:
    """Retrieves historical match telemetry archives and streams them to S3 Bronze.

    Due to payload size, archives are processed in-memory to avoid disk I/O
    bottlenecks on ephemeral execution runners.
    """
    log.info("extract_cricsheet.start", url=archive_url)

    archive_response = requests.get(archive_url, timeout=30)
    archive_response.raise_for_status()

    storage_client = get_s3_client()
    target_bucket = get_settings().coverdrive_s3_bucket
    s3_prefix = "bronze/cricsheet"

    uploaded_count = 0
    with zipfile.ZipFile(io.BytesIO(archive_response.content)) as zip_ref:
        json_filenames = [f for f in zip_ref.namelist() if f.endswith(".json")]
        json_filenames.sort(reverse=True)

        for filename in json_filenames[:upload_limit]:
            payload_data = zip_ref.read(filename)
            s3_key = f"{s3_prefix}/{filename}"

            storage_client.put_object(
                Bucket=target_bucket,
                Key=s3_key,
                Body=payload_data,
                ContentType="application/json",
            )
            uploaded_count += 1
            if uploaded_count % 10 == 0:
                log.info("extract_cricsheet.progress", uploaded=uploaded_count)

    log.info("extract_cricsheet.complete", total_uploaded=uploaded_count)


if __name__ == "__main__":
    configure_logging()
    extract_telemetry_archives()
