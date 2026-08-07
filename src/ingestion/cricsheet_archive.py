"""Extraction module for historical T20 match archives via Cricsheet."""

from __future__ import annotations

import io
import os
import zipfile
from typing import Final

import requests
from structlog import get_logger

from src.common.utils import (
    configure_logging,
    get_s3_client,
    get_settings,
)

log = get_logger(__name__)

CRICSHEET_ARCHIVE_URL: Final = "https://cricsheet.org/downloads/t20s_json.zip"
MAX_MATCHES_LIMIT: Final = 50


def extract_telemetry_archives(
    archive_url: str = CRICSHEET_ARCHIVE_URL, upload_limit: int | None = None
) -> None:
    """Retrieves historical match telemetry archives and streams them to S3 Bronze.

    Due to payload size, archives are processed in-memory to avoid disk I/O
    bottlenecks on ephemeral execution runners.
    """
    if upload_limit is None:
        upload_limit = int(os.getenv("COVERDRIVE_MATCH_LIMIT", str(MAX_MATCHES_LIMIT)))

    log.info("extract_cricsheet.start", url=archive_url, upload_limit=upload_limit)

    archive_response = requests.get(archive_url, timeout=30)
    archive_response.raise_for_status()

    storage_client = get_s3_client()
    target_bucket = get_settings().coverdrive_s3_bucket
    s3_prefix = "bronze/cricsheet"

    # Direct Zip Landing (Production Mode: upload 1 single zip in ~1.5 seconds)
    if upload_limit <= 0 or os.getenv("COVERDRIVE_DIRECT_ZIP", "1") == "1":
        archive_key = f"{s3_prefix}/t20s_json.zip"
        log.info("extract_cricsheet.direct_zip.start", target=f"s3://{target_bucket}/{archive_key}")
        storage_client.put_object(
            Bucket=target_bucket,
            Key=archive_key,
            Body=archive_response.content,
            ContentType="application/zip",
        )
        log.info("extract_cricsheet.direct_zip.complete", bytes=len(archive_response.content))
        return

    uploaded_count = 0
    with zipfile.ZipFile(io.BytesIO(archive_response.content)) as zip_ref:
        json_filenames = [f for f in zip_ref.namelist() if f.endswith(".json")]
        json_filenames.sort(reverse=True)
        target_files = json_filenames[:upload_limit]

        def upload_single_file(filename: str) -> None:
            payload_data = zip_ref.read(filename)
            s3_key = f"{s3_prefix}/{filename}"
            for attempt in range(3):
                try:
                    storage_client.put_object(
                        Bucket=target_bucket,
                        Key=s3_key,
                        Body=payload_data,
                        ContentType="application/json",
                    )
                    break
                except Exception as err:
                    if attempt == 2:
                        log.warning("upload.single_file.failed", filename=filename, error=str(err))
                    else:
                        import time

                        time.sleep(0.2 * (attempt + 1))

        from concurrent.futures import ThreadPoolExecutor, as_completed

        with ThreadPoolExecutor(max_workers=60) as executor:
            futures = {executor.submit(upload_single_file, fn): fn for fn in target_files}
            for future in as_completed(futures):
                future.result()
                uploaded_count += 1
                if uploaded_count % 50 == 0 or uploaded_count == len(target_files):
                    log.info(
                        "extract_cricsheet.progress",
                        uploaded=uploaded_count,
                        total=len(target_files),
                    )

    log.info("extract_cricsheet.complete", total_uploaded=uploaded_count)


if __name__ == "__main__":
    configure_logging()
    extract_telemetry_archives()
