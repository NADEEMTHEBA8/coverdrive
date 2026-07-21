"""Bronze ingestion: Download historical weather from Open-Meteo for scraped matches."""

import argparse
import json
import time
from typing import Final

import requests
from structlog import get_logger

from coverdrive.utils import (
    configure_logging,
    get_s3_client,
    get_settings,
)

log = get_logger(__name__)

GEOCODE_URL: Final = "https://geocoding-api.open-meteo.com/v1/search"
WEATHER_URL: Final = "https://archive-api.open-meteo.com/v1/archive"


def _get_lat_lon(city: str) -> tuple[float, float] | None:
    """Fetch coordinates for a city using Open-Meteo geocoding."""
    resp = requests.get(
        GEOCODE_URL, params={"name": city, "count": 1, "format": "json"}, timeout=10
    )
    if resp.status_code != 200:
        return None
    data = resp.json()
    if not data.get("results"):
        return None
    res = data["results"][0]
    return res.get("latitude"), res.get("longitude")


def _get_weather(lat: float, lon: float, date_str: str) -> dict | None:
    """Fetch historical weather for a specific lat/lon and date."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": date_str,
        "end_date": date_str,
        "daily": ["temperature_2m_max", "precipitation_sum", "rain_sum"],
        "timezone": "auto",
    }
    resp = requests.get(WEATHER_URL, params=params, timeout=10)
    if resp.status_code != 200:
        return None
    return resp.json()


def ingest_weather() -> None:
    """Read Cricsheet JSONs in S3, extract city/date, and fetch weather."""
    log.info("ingest_weather.start")
    s3 = get_s3_client()
    bucket = get_settings().coverdrive_s3_bucket
    bronze_prefix = "bronze/cricsheet"
    weather_prefix = "bronze/weather"

    # List all Cricsheet matches
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=bronze_prefix)

    processed_cities = {}  # Cache lat/lon to avoid hammering geocode API
    uploaded = 0

    for page in pages:
        if "Contents" not in page:
            continue
        for obj in page["Contents"]:
            key = obj["Key"]
            if not key.endswith(".json"):
                continue

            # Read match JSON
            resp = s3.get_object(Bucket=bucket, Key=key)
            match_data = json.loads(resp["Body"].read().decode("utf-8"))

            info = match_data.get("info", {})
            city = info.get("city")
            dates = info.get("dates", [])
            match_id = key.split("/")[-1].replace(".json", "")

            if not city or not dates:
                continue

            date_str = dates[0]

            # Geocode
            if city not in processed_cities:
                coords = _get_lat_lon(city)
                processed_cities[city] = coords
                time.sleep(0.5)  # Respect free API rate limits

            coords = processed_cities[city]
            if not coords:
                continue

            lat, lon = coords

            # Fetch Weather
            weather_data = _get_weather(lat, lon, date_str)
            if weather_data:
                # Add match_id for joining later
                weather_data["match_id"] = match_id

                s3.put_object(
                    Bucket=bucket,
                    Key=f"{weather_prefix}/{match_id}.json",
                    Body=json.dumps(weather_data).encode("utf-8"),
                    ContentType="application/json",
                )
                uploaded += 1
                time.sleep(0.5)  # Respect free API rate limits

            if uploaded % 10 == 0 and uploaded > 0:
                log.info("ingest_weather.progress", uploaded=uploaded)

    log.info("ingest_weather.complete", total_uploaded=uploaded)


if __name__ == "__main__":
    configure_logging()
    parser = argparse.ArgumentParser(description="Ingest Weather Data")
    parser.parse_args()
    ingest_weather()
