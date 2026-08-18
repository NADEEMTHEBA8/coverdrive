"""Extraction module for historical weather telemetry via Open-Meteo API."""

from __future__ import annotations

import json
from typing import Any, Final

import requests
from requests.exceptions import RequestException
from structlog import get_logger
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from src.common.utils import configure_logging, get_s3_client, get_settings

log = get_logger(__name__)
GEOCODE_ENDPOINT: Final = "https://geocoding-api.open-meteo.com/v1/search"
WEATHER_ENDPOINT: Final = "https://archive-api.open-meteo.com/v1/archive"


class RateLimitExhaustedError(Exception):
    """Raised when upstream API rate limits cannot be bypassed after maximum retries."""


@retry(
    retry=retry_if_exception_type((RequestException, RateLimitExhaustedError)),
    wait=wait_exponential(multiplier=2, min=2, max=60),
    stop=stop_after_attempt(5),
    reraise=True,
)
def fetch_venue_coordinates(city_name: str) -> tuple[float, float] | None:
    """Resolves a venue city string to geocoordinates with exponential backoff (HTTP 429)."""
    params: dict[str, Any] = {"name": city_name, "count": 1, "format": "json"}
    try:
        response = requests.get(GEOCODE_ENDPOINT, params=params, timeout=10)
        if response.status_code == 429:
            log.warning("rate_limit_breached.geocoding", city=city_name)
            raise RateLimitExhaustedError("Open-Meteo geocoding rate limit exceeded.")
        response.raise_for_status()
    except RequestException as err:
        log.error("geocoding_error", city=city_name, error=str(err))
        raise
    payload: dict[str, Any] = response.json()
    results = payload.get("results")
    if not results:
        return None
    res = results[0]
    lat, lon = (res.get("latitude"), res.get("longitude"))
    if lat is not None and lon is not None:
        return (float(lat), float(lon))
    return None


@retry(
    retry=retry_if_exception_type((RequestException, RateLimitExhaustedError)),
    wait=wait_exponential(multiplier=2, min=2, max=60),
    stop=stop_after_attempt(5),
    reraise=True,
)
def fetch_historical_weather(lat: float, lon: float, date_str: str) -> dict[str, Any] | None:
    """Fetches historical weather payload for specific coordinates and date."""
    params: dict[str, Any] = {
        "latitude": lat,
        "longitude": lon,
        "start_date": date_str,
        "end_date": date_str,
        "daily": ["temperature_2m_max", "precipitation_sum", "rain_sum"],
        "timezone": "auto",
    }
    try:
        response = requests.get(WEATHER_ENDPOINT, params=params, timeout=10)
        if response.status_code == 429:
            log.warning("rate_limit_breached.weather", lat=lat, lon=lon, date=date_str)
            raise RateLimitExhaustedError("Open-Meteo weather rate limit exceeded.")
        response.raise_for_status()
    except RequestException as err:
        log.error("weather_error", lat=lat, lon=lon, date=date_str, error=str(err))
        raise
    payload: dict[str, Any] = response.json()
    return payload


def run_ingestion() -> None:
    """Extract weather telemetry for landed Cricsheet match files."""
    log.info("ingest_weather.start")
    s3_client = get_s3_client()
    bucket_name = get_settings().coverdrive_s3_bucket
    bronze_prefix = "bronze/cricsheet"
    weather_prefix = "bronze/weather"
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_name, Prefix=bronze_prefix)
    processed_cities: dict[str, tuple[float, float] | None] = {}
    uploaded_count = 0
    for page in pages:
        if "Contents" not in page:
            continue
        for obj in page["Contents"]:
            key = obj["Key"]
            if not key.endswith(".json"):
                continue
            resp = s3_client.get_object(Bucket=bucket_name, Key=key)
            match_data = json.loads(resp["Body"].read().decode("utf-8"))
            info = match_data.get("info", {})
            city = info.get("city")
            dates = info.get("dates", [])
            match_id = key.split("/")[-1].replace(".json", "")
            if not city or not dates:
                continue
            date_str = dates[0]
            if city not in processed_cities:
                processed_cities[city] = fetch_venue_coordinates(city)
            coords = processed_cities[city]
            if not coords:
                continue
            lat, lon = coords
            weather_payload = fetch_historical_weather(lat, lon, date_str)
            if weather_payload:
                weather_payload["match_id"] = match_id
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=f"{weather_prefix}/{match_id}.json",
                    Body=json.dumps(weather_payload).encode("utf-8"),
                    ContentType="application/json",
                )
                uploaded_count += 1
            if uploaded_count % 10 == 0 and uploaded_count > 0:
                log.info("ingest_weather.progress", uploaded=uploaded_count)
    log.info("ingest_weather.complete", total_uploaded=uploaded_count)


if __name__ == "__main__":
    configure_logging()
    run_ingestion()
