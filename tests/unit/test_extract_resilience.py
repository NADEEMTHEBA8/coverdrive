"""Unit tests for defensive extraction, signature-based table parsing, and rate limit backoff."""

from __future__ import annotations

import pytest
import requests

try:
    from src.ingestion.espn_html_extractor import SchemaDriftError, _parse_html_table
    from src.ingestion.open_meteo_api import fetch_venue_coordinates
except ImportError:
    from coverdrive.extract.espn_html_extractor import SchemaDriftError, _parse_html_table
    from coverdrive.extract.open_meteo_api import fetch_venue_coordinates


def test_signature_matching_finds_correct_table_despite_decoy_tables() -> None:
    """HTML containing navigation/ad tables is parsed by probing for target column signatures."""
    payload = '\n    <html>\n        <body>\n            <table><tr><td>Ad Banner</td></tr></table>\n            <table><tr><td>Navigation Link 1</td><td>Link 2</td></tr></table>\n            <table id="target">\n                <tr><th>Player</th><th>Runs</th><th>Ave</th></tr>\n                <tr><td>N Theba (IND)</td><td>10500</td><td>50.5</td></tr>\n            </table>\n        </body>\n    </html>\n    '
    df = _parse_html_table(payload, expected_signatures=["player", "runs"])
    assert len(df) == 1
    assert "Player" in df.columns
    assert df.iloc[0]["Player"] == "N Theba (IND)"


def test_signature_matching_raises_schema_drift_error_when_missing() -> None:
    """If upstream DOM alters completely, SchemaDriftError is raised loudly."""
    drifted_payload = "\n    <html>\n        <body>\n            <table><tr><td>Athlete</td><td>Score</td></tr></table>\n        </body>\n    </html>\n    "
    with pytest.raises(SchemaDriftError, match="Schema drift detected"):
        _parse_html_table(drifted_payload, expected_signatures=["player", "runs"])


def test_open_meteo_api_retries_on_rate_limit_429(monkeypatch: pytest.MonkeyPatch) -> None:
    """HTTP 429 rate limit triggers retry sequence with RateLimitExhaustedError."""
    attempts = {"count": 0}

    def mock_get(*args, **kwargs):
        attempts["count"] += 1
        response = requests.Response()
        if attempts["count"] < 2:
            response.status_code = 429
        else:
            response.status_code = 200
            response._content = b'{"results": [{"latitude": 12.97, "longitude": 77.59}]}'
        return response

    monkeypatch.setattr(requests, "get", mock_get)
    coords = fetch_venue_coordinates("Bengaluru")
    assert coords == (12.97, 77.59)
    assert attempts["count"] == 2
