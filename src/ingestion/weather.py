"""Measured race-day weather from the Open-Meteo ERA5 archive.

Fetches one daily observation per race at the circuit's own coordinates, which
come from the Jolpica races payload the ingest already holds in memory. No
lookup table, no second source of truth for where a circuit is.

WHY MEASURED WEATHER AT ALL
---------------------------
A race report says "the race was held in rainy conditions". A rainfall total
says 19.1 mm. Those are different claims and they disagree more often than you
would expect — a daily total cannot distinguish rain that fell overnight from
rain that fell during the race. Holding both is what makes the question
answerable instead of a matter of opinion.

THE LAG IS THE IMPORTANT PART
-----------------------------
ERA5 is a reanalysis product. Observations are assimilated and published on a
lag of roughly five days, so a race from last weekend returns a row of nulls
rather than an error. Landing that as zero would put "0.0 mm — dry" against a
race nobody has data for yet, which is worse than having no row: it is a
confident wrong answer. `is_available` gates on the lag, and a payload that
comes back empty anyway is reported rather than coerced.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from config import (
    ARCHIVE_LAG_DAYS,
    OPEN_METEO_ARCHIVE_URL,
    OPEN_METEO_DAILY,
    OPEN_METEO_REQUESTS_PER_SECOND,
    REQUEST_TIMEOUT_SECONDS,
)
from jolpica_client import RateBudget

log = logging.getLogger(__name__)


class WeatherError(RuntimeError):
    """Raised when an observation cannot be fetched."""


# Open-Meteo's free tier allows far more than this project needs; the budget
# exists so a backfill is a polite neighbour, not because a limit binds.
_budget = RateBudget(OPEN_METEO_REQUESTS_PER_SECOND, per_hour=10_000)


def is_available(race_date: dt.date, today: dt.date | None = None) -> bool:
    """Whether ERA5 should have published this race day yet.

    Returning False is not an error — it is the correct answer for a race that
    happened three days ago, and the caller should skip rather than land nulls.
    """
    today = today or dt.date.today()
    return race_date <= today - dt.timedelta(days=ARCHIVE_LAG_DAYS)


def build_url(latitude: float, longitude: float, race_date: dt.date) -> str:
    """One day, one location. Start and end date are the same by design."""
    query = urllib.parse.urlencode(
        {
            "latitude": f"{latitude:.4f}",
            "longitude": f"{longitude:.4f}",
            "start_date": race_date.isoformat(),
            "end_date": race_date.isoformat(),
            "daily": ",".join(OPEN_METEO_DAILY),
            "timezone": "UTC",
        }
    )
    return f"{OPEN_METEO_ARCHIVE_URL}?{query}"


def has_observation(payload: dict[str, Any]) -> bool:
    """True when the archive actually returned a measurement.

    Open-Meteo answers 200 with a `daily` block of nulls for a date it has not
    assimilated. Treating that as data is how a race with no observation becomes
    a race with no rain.
    """
    daily = payload.get("daily") or {}
    values = daily.get("precipitation_sum")
    return bool(values) and values[0] is not None


def fetch_race_weather(
    latitude: float, longitude: float, race_date: dt.date
) -> dict[str, Any]:
    """Fetch one race day. Raises rather than returning a partial result."""
    url = build_url(latitude, longitude, race_date)
    _budget.acquire()
    try:
        request = urllib.request.Request(
            url, headers={"User-Agent": "f1-capstone-pipeline/1.0"}
        )
        with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise WeatherError(f"{type(exc).__name__} for {url}") from exc

    if not has_observation(payload):
        raise WeatherError(
            f"archive returned no observation for {race_date} at "
            f"{latitude:.4f},{longitude:.4f} — likely still inside the "
            f"{ARCHIVE_LAG_DAYS}-day publication lag"
        )
    return payload
