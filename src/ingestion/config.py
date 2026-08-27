"""Shared configuration for F1 ingestion.

Every name that appears in more than one place lives here. The catalog is the one
value that may change (Free Edition may force a fallback catalog), so nothing
downstream hard-codes it.
"""

import datetime as dt

CATALOG = "f1"
RAW_SCHEMA = "raw"
VOLUME = "landing"

VOLUME_ROOT = f"/Volumes/{CATALOG}/{RAW_SCHEMA}/{VOLUME}"

BASE_URL = "https://api.jolpi.ca/ergast/f1"

# --- Open-Meteo ------------------------------------------------------------
# Measured race-day weather at the circuit's coordinates, from the ERA5
# reanalysis archive. Free, keyless, non-commercial use.
#
# This is the second source, and it is deliberately *measured* rather than
# forecast or scraped from a race report: the whole point is to be able to
# compare what fell from the sky against what the report says happened on track.
OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

# Daily fields requested. Kept minimal — every extra field is a wider Bronze row
# for a question nobody has asked yet.
OPEN_METEO_DAILY = (
    "precipitation_sum",
    "rain_sum",
    "temperature_2m_max",
    "temperature_2m_min",
    "wind_speed_10m_max",
    "weather_code",
)

# ERA5 is a reanalysis product, not a live feed: observations are assimilated
# and published on a lag. Asking for a race inside this window returns nulls,
# which would land as a row claiming no rain at a race nobody has data for.
ARCHIVE_LAG_DAYS = 5

# 1.0 mm over a race day is the point where rain starts affecting tyre choice
# and grip rather than merely being noted. Applied in Silver, defined here so
# the pipeline and any analysis agree on one number.
WET_THRESHOLD_MM = 1.0

# Open-Meteo's free tier allows ~10,000 calls/day, far above the ~71 this
# project makes. The throttle is politeness, not necessity.
OPEN_METEO_REQUESTS_PER_SECOND = 5.0

# The first season this project covers. Everything above it is derived.
#
# This used to be `BACKFILL_SEASONS = [2024, 2025]` and `LIVE_SEASON = 2026`,
# which is the worst kind of bug: on 1 January the weekly job keeps running,
# keeps succeeding, and ingests nothing ever again. A job that does nothing
# looks exactly like a job with nothing to do, so the platform would go stale
# in silence and stay that way until somebody noticed the dashboard had stopped
# moving.
FIRST_SEASON = 2024


def live_season(today: dt.date | None = None) -> int:
    """The season currently running.

    The calendar year. In January and February the new season has a published
    schedule but no completed races, which costs one wasted calendar request
    and lands nothing — the round loop skips races whose date is in the future.
    """
    return (today or dt.date.today()).year


def backfill_seasons(today: dt.date | None = None) -> list[int]:
    """Every season this project covers, oldest first."""
    return list(range(FIRST_SEASON, live_season(today) + 1))


def incremental_seasons(today: dt.date | None = None) -> list[int]:
    """The seasons a routine run must look at: the current one and the last.

    The previous season is included because a race in late December stays
    *open* for CLOSED_AFTER_DAYS into January — results, standings and lap
    times are all still being corrected. Dropping to the current season on
    1 January would freeze that final round with whatever was published on the
    day, and nothing downstream would ever revisit it.

    It is close to free: a season whose rounds are all closed costs exactly one
    request — the calendar — because `should_write` decides the rest from files
    already on disk.
    """
    current = live_season(today)
    return [current - 1, current] if current > FIRST_SEASON else [current]

# Jolpica publishes two limits and they bind at different scales:
#   * 4 requests/second burst  — REQUESTS_PER_SECOND handles this
#   * 500 requests/hour sustained — REQUESTS_PER_HOUR handles this
#
# Only the burst limit used to be enforced, which was fine while the endpoint
# set totalled ~200 calls per backfill. It stops being fine with `laps`: at 12
# pages per round, three seasons is ~850 requests, and 2 req/s would fire 7,200
# in an hour. The sustained limit is the real ceiling for any large backfill.
#
# 450 rather than 500 leaves headroom for the retries a long run will make.
#
# 2.0 req/s was set from Jolpica's published 4/s burst limit. Measured against
# the real API during a laps backfill it produced a 429 on roughly 37% of
# requests — the enforced burst rate is well under the documented one for
# unauthenticated clients. Worse, every retry spends the hourly budget, so a
# rate that trips throttling burns the allowance twice: once on the rejected
# request and again on the retry.
#
# 0.5 req/s spaces a race's 11 lap pages over ~22 seconds, which the API
# accepts without complaint. Slower per request, faster overall.
REQUESTS_PER_SECOND = 0.5
REQUESTS_PER_HOUR = 450
PAGE_SIZE = 100
MAX_RETRIES = 5
BACKOFF_BASE_SECONDS = 2.0
REQUEST_TIMEOUT_SECONDS = 30

# Season-level endpoints: one call per season, no round segment.
# `sprint` is season-level on purpose: the per-round form would need a call for
# every round to find the ~6 that have a sprint, while the season form returns
# them all in two pages. Sprint points count toward the championship, so without
# this endpoint the marts understate points on every sprint weekend.
SEASON_ENDPOINTS = {
    "races": "{season}/races",
    "drivers": "{season}/drivers",
    "constructors": "{season}/constructors",
    "sprint": "{season}/sprint",
}

# Round-level endpoints: one call per (season, round).
ROUND_ENDPOINTS = {
    "results": "{season}/{round}/results",
    "qualifying": "{season}/{round}/qualifying",
    "driver_standings": "{season}/{round}/driverStandings",
    "constructor_standings": "{season}/{round}/constructorStandings",
    # One page per race (~30 stops), so it costs one request per round — the
    # cheap half of the strategy layer. `laps` is the expensive half.
    "pitstops": "{season}/{round}/pitstops",
    # ~11 pages per race: `total` counts timings (one per driver per lap), so a
    # 53-lap race with 20 cars is ~1,008 records. Three seasons is ~780
    # requests against a 450/hour ceiling — the single most expensive endpoint,
    # and the reason RateBudget enforces the sustained limit.
    "laps": "{season}/{round}/laps",
}

ALL_ENDPOINTS = {**SEASON_ENDPOINTS, **ROUND_ENDPOINTS}

# The MRData sub-table each endpoint's records live under, and the array key
# holding the records. Used by the Silver layer; kept here so the contract is
# declared in exactly one place.
#
# `weather` is deliberately absent: Open-Meteo returns flat parallel arrays, not
# an MRData envelope, so Silver parses it with its own schema rather than the
# generic MRData path.
ENDPOINT_SHAPE = {
    "races": ("RaceTable", "Races"),
    "drivers": ("DriverTable", "Drivers"),
    "constructors": ("ConstructorTable", "Constructors"),
    "sprint": ("RaceTable", "Races"),
    "results": ("RaceTable", "Races"),
    "qualifying": ("RaceTable", "Races"),
    "driver_standings": ("StandingsTable", "StandingsLists"),
    "constructor_standings": ("StandingsTable", "StandingsLists"),
    "pitstops": ("RaceTable", "Races"),
    "laps": ("RaceTable", "Races"),
}