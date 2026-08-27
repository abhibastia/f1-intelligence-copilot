"""Entry point for F1 raw ingestion.

Runs identically as a Databricks Job (writing straight to the UC Volume) and on
a laptop (writing to a local staging directory that is then uploaded). Modes:

  backfill    — every configured season, including the live one
  incremental — the live season only; what the scheduled Job runs weekly

Usage:
    python ingest.py --mode backfill --root /Volumes/f1/raw/landing
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import sys
from dataclasses import dataclass, field

# A Databricks job does not guarantee the task file's directory is on sys.path,
# so the sibling imports below would fail. Add it explicitly.
#
# __file__ cannot be relied on here. A serverless `spark_python_task` runs the
# file through exec(compile(...)) in a notebook-style kernel, where __file__ is
# never bound - so this line raised NameError before a single API call was made,
# and the job had never once completed. Running it locally always worked, which
# is exactly why the bug survived: `python ingest.py` binds __file__ and the job
# runner does not.
def _module_dir() -> str:
    if "__file__" in globals():
        return os.path.dirname(os.path.abspath(__file__))
    # Fall back to the notebook context's own path when the runner provides one,
    # then to the working directory.
    try:
        from dbutils import DBUtils  # noqa: F401  (only present on Databricks)
    except Exception:
        pass
    for candidate in (os.environ.get("DATABRICKS_SOURCE_DIR"), os.getcwd()):
        if candidate and os.path.isdir(candidate):
            return candidate
    return "."


sys.path.insert(0, _module_dir())

from config import (
    ALL_ENDPOINTS,
    backfill_seasons,
    BASE_URL,
    incremental_seasons,
    ROUND_ENDPOINTS,
    SEASON_ENDPOINTS,
    VOLUME_ROOT,
)
from jolpica_client import JolpicaError, fetch_all
from landing_writer import should_write, write_payload

import weather
from config import ARCHIVE_LAG_DAYS

log = logging.getLogger("ingest")


@dataclass
class RunSummary:
    files_written: int = 0
    partitions_skipped: int = 0
    requests_made: int = 0
    failures: list[str] = field(default_factory=list)

    def report(self) -> None:
        log.info("─" * 60)
        log.info("files written      : %s", self.files_written)
        log.info("partitions skipped : %s", self.partitions_skipped)
        log.info("API requests       : %s", self.requests_made)
        log.info("failures           : %s", len(self.failures))
        for failure in self.failures:
            log.error("  %s", failure)
        log.info("─" * 60)


def parse_race_calendar(races_payload: dict) -> dict[int, dt.date]:
    """Map round number → race date from a season's race list."""
    races = races_payload["MRData"]["RaceTable"]["Races"]
    calendar: dict[int, dt.date] = {}
    for race in races:
        try:
            calendar[int(race["round"])] = dt.date.fromisoformat(race["date"])
        except (KeyError, ValueError):
            log.warning("unparseable race entry: %s", race.get("round"))
    return calendar


def parse_race_coordinates(races_payload: dict) -> dict[int, tuple[float, float]]:
    """Map round number -> (latitude, longitude) of the circuit.

    The coordinates are already in the races payload under Circuit.Location, so
    the weather source needs no lookup table and no second opinion about where
    a circuit is. Kept separate from parse_race_calendar rather than widening
    its return type, because every existing caller wants a plain date.
    """
    races = races_payload["MRData"]["RaceTable"]["Races"]
    coords: dict[int, tuple[float, float]] = {}
    for race in races:
        try:
            location = race["Circuit"]["Location"]
            coords[int(race["round"])] = (
                float(location["lat"]), float(location["long"])
            )
        except (KeyError, TypeError, ValueError):
            log.warning("no usable coordinates for round %s", race.get("round"))
    return coords


def ingest_season(season: int, root: str, summary: RunSummary) -> None:
    log.info("=== season %s ===", season)
    today = dt.date.today()

    # The race calendar drives everything: which rounds exist, and whether each
    # one is closed. Always fetched first, and always re-fetched for the live
    # season because the calendar itself can shift.
    try:
        races = fetch_all(SEASON_ENDPOINTS["races"].format(season=season))
        summary.requests_made += 1
    except JolpicaError as exc:
        summary.failures.append(f"season {season} calendar: {exc}")
        return

    calendar = parse_race_calendar(races)
    if not calendar:
        log.warning("season %s has no races — skipping", season)
        return
    coordinates = parse_race_coordinates(races)

    # Season-level endpoints. Treated as closed once the season's last race is
    # well past, using that date as the season's own "race date".
    last_race_date = max(calendar.values())
    for endpoint, template in SEASON_ENDPOINTS.items():
        payload = races if endpoint == "races" else None
        if not should_write(root, endpoint, season, None, last_race_date):
            summary.partitions_skipped += 1
            continue
        try:
            if payload is None:
                payload = fetch_all(template.format(season=season))
                summary.requests_made += 1
            write_payload(
                root, endpoint, season, None, payload,
                f"{BASE_URL}/{template.format(season=season)}/",
            )
            summary.files_written += 1
        except JolpicaError as exc:
            summary.failures.append(f"{endpoint} {season}: {exc}")

    # Round-level endpoints. Future rounds have no data yet — pulling them just
    # burns rate limit and lands empty payloads.
    for round_no in sorted(calendar):
        race_date = calendar[round_no]
        if race_date > today:
            continue

        for endpoint, template in ROUND_ENDPOINTS.items():
            if not should_write(root, endpoint, season, round_no, race_date):
                summary.partitions_skipped += 1
                continue
            path = template.format(season=season, round=round_no)
            try:
                payload = fetch_all(path)
                summary.requests_made += 1
                write_payload(
                    root, endpoint, season, round_no, payload, f"{BASE_URL}/{path}/"
                )
                summary.files_written += 1
            except JolpicaError as exc:
                summary.failures.append(f"{endpoint} {season} r{round_no}: {exc}")

        ingest_race_weather(season, round_no, race_date, coordinates, root, summary)


def ingest_race_weather(
    season: int,
    round_no: int,
    race_date: dt.date,
    coordinates: dict[int, tuple[float, float]],
    root: str,
    summary: RunSummary,
) -> None:
    """Land one measured observation for a race, or skip with a reason.

    Separate from the Jolpica loop because it is a different API with a
    different failure mode: a race inside the ERA5 publication lag is not an
    error, it is simply not published yet, and must not land as a row claiming
    no rain.
    """
    if round_no not in coordinates:
        summary.failures.append(f"weather {season} r{round_no}: no coordinates")
        return

    if not weather.is_available(race_date):
        log.info("weather %s r%s: inside the %s-day archive lag — skipping",
                 season, round_no, ARCHIVE_LAG_DAYS)
        summary.partitions_skipped += 1
        return

    if not should_write(root, "weather", season, round_no, race_date):
        summary.partitions_skipped += 1
        return

    latitude, longitude = coordinates[round_no]
    try:
        payload = weather.fetch_race_weather(latitude, longitude, race_date)
        summary.requests_made += 1
        write_payload(
            root, "weather", season, round_no, payload,
            weather.build_url(latitude, longitude, race_date),
        )
        summary.files_written += 1
    except weather.WeatherError as exc:
        summary.failures.append(f"weather {season} r{round_no}: {exc}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest Jolpica F1 data")
    parser.add_argument("--mode", choices=["backfill", "incremental"], default="incremental")
    parser.add_argument("--root", default=VOLUME_ROOT, help="landing zone root path")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s"
    )

    # Both are derived from today's date, never hardcoded — see config.py.
    seasons = backfill_seasons() if args.mode == "backfill" else incremental_seasons()
    log.info("mode=%s seasons=%s root=%s", args.mode, seasons, args.root)
    log.info("endpoints: %s", ", ".join(sorted(ALL_ENDPOINTS)))

    summary = RunSummary()
    for season in seasons:
        ingest_season(season, args.root, summary)
    summary.report()

    return 1 if summary.failures else 0


if __name__ == "__main__":
    _status = main()
    # Only exit non-zero. A serverless `spark_python_task` runs this file
    # through exec(), where SystemExit propagates to the task runner and is
    # reported as a failed task - even SystemExit(0). The ingestion had in fact
    # completed and landed its files; the job still showed FAILED, which is a
    # worse outcome than a silent success because it hides real failures behind
    # noise. Locally `python ingest.py` is unaffected: a clean run simply
    # returns, and a run with failures still exits non-zero.
    if _status:
        sys.exit(_status)
