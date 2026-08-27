"""
The race spine: season, round, date, circuit coordinates, Wikipedia URL.

Read from the medallion pipeline's local landing files (`src/ingestion/`)
rather than from Delta. That is deliberate and is the single most important
cost decision in this build: Databricks Free Edition has a daily compute quota
that cannot be recovered until the next day, and every Gold query spends it.
The landing files hold exactly the same `races` payloads the pipeline parsed,
so reading them locally is free and gives an identical spine.

Before the merge into this repository, this read a sibling project's landing
directory over F1_LANDING_DIR — the ingestion job and the harvest step lived in
two separate repos. They now share one `./landing`, so that is the default; the
override still exists for anyone running harvest against a landing directory
elsewhere.

The spine is what makes the whole project one system rather than two. Every
downstream row - race report chunk, weather observation, prediction, note -
carries the same (season, round) key the Gold marts use, so the agent can pivot
from a semantic hit in a race report straight into the results table for that
race.
"""

import glob
import json
import logging
import os
from dataclasses import dataclass, asdict

logger = logging.getLogger("races")

LANDING_DIR = os.environ.get("F1_LANDING_DIR", "./landing")


@dataclass(frozen=True)
class Race:
    season: int
    round: int
    race_name: str
    race_date: str          # ISO date
    circuit_id: str
    circuit_name: str
    circuit_country: str
    circuit_lat: float
    circuit_long: float
    wikipedia_url: str

    @property
    def key(self) -> tuple[int, int]:
        return (self.season, self.round)

    def to_dict(self) -> dict:
        return asdict(self)


def load_races(landing_dir: str | None = None) -> list[Race]:
    """Load every race across every season, newest ingest winning on conflict.

    The ingestion job may have written a season more than once (a re-run after
    new rounds closed). Files are processed in filename order, which is
    timestamp order, and later writes overwrite earlier ones for the same
    (season, round) - so the most recent pull wins without needing to parse
    ingest timestamps.
    """
    landing_dir = landing_dir or LANDING_DIR
    pattern = os.path.join(landing_dir, "races", "**", "*.json")
    paths = sorted(glob.glob(pattern, recursive=True))
    if not paths:
        raise FileNotFoundError(
            f"No race files under {pattern}. Set F1_LANDING_DIR to the "
            "medallion project's landing directory."
        )

    races: dict[tuple[int, int], Race] = {}
    for path in paths:
        with open(path) as fh:
            payload = json.load(fh)
        # Landing files wrap the API response: {_ingest_ts, _endpoint, payload}
        items = (
            payload.get("payload", {})
            .get("MRData", {})
            .get("RaceTable", {})
            .get("Races", [])
        )
        for item in items:
            location = item.get("Circuit", {}).get("Location", {})
            try:
                race = Race(
                    season=int(item["season"]),
                    round=int(item["round"]),
                    race_name=item.get("raceName", ""),
                    race_date=item.get("date", ""),
                    circuit_id=item.get("Circuit", {}).get("circuitId", ""),
                    circuit_name=item.get("Circuit", {}).get("circuitName", ""),
                    circuit_country=location.get("country", ""),
                    circuit_lat=float(location["lat"]),
                    circuit_long=float(location["long"]),
                    wikipedia_url=item.get("url", ""),
                )
            except (KeyError, TypeError, ValueError) as exc:
                # A malformed race is worth naming, not worth aborting a
                # 71-race harvest over.
                logger.warning("Skipping malformed race in %s: %s", path, exc)
                continue
            races[race.key] = race

    return sorted(races.values(), key=lambda r: r.key)


def completed_races(races: list[Race], as_of: str) -> list[Race]:
    """Races that have actually been run as of a date.

    Future rounds have a Wikipedia stub and no weather observations, so
    harvesting them produces empty text and archive misses. Filtering here keeps
    that decision in one place instead of scattered through both harvesters.
    """
    return [r for r in races if r.race_date and r.race_date < as_of]
