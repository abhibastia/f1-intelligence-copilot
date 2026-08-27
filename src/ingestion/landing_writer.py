"""Writes raw Jolpica payloads into the landing zone.

The whole point of this module is idempotency. Jolpica's endpoints return full
snapshots, so a naive re-run would land a second copy of every round and
double-count it downstream. The rule:

  * a round whose race was more than CLOSED_AFTER_DAYS ago is **closed** — it is
    written once and skipped on every later run;
  * the most recent round is **open** — re-pulled each run, because results and
    standings are amended after the flag (penalties, appeals, DSQs).

Open rounds therefore land multiple snapshots. Silver MUST deduplicate by
natural key on the greatest _ingest_ts. Without that, the live round
double-counts. This is the other half of the contract.

Paths use plain `os`, which works unchanged against a local staging directory
and against /Volumes on a Databricks job.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
from typing import Any

log = logging.getLogger(__name__)

# Results settle within a few days of the flag. A week is comfortably safe
# without keeping rounds open long enough to accumulate junk snapshots.
CLOSED_AFTER_DAYS = 7


def partition_dir(root: str, endpoint: str, season: int, round_no: int | None) -> str:
    parts = [root, endpoint, f"season={season}"]
    if round_no is not None:
        parts.append(f"round={round_no}")
    return os.path.join(*parts)


def has_any_file(directory: str) -> bool:
    return os.path.isdir(directory) and any(
        name.endswith(".json") for name in os.listdir(directory)
    )


def is_closed(race_date: dt.date | None, today: dt.date | None = None) -> bool:
    """A round with no known date is treated as open — safer to re-pull."""
    if race_date is None:
        return False
    today = today or dt.date.today()
    return race_date < today - dt.timedelta(days=CLOSED_AFTER_DAYS)


def should_write(
    root: str,
    endpoint: str,
    season: int,
    round_no: int | None,
    race_date: dt.date | None,
) -> bool:
    directory = partition_dir(root, endpoint, season, round_no)
    if not has_any_file(directory):
        return True
    # Already have data: only re-pull while the round is still open.
    return not is_closed(race_date)


def write_payload(
    root: str,
    endpoint: str,
    season: int,
    round_no: int | None,
    payload: dict[str, Any],
    source_url: str,
) -> str:
    """Wrap the raw MRData in an ingestion envelope and land it as one JSON file.

    The envelope is what makes Bronze auditable and Silver's dedupe possible —
    storing bare MRData would leave no ingestion timestamp to order snapshots by.
    """
    ingest_ts = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    envelope = {
        "_ingest_ts": ingest_ts.isoformat().replace("+00:00", "Z"),
        "_source_url": source_url,
        "_season": str(season),
        "_round": str(round_no) if round_no is not None else None,
        "_endpoint": endpoint,
        "payload": payload,
    }

    directory = partition_dir(root, endpoint, season, round_no)
    os.makedirs(directory, exist_ok=True)

    stamp = ingest_ts.strftime("%Y%m%dT%H%M%SZ")
    round_part = f"_{round_no}" if round_no is not None else ""
    filename = f"{endpoint}_{season}{round_part}_{stamp}.json"
    path = os.path.join(directory, filename)

    # One JSON object per file, written on a single line so Auto Loader can read
    # it without multiLine (which forces whole-file reads and is slower).
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(envelope, handle, ensure_ascii=False)

    log.info("wrote %s", path)
    return path
