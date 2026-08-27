"""
Lap times and pit stops — the strategy layer.

Jolpica paginates hard: one race's laps is ~1,133 rows across 12 pages at 100
per page, so 59 races is ~708 requests. Pit stops are one page per race, 59
requests total. Both are fetched here, pit stops first, so a partial run still
leaves the cheaper and more strategically dense dataset complete.

Resumable by design: each race writes its own file and existing files are
skipped, because a 708-request run WILL be interrupted and re-fetching what we
already have is the fastest way to get rate-limited.
"""

import json, logging, os, time
import requests

logger = logging.getLogger("laps")
BASE = "https://api.jolpi.ca/ergast/f1"
UA = {"User-Agent": "f1-strategy-copilot/1.0 (educational capstone)"}
PAGE = 100
DELAY = 0.35          # ~3/s, inside Jolpica's 4/s burst and 500/hr sustained
MAX_RETRIES = 4


def _get(url, params):
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, params=params, headers=UA, timeout=40)
            if r.status_code in (429, 503):
                wait = float(r.headers.get("Retry-After") or 2 ** attempt + 1)
                logger.warning("  throttled, waiting %.0fs", wait)
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()["MRData"]
        except requests.RequestException as exc:
            if attempt == MAX_RETRIES - 1:
                raise
            # Say so. A silent retry makes a run that fought the network for a
            # minute indistinguishable from one that sailed through, which is
            # exactly the thing you want to know when a harvest is slow.
            logger.warning("  %s, retrying (%d/%d)", type(exc).__name__,
                           attempt + 1, MAX_RETRIES)
            time.sleep(2 ** attempt + 1)
    raise RuntimeError("unreachable")


def fetch_paginated(season, rnd, endpoint):
    """Fetch every page for one race/endpoint, returning the Races list."""
    rows, offset, total = [], 0, None
    while total is None or offset < total:
        d = _get(f"{BASE}/{season}/{rnd}/{endpoint}/", {"limit": PAGE, "offset": offset})
        total = int(d.get("total", 0))
        races = d.get("RaceTable", {}).get("Races", [])
        if not races:
            break
        key = "Laps" if endpoint == "laps" else "PitStops"
        rows.extend(races[0].get(key, []))
        offset += PAGE
        time.sleep(DELAY)
    return rows


def harvest(races, endpoint, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    done = failed = skipped = 0
    for i, race in enumerate(races, 1):
        path = os.path.join(out_dir, f"{race.season}_{race.round:02d}.json")
        if os.path.exists(path):
            skipped += 1
            continue
        try:
            rows = fetch_paginated(race.season, race.round, endpoint)
            json.dump({"season": race.season, "round": race.round,
                       "race_name": race.race_name, endpoint: rows},
                      open(path, "w"))
            done += 1
            logger.info("[%d/%d] %s %s — %d %s rows",
                        i, len(races), race.season, race.race_name, len(rows), endpoint)
        except Exception as exc:
            failed += 1
            logger.warning("[%d/%d] %s %s — FAILED: %s",
                           i, len(races), race.season, race.race_name, exc)
    return {"fetched": done, "skipped": skipped, "failed": failed}
