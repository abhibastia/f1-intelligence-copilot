#!/usr/bin/env python3
"""Recompute the season-progress numbers this repo's docs hardcode, and check
they still match.

    python3 scripts/check_doc_counts.py

WHY THIS EXISTS
---------------
`architecture.md`, `runbook.md` and the README all state real numbers — round
count, lap timings, the quarantine census, reconciliation — as prose. Those
numbers are correct the day they're written and silently wrong the next race
weekend, because the season keeps producing rows. A 2026-08-22 audit found
five separate documents still describing a five-Gold-mart, 129-test platform
that had grown a sixth mart and ninety-five more tests months earlier —
nothing failed, nothing flagged it, it was only found by reading every doc by
hand.

This recomputes the same numbers from the local `landing/` raw JSON cache —
no Spark, no warehouse, no compute quota spent — and checks the literal
number each doc currently states against that recomputation. It cannot see a
brand-new *kind* of stale claim, only the ones registered in CLAIMS below; add
a line here whenever a new hardcoded count is added to a doc.

Some claims in these docs (the pace-vs-finish "150 driver-races" finding, the
strategy "0.69 places" finding) depend on Gold-layer computation this script
does not reproduce, because they need real aggregation over `driver_performance`
/ `lap_pace`. Those are listed as informational, not checked — a genuine
audit of them needs a warehouse query or `tests/spark`.

Run this after every pipeline run that lands a new round, before touching any
doc by hand.
"""

import glob
import json
import os
import re
import sys

LANDING = "landing"

WORDS = {
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
}


def to_seconds(t: str) -> float:
    if ":" in t:
        m, s = t.split(":")
        return int(m) * 60 + float(s)
    return float(t)


def count_rounds() -> tuple[int, dict[int, int]]:
    per_season = {}
    for season_dir in sorted(glob.glob(f"{LANDING}/results/season=*")):
        season = int(season_dir.split("season=")[1])
        per_season[season] = len(glob.glob(f"{season_dir}/round=*"))
    return sum(per_season.values()), per_season


def count_laps() -> tuple[int, int]:
    """(total lap timings landed, timings outside the 40-300s plausible range)."""
    total = bad = 0
    for f in glob.glob(f"{LANDING}/laps/season=*/round=*/*.json"):
        payload = json.load(open(f))
        for race in payload["payload"]["MRData"]["RaceTable"]["Races"]:
            for lap in race.get("Laps", []):
                for timing in lap.get("Timings", []):
                    total += 1
                    try:
                        secs = to_seconds(timing["time"])
                    except (KeyError, ValueError):
                        continue
                    if not (40 <= secs <= 300):
                        bad += 1
    return total, bad


def count_bad_standings() -> int:
    bad = 0
    for f in glob.glob(f"{LANDING}/driver_standings/season=*/round=*/*.json"):
        payload = json.load(open(f))
        for lst in payload["payload"]["MRData"]["StandingsTable"]["StandingsLists"]:
            for s in lst["DriverStandings"]:
                if not s.get("position"):
                    bad += 1
    return bad


def count_bad_pitstops() -> tuple[int, int]:
    total = bad = 0
    for f in glob.glob(f"{LANDING}/pitstops/season=*/round=*/*.json"):
        payload = json.load(open(f))
        for race in payload["payload"]["MRData"]["RaceTable"]["Races"]:
            for p in race.get("PitStops", []):
                total += 1
                if not p.get("duration"):
                    bad += 1
    return total, bad


def count_reconciliation_mismatches() -> tuple[int, int]:
    """Race + sprint points per driver-season vs. the published standings.

    The `sprint` endpoint is season-level and re-pulled while a season is
    open, so several files can exist for the same season — take the most
    recently landed one per season, the same way Silver's natural-key dedupe
    keeps the greatest `_ingest_ts`. Summing across every re-pull instead
    double- or triple-counts sprint points for the live season and reports
    false mismatches.
    """
    from collections import defaultdict

    race_points: dict[tuple[int, str], float] = defaultdict(float)
    for f in glob.glob(f"{LANDING}/results/season=*/round=*/*.json"):
        payload = json.load(open(f))
        for race in payload["payload"]["MRData"]["RaceTable"]["Races"]:
            season = int(race["season"])
            for r in race.get("Results", []):
                race_points[(season, r["Driver"]["driverId"])] += float(r["points"])

    latest_sprint_file: dict[int, tuple[float, str]] = {}
    for f in glob.glob(f"{LANDING}/sprint/season=*/*.json"):
        season = int(f.split("season=")[1].split("/")[0])
        mtime = os.path.getmtime(f)
        if season not in latest_sprint_file or mtime > latest_sprint_file[season][0]:
            latest_sprint_file[season] = (mtime, f)

    sprint_points: dict[tuple[int, str], float] = defaultdict(float)
    for season, (_, f) in latest_sprint_file.items():
        payload = json.load(open(f))
        for race in payload["payload"]["MRData"]["RaceTable"]["Races"]:
            for r in race.get("SprintResults", []):
                sprint_points[(int(race["season"]), r["Driver"]["driverId"])] += float(r["points"])

    computed: dict[tuple[int, str], float] = defaultdict(float)
    for k, v in race_points.items():
        computed[k] += v
    for k, v in sprint_points.items():
        computed[k] += v

    published: dict[tuple[int, str], tuple[int, float]] = {}
    for f in glob.glob(f"{LANDING}/driver_standings/season=*/round=*/*.json"):
        payload = json.load(open(f))
        for lst in payload["payload"]["MRData"]["StandingsTable"]["StandingsLists"]:
            season = int(lst["season"])
            rnd = int(lst["round"])
            for ds in lst["DriverStandings"]:
                key = (season, ds["Driver"]["driverId"])
                if key not in published or rnd > published[key][0]:
                    published[key] = (rnd, float(ds["points"]))

    mismatches = checked = 0
    for key, (_, pts) in published.items():
        checked += 1
        if abs(computed.get(key, 0.0) - pts) > 1e-6:
            mismatches += 1
    return mismatches, checked


def squeeze(text: str) -> str:
    """Collapse whitespace/newlines so a claim wrapped across lines still matches."""
    return re.sub(r"\s+", " ", text)


def read(path: str) -> str:
    return squeeze(open(path, encoding="utf-8").read())


def word_or_digit(s: str) -> int | None:
    cleaned = s.replace(",", "")
    return int(cleaned) if cleaned.isdigit() else WORDS.get(s.lower())


def main() -> int:
    total_rounds, per_season = count_rounds()
    total_laps, bad_laps = count_laps()
    clean_laps = total_laps - bad_laps
    bad_standings = count_bad_standings()
    total_pit, bad_pit = count_bad_pitstops()
    mismatches, checked_driver_seasons = count_reconciliation_mismatches()

    print("Recomputed from landing/ (no Spark, no warehouse):")
    print(f"  seasons               {sorted(per_season)}")
    print(
        f"  rounds                {total_rounds}  ({' + '.join(str(v) for _, v in sorted(per_season.items()))})"
    )
    print(
        f"  lap timings           {total_laps} landed, {bad_laps} outside 40-300s, {clean_laps} clean"
    )
    print(f"  bad driver_standings  {bad_standings} row(s) with no position")
    print(f"  bad pit stops         {bad_pit} of {total_pit} with an empty duration")
    print(
        f"  reconciliation        {mismatches} mismatch(es) across {checked_driver_seasons} driver-seasons"
    )
    print()

    # (file, regex capturing the claimed number, truth value, label)
    CLAIMS = [
        (
            "README.md",
            r"laps\*{0,2} \(driver × race × lap, ([\d,]+) rows\)",
            clean_laps,
            "grain table: clean lap-timing rows",
        ),
        (
            "docs/architecture.md",
            r"fact_lap.{0,60}\| ([\d,]+) \|",
            clean_laps,
            "§7.1 grain table: fact_lap row count",
        ),
        (
            "docs/architecture.md",
            r"finest grain is the lap\*\*, at ([\d,]+) rows",
            clean_laps,
            "§7.1 prose: finest-grain lap count",
        ),
        ("docs/runbook.md", r"(\d+) lap rows fail", bad_laps, "quarantine census: bad laps"),
        (
            "docs/runbook.md",
            r"(\d+) standings rows with no championship position",
            bad_standings,
            "quarantine census: bad standings",
        ),
        (
            "docs/runbook.md",
            r"(\d+) pit stops published with.{0,3}an empty duration",
            bad_pit,
            "quarantine census: bad pit stops",
        ),
        ("docs/architecture.md", r"(\d+) lap rows fail", bad_laps, "quarantine census: bad laps"),
        (
            "docs/architecture.md",
            r"(\d+) standings rows arrive with no championship position",
            bad_standings,
            "quarantine census: bad standings",
        ),
        (
            "docs/architecture.md",
            r"(\d+) pit stops are published with.{0,3}an empty duration",
            bad_pit,
            "quarantine census: bad pit stops",
        ),
        (
            "docs/architecture.md",
            r"\*\*(\w+) quarantined `driver_standing` rows\.?\*\*",
            bad_standings,
            "gaps list: quarantined driver_standing rows",
        ),
    ]

    ok = fail = 0
    for entry in CLAIMS:
        path, pattern, truth, label = entry[0], entry[1], entry[2], entry[3]
        text = read(path)
        m = re.search(pattern, text)
        if not m:
            print(f"  ??    {path:24s} {label} — pattern not found (rewritten? update this script)")
            fail += 1
            continue
        raw = m.group(1)
        claimed = word_or_digit(raw)
        if claimed is None:
            print(f"  ??    {path:24s} {label} — matched {raw!r}, couldn't parse as a number")
            fail += 1
        elif claimed == truth:
            print(f"  ok    {path:24s} {label}: {claimed}")
            ok += 1
        else:
            print(f"  FAIL  {path:24s} {label}: doc says {claimed}, recomputed {truth}")
            fail += 1

    print(f"\n{ok}/{len(CLAIMS)} claims match the recomputed truth")

    print("\nNot checked here — need Spark/warehouse, spot-check by hand:")
    print(
        '  README.md               "150 driver-races ... 232 ahead" (pace vs. finish, driver_performance + lap_pace)'
    )
    print('  README.md               "0.69 places ... 0.40" (strategy finding, race_strategy)')
    print(
        "  README.md / CLAUDE.md   dim_driver SCD-2 history (versions / drivers / historical rows) —"
    )
    print("                          query: SELECT COUNT(*), COUNT(DISTINCT driver_id),")
    print(
        "                          SUM(CASE WHEN __END_AT IS NOT NULL THEN 1 ELSE 0 END) FROM f1.silver.dim_driver"
    )

    return 1 if fail else 0


if __name__ == "__main__":
    sys.exit(main())
