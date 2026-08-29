"""
Reconstruct pit-stop stints from seeded pit stops. Pure Lakebase-to-Lakebase
derivation - no network calls, no Databricks compute.

    python -m f1lake.load_strategy

A stint is the run between pit stops. Jolpica gives stops, not stints, so they
are derived: a driver with two stops ran three stints, and the stop laps are the
boundaries. The final stint's end lap comes from the driver's completed laps in
the results mart - without it, every last stint would look open-ended.

Pit stops themselves are seeded from the governed `f1.silver.fact_pit_stop`
table by `f1lake.seed_gold.seed_pit_stops()`, not fetched here - this module
used to fetch them a second time, independently, from Jolpica (harvest/laps.py),
which was the same shape of duplication weather was before seed_weather()
existed. `main()` below assumes pit stops are already in Lakebase; run
`python -m f1lake.seed_gold` first if they are not.
"""

import argparse
import logging

from f1lake import schema
from f1lake.schema import execute_values

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("strategy")


def parse_duration(value) -> float | None:
    """Parse a pit-stop duration into seconds.

    Jolpica reports most stops as plain seconds ("23.456") but long ones as
    M:SS.mmm ("1:05.820"). Treating the second form as unparseable dropped
    exactly the stops worth finding - a 65-second stop is a race-defining
    failure, not noise, and silently discarding it would have made every
    "slowest stop" query wrong.

    Kept here as a small, independently useful, tested utility even though
    seed_pit_stops() reads duration_s already parsed by
    src/pipeline/02c_silver_pitstops.py - the governed pipeline's own parser,
    not this one.
    """
    if value in (None, ""):
        return None
    text = str(value).strip()
    try:
        return float(text)
    except ValueError:
        pass
    if ":" in text:
        try:
            minutes, seconds = text.split(":", 1)
            return int(minutes) * 60 + float(seconds)
        except (TypeError, ValueError):
            return None
    return None


def build_stints() -> int:
    """Derive stints from stop laps plus each driver's completed lap count."""
    stops = schema.query(f"""
        SELECT season, round, driver_id, stop_number, lap
        FROM {schema.PIT_STOPS} ORDER BY season, round, driver_id, stop_number""")
    finished = {
        (r["season"], r["round"], r["driver_id"]): r["laps_completed"]
        for r in schema.query("""
                    SELECT season, round, driver_id, laps_completed
                    FROM f1_driver_performance WHERE laps_completed IS NOT NULL""")
    }

    by_driver: dict = {}
    for s in stops:
        by_driver.setdefault((s["season"], s["round"], s["driver_id"]), []).append(s["lap"])

    rows = []
    for (season, rnd, driver), laps in by_driver.items():
        laps = sorted(laps)
        total = finished.get((season, rnd, driver))
        try:
            total = int(float(total)) if total is not None else None
        except (TypeError, ValueError):
            total = None
        boundaries = [1] + [lap + 1 for lap in laps]
        ends = laps + [total]
        for i, (start, end) in enumerate(zip(boundaries, ends), start=1):
            length = (end - start + 1) if (end is not None and end >= start) else None
            rows.append(
                (
                    season,
                    rnd,
                    driver,
                    i,
                    start,
                    end,
                    length,
                    "race start" if i == 1 else f"pit stop {i - 1}",
                    "race end" if i == len(boundaries) else f"pit stop {i}",
                )
            )
    if not rows:
        return 0
    sql = f"""INSERT INTO {schema.STINTS}
              (season, round, driver_id, stint_number, start_lap, end_lap, laps,
               entry_reason, exit_reason)
              VALUES %s
              ON CONFLICT (season, round, driver_id, stint_number) DO UPDATE SET
                start_lap=EXCLUDED.start_lap, end_lap=EXCLUDED.end_lap,
                laps=EXCLUDED.laps, entry_reason=EXCLUDED.entry_reason,
                exit_reason=EXCLUDED.exit_reason"""
    with schema.connection() as conn:
        with schema.cursor(conn) as cur:
            execute_values(cur, sql, rows, page_size=500)
            conn.commit()
    return len(rows)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.parse_args()
    schema.ensure_schema()
    logger.info("stints derived: %d", build_stints())
    t = schema.query(f"""SELECT (SELECT count(*) FROM {schema.PIT_STOPS}) stops,
                                (SELECT count(*) FROM {schema.STINTS}) stints,
                                (SELECT count(DISTINCT (season,round)) FROM {schema.PIT_STOPS}) races""")[
        0
    ]
    logger.info("totals: %(stops)d stops, %(stints)d stints across %(races)d races", t)


if __name__ == "__main__":
    main()
