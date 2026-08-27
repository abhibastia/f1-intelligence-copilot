"""
Load pit stops and reconstruct stints. Runs locally, costs no Databricks compute.

    python -m f1lake.load_strategy

A stint is the run between pit stops. Jolpica gives stops, not stints, so they
are derived: a driver with two stops ran three stints, and the stop laps are the
boundaries. The final stint's end lap comes from the driver's completed laps in
the results mart - without it, every last stint would look open-ended.

This is the cheapest strategically dense data available. Lap times would add
per-stint PACE, but stop timing alone already answers who pitted first, who
gambled, and how many stops each strategy took.
"""

import argparse, glob, json, logging, os
from psycopg2.extras import execute_values
from f1lake import schema

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("strategy")


def parse_duration(value) -> float | None:
    """Parse a pit-stop duration into seconds.

    Jolpica reports most stops as plain seconds ("23.456") but long ones as
    M:SS.mmm ("1:05.820"). Treating the second form as unparseable dropped
    exactly the stops worth finding - a 65-second stop is a race-defining
    failure, not noise, and silently discarding it would have made every
    "slowest stop" query wrong.
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


def load_pit_stops(data_dir: str) -> int:
    rows = []
    for path in sorted(glob.glob(os.path.join(data_dir, "pitstops", "*.json"))):
        d = json.load(open(path))
        for s in d.get("pitstops", []):
            try:
                rows.append((d["season"], d["round"], s["driverId"],
                             int(s["stop"]), int(s["lap"]), s.get("time"),
                             parse_duration(s.get("duration"))))
            except (KeyError, TypeError, ValueError) as exc:
                logger.warning("  skipping malformed stop in %s: %s", path, exc)
    if not rows:
        return 0
    sql = f"""INSERT INTO {schema.PIT_STOPS}
              (season, round, driver_id, stop_number, lap, time_of_day, duration_s)
              VALUES %s
              ON CONFLICT (season, round, driver_id, stop_number) DO UPDATE SET
                lap=EXCLUDED.lap, time_of_day=EXCLUDED.time_of_day,
                duration_s=EXCLUDED.duration_s"""
    with schema.connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=500)
            conn.commit()
    return len(rows)


def build_stints() -> int:
    """Derive stints from stop laps plus each driver's completed lap count."""
    stops = schema.query(f"""
        SELECT season, round, driver_id, stop_number, lap
        FROM {schema.PIT_STOPS} ORDER BY season, round, driver_id, stop_number""")
    finished = {(r["season"], r["round"], r["driver_id"]): r["laps_completed"]
                for r in schema.query("""
                    SELECT season, round, driver_id, laps_completed
                    FROM f1_driver_performance WHERE laps_completed IS NOT NULL""")}

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
        boundaries = [1] + [l + 1 for l in laps]
        ends = laps + [total]
        for i, (start, end) in enumerate(zip(boundaries, ends), start=1):
            length = (end - start + 1) if (end is not None and end >= start) else None
            rows.append((season, rnd, driver, i, start, end, length,
                         "race start" if i == 1 else f"pit stop {i-1}",
                         "race end" if i == len(boundaries) else f"pit stop {i}"))
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
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=500)
            conn.commit()
    return len(rows)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--data", default="data")
    a = p.parse_args()
    schema.ensure_schema()
    logger.info("pit stops loaded: %d", load_pit_stops(a.data))
    logger.info("stints derived  : %d", build_stints())
    t = schema.query(f"""SELECT (SELECT count(*) FROM {schema.PIT_STOPS}) stops,
                                (SELECT count(*) FROM {schema.STINTS}) stints,
                                (SELECT count(DISTINCT (season,round)) FROM {schema.PIT_STOPS}) races""")[0]
    logger.info("totals: %(stops)d stops, %(stints)d stints across %(races)d races", t)


if __name__ == "__main__":
    main()
