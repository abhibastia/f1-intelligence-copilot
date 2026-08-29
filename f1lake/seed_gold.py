"""
Seed the medallion Gold marts from Delta into Lakebase. Run once (or after a
pipeline refresh).

WHY THIS BRIDGE EXISTS
----------------------
The Spark pipeline's Gold marts live in Delta; the agent and the apps live on
Lakebase Postgres. Querying Delta per agent request would mean a SQL warehouse
query every time someone asks a question - and on Free Edition the daily compute
quota is unrecoverable until the next day, so an agent that spends compute per
turn is an agent that stops working mid-demo.

Copying Gold into Lakebase once inverts that: a handful of warehouse queries
total, and from then on every read the agent performs is Postgres. The Delta
marts remain the source of truth and the analytical layer; Lakebase is the
serving layer.

SIX MARTS, NOT TWO
-------------------
The Lakebase side originally only seeded driver_performance and
championship_progression, then filled the gap for weather and strategy with a
second, independent local pipeline: harvest/weather.py called Open-Meteo again,
and f1lake/load_strategy.py re-derived stints from raw pit-stop JSON. Both were
real duplication of what the governed Spark pipeline already computes,
quality-checks and materialises as Gold: race_conditions (weather, with
outcome context) and race_strategy (stops/stints with field comparison).
Weather is now seeded straight from race_conditions via `seed_weather()`
below, which is why harvest/weather.py no longer exists. race_strategy,
lap_pace and constructor_standings are capability the Lakebase side never had
before this merge - see f1_broker.race_pace and f1_broker.constructor_standings.

The per-stint STINTS/PIT_STOPS pair (f1lake/load_strategy.py) is NOT
superseded: race_strategy's grain is driver x race (stop counts, field
comparison), while STINTS carries the finer per-stint start/end laps that Gold
does not expose. They are complementary, not duplicates.

DELIBERATELY SCHEMA-AGNOSTIC
----------------------------
Columns are discovered from the returned rows rather than hard-coded, and types
are inferred from Python values. That avoids a third warehouse query just to read
information_schema, and it means a change to a Gold mart's columns does not
silently truncate the seed - the new column simply appears. The one exception is
`seed_weather()`: f1_race_weather has a fixed shape that f1_broker.race_weather
and .wet_races depend on column-by-column, so it is mapped explicitly rather
than recreated from whatever race_conditions happens to return.

    DATABRICKS_CONFIG_PROFILE=<profile> python -m f1lake.seed_gold

PORTABLE BY DESIGN
-------------------
`run_query()` uses the SQL Statement Execution API via `databricks-sdk`
(`WorkspaceClient().statement_execution`), not the `databricks` CLI. That is
what makes this module callable identically from a laptop (profile-based auth)
and from inside a Databricks job (ambient auth, no profile) - see
`jobs/run_seed_gold.py`, which is a thin wrapper around `main()`. It is also
what avoids the failure mode the earlier Spark-based job/notebook seeders hit:
`spark.table(materialized_view).collect()` from job compute forced a recompute
and killed the kernel. A SQL warehouse query never touches that path - it is
the exact same query this module always ran, just issued through the SDK
instead of shelling out to a CLI binary that job compute doesn't have.
"""

import argparse
import json
import logging
import os
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import ExecuteStatementRequestOnWaitTimeout, StatementState

from f1lake import schema, load_strategy
from f1lake.schema import execute_values

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("seed-gold")

# (Delta source, Lakebase target, candidate key sets in preference order)
#
# Candidates rather than one fixed key: a wrong guess would otherwise cost
# another warehouse query to discover, and the marts name the driver column
# `driver_id` where an earlier draft assumed `driver_ref`. The first candidate
# whose columns all exist AND which is actually unique in the data wins.
MARTS = [
    ("f1.gold.driver_performance", "f1_driver_performance",
     [["season", "round", "driver_id"], ["season", "round", "driver_ref"]]),
    ("f1.gold.championship_progression", "f1_championship",
     [["season", "round", "driver_id"], ["season", "round", "driver_ref"],
      ["season", "round", "constructor_id"]]),
    ("f1.gold.race_strategy", schema.RACE_STRATEGY_SUMMARY,
     [["season", "round", "driver_id"]]),
    ("f1.gold.lap_pace", schema.LAP_PACE,
     [["season", "round", "driver_id"]]),
    ("f1.gold.constructor_standings", schema.CONSTRUCTOR_STANDINGS,
     [["season", "round", "constructor_id"]]),
]

# Kept in step with src/pipeline/02b_silver_weather.py's WET_THRESHOLD_MM. Both
# name the same 1.0 mm cut so a race is never "wet" in Gold and "dry" in
# Lakebase.
WET_THRESHOLD_MM = 1.0

CACHE_DIR = "data"

STATEMENT_POLL_SECONDS = 2
STATEMENT_TIMEOUT_SECONDS = 300


def _client(profile: str | None) -> WorkspaceClient:
    """A profile means a laptop invocation; None means ambient auth - the case
    running inside a Databricks job, where the runtime's own identity is used
    and there is nothing to pass."""
    return WorkspaceClient(profile=profile) if profile else WorkspaceClient()


def _warehouse_id(w: WorkspaceClient) -> str:
    """Resolve the SQL warehouse. DATABRICKS_WAREHOUSE_ID first (set locally in
    .env or as a job's environment) - falling back to the first warehouse the
    caller can see, so this still works with nothing configured beyond auth.
    """
    configured = os.environ.get("DATABRICKS_WAREHOUSE_ID")
    if configured:
        return configured
    warehouses = list(w.warehouses.list())
    if not warehouses:
        raise RuntimeError(
            "No SQL warehouse visible to this identity, and DATABRICKS_WAREHOUSE_ID "
            "is not set. Find one with `databricks warehouses list --profile <profile>`."
        )
    return warehouses[0].id


def run_query(sql: str, profile: str | None, cache_key: str | None = None,
              refresh: bool = False) -> list[dict]:
    """Execute one SQL statement against a SQL warehouse via the Statement
    Execution API, and return the rows as plain dicts of strings - the same
    shape the old CLI-subprocess version returned, so every caller downstream
    (pg_type, coerce, pick_key) is unchanged.

    Results are cached to disk. Free Edition's daily compute quota is
    unrecoverable until the next day, so a seeding run that fails on a Postgres
    detail after the query succeeded must not pay for that query twice. Pass
    --refresh to deliberately re-read from Delta.
    """
    cache_path = os.path.join(CACHE_DIR, f"{cache_key}.json") if cache_key else None
    if cache_path and not refresh and os.path.exists(cache_path):
        logger.info("  (cached — no warehouse query)")
        return json.load(open(cache_path))

    w = _client(profile)
    warehouse_id = _warehouse_id(w)

    response = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id, statement=sql,
        wait_timeout="30s",
        on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,
    )
    statement_id = response.statement_id
    waited = 0
    while response.status.state in (StatementState.PENDING, StatementState.RUNNING):
        if waited >= STATEMENT_TIMEOUT_SECONDS:
            raise RuntimeError(f"Query timed out after {STATEMENT_TIMEOUT_SECONDS}s: {sql[:200]}")
        time.sleep(STATEMENT_POLL_SECONDS)
        waited += STATEMENT_POLL_SECONDS
        response = w.statement_execution.get_statement(statement_id)

    if response.status.state != StatementState.SUCCEEDED:
        error = response.status.error
        message = error.message if error else str(response.status.state)
        raise RuntimeError(f"Query failed ({response.status.state}): {message}\n{sql[:200]}")

    columns = [c.name for c in response.manifest.schema.columns]
    rows = [dict(zip(columns, r)) for r in (response.result.data_array or [])]

    # Large marts can come back in more than one chunk; the first is inline on
    # the initial response, the rest are fetched by index.
    chunk = response.result.next_chunk_index
    while chunk is not None:
        page = w.statement_execution.get_statement_result_chunk_n(statement_id, chunk)
        rows.extend(dict(zip(columns, r)) for r in (page.data_array or []))
        chunk = page.next_chunk_index

    if cache_path:
        os.makedirs(CACHE_DIR, exist_ok=True)
        json.dump(rows, open(cache_path, "w"))
    return rows


def pg_type(values: list) -> str:
    """Infer a Postgres column type from the values actually present.

    The CLI returns everything as strings, so this inspects the content: a
    column is numeric only if every non-null value parses as a number. Anything
    ambiguous stays TEXT, because a wrong cast loses data while an over-wide
    text column merely looks untidy.
    """
    seen = [v for v in values if v is not None and v != ""]
    if not seen:
        return "TEXT"
    try:
        parsed = [float(v) for v in seen]
    except (TypeError, ValueError):
        return "TEXT"
    if all(float(p).is_integer() for p in parsed):
        return "BIGINT"
    return "DOUBLE PRECISION"


def coerce(value, column_type: str):
    if value is None or value == "":
        return None
    if column_type == "BIGINT":
        return int(float(value))
    if column_type == "DOUBLE PRECISION":
        return float(value)
    return value


def pick_key(rows: list[dict], columns: list[str],
             candidates: list[list[str]]) -> list[str]:
    """Choose the first candidate key that exists and is genuinely unique."""
    for candidate in candidates:
        if not all(c in columns for c in candidate):
            continue
        seen = {tuple(r.get(c) for c in candidate) for r in rows}
        if len(seen) == len(rows):
            return candidate
        logger.warning("  key %s exists but is not unique (%d/%d distinct)",
                       candidate, len(seen), len(rows))
    raise RuntimeError(
        f"No usable key among {candidates}. Available columns: {columns}"
    )


def seed(source: str, target: str, candidates: list[list[str]],
         profile: str | None, refresh: bool = False) -> int:
    logger.info("Reading %s ...", source)
    rows = run_query(f"SELECT * FROM {source}", profile,
                     cache_key=target, refresh=refresh)
    if not rows:
        logger.warning("  %s returned no rows - skipping", source)
        return 0

    columns = list(rows[0].keys())
    types = {c: pg_type([r.get(c) for r in rows]) for c in columns}
    keys = pick_key(rows, columns, candidates)
    logger.info("  %d rows, %d columns, key=%s", len(rows), len(columns), keys)

    column_ddl = ",\n            ".join(f'"{c}" {types[c]}' for c in columns)
    key_ddl = ", ".join(f'"{k}"' for k in keys)
    updates = ", ".join(
        f'"{c}" = EXCLUDED."{c}"' for c in columns if c not in keys
    ) or f'"{keys[0]}" = EXCLUDED."{keys[0]}"'

    with schema.connection() as conn:
        conn.autocommit = True
        with schema.cursor(conn) as cur:
            # Recreated on each seed: the mart is derived data with Delta as the
            # source of truth, so a stale column left behind by an earlier shape
            # would be a lie rather than history worth keeping.
            cur.execute(f"DROP TABLE IF EXISTS {target}")
            cur.execute(f"""
                CREATE TABLE {target} (
            {column_ddl},
            seeded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY ({key_ddl})
                )
            """)

    payload = [tuple(coerce(r.get(c), types[c]) for c in columns) for r in rows]
    # The mart's grain should already be unique, but a duplicate key would abort
    # the whole batch with "ON CONFLICT DO UPDATE command cannot affect row a
    # second time". Deduplicate on the key, last row winning.
    deduped = {tuple(row[columns.index(k)] for k in keys): row for row in payload}
    if len(deduped) != len(payload):
        logger.warning("  %d duplicate key(s) collapsed", len(payload) - len(deduped))

    column_list = ", ".join(f'"{c}"' for c in columns)
    insert = (
        f'INSERT INTO {target} ({column_list}) '
        f"VALUES %s ON CONFLICT ({key_ddl}) DO UPDATE SET {updates}"
    )
    with schema.connection() as conn:
        with schema.cursor(conn) as cur:
            execute_values(cur, insert, list(deduped.values()), page_size=500)
            conn.commit()

    logger.info("  -> %s: %d rows", target, len(deduped))
    return len(deduped)


def seed_weather(profile: str | None, refresh: bool = False) -> int:
    """Seed f1_race_weather from the governed `f1.gold.race_conditions` mart.

    Mapped explicitly rather than through seed()'s auto-schema path: the target
    table's shape is fixed by f1lake/schema.py and read column-by-column in
    f1_broker.race_weather / .wet_races, so recreating it from whatever
    race_conditions happens to return would silently drop that contract.
    """
    logger.info("Reading f1.gold.race_conditions ...")
    rows = run_query("SELECT * FROM f1.gold.race_conditions", profile,
                     cache_key="f1_race_weather", refresh=refresh)
    observed = [r for r in rows if r.get("weather_available") in (True, "true", "True")]
    logger.info("  %d races, %d with a weather observation", len(rows), len(observed))
    if not observed:
        return 0

    payload = [
        (
            int(r["season"]), int(r["round"]), r["race_name"], r["race_date"] or None,
            r["circuit_id"], r["circuit_name"], r["conditions"],
            coerce(r["temp_max_c"], "DOUBLE PRECISION"), coerce(r["temp_min_c"], "DOUBLE PRECISION"),
            coerce(r["precipitation_mm"], "DOUBLE PRECISION"), coerce(r["rain_mm"], "DOUBLE PRECISION"),
            coerce(r["wind_max_kmh"], "DOUBLE PRECISION"),
            str(r["was_wet"]).lower() in ("true", "1"), WET_THRESHOLD_MM,
            "databricks-gold:race_conditions",
        )
        for r in observed
    ]
    sql = f"""
        INSERT INTO {schema.WEATHER}
            (season, round, race_name, race_date, circuit_id, circuit_name,
             conditions, temp_max, temp_min, precipitation_mm, rain_mm,
             wind_speed_max, was_wet, wet_threshold_mm, source)
        VALUES %s
        ON CONFLICT (season, round) DO UPDATE SET
            conditions=EXCLUDED.conditions, temp_max=EXCLUDED.temp_max,
            temp_min=EXCLUDED.temp_min, precipitation_mm=EXCLUDED.precipitation_mm,
            rain_mm=EXCLUDED.rain_mm, wind_speed_max=EXCLUDED.wind_speed_max,
            was_wet=EXCLUDED.was_wet, wet_threshold_mm=EXCLUDED.wet_threshold_mm,
            source=EXCLUDED.source, synced_at=now()
    """
    with schema.connection() as conn:
        with schema.cursor(conn) as cur:
            execute_values(cur, sql, payload, page_size=500)
            conn.commit()
    logger.info("  -> %s: %d rows", schema.WEATHER, len(payload))
    return len(payload)


def seed_pit_stops(profile: str | None, refresh: bool = False) -> int:
    """Seed f1_pit_stops from the governed `f1.silver.fact_pit_stop` table.

    Pit stops used to be fetched a second time, independently, from Jolpica by
    harvest/laps.py - the same shape of duplication weather was before
    seed_weather() existed. src/ingestion/config.py already lists `pitstops`
    as a governed round-level endpoint, and
    src/pipeline/02c_silver_pitstops.py already parses both duration formats
    (plain seconds and M:SS.mmm) and flags real service stops vs. red-flag
    stoppages. Reading it here removes the second fetch entirely.

    Mapped explicitly, like seed_weather(): f1_pit_stops has a fixed shape
    f1_broker.race_strategy and load_strategy.build_stints() depend on
    column-by-column.
    """
    logger.info("Reading f1.silver.fact_pit_stop ...")
    rows = run_query("SELECT * FROM f1.silver.fact_pit_stop", profile,
                     cache_key="f1_pit_stops", refresh=refresh)
    logger.info("  %d pit stops", len(rows))
    if not rows:
        return 0

    payload = [
        (
            int(r["season"]), int(r["round"]), r["driver_id"],
            int(r["stop_number"]), int(r["lap"]), r["stop_time_of_day"],
            coerce(r["duration_s"], "DOUBLE PRECISION"),
        )
        for r in rows
    ]
    sql = f"""
        INSERT INTO {schema.PIT_STOPS}
            (season, round, driver_id, stop_number, lap, time_of_day, duration_s)
        VALUES %s
        ON CONFLICT (season, round, driver_id, stop_number) DO UPDATE SET
            lap=EXCLUDED.lap, time_of_day=EXCLUDED.time_of_day,
            duration_s=EXCLUDED.duration_s
    """
    with schema.connection() as conn:
        with schema.cursor(conn) as cur:
            execute_values(cur, sql, payload, page_size=500)
            conn.commit()
    logger.info("  -> %s: %d rows", schema.PIT_STOPS, len(payload))
    return len(payload)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    # No hardcoded default and no requirement: a default profile means a
    # fork's seed run silently targets a workspace its author never heard of,
    # and inside a Databricks job there is no profile at all - the runtime's
    # own identity is used. Falls back to DATABRICKS_CONFIG_PROFILE locally.
    p.add_argument("--profile", default=os.environ.get("DATABRICKS_CONFIG_PROFILE"))
    p.add_argument("--refresh", action="store_true",
                   help="re-read from Delta instead of using the cached result")
    # Job compute has no CLI profile to resolve a default warehouse from, so
    # the bundle passes this explicitly (spark_python_task parameters, not an
    # env var - see resources/f1_jobs.yml). Locally, DATABRICKS_WAREHOUSE_ID
    # or warehouse auto-discovery (_warehouse_id) cover it without this flag.
    p.add_argument("--warehouse-id", default=None)
    args = p.parse_args()
    if args.warehouse_id:
        os.environ["DATABRICKS_WAREHOUSE_ID"] = args.warehouse_id

    schema.ensure_schema()
    total = seed_weather(args.profile, args.refresh)
    total += seed_pit_stops(args.profile, args.refresh)
    for source, target, candidates in MARTS:
        total += seed(source, target, candidates, args.profile, args.refresh)

    logger.info("Deriving stints from seeded pit stops ...")
    stints = load_strategy.build_stints()
    logger.info("  -> %s: %d rows", schema.STINTS, stints)
    total += stints

    logger.info("\nSeeded %d rows across %d marts plus pit stops and stints. "
                "No further warehouse compute is needed to serve them.",
                total, len(MARTS))


if __name__ == "__main__":
    main()
