"""Databricks task: seed the Delta Gold marts into Lakebase.

Reads Gold with Spark rather than through the SQL warehouse, which is both
cheaper and the natural thing to do from inside a job. Delta stays the source of
truth; Lakebase is the serving copy the agent and apps read, so a page render or
an agent turn costs no warehouse compute.
"""
import logging, os, sys

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("seed-job")
_here = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
sys.path.insert(0, os.path.dirname(_here))

from pyspark.sql import SparkSession
from psycopg2.extras import execute_values
from f1lake import schema

spark = SparkSession.builder.getOrCreate()
schema.ensure_schema()

MARTS = [("f1.gold.driver_performance", "f1_driver_performance",
          ["season", "round", "driver_id"]),
         ("f1.gold.championship_progression", "f1_championship",
          ["season", "round", "driver_id"]),
         ("f1.gold.race_strategy", schema.RACE_STRATEGY_SUMMARY,
          ["season", "round", "driver_id"]),
         ("f1.gold.lap_pace", schema.LAP_PACE,
          ["season", "round", "driver_id"]),
         ("f1.gold.constructor_standings", schema.CONSTRUCTOR_STANDINGS,
          ["season", "round", "constructor_id"])]

# race_conditions -> f1_race_weather needs the same explicit column mapping as
# f1lake.seed_gold.seed_weather() (the target table's shape is fixed, read
# column-by-column by f1_broker). Not reproduced here: this task type is the
# unverified path (see resources/f1_jobs.yml) — `python -m f1lake.seed_gold`,
# run locally, is what actually seeds weather today.

PG = {"bigint": "BIGINT", "int": "BIGINT", "long": "BIGINT", "smallint": "BIGINT",
      "double": "DOUBLE PRECISION", "float": "DOUBLE PRECISION",
      "boolean": "BOOLEAN", "date": "DATE", "timestamp": "TIMESTAMPTZ"}

for source, target, keys in MARTS:
    df = spark.table(source)
    cols = df.columns
    types = {f.name: PG.get(f.dataType.simpleString().split("(")[0], "TEXT")
             for f in df.schema.fields}
    rows = [tuple(r[c] for c in cols) for r in df.collect()]
    log.info("%s -> %s: %d rows, %d columns", source, target, len(rows), len(cols))

    ddl = ",\n  ".join(f'"{c}" {types[c]}' for c in cols)
    with schema.connection() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            # Recreated each run: the mart is derived, Delta is the source of
            # truth, and a stale column from an earlier shape would be a lie.
            cur.execute(f"DROP TABLE IF EXISTS {target}")
            cur.execute(f'CREATE TABLE {target} (\n  {ddl},\n  '
                        f'PRIMARY KEY ({", ".join(chr(34)+k+chr(34) for k in keys)}))')

    seen = {}
    for row in rows:
        seen[tuple(row[cols.index(k)] for k in keys)] = row
    with schema.connection() as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                f'INSERT INTO {target} ({", ".join(chr(34)+c+chr(34) for c in cols)}) VALUES %s',
                list(seen.values()), page_size=500)
            conn.commit()
    log.info("  seeded %d row(s)", len(seen))
