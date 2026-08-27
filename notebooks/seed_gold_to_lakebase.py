# Databricks notebook source
# MAGIC %md
# MAGIC # Seed the Delta Gold marts into Lakebase
# MAGIC
# MAGIC Delta stays the source of truth; Lakebase is the serving copy the agent
# MAGIC and both apps read, so an agent turn or a page render costs no warehouse
# MAGIC compute.
# MAGIC
# MAGIC Written as a **notebook task** rather than a `spark_python_task`. The
# MAGIC script form crashed the serverless kernel repeatedly with "The Python
# MAGIC process exited unexpectedly", while notebook tasks doing the same
# MAGIC psycopg2 + Spark work run cleanly — so the task type was the variable,
# MAGIC not the code.

# COMMAND ----------
# MAGIC %pip install -q psycopg2-binary 'databricks-sdk>=0.30.0'

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("lakebase_secret_scope", "database", "Lakebase secret scope")
dbutils.widgets.text("lakebase_secret_key", "lakebase-url", "Lakebase secret key")
SCOPE = dbutils.widgets.get("lakebase_secret_scope")
KEY = dbutils.widgets.get("lakebase_secret_key")

import base64
from databricks.sdk import WorkspaceClient
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor

URL = base64.b64decode(
    WorkspaceClient().secrets.get_secret(scope=SCOPE, key=KEY).value).decode()

MARTS = [("f1.gold.driver_performance", "f1_driver_performance",
          ["season", "round", "driver_id"]),
         ("f1.gold.championship_progression", "f1_championship",
          ["season", "round", "driver_id"]),
         ("f1.gold.race_strategy", "f1_race_strategy_summary",
          ["season", "round", "driver_id"]),
         ("f1.gold.lap_pace", "f1_lap_pace",
          ["season", "round", "driver_id"]),
         ("f1.gold.constructor_standings", "f1_constructor_standings",
          ["season", "round", "constructor_id"])]

# race_conditions -> f1_race_weather needs the explicit column mapping in
# f1lake.seed_gold.seed_weather() (fixed target shape, read column-by-column by
# f1_broker) and is not reproduced here — this notebook task is the unverified
# path (see resources/f1_jobs.yml); `python -m f1lake.seed_gold`, run locally,
# is what actually seeds weather today.

PG = {"bigint": "BIGINT", "int": "BIGINT", "long": "BIGINT", "smallint": "BIGINT",
      "double": "DOUBLE PRECISION", "float": "DOUBLE PRECISION",
      "boolean": "BOOLEAN", "date": "DATE", "timestamp": "TIMESTAMPTZ"}

for source, target, keys in MARTS:
    df = spark.table(source)
    cols = df.columns
    types = {f.name: PG.get(f.dataType.simpleString().split("(")[0], "TEXT")
             for f in df.schema.fields}
    rows = [tuple(r[c] for c in cols) for r in df.collect()]
    print(f"{source} -> {target}: {len(rows)} rows, {len(cols)} columns")

    ddl = ",\n  ".join(f'"{c}" {types[c]}' for c in cols)
    keydef = ", ".join(f'"{k}"' for k in keys)
    with psycopg2.connect(URL) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            # Recreated each run: the mart is derived and Delta is the source of
            # truth, so a stale column from an earlier shape would be a lie.
            cur.execute(f"DROP TABLE IF EXISTS {target}")
            cur.execute(f'CREATE TABLE {target} (\n  {ddl},\n  PRIMARY KEY ({keydef}))')

    seen = {}
    for row in rows:
        seen[tuple(row[cols.index(k)] for k in keys)] = row
    collist = ", ".join(f'"{c}"' for c in cols)
    with psycopg2.connect(URL) as conn:
        with conn.cursor() as cur:
            execute_values(cur, f"INSERT INTO {target} ({collist}) VALUES %s",
                           list(seen.values()), page_size=500)
            conn.commit()
    print(f"  seeded {len(seen)} row(s)")

# COMMAND ----------
with psycopg2.connect(URL, cursor_factory=RealDictCursor) as conn:
    with conn.cursor() as cur:
        cur.execute("""SELECT (SELECT count(*) FROM f1_driver_performance) perf,
                              (SELECT count(*) FROM f1_championship) champ""")
        print("Lakebase now holds:", dict(cur.fetchone()))
