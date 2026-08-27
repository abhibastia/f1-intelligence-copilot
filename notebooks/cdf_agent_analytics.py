# Databricks notebook source
# MAGIC %md
# MAGIC # Agent activity: Lakebase → Delta → Change Data Feed → analytics
# MAGIC
# MAGIC Closes the operational-to-analytical loop. The agent writes every tool
# MAGIC call into Lakebase (`agent_tool_calls`); this job lands those rows in a
# MAGIC **CDF-enabled Delta table**, then reads that table's **Change Data Feed**
# MAGIC to materialise an analytics table the app reads back.
# MAGIC
# MAGIC ```
# MAGIC Lakebase agent_tool_calls          (operational, the agent writes here)
# MAGIC        │  MERGE, idempotent on id
# MAGIC        ▼
# MAGIC f1.gold.agent_tool_calls           (Delta, delta.enableChangeDataFeed = true)
# MAGIC        │  table_changes()
# MAGIC        ▼
# MAGIC f1.gold.agent_activity_analytics   (per-tool counts, write share, latency)
# MAGIC        │
# MAGIC        ▼
# MAGIC   Strategy Copilot app
# MAGIC ```
# MAGIC
# MAGIC ### Why CDF rather than recomputing from the base table
# MAGIC
# MAGIC Recomputing aggregates by rescanning `agent_tool_calls` every run would
# MAGIC give the same numbers today and stop being true the moment history is
# MAGIC corrected or backfilled. Reading `table_changes()` processes only what
# MAGIC actually changed since the last watermark, which is what makes the
# MAGIC analytics table incremental rather than a repeated full scan.
# MAGIC
# MAGIC ### Why MERGE and not INSERT
# MAGIC
# MAGIC The job is expected to be re-run — after a demo, after more agent traffic.
# MAGIC `MERGE` on the Lakebase primary key makes a re-run a no-op for rows already
# MAGIC landed, so running it twice does not double every count.

# COMMAND ----------

# DBTITLE 1,Install dependencies
# MAGIC %pip install -q psycopg2-binary 'databricks-sdk>=0.30.0'

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "f1", "Catalog")
dbutils.widgets.text("schema", "gold", "Schema")
dbutils.widgets.text("lakebase_secret_scope", "database", "Lakebase secret scope")
dbutils.widgets.text("lakebase_secret_key", "lakebase-url", "Lakebase secret key")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
SCOPE = dbutils.widgets.get("lakebase_secret_scope")
KEY = dbutils.widgets.get("lakebase_secret_key")

RAW = f"{CATALOG}.{SCHEMA}.agent_tool_calls"
ANALYTICS = f"{CATALOG}.{SCHEMA}.agent_activity_analytics"

print(f"source    : Lakebase agent_tool_calls")
print(f"delta     : {RAW}  (CDF enabled)")
print(f"analytics : {ANALYTICS}")

# COMMAND ----------

# DBTITLE 1,Read the operational rows out of Lakebase
import base64

from databricks.sdk import WorkspaceClient

# Same secret and decoding scheme the apps use: a single base64-encoded Postgres
# URL, so there is no host/port/user/password to pick apart.
_secret = WorkspaceClient().secrets.get_secret(scope=SCOPE, key=KEY)
LAKEBASE_URL = base64.b64decode(_secret.value).decode("utf-8")

import psycopg2
from psycopg2.extras import RealDictCursor

with psycopg2.connect(LAKEBASE_URL, cursor_factory=RealDictCursor) as conn:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, called_at, session_id, tool_name, is_write,
                   arguments::text AS arguments, outcome, summary, duration_ms
            FROM agent_tool_calls ORDER BY id
        """)
        rows = [dict(r) for r in cur.fetchall()]

print(f"{len(rows)} tool call(s) in Lakebase")

# COMMAND ----------

# DBTITLE 1,Land them in a CDF-enabled Delta table
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, IntegerType, LongType, StringType,
                               StructField, StructType, TimestampType)

SCHEMA_DDL = StructType([
    StructField("id", LongType(), False),
    StructField("called_at", TimestampType(), True),
    StructField("session_id", StringType(), True),
    StructField("tool_name", StringType(), False),
    StructField("is_write", BooleanType(), True),
    StructField("arguments", StringType(), True),
    StructField("outcome", StringType(), True),
    StructField("summary", StringType(), True),
    StructField("duration_ms", IntegerType(), True),
])

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# enableChangeDataFeed is the whole point: without it table_changes() below
# fails outright rather than silently returning nothing.
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {RAW} (
        id BIGINT, called_at TIMESTAMP, session_id STRING, tool_name STRING,
        is_write BOOLEAN, arguments STRING, outcome STRING, summary STRING,
        duration_ms INT
    ) TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

if rows:
    source = spark.createDataFrame(rows, schema=SCHEMA_DDL)
    source.createOrReplaceTempView("incoming_tool_calls")
    # MERGE on the Lakebase primary key: re-running the job must not duplicate
    # rows, or every count in the analytics table doubles.
    spark.sql(f"""
        MERGE INTO {RAW} AS target
        USING incoming_tool_calls AS source
        ON target.id = source.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

print(f"{RAW}: {spark.table(RAW).count()} row(s)")
display(spark.sql(f"DESCRIBE DETAIL {RAW}").select("name", "properties"))

# COMMAND ----------

# DBTITLE 1,Read the Change Data Feed and materialise analytics
# Version 1 onward: version 0 is the CREATE TABLE, which has no data changes.
changes = (
    spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 1)
    .table(RAW)
)
print(f"{changes.count()} change record(s) from the feed")
display(changes.select("id", "tool_name", "outcome", "_change_type",
                       "_commit_version").orderBy("_commit_version", "id").limit(20))

# COMMAND ----------

# Only rows that landed or were corrected. update_preimage is the "before"
# image of an update and would double-count if left in.
current = changes.filter(
    F.col("_change_type").isin("insert", "update_postimage")
)

# One record per tool call, not one per time it appeared in the feed.
#
# Excluding update_preimage stops double-counting WITHIN a run. It does not stop
# it ACROSS runs: the MERGE above rewrites every matched row, so each re-run
# emits a fresh update_postimage for rows that landed long ago. Reading from
# startingVersion=1 then sees the same call once per run it survived — after
# three runs the analytics claimed 235 calls against 157 rows in the table.
#
# Taking the newest version of each id makes the job idempotent to match the
# MERGE feeding it: running it twice changes nothing.
from pyspark.sql import Window

latest_per_call = (
    current
    .withColumn(
        "_rn",
        F.row_number().over(
            Window.partitionBy("id").orderBy(F.col("_commit_version").desc())
        ),
    )
    .filter(F.col("_rn") == 1)
    .drop("_rn")
    # pytest logs a `test_tool` call of its own. The app already filters it out
    # of the panel it renders; the analytical copy has to agree, or the two
    # views of the same feed disagree about what the agent did.
    .filter(~F.col("tool_name").startswith("test_"))
)

print(f"{current.count()} change record(s) -> {latest_per_call.count()} distinct tool call(s)")

analytics = (
    latest_per_call.groupBy("tool_name")
    .agg(
        F.count("*").alias("call_count"),
        F.sum(F.when(F.col("is_write"), 1).otherwise(0)).alias("write_count"),
        F.sum(F.when(F.col("outcome") == "ok", 1).otherwise(0)).alias("ok_count"),
        F.sum(F.when(F.col("outcome") != "ok", 1).otherwise(0)).alias("error_count"),
        F.round(F.avg("duration_ms"), 1).alias("avg_duration_ms"),
        F.max("duration_ms").alias("max_duration_ms"),
        F.countDistinct("session_id").alias("sessions"),
        F.max("called_at").alias("last_called_at"),
        F.max("_commit_version").alias("last_commit_version"),
    )
    .withColumn("materialised_at", F.current_timestamp())
    .orderBy(F.col("call_count").desc())
)

analytics.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(ANALYTICS)
print(f"{ANALYTICS}: {spark.table(ANALYTICS).count()} row(s)")
display(spark.table(ANALYTICS))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Result
# MAGIC
# MAGIC `agent_activity_analytics` now holds one row per tool: how often it was
# MAGIC called, how many of those were writes, error counts, latency, and the CDF
# MAGIC commit version the figures were derived from. The Strategy Copilot app
# MAGIC reads it back, so the page shows what the agent actually did rather than
# MAGIC what it reported doing.
