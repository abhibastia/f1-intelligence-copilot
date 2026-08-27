"""Silver — lap times, the pace layer.

One row per driver per lap: position and lap time. This is the finest grain in
the project and the only place that can answer "who was actually fast", as
opposed to "who finished ahead" — a driver can be quicker all afternoon and
still lose the race in the pit lane.

THREE LEVELS OF NESTING, NOT TWO
--------------------------------
Every other endpoint nests twice: a race holds an array of records. Laps nest
three times — a race holds laps, and each lap holds one timing per driver. That
is why `total` reports 1,008 for a 53-lap race with 20 cars, and why the
pagination counter had to learn about Timings specifically.

It also means pages split mid-lap. A 53-lap race comes back as ~61 lap elements
because lap 5's timings can straddle a page boundary, appearing twice with the
same number and different drivers. Deduplicating on (season, round, lap,
driver_id) rather than on the lap element is what makes that a non-event.

LAP TIMES ARE M:SS.mmm, ALWAYS
------------------------------
Unlike pit stop durations, which switch format, a lap time is always
`1:27.623`. It is still parsed rather than cast, because casting returns NULL
and a NULL lap time is indistinguishable from a lap nobody set.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Catalog comes from the pipeline `configuration` block. No default on
# purpose: a fallback of "f1" means a prod pipeline whose configuration is
# missing or misspelled writes silently into the dev catalog instead of
# failing, and nothing downstream can tell the difference afterwards.
CATALOG = spark.conf.get("f1.catalog")
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"

LAPS_SCHEMA = (
    "payload STRUCT<MRData STRUCT<RaceTable STRUCT<Races ARRAY<STRUCT<"
    "  season STRING, round STRING,"
    "  Laps ARRAY<STRUCT<"
    "    number STRING,"
    "    Timings ARRAY<STRUCT<driverId STRING, position STRING, time STRING>>"
    "  >>"
    ">>>>>"
)

# `1:27.623` -> 87.623. Split rather than regex: a lap under a minute would have
# no colon at all, and a red-flagged lap can exceed ten minutes.
LAP_SECONDS = F.when(
    F.col("lap_time_raw").contains(":"),
    F.split(F.col("lap_time_raw"), ":")[0].cast("double") * 60
    + F.split(F.col("lap_time_raw"), ":")[1].cast("double"),
).otherwise(F.col("lap_time_raw").cast("double"))

LAP_RULES = {
    "season_present": "season IS NOT NULL",
    "round_present": "round IS NOT NULL",
    "driver_present": "driver_id IS NOT NULL",
    "lap_present": "lap IS NOT NULL",
    "plausible_lap": "lap BETWEEN 1 AND 100",
    "position_present": "position IS NOT NULL",
    "lap_time_parsed": "lap_time_s IS NOT NULL",
    # The slowest legitimate racing lap in this era is comfortably under three
    # minutes; the fastest is over fifty seconds. Outside that the string was
    # parsed wrong rather than the lap being unusual.
    "plausible_lap_time": "lap_time_s BETWEEN 40 AND 300",
}


def quarantine_reason(rules: dict[str, str]):
    """Name every rule this row violates, semicolon separated."""
    return F.concat_ws(
        ";",
        *[F.when(~F.expr(rule), F.lit(name)) for name, rule in rules.items()],
    )


@dp.temporary_view(name="stg_lap")
def stg_lap():
    """Three explodes: races, then laps, then the per-driver timings."""
    df = (
        spark.read.table(f"{BRONZE}.raw_laps")
        .withColumn("parsed", F.from_json("raw_payload", LAPS_SCHEMA))
        .select(
            "_ingest_ts",
            "_file_path",
            F.explode("parsed.payload.MRData.RaceTable.Races").alias("race"),
        )
        .select(
            "_ingest_ts",
            "_file_path",
            F.col("race.season").cast("int").alias("season"),
            F.col("race.round").cast("int").alias("round"),
            F.explode("race.Laps").alias("lap_rec"),
        )
        .select(
            "_ingest_ts",
            "_file_path",
            "season",
            "round",
            F.col("lap_rec.number").cast("int").alias("lap"),
            F.explode("lap_rec.Timings").alias("t"),
        )
        .select(
            "season",
            "round",
            "lap",
            F.col("t.driverId").alias("driver_id"),
            F.col("t.position").cast("int").alias("position"),
            F.col("t.time").alias("lap_time_raw"),
            "_ingest_ts",
            "_file_path",
        )
        .withColumn("lap_time_s", LAP_SECONDS)
    )

    # On (season, round, lap, driver_id), not on the lap element: pages split
    # mid-lap, so the same lap number legitimately appears more than once with
    # different drivers.
    window = Window.partitionBy("season", "round", "lap", "driver_id").orderBy(
        F.col("_ingest_ts").desc(), F.col("_file_path").desc()
    )
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


@dp.materialized_view(
    name=f"{SILVER}.fact_lap",
    comment=(
        "One row per driver per lap: running position and lap time in seconds. "
        "The finest grain in the project — the only source that can separate "
        "pace from finishing order."
    ),
    table_properties={"quality": "silver"},
    cluster_by=["season", "round"],
)
@dp.expect_all_or_drop(LAP_RULES)
@dp.expect_or_fail("plausible_season", "season BETWEEN 1950 AND 2100")
def fact_lap():
    return spark.read.table("stg_lap")


@dp.materialized_view(
    name=f"{SILVER}.quarantine_lap",
    comment="Lap rows rejected by fact_lap quality rules, with the reason.",
)
def quarantine_lap():
    rules = " AND ".join(f"({rule})" for rule in LAP_RULES.values())
    return (
        spark.read.table("stg_lap")
        .filter(f"NOT ({rules})")
        .withColumn("_quarantine_reason", quarantine_reason(LAP_RULES))
        .withColumn("_quarantined_at", F.current_timestamp())
    )
