"""Silver — pit stops, the strategy layer.

One row per stop: which driver, which lap, how long. This is what separates
"who won" from "how they won" — a driver on one stop while a rival ran three
means the race was decided in the pit lane rather than on track.

TWO DURATION FORMATS, AND WHY IT MATTERS
----------------------------------------
Jolpica returns `duration` as a string in one of two shapes: `21.789` for a
normal stop, and `M:SS.mmm` — `12:57.770`, `35:54.149` — when the car sat in
the pit lane through a red flag or suspension. 83 of 2,081 stops in this dataset
use the second form.

Casting straight to DOUBLE returns NULL for those, so a naive parse silently
drops 4% of stops — and not a random 4%: precisely the ones where something
unusual happened. Both forms are parsed here.

That creates the opposite problem. A 35-minute stoppage is a real duration but
not a pit stop in any useful sense, and averaging it in makes a race look like
its crew forgot how to change tyres. `is_service_stop` separates the two so the
mart can count every stop while timing only the ones that were stops.
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

# A pit stop is ~20-35 s. Anything past two minutes is a stoppage, not service.
SERVICE_STOP_MAX_SECONDS = 120.0

PITSTOPS_SCHEMA = (
    "payload STRUCT<MRData STRUCT<RaceTable STRUCT<Races ARRAY<STRUCT<"
    "  season STRING, round STRING, raceName STRING, date STRING,"
    "  PitStops ARRAY<STRUCT<"
    "    driverId STRING, lap STRING, stop STRING, time STRING, duration STRING"
    "  >>"
    ">>>>>"
)

# `duration` is M:SS.mmm or plain seconds. Split on the colon rather than
# matching a format: the minutes component has no fixed width and a regex that
# assumes one would fail on a 35-minute red flag.
DURATION_SECONDS = F.when(
    F.col("duration_raw").contains(":"),
    F.split(F.col("duration_raw"), ":")[0].cast("double") * 60
    + F.split(F.col("duration_raw"), ":")[1].cast("double"),
).otherwise(F.col("duration_raw").cast("double"))

PIT_STOP_RULES = {
    "season_present": "season IS NOT NULL",
    "round_present": "round IS NOT NULL",
    "driver_present": "driver_id IS NOT NULL",
    "lap_present": "lap IS NOT NULL",
    "plausible_lap": "lap BETWEEN 1 AND 100",
    "stop_number_present": "stop_number IS NOT NULL",
    "duration_parsed": "duration_s IS NOT NULL",
    "non_negative_duration": "duration_s >= 0",
}


def quarantine_reason(rules: dict[str, str]):
    """Name every rule this row violates, semicolon separated."""
    return F.concat_ws(
        ";",
        *[F.when(~F.expr(rule), F.lit(name)) for name, rule in rules.items()],
    )


@dp.temporary_view(name="stg_pit_stop")
def stg_pit_stop():
    """Two explodes: the outer array holds one race, the inner holds the stops."""
    df = (
        spark.read.table(f"{BRONZE}.raw_pitstops")
        .withColumn("parsed", F.from_json("raw_payload", PITSTOPS_SCHEMA))
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
            F.explode("race.PitStops").alias("s"),
        )
        .select(
            "season",
            "round",
            F.col("s.driverId").alias("driver_id"),
            F.col("s.lap").cast("int").alias("lap"),
            F.col("s.stop").cast("int").alias("stop_number"),
            F.col("s.time").alias("stop_time_of_day"),
            F.col("s.duration").alias("duration_raw"),
            "_ingest_ts",
            "_file_path",
        )
        .withColumn("duration_s", DURATION_SECONDS)
        .withColumn(
            "is_service_stop",
            F.col("duration_s") < F.lit(SERVICE_STOP_MAX_SECONDS),
        )
    )

    window = Window.partitionBy("season", "round", "driver_id", "stop_number").orderBy(
        F.col("_ingest_ts").desc(), F.col("_file_path").desc()
    )
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


@dp.materialized_view(
    name=f"{SILVER}.fact_pit_stop",
    comment=(
        "One row per pit stop: driver, lap, stop number and duration in seconds. "
        "Durations arrive as plain seconds or M:SS.mmm; both are parsed. "
        "is_service_stop separates real tyre changes from red-flag stoppages."
    ),
    table_properties={"quality": "silver"},
    cluster_by=["season", "round"],
)
@dp.expect_all_or_drop(PIT_STOP_RULES)
@dp.expect_or_fail("plausible_season", "season BETWEEN 1950 AND 2100")
def fact_pit_stop():
    return spark.read.table("stg_pit_stop")


@dp.materialized_view(
    name=f"{SILVER}.quarantine_pit_stop",
    comment="Pit stop rows rejected by fact_pit_stop quality rules, with the reason.",
)
def quarantine_pit_stop():
    rules = " AND ".join(f"({rule})" for rule in PIT_STOP_RULES.values())
    return (
        spark.read.table("stg_pit_stop")
        .filter(f"NOT ({rules})")
        .withColumn("_quarantine_reason", quarantine_reason(PIT_STOP_RULES))
        .withColumn("_quarantined_at", F.current_timestamp())
    )
