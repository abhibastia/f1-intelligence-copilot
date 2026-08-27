"""Silver — SCD Type 2 conformed dimensions.

`dim_driver` is deliberately built from **results**, not from the `/drivers`
endpoint. Every field the drivers endpoint returns (name, date of birth,
nationality, permanent number) is static, so running Auto CDC over it would
produce a dimension with no history rows at all — the SCD-2 pattern implemented
in form but never exercised. The attribute that genuinely changes within a
season is the driver's **constructor**, and that only appears in results.

Sequencing by `race_date` therefore yields real version history wherever a
driver changes team mid-season, and `track_history_column_list` restricts new
versions to team changes rather than emitting one per race.

Acceptance test — this must return rows:

    SELECT driver_id, constructor_id, __START_AT, __END_AT
    FROM f1.silver.dim_driver WHERE __END_AT IS NOT NULL;

The CDC source must be a streaming view, so these read Bronze directly rather
than the Silver facts (which are materialized views and cannot be streamed).
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Catalog comes from the pipeline `configuration` block. No default on
# purpose: a fallback of "f1" means a prod pipeline whose configuration is
# missing or misspelled writes silently into the dev catalog instead of
# failing, and nothing downstream can tell the difference afterwards.
CATALOG = spark.conf.get("f1.catalog")
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"

_DRIVER = (
    "driverId STRING, permanentNumber STRING, code STRING, url STRING, "
    "givenName STRING, familyName STRING, dateOfBirth STRING, nationality STRING"
)
_CONSTRUCTOR = "constructorId STRING, url STRING, name STRING, nationality STRING"

RESULTS_SCHEMA = (
    f"payload STRUCT<MRData STRUCT<RaceTable STRUCT<Races ARRAY<STRUCT<"
    f"season STRING, round STRING, date STRING, "
    f"Results ARRAY<STRUCT<Driver STRUCT<{_DRIVER}>, Constructor STRUCT<{_CONSTRUCTOR}>>>"
    f">>>>>"
)

CONSTRUCTORS_SCHEMA = (
    f"payload STRUCT<MRData STRUCT<ConstructorTable STRUCT<"
    f"season STRING, Constructors ARRAY<STRUCT<{_CONSTRUCTOR}>>"
    f">>>"
)


# ──────────────────────────── dim_driver ────────────────────────────────

@dp.temporary_view(name="driver_team_events")
def driver_team_events():
    """One event per driver per race carrying the team they drove for."""
    races = (
        spark.readStream.table(f"{BRONZE}.raw_results")
        .withColumn("parsed", F.from_json("raw_payload", RESULTS_SCHEMA))
        .select(F.explode("parsed.payload.MRData.RaceTable.Races").alias("race"))
    )

    events = races.select(
        F.to_date("race.date").alias("race_date"),
        F.col("race.season").cast("int").alias("season"),
        F.explode("race.Results").alias("r"),
    ).select(
        "race_date",
        "season",
        F.col("r.Driver.driverId").alias("driver_id"),
        F.col("r.Driver.code").alias("driver_code"),
        F.col("r.Driver.permanentNumber").cast("int").alias("driver_number"),
        F.col("r.Driver.givenName").alias("given_name"),
        F.col("r.Driver.familyName").alias("family_name"),
        F.concat_ws(" ", "r.Driver.givenName", "r.Driver.familyName").alias("driver_name"),
        F.to_date("r.Driver.dateOfBirth").alias("date_of_birth"),
        F.col("r.Driver.nationality").alias("nationality"),
        F.col("r.Constructor.constructorId").alias("constructor_id"),
        F.col("r.Constructor.name").alias("constructor_name"),
    )

    # Open rounds land several snapshots; identical events would otherwise tie on
    # the sequence key. Dropping them keeps the CDC input unambiguous.
    return events.filter(
        F.col("driver_id").isNotNull() & F.col("race_date").isNotNull()
    ).dropDuplicates(["driver_id", "race_date"])


dp.create_streaming_table(
    name=f"{SILVER}.dim_driver",
    comment=(
        "SCD Type 2 driver dimension. A new version is written when a driver "
        "changes constructor; __END_AT IS NULL marks the current row."
    ),
    table_properties={"quality": "silver"},
)

dp.create_auto_cdc_flow(
    target=f"{SILVER}.dim_driver",
    source="driver_team_events",
    keys=["driver_id"],
    sequence_by="race_date",
    stored_as_scd_type=2,
    # Only team changes open a new version — without this every race would.
    track_history_column_list=["constructor_id", "constructor_name"],
)


# ────────────────────────── dim_constructor ─────────────────────────────

@dp.temporary_view(name="constructor_events")
def constructor_events():
    """One event per constructor per season, for cross-season rebrands."""
    events = (
        spark.readStream.table(f"{BRONZE}.raw_constructors")
        .withColumn("parsed", F.from_json("raw_payload", CONSTRUCTORS_SCHEMA))
        .select(
            F.col("parsed.payload.MRData.ConstructorTable.season").cast("int").alias("season"),
            F.explode("parsed.payload.MRData.ConstructorTable.Constructors").alias("c"),
        )
        .select(
            "season",
            F.col("c.constructorId").alias("constructor_id"),
            F.col("c.name").alias("constructor_name"),
            F.col("c.nationality").alias("nationality"),
            F.col("c.url").alias("wikipedia_url"),
        )
    )
    return events.filter(F.col("constructor_id").isNotNull()).dropDuplicates(
        ["constructor_id", "season"]
    )


dp.create_streaming_table(
    name=f"{SILVER}.dim_constructor",
    comment=(
        "SCD Type 2 constructor dimension, sequenced by season so team rebrands "
        "are preserved. __END_AT IS NULL marks the current row."
    ),
    table_properties={"quality": "silver"},
)

dp.create_auto_cdc_flow(
    target=f"{SILVER}.dim_constructor",
    source="constructor_events",
    keys=["constructor_id"],
    sequence_by="season",
    stored_as_scd_type=2,
    track_history_column_list=["constructor_name", "nationality"],
)
