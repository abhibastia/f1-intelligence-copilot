"""Silver — flattened, typed, deduplicated facts with quality enforcement.

Three things happen here, in order:

1. **Parse.** Bronze holds raw JSON text. Every payload is parsed with an
   explicit schema declared below — `from_json` ignores fields we don't list, so
   optional Ergast members (FastestLap, Sprint, Q3) are absent-safe by
   construction rather than by inference.

2. **Deduplicate.** The landing zone re-pulls the open round on every run, so a
   round can have several snapshots. Each fact keeps only the newest row per
   natural key. Skipping this double-counts the live round in Gold — it is the
   second half of the idempotency contract described in landing_writer.py.

3. **Enforce.** Structural rules drop bad rows *and* route them to a companion
   quarantine table, so nothing is silently lost. Both are generated from the
   same rule dict, which is why the two can never disagree.

These are materialized views, not streaming tables: deduplication is a
full-partition window function, which streaming append-mode cannot express.
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
GOLD = f"{CATALOG}.gold"

# ─────────────────────────── payload schemas ────────────────────────────
# Written as DDL strings: verbose, but unambiguous and reviewable against the
# API response. Every leaf is STRING — Ergast returns everything as text, and
# casting is an explicit, testable step below.

_DRIVER = (
    "driverId STRING, permanentNumber STRING, code STRING, url STRING, "
    "givenName STRING, familyName STRING, dateOfBirth STRING, nationality STRING"
)
_CONSTRUCTOR = "constructorId STRING, url STRING, name STRING, nationality STRING"
_CIRCUIT = (
    "circuitId STRING, url STRING, circuitName STRING, "
    "Location STRUCT<lat STRING, long STRING, locality STRING, country STRING>"
)

_RESULT_ITEM = (
    f"number STRING, position STRING, positionText STRING, points STRING, "
    f"Driver STRUCT<{_DRIVER}>, Constructor STRUCT<{_CONSTRUCTOR}>, "
    f"grid STRING, laps STRING, status STRING, "
    f"Time STRUCT<millis STRING, time STRING>, "
    f"FastestLap STRUCT<rank STRING, lap STRING, Time STRUCT<time STRING>, "
    f"AverageSpeed STRUCT<units STRING, speed STRING>>"
)

_QUALI_ITEM = (
    f"number STRING, position STRING, "
    f"Driver STRUCT<{_DRIVER}>, Constructor STRUCT<{_CONSTRUCTOR}>, "
    f"Q1 STRING, Q2 STRING, Q3 STRING"
)


def _race_table_schema(inner_array: str, inner_item: str) -> str:
    return (
        f"payload STRUCT<MRData STRUCT<RaceTable STRUCT<Races ARRAY<STRUCT<"
        f"season STRING, round STRING, url STRING, raceName STRING, "
        f"Circuit STRUCT<{_CIRCUIT}>, date STRING, time STRING, "
        f"{inner_array} ARRAY<STRUCT<{inner_item}>>"
        f">>>>>"
    )


RESULTS_SCHEMA = _race_table_schema("Results", _RESULT_ITEM)
QUALI_SCHEMA = _race_table_schema("QualifyingResults", _QUALI_ITEM)
SPRINT_SCHEMA = _race_table_schema("SprintResults", _RESULT_ITEM)

RACES_SCHEMA = (
    f"payload STRUCT<MRData STRUCT<RaceTable STRUCT<Races ARRAY<STRUCT<"
    f"season STRING, round STRING, url STRING, raceName STRING, "
    f"Circuit STRUCT<{_CIRCUIT}>, date STRING, time STRING"
    f">>>>>"
)

DRIVER_STANDINGS_SCHEMA = (
    f"payload STRUCT<MRData STRUCT<StandingsTable STRUCT<StandingsLists ARRAY<STRUCT<"
    f"season STRING, round STRING, DriverStandings ARRAY<STRUCT<"
    f"position STRING, positionText STRING, points STRING, wins STRING, "
    f"Driver STRUCT<{_DRIVER}>, Constructors ARRAY<STRUCT<{_CONSTRUCTOR}>>"
    f">>>>>>>"
)

CONSTRUCTOR_STANDINGS_SCHEMA = (
    f"payload STRUCT<MRData STRUCT<StandingsTable STRUCT<StandingsLists ARRAY<STRUCT<"
    f"season STRING, round STRING, ConstructorStandings ARRAY<STRUCT<"
    f"position STRING, positionText STRING, points STRING, wins STRING, "
    f"Constructor STRUCT<{_CONSTRUCTOR}>"
    f">>>>>>>"
)


# ─────────────────────────── shared helpers ─────────────────────────────

def parse(table: str, schema: str, path: str):
    """Read a Bronze table and explode one payload array into rows."""
    return (
        spark.read.table(table)
        .withColumn("parsed", F.from_json("raw_payload", schema))
        .select("_ingest_ts", "_file_path", F.explode(F.col(path)).alias("item"))
    )


def dedupe(df, keys: list[str]):
    """Keep the newest snapshot per natural key.

    Ordered by _ingest_ts descending, with _file_path as a deterministic
    tie-break so two snapshots written in the same second can't shuffle.
    """
    window = Window.partitionBy(*keys).orderBy(
        F.col("_ingest_ts").desc(), F.col("_file_path").desc()
    )
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def quarantine_reason(rules: dict[str, str]):
    """Name every rule this row violates, semicolon separated."""
    return F.concat_ws(
        ";",
        F.array(*[F.when(F.expr(f"NOT ({cond})"), F.lit(name)) for name, cond in rules.items()]),
    )


def invalid_predicate(rules: dict[str, str]) -> str:
    return " OR ".join(f"NOT ({cond})" for cond in rules.values())


# Time strings like "1:29.179" or "22.457" → milliseconds.
def lap_time_millis(column: str):
    parts = F.split(F.col(column), ":")
    seconds_only = parts.getItem(0).cast("double")
    minutes = parts.getItem(0).cast("double") * 60 + parts.getItem(1).cast("double")
    return F.when(F.col(column).isNull() | (F.col(column) == ""), None).otherwise(
        F.when(F.size(parts) == 2, minutes).otherwise(seconds_only) * 1000
    ).cast("long")


# ────────────────────────────── dim_race ────────────────────────────────

RACE_RULES = {
    "season_present": "season IS NOT NULL",
    "round_present": "round IS NOT NULL",
    "race_date_present": "race_date IS NOT NULL",
    "circuit_present": "circuit_id IS NOT NULL",
}


@dp.temporary_view(name="stg_race")
def stg_race():
    df = parse(f"{BRONZE}.raw_races", RACES_SCHEMA, "parsed.payload.MRData.RaceTable.Races")
    df = df.select(
        F.col("item.season").cast("int").alias("season"),
        F.col("item.round").cast("int").alias("round"),
        F.col("item.raceName").alias("race_name"),
        F.to_date("item.date").alias("race_date"),
        F.col("item.time").alias("race_time_utc"),
        F.col("item.url").alias("wikipedia_url"),
        F.col("item.Circuit.circuitId").alias("circuit_id"),
        F.col("item.Circuit.circuitName").alias("circuit_name"),
        F.col("item.Circuit.Location.locality").alias("circuit_locality"),
        F.col("item.Circuit.Location.country").alias("circuit_country"),
        F.col("item.Circuit.Location.lat").cast("double").alias("circuit_lat"),
        F.col("item.Circuit.Location.long").cast("double").alias("circuit_long"),
        "_ingest_ts",
        "_file_path",
    )
    return dedupe(df, ["season", "round"])


@dp.materialized_view(
    name=f"{SILVER}.dim_race",
    comment="One row per race: schedule, circuit, and location. Conformed race dimension.",
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop(RACE_RULES)
@dp.expect_or_fail("plausible_season", "season BETWEEN 1950 AND 2100")
def dim_race():
    return spark.read.table("stg_race")


@dp.materialized_view(
    name=f"{SILVER}.quarantine_race",
    comment="Race rows rejected by dim_race quality rules, with the reason.",
)
def quarantine_race():
    return (
        spark.read.table("stg_race")
        .filter(F.expr(invalid_predicate(RACE_RULES)))
        .withColumn("_quarantine_reason", quarantine_reason(RACE_RULES))
        .withColumn("_quarantined_at", F.current_timestamp())
    )


# ───────────────────────────── fact_result ──────────────────────────────

RESULT_RULES = {
    "driver_id_present": "driver_id IS NOT NULL",
    "constructor_id_present": "constructor_id IS NOT NULL",
    "season_present": "season IS NOT NULL",
    "round_present": "round IS NOT NULL",
    "race_date_present": "race_date IS NOT NULL",
    "classification_present": "position IS NOT NULL OR position_text IS NOT NULL",
}


@dp.temporary_view(name="stg_result")
def stg_result():
    races = parse(
        f"{BRONZE}.raw_results", RESULTS_SCHEMA, "parsed.payload.MRData.RaceTable.Races"
    )
    df = races.select(
        F.col("item.season").cast("int").alias("season"),
        F.col("item.round").cast("int").alias("round"),
        F.to_date("item.date").alias("race_date"),
        F.col("item.raceName").alias("race_name"),
        F.col("item.Circuit.circuitId").alias("circuit_id"),
        F.explode("item.Results").alias("r"),
        "_ingest_ts",
        "_file_path",
    )
    df = df.select(
        "season",
        "round",
        "race_date",
        "race_name",
        "circuit_id",
        F.col("r.Driver.driverId").alias("driver_id"),
        F.col("r.Driver.code").alias("driver_code"),
        F.col("r.Driver.permanentNumber").cast("int").alias("driver_number"),
        F.col("r.Driver.givenName").alias("driver_given_name"),
        F.col("r.Driver.familyName").alias("driver_family_name"),
        F.concat_ws(" ", "r.Driver.givenName", "r.Driver.familyName").alias("driver_name"),
        F.to_date("r.Driver.dateOfBirth").alias("driver_dob"),
        F.col("r.Driver.nationality").alias("driver_nationality"),
        F.col("r.Constructor.constructorId").alias("constructor_id"),
        F.col("r.Constructor.name").alias("constructor_name"),
        F.col("r.Constructor.nationality").alias("constructor_nationality"),
        F.col("r.grid").cast("int").alias("grid_position"),
        F.col("r.position").cast("int").alias("position"),
        F.col("r.positionText").alias("position_text"),
        F.col("r.points").cast("double").alias("points"),
        F.col("r.laps").cast("int").alias("laps_completed"),
        F.col("r.status").alias("status"),
        F.col("r.Time.millis").cast("long").alias("total_race_millis"),
        F.col("r.FastestLap.rank").cast("int").alias("fastest_lap_rank"),
        F.col("r.FastestLap.lap").cast("int").alias("fastest_lap_number"),
        F.col("r.FastestLap.Time.time").alias("fastest_lap_time"),
        F.col("r.FastestLap.AverageSpeed.speed").cast("double").alias("fastest_lap_kph"),
        "_ingest_ts",
        "_file_path",
    )

    # `positionText` carries the classification: a number when classified, or
    # R/D/E/W/F/N when not. Anything non-numeric means the driver did not finish.
    df = df.withColumn(
        "dnf_flag", F.when(F.col("position_text").rlike("^[0-9]+$"), False).otherwise(True)
    ).withColumn(
        "positions_gained",
        F.when(
            F.col("grid_position").isNotNull()
            & F.col("position").isNotNull()
            & (F.col("grid_position") > 0),
            F.col("grid_position") - F.col("position"),
        ),
    ).withColumn("is_fastest_lap", F.col("fastest_lap_rank") == 1)

    return dedupe(df, ["season", "round", "driver_id"])


@dp.materialized_view(
    name=f"{SILVER}.fact_result",
    comment="Race result per driver per race: grid, finish, points, status, fastest lap.",
    table_properties={"quality": "silver"},
    cluster_by=["season", "round"],
)
@dp.expect_all_or_drop(RESULT_RULES)
@dp.expect_or_fail("plausible_season", "season BETWEEN 1950 AND 2100")
@dp.expect_all({
    "non_negative_points": "points >= 0",
    "plausible_grid": "grid_position IS NULL OR grid_position BETWEEN 0 AND 30",
    "plausible_position": "position IS NULL OR position BETWEEN 1 AND 30",
    "laps_non_negative": "laps_completed >= 0",
})
def fact_result():
    return spark.read.table("stg_result")


@dp.materialized_view(
    name=f"{SILVER}.quarantine_result",
    comment="Result rows rejected by fact_result quality rules, with the reason.",
)
def quarantine_result():
    return (
        spark.read.table("stg_result")
        .filter(F.expr(invalid_predicate(RESULT_RULES)))
        .withColumn("_quarantine_reason", quarantine_reason(RESULT_RULES))
        .withColumn("_quarantined_at", F.current_timestamp())
    )


# ────────────────────────── fact_sprint_result ──────────────────────────
# Sprint races award championship points (8 for a win, down to 1 for eighth).
# Without this fact every mart understates points on the ~6 sprint weekends a
# season, and the standings reconciliation fails by exactly that margin.

SPRINT_RULES = {
    "driver_id_present": "driver_id IS NOT NULL",
    "season_present": "season IS NOT NULL",
    "round_present": "round IS NOT NULL",
    # The staged column is `sprint_position`, not `position` — these rules are
    # evaluated against stg_sprint_result, whose aliases differ from stg_result.
    "classification_present": "sprint_position IS NOT NULL OR position_text IS NOT NULL",
}


@dp.temporary_view(name="stg_sprint_result")
def stg_sprint_result():
    races = parse(
        f"{BRONZE}.raw_sprint", SPRINT_SCHEMA, "parsed.payload.MRData.RaceTable.Races"
    )
    df = races.select(
        F.col("item.season").cast("int").alias("season"),
        F.col("item.round").cast("int").alias("round"),
        F.to_date("item.date").alias("race_date"),
        F.explode("item.SprintResults").alias("s"),
        "_ingest_ts",
        "_file_path",
    )
    df = df.select(
        "season",
        "round",
        "race_date",
        F.col("s.Driver.driverId").alias("driver_id"),
        F.col("s.Constructor.constructorId").alias("constructor_id"),
        F.col("s.grid").cast("int").alias("sprint_grid_position"),
        F.col("s.position").cast("int").alias("sprint_position"),
        F.col("s.positionText").alias("position_text"),
        F.col("s.points").cast("double").alias("sprint_points"),
        F.col("s.laps").cast("int").alias("sprint_laps"),
        F.col("s.status").alias("sprint_status"),
        "_ingest_ts",
        "_file_path",
    )
    return dedupe(df, ["season", "round", "driver_id"])


@dp.materialized_view(
    name=f"{SILVER}.fact_sprint_result",
    comment="Sprint race result per driver. Only rounds with a sprint appear here.",
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop(SPRINT_RULES)
@dp.expect("non_negative_sprint_points", "sprint_points >= 0")
def fact_sprint_result():
    return spark.read.table("stg_sprint_result")


@dp.materialized_view(
    name=f"{SILVER}.quarantine_sprint_result",
    comment="Sprint rows rejected by quality rules, with the reason.",
)
def quarantine_sprint_result():
    return (
        spark.read.table("stg_sprint_result")
        .filter(F.expr(invalid_predicate(SPRINT_RULES)))
        .withColumn("_quarantine_reason", quarantine_reason(SPRINT_RULES))
        .withColumn("_quarantined_at", F.current_timestamp())
    )


# ─────────────────────────── fact_qualifying ────────────────────────────

QUALI_RULES = {
    "driver_id_present": "driver_id IS NOT NULL",
    "season_present": "season IS NOT NULL",
    "round_present": "round IS NOT NULL",
    "quali_position_present": "quali_position IS NOT NULL",
}


@dp.temporary_view(name="stg_qualifying")
def stg_qualifying():
    races = parse(
        f"{BRONZE}.raw_qualifying", QUALI_SCHEMA, "parsed.payload.MRData.RaceTable.Races"
    )
    df = races.select(
        F.col("item.season").cast("int").alias("season"),
        F.col("item.round").cast("int").alias("round"),
        F.to_date("item.date").alias("race_date"),
        F.explode("item.QualifyingResults").alias("q"),
        "_ingest_ts",
        "_file_path",
    )
    df = df.select(
        "season",
        "round",
        "race_date",
        F.col("q.Driver.driverId").alias("driver_id"),
        F.col("q.Constructor.constructorId").alias("constructor_id"),
        F.col("q.position").cast("int").alias("quali_position"),
        F.col("q.Q1").alias("q1_time"),
        F.col("q.Q2").alias("q2_time"),
        F.col("q.Q3").alias("q3_time"),
        "_ingest_ts",
        "_file_path",
    )

    # Q2/Q3 are absent for drivers eliminated earlier — that is data, not a defect.
    df = (
        df.withColumn("q1_millis", lap_time_millis("q1_time"))
        .withColumn("q2_millis", lap_time_millis("q2_time"))
        .withColumn("q3_millis", lap_time_millis("q3_time"))
        .withColumn(
            "quali_stage_reached",
            F.when(F.col("q3_time").isNotNull(), "Q3")
            .when(F.col("q2_time").isNotNull(), "Q2")
            .otherwise("Q1"),
        )
    )

    return dedupe(df, ["season", "round", "driver_id"])


@dp.materialized_view(
    name=f"{SILVER}.fact_qualifying",
    comment="Qualifying result per driver per race, with Q1/Q2/Q3 parsed to milliseconds.",
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop(QUALI_RULES)
@dp.expect_all({
    "plausible_quali_position": "quali_position BETWEEN 1 AND 30",
    "q1_before_q3": "q1_millis IS NULL OR q3_millis IS NULL OR q3_millis <= q1_millis",
})
def fact_qualifying():
    return spark.read.table("stg_qualifying")


@dp.materialized_view(
    name=f"{SILVER}.quarantine_qualifying",
    comment="Qualifying rows rejected by fact_qualifying quality rules, with the reason.",
)
def quarantine_qualifying():
    return (
        spark.read.table("stg_qualifying")
        .filter(F.expr(invalid_predicate(QUALI_RULES)))
        .withColumn("_quarantine_reason", quarantine_reason(QUALI_RULES))
        .withColumn("_quarantined_at", F.current_timestamp())
    )


# ──────────────────────── fact_driver_standing ──────────────────────────

DRIVER_STANDING_RULES = {
    "driver_id_present": "driver_id IS NOT NULL",
    "season_present": "season IS NOT NULL",
    "round_present": "round IS NOT NULL",
    "position_present": "championship_position IS NOT NULL",
    "points_present": "cumulative_points IS NOT NULL",
}


@dp.temporary_view(name="stg_driver_standing")
def stg_driver_standing():
    lists = parse(
        f"{BRONZE}.raw_driver_standings",
        DRIVER_STANDINGS_SCHEMA,
        "parsed.payload.MRData.StandingsTable.StandingsLists",
    )
    df = lists.select(
        F.col("item.season").cast("int").alias("season"),
        F.col("item.round").cast("int").alias("round"),
        F.explode("item.DriverStandings").alias("s"),
        "_ingest_ts",
        "_file_path",
    )
    df = df.select(
        "season",
        "round",
        F.col("s.Driver.driverId").alias("driver_id"),
        F.concat_ws(" ", "s.Driver.givenName", "s.Driver.familyName").alias("driver_name"),
        F.col("s.position").cast("int").alias("championship_position"),
        F.col("s.positionText").alias("championship_position_text"),
        F.col("s.points").cast("double").alias("cumulative_points"),
        F.col("s.wins").cast("int").alias("cumulative_wins"),
        # A driver who changed teams mid-season lists every constructor they have
        # driven for this season; the last entry is the current one.
        F.element_at(F.col("s.Constructors.constructorId"), -1).alias("constructor_id"),
        F.element_at(F.col("s.Constructors.name"), -1).alias("constructor_name"),
        F.size("s.Constructors").alias("constructor_count"),
        "_ingest_ts",
        "_file_path",
    )
    return dedupe(df, ["season", "round", "driver_id"])


@dp.materialized_view(
    name=f"{SILVER}.fact_driver_standing",
    comment="Driver championship standing after each round: cumulative points, wins, position.",
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop(DRIVER_STANDING_RULES)
@dp.expect_all({
    "non_negative_points": "cumulative_points >= 0",
    "non_negative_wins": "cumulative_wins >= 0",
})
def fact_driver_standing():
    return spark.read.table("stg_driver_standing")


@dp.materialized_view(
    name=f"{SILVER}.quarantine_driver_standing",
    comment="Driver standing rows rejected by quality rules, with the reason.",
)
def quarantine_driver_standing():
    return (
        spark.read.table("stg_driver_standing")
        .filter(F.expr(invalid_predicate(DRIVER_STANDING_RULES)))
        .withColumn("_quarantine_reason", quarantine_reason(DRIVER_STANDING_RULES))
        .withColumn("_quarantined_at", F.current_timestamp())
    )


# ────────────────────── fact_constructor_standing ───────────────────────

CONSTRUCTOR_STANDING_RULES = {
    "constructor_id_present": "constructor_id IS NOT NULL",
    "season_present": "season IS NOT NULL",
    "round_present": "round IS NOT NULL",
    "position_present": "championship_position IS NOT NULL",
    "points_present": "cumulative_points IS NOT NULL",
}


@dp.temporary_view(name="stg_constructor_standing")
def stg_constructor_standing():
    lists = parse(
        f"{BRONZE}.raw_constructor_standings",
        CONSTRUCTOR_STANDINGS_SCHEMA,
        "parsed.payload.MRData.StandingsTable.StandingsLists",
    )
    df = lists.select(
        F.col("item.season").cast("int").alias("season"),
        F.col("item.round").cast("int").alias("round"),
        F.explode("item.ConstructorStandings").alias("s"),
        "_ingest_ts",
        "_file_path",
    )
    df = df.select(
        "season",
        "round",
        F.col("s.Constructor.constructorId").alias("constructor_id"),
        F.col("s.Constructor.name").alias("constructor_name"),
        F.col("s.Constructor.nationality").alias("constructor_nationality"),
        F.col("s.position").cast("int").alias("championship_position"),
        F.col("s.positionText").alias("championship_position_text"),
        F.col("s.points").cast("double").alias("cumulative_points"),
        F.col("s.wins").cast("int").alias("cumulative_wins"),
        "_ingest_ts",
        "_file_path",
    )
    return dedupe(df, ["season", "round", "constructor_id"])


@dp.materialized_view(
    name=f"{SILVER}.fact_constructor_standing",
    comment="Constructor championship standing after each round.",
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop(CONSTRUCTOR_STANDING_RULES)
@dp.expect_all({
    "non_negative_points": "cumulative_points >= 0",
    "non_negative_wins": "cumulative_wins >= 0",
})
def fact_constructor_standing():
    return spark.read.table("stg_constructor_standing")


@dp.materialized_view(
    name=f"{SILVER}.quarantine_constructor_standing",
    comment="Constructor standing rows rejected by quality rules, with the reason.",
)
def quarantine_constructor_standing():
    return (
        spark.read.table("stg_constructor_standing")
        .filter(F.expr(invalid_predicate(CONSTRUCTOR_STANDING_RULES)))
        .withColumn("_quarantine_reason", quarantine_reason(CONSTRUCTOR_STANDING_RULES))
        .withColumn("_quarantined_at", F.current_timestamp())
    )
