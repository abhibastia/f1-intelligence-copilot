"""Silver — measured race-day weather.

One row per race that has an ERA5 observation, typed and quality-checked.

WHY THIS IS A MATERIALIZED VIEW AND NOT AN AUTO CDC TARGET
-----------------------------------------------------------
The facts sourced from Jolpica are candidates for Auto CDC because a result is
provisional when published — stewards apply penalties after the flag and the
payload is reissued. None of that is true here. ERA5 is a reanalysis archive:
once a race day is published the observation does not change, and ingestion
only lands a round after the publication lag has passed, so there is exactly one
snapshot per race. Upsert semantics would buy nothing, and Change Data Feed on a
recomputed view reports every row as changed on every run — noise, not lineage.

CDC and CDF belong where data is amended. This is not that.

ABSENT IS NOT ZERO
------------------
A race with no observation has **no row here**, rather than a row of nulls or —
far worse — a row reading 0.0 mm. Gold left-joins onto this table and reports
the difference explicitly, because "no data" and "no rain" are different answers
and only one of them is safe to put on a dashboard.
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

# Kept in step with config.WET_THRESHOLD_MM. 1.0 mm across a race day is where
# rain starts affecting tyre choice and grip rather than merely being noted.
WET_THRESHOLD_MM = 1.0

# Open-Meteo returns parallel arrays under `daily`, one element per requested
# day. We request exactly one day per race, so every array has length 1.
#
# The outer `payload` wrapper is not optional: raw_payload holds the whole
# ingestion envelope, and the measurement sits inside it. Omitting it makes
# from_json return null for every field while season and round — which come
# from the envelope via Bronze — still parse, so the rows look half-populated
# rather than obviously broken.
WEATHER_SCHEMA = (
    "payload STRUCT<"
    "  latitude DOUBLE, longitude DOUBLE, timezone STRING, "
    "daily STRUCT<"
    "  time: ARRAY<STRING>,"
    "  precipitation_sum: ARRAY<DOUBLE>,"
    "  rain_sum: ARRAY<DOUBLE>,"
    "  temperature_2m_max: ARRAY<DOUBLE>,"
    "  temperature_2m_min: ARRAY<DOUBLE>,"
    "  wind_speed_10m_max: ARRAY<DOUBLE>,"
    "  weather_code: ARRAY<BIGINT>"
    "  >"
    ">"
)

# WMO 4677 present-weather codes, collapsed to what a race report would say.
# Kept as a CASE rather than a lookup table: ten values that never change do not
# need a dimension, and inlining keeps the mart readable in the lineage graph.
CONDITIONS = (
    F.when(F.col("weather_code").isin(0), "clear sky")
    .when(F.col("weather_code").isin(1, 2, 3), "overcast")
    .when(F.col("weather_code").isin(45, 48), "fog")
    .when(F.col("weather_code").isin(51, 53, 55), "drizzle")
    .when(F.col("weather_code").isin(56, 57), "freezing drizzle")
    .when(F.col("weather_code").isin(61, 63, 65), "rain")
    .when(F.col("weather_code").isin(66, 67), "freezing rain")
    .when(F.col("weather_code").isin(71, 73, 75, 77), "snow")
    .when(F.col("weather_code").isin(80, 81, 82), "rain showers")
    .when(F.col("weather_code").isin(95, 96, 99), "thunderstorm")
    .otherwise("unknown")
)

WEATHER_RULES = {
    "season_present": "season IS NOT NULL",
    "round_present": "round IS NOT NULL",
    "observation_date_present": "observation_date IS NOT NULL",
    "precipitation_present": "precipitation_mm IS NOT NULL",
    "non_negative_precipitation": "precipitation_mm >= 0",
    # ERA5 covers the whole planet; no circuit is outside this range, so a value
    # beyond it means the payload was parsed wrong rather than that it was cold.
    "plausible_temperature": "temp_max_c BETWEEN -60 AND 60",
    "coordinates_present": "latitude IS NOT NULL AND longitude IS NOT NULL",
}


def quarantine_reason(rules: dict[str, str]):
    """Name every rule this row violates, semicolon separated."""
    return F.concat_ws(
        ";",
        *[F.when(~F.expr(rule), F.lit(name)) for name, rule in rules.items()],
    )


@dp.temporary_view(name="stg_weather")
def stg_weather():
    """Flatten one Open-Meteo payload into one row.

    The envelope carries season and round; the payload carries the measurement.
    Element [0] of each daily array is the race day — there is only ever one,
    because the fetcher requests start_date == end_date.
    """
    df = (
        spark.read.table(f"{BRONZE}.raw_weather")
        .withColumn("parsed", F.from_json("raw_payload", WEATHER_SCHEMA))
        .select(
            F.col("_season").cast("int").alias("season"),
            F.col("_round").cast("int").alias("round"),
            F.to_date(F.col("parsed.payload.daily.time")[0]).alias("observation_date"),
            F.col("parsed.payload.latitude").alias("latitude"),
            F.col("parsed.payload.longitude").alias("longitude"),
            F.col("parsed.payload.daily.precipitation_sum")[0].alias("precipitation_mm"),
            F.col("parsed.payload.daily.rain_sum")[0].alias("rain_mm"),
            F.col("parsed.payload.daily.temperature_2m_max")[0].alias("temp_max_c"),
            F.col("parsed.payload.daily.temperature_2m_min")[0].alias("temp_min_c"),
            F.col("parsed.payload.daily.wind_speed_10m_max")[0].alias("wind_max_kmh"),
            F.col("parsed.payload.daily.weather_code")[0].alias("weather_code"),
            "_ingest_ts",
            "_file_path",
        )
        .withColumn("conditions", CONDITIONS)
        # Derived once, here, so every downstream consumer agrees on what "wet"
        # means. A dashboard tile computing its own threshold is how two tiles
        # end up disagreeing about the same race.
        .withColumn(
            "was_wet",
            F.when(F.col("precipitation_mm").isNull(), None).otherwise(
                F.col("precipitation_mm") >= F.lit(WET_THRESHOLD_MM)
            ),
        )
    )

    window = Window.partitionBy("season", "round").orderBy(
        F.col("_ingest_ts").desc(), F.col("_file_path").desc()
    )
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


@dp.materialized_view(
    name=f"{SILVER}.fact_race_weather",
    comment=(
        "Measured race-day weather at the circuit's coordinates, from the "
        "Open-Meteo ERA5 archive. One row per race that has an observation; a "
        "race with no row has no published observation, which is NOT the same "
        "as no rain."
    ),
    table_properties={"quality": "silver"},
    cluster_by=["season", "round"],
)
@dp.expect_all_or_drop(WEATHER_RULES)
@dp.expect_or_fail("plausible_season", "season BETWEEN 1950 AND 2100")
def fact_race_weather():
    return spark.read.table("stg_weather")


@dp.materialized_view(
    name=f"{SILVER}.quarantine_race_weather",
    comment="Weather rows rejected by fact_race_weather quality rules, with the reason.",
)
def quarantine_race_weather():
    rules = " AND ".join(f"({rule})" for rule in WEATHER_RULES.values())
    return (
        spark.read.table("stg_weather")
        .filter(f"NOT ({rules})")
        .withColumn("_quarantine_reason", quarantine_reason(WEATHER_RULES))
        .withColumn("_quarantined_at", F.current_timestamp())
    )
