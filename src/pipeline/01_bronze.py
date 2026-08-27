"""Bronze — raw capture of Jolpica payloads from the landing Volume.

One streaming table per endpoint, each ingesting with Auto Loader. Bronze never
drops a row and never types a field: whatever the API returned is still here,
byte for byte, alongside the provenance needed to audit it.

Files are read as `text` rather than `json` deliberately. The ingestion writer
lands exactly one single-line JSON object per file, so `wholeText` gives one row
per file with the payload intact. Reading as JSON would make Auto Loader infer a
schema per endpoint, and optional Ergast fields (FastestLap, Sprint, Q3) appear
only in some files — schema evolution would then fail-and-retry the pipeline on
first encounter. Parsing is deferred to Silver, where the schema is explicit.
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

LANDING_ROOT = spark.conf.get("f1.landing_root")

# Endpoint -> the host its payloads must come from.
#
# The expectation used to be a single hardcoded jolpi.ca check, which was
# correct while every source was Jolpica. Weather comes from Open-Meteo, so a
# shared rule would fail on every weather row — and an expectation that is
# always violated teaches you to ignore the one signal meant to catch a payload
# landing in the wrong folder.
ENDPOINT_SOURCES = {
    "races": "https://api.jolpi.ca/%",
    "drivers": "https://api.jolpi.ca/%",
    "constructors": "https://api.jolpi.ca/%",
    "sprint": "https://api.jolpi.ca/%",
    "results": "https://api.jolpi.ca/%",
    "qualifying": "https://api.jolpi.ca/%",
    "driver_standings": "https://api.jolpi.ca/%",
    "constructor_standings": "https://api.jolpi.ca/%",
    "pitstops": "https://api.jolpi.ca/%",
    "laps": "https://api.jolpi.ca/%",
    "weather": "https://archive-api.open-meteo.com/%",
}

ENDPOINTS = list(ENDPOINT_SOURCES)


def _bronze_table(endpoint: str):
    """Build one Auto Loader streaming table for an endpoint.

    Defined in a factory so each table binds its own `endpoint` — a bare loop
    would let every closure capture the final value.
    """

    @dp.table(
        name=f"{BRONZE}.raw_{endpoint}",
        comment=f"Raw Jolpica {endpoint} payloads, one row per landed file.",
        table_properties={"quality": "bronze"},
    )
    @dp.expect("payload_present", "raw_payload IS NOT NULL AND length(raw_payload) > 0")
    @dp.expect("expected_source", f"_source_url LIKE '{ENDPOINT_SOURCES[endpoint]}'")
    def _table():
        df = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "text")
            .option("wholeText", "true")
            .load(f"{LANDING_ROOT}/{endpoint}/")
        )

        # Pull the envelope fields out of the raw line without parsing the whole
        # payload — enough to make Bronze queryable and auditable on its own.
        df = df.select(
            F.col("value").alias("raw_payload"),
            F.get_json_object("value", "$._ingest_ts").alias("_ingest_ts_str"),
            F.get_json_object("value", "$._source_url").alias("_source_url"),
            F.get_json_object("value", "$._season").alias("_season"),
            F.get_json_object("value", "$._round").alias("_round"),
            F.get_json_object("value", "$._endpoint").alias("_endpoint"),
            F.col("_metadata.file_path").alias("_file_path"),
            F.col("_metadata.file_modification_time").alias("_file_modified_at"),
        )

        return df.withColumn(
            "_ingest_ts", F.to_timestamp("_ingest_ts_str")
        ).withColumn("_ingested_at", F.current_timestamp()).drop("_ingest_ts_str")

    return _table


for _endpoint in ENDPOINTS:
    _bronze_table(_endpoint)
