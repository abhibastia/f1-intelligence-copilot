# Runbook

How to operate this pipeline, and what to do when it misbehaves.

`docs/architecture.md` explains *why* it is built this way. This explains *how
to run it*. Every failure listed below has actually happened here.

---

## 1. Prerequisites

### The two sources

| Source | Endpoints | Auth | Limits |
|---|---|---|---|
| **Jolpica-F1** — `api.jolpi.ca/ergast/f1` | races, drivers, constructors, sprint, results, qualifying, driver & constructor standings, pitstops, laps | none | 4 req/s burst and 500/hour published; the *enforced* burst rate is lower, so the client runs at 0.5 req/s with a 450/hour budget |
| **Open-Meteo ERA5 archive** — `archive-api.open-meteo.com` | measured daily weather at each circuit's coordinates | none | ~10,000 calls/day, far above the ~71 this project makes |

Neither needs a key, which is why `setup_secrets.py` is optional. The
coordinates come from the Jolpica races payload, so **races must be fetched
before weather** — that is the only ordering constraint in ingestion.

Weather is a different shape from everything else: Open-Meteo returns flat
parallel arrays rather than an `MRData` envelope, so it has its own Silver file
(`02b_silver_weather.py`) and its own schema. Bronze's `expected_source`
expectation is per-endpoint for the same reason — a single hardcoded `jolpi.ca`
check would fail on every weather row, and an expectation that always fails is
one you learn to ignore.

### Access

```bash
databricks auth login --host <workspace-url> --profile <profile>
export DATABRICKS_PROFILE=<profile>       # every script reads this
python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
```

No script here has a default profile. If you omit it they stop and list the
profiles configured on the machine rather than picking one.

`./scripts/bootstrap.sh --profile <profile>` does everything in this section
end to end, prompting before the one step that spends compute.

**The catalog must exist before anything else.** Free Edition cannot create
catalogs over the API — `databricks catalogs create` fails with *"Metastore
storage root URL does not exist"*, and `CREATE CATALOG` over SQL needs a
warehouse. Make `f1` once in the UI: **Catalog → Create catalog → Default
storage**. Schemas and Volumes are fine over the CLI:

```bash
./scripts/create_catalog.sh --profile <profile>
```

Then apply the access model — no compute, safe to re-run:

```bash
python3 scripts/apply_grants.py --profile <profile>
```

---

## 2. Normal operation

### Run everything

```bash
databricks bundle deploy -t dev --profile <profile>
databricks bundle run f1_end_to_end -t dev --profile <profile>
```

`f1_end_to_end` runs unit tests → ingest → pipeline → validation. The tests come
first so a typo costs seconds instead of a cluster start; validation comes last
so "the update completed" is not mistaken for "the marts reconcile". Ingest
precedes the pipeline in the same run, so the pipeline never reads a landing
zone that is mid-write.

`f1_ingest_incremental` is the cheaper weekly job — ingest, refresh and
validate, but no unit tests, because the pipeline code cannot change between
scheduled runs. Use it for steady state; use `f1_end_to_end` after a code
change.

Both jobs time out after an hour and email on failure. On Free Edition a hung
task spends tomorrow's compute allowance as well as today's, and an unwatched
failure is indistinguishable from a quiet week with no new races.

Neither job hardcodes a season. `config.live_season()` is the calendar year and
a routine run also revisits the previous season, because a December race stays
open into January while results and lap times are still being corrected.

### Run the ingest alone

```bash
cd src/ingestion
python3 ingest.py --mode incremental --root /Volumes/f1/raw/landing   # live season only
python3 ingest.py --mode backfill    --root /Volumes/f1/raw/landing   # every season
```

Both are safe to re-run. Closed rounds are skipped, so a repeat backfill makes a
handful of calls rather than several hundred.

### Run the pipeline alone

```bash
databricks bundle deploy -t dev --profile <profile>     # code changes need this first
databricks pipelines start-update <pipeline-id> --profile <profile>
```

**Poll the update, not the pipeline.** Pipeline state lags and will tell you
`IDLE` while an update is still running:

```bash
databricks pipelines get-update <pipeline-id> <update-id> --profile <profile>
```

### Refresh the dashboard

Editing `dashboards/*.lvdash.json` and deploying updates the **draft**. Viewers
see the **published** copy, which does not change until you publish:

```bash
databricks bundle summary -t dev --profile <profile>        # find the ids
databricks lakeview publish <id> --warehouse-id <warehouse-id> --profile <profile>
```

Forgetting this is why
tiles look empty to everyone except the person who edited them.

**A blank tile is usually the spec, not the data.** Three causes, all of which
have happened here and none of which produce a useful error: a `table` widget
declared `version: 3` when the only supported version is 2; a filter querying
`associative_filter_predicate_group`, which is not a column; and a field named
`constructor`, which every JavaScript object inherits, so the encoding binds to
`Object.prototype.constructor` and the tile silently draws nothing. `pytest`
now fails on all three.

### Push the Genie agent

Editing `genie/f1_gold_space.json` only changes the file. Nothing deploys it —
there is no bundle resource or script for this, unlike the pipeline and the
dashboard — so push it by hand:

```bash
databricks genie update-space 01f198328efd1d7bb0a3d43205fda74b --profile <profile> \
  --json "{\"serialized_space\": $(cat genie/f1_gold_space.json | jq -c '.' | jq -Rs '.')}"
```

**The space's `title` and `description` are not in this file.** They are
metadata set at creation time (`databricks genie create-space --description
...`), live only in the workspace, and nothing catches them drifting from the
data the space actually reads — that is how the description came to say
"five Gold marts" after a sixth mart and the `driver_metrics` metric view were
added. Check and update them directly when the mart set changes:

```bash
databricks genie get-space 01f198328efd1d7bb0a3d43205fda74b --profile <profile> -o json \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['description'])"
databricks genie update-space 01f198328efd1d7bb0a3d43205fda74b --profile <profile> \
  --json '{"description": "..."}'
```

---

## 3. Health checks

```bash
# Files landed, per endpoint
databricks fs ls dbfs:/Volumes/f1/raw/landing/<endpoint> --profile <profile>

# Bronze row counts should equal file counts
SELECT '<endpoint>' t, count(*) FROM f1.bronze.raw_<endpoint>;

# Anything quarantined, and why
SELECT _quarantine_reason, count(*) FROM f1.silver.quarantine_<fact> GROUP BY 1;

# Expectation pass/fail per dataset
-- sql/dq_event_log.sql

# Marts reconcile against published standings
-- sql/validation_checks.sql          the narrative version, for reading
python3 scripts/validate_marts.py --catalog f1    # the same checks, with an exit code

# Who can read what
python3 scripts/apply_grants.py --profile <profile> --show
```

Run the tests before any long ingest — they need no cluster and take under a
second:

```bash
.venv/bin/python -m pytest
```

**After a pipeline run that lands a new round**, before touching any doc by
hand:

```bash
python3 scripts/check_doc_counts.py
```

Recomputes the round count, lap-timing totals, the quarantine census, and the
championship reconciliation from the local `landing/` cache, and checks the
literal number each doc currently states against that recomputation — no
Spark, no warehouse. It caught the same class of drift a 2026-08-22 audit
found by hand (docs still describing a five-Gold-mart platform months after a
sixth mart shipped): every one of those numbers goes stale again the next race
weekend, not just once. A few things it does *not* cover — the pace-vs-finish
and strategy findings, `dim_driver`'s SCD-2 history — need a warehouse query;
it prints those as a reminder rather than silently skipping them.

---

## 4. When it breaks

### The pipeline update failed and the message is useless

`.message` on a pipeline event is only ever *"Update X is FAILED"*. The real
cause is nested:

```bash
databricks pipelines list-pipeline-events <pipeline-id> --profile <profile> -o json \
  | python3 -c "
import json,sys
for e in json.load(sys.stdin):
    if e.get('level') == 'ERROR':
        print((e.get('error',{}).get('exceptions') or [{}])[0].get('message',''))
        break"
```

`scripts/run_pipeline.sh <pipeline_id> --profile <profile>` wraps this whole
loop end to end — starts an update, polls *that update* (not the pipeline,
which flips back to RUNNING on retry) to a terminal state, and prints the real
error on failure.

### Every row in a fact went to quarantine

Almost always a schema mismatch, not bad data. Check `_quarantine_reason`: if
the envelope fields (`season`, `round`) parsed while everything else is null,
the `from_json` schema is wrong.

The usual cause is forgetting that `raw_payload` holds the **whole ingestion
envelope**. The measurement sits under `payload`:

```python
"payload STRUCT< ...actual fields... >"      # correct
"...actual fields..."                         # returns null for every field
```

### A handful of rows quarantined

Read them before assuming a bug — they are often genuinely bad upstream data.
Jolpica returns an empty string for two of Tsunoda's pit stop durations, for
instance. That is the pattern working, not failing.

### HTTP 429 during a backfill

Jolpica publishes 4 req/s burst and 500/hour sustained. The **enforced** burst
rate is well below the published one for unauthenticated clients; 2 req/s
produced a 429 on 37% of requests during a laps backfill.

`REQUESTS_PER_SECOND = 0.5` in `config.py` is calibrated against the real API,
not the docs. If you raise it, watch the log:

```bash
grep -c "HTTP 429" <log>
```

Retries are correct but not free — each one spends the hourly budget, so a rate
that trips throttling burns the allowance twice. If throttling is frequent,
**stop and lower the rate**; the harvest is idempotent, so a restart resumes
from what is already on disk rather than starting over.

### The ingest job fails immediately with NameError

`__file__` is not bound in a serverless `spark_python_task` — the file runs
through `exec(compile(...))`. `_module_dir()` handles it. This works locally and
fails on the job, so test the job, not just the script.

### Changing a dataset from materialized view to streaming table

**Cannot be done in place, and a full refresh does not help.** The deploy will
fail. Drop the table first:

```sql
DROP TABLE f1.silver.<name>;
```

then deploy and run. Expect a full re-read of Bronze afterwards, since the
streaming checkpoint is gone.

### "Cannot run the resource — free daily limit"

Free Edition's compute quota is unrecoverable until the next day and applies to
the SQL warehouse and pipeline alike.

- Use **selective refresh** on one table rather than a full pipeline run while
  iterating.
- Run `python3 scripts/check_expectations.py` first — it catches a column named
  in an expectation that does not exist in the staged view, which otherwise
  costs a cluster start and a failed update to discover.
- Validate transformation logic in plain Python before running Spark.

### Weather rows are missing for recent races

Working as intended. Weather comes from the **Open-Meteo ERA5 reanalysis
archive**, not from a forecast and not from a race report. ERA5 publishes on a
~5 day lag, so `is_available` skips
races inside it rather than landing a row of nulls. `race_conditions` reports
those as `weather_available = false` and `rainfall_verdict = 'no observation'`.

**A race with no observation is not a dry race.** If you ever see 0.0 mm for a
race that has not been published yet, something has coerced a null and that is
a bug worth chasing.

---

## 5. Backfilling

```bash
cd src/ingestion
python3 ingest.py --mode backfill --root ./landing        # local first
databricks fs cp -r --overwrite landing dbfs:/Volumes/f1/raw/landing --profile <profile>
```

Running locally costs no Databricks compute, which matters for `laps`: ~11 pages
per round, ~780 requests across three seasons, over an hour of wall clock at a
safe rate. Everything else is small enough to run in-platform.

To force a re-fetch of a closed round, delete its partition first — the skip
logic keys on files existing, not on their content:

```bash
databricks fs rm -r dbfs:/Volumes/f1/raw/landing/<endpoint>/season=<s>/round=<r> --profile <profile>
```

---

## 6. Where things live

| | |
|---|---|
| Sources | Jolpica-F1 (10 endpoints) and the Open-Meteo ERA5 archive (weather) — both public, both keyless |
| Raw payloads | `/Volumes/f1/raw/landing/<endpoint>/season=/round=/` |
| Bronze | `f1.bronze.raw_<endpoint>` — one streaming table per endpoint |
| Silver | `f1.silver.fact_*`, `dim_*`, `quarantine_*` |
| Gold | `f1.gold.driver_performance`, `championship_progression`, `race_conditions`, `race_strategy`, `lap_pace`, `constructor_standings`, plus the `driver_metrics` metric view |
| Not ours | `f1.gold.agent_activity_analytics`, `agent_tool_calls` — a separate project's, kept for later |
| Pipeline event log | `f1.gold.pipeline_event_log` |
| Genie agent | `genie/f1_gold_space.json` — space `01f198328efd1d7bb0a3d43205fda74b`; `title`/`description` are workspace-only, see §2 |
| Ingestion code | `src/ingestion/` — plain modules, unit tested |
| Pipeline code | `src/pipeline/` — Lakeflow declarative definitions |
| Tests | `tests/` — 224 local, no cluster or network; `tests/spark/` runs on Databricks |
| Access model | `scripts/apply_grants.py` — idempotent, no compute |
| Design record | `docs/architecture.md` — why, and what is still missing |

---

## 7. Things that look wrong and are not

- **Bronze has more rows than you expect for the live season.** The current
  round is re-polled every run because results are amended after the flag.
  Silver deduplicates on the natural key, keeping the newest `_ingest_ts`.
- **A `laps` payload has more lap elements than the race had laps.** Pages split
  mid-lap, so one lap number can appear twice with different drivers. Silver
  dedupes on `(season, round, lap, driver_id)`.
- **`race_strategy` shows a driver with more stops than the field and worse
  results.** That is the finding, not an error — going off-strategy loses about
  1.09 positions on average — staying with the field gains 0.69 places, deviating
  in either direction loses 0.40. The dashboard's Constructor Benchmarking page
  shows the split.
- **The wettest race has the lowest retirement rate.** Also the finding. Monza
  2024 measured 19.1 mm and ran dry, because a daily total cannot tell rain that
  fell overnight from rain that fell during the race.
- **The quarantine census is not zero and should not be.** 89 lap rows fail
  `plausible_lap_time` — red-flag and safety-car laps outside 40–300 s — plus 8
  standings rows with no championship position and 2 pit stops published with an
  empty duration. Zero everywhere would mean the expectations had stopped being
  evaluated.
- **`f1.gold` holds eight objects where the docs describe six.** Two belong to a
  separate agent project. Nothing here writes or reads them.
- **`table_changes()` fails on a fact or a mart.** Not a misconfiguration: CDF is
  unsupported on materialised views, and every Silver fact and Gold mart is one.
  It works on the streaming tables — Bronze and the SCD-2 dimensions — where
  Lakeflow enables it by default. Reading from version 0 there fails on
  `deletedFileRetentionDuration` (168 hours), which is retention, not CDF.
  `docs/architecture.md` §5.4 has the measurements.
