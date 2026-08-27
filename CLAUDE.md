# F1 Intelligence Copilot — working constraints

Merged from two Databricks Free Edition capstones — formula1-capstone-project
(the governed medallion pipeline) and f1-strategy-copilot (the Lakebase / RAG /
agent layer). This file holds the decisions that are expensive to rediscover,
from both halves. `docs/architecture.md` is the pipeline design record;
`docs/copilot_design.md` is the agent/RAG design record.

## Environment

- Every command in this repository takes an explicit `--profile <profile>` (or
  `DATABRICKS_CONFIG_PROFILE`). Never auto-pick or hardcode one — see
  `databricks.yml`'s own note on why: a pinned profile means a fork deploys
  against a workspace its author never heard of.
- SQL warehouse: set your own via `BUNDLE_VAR_warehouse_id` (see
  `.env.example`) — find one with `databricks warehouses list --profile <profile>`.
- **Free Edition has a daily compute quota.** When it is exhausted every
  warehouse and pipeline run fails with "hit your free daily limit". Assume
  compute is scarce: prefer selective refresh over full refresh, validate logic
  locally before running anything, and remember that `f1lake.seed_gold` reads
  Gold through the SQL warehouse — every mart it seeds is a query against that
  same quota, cached to `data/*.json` so a failed seed does not re-spend it.
- **Catalogs cannot be created over the API on Free Edition** — the CLI, the
  storage-root override, and SQL all refuse. Catalogs must be made in the UI
  with Default Storage. Schemas and Volumes *can* be created via the CLI.

## Non-negotiable design decisions — the pipeline

1. **`dim_driver` is built from the results endpoint, not `/drivers`.** Every
   field `/drivers` returns is static, so Auto CDC over it yields zero history
   rows. The attribute that changes is the driver's constructor, and it only
   exists in results. Verified: this produces 44 versions over 28 drivers, 16
   of them historical. Do not "simplify" this back to the drivers endpoint.
2. **Sprint points are part of the championship.** Summing only race points
   leaves 13 of 24 drivers short of their official 2024 total. `sprint` is a
   required endpoint, and Gold must expose `total_points = race + sprint`.
3. **Idempotency is a two-part contract and both halves are required.**
   Ingestion skips closed rounds and re-pulls the open one
   (`src/ingestion/landing_writer.py`); Silver deduplicates by natural key on
   the greatest `_ingest_ts` (`src/pipeline/02_silver_facts.py`). Removing
   either double-counts the live round.
4. **Bronze reads files as `text`, not `json`.** Optional Ergast fields
   (FastestLap, Sprint, Q3) appear in only some files, so JSON schema inference
   plus `addNewColumns` fails-and-retries the pipeline on first encounter.
   Payloads are single-line JSON, so `wholeText` gives one row per file and
   Silver parses with explicit schemas.
5. **Silver facts are materialized views, not streaming tables.** Deduplication
   is a full-partition window function, which streaming append mode cannot
   express. The SCD-2 CDC sources read Bronze directly because Auto CDC
   requires a streaming source and MVs cannot be streamed.
6. **Gold joins dimensions as-of `race_date`**, never on the current row. A
   current-row join silently reattributes historical results to a driver's
   present team.

## Non-negotiable design decisions — Lakebase / RAG / agent

7. **Weather is seeded from Gold, never fetched locally a second time.**
   `f1lake/seed_gold.seed_weather()` maps `f1.gold.race_conditions` into
   `f1_race_weather`. There is deliberately no `harvest/weather.py`. Do not add
   a second Open-Meteo call back in; if a weather field is missing
   (`wind_gusts_max`, `temp_mean`), extend `race_conditions` in Gold rather
   than fetching it independently.
8. **Every agent read goes through Postgres, never Delta.** Free Edition's
   daily compute quota is unrecoverable until the next day; an agent that
   spends a warehouse query per turn dies mid-demo. Gold is seeded into
   Lakebase; every tool in `f1_broker.py` reads Postgres only.
9. **Identity resolution is returned, never assumed.** `resolve_driver` /
   `resolve_race` report what they matched, and raise on an ambiguous match
   rather than picking the first row.
10. **Writes go through `schema.returning()`, never `schema.query()`.** An
    `INSERT … RETURNING` run through `query()` looks entirely successful — it
    hands back the new row — but never commits, so the row does not survive
    the connection closing. Verified by `f1lake/schema.smoke_test()`, which
    checks on a **new** connection on purpose.
11. **`f1_broker.py` is a single source of truth**, canonically
    `mcp_server/f1_broker.py`. `app/f1_broker.py`, and the `f1lake/` copies
    inside both app directories, are GENERATED — `scripts/build_app.sh`
    regenerates them from the canonical copies. Run it (both `app` and
    `mcp_server`) before committing a change to `f1lake/{schema,embedder,
    __init__}.py` or `mcp_server/f1_broker.py`, and check `git diff` for drift.

## API conventions

- Modern Lakeflow API only: `from pyspark import pipelines as dp`,
  `dp.create_auto_cdc_flow`, `@dp.materialized_view`. Never `import dlt`,
  `apply_changes`, or `LIVE.` prefixes.
- `CREATE OR REFRESH`, never `CREATE OR REPLACE`, for pipeline datasets.
- SCD-2 columns are `__START_AT` / `__END_AT` (double underscore). Current
  rows: `__END_AT IS NULL`.
- Poll a pipeline *update*, not the pipeline, and read
  `error.exceptions[0].message` for the real error — the top-level message only
  says "Update X is FAILED".
- Volume paths in `databricks fs` need the `dbfs:` prefix.

## Jolpica API

- Base `https://api.jolpi.ca/ergast/f1`, no auth. 4 req/s burst, 500 req/hr.
  429s are routine — the client backs off and recovers.
- `total` in a paginated response counts *inner* records, not outer array
  elements. Paging on `len(outer_array)` loops forever.
- Raw JSON is cached in the Volume, so re-runs never need to re-hit the API.
  **Never loop the backfill to fix a downstream bug.**

## Wikipedia

- Requires a real contact-string User-Agent (`WIKI_USER_AGENT`); a generic one
  throttles after ~10 requests.
- Resumable by design: `harvest/run.py` keeps what a previous run already
  fetched and only requests what is missing.

## Verification bar

Nothing is "done" until `sql/validation_checks.sql` passes: reconciliation
returns zero rows, `dim_driver` returns rows for `__END_AT IS NOT NULL`, and no
Silver fact has duplicate natural keys — plus, for the Lakebase side,
`python3 -m f1lake.schema --smoke` succeeds on a fresh connection.
