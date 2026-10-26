# Setup and operations

Everything needed to stand this platform up from a fresh clone, and the exact
commands to run each component end to end. For what the system *is* and why it
is built this way, start at the [root README](../README.md); for day-to-day
operation and failure modes once it is running, see
[`runbook.md`](runbook.md).

## Prerequisites

- Databricks Free Edition workspace, **identity verified** (unlocks outbound
  internet for in-platform API calls)
- Databricks CLI ≥ 0.294, authenticated: `databricks auth login --profile <profile>`
- Python 3.11+ locally
- A Unity Catalog catalog named `f1`
  (**Free Edition cannot create catalogs over the API** — make it once in the
  UI: Catalog → Create catalog → `f1` → Default storage)

## Lakebase

The serving store. Create a project, add a role you control the password for,
and note the endpoint host:

```bash
PROJECT=f1-copilot
databricks postgres create-project $PROJECT \
  --json '{"spec": {"display_name": "F1 Intelligence Copilot"}}' --profile <profile>
databricks postgres list-branches  projects/$PROJECT --profile <profile>
databricks postgres list-endpoints projects/$PROJECT/branches/production --profile <profile>
```

Take `status.hosts.host` from the endpoint — that is `<host>`. Then create a
**native Postgres role** with a password (short-lived OAuth credentials expire
after an hour, which is a bad fit for a long-running app):

```bash
databricks postgres create-role projects/$PROJECT/branches/production <role> \
  --json '{"spec": {"password": "<password>"}}' --profile <profile>
```

`ensure_schema()` (Step 4 below) creates every table, index, and the two
extensions it needs — `vector` for embeddings and `unaccent` for name matching.

## Secrets

One secret: a base64-encoded Postgres connection URL.

```bash
databricks secrets create-scope database --profile <profile>
printf 'postgresql://<role>:<password>@<host>:5432/databricks_postgres?sslmode=require' \
  | base64 \
  | databricks secrets put-secret database lakebase-url --profile <profile>
```

Both apps also need read access on this scope — their service principals don't
exist until the apps are created, so that grant is folded into Step 4 below.

## Environment variables

Copy [`.env.example`](../.env.example). No API keys exist in this project —
Jolpica, Wikimedia and Open-Meteo are all free and keyless.

## Local environment

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
export DATABRICKS_CONFIG_PROFILE=<profile>
```

## Runtime

Everything is **serverless** — no cluster to size. The Lakeflow pipeline and
the jobs run on serverless compute; the apps run on Databricks Apps compute.

---

## Running each component end to end

### Step 1 — Governed pipeline (Databricks)

```bash
./scripts/create_catalog.sh                        # schemas + landing Volume
databricks bundle validate --strict -t dev --profile <profile>
databricks bundle deploy -t dev --profile <profile>
databricks bundle run f1_ingest_incremental -t dev --profile <profile>
databricks bundle run f1_medallion_pipeline -t dev --profile <profile>
```

The ingest job calls Jolpica **from inside Databricks** and lands raw JSON in
`f1.raw.landing`; the Lakeflow pipeline reads it through Bronze → Silver → Gold,
producing all six Gold marts. Writes are idempotent, so a re-run costs a
handful of API calls, not hundreds.

To backfill locally instead: `python3 src/ingestion/ingest.py --mode backfill
--root ./landing`, then `./scripts/upload_landing.sh`.

After changing pipeline code, run the gated version instead of the two
`bundle run` lines above — it brackets the same ingest + pipeline work with a
unit-test task before and `validate_marts.py` after, so a typo fails in
seconds rather than after a cluster start, and "the update completed" only
counts once the marts actually reconcile:

```bash
databricks bundle run f1_end_to_end -t dev --profile <profile>
```

Deliberately unscheduled — see `resources/f1_end_to_end.job.yml` — it costs
roughly twice the daily quota of `f1_ingest_incremental`, so it runs on
request, not on a schedule.

### Steps 2–3 — Harvest, embed and seed Lakebase (on Databricks)

The whole Lakebase-serving layer — Wikipedia harvest, embedding, and seeding
all six Gold marts (plus weather, pit stops and derived stints) — runs as one
Databricks job:

```bash
databricks bundle run f1_full_refresh -t dev --profile <profile>
```

`harvest` writes each race to Lakebase immediately after fetching it, so a
retry only re-fetches what's missing; `embed` and `seed_gold` need no
`--profile` at all — they authenticate as the job's own runtime identity. See
`resources/f1_jobs.yml` for what's verified and why (short version: it used to
crash with a generic "Python process exited unexpectedly" that turned out to
be `psycopg2-binary`'s compiled OpenSSL colliding with the serverless
runtime's own native extensions — fixed by switching to `pg8000`, a
pure-Python driver).

**For local testing only**, the same modules run standalone:

```bash
python3 -m harvest.run                   # Wikipedia race reports
python3 -m f1lake.load                   # chunk, embed, load to Lakebase
python3 -m f1lake.load_strategy          # derive stints (after seed_gold)
python3 -m f1lake.seed_gold --profile <profile>   # weather, pit stops, six marts
```

or the whole thing at once: `python3 scripts/full_refresh.py`.

### Step 4 — Deploy the two apps

```bash
databricks apps create f1-intelligence-mcp --profile <profile>
databricks apps create f1-intelligence-ui  --profile <profile>

for APP in f1-intelligence-mcp f1-intelligence-ui; do
  SP=$(databricks apps get "$APP" --profile <profile> -o json \
        | python3 -c 'import json,sys; print(json.load(sys.stdin)["service_principal_client_id"])')
  databricks secrets put-acl database "$SP" READ --profile <profile>
done

./scripts/build_app.sh mcp_server && ./scripts/build_app.sh app
databricks sync mcp_server /Workspace/Users/<you>/f1-intelligence-mcp --profile <profile> --full
databricks apps start  f1-intelligence-mcp --profile <profile>
databricks apps deploy f1-intelligence-mcp \
  --source-code-path /Workspace/Users/<you>/f1-intelligence-mcp --profile <profile>
# repeat for app/ → f1-intelligence-ui
```

`app/app.yaml`'s `MCP_SERVER_URL` is only the footer link, but replace it with
your own MCP app's URL and redeploy.

### Step 5 — Change Data Feed analytics

Runs automatically as the last task of `f1_full_refresh` (Steps 2–3), or on
its own:

```bash
databricks bundle run f1_cdf_analytics -t dev --profile <profile>
```

`cdf_job.json` / `databricks jobs submit` is no longer needed — it existed
because this notebook used to crash under `bundle run` specifically, which
turned out to be the same `psycopg2` issue as Steps 2–3, fixed the same way.
See `resources/f1_jobs.yml` for the full story if you're curious.

### Step 6 — Genie

```bash
databricks workspace mkdirs /Workspace/Users/<you>/genie_spaces --profile <profile>
databricks genie create-space --profile <profile> --json "{
  \"warehouse_id\": \"<warehouse-id>\",
  \"title\": \"F1 Intelligence — Gold\",
  \"parent_path\": \"/Workspace/Users/<you>/genie_spaces\",
  \"serialized_space\": $(python3 -c 'import json;print(json.dumps(open("genie/f1_gold_space.json").read()))')
}"
```

Scoped to Gold only, deliberately: point a natural-language agent at Silver and
it will join a driver to their *current* team and report a past result under
the wrong constructor.

### Step 7 — Tests

```bash
.venv/bin/python -m pytest -q                              # everything runnable locally
.venv/bin/python -m pytest -q -m "not integration"          # skips the live-Lakebase tests
```

Tests marked `integration` open a real Lakebase connection and clean up every
row they write. `tests/spark/` needs a real Spark session and runs on
Databricks as the first task of `f1_end_to_end`, not locally.

Lint/format is `ruff` (config in `pyproject.toml`). `pip install -r
requirements-dev.txt && pre-commit install` runs it on every commit; CI runs
the same check.

### Step 8 — Access control and one-command refresh

```bash
python3 scripts/apply_grants.py --profile <profile> --dry-run
python3 scripts/full_refresh.py --dry-run       # what would run, and current counts
python3 scripts/full_refresh.py --with-spark    # Gold seed + CDF job included
```
