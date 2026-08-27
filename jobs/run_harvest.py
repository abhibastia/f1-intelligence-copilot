"""Databricks task: harvest Wikipedia race reports into Lakebase.

Runs the SAME modules as the local CLI (harvest.races, harvest.wikipedia). The
only difference is where the race spine comes from: locally it reads the
medallion pipeline's own landing directory, and on Databricks it reads the
Unity Catalog Volume the ingestion job writes. The layouts are identical, so
F1_LANDING_DIR is the whole adaptation.

WRITES PER RACE, NOT AT THE END
--------------------------------
Each race's report is chunked and written to Lakebase immediately after it's
fetched (harvest.wikipedia.fetch_and_store), and "already harvested" is
checked against Lakebase (f1lake.load.harvested_races()) rather than a local
file. This job previously fetched everything into /tmp and only wrote to
Lakebase once, at the end - so a kill partway through (documented in
resources/f1_jobs.yml as "kernel is killed as unresponsive" on Databricks'
shared, more heavily throttled egress IP) lost all progress, and every retry
restarted from zero. Writing per race means a kill loses at most one race, and
the next run - retry or scheduled - only re-fetches what's actually missing.

NOT HARVESTED HERE
-------------------
Weather is seeded from the governed f1.gold.race_conditions mart
(f1lake.seed_gold.seed_weather), not fetched from Open-Meteo a second time.
Pit stops are seeded from the governed f1.silver.fact_pit_stop table
(f1lake.seed_gold.seed_pit_stops), not fetched from Jolpica a second time -
both endpoints are already ingested by f1_ingest_incremental. Wikipedia prose
has no equivalent in the governed pipeline, so it's the one thing this job
still fetches.

__file__ is not bound under a serverless spark_python_task (the file is run
through exec), so the repo root is located without it.
"""
import datetime, logging, os, sys

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("harvest-job")

_here = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
ROOT = os.path.dirname(_here)
sys.path.insert(0, ROOT)

os.environ.setdefault("F1_LANDING_DIR", "/Volumes/f1/raw/landing")

from harvest import races as R, wikipedia as WK
from f1lake import schema, load as L

schema.ensure_schema()

all_races = R.load_races()
run = R.completed_races(all_races, datetime.date.today().isoformat())
have = L.harvested_races()
todo = [r for r in run if (r.season, r.round) not in have]
log.info("races known=%d, already run=%d, already harvested=%d, fetching=%d",
         len(all_races), len(run), len(have), len(todo))

stats = WK.fetch_and_store(todo, store=lambda report: L.load_documents([report]))
log.info("race reports: %d stored, %d failed", stats["fetched"], stats["failed"])
