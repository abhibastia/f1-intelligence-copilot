"""Databricks task: seed the Delta Gold marts (plus weather and pit stops)
into Lakebase.

Thin wrapper around f1lake.seed_gold.main() - the same module the verified
local CLI runs (`python -m f1lake.seed_gold`). There is no separate
Databricks-only implementation any more: the earlier version of this file read
Gold with `spark.table(x).collect()`, which forced a recompute on a
materialized view and killed the kernel. f1lake.seed_gold reads through the
SQL Statement Execution API instead - the exact same warehouse query the
verified local path always used, just issued via databricks-sdk instead of
shelling out to the `databricks` CLI (which isn't present in job compute).
That is what makes one implementation correct in both places: called with no
`--profile`, it uses this job's own ambient identity. `--warehouse-id` arrives
via resources/f1_jobs.yml's spark_python_task `parameters`, since job compute
has no CLI profile to auto-discover a default warehouse from.
"""

import logging
import os
import sys

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("seed-job")
_here = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
sys.path.insert(0, os.path.dirname(_here))

from f1lake import seed_gold

seed_gold.main()
