#!/usr/bin/env python3
"""Rebuild everything Lakebase serves, in order, from one command.

    python3 scripts/full_refresh.py            # every stage
    python3 scripts/full_refresh.py --dry-run  # what would run, and current counts
    python3 scripts/full_refresh.py --only embed
    python3 scripts/full_refresh.py --skip seed

WHY THIS EXISTS
---------------
The stages already existed as separate scripts, which is fine once you know
the order and wrong the first time you do not. Embeddings depend on harvested
documents; the Gold seed depends on the Spark pipeline having run; the CDF
analytics depend on there being agent activity to analyse. Running them out of
order does not fail loudly - it produces a half-populated database that looks
fine until a query returns nothing.

It also reports what each stage did, because "it finished" is not the same as
"it worked". Every stage prints the row counts it changed, so a stage that ran
against an empty source is visible rather than silent.

FREE EDITION
------------
The Spark stages cost compute against a daily quota that does not reset until
the next day, so they are OPT-IN rather than default: `--with-spark` includes
the Gold seed and the CDF job. Without it this runs the three stages that need
nothing but network and Lakebase, which is what you want when re-running after
a failure partway through.

Every stage is idempotent. Re-running skips what is already present rather
than duplicating it, so interrupting this and starting again is safe.
"""
import argparse
import os
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


def _counts() -> dict:
    from f1lake import schema
    return schema.query("""
        SELECT (SELECT count(*) FROM f1_races)          AS races,
               (SELECT count(*) FROM f1_documents)      AS documents,
               (SELECT count(*) FROM f1_embeddings)     AS embeddings,
               (SELECT count(*) FROM f1_race_weather)   AS weather,
               (SELECT count(*) FROM f1_pit_stops)      AS pit_stops,
               (SELECT count(*) FROM f1_driver_performance) AS results,
               (SELECT count(*) FROM agent_tool_calls)  AS tool_calls""")[0]


def _report(before: dict, after: dict) -> str:
    moved = [f"{k} {before[k]}->{after[k]}" for k in after if after[k] != before.get(k)]
    return ", ".join(moved) if moved else "no change"


# Each stage is (name, description, callable). Ordered by dependency: nothing
# here can be moved without breaking what follows it.
def stage_schema():
    from f1lake import schema
    schema.ensure_schema()


def stage_harvest():
    subprocess.run([sys.executable, os.path.join(ROOT, "jobs", "run_harvest.py")],
                   check=True, cwd=ROOT)


def stage_embed():
    subprocess.run([sys.executable, os.path.join(ROOT, "jobs", "run_embed.py")],
                   check=True, cwd=ROOT)


def stage_seed():
    subprocess.run([sys.executable, os.path.join(ROOT, "jobs", "run_seed_gold.py")],
                   check=True, cwd=ROOT)


def stage_cdf():
    """Submitted rather than imported: the notebook needs a Spark session and a
    Databricks runtime, neither of which exist in this process."""
    payload = os.path.join(ROOT, "cdf_job.json")
    subprocess.run(["databricks", "jobs", "submit", "--json", f"@{payload}",
                    "--profile", os.environ.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT")],
                   check=True, cwd=ROOT)


STAGES = [
    ("schema",  "create tables and indexes (idempotent)",        stage_schema, False),
    ("harvest", "race reports, weather, pit stops -> Lakebase",  stage_harvest, False),
    ("embed",   "chunk and embed new documents",                 stage_embed,  False),
    ("seed",    "Delta Gold marts -> Lakebase",                  stage_seed,   True),
    ("cdf",     "tool calls -> Delta -> Change Data Feed",       stage_cdf,    True),
]


def main() -> int:
    names = [s[0] for s in STAGES]
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--only", choices=names, action="append",
                        help="run only these stages (repeatable)")
    parser.add_argument("--skip", choices=names, action="append", default=[],
                        help="skip these stages (repeatable)")
    parser.add_argument("--with-spark", action="store_true",
                        help="include the stages that spend Databricks compute")
    parser.add_argument("--dry-run", action="store_true",
                        help="list what would run, with current row counts")
    args = parser.parse_args()

    selected = [s for s in STAGES
                if (not args.only or s[0] in args.only) and s[0] not in args.skip]
    if not args.only and not args.with_spark:
        selected = [s for s in selected if not s[3]]

    print("\nStages")
    for name, desc, _, spark in selected:
        print(f"  {name:<8} {desc}{'   [spends compute]' if spark else ''}")
    held = [s[0] for s in STAGES if s[3] and s not in selected]
    if held:
        print(f"\n  held back (needs --with-spark): {', '.join(held)}")

    before = _counts()
    print("\nBefore")
    for k, v in before.items():
        print(f"  {k:<12} {v}")

    if args.dry_run:
        print("\nDry run — nothing executed.\n")
        return 0

    print()
    failed = []
    for name, desc, fn, _ in selected:
        started = time.perf_counter()
        print(f"→ {name}: {desc}")
        try:
            fn()
            print(f"  {name} finished in {time.perf_counter() - started:.0f}s")
        except Exception as exc:
            # Carry on rather than aborting. The stages are independent enough
            # that a Wikipedia timeout should not stop the seed from running,
            # and a partial refresh reported honestly beats a run that stops at
            # the first hiccup and leaves you guessing what completed.
            print(f"  {name} FAILED after {time.perf_counter() - started:.0f}s: {exc}")
            failed.append(name)
        print()

    after = _counts()
    print("After")
    for k, v in after.items():
        marker = "  <-- changed" if v != before.get(k) else ""
        print(f"  {k:<12} {v}{marker}")
    print(f"\nSummary: {_report(before, after)}")
    if failed:
        print(f"FAILED stages: {', '.join(failed)}")
        return 1
    print("All stages completed.\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
