"""Run the correctness checks and fail loudly. The executable half of validation.

    python3 scripts/validate_marts.py --catalog f1        # as a job task
    databricks bundle run f1_end_to_end -t dev            # runs it after the pipeline

`sql/validation_checks.sql` is the narrative version — it explains what each
check proves and why the exemptions are exemptions, and a human reads it. This
file is the same checks with an exit code, so a scheduled run cannot quietly
publish marts that fail them.

It runs on Spark rather than through a SQL warehouse on purpose: the job's
pipeline task has already paid for serverless compute, and going through a
warehouse would start a second one against the same Free Edition daily quota.

Every check declares what it expects. `zero` means the query must return no
rows; `nonzero` means it must return some — SCD-2 history is the check that
fails by being empty, and reporting "0 rows, all clear" for it would invert the
result.
"""

import argparse
import sys

from pyspark.sql import SparkSession

CHECKS = [
    (
        "championship reconciliation",
        "zero",
        """
        WITH earned AS (
          SELECT season, driver_id, SUM(total_points) AS p
          FROM {c}.gold.driver_performance GROUP BY 1, 2
        ),
        final_round AS (
          SELECT season, MAX(round) AS last_round
          FROM {c}.gold.championship_progression GROUP BY 1
        ),
        official AS (
          SELECT c.season, c.driver_id, c.cumulative_points AS s
          FROM {c}.gold.championship_progression c
          JOIN final_round f ON f.season = c.season AND f.last_round = c.round
        )
        SELECT o.season, o.driver_id, e.p, o.s
        FROM official o JOIN earned e
          ON e.season = o.season AND e.driver_id = o.driver_id
        WHERE ABS(e.p - o.s) > 1e-9
        """,
    ),
    (
        "constructor championship reconciliation",
        "zero",
        """
        WITH final_round AS (
          SELECT season, MAX(round) AS last_round
          FROM {c}.gold.constructor_standings GROUP BY season
        ),
        official AS (
          SELECT s.season, s.constructor_id, s.cumulative_points AS p
          FROM {c}.gold.constructor_standings s
          JOIN final_round f ON f.season = s.season AND f.last_round = s.round
        ),
        earned AS (
          SELECT season, constructor_id_as_of_race AS constructor_id,
                 SUM(total_points) AS p
          FROM {c}.gold.driver_performance
          WHERE constructor_id_as_of_race IS NOT NULL
          GROUP BY season, constructor_id_as_of_race
        )
        -- Two independent endpoints, parsed separately, made to agree. The
        -- as-of-race attribution is what makes it non-trivial: a driver who
        -- changes team mid-season carries their points with them.
        SELECT o.season, o.constructor_id, e.p, o.p
        FROM official o
        LEFT JOIN earned e
          ON e.season = o.season AND e.constructor_id = o.constructor_id
        WHERE ABS(COALESCE(e.p, 0) - o.p) > 1e-9
        """,
    ),
    (
        "SCD-2 history exists",
        "nonzero",
        "SELECT driver_id FROM {c}.silver.dim_driver WHERE __END_AT IS NOT NULL",
    ),
    (
        "one current row per driver",
        "zero",
        """
        SELECT driver_id FROM {c}.silver.dim_driver
        WHERE __END_AT IS NULL GROUP BY driver_id HAVING COUNT(*) <> 1
        """,
    ),
    (
        "no duplicate natural keys (driver x race facts)",
        "zero",
        """
        SELECT 'fact_result' AS t, season, round, driver_id
        FROM {c}.silver.fact_result GROUP BY 1,2,3,4 HAVING COUNT(*) > 1
        UNION ALL SELECT 'fact_qualifying', season, round, driver_id
        FROM {c}.silver.fact_qualifying GROUP BY 1,2,3,4 HAVING COUNT(*) > 1
        UNION ALL SELECT 'fact_driver_standing', season, round, driver_id
        FROM {c}.silver.fact_driver_standing GROUP BY 1,2,3,4 HAVING COUNT(*) > 1
        UNION ALL SELECT 'fact_sprint_result', season, round, driver_id
        FROM {c}.silver.fact_sprint_result GROUP BY 1,2,3,4 HAVING COUNT(*) > 1
        """,
    ),
    (
        "no duplicate natural keys (lap and pit stop)",
        "zero",
        """
        SELECT 'fact_lap' AS t, season, round, lap, driver_id
        FROM {c}.silver.fact_lap GROUP BY 1,2,3,4,5 HAVING COUNT(*) > 1
        UNION ALL SELECT 'fact_pit_stop', season, round, stop_number, driver_id
        FROM {c}.silver.fact_pit_stop GROUP BY 1,2,3,4,5 HAVING COUNT(*) > 1
        """,
    ),
    (
        "lap timings reconcile with results",
        "zero",
        """
        WITH timed AS (
          SELECT season, round, driver_id, MAX(lap) AS last_timed_lap
          FROM {c}.silver.fact_lap GROUP BY 1, 2, 3
        )
        SELECT r.season, r.round, r.driver_name, r.laps_completed, t.last_timed_lap
        FROM {c}.gold.driver_performance r
        JOIN timed t ON t.season = r.season AND t.round = r.round
                    AND t.driver_id = r.driver_id
        -- Disqualification zeroes the results lap count while the laps driven
        -- were still timed; a lapped runner is classified on the lap the leader
        -- finished, so one lap of difference is expected, not tolerated error.
        WHERE r.status <> 'Disqualified'
          AND ABS(t.last_timed_lap - r.laps_completed) > 1
        """,
    ),
    (
        "pit stops fall inside the race",
        "zero",
        """
        WITH race_distance AS (
          SELECT season, round, MAX(lap) AS last_lap
          FROM {c}.silver.fact_lap GROUP BY 1, 2
        )
        SELECT p.season, p.round, p.driver_id, p.lap
        FROM {c}.silver.fact_pit_stop p
        JOIN race_distance rd ON rd.season = p.season AND rd.round = p.round
        WHERE p.lap < 1 OR p.lap > rd.last_lap
        """,
    ),
    (
        "stop-number gaps are explained by quarantine",
        "zero",
        """
        WITH gaps AS (
          SELECT season, round, driver_id,
                 COUNT(*) AS kept, MAX(stop_number) AS highest
          FROM {c}.silver.fact_pit_stop GROUP BY 1, 2, 3
          HAVING MAX(stop_number) <> COUNT(*)
        ),
        dropped AS (
          SELECT season, round, driver_id, COUNT(*) AS quarantined
          FROM {c}.silver.quarantine_pit_stop GROUP BY 1, 2, 3
        )
        SELECT g.season, g.round, g.driver_id
        FROM gaps g LEFT JOIN dropped d
          ON d.season = g.season AND d.round = g.round AND d.driver_id = g.driver_id
        WHERE g.highest - g.kept <> COALESCE(d.quarantined, 0)
        """,
    ),
    (
        "lap_pace arithmetic holds",
        "zero",
        """
        SELECT season, round, driver_name FROM {c}.gold.lap_pace
        WHERE clean_laps > laps_recorded
           OR laps_led > laps_recorded
           OR median_clean_lap_s < best_lap_s
           OR pace_deficit_pct < 0
           OR clean_laps < 1
        """,
    ),
    (
        "every raced round has laps and pit stops",
        "zero",
        """
        WITH raced AS (SELECT DISTINCT season, round FROM {c}.silver.fact_result)
        SELECT r.season, r.round FROM raced r
        WHERE NOT EXISTS (SELECT 1 FROM {c}.silver.fact_lap l
                           WHERE l.season = r.season AND l.round = r.round)
           OR NOT EXISTS (SELECT 1 FROM {c}.silver.fact_pit_stop p
                           WHERE p.season = r.season AND p.round = r.round)
        """,
    ),
]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    # Required, not defaulted. The job passes ${var.catalog}; a default would
    # let a prod invocation quietly validate the dev catalog and report green.
    parser.add_argument("--catalog", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("f1-validation").getOrCreate()

    failures = []
    for name, expectation, sql in CHECKS:
        rows = spark.sql(sql.format(c=args.catalog)).count()
        passed = (rows == 0) if expectation == "zero" else (rows > 0)
        print(f"  {'PASS' if passed else 'FAIL'}  {name:48s} "
              f"{rows} row(s), expected {expectation}")
        if not passed:
            failures.append((name, rows, sql))

    # The quarantine census is reported, never asserted. Three sources reject
    # rows on clean Jolpica data — see check 6 in sql/validation_checks.sql —
    # so a threshold here would either be noise or a lie.
    print("\n  quarantine census")
    for source in ("race", "result", "qualifying", "sprint_result",
                   "driver_standing", "constructor_standing",
                   "race_weather", "pit_stop", "lap"):
        table = f"{args.catalog}.silver.quarantine_{source}"
        try:
            print(f"    {source:22s} {spark.table(table).count()}")
        except Exception:  # noqa: BLE001 — a missing quarantine view is not a failure
            print(f"    {source:22s} (not present)")

    print(f"\n{len(CHECKS) - len(failures)}/{len(CHECKS)} checks passed")
    for name, rows, sql in failures:
        print(f"\n─── {name}: {rows} offending row(s) ───{sql}")
    return 1 if failures else 0


# A serverless task treats *any* SystemExit as a failed workload — including
# SystemExit(0). Exiting cleanly therefore means falling off the end of the
# module, and only a failure raises.
if __name__ == "__main__":
    _failures = main()
    if _failures:
        raise SystemExit(f"{_failures} validation check(s) failed — see the offending rows above")
