"""Unit tests for the Lakeflow pipeline's transformation logic.

These run ON Databricks, as the first task of the `f1_end_to_end` job, before
the pipeline is allowed to start. They need a real Spark session — the things
worth testing here are Column expressions and `from_json` schemas, and a mock
of those tests nothing but the mock.

    databricks bundle run f1_end_to_end -t dev --profile <profile>

WHY NOT LOCAL PYTEST
--------------------
PySpark needs a JVM. Requiring every contributor to install Java to run the test
suite is a bad trade when the pipeline's own runtime is a keystroke away, so the
local suite (`pytest`) covers the pure-Python ingestion layer and this file
covers the pipeline layer where Spark actually lives.

HOW IT LOADS THE PIPELINE CODE
------------------------------
It executes the real pipeline files with a stubbed `pyspark.pipelines`, so the
decorators become no-ops and the module-level constants — the parsers, the
schemas, the rule dicts — land in a namespace we can assert against. No
transformation logic is copied here. A test that restates the expression it is
testing passes forever, including after the expression becomes wrong.

Written as plain asserts with a `main()` rather than pytest, because the
serverless job environment has no test runner and adding one to buy `assert`
would be a dependency for nothing.
"""

import json
import sys
import types
from pathlib import Path

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

def _find_pipeline_dir() -> Path:
    """Locate src/pipeline without relying on `__file__`.

    A serverless `spark_python_task` execs the file rather than importing it,
    so `__file__` is undefined — the module-level `Path(__file__)` this
    replaced failed the task before a single test ran. `sys.argv[0]` carries
    the path in that environment, and walking upward for the directory works
    from any of the three ways this file gets run: job task, `python
    tests/spark/...` from the repo root, or an editor's run button.
    """
    candidates = []
    if "__file__" in globals():
        candidates.append(Path(globals()["__file__"]).resolve().parent)
    if sys.argv and sys.argv[0]:
        candidates.append(Path(sys.argv[0]).resolve().parent)
    candidates.append(Path.cwd())

    for start in candidates:
        for directory in (start, *start.parents):
            found = directory / "src" / "pipeline"
            if found.is_dir():
                return found
    raise RuntimeError(
        f"src/pipeline not found from any of {[str(c) for c in candidates]}"
    )


PIPELINE_DIR = _find_pipeline_dir()

# What the pipeline files read out of `spark.conf` at import time. The
# landing_root is read with no default in 01_bronze, so it must be answerable
# before any pipeline file is executed. Nothing reads a file here.
CONF = {"f1.catalog": "f1", "f1.landing_root": "/Volumes/f1/raw/landing"}


class _Conf:
    """Answers the pipeline's `f1.*` keys, delegates everything else.

    Serverless runs on Spark Connect, which refuses arbitrary SQL
    configuration: `spark.conf.set("f1.catalog", ...)` does not stick and the
    matching `get` raises CONFIG_NOT_AVAILABLE. The pipeline files themselves
    are fine — Lakeflow supplies those keys from the pipeline's `configuration`
    block — but a plain job task has no such block, so the test supplies them.
    """

    def __init__(self, real, overrides):
        self._real = real
        self._overrides = overrides

    def get(self, key, default=None):
        if key in self._overrides:
            return self._overrides[key]
        return self._real.get(key, default)

    def set(self, key, value):
        self._overrides[key] = value


class _Spark:
    """A SparkSession with `conf` swapped out. Everything else passes through."""

    def __init__(self, session, overrides):
        self._session = session
        self.conf = _Conf(session.conf, overrides)

    def __getattr__(self, name):
        return getattr(self._session, name)


def _stub_pipelines_module():
    """A `dp` whose every decorator is a no-op.

    Returns a module object that answers any attribute with a decorator
    factory, so `@dp.materialized_view(name=..., cluster_by=[...])`,
    `@dp.expect_all_or_drop({...})` and `dp.create_auto_cdc_flow(...)` all
    evaluate without registering anything.
    """
    mod = types.ModuleType("pyspark.pipelines")

    def anything(*_args, **_kwargs):
        def decorate(fn):
            return fn
        return decorate

    def module_getattr(name):
        # Dunders must still miss. Answering `__file__` with a function sends
        # inspect.getmodule down a path that ends in AttributeError, which
        # replaces a readable test failure with an unreadable traceback.
        if name.startswith("__") and name.endswith("__"):
            raise AttributeError(name)
        return anything

    mod.__getattr__ = module_getattr
    for name in (
        "table", "materialized_view", "temporary_view", "view",
        "expect", "expect_or_fail", "expect_or_drop",
        "expect_all", "expect_all_or_drop", "expect_all_or_fail",
        "create_streaming_table", "create_auto_cdc_flow", "append_flow",
    ):
        setattr(mod, name, anything)
    return mod


def load(filename, spark):
    """Execute one pipeline file and hand back its module namespace."""
    source = (PIPELINE_DIR / filename).read_text()
    ns = {
        "__name__": filename.replace(".py", ""),
        # Pipeline files never reference __file__ — Lakeflow does not define it
        # either — but providing it costs nothing and keeps a future one honest.
        "__file__": str(PIPELINE_DIR / filename),
        "spark": spark,
    }
    sys.modules["pyspark.pipelines"] = _stub_pipelines_module()
    exec(compile(source, filename, "exec"), ns)
    return ns


# ─────────────────────────────── the tests ───────────────────────────────

def test_lap_time_parsing(spark, laps):
    """`1:27.623` is 87.623 seconds, and a NULL stays NULL.

    Casting a lap time straight to DOUBLE returns NULL for every M:SS.mmm
    value, and a NULL lap time is indistinguishable from a lap nobody set —
    which is why this is parsed rather than cast.
    """
    cases = [
        ("1:27.623", 87.623),   # the ordinary case
        ("0:59.812", 59.812),   # sub-minute, still colon-formatted
        ("59.812", 59.812),     # no colon at all
        ("2:03.500", 123.5),    # minutes carry correctly
        ("10:15.250", 615.25),  # red-flag lap, two-digit minutes
        (None, None),           # absence survives as absence
    ]
    df = spark.createDataFrame(
        [Row(lap_time_raw=raw) for raw, _ in cases]
    ).withColumn("parsed", laps["LAP_SECONDS"])
    got = [r["parsed"] for r in df.collect()]
    want = [expected for _, expected in cases]
    for raw_case, g, w in zip(cases, got, want):
        assert (g is None and w is None) or abs(g - w) < 1e-6, \
            f"lap time {raw_case[0]!r} parsed as {g}, expected {w}"


def test_pit_duration_parsing(spark, pits):
    """Both published duration formats parse, and the split is where it should be.

    83 of 2,081 stops in this dataset arrive as M:SS.mmm because the car sat in
    the pit lane through a red flag. A naive cast drops exactly those — not a
    random 4%, but precisely the stops where something unusual happened.
    """
    cases = [
        ("21.789", 21.789, True),      # a normal stop, timed
        ("2.100", 2.1, True),          # a drive-through, still service
        ("12:57.770", 777.77, False),  # red flag, not a service stop
        ("35:54.149", 2154.149, False),  # 35*60 + 54.149
        (None, None, None),
    ]
    df = (
        spark.createDataFrame([Row(duration_raw=raw) for raw, _, _ in cases])
        .withColumn("duration_s", pits["DURATION_SECONDS"])
    )
    df = df.withColumn(
        "is_service_stop",
        F.col("duration_s") < F.lit(pits["SERVICE_STOP_MAX_SECONDS"]),
    )
    for row, (raw, want_s, want_service) in zip(df.collect(), cases):
        got = row["duration_s"]
        assert (got is None and want_s is None) or abs(got - want_s) < 1e-6, \
            f"duration {raw!r} parsed as {got}, expected {want_s}"
        assert row["is_service_stop"] == want_service, \
            f"duration {raw!r} service flag {row['is_service_stop']}, " \
            f"expected {want_service}"


def test_laps_schema_survives_three_levels(spark, laps):
    """The laps payload nests three deep, and the schema must reach the bottom.

    Every other endpoint nests twice: a race holds an array of records. Laps
    hold laps which hold one timing per driver. A schema that stops at two
    levels parses without error and yields an empty array, so this asserts the
    row count after all three explodes rather than that parsing "worked".
    """
    payload = json.dumps({
        "MRData": {"RaceTable": {"Races": [{
            "season": "2024", "round": "1",
            "Laps": [
                {"number": "1", "Timings": [
                    {"driverId": "verstappen", "position": "1", "time": "1:33.421"},
                    {"driverId": "leclerc", "position": "2", "time": "1:34.008"},
                ]},
                {"number": "2", "Timings": [
                    {"driverId": "verstappen", "position": "1", "time": "1:32.100"},
                ]},
            ],
        }]}}
    })
    df = (
        spark.createDataFrame([Row(raw_payload=json.dumps({"payload": json.loads(payload)}))])
        .withColumn("parsed", F.from_json("raw_payload", laps["LAPS_SCHEMA"]))
        .select(F.explode("parsed.payload.MRData.RaceTable.Races").alias("race"))
        .select(F.explode("race.Laps").alias("lap_rec"))
        .select(F.explode("lap_rec.Timings").alias("t"))
    )
    assert df.count() == 3, \
        f"three timings across two laps should explode to 3 rows, got {df.count()}"
    assert df.select("t.driverId").first()["driverId"] == "verstappen"


def test_expectation_rules_reference_real_columns(laps, pits):
    """Rule dicts name columns the staged view actually emits.

    `scripts/check_expectations.py` proves this statically for every Silver
    file. This asserts the dicts are non-empty and wired to the right column
    names, so a rule set that silently became `{}` — which passes every static
    check and validates nothing at runtime — fails here.
    """
    assert laps["LAP_RULES"], "LAP_RULES is empty"
    assert pits["PIT_STOP_RULES"], "PIT_STOP_RULES is empty"
    joined = " ".join(laps["LAP_RULES"].values())
    assert "lap_time_s" in joined, "no rule guards the parsed lap time"
    assert "duration_s" in " ".join(pits["PIT_STOP_RULES"].values()), \
        "no rule guards the parsed stop duration"


def test_wet_threshold_matches_ingestion_config(weather):
    """Silver and the ingestion config must agree on what 'wet' means.

    Two definitions of a threshold is how one dashboard tile disagrees with
    the next about the same race.
    """
    sys.path.insert(0, str(PIPELINE_DIR.parent / "ingestion"))
    import config  # noqa: PLC0415  — path is only valid once inserted above

    assert weather["WET_THRESHOLD_MM"] == config.WET_THRESHOLD_MM, (
        f"Silver says {weather['WET_THRESHOLD_MM']} mm, "
        f"config says {config.WET_THRESHOLD_MM} mm"
    )


def main() -> int:
    session = SparkSession.builder.appName("f1-pipeline-unit-tests").getOrCreate()
    spark = _Spark(session, dict(CONF))

    laps = load("02d_silver_laps.py", spark)
    pits = load("02c_silver_pitstops.py", spark)
    weather = load("02b_silver_weather.py", spark)

    tests = [
        ("lap time parsing", lambda: test_lap_time_parsing(spark, laps)),
        ("pit duration parsing", lambda: test_pit_duration_parsing(spark, pits)),
        ("laps schema depth", lambda: test_laps_schema_survives_three_levels(spark, laps)),
        ("expectation rules", lambda: test_expectation_rules_reference_real_columns(laps, pits)),
        ("wet threshold agreement", lambda: test_wet_threshold_matches_ingestion_config(weather)),
    ]

    failures = 0
    for name, run in tests:
        try:
            run()
            print(f"  PASS  {name}")
        except AssertionError as exc:
            failures += 1
            print(f"  FAIL  {name}: {exc}")
        except Exception as exc:  # noqa: BLE001 — a broken test is a failed test
            failures += 1
            print(f"  ERROR {name}: {type(exc).__name__}: {exc}")

    print(f"\n{len(tests) - failures}/{len(tests)} pipeline unit tests passed")
    return 1 if failures else 0


# A serverless task treats *any* SystemExit as a failed workload — including
# SystemExit(0). Exiting cleanly therefore means falling off the end of the
# module, and only a failure raises.
if __name__ == "__main__":
    _failures = main()
    if _failures:
        raise SystemExit(f"{_failures} pipeline unit test(s) failed — see the PASS/FAIL lines above")
