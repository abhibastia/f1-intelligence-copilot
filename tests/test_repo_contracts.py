"""Contract tests: the rules in CLAUDE.md, enforced instead of remembered.

Every assertion here corresponds to a mistake that has actually been made in
this repository and cost either a failed pipeline update or a dashboard tile
that rendered nothing. They run in milliseconds with no Spark, no workspace and
no credentials, so there is no reason not to run them before every push.

    pytest                       # these plus the ingestion suite
"""

import ast
import json
import re
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
PIPELINE_FILES = sorted((ROOT / "src" / "pipeline").glob("*.py"))
DASHBOARDS = sorted((ROOT / "dashboards").glob("*.lvdash.json"))


# ───────────────────────────── pipeline API ──────────────────────────────

# Legacy DLT constructs. The modern API is `from pyspark import pipelines as dp`;
# mixing the two produces errors that name neither.
FORBIDDEN = {
    r"\bimport dlt\b": "use `from pyspark import pipelines as dp`",
    r"\bdlt\.": "use the `dp.` namespace",
    r"\bapply_changes\s*\(": "use `dp.create_auto_cdc_flow`",
    r"\bLIVE\.": "the LIVE. prefix errors in modern pipelines",
    r"CREATE OR REPLACE\s+(STREAMING TABLE|MATERIALIZED VIEW)":
        "use CREATE OR REFRESH for pipeline datasets",
}


@pytest.mark.parametrize("path", PIPELINE_FILES, ids=lambda p: p.name)
def test_pipeline_file_uses_modern_api(path):
    source = path.read_text()
    for pattern, why in FORBIDDEN.items():
        assert not re.search(pattern, source), f"{path.name}: {why}"


@pytest.mark.parametrize("path", PIPELINE_FILES, ids=lambda p: p.name)
def test_pipeline_file_parses(path):
    """A syntax error here costs a cluster start to discover."""
    ast.parse(path.read_text())


def test_scd2_columns_use_double_underscore():
    """`__START_AT` / `__END_AT`. A single underscore silently matches nothing."""
    gold = (ROOT / "src" / "pipeline" / "04_gold.py").read_text()
    assert "__START_AT" in gold and "__END_AT" in gold
    assert not re.search(r"(?<!_)_START_AT", gold), "single-underscore SCD-2 column"


@pytest.mark.parametrize("path", PIPELINE_FILES, ids=lambda p: p.name)
def test_catalog_has_no_silent_fallback(path):
    """`spark.conf.get("f1.catalog")` must have no default.

    A fallback of "f1" means a prod pipeline whose configuration block is
    missing or misspelled writes into the dev catalog and reports success.
    Nothing downstream can tell that happened afterwards — the tables are
    simply in the wrong place, populated and plausible.
    """
    source = path.read_text()
    assert not re.search(r'spark\.conf\.get\(\s*"f1\.catalog"\s*,', source), (
        f"{path.name}: f1.catalog has a default; it must fail instead"
    )


def test_jobs_are_bounded_and_alert_on_failure():
    """Every job needs a timeout and somewhere to shout.

    On Free Edition compute is a daily allowance: a hung task spends
    tomorrow's run as well as today's, and an unwatched failure is
    indistinguishable from a healthy week with no new races.
    """
    import yaml  # noqa: PLC0415 — only this test needs it

    for path in sorted((ROOT / "resources").glob("*.job.yml")):
        jobs = (yaml.safe_load(path.read_text()) or {})["resources"]["jobs"]
        for name, job in jobs.items():
            assert job.get("timeout_seconds"), f"{name} has no timeout_seconds"
            assert job.get("email_notifications", {}).get("on_failure"), (
                f"{name} notifies nobody on failure"
            )


def test_scheduled_job_validates_its_output():
    """The run that happens weekly must check correctness, not just finish.

    Validation that only runs when a human asks is not a guarantee.
    """
    import yaml  # noqa: PLC0415

    for path in sorted((ROOT / "resources").glob("*.job.yml")):
        jobs = (yaml.safe_load(path.read_text()) or {})["resources"]["jobs"]
        for name, job in jobs.items():
            if "schedule" not in job:
                continue
            task_keys = {t["task_key"] for t in job["tasks"]}
            assert "validate" in task_keys, (
                f"{name} is scheduled but never validates its marts"
            )


def test_expectation_columns_resolve():
    """Delegates to the pre-flight checker, which walks every Silver file."""
    result = subprocess.run(
        [sys.executable, "scripts/check_expectations.py"],
        cwd=ROOT, capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_every_document_is_reachable_from_the_readme():
    """A document nothing links to is a document nobody reads.

    `docs/runbook.md` is a required deliverable and was referenced from nowhere
    — not the README, not the architecture document, not the ADRs. A reviewer
    had to browse docs/ to find it, which is the same as it not existing.
    """
    readme = (ROOT / "README.md").read_text()
    unlinked = [p.name for p in sorted((ROOT / "docs").glob("*.md"))
                if p.name not in readme]
    assert not unlinked, (
        f"documents not linked from the README: {unlinked}. "
        f"Add them to the Layout table, or delete them."
    )


def test_no_document_claims_the_wrong_dashboard_page_count():
    """The demo script said five decision pages when there were six.

    Counts written in prose drift silently, and the demo script is the one
    document somebody reads aloud under pressure.
    """
    pages = len(json.loads(
        (ROOT / "dashboards" / "f1_race_intelligence.lvdash.json").read_text())["pages"])
    words = {1: "one", 2: "two", 3: "three", 4: "four", 5: "five", 6: "six", 7: "seven"}
    for doc in sorted((ROOT / "docs").glob("*.md")) + [ROOT / "README.md"]:
        text = doc.read_text()
        for n, word in words.items():
            if n == pages:
                continue
            for claim in (f"{word} decision pages", f"{n} decision pages"):
                assert claim not in text, (
                    f"{doc.name} claims {claim!r} but the dashboard has {pages}")


# ─────────────────────────────── dashboards ──────────────────────────────

def _widgets(dashboard):
    for page in dashboard.get("pages", []):
        for item in page.get("layout", []):
            yield item["widget"]


# widgetType -> the only spec version that renders. A wrong version does not
# error: the tile draws "Visualization has no fields selected", which reads
# like a field mismatch and is not one.
WIDGET_VERSIONS = {
    "counter": 2, "table": 2, "bar": 3, "line": 3,
    "pie": 3, "scatter": 3, "area": 3,
    "filter-single-select": 2, "filter-multi-select": 2,
    "filter-date-range-picker": 2, "symbol-map": 2,
}


@pytest.mark.parametrize("path", DASHBOARDS, ids=lambda p: p.name)
def test_widget_spec_versions(path):
    dashboard = json.loads(path.read_text())
    for widget in _widgets(dashboard):
        spec = widget.get("spec")
        if not spec:
            continue
        expected = WIDGET_VERSIONS.get(spec["widgetType"])
        if expected is None:
            continue
        assert spec["version"] == expected, (
            f"{path.name}: {widget['name']} is {spec['widgetType']} "
            f"v{spec['version']}, must be v{expected}"
        )


@pytest.mark.parametrize("path", DASHBOARDS, ids=lambda p: p.name)
def test_no_field_named_constructor(path):
    """`constructor` is inherited by every JavaScript object.

    A field by that name resolves to `Object.prototype.constructor` in the
    renderer's lookup instead of missing, so the encoding binds to a function
    and the tile draws nothing — silently, with no error anywhere. Alias the
    column to `team`; `displayName` can still read "Constructor".
    """
    blob = path.read_text()
    assert '"constructor"' not in blob, (
        f"{path.name}: a field is named `constructor` — alias it to `team`"
    )


@pytest.mark.parametrize("path", DASHBOARDS, ids=lambda p: p.name)
def test_filters_bind_to_real_columns(path):
    """`associative_filter_predicate_group` is not a column and never was."""
    assert "associative_filter_predicate_group" not in path.read_text(), (
        f"{path.name}: filter queries a column that does not exist"
    )


@pytest.mark.parametrize("path", DASHBOARDS, ids=lambda p: p.name)
def test_widget_fields_match_encodings(path):
    """`query.fields[].name` must equal `encodings.*.fieldName`, exactly.

    A mismatch renders "no selected fields to visualize".
    """
    dashboard = json.loads(path.read_text())
    for widget in _widgets(dashboard):
        spec = widget.get("spec")
        if not spec:
            continue
        available = {
            field["name"]
            for query in widget.get("queries", [])
            for field in query["query"].get("fields", [])
        }
        referenced = []

        def collect(node):
            if isinstance(node, dict):
                if "fieldName" in node:
                    referenced.append(node["fieldName"])
                for value in node.values():
                    collect(value)
            elif isinstance(node, list):
                for value in node:
                    collect(value)

        collect(spec["encodings"])
        missing = sorted(set(referenced) - available)
        assert not missing, (
            f"{path.name}: {widget['name']} encodes {missing}, "
            f"which its query does not select"
        )


@pytest.mark.parametrize("path", DASHBOARDS, ids=lambda p: p.name)
def test_dataset_queries_use_bare_table_names(path):
    """Catalog and schema are bound at deploy time by the bundle.

    Hard-coding `f1.gold.x` in a dataset query makes the dashboard undeployable
    against any other catalog — including the prod target.
    """
    dashboard = json.loads(path.read_text())
    for dataset in dashboard["datasets"]:
        sql = "".join(dataset["queryLines"])
        assert not re.search(r"FROM\s+\w+\.\w+\.", sql, re.I), (
            f"{path.name}: dataset {dataset['name']} hard-codes a catalog"
        )


# ──────────────────────────────── bundle ─────────────────────────────────

def test_bundle_pins_no_workspace_or_profile():
    """A pinned host or profile makes the bundle undeployable by anyone else."""
    bundle = (ROOT / "databricks.yml").read_text()
    targets = bundle.split("targets:", 1)[1]
    assert not re.search(r"^\s+profile:", targets, re.M), \
        "databricks.yml pins a CLI profile — pass --profile instead"
    assert not re.search(r"^\s+host:", targets, re.M), \
        "databricks.yml pins a workspace host — it comes from the profile"
