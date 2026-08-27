"""The Genie agent definition, checked before it reaches the API.

`genie/f1_gold_space.json` is the natural-language interface over the marts. It
is version-controlled because a space configured only in the UI is a space
nobody can rebuild — and because the thing most worth protecting is its *scope*.

Two classes of assertion here:

**Scope.** The agent may read Gold and nothing else. Point it at Silver and it
will cheerfully join a driver to their current team and report a 2024 result
under the wrong constructor — plausible, formatted, and wrong. The marts already
resolve the as-of-race-date join; that is the whole reason they exist.

**Shape.** The create-space API rejects several things silently enough to waste a
round trip: ids must be 32-char hex and unique across all three lists, text
fields must be arrays, `text_instructions` accepts at most one item, and the
table and instruction lists must be sorted. Catching that here costs
milliseconds instead of an API call.
"""

import json
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPACE = json.loads((ROOT / "genie" / "f1_gold_space.json").read_text())

TABLES = [t["identifier"] for t in SPACE["data_sources"]["tables"]]
SAMPLES = SPACE["config"]["sample_questions"]
SQLS = SPACE["instructions"]["example_question_sqls"]
INSTRUCTIONS = SPACE["instructions"]["text_instructions"]
HEX32 = re.compile(r"^[0-9a-f]{32}$")


# ─────────────────────────────── scope ───────────────────────────────

@pytest.mark.parametrize("identifier", TABLES)
def test_agent_reads_gold_only(identifier):
    assert identifier.startswith("f1.gold."), (
        f"{identifier} is outside Gold. The agent must not reach Silver, Bronze "
        f"or raw — the marts resolve the as-of-race-date joins that make an "
        f"answer correct."
    )


@pytest.mark.parametrize("identifier", TABLES)
def test_agent_excludes_tables_this_project_does_not_own(identifier):
    """`f1.gold` also holds two tables from a separate agent project."""
    assert not identifier.split(".")[-1].startswith("agent_"), (
        f"{identifier} belongs to the separate agent project, not this platform"
    )


def test_metric_view_measures_are_wrapped():
    """`MEASURE()` is mandatory on a metric view, and `SELECT *` is unsupported.

    A certified query that reads a metric view without MEASURE() teaches the
    model a pattern that always fails.
    """
    for entry in SQLS:
        sql = "".join(entry["sql"])
        if "driver_metrics" not in sql:
            continue
        assert "MEASURE(" in sql, f"certified SQL {entry['id']} reads the metric view without MEASURE()"
        assert "SELECT *" not in sql.upper(), f"certified SQL {entry['id']} uses SELECT * on a metric view"


def test_certified_sql_stays_inside_gold():
    for entry in SQLS:
        sql = "".join(entry["sql"])
        for match in re.findall(r"\bFROM\s+([\w.]+)|\bJOIN\s+([\w.]+)", sql):
            table = match[0] or match[1]
            if "." not in table:          # a CTE alias, not a table
                continue
            assert table.startswith("f1.gold."), (
                f"certified SQL {entry['id']} reads {table}, outside Gold"
            )


def test_every_mart_is_reachable():
    """A mart nobody can query through the agent is a mart the agent cannot use."""
    expected = {
        "f1.gold.championship_progression",
        "f1.gold.constructor_standings",
        "f1.gold.driver_metrics",
        "f1.gold.driver_performance",
        "f1.gold.lap_pace",
        "f1.gold.race_conditions",
        "f1.gold.race_strategy",
    }
    assert set(TABLES) == expected


# ─────────────────────────────── shape ───────────────────────────────

def test_ids_are_hex32_and_globally_unique():
    ids = [item["id"] for item in (*SAMPLES, *SQLS, *INSTRUCTIONS)]
    for value in ids:
        assert HEX32.match(value), f"{value!r} is not 32 lowercase hex characters"
    assert len(ids) == len(set(ids)), "ids must be unique across all three lists"


def test_text_fields_are_arrays():
    """`"question": "text"` is rejected; it must be `["text"]`."""
    for item in SAMPLES:
        assert isinstance(item["question"], list)
    for item in SQLS:
        assert isinstance(item["question"], list) and isinstance(item["sql"], list)
    for item in INSTRUCTIONS:
        assert isinstance(item["content"], list)


def test_at_most_one_text_instruction():
    """The API rejects more than one; all guidance merges into a single entry."""
    assert len(INSTRUCTIONS) == 1


def test_sort_order_the_api_requires():
    assert TABLES == sorted(TABLES), "data_sources.tables must be sorted by identifier"
    for name, items in (("example_question_sqls", SQLS), ("text_instructions", INSTRUCTIONS)):
        ids = [i["id"] for i in items]
        assert ids == sorted(ids), f"{name} must be sorted by id"


# ──────────────────────────── domain rules ────────────────────────────

def test_instructions_carry_the_rules_that_change_answers():
    """The traps found building this must survive an edit to the instructions."""
    content = "".join(INSTRUCTIONS[0]["content"])
    for rule, why in [
        ("total_points", "race points alone leave 13 of 24 drivers short of their 2024 total"),
        ("constructor_name_as_of_race", "a current-team join reattributes historical results"),
        ("weather_available", "missing weather is not a dry race"),
        ("median_clean_lap_s", "pace is ranked on clean laps, not finishing position"),
    ]:
        assert rule in content, f"instructions no longer mention {rule} — {why}"


def test_agent_has_certified_examples_for_each_theme():
    """Certified SQL is what makes answers repeatable rather than improvised."""
    assert len(SQLS) >= 5
    joined = " ".join("".join(e["sql"]) for e in SQLS).lower()
    for mart in ("driver_performance", "lap_pace", "race_conditions", "race_strategy"):
        assert mart in joined, f"no certified query demonstrates {mart}"
