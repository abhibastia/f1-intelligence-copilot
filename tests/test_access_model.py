"""The Unity Catalog access model, asserted rather than described.

`scripts/apply_grants.py` is what makes the README's "governed by Unity
Catalog" a fact instead of a claim. These tests pin the shape of that model so
a later edit cannot quietly widen it — the failure mode being a consumer tier
that gains Bronze without anyone noticing, which is exactly the kind of change
that looks harmless in a diff.

No workspace, no credentials: the model is a pure function of its arguments.
"""

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

_spec = importlib.util.spec_from_file_location(
    "apply_grants", ROOT / "scripts" / "apply_grants.py"
)
apply_grants = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(apply_grants)

CONSUMERS = apply_grants.CONSUMERS


def model(engineers=None):
    return {
        (securable, name): grants
        for securable, name, grants in apply_grants.access_model("f1", engineers)
    }


@pytest.mark.parametrize("engineers", [None, "data-engineers"])
def test_consumers_read_gold_and_nothing_below_it(engineers):
    """Checked in BOTH shapes of the model.

    The first version of this test only inspected the no-engineer model, where
    the lower layers are absent entirely — so granting consumers SELECT on
    Silver in the engineer path passed clean. Caught by deliberately widening
    the tier and finding the test still green.
    """
    m = model(engineers)
    assert "SELECT" in m[("schema", "f1.gold")][CONSUMERS]

    for (securable, name), grants in m.items():
        # The catalog (traversal) and anything inside the Gold schema — the
        # marts and the metric view over them — are the consumer tier by
        # design. Everything else must be absent for them.
        if name == "f1" or name == "f1.gold" or name.startswith("f1.gold."):
            continue
        assert CONSUMERS not in grants, (
            f"consumers were granted {grants.get(CONSUMERS)} on {securable} "
            f"{name}; the marts are the only layer they should read"
        )


@pytest.mark.parametrize("engineers", [None, "data-engineers"])
def test_consumers_cannot_read_the_landing_volume(engineers):
    volume = model(engineers).get(("volume", "f1.raw.landing"), {})
    assert CONSUMERS not in volume


@pytest.mark.parametrize("engineers", [None, "data-engineers"])
def test_consumers_reach_the_metric_view(engineers):
    """The semantic layer is only useful if the consumer tier can query it."""
    metric_view = model(engineers).get(("table", "f1.gold.driver_metrics"), {})
    assert "SELECT" in metric_view.get(CONSUMERS, []), (
        "consumers cannot read the metric view, so the governed definitions "
        "are unreachable by the tier they exist for"
    )


def test_catalog_grant_is_traversal_not_reading():
    """USE_CATALOG alone reads nothing — SELECT lives on the schema."""
    assert model()[("catalog", "f1")][CONSUMERS] == ["USE_CATALOG"]


def test_engineers_reach_the_lower_layers():
    m = model("data-engineers")
    for layer in ("silver", "bronze"):
        assert "SELECT" in m[("schema", f"f1.{layer}")]["data-engineers"]
    assert m[("volume", "f1.raw.landing")]["data-engineers"] == ["READ_VOLUME"]


def test_nobody_is_granted_write_on_the_landing_volume():
    """One writer only: the ingestion job.

    A second writer breaks the idempotency contract that lets Silver
    deduplicate on the newest _ingest_ts.
    """
    for grants in model("data-engineers").values():
        for privileges in grants.values():
            assert "WRITE_VOLUME" not in privileges


@pytest.mark.parametrize("engineers", [None, "data-engineers"])
def test_privileges_use_the_stored_spelling(engineers):
    """Unity Catalog returns USE_CATALOG, never "USE CATALOG".

    The API accepts both, so the spaced form applies cleanly and then never
    matches on read-back: every run reports a change and re-applies for ever.
    """
    for grants in model(engineers).values():
        for privileges in grants.values():
            for privilege in privileges:
                assert " " not in privilege, f"{privilege!r} must be underscored"


def test_no_workspace_local_group_is_hardcoded():
    """UC rejects workspace-local groups: "Could not find principal admins"."""
    source = (ROOT / "scripts" / "apply_grants.py").read_text()
    for group in ('"admins"', '"users"'):
        assert group not in source, (
            f"{group} is a workspace-local group and Unity Catalog cannot resolve it"
        )
