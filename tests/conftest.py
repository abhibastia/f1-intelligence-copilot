"""Shared fixtures for the merged repository.

Tests are split by what they need:

  - Pure-logic tests import only the modules under test and run anywhere.
  - `src/ingestion` tests import bare-name modules the way a Databricks
    `spark_python_task` runs them, so `src/ingestion` is put on sys.path rather
    than restructured into a package to suit the tests.
  - Tests touching Lakebase are marked `integration` and skip cleanly when no
    credentials are configured, so `pytest tests/` is always runnable.

That split matters. Every Lakebase bug this suite covers was found by running
the system, not by unit-testing it — so those tests exist to stop regressions,
and they must not require the whole platform to be up in order to do that.
"""

import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Order matters, and getting it wrong is not hypothetical. mcp_server/ contains
# a GENERATED copy of f1lake (build_app.sh puts it there so a Databricks App can
# import it), and that copy carries only the runtime modules - no load.py. Put
# mcp_server first on sys.path and `import f1lake.load` resolves to the copy and
# fails. The repo root must win; mcp_server is appended only so f1_broker is
# importable.
sys.path.insert(0, ROOT)
sys.path.append(os.path.join(ROOT, "mcp_server"))

# src/ingestion is not a package - its modules import each other by bare name
# because a Databricks spark_python_task runs the entry point with its own
# directory on sys.path. Tests reproduce that rather than restructuring the
# source to suit them.
sys.path.insert(0, os.path.join(ROOT, "src", "ingestion"))


def pytest_configure(config):
    config.addinivalue_line("markers", "integration: needs a live Lakebase connection")


@pytest.fixture(scope="session")
def lakebase():
    """Yield the schema module, or skip if Lakebase is not reachable."""
    try:
        from f1lake import schema

        schema.query("SELECT 1")
        return schema
    except Exception as exc:
        pytest.skip(f"Lakebase unavailable: {str(exc)[:80]}")


@pytest.fixture
def marker(lakebase):
    """A unique marker for a write test, removed when the test finishes.

    The write tests exercise the real Lakebase, because the bug they exist to
    catch - an INSERT ... RETURNING that hands back a row and then rolls back on
    connection close - only reproduces against a real connection. Free Edition
    has no second database to point them at.

    What that costs is that every run left rows behind. They accumulated in the
    same watchlist the app renders under "What the assistant has done", until
    two dozen `test-*` and `idem-*` entries buried the three real ones - in the
    section whose entire job is showing what the agent actually did.

    Deleting by exact marker keeps the cleanup surgical: a test can only ever
    remove rows it created itself, so a crashed run cannot take real data with
    it.
    """
    import uuid as _uuid

    created: list[str] = []

    def _make(prefix: str) -> str:
        value = f"{prefix}-{_uuid.uuid4().hex[:8]}"
        created.append(value)
        return value

    yield _make

    for value in created:
        lakebase.execute("DELETE FROM f1_watchlist WHERE entity_ref = %s", (value,))
        lakebase.execute("DELETE FROM f1_race_notes WHERE note = %s", (value,))
