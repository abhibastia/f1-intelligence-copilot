"""Agent wiring and the write path.

The most valuable test here is the round trip: a write that reports success but
never commits looks identical to one that worked, from the caller's side. Only
reading back afterwards distinguishes them - and that bug shipped once already.
"""

import pytest

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def broker(lakebase):
    import f1_broker

    return f1_broker


@pytest.fixture(scope="module")
def agent():
    """The app's agent module. Imported here rather than at file scope because
    app/ is only put on the path by conftest, and importing it eagerly would
    make collection fail before any fixture has run."""
    import os
    import sys

    sys.path.insert(
        0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "app")
    )
    import agent as agent_module

    return agent_module


class TestToolWiring:
    def test_every_advertised_tool_is_callable(self, agent):
        """A tool in the schema list with no dispatch entry is advertised to the
        model and then fails at call time."""
        advertised = {name for name, _, _, _ in agent.TOOLS}
        assert advertised == set(agent.DISPATCH), advertised.symmetric_difference(
            set(agent.DISPATCH)
        )

    def test_write_tools_are_declared(self):
        import agent

        assert agent.WRITE_TOOLS <= set(agent.DISPATCH)
        assert len(agent.WRITE_TOOLS) == 6

    def test_mcp_server_exposes_the_same_tools(self):
        import asyncio

        import agent
        from f1_mcp_server import mcp

        names = {t.name for t in asyncio.run(mcp.list_tools())}
        assert {n for n, _, _, _ in agent.TOOLS} <= names


class TestWritesPersist:
    """Reading back is the whole point.

    INSERT ... RETURNING through a helper that never commits returns a real row
    with a real id, so the write looks entirely successful and then rolls back
    on connection close. The agent said "saved" and the database disagreed.
    """

    def test_watchlist_write_survives_the_connection(self, broker, marker):
        ref = marker("test")
        written = broker.add_watchlist("constructor", ref, note="round-trip test")
        assert written["written"] is True

        found = [i for i in broker.get_watchlist()["items"] if i["entity_ref"] == ref]
        assert found, "write reported success but the row is not readable"

    def test_note_write_survives_the_connection(self, broker, marker):
        ref = marker("note")
        written = broker.save_note(2024, "Sao Paulo", ref)
        assert written["written"] is True
        assert written["row"]["race_name"]  # resolution reported back

        assert any(n["note"] == ref for n in broker.get_notes(2024)["notes"])


class TestDeletesPersist:
    """The delete tools are the UI's one direct write (see app.py) as well as
    agent tools, so the same round-trip risk applies in reverse: a DELETE that
    reports success but never commits would leave the row readable, and a
    caller that trusts the return value would tell a user something is gone
    when it is not."""

    def test_watchlist_removal_survives_the_connection(self, broker, marker):
        ref = marker("test")
        broker.add_watchlist("constructor", ref)
        item_id = next(i["id"] for i in broker.get_watchlist()["items"] if i["entity_ref"] == ref)

        removed = broker.remove_from_watchlist(item_id)
        assert removed["written"] is True

        assert not any(i["entity_ref"] == ref for i in broker.get_watchlist()["items"])

    def test_note_deletion_survives_the_connection(self, broker, marker):
        ref = marker("note")
        written = broker.save_note(2024, "Sao Paulo", ref)
        item_id = written["row"]["id"]

        deleted = broker.delete_note(item_id)
        assert deleted["written"] is True

        assert not any(n["note"] == ref for n in broker.get_notes(2024)["notes"])

    def test_deleting_an_unowned_or_missing_id_raises(self, broker):
        with pytest.raises(ValueError):
            broker.delete_note(2**31 - 1)

    def test_prediction_requires_valid_confidence(self, broker):
        with pytest.raises(ValueError):
            broker.log_prediction(2024, 1, "something", confidence="certain")

    def test_write_to_unknown_race_is_refused(self, broker):
        with pytest.raises(broker.UnknownRaceError):
            broker.save_note(2024, "Atlantis", "should never be stored")

    def test_watchlist_add_is_idempotent(self, broker, marker):
        ref = marker("idem")
        broker.add_watchlist("circuit", ref)
        broker.add_watchlist("circuit", ref)
        matches = [i for i in broker.get_watchlist()["items"] if i["entity_ref"] == ref]
        assert len(matches) == 1, "adding twice created a duplicate"


class TestTelemetry:
    def test_tool_call_logging_never_raises(self, lakebase):
        """Telemetry must not break the thing it observes. By the time this
        runs the tool has already succeeded."""
        lakebase.log_tool_call(
            tool_name="test_tool",
            arguments={"k": "v"},
            outcome="ok",
            is_write=False,
            summary="unit test",
            duration_ms=1,
        )

    def test_logging_failure_is_swallowed(self, lakebase, monkeypatch):
        def explode(*a, **k):
            raise RuntimeError("database on fire")

        monkeypatch.setattr(lakebase, "connection", explode)
        lakebase.log_tool_call("t", {}, "ok")  # must not raise


class TestTraceEvidence:
    """The trace is the project's evidence that an answer came from data. A tool
    name alone does not carry that - it reads identically whether the tool
    answered or returned an empty row."""

    def test_weather_evidence_names_the_source_not_the_conclusion(self, agent):
        """was_wet is derived from the daily rainfall total alone. Rendering it
        as "wet race" put that phrase directly above answers that correctly say
        the track was dry, which reads as self-contradiction rather than as the
        report overruling the rainfall."""
        ev = agent._evidence(
            "get_race_weather",
            {
                "resolved_race": "Italian Grand Prix",
                "precipitation_mm": 19.1,
                "conditions": "heavy rain",
                "was_wet": True,
            },
        )
        assert "19.1 mm" in ev
        assert "flagged wet by rainfall" in ev
        assert "wet race" not in ev

    def test_evidence_reports_retrieval_depth(self, agent):
        ev = agent._evidence(
            "search_race_reports", {"results": [{"similarity": 0.61}, {"similarity": 0.4}]}
        )
        assert "2 passages" in ev and "0.61" in ev

    def test_error_evidence_surfaces_the_message(self, agent):
        ev = agent._evidence(
            "get_race_strategy", {"error": "not_found", "message": "No race matched 'Foo'"}
        )
        assert "No race matched" in ev


class TestFollowups:
    """Follow-ups are read off the trace rather than generated, so a suggestion
    can only ever be a question this corpus can answer."""

    def test_followups_use_the_resolved_race(self, agent):
        out = agent._followups(
            [
                {
                    "tool": "get_race_strategy",
                    "arguments": {"season": 2024, "race": "Sao Paulo"},
                    "result": {"resolved_race": "São Paulo Grand Prix"},
                    "is_write": False,
                }
            ]
        )
        assert any("São Paulo Grand Prix" in q for q in out)
        assert len(out) == 3

    def test_no_followup_repeats_a_tool_already_called(self, agent):
        """Suggesting the weather after the weather was just fetched wastes the
        one place the user is looking for what to do next."""
        trace = [
            {
                "tool": "get_race_weather",
                "arguments": {"season": 2024, "race": "Monza"},
                "result": {"resolved_race": "Italian Grand Prix"},
                "is_write": False,
            }
        ]
        assert not any(
            q.startswith("Was the 2024 Italian Grand Prix actually a wet")
            for q in agent._followups(trace)
        )

    def test_write_evidence_carries_the_stored_row_id(self, agent):
        """A write that never commits looks identical to one that worked. The
        id comes back from the database, so showing it in the trace is what
        distinguishes "a call was made" from "a row exists"."""
        ev = agent._evidence(
            "save_race_note",
            {
                "written": True,
                "action": "save_race_note",
                "row": {"id": 18, "season": 2024, "round": 16},
            },
        )
        assert "id 18" in ev

    def test_a_write_offers_to_show_what_was_saved(self, agent):
        out = agent._followups(
            [
                {
                    "tool": "add_to_watchlist",
                    "arguments": {"entity_ref": "Norris"},
                    "result": {"written": True, "id": 7},
                    "is_write": True,
                }
            ]
        )
        assert out[0] == "Show me everything you have saved"

    def test_empty_trace_still_offers_something(self, agent):
        assert len(agent._followups([])) == 3


class TestWroteFlag:
    """`wrote` drives the "Saved - view it" link in the UI. A write tool that
    was called but failed must not set it, or the page sends a user to look for
    a row that does not exist - the exact failure this project is about."""

    def test_failed_write_does_not_report_success(self, broker, agent, monkeypatch):
        def boom(*_a, **_k):
            raise broker.UnknownRaceError("No race matching 'Atlantis' in 2024")

        monkeypatch.setitem(agent.DISPATCH, "save_race_note", boom)
        result = agent.ask("Save a note about the 2024 Atlantis Grand Prix: test.")
        assert result["wrote"] is False, "a failed write reported success"

    def test_successful_write_reports_success(self, broker, agent, marker):
        ref = marker("note")
        result = agent.ask(f"Save a note about the 2024 Sao Paulo Grand Prix: {ref}")
        assert result["wrote"] is True
        assert any(n["note"] == ref for n in broker.get_notes(2024)["notes"])
