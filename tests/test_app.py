"""The Flask frontend — routes, error handling, and the one direct write it
makes (see app.py's module docstring: deletes are the exception to "read-only").

Split the same way as the rest of the suite: validation and error-path
behaviour is tested with the real routes but a monkeypatched broker/agent/
ui_data call, so a laptop with no Databricks connectivity still catches these
regressions. What actually reaches Lakebase is `integration`-marked and skips
cleanly without credentials.
"""
import os
import sys

import pytest

APP_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "app")


@pytest.fixture(scope="module")
def app_module():
    """The Flask app module. Imported here, not at file scope, so app/ only
    goes on sys.path when these tests actually run — same reasoning as
    test_agent.py's `agent` fixture."""
    sys.path.insert(0, APP_DIR)
    import app as app_mod
    return app_mod


@pytest.fixture
def client(app_module):
    app_module.app.testing = True
    return app_module.app.test_client()


class TestHealthz:
    def test_does_not_touch_lakebase(self, client, app_module, monkeypatch):
        """The liveness probe must stay up even when the database is down, or
        a Lakebase outage gets misread as the container being dead — the exact
        reason the route exists."""
        def boom(*_a, **_k):
            raise AssertionError("healthz must never query Lakebase")
        monkeypatch.setattr(app_module.schema, "query", boom)

        resp = client.get("/healthz")
        assert resp.status_code == 200
        assert resp.get_json() == {"status": "ok", "app": "f1-intelligence-copilot-ui"}


class TestIndexErrorPath:
    """A Lakebase outage must render an explanation, not a stack trace — the
    module docstring's whole reason for the try/except around render_template.
    Exercised with a monkeypatched ui_data, so this needs no live database."""

    def test_a_lakebase_failure_renders_the_error_panel_not_a_500(
            self, client, app_module, monkeypatch):
        def boom():
            raise RuntimeError('could not translate host name "ep-xyz.cloud.databricks.com"')
        monkeypatch.setattr(app_module.ui_data, "seasons", boom)

        resp = client.get("/")
        assert resp.status_code == 200
        body = resp.get_data(as_text=True)
        assert "Could not load data from Lakebase" in body
        assert "ep-xyz" not in body, "the connection target must not reach the browser"

    def test_a_healthy_render_never_shows_the_error_panel(
            self, client, app_module, monkeypatch):
        monkeypatch.setattr(app_module.ui_data, "seasons", lambda: [2024])
        monkeypatch.setattr(app_module.ui_data, "corpus_stats", lambda: {})
        # rain_vs_chaos() always returns one row per fixed threshold in real
        # use (never empty) - the template indexes thresholds[0]/[-1] on that
        # assumption, so the stub has to honor it too.
        monkeypatch.setattr(app_module.ui_data, "rain_vs_chaos", lambda: [
            {"threshold": 0.0, "races": 10, "dnf_pct": 12.5, "pos_change": 3.2},
            {"threshold": 15.0, "races": 2, "dnf_pct": 15.7, "pos_change": 4.1},
        ])
        monkeypatch.setattr(app_module.ui_data, "thesis_races", lambda: [])
        monkeypatch.setattr(app_module.ui_data, "season_races", lambda season: [])
        monkeypatch.setattr(app_module.ui_data, "standings", lambda season: [])
        monkeypatch.setattr(app_module.ui_data, "agent_activity",
                             lambda: {"watchlist": [], "predictions": [], "notes": []})
        monkeypatch.setattr(app_module.ui_data, "strategy_races", lambda season: [])

        resp = client.get("/")
        assert resp.status_code == 200
        assert "Could not load data from Lakebase" not in resp.get_data(as_text=True)


class TestChat:
    def test_empty_question_is_rejected_before_calling_the_agent(
            self, client, app_module, monkeypatch):
        def boom(*_a, **_k):
            raise AssertionError("agent.ask must not run for an empty question")
        monkeypatch.setattr(app_module.agent, "ask", boom)

        resp = client.post("/api/chat", json={"question": "   "})
        assert resp.status_code == 400
        assert "question" in resp.get_json()["error"].lower()

    def test_agent_failure_returns_a_safe_message_not_a_500(
            self, client, app_module, monkeypatch):
        def boom(question, history=None):
            raise RuntimeError('could not translate host name "ep-abc123.cloud.databricks.com"')
        monkeypatch.setattr(app_module.agent, "ask", boom)

        resp = client.post("/api/chat", json={"question": "Who won the 2024 title?"})
        assert resp.status_code == 503
        assert "ep-abc123" not in resp.get_json()["detail"]

    def test_history_is_capped_and_filtered_before_reaching_the_agent(
            self, client, app_module, monkeypatch):
        """A long thread would push the tool results the model actually needs
        out of the context window — see api_chat's docstring."""
        captured = {}
        def fake_ask(question, history=None):
            captured["history"] = history
            return {"answer": "ok", "trace": [], "wrote": False}
        monkeypatch.setattr(app_module.agent, "ask", fake_ask)

        history = [{"role": "user", "content": f"q{i}"} for i in range(10)]
        history.append({"role": "system", "content": "should be dropped"})
        resp = client.post("/api/chat", json={"question": "next?", "history": history})

        assert resp.status_code == 200
        assert len(captured["history"]) == 6
        assert all(m["role"] in ("user", "assistant") for m in captured["history"])


class TestSearch:
    def test_empty_query_is_rejected(self, client):
        resp = client.get("/api/search?q=")
        assert resp.status_code == 400

    def test_search_failure_returns_a_safe_message_not_a_500(
            self, client, app_module, monkeypatch):
        def boom(*_a, **_k):
            raise RuntimeError('password authentication failed for user "svc_f1"')
        monkeypatch.setattr(app_module.ui_data, "search", boom)

        resp = client.get("/api/search?q=rain")
        assert resp.status_code == 503
        assert "svc_f1" not in resp.get_json()["error"]

    def test_search_result_shape(self, client, app_module, monkeypatch):
        monkeypatch.setattr(
            app_module.ui_data, "search",
            lambda query, top_k=6, season=None: [{"race_name": "Italian Grand Prix"}])

        resp = client.get("/api/search?q=rain&k=3")
        assert resp.get_json() == {
            "query": "rain", "count": 1,
            "results": [{"race_name": "Italian Grand Prix"}],
        }


class TestDeleteRoutesNeverBreakThePage:
    """A delete is fired from a form on the page the user is already looking
    at. If f1_broker raises — the row was already gone, or Lakebase hiccuped —
    the route must still redirect back rather than 500, or one click turns a
    working page into a broken one."""

    @pytest.mark.parametrize("path,fn_name", [
        ("/api/watchlist/1/delete", "remove_from_watchlist"),
        ("/api/predictions/1/delete", "delete_prediction"),
        ("/api/notes/1/delete", "delete_note"),
    ])
    def test_a_failed_delete_still_redirects_to_the_activity_section(
            self, client, app_module, monkeypatch, path, fn_name):
        def boom(item_id):
            raise ValueError(f"No such row {item_id}")
        monkeypatch.setattr(app_module.f1_broker, fn_name, boom)

        resp = client.post(path)
        assert resp.status_code == 302
        assert resp.headers["Location"].endswith("/#activity")

    @pytest.mark.parametrize("path,fn_name", [
        ("/api/watchlist/1/delete", "remove_from_watchlist"),
        ("/api/predictions/1/delete", "delete_prediction"),
        ("/api/notes/1/delete", "delete_note"),
    ])
    def test_a_successful_delete_calls_the_matching_broker_function_with_the_id(
            self, client, app_module, monkeypatch, path, fn_name):
        calls = []
        monkeypatch.setattr(
            app_module.f1_broker, fn_name,
            lambda item_id: calls.append(item_id) or {"written": True})

        resp = client.post(path)
        assert resp.status_code == 302
        assert calls == [1]


@pytest.mark.integration
class TestIndexAgainstLiveLakebase:
    def test_renders_for_a_real_season_with_no_error(self, client, lakebase):
        resp = client.get("/")
        assert resp.status_code == 200
        body = resp.get_data(as_text=True)
        assert "F1 Intelligence Copilot" in body
        assert "Could not load data from Lakebase" not in body


@pytest.mark.integration
class TestDeleteRoutesAgainstLiveLakebase:
    """Exercises the actual HTTP route, not just the broker function directly
    (that round trip is already covered by test_agent.py's TestDeletesPersist)
    — proof that the id parsed from the URL reaches the right row in the right
    table through app.py's own wiring."""

    def test_deleting_a_watchlist_item_through_the_route_removes_it(
            self, client, app_module, marker):
        ref = marker("test")
        app_module.f1_broker.add_watchlist("constructor", ref)
        item_id = next(i["id"] for i in app_module.f1_broker.get_watchlist()["items"]
                       if i["entity_ref"] == ref)

        resp = client.post(f"/api/watchlist/{item_id}/delete")
        assert resp.status_code == 302
        assert not any(i["entity_ref"] == ref
                       for i in app_module.f1_broker.get_watchlist()["items"])

    def test_deleting_a_note_through_the_route_removes_it(
            self, client, app_module, marker):
        ref = marker("note")
        written = app_module.f1_broker.save_note(2024, "Sao Paulo", ref)
        item_id = written["row"]["id"]

        resp = client.post(f"/api/notes/{item_id}/delete")
        assert resp.status_code == 302
        assert not any(n["note"] == ref
                       for n in app_module.f1_broker.get_notes(2024)["notes"])

    def test_deleting_a_prediction_through_the_route_removes_it(
            self, client, app_module, lakebase):
        written = app_module.f1_broker.log_prediction(
            2024, 1, "route-delete-test", confidence="low")
        item_id = written["row"]["id"]

        resp = client.post(f"/api/predictions/{item_id}/delete")
        assert resp.status_code == 302
        remaining = app_module.f1_broker.get_predictions(2024)["predictions"]
        assert not any(p["id"] == item_id for p in remaining)
