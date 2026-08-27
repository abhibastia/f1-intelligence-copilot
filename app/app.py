"""
F1 Intelligence Copilot — frontend.

A Databricks App that presents the project's central finding and lets a user
explore the corpus the agent reasons over: race results from the Spark pipeline,
measured race-day weather, and the narrative of race reports.

MOSTLY READ-ONLY, DELIBERATELY
-------------------------------
Everything the assistant can *create* here - watchlist entries, predictions,
notes - goes through the agent's MCP tools, never a form on this page: that's
what makes it demonstrable that the agent, specifically, took the action.
Deleting one of those rows is the one direct write this page makes on its own
(/api/*/delete below) - a deterministic "remove row N" with no upside to
routing through an LLM, calling the exact same f1_broker functions the
matching agent tools call. One implementation, reachable from chat or from a
button in section 06.

Serves entirely from Lakebase, so rendering a page costs no Databricks compute.

Run locally:
    python app.py
"""

import logging
import os

from flask import Flask, jsonify, redirect, render_template, request, url_for

import agent
import f1_broker
import ui_data
from f1lake import schema

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("f1-intelligence-copilot-ui")

app = Flask(__name__)

MCP_URL = os.environ.get(
    "MCP_SERVER_URL",
    "https://f1-intelligence-mcp-<workspace-id>.aws.databricksapps.com",
)

# The published AI/BI dashboard (dashboards/f1_race_intelligence.lvdash.json).
# Set by app.yaml after `databricks bundle deploy` publishes it - find yours
# with `databricks bundle summary` or `databricks lakeview list`.
DASHBOARD_URL = os.environ.get("DASHBOARD_URL", "")


@app.route("/healthz")
def healthz():
    """Liveness probe. Deliberately does not touch Lakebase, so a database
    problem cannot make the platform conclude the container is dead."""
    return jsonify({"status": "ok", "app": "f1-intelligence-copilot-ui"})


@app.route("/")
def index():
    season = request.args.get("season", type=int)
    try:
        available = ui_data.seasons()
        if season not in available:
            season = available[0] if available else None
        return render_template(
            "index.html",
            stats=ui_data.corpus_stats(),
            thresholds=ui_data.rain_vs_chaos(),
            thesis=ui_data.thesis_races(),
            seasons=available,
            season=season,
            races=ui_data.season_races(season) if season else [],
            standings=ui_data.standings(season) if season else [],
            activity=ui_data.agent_activity(),
            strategy=ui_data.strategy_races(season) if season else [],
            mcp_url=MCP_URL,
            dashboard_url=DASHBOARD_URL,
            error=None,
        )
    except Exception as exc:
        # Render an explanation rather than a 500. The most likely cause is a
        # missing secret ACL on this app's service principal, and a stack trace
        # in the browser would not say so.
        logger.exception("Could not render the dashboard")
        return render_template(
            "index.html", stats={}, thresholds=[], thesis=[], seasons=[],
            season=None, races=[], standings=[], activity={}, strategy=[],
            mcp_url=MCP_URL, dashboard_url=DASHBOARD_URL,
            error=schema.safe_message(exc),
        )


# --------------------------------------------------------------------------
# Deletes — the one direct write this page makes; see the module docstring.
# --------------------------------------------------------------------------

@app.route("/api/watchlist/<int:item_id>/delete", methods=["POST"])
def delete_watchlist_item(item_id):
    try:
        f1_broker.remove_from_watchlist(item_id)
    except Exception:
        logger.exception("Could not remove watchlist item %s", item_id)
    return redirect(url_for("index", _anchor="activity"))


@app.route("/api/predictions/<int:item_id>/delete", methods=["POST"])
def delete_prediction_item(item_id):
    try:
        f1_broker.delete_prediction(item_id)
    except Exception:
        logger.exception("Could not delete prediction %s", item_id)
    return redirect(url_for("index", _anchor="activity"))


@app.route("/api/notes/<int:item_id>/delete", methods=["POST"])
def delete_note_item(item_id):
    try:
        f1_broker.delete_note(item_id)
    except Exception:
        logger.exception("Could not delete note %s", item_id)
    return redirect(url_for("index", _anchor="activity"))


@app.route("/api/chat", methods=["POST"])
def api_chat():
    """Ask the assistant a question.

    Returns the answer AND the full tool-call trace. A chat endpoint that
    returned only prose would give a reader no way to tell whether the agent
    used its tools or invented the answer - which is the whole thing being
    demonstrated. The trace is rendered inline in the UI for the same reason.

    Conversation state stays on the client. The agent is stateless per request,
    so nothing here depends on sticky sessions or a server-side store, and a
    restarted container loses no conversation the browser still holds.
    """
    body = request.get_json(silent=True) or {}
    question = (body.get("question") or "").strip()
    if not question:
        return jsonify({"error": "Ask a question first."}), 400

    history = body.get("history") or []
    # Cap the history sent upstream. A long thread would push the tool results
    # the model actually needs out of the context window.
    history = [m for m in history if m.get("role") in ("user", "assistant")][-6:]

    try:
        result = agent.ask(question, history=history)
        return jsonify(result)
    except Exception as exc:
        logger.exception("Agent call failed")
        return jsonify({
            "error": "The assistant could not answer that.",
            "detail": schema.safe_message(exc),
        }), 503


@app.route("/api/search")
def api_search():
    """Semantic search over race reports. Backs the search box."""
    query = (request.args.get("q") or "").strip()
    if not query:
        return jsonify({"error": "Enter something to search for."}), 400
    season = request.args.get("season", type=int)
    try:
        results = ui_data.search(query, top_k=request.args.get("k", 6, type=int),
                                 season=season)
        return jsonify({"query": query, "count": len(results), "results": results})
    except Exception as exc:
        logger.exception("Search failed")
        return jsonify({"error": schema.safe_message(exc)}), 503


if __name__ == "__main__":
    port = int(os.getenv("DATABRICKS_APP_PORT", os.getenv("PORT", 8001)))
    # debug defaults off: app.yaml runs this same entrypoint in the deployed
    # app, and Flask's debug mode exposes the Werkzeug console to anyone who can
    # trigger a 500.
    debug = os.getenv("FLASK_DEBUG", "").lower() in {"1", "true", "yes"}
    app.run(debug=debug, host="0.0.0.0", port=port)
