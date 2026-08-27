"""
F1 Intelligence Copilot — MCP server.

Exposes F1 tools over MCP so a Databricks agent can call them:

  READ                              WRITE
  - get_driver_season               - add_to_watchlist
  - compare_constructors            - log_prediction
  - get_championship_standings      - save_race_note
  - get_constructor_standings       - remove_from_watchlist
  - get_race_weather                - delete_prediction
  - find_wet_races                  - delete_note
  - search_race_reports
  - get_race_strategy · find_strategy_races
  - get_race_pace
  - get_watchlist / get_predictions / get_race_notes

Every tool is thin: validate, call one `f1_broker` function, shape the result.
All SQL and all embedding live in the broker, so the F1 logic is testable with a
plain Python call - no agent, no MCP client, no deployed app.

THREE DATA SOURCES, ONE KEY
---------------------------
Jolpica-F1 (results, standings, via the Spark medallion pipeline), Wikipedia
(race-report prose, embedded for semantic search), and Open-Meteo's archive
(measured race-day weather). All three carry (season, round), so a semantic hit
in a race report pivots straight into that race's results and its rainfall.

Deploy as a Databricks App. Run locally:
    python f1_mcp_server.py
"""

import html
import logging
import os

from fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse

import f1_broker
from f1_broker import UnknownDriverError, UnknownRaceError
from f1lake import schema

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("f1-mcp-server")

mcp = FastMCP("f1-intelligence-copilot")


def _error(exc: Exception, context: str) -> dict:
    """Turn an exception into something the agent can act on.

    Returning a structured error rather than raising keeps a bad driver name
    from surfacing as a stack trace, and gives the model a specific remedy - ask
    which driver, rather than invent a result.
    """
    if isinstance(exc, UnknownDriverError):
        return {"error": "unknown_driver", "message": str(exc), "context": context,
                "suggestion": "Ask the user which driver they mean. Do not guess."}
    if isinstance(exc, UnknownRaceError):
        return {"error": "unknown_race", "message": str(exc), "context": context,
                "suggestion": "Ask the user to name the race or give a round number."}
    if isinstance(exc, ValueError):
        return {"error": "invalid_arguments", "message": str(exc), "context": context,
                "suggestion": "Correct the arguments and try once more."}
    # str(exc) here is whatever the driver raised. psycopg2 names the host and
    # role it failed to reach, and this message is repeated by the agent and
    # rendered in the UI trace - so the detail goes to the log, not the caller.
    logger.exception("Unexpected failure: %s", context)
    return {"error": "unexpected_error", "message": schema.safe_message(exc),
            "context": context,
            "suggestion": "Tell the user the request failed. Do not invent data."}


# --------------------------------------------------------------------------
# READ tools
# --------------------------------------------------------------------------

@mcp.tool
def get_driver_season(driver: str, season: int) -> dict:
    """
    Get a driver's race-by-race results and season totals.

    Args:
        driver: Driver name, surname or three-letter code — "Verstappen",
            "Max Verstappen", "VER", or the id "max_verstappen".
        season: Championship year, e.g. 2024.

    Returns:
        A dict with resolved_driver, total_points, wins, podiums, dnfs, the
        constructors driven for, and a per-race `results` list. On failure a
        dict with an "error" key. An ambiguous driver name returns
        "unknown_driver" listing the candidates rather than picking one.
    """
    try:
        return f1_broker.driver_season(driver, season)
    except Exception as exc:
        return _error(exc, f"get_driver_season({driver!r}, {season})")


@mcp.tool
def compare_constructors(constructor_a: str, constructor_b: str, season: int) -> dict:
    """
    Compare two constructors' form across a season.

    Aggregates every car entered by each team: points, wins, podiums, DNFs and
    average finishing position, then reports which is ahead and by how much —
    so the agent states a comparison rather than doing arithmetic on raw rows.

    Args:
        constructor_a: Team name, e.g. "Ferrari".
        constructor_b: Team name, e.g. "McLaren".
        season: Championship year.

    Returns:
        A dict with per-team figures under "compared", plus ahead_on_points and
        points_gap. On failure a dict with an "error" key.
    """
    try:
        return f1_broker.compare_constructors(constructor_a, constructor_b, season)
    except Exception as exc:
        return _error(exc, f"compare_constructors({constructor_a!r}, {constructor_b!r}, {season})")


@mcp.tool
def get_championship_standings(season: int, round: int | None = None) -> dict:
    """
    Get the drivers' championship standings after a given round.

    Args:
        season: Championship year.
        round: Round number. Omit for the latest round on record.

    Returns:
        A dict with the round used and a "standings" list carrying position,
        driver, constructor, cumulative points and wins, and gap to leader.
    """
    try:
        return f1_broker.championship_standings(season, round)
    except Exception as exc:
        return _error(exc, f"get_championship_standings({season}, {round})")


@mcp.tool
def get_race_weather(season: int, race: str) -> dict:
    """
    Get MEASURED race-day weather for a race.

    Observations come from Open-Meteo's ERA5 archive at the circuit's
    coordinates on the race date — not from the race report's description. A
    race is flagged `was_wet` when rainfall reached 1.0 mm, the point at which
    rain starts affecting tyre choice and grip rather than merely being noted.

    Args:
        season: Championship year.
        race: Round number ("21") or race name ("São Paulo", "Brazilian").

    Returns:
        A dict with resolved_race, conditions, temperatures, precipitation_mm,
        wind, and was_wet. If no observation exists, `weather_available` is
        false — which means NO DATA, not fair weather. Say so rather than
        implying the race was dry.
    """
    try:
        return f1_broker.race_weather(season, race)
    except Exception as exc:
        return _error(exc, f"get_race_weather({season}, {race!r})")


@mcp.tool
def find_wet_races(season: int | None = None, limit: int = 10) -> dict:
    """
    List races where measured rainfall crossed the wet threshold.

    Answers "which races were actually wet?" from rainfall in millimetres
    rather than from adjectives in a race report.

    Args:
        season: Restrict to one championship year. Omit for all seasons.
        limit: Maximum races to return, 1–25. Default 10.

    Returns:
        A dict with wet_race_count, the threshold applied, and races ordered by
        rainfall descending.
    """
    try:
        return f1_broker.wet_races(season, limit)
    except Exception as exc:
        return _error(exc, f"find_wet_races({season}, {limit})")


@mcp.tool
def search_race_reports(query: str, top_k: int = 5,
                        season: int | None = None, round: int | None = None) -> dict:
    """
    Semantic search over Formula 1 race-report prose.

    Searches the narrative of Wikipedia race reports by meaning, not keywords,
    so "chaotic wet race decided by a safety car" finds the right race without
    those words appearing. Each hit carries the measured weather for that race,
    so the narrative and the rainfall can be checked against each other.

    Args:
        query: What to look for, in plain language.
        top_k: Number of passages to return, 1–25. Default 5.
        season: Restrict to one championship year.
        round: Restrict to one round.

    Returns:
        A dict with a "results" list. Each result has season, round, race_name,
        the section it came from, similarity, the passage text, and that race's
        was_wet / precipitation_mm.
    """
    try:
        return f1_broker.search_reports(query, top_k, season, round)
    except Exception as exc:
        return _error(exc, f"search_race_reports({query!r})")


@mcp.tool
def get_season_schedule(season: int) -> dict:
    """
    List every round of a season in order, with its winner and rainfall.

    Use this to resolve a relative reference before calling a per-race tool.
    "The next race", "the round before that" and "the following weekend" are not
    race names and will not resolve - look the round number up here first, then
    pass that number.

    Args:
        season: Championship year.

    Returns:
        A dict with rounds, completed, and a schedule list carrying round,
        race_name, race_date, circuit, winner, precipitation_mm and conditions.
        A round with a null winner has not been raced yet.
    """
    try:
        return f1_broker.season_schedule(season)
    except Exception as exc:
        return _error(exc, f"get_season_schedule({season})")


@mcp.tool
def get_race_strategy(season: int, race: str) -> dict:
    """
    Get the pit-stop and stint strategy for one race, per driver.

    This is what separates "who won" from "how they won". A driver running one
    stint while a rival ran four means the result was decided in the pit lane.

    Args:
        season: Championship year.
        race: Round number, race name, or circuit ("Monza", "Suzuka").

    Returns:
        A dict with resolved_race, stint_spread, most_common_strategy, the five
        slowest stops, and per-driver stint and stop counts with finishing
        position. Tyre compounds are not available from this data source - use
        search_race_reports for the report's "Tyre choices" section.
    """
    try:
        return f1_broker.race_strategy(season, race)
    except Exception as exc:
        return _error(exc, f"get_race_strategy({season}, {race!r})")


@mcp.tool
def find_strategy_races(season: int, limit: int = 8) -> dict:
    """
    Find races where teams disagreed most about strategy.

    Ranked by the gap between the fewest and most stints any driver ran. A wide
    spread means the field genuinely split on approach - a better way to find
    interesting races than ranking by rainfall.

    Args:
        season: Championship year.
        limit: Maximum races to return, 1-25.

    Returns:
        A dict with races ordered by stint spread, each carrying min/max/avg
        stints and that race's measured rainfall.
    """
    try:
        return f1_broker.strategy_spread(season, limit)
    except Exception as exc:
        return _error(exc, f"find_strategy_races({season})")


@mcp.tool
def get_race_pace(season: int, race: str) -> dict:
    """
    Get race pace per driver, independent of finishing position.

    Measures how fast each driver actually was, as distinct from where they
    finished. Pace is the median of each driver's own clean laps (laps within
    107% of their own best in that race, which drops in-laps, out-laps and
    traffic behind a safety car), expressed relative to the fastest median in
    the race. A winner who controlled the race from the front is often not the
    quickest car on the day, and this tool is how that shows up.

    Args:
        season: Championship year.
        race: Round number, race name, or circuit ("Suzuka", "Monza").

    Returns:
        A dict with resolved_race, fastest_on_the_day, and a per-driver
        by_driver list carrying median_clean_lap_s, best_lap_s, consistency_s
        and pace_deficit_pct against the fastest driver in the race.
    """
    try:
        return f1_broker.race_pace(season, race)
    except Exception as exc:
        return _error(exc, f"get_race_pace({season}, {race!r})")


@mcp.tool
def get_constructor_standings(season: int, round: int | None = None) -> dict:
    """
    Get the constructors' championship standings after a given round.

    The team-level mirror of get_championship_standings — use this when the
    question is about a team's title fight rather than one driver's.

    Args:
        season: Championship year.
        round: Round number. Omit for the latest round on record.

    Returns:
        A dict with the round used and a "standings" list carrying position,
        constructor, cumulative points and wins, and gap to leader.
    """
    try:
        return f1_broker.constructor_standings(season, round)
    except Exception as exc:
        return _error(exc, f"get_constructor_standings({season}, {round})")


@mcp.tool
def get_watchlist() -> dict:
    """
    List the drivers, constructors and circuits currently being tracked.

    Returns:
        A dict with a count and the tracked items, newest first.
    """
    try:
        return f1_broker.get_watchlist()
    except Exception as exc:
        return _error(exc, "get_watchlist()")


@mcp.tool
def get_predictions(season: int | None = None, limit: int = 20) -> dict:
    """
    List previously logged race predictions.

    Args:
        season: Restrict to one championship year.
        limit: Maximum predictions to return, 1–25.

    Returns:
        A dict with a count and the predictions, newest first.
    """
    try:
        return f1_broker.get_predictions(season, limit=limit)
    except Exception as exc:
        return _error(exc, f"get_predictions({season})")


@mcp.tool
def get_race_notes(season: int | None = None, round: int | None = None,
                   limit: int = 20) -> dict:
    """
    List saved analyst notes, optionally for one race.

    Args:
        season: Restrict to one championship year.
        round: Restrict to one round.
        limit: Maximum notes to return, 1–25.

    Returns:
        A dict with a count and the notes, newest first.
    """
    try:
        return f1_broker.get_notes(season, round, limit=limit)
    except Exception as exc:
        return _error(exc, f"get_race_notes({season}, {round})")


# --------------------------------------------------------------------------
# WRITE tools — the agent takes real actions against the database
# --------------------------------------------------------------------------

@mcp.tool
def add_to_watchlist(entity_type: str, entity_ref: str, note: str | None = None) -> dict:
    """
    Add a driver, constructor or circuit to the watchlist. WRITES to the database.

    A driver reference is resolved to a real driver before storing, so the
    watchlist holds "Max Verstappen" rather than whatever was typed. Adding the
    same entity twice updates the note instead of creating a duplicate.

    Args:
        entity_type: "driver", "constructor" or "circuit".
        entity_ref: Who or what to track, e.g. "Verstappen" or "Ferrari".
        note: Optional reason for tracking it.

    Returns:
        A dict with written=true and the row that was stored, so the agent can
        confirm what happened rather than assert it.
    """
    try:
        return f1_broker.add_watchlist(entity_type, entity_ref, note)
    except Exception as exc:
        return _error(exc, f"add_to_watchlist({entity_type!r}, {entity_ref!r})")


@mcp.tool
def log_prediction(season: int, race: str, prediction: str,
                   confidence: str = "medium", rationale: str | None = None) -> dict:
    """
    Record a prediction against a specific race. WRITES to the database.

    Args:
        season: Championship year.
        race: Round number or race name.
        prediction: What is being predicted, in plain language.
        confidence: "low", "medium" or "high". Default "medium".
        rationale: Optional reasoning — worth filling in, because a prediction
            without a reason cannot be learned from later.

    Returns:
        A dict with written=true and the stored row, including the resolved
        race name.
    """
    try:
        return f1_broker.log_prediction(season, race, prediction, confidence, rationale)
    except Exception as exc:
        return _error(exc, f"log_prediction({season}, {race!r})")


@mcp.tool
def save_race_note(season: int, race: str, note: str) -> dict:
    """
    Save an analyst note against a race. WRITES to the database.

    Args:
        season: Championship year.
        race: Round number or race name.
        note: The note text.

    Returns:
        A dict with written=true and the stored row, including the resolved
        race name.
    """
    try:
        return f1_broker.save_note(season, race, note)
    except Exception as exc:
        return _error(exc, f"save_race_note({season}, {race!r})")


@mcp.tool
def remove_from_watchlist(item_id: int) -> dict:
    """
    Remove an entry from the watchlist by id. WRITES to the database.

    Args:
        item_id: The watchlist row's id, as returned by add_to_watchlist or
            get_watchlist.

    Returns:
        A dict with written=true and the row that was removed. Errors if no
        watchlist entry with that id exists for this user.
    """
    try:
        return f1_broker.remove_from_watchlist(item_id)
    except Exception as exc:
        return _error(exc, f"remove_from_watchlist({item_id})")


@mcp.tool
def delete_prediction(item_id: int) -> dict:
    """
    Delete a logged prediction by id. WRITES to the database.

    Args:
        item_id: The prediction row's id, as returned by log_prediction or
            get_predictions.

    Returns:
        A dict with written=true and the row that was deleted. Errors if no
        prediction with that id exists for this user.
    """
    try:
        return f1_broker.delete_prediction(item_id)
    except Exception as exc:
        return _error(exc, f"delete_prediction({item_id})")


@mcp.tool
def delete_note(item_id: int) -> dict:
    """
    Delete a saved race note by id. WRITES to the database.

    Args:
        item_id: The note row's id, as returned by save_race_note or
            get_race_notes.

    Returns:
        A dict with written=true and the row that was deleted. Errors if no
        note with that id exists for this user.
    """
    try:
        return f1_broker.delete_note(item_id)
    except Exception as exc:
        return _error(exc, f"delete_note({item_id})")


# --------------------------------------------------------------------------
# Human-facing routes
#
# FastMCP serves the protocol at /mcp and defines nothing at /, so opening the
# app URL in a browser returns a bare "Not Found" - correct for an MCP server,
# indistinguishable from a broken deployment to anyone checking.
# --------------------------------------------------------------------------

@mcp.custom_route("/health", methods=["GET"])
async def health(_request: Request) -> JSONResponse:
    """Liveness probe. Does not touch Lakebase, so a database problem cannot
    make the platform conclude the container is dead."""
    return JSONResponse({"status": "ok", "server": "f1-intelligence-copilot"})


@mcp.custom_route("/", methods=["GET"])
async def landing(request: Request) -> HTMLResponse:
    tools = await mcp.list_tools()
    writes = {"add_to_watchlist", "log_prediction", "save_race_note",
              "remove_from_watchlist", "delete_prediction", "delete_note"}
    rows = "".join(
        f"<tr><td><code>{html.escape(t.name)}</code></td>"
        f"<td>{'<b>write</b>' if t.name in writes else 'read'}</td>"
        f"<td>{html.escape(((t.description or '').strip().splitlines() or [''])[0])}</td></tr>"
        for t in sorted(tools, key=lambda t: (t.name in writes, t.name))
    )
    # Behind the Databricks Apps proxy request.url is the internal address
    # (localhost:8000), which is useless to anyone copying it into the external
    # MCP registration form.
    fhost = request.headers.get("x-forwarded-host")
    fproto = request.headers.get("x-forwarded-proto", "https")
    endpoint = html.escape(f"{fproto}://{fhost}/mcp" if fhost
                           else str(request.url.replace(path="/mcp", query="")))
    return HTMLResponse(f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>F1 Intelligence Copilot — MCP Server</title>
<style>
 :root {{ color-scheme: light dark; }}
 body {{ font:16px/1.6 system-ui,sans-serif; max-width:52rem; margin:3rem auto; padding:0 1.25rem; }}
 h1 {{ font-size:1.5rem; margin-bottom:.2rem; }} .sub {{ opacity:.7; margin-top:0; }}
 .ok {{ display:inline-block; padding:.15rem .6rem; border-radius:1rem; background:#137333; color:#fff; font-size:.8rem; }}
 table {{ border-collapse:collapse; width:100%; margin:1rem 0; }}
 td,th {{ text-align:left; padding:.45rem .5rem; border-bottom:1px solid rgba(128,128,128,.3); vertical-align:top; }}
 .box {{ padding:.7rem 1rem; border:1px solid rgba(128,128,128,.35); border-radius:.5rem; overflow-x:auto; }}
</style></head><body>
<h1>F1 Intelligence Copilot <span class="ok">running</span></h1>
<p class="sub">MCP server over F1 results, race-report prose and measured race-day weather.</p>
<h2>MCP endpoint</h2>
<div class="box"><code>{endpoint}</code></div>
<p>Register that URL as an external MCP server (streamable HTTP).
This page is for humans; agents talk to <code>/mcp</code>.</p>
<h2>Tools ({len(tools)})</h2>
<table><tr><th>Tool</th><th>Kind</th><th>What it does</th></tr>{rows}</table>
<h2>Data sources</h2>
<p>Jolpica-F1 via a Spark medallion pipeline (results, standings) ·
Wikipedia race reports (embedded for semantic search) ·
Open-Meteo ERA5 archive (measured race-day weather).</p>
</body></html>""")


def build_app():
    """Serve MCP on every path the Databricks gateway might call.

    Databricks registers an App-hosted MCP server by storing the app URL with
    "/mcp" already appended, then appends "/mcp" again when calling it - so it
    requests "/mcp/mcp", which a single-mount server answers with a 404 whose
    body is the bare string "Not Found". The gateway JSON-parses that and
    reports `Unrecognized token 'Not'`, which reads like a broken server but is
    a path mismatch.

    Starlette Mounts were tried and rejected: mounting at two prefixes makes
    "/mcp" answer 307 and "/mcp/mcp" answer 404, and a gateway that does not
    follow redirects fails either way. Rewriting the path before routing is
    exact - one app, one route, no redirects.
    """
    mcp_asgi = mcp.http_app(path="/mcp")

    async def rewrite(scope, receive, send):
        if scope["type"] == "http" and scope.get("path", "").rstrip("/") == "/mcp/mcp":
            scope = dict(scope)
            scope["path"] = "/mcp"
            scope["raw_path"] = b"/mcp"
        await mcp_asgi(scope, receive, send)

    # The MCP app owns the streamable-HTTP session manager, started by its
    # lifespan. Losing it yields a server that 500s on the first request.
    rewrite.lifespan = mcp_asgi.lifespan
    return rewrite


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("DATABRICKS_APP_PORT", os.getenv("PORT", 8000)))
    logger.info("Starting F1 MCP server on port %d", port)
    uvicorn.run(build_app(), host="0.0.0.0", port=port)
