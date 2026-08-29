"""
The assistant, running inside the frontend.

Same tools as the MCP server, exercised through the same `f1_broker` functions -
including the three that write. The MCP server exposes them over the protocol so
a Databricks agent can call them; this module calls them in-process so the app
itself is a working demo rather than a page that tells you to go and use one.

WHY IN-PROCESS AND NOT OVER MCP
--------------------------------
The MCP server is a separate Databricks App behind its own OAuth. Calling it
from here would mean this app authenticating to that app on every turn - a
second network hop and a second identity to get wrong - to reach Python
functions that are already importable. The MCP surface exists for external
agents; a shared broker means both paths run identical code, so a bug in one is
a bug in both rather than a divergence.

TOOL RESULTS ARE TRUNCATED BEFORE THEY REACH THE MODEL
-------------------------------------------------------
`get_driver_season` returns 24 races of detail. Feeding that back verbatim
burns the context window and buys nothing - the model needs the summary, not
every row. Each result is trimmed to what is answerable from.
"""

import json
import logging
import time
import uuid

import f1_broker
import requests
from databricks.sdk import WorkspaceClient

from f1lake import schema

logger = logging.getLogger("agent")

MODEL = "databricks-meta-llama-3-3-70b-instruct"
MAX_TURNS = 5  # tool round-trips before we stop and answer with what we have
TIMEOUT = 90

SYSTEM_PROMPT = """You are an F1 race analyst assistant. You answer questions about \
Formula 1 using ONLY the tools provided. You have no F1 knowledge of your own.

WHAT YOU HAVE
- Race results and championship standings for 2024-2026 (from a data pipeline).
- The narrative of 58 Wikipedia race reports, searchable by meaning.
- MEASURED race-day weather from a weather archive, per circuit per race date.

TOOL USE
- Call the most specific tool for the question. Do not call them all.
- For "what happened in race X", use search_race_reports - the narrative is
  there, the results table is not a story.
- For "why did X win/lose" or anything about pit stops, tyres or strategy, call
  get_race_strategy AND search_race_reports. The numbers say what the strategy
  WAS; the report says why it was chosen and whether it worked.
- Ask search_race_reports for at least 5 passages. One or two is not enough to
  find the sentence that answers a question, and a thin search is how you end
  up saying "the report does not mention it" when it does.
- For "was it wet", use get_race_weather. That is measurement, not opinion.
- When a user asks you to track, predict, or note something, actually call the
  write tool. Then confirm what you saved, quoting the row you got back.

THE ONE THING THAT MATTERS MOST
Measured rainfall is a DAILY total. It cannot tell you whether rain fell during
the race or overnight. If someone asks whether a race was wet, check both the
weather AND the race report - the report says whether the track was actually
wet while they were racing. Monza 2024 had the most rain of any race here and
ran dry. Say so when it applies; never treat rainfall alone as proof.

WHEN THE QUESTION IS WRONG
Users ask loaded questions - "why did Ferrari lose Monza?" when Ferrari won it.
Correct the premise FIRST, in the opening sentence, then answer what actually
happened. Never say "it is not possible to determine why they lost" - that
reads as a failure to answer when you in fact have the answer. Say "Ferrari did
not lose - Leclerc won on a one-stop" and carry on.

GUARDRAILS
- Never state a figure you did not get from a tool call in this conversation.
- If a tool returns an "error" key, follow its "suggestion". Ask the user to
  clarify rather than guessing.
- Every result tells you what it resolved - "resolved_driver", "resolved_race".
  If that does not match what the user meant, say which one you used.
- Be brief. Lead with the answer, then the numbers that support it.
- Never open with what you could not find. Open with what you know.

USE WHAT THE TOOLS GAVE YOU
- If you called get_race_strategy, quote the numbers: grid position, finishing
  position, stop count, and how that compared to the rest of the field. Do not
  say "the exact details of the strategy are not specified" when the tool just
  handed you the stint counts - that is false, and it wastes the call.
- Never explain a result with "driving skill", "team performance" or similar
  filler. Those are not in the data. Explain with grid position, stops, stint
  lengths, rainfall, or what the report says happened.
- Do not hedge a conclusion the evidence supports. If rainfall was high and the
  report describes a dry track, say the track was dry - not "may have been dry".

- Asked "which races...", report the TOP 3-5 and say what separates them.
  find_strategy_races returns a season ordered by stint spread; listing twelve
  of them is a table, not an answer. Ask for a small limit and name the ones
  that stand out.

TOOL ARGUMENTS
- `race` takes ONE race: a round number, a race name, or a circuit. There is no
  "all". To look across a season use find_strategy_races or find_wet_races.
- NEVER pass a relative phrase as `race`. "the next race", "the one before that"
  and "the following round" are not race names and will not resolve. Call
  get_season_schedule first, read the round number you need off it, then pass
  that number. This applies to any follow-up that refers to an earlier answer.
- If a per-race tool comes back empty, call get_season_schedule BEFORE searching.
  A round with no winner has not been raced yet: say that plainly, say when it is
  scheduled, and stop. Re-running the same search with reworded queries cannot
  conjure a result that does not exist yet, and three near-identical searches
  read as flailing."""


# Tool schemas. Kept beside the dispatch table so a tool cannot be advertised
# without being callable.
TOOLS = [
    (
        "get_driver_season",
        "A driver's race-by-race results and season totals.",
        {
            "driver": ("string", "Driver name, surname or code, e.g. 'Verstappen'"),
            "season": ("integer", "Championship year, e.g. 2024"),
        },
        ["driver", "season"],
    ),
    (
        "compare_constructors",
        "Compare two teams' form across a season.",
        {
            "constructor_a": ("string", "First team, e.g. 'Ferrari'"),
            "constructor_b": ("string", "Second team, e.g. 'McLaren'"),
            "season": ("integer", "Championship year"),
        },
        ["constructor_a", "constructor_b", "season"],
    ),
    (
        "get_championship_standings",
        "Drivers' championship standings after a round.",
        {
            "season": ("integer", "Championship year"),
            "round": ("integer", "Round number; omit for the latest"),
        },
        ["season"],
    ),
    (
        "get_race_weather",
        "MEASURED race-day weather for one race.",
        {
            "season": ("integer", "Championship year"),
            "race": ("string", "Round number or race name, e.g. 'Sao Paulo'"),
        },
        ["season", "race"],
    ),
    (
        "find_wet_races",
        "Races where measured rainfall crossed the wet threshold.",
        {
            "season": ("integer", "Restrict to one year; omit for all"),
            "limit": ("integer", "Max races, 1-25"),
        },
        [],
    ),
    (
        "search_race_reports",
        "Search race-report narrative by meaning.",
        {
            "query": ("string", "What to look for, in plain language"),
            "top_k": ("integer", "How many passages, 1-10"),
            "season": ("integer", "Restrict to one year"),
        },
        ["query"],
    ),
    (
        "get_season_schedule",
        "Every round of a season in order, with winner and rainfall. "
        "Use this to turn a relative reference - 'the next race', 'the round before' - "
        "into a round number before calling any per-race tool.",
        {"season": ("integer", "Championship year, e.g. 2024")},
        ["season"],
    ),
    (
        "get_race_strategy",
        "Pit stops and stints for one race, per driver - how a result was won.",
        {
            "season": ("integer", "Championship year"),
            "race": ("string", "Round number, race name or circuit, e.g. 'Suzuka'"),
        },
        ["season", "race"],
    ),
    (
        "find_strategy_races",
        "Races where teams disagreed most about strategy.",
        {"season": ("integer", "Championship year"), "limit": ("integer", "Max races, 1-25")},
        ["season"],
    ),
    (
        "get_race_pace",
        "Race pace per driver, independent of finishing position - "
        "who was actually fast, as distinct from who finished ahead.",
        {
            "season": ("integer", "Championship year"),
            "race": ("string", "Round number, race name or circuit, e.g. 'Suzuka'"),
        },
        ["season", "race"],
    ),
    (
        "get_constructor_standings",
        "Constructors' championship standings after a round.",
        {
            "season": ("integer", "Championship year"),
            "round": ("integer", "Round number; omit for the latest"),
        },
        ["season"],
    ),
    ("get_watchlist", "List tracked drivers, constructors and circuits.", {}, []),
    (
        "get_predictions",
        "List previously logged predictions.",
        {"season": ("integer", "Restrict to one year")},
        [],
    ),
    (
        "get_race_notes",
        "List saved race notes.",
        {"season": ("integer", "Restrict to one year")},
        [],
    ),
    # --- writes ---
    (
        "add_to_watchlist",
        "WRITE. Track a driver, constructor or circuit.",
        {
            "entity_type": ("string", "'driver', 'constructor' or 'circuit'"),
            "entity_ref": ("string", "Who or what to track"),
            "note": ("string", "Optional reason"),
        },
        ["entity_type", "entity_ref"],
    ),
    (
        "log_prediction",
        "WRITE. Record a prediction against a race.",
        {
            "season": ("integer", "Championship year"),
            "race": ("string", "Round number or race name"),
            "prediction": ("string", "What you predict, in plain language"),
            "confidence": ("string", "'low', 'medium' or 'high'"),
            "rationale": ("string", "Why"),
        },
        ["season", "race", "prediction"],
    ),
    (
        "save_race_note",
        "WRITE. Save an analyst note against a race.",
        {
            "season": ("integer", "Championship year"),
            "race": ("string", "Round number or race name"),
            "note": ("string", "The note text"),
        },
        ["season", "race", "note"],
    ),
    (
        "remove_from_watchlist",
        "WRITE. Remove a watchlist entry by id.",
        {"item_id": ("integer", "The watchlist row's id")},
        ["item_id"],
    ),
    (
        "delete_prediction",
        "WRITE. Delete a logged prediction by id.",
        {"item_id": ("integer", "The prediction row's id")},
        ["item_id"],
    ),
    (
        "delete_note",
        "WRITE. Delete a saved race note by id.",
        {"item_id": ("integer", "The note row's id")},
        ["item_id"],
    ),
]

WRITE_TOOLS = {
    "add_to_watchlist",
    "log_prediction",
    "save_race_note",
    "remove_from_watchlist",
    "delete_prediction",
    "delete_note",
}

DISPATCH = {
    "get_driver_season": lambda a: f1_broker.driver_season(a["driver"], a["season"]),
    "compare_constructors": lambda a: f1_broker.compare_constructors(
        a["constructor_a"], a["constructor_b"], a["season"]
    ),
    "get_championship_standings": lambda a: f1_broker.championship_standings(
        a["season"], a.get("round")
    ),
    "get_race_weather": lambda a: f1_broker.race_weather(a["season"], a["race"]),
    "find_wet_races": lambda a: f1_broker.wet_races(a.get("season"), a.get("limit", 10)),
    "search_race_reports": lambda a: f1_broker.search_reports(
        a["query"], a.get("top_k", 5), a.get("season")
    ),
    "get_season_schedule": lambda a: f1_broker.season_schedule(a["season"]),
    "get_race_strategy": lambda a: f1_broker.race_strategy(a["season"], a["race"]),
    "find_strategy_races": lambda a: f1_broker.strategy_spread(a["season"], a.get("limit", 8)),
    "get_race_pace": lambda a: f1_broker.race_pace(a["season"], a["race"]),
    "get_constructor_standings": lambda a: f1_broker.constructor_standings(
        a["season"], a.get("round")
    ),
    "get_watchlist": lambda a: f1_broker.get_watchlist(),
    "get_predictions": lambda a: f1_broker.get_predictions(a.get("season")),
    "get_race_notes": lambda a: f1_broker.get_notes(a.get("season")),
    "add_to_watchlist": lambda a: f1_broker.add_watchlist(
        a["entity_type"], a["entity_ref"], a.get("note")
    ),
    "log_prediction": lambda a: f1_broker.log_prediction(
        a["season"], a["race"], a["prediction"], a.get("confidence", "medium"), a.get("rationale")
    ),
    "save_race_note": lambda a: f1_broker.save_note(a["season"], a["race"], a["note"]),
    "remove_from_watchlist": lambda a: f1_broker.remove_from_watchlist(a["item_id"]),
    "delete_prediction": lambda a: f1_broker.delete_prediction(a["item_id"]),
    "delete_note": lambda a: f1_broker.delete_note(a["item_id"]),
}


def _schemas() -> list[dict]:
    out = []
    for name, desc, props, required in TOOLS:
        out.append(
            {
                "type": "function",
                "function": {
                    "name": name,
                    "description": desc,
                    "parameters": {
                        "type": "object",
                        "properties": {
                            k: {"type": t, "description": d} for k, (t, d) in props.items()
                        },
                        "required": required,
                    },
                },
            }
        )
    return out


def _trim(name: str, result) -> dict:
    """Shrink a tool result to what the model can actually answer from.

    get_driver_season returns 24 races of detail and search returns full
    passages; sending those back verbatim floods the context and degrades the
    answer. The fields dropped here are the ones the UI shows, not the ones the
    model reasons over.
    """
    if not isinstance(result, dict):
        return {"result": result}
    out = dict(result)
    if name == "get_driver_season" and "results" in out:
        out["results"] = [
            {
                k: r.get(k)
                for k in (
                    "round",
                    "race_name",
                    "grid_position",
                    "finish_position",
                    "total_points",
                    "status",
                )
            }
            for r in out["results"]
        ]
    if name == "search_race_reports" and "results" in out:
        out["results"] = [
            {
                "season": r.get("season"),
                "round": r.get("round"),
                "race_name": r.get("race_name"),
                "section": r.get("section"),
                "similarity": round(float(r.get("similarity") or 0), 3),
                "was_wet": r.get("was_wet"),
                "precipitation_mm": r.get("precipitation_mm"),
                "text": (r.get("chunk_text") or "")[:700],
            }
            for r in out["results"]
        ]
    if name in ("get_race_strategy", "get_race_pace") and "by_driver" in out:
        out["by_driver"] = out["by_driver"][:12]
    if name in ("get_championship_standings", "get_constructor_standings") and "standings" in out:
        out["standings"] = out["standings"][:12]
    return out


def _post(messages: list[dict]) -> dict:
    w = WorkspaceClient()
    headers = w.config.authenticate()
    headers["Content-Type"] = "application/json"
    response = requests.post(
        f"{w.config.host.rstrip('/')}/serving-endpoints/{MODEL}/invocations",
        headers=headers,
        json={
            "messages": messages,
            "tools": _schemas(),
            "tool_choice": "auto",
            "max_tokens": 900,
            "temperature": 0.1,
        },
        timeout=TIMEOUT,
    )
    response.raise_for_status()
    return response.json()["choices"][0]["message"]


def _summarise(name: str, result) -> str | None:
    """One readable line per call, so the analytics table shows what happened
    rather than a JSON blob."""
    if not isinstance(result, dict):
        return None
    if result.get("error"):
        return f"{result['error']}: {str(result.get('message', ''))[:120]}"
    for key in ("resolved_driver", "resolved_race", "ahead_on_points", "leader"):
        if result.get(key):
            return f"{key}={result[key]}"
    if result.get("written"):
        return f"wrote {result.get('action')}"
    if "results" in result:
        return f"{len(result['results'])} passages"
    if "races" in result:
        return f"{len(result['races'])} races"
    return None


def _evidence(name: str, result) -> str | None:
    """The one fact a reader needs to see that the tool actually returned data.

    Distinct from _summarise, which feeds the analytics table and must stay
    stable. This is for the trace shown in the UI, where "get_race_weather"
    alone proves nothing: a tool name with no value beside it is indistinguishable
    from a tool that returned an empty row. The number is the evidence.
    """
    if not isinstance(result, dict):
        return None
    if result.get("error"):
        return str(result.get("message") or result["error"])[:110]
    if result.get("status") == "not_yet_raced":
        return "not yet raced"

    bits: list[str] = []
    if result.get("precipitation_mm") is not None:
        bits.append(f"{result['precipitation_mm']} mm")
        if result.get("conditions"):
            bits.append(str(result["conditions"]))
        if result.get("was_wet") is not None:
            # Name the source, not the conclusion. was_wet is derived from the
            # daily rainfall total alone, so a bare "wet race" in the trace sits
            # directly above answers that correctly say the race ran dry - which
            # reads as the agent contradicting its own evidence rather than as
            # the rainfall flag being the thing the race report overrules.
            bits.append(
                "flagged wet by rainfall" if result["was_wet"] else "under the wet threshold"
            )
    elif result.get("weather_available") is False:
        bits.append("no weather recorded")

    if result.get("stint_spread"):
        bits.append(f"stint spread {result['stint_spread']}")
    if result.get("fastest_on_the_day"):
        bits.append(f"fastest: {result['fastest_on_the_day']}")
    if result.get("drivers_analysed"):
        bits.append(f"{result['drivers_analysed']} drivers")
    if isinstance(result.get("results"), list):
        bits.append(f"{len(result['results'])} passages")
        top = result["results"][0].get("similarity") if result["results"] else None
        if isinstance(top, (int, float)):
            bits.append(f"top {top:.2f}")
    if isinstance(result.get("races"), list):
        bits.append(f"{len(result['races'])} races")
    if isinstance(result.get("schedule"), list):
        bits.append(f"{result.get('completed')} of {result.get('rounds')} rounds run")
    if result.get("written"):
        # The write tools return the stored row under "row" - that round trip is
        # the point, since an INSERT that never committed looks identical from
        # the caller's side. Surfacing the primary key makes the trace show the
        # row came back from the database rather than just that a call was made.
        row = result.get("row") if isinstance(result.get("row"), dict) else {}
        bits.append(f"saved · id {row['id']}" if row.get("id") else "saved")
    for key in ("resolved_race", "resolved_driver", "leader", "ahead_on_points"):
        if result.get(key):
            bits.insert(0, str(result[key]))
            break
    return " · ".join(bits)[:140] or None


# Follow-ups are derived from what the tools actually returned rather than
# generated by a second model call. A model asked to invent follow-ups will
# happily suggest questions this data cannot answer, and it costs another round
# trip on every turn. Reading them off the trace is free and cannot suggest a
# race that is not in the corpus.
_GENERIC_FOLLOWUPS = [
    "Which 2024 races were decided by strategy?",
    "Was Monza 2024 actually a wet race?",
    "Show me everything you have saved",
]


def _followups(trace: list[dict]) -> list[str]:
    """Up to three next questions that this data can answer."""
    if not trace:
        return _GENERIC_FOLLOWUPS[:3]

    called = {t["tool"] for t in trace}
    race = season = driver = None
    for t in trace:
        args, res = t.get("arguments") or {}, t.get("result")
        if isinstance(res, dict):
            race = res.get("resolved_race") or race
            driver = res.get("resolved_driver") or driver
        season = args.get("season") or season
        race = race or args.get("race")
        driver = driver or args.get("driver")

    out: list[str] = []
    if race and season:
        if "get_race_weather" not in called:
            out.append(f"Was the {season} {race} actually a wet race?")
        if "search_race_reports" not in called:
            out.append(f"What does the race report say about the {season} {race}?")
        if "get_race_strategy" not in called:
            out.append(f"How was the {season} {race} won in the pit lane?")
        out.append(f"Save a note about the {season} {race}")
    if driver:
        out.append(f"Track {driver} for me")
    if season and "find_strategy_races" not in called:
        out.append(f"Which {season} races were decided by strategy?")
    if called & WRITE_TOOLS:
        out.insert(0, "Show me everything you have saved")

    seen, unique = set(), []
    for q in out + _GENERIC_FOLLOWUPS:
        if q not in seen:
            seen.add(q)
            unique.append(q)
    return unique[:3]


def ask(question: str, history: list[dict] | None = None, session_id: str | None = None) -> dict:
    """Answer a question, calling tools as needed.

    Returns the answer plus the full trace of tool calls, because a demo that
    only shows the prose gives no way to tell whether the agent used its tools
    or made the answer up.
    """
    session_id = session_id or uuid.uuid4().hex[:12]
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    messages += history or []
    messages.append({"role": "user", "content": question})

    trace = []
    wrote_ok = False
    for _ in range(MAX_TURNS):
        message = _post(messages)
        calls = message.get("tool_calls") or []
        if not calls:
            return {
                "answer": _text(message.get("content")),
                "trace": trace,
                "wrote": wrote_ok,
                "followups": _followups(trace),
            }

        messages.append(
            {"role": "assistant", "content": message.get("content") or "", "tool_calls": calls}
        )
        for call in calls:
            name = call["function"]["name"]
            try:
                args = json.loads(call["function"]["arguments"] or "{}")
            except json.JSONDecodeError:
                args = {}
            started = time.perf_counter()
            try:
                result = (
                    DISPATCH[name](args)
                    if name in DISPATCH
                    else {"error": "unknown_tool", "message": f"No tool named {name}"}
                )
            except Exception as exc:
                logger.exception("Tool %s failed", name)
                result = {
                    "error": "tool_failed",
                    "message": schema.safe_message(exc),
                    "suggestion": "Tell the user the lookup failed. Do not guess.",
                }
            duration_ms = int((time.perf_counter() - started) * 1000)
            trimmed = _trim(name, result)

            # Feeds the Change Data Feed analytics loop. Best effort by design.
            schema.log_tool_call(
                tool_name=name,
                arguments=args,
                outcome=(result.get("error", "ok") if isinstance(result, dict) else "ok"),
                is_write=name in WRITE_TOOLS,
                summary=_summarise(name, result),
                duration_ms=duration_ms,
                session_id=session_id,
            )
            # "Did a write succeed" is not "was a write tool called". A write
            # that failed to resolve its race still put the tool in the trace,
            # and the page used that to render "Saved - view it under what the
            # assistant has done", linking to a row that was never created.
            # This project exists to catch writes that report success; the flag
            # reporting them cannot be the one that lies.
            if name in WRITE_TOOLS and isinstance(result, dict) and result.get("written"):
                wrote_ok = True
            trace.append(
                {
                    "tool": name,
                    "arguments": args,
                    "result": trimmed,
                    "is_write": name in WRITE_TOOLS,
                    "evidence": _evidence(name, result),
                }
            )
            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": call["id"],
                    "content": json.dumps(trimmed, default=str)[:6000],
                }
            )

    # Ran out of turns. Answer from what we have rather than looping forever.
    messages.append(
        {"role": "user", "content": "Answer now using what the tools already returned."}
    )
    message = _post(messages)
    return {
        "answer": _text(message.get("content")),
        "trace": trace,
        "wrote": wrote_ok,
        "followups": _followups(trace),
        "note": "Stopped after the maximum number of tool calls.",
    }


def _text(content) -> str:
    """Normalise a message payload to plain text.

    Reasoning models return a list of typed blocks including chain-of-thought
    rather than a string; returning that verbatim would leak the model's
    internal deliberation into the UI.
    """
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = [
            b.get("text", "") for b in content if isinstance(b, dict) and b.get("type") == "text"
        ]
        if parts:
            return "\n".join(p for p in parts if p).strip()
    return str(content or "")
