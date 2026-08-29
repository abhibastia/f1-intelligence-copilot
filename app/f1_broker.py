"""
Data access for the F1 MCP server — every query and every write lives here.

The tools in `f1_mcp_server.py` stay thin: validate arguments, call one function
here, shape the result. Nothing in this module knows MCP exists, which is what
makes the F1 logic testable with a plain Python call — no agent, no MCP client,
no deployed app.

EVERYTHING READS POSTGRES, NOTHING READS DELTA
----------------------------------------------
The Spark pipeline's Gold marts were seeded into Lakebase once (see
`f1lake/seed_gold.py`). Serving them from Postgres means an agent turn costs no
Databricks compute — which matters because Free Edition's daily quota is
unrecoverable until the next day, and an agent that spends compute per question
is an agent that dies mid-demo.

IDENTITY RESOLUTION IS RETURNED, NEVER ASSUMED
-----------------------------------------------
Users say "Verstappen"; the data says `driver_id='max_verstappen'`. Every read
that resolves a name returns what it matched, so a wrong match is visible in the
agent's answer rather than silently wrong. That is the `resolved_location`
lesson from the weather MCP server, applied to drivers and races.
"""

import logging

from f1lake import embedder, schema

logger = logging.getLogger("f1-broker")

DRIVERS = "f1_driver_performance"
CHAMPIONSHIP = "f1_championship"

MAX_RESULTS = 25

# schema.RACE_STRATEGY_SUMMARY, schema.LAP_PACE and schema.CONSTRUCTOR_STANDINGS
# are seeded from Gold marts (race_strategy, lap_pace, constructor_standings)
# that the standalone f1-strategy-copilot project never surfaced to the agent
# at all — see f1lake/seed_gold.py.


class UnknownDriverError(ValueError):
    """The driver string could not be resolved to exactly one driver."""


class UnknownRaceError(ValueError):
    """The race could not be resolved."""


# --------------------------------------------------------------------------
# Resolution
# --------------------------------------------------------------------------


def resolve_driver(name: str, season: int | None = None) -> dict:
    """Resolve a free-text driver reference to a single driver.

    Matches against driver_id, full name and the three-letter code, in that
    order of specificity. An ambiguous match raises rather than silently
    picking the first — "Schumacher" should ask a question, not guess.
    """
    if not isinstance(name, str) or not name.strip():
        raise UnknownDriverError("Driver must be a non-empty string")
    needle = name.strip().lower()

    where = "WHERE (lower(driver_id) = %s OR lower(driver_name) = %s OR lower(driver_code) = %s)"
    params: list = [needle, needle, needle]
    if season:
        where += " AND season = %s"
        params.append(int(season))

    exact = schema.query(
        f"SELECT DISTINCT driver_id, driver_name, driver_code FROM {DRIVERS} {where}",
        tuple(params),
    )
    if len(exact) == 1:
        return exact[0]

    # Fall back to a substring match, which is what catches "verstappen".
    where = "WHERE (unaccent(lower(driver_name)) LIKE unaccent(%s)     OR lower(driver_id) LIKE %s)"
    params = [f"%{needle}%", f"%{needle}%"]
    if season:
        where += " AND season = %s"
        params.append(int(season))

    fuzzy = schema.query(
        f"SELECT DISTINCT driver_id, driver_name, driver_code FROM {DRIVERS} {where} "
        f"ORDER BY driver_name",
        tuple(params),
    )
    if not fuzzy:
        raise UnknownDriverError(
            f"No driver matching {name!r}" + (f" in {season}" if season else "")
        )
    if len(fuzzy) > 1:
        options = ", ".join(d["driver_name"] for d in fuzzy[:6])
        raise UnknownDriverError(f"{name!r} matches several drivers: {options}. Ask which one.")
    return fuzzy[0]


def resolve_race(season: int, round_or_name) -> dict:
    """Resolve (season, round) or (season, race name) to one race."""
    season = int(season)
    needle = None  # set only on the name path; guards the alias lookup
    try:
        rnd = int(round_or_name)
        rows = schema.query(
            f"SELECT season, round, race_name, race_date, circuit_name "
            f"FROM {schema.RACES} WHERE season = %s AND round = %s",
            (season, rnd),
        )
    except (TypeError, ValueError):
        # Match the circuit as well as the race name, and fold accents.
        #
        # Both halves were real failures. People say "Monza", "Spa" and
        # "Silverstone" far more often than "Italian Grand Prix", and matching
        # only race_name made the agent burn three extra tool calls retrying.
        # Separately, the data says "Sao Paulo Grand Prix" with an accent while
        # nobody types one, so an exact LIKE silently failed and the agent told
        # the user the race did not exist.
        needle = str(round_or_name).strip().lower()
        rows = schema.query(
            f"SELECT season, round, race_name, race_date, circuit_name "
            f"FROM {schema.RACES} "
            f"WHERE season = %s AND ("
            f"     unaccent(lower(race_name))    LIKE unaccent(%s) "
            f"  OR unaccent(lower(circuit_name)) LIKE unaccent(%s) "
            f"  OR lower(circuit_id)             LIKE %s) "
            f"ORDER BY round",
            (season, f"%{needle}%", f"%{needle}%", f"%{needle}%"),
        )
    if not rows and needle:
        # Races get renamed. The same circuit is the "São Paulo Grand Prix" in
        # 2024 and the "Brazilian Grand Prix" in 2026; Barcelona and Spanish
        # likewise. Asking about Brazil in 2024 therefore failed while the same
        # question about 2026 worked, which reads as missing data rather than a
        # naming change.
        #
        # The aliases are derived from the corpus rather than hardcoded: find
        # the circuit that has ever carried this name, then take that circuit's
        # race in the season asked for. A future rename needs no code change.
        rows = schema.query(
            f"SELECT season, round, race_name, race_date, circuit_name "
            f"FROM {schema.RACES} "
            f"WHERE season = %s AND circuit_id IN ("
            f"    SELECT DISTINCT circuit_id FROM {schema.RACES} "
            f"     WHERE unaccent(lower(race_name)) LIKE unaccent(%s)) "
            f"ORDER BY round",
            (season, f"%{needle}%"),
        )
    if not rows:
        raise UnknownRaceError(f"No race matching {round_or_name!r} in {season}")
    if len(rows) > 1:
        options = ", ".join(f"R{r['round']} {r['race_name']}" for r in rows[:6])
        raise UnknownRaceError(f"{round_or_name!r} matches several races in {season}: {options}")
    return rows[0]


# --------------------------------------------------------------------------
# Reads
# --------------------------------------------------------------------------


def driver_season(name: str, season: int) -> dict:
    """A driver's race-by-race season, with totals."""
    driver = resolve_driver(name, season)
    rows = schema.query(
        f"""SELECT season, round, race_name, race_date, circuit_name,
                   constructor_name_as_of_race, grid_position, finish_position,
                   position_text, positions_gained, race_points, sprint_points,
                   total_points, is_win, is_podium, dnf_flag, status
            FROM {DRIVERS}
            WHERE driver_id = %s AND season = %s
            ORDER BY round""",
        (driver["driver_id"], int(season)),
    )
    if not rows:
        raise UnknownDriverError(f"{driver['driver_name']} has no {season} races on record")

    def total(field):
        return sum(float(r[field] or 0) for r in rows)

    return {
        "resolved_driver": driver["driver_name"],
        "driver_id": driver["driver_id"],
        "season": int(season),
        "races": len(rows),
        "total_points": round(total("total_points"), 1),
        "wins": sum(1 for r in rows if str(r["is_win"]).lower() in ("true", "1")),
        "podiums": sum(1 for r in rows if str(r["is_podium"]).lower() in ("true", "1")),
        "dnfs": sum(1 for r in rows if str(r["dnf_flag"]).lower() in ("true", "1")),
        "constructors": sorted(
            {r["constructor_name_as_of_race"] for r in rows if r["constructor_name_as_of_race"]}
        ),
        "results": rows,
    }


def compare_constructors(a: str, b: str, season: int) -> dict:
    """Compare two constructors' season form, side by side."""
    season = int(season)
    out = {}
    for label in (a, b):
        rows = schema.query(
            f"""SELECT constructor_name_as_of_race AS constructor,
                       count(*) AS entries,
                       sum(COALESCE(total_points, 0)) AS points,
                       sum(CASE WHEN lower(is_win::text) IN ('true','1') THEN 1 ELSE 0 END) AS wins,
                       sum(CASE WHEN lower(is_podium::text) IN ('true','1') THEN 1 ELSE 0 END) AS podiums,
                       sum(CASE WHEN lower(dnf_flag::text) IN ('true','1') THEN 1 ELSE 0 END) AS dnfs,
                       avg(NULLIF(finish_position, 0)) AS avg_finish
                FROM {DRIVERS}
                WHERE season = %s AND lower(constructor_name_as_of_race) LIKE %s
                GROUP BY constructor_name_as_of_race""",
            (season, f"%{str(label).strip().lower()}%"),
        )
        if not rows:
            raise UnknownDriverError(f"No constructor matching {label!r} in {season}")
        row = rows[0]
        out[label] = {
            "resolved_constructor": row["constructor"],
            "entries": int(row["entries"]),
            "points": round(float(row["points"] or 0), 1),
            "wins": int(row["wins"]),
            "podiums": int(row["podiums"]),
            "dnfs": int(row["dnfs"]),
            "avg_finish": round(float(row["avg_finish"]), 2) if row["avg_finish"] else None,
        }

    first, second = out[a], out[b]
    leader = first if first["points"] >= second["points"] else second
    return {
        "season": season,
        "compared": out,
        "ahead_on_points": leader["resolved_constructor"],
        "points_gap": round(abs(first["points"] - second["points"]), 1),
    }


def championship_standings(season: int, round_: int | None = None) -> dict:
    """Championship state after a given round, or after the latest round."""
    season = int(season)
    if round_ is None:
        latest = schema.query(
            f"SELECT max(round) AS r FROM {CHAMPIONSHIP} WHERE season = %s", (season,)
        )
        round_ = latest[0]["r"] if latest and latest[0]["r"] is not None else None
        if round_ is None:
            raise UnknownRaceError(f"No championship data for {season}")
    round_ = int(round_)

    rows = schema.query(
        f"""SELECT championship_position, driver_id, driver_name,
                   constructor_name_as_of_race, cumulative_points, cumulative_wins,
                   gap_to_leader, points_gained_in_round, position_change_vs_prev_round
            FROM {CHAMPIONSHIP}
            WHERE season = %s AND round = %s
            ORDER BY championship_position""",
        (season, round_),
    )
    if not rows:
        raise UnknownRaceError(f"No standings for {season} round {round_}")
    return {
        "season": season,
        "round": round_,
        "standings": rows,
        "leader": rows[0]["driver_name"] if rows else None,
    }


def race_weather(season: int, round_or_name) -> dict:
    """Measured race-day weather for one race."""
    race = resolve_race(season, round_or_name)
    rows = schema.query(
        f"""SELECT season, round, race_name, race_date, circuit_name, conditions,
                   temp_max, temp_min, temp_mean, precipitation_mm, rain_mm,
                   wind_speed_max, wind_gusts_max, was_wet, wet_threshold_mm, source
            FROM {schema.WEATHER} WHERE season = %s AND round = %s""",
        (race["season"], race["round"]),
    )
    if not rows:
        return {
            "resolved_race": race["race_name"],
            "season": race["season"],
            "round": race["round"],
            "weather_available": False,
            "note": "No weather observation for this race. The archive lags "
            "about five days, so very recent races have none yet. "
            "This means NO DATA, not fair weather.",
        }
    observation = dict(rows[0])
    observation["resolved_race"] = race["race_name"]
    observation["weather_available"] = True
    return observation


def wet_races(season: int | None = None, limit: int = 10) -> dict:
    """Races where measured rainfall crossed the wet threshold."""
    limit = max(1, min(int(limit), MAX_RESULTS))
    where, params = "WHERE was_wet", []
    if season:
        where += " AND season = %s"
        params.append(int(season))
    params.append(limit)
    rows = schema.query(
        f"""SELECT season, round, race_name, race_date, circuit_name, conditions,
                   precipitation_mm, temp_max, wind_gusts_max, wet_threshold_mm
            FROM {schema.WEATHER} {where}
            ORDER BY precipitation_mm DESC LIMIT %s""",
        tuple(params),
    )
    return {
        "season": int(season) if season else "all",
        "wet_race_count": len(rows),
        "threshold_mm": rows[0]["wet_threshold_mm"] if rows else 1.0,
        "races": rows,
        "note": "Wetness is measured rainfall from the weather archive, not a "
        "description taken from the race report.",
    }


def search_reports(
    query: str, top_k: int = 5, season: int | None = None, round_: int | None = None
) -> dict:
    """Semantic search over race-report prose, joined to measured weather."""
    if not isinstance(query, str) or not query.strip():
        raise ValueError("query must be a non-empty string")
    # Floor of 3, not 1. A model asking for a single passage gets a coin flip:
    # one run answered the Monza question correctly and the next hedged, because
    # top_k=1 drew a passage from the wrong race. Retrieval quality should not
    # depend on the model guessing a good k, so the floor is enforced here
    # rather than left to the prompt.
    top_k = max(3, min(int(top_k), MAX_RESULTS))

    vector = embedder.to_pgvector(embedder.embed_query(query.strip()))
    clauses, params = [], [vector]
    if season:
        clauses.append("e.season = %s")
        params.append(int(season))
    if round_ is not None:
        clauses.append("e.round = %s")
        params.append(int(round_))
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params += [vector, top_k]

    # The filter is composed here rather than as a `WHERE (%s IS NULL OR ...)`
    # catch-all: that form is opaque to the planner and makes it order the HNSW
    # scan over rows it will then discard.
    rows = schema.query(
        f"""SELECT e.season, e.round, d.race_name, d.race_date, e.section,
                   d.url, e.chunk_text,
                   1 - (e.embedding <=> %s::vector) AS similarity,
                   w.was_wet, w.precipitation_mm, w.conditions
            FROM {schema.EMBEDDINGS} e
            JOIN {schema.DOCUMENTS} d ON d.id = e.document_id
            LEFT JOIN {schema.WEATHER} w ON w.season = e.season AND w.round = e.round
            {where}
            ORDER BY e.embedding <=> %s::vector
            LIMIT %s""",
        tuple(params),
    )
    return {"query": query.strip(), "top_k": top_k, "matches": len(rows), "results": rows}


def _has_results(season: int, rnd: int) -> bool:
    """Whether a round has been run. A scheduled round exists in f1_races long
    before anyone drives it, so presence in the spine proves nothing."""
    return bool(
        schema.query(
            f"SELECT 1 FROM {DRIVERS} WHERE season = %s AND round = %s LIMIT 1",
            (int(season), int(rnd)),
        )
    )


def season_schedule(season: int) -> dict:
    """Every round of a season in order, with winner and measured rainfall.

    Exists because the per-race tools all need a race identified up front, which
    leaves relative references - "the next race", "the round before that" -
    unanswerable. Without this the model had no way to turn "next race" into a
    round number, and would either pass the phrase through as a race name or
    guess. A cheap ordered list is what makes those follow-ups answerable.
    """
    rounds = schema.query(
        f"""
        SELECT r.season, r.round, r.race_name, r.race_date, r.circuit_name,
               r.circuit_country,
               (SELECT p.driver_name FROM {DRIVERS} p
                 WHERE p.season = r.season AND p.round = r.round
                   AND p.finish_position = 1 LIMIT 1) AS winner,
               w.precipitation_mm, w.conditions, w.was_wet
        FROM {schema.RACES} r
        LEFT JOIN {schema.WEATHER} w ON w.season = r.season AND w.round = r.round
        WHERE r.season = %s
        ORDER BY r.round""",
        (int(season),),
    )
    if not rounds:
        raise ValueError(f"No races found for season {season}.")
    # Flag which rounds have actually been run, so the model does not report a
    # scheduled-but-unraced round as though it had a result.
    completed = [r for r in rounds if r["winner"]]
    return {
        "season": int(season),
        "rounds": len(rounds),
        "completed": len(completed),
        "schedule": rounds,
    }


def race_strategy(season: int, round_or_name) -> dict:
    """Pit stops and reconstructed stints for one race, per driver.

    This is what separates "who won" from "how they won". A driver on one stint
    while a rival ran four means the result was decided in the pit lane.
    """
    race = resolve_race(season, round_or_name)
    drivers = schema.query(
        f"""
        SELECT s.driver_id,
               max(p.driver_name)                       AS driver_name,
               max(p.constructor_name_as_of_race)       AS constructor,
               max(p.finish_position)                   AS finish_position,
               max(p.grid_position)                     AS grid_position,
               count(*)                                 AS stints,
               count(*) - 1                             AS stops
        FROM {schema.STINTS} s
        LEFT JOIN {DRIVERS} p
               ON p.season = s.season AND p.round = s.round AND p.driver_id = s.driver_id
        WHERE s.season = %s AND s.round = %s
        GROUP BY s.driver_id
        ORDER BY min(COALESCE(p.finish_position, 99))""",
        (race["season"], race["round"]),
    )

    stops = schema.query(
        f"""
        SELECT driver_id, stop_number, lap, duration_s
        FROM {schema.PIT_STOPS}
        WHERE season = %s AND round = %s AND duration_s IS NOT NULL
        ORDER BY duration_s DESC LIMIT 5""",
        (race["season"], race["round"]),
    )

    if not drivers:
        # Distinguish "we have no strategy data" from "this race has not been
        # run". Returning empty fields for both left the model searching the
        # report corpus for a result that cannot exist yet, and answering "not
        # specified in the search results" - which reads as a retrieval failure
        # rather than the truth, that the race is still in the future.
        if not _has_results(race["season"], race["round"]):
            return {
                "resolved_race": race["race_name"],
                "season": race["season"],
                "round": race["round"],
                "status": "not_yet_raced",
                "race_date": str(race.get("race_date")),
                "message": (
                    f"The {race['season']} {race['race_name']} is round "
                    f"{race['round']}, scheduled for {race.get('race_date')}, "
                    "and has not been raced yet. There is no result, no "
                    "strategy and no race report. Do not search for one."
                ),
            }

    counts = [int(d["stints"]) for d in drivers if d["stints"]]
    return {
        "resolved_race": race["race_name"],
        "season": race["season"],
        "round": race["round"],
        "drivers_analysed": len(drivers),
        "stint_spread": (f"{min(counts)}-{max(counts)}" if counts else None),
        "most_common_strategy": (
            f"{max(set(counts), key=counts.count)} stint(s)" if counts else None
        ),
        "slowest_stops": stops,
        "by_driver": drivers,
        "note": "Stints are derived from pit-stop laps. Tyre compounds are not "
        "available from this data source - the race report's 'Tyre "
        "choices' section covers those.",
    }


def strategy_spread(season: int, limit: int = 8) -> dict:
    """Races where teams disagreed most about strategy.

    Ranked by the gap between the fewest and most stints any driver ran. A wide
    spread means the field genuinely split on approach, which is where the
    interesting decisions are - far better than ranking by rainfall.
    """
    limit = max(1, min(int(limit), MAX_RESULTS))
    rows = schema.query(
        f"""
        SELECT s.season, s.round, r.race_name, w.was_wet, w.precipitation_mm,
               min(t.stints) AS min_stints, max(t.stints) AS max_stints,
               round(avg(t.stints)::numeric, 2) AS avg_stints,
               max(t.stints) - min(t.stints) AS spread
        FROM (SELECT season, round, driver_id, count(*) AS stints
                FROM {schema.STINTS} GROUP BY 1,2,3) t
        JOIN {schema.STINTS} s
          ON s.season = t.season AND s.round = t.round AND s.driver_id = t.driver_id
        JOIN {schema.RACES} r ON r.season = s.season AND r.round = s.round
        LEFT JOIN {schema.WEATHER} w ON w.season = s.season AND w.round = s.round
        WHERE s.season = %s
        GROUP BY s.season, s.round, r.race_name, w.was_wet, w.precipitation_mm
        ORDER BY spread DESC, s.round LIMIT %s""",
        (int(season), limit),
    )
    return {
        "season": int(season),
        "races": rows,
        "note": "Spread is the gap between the fewest and most stints any "
        "driver ran. A wide spread means the field split on strategy.",
    }


def race_pace(season: int, round_or_name) -> dict:
    """Race pace per driver, independent of finishing position.

    Sourced from Gold's lap_pace mart: median clean-lap time, best lap,
    consistency and laps led, each expressed relative to the fastest median in
    the race. This is what separates "who was actually fast" from "who
    finished ahead" — a winner who controlled the race from the front is often
    not the quickest car on the day.
    """
    race = resolve_race(season, round_or_name)
    rows = schema.query(
        f"""
        SELECT driver_id, driver_name, constructor_name_as_of_race,
               laps_recorded, clean_laps, laps_led, median_clean_lap_s,
               best_lap_s, consistency_s, reference_median_s, pace_deficit_pct,
               grid_position, finish_position, positions_gained, dnf_flag,
               pace_summary
        FROM {schema.LAP_PACE}
        WHERE season = %s AND round = %s
        ORDER BY median_clean_lap_s""",
        (race["season"], race["round"]),
    )
    if not rows:
        if not _has_results(race["season"], race["round"]):
            return {
                "resolved_race": race["race_name"],
                "season": race["season"],
                "round": race["round"],
                "status": "not_yet_raced",
                "race_date": str(race.get("race_date")),
                "message": (
                    f"The {race['season']} {race['race_name']} is round "
                    f"{race['round']}, scheduled for {race.get('race_date')}, "
                    "and has not been raced yet. There is no lap-time data."
                ),
            }
        return {
            "resolved_race": race["race_name"],
            "season": race["season"],
            "round": race["round"],
            "drivers_analysed": 0,
            "note": "No lap-time data recorded for this race.",
        }
    return {
        "resolved_race": race["race_name"],
        "season": race["season"],
        "round": race["round"],
        "drivers_analysed": len(rows),
        "fastest_on_the_day": rows[0]["driver_name"],
        "by_driver": rows,
        "note": "Pace is measured on clean laps only - within 107% of each "
        "driver's own best in that race - so traffic, pit cycles and "
        "safety-car laps do not distort it. pace_deficit_pct is "
        "relative to the fastest median in the race, not the winner's.",
    }


def constructor_standings(season: int, round_: int | None = None) -> dict:
    """Constructors' championship state after a given round, or the latest.

    The team-level mirror of championship_standings. Read from the standings
    endpoint rather than summed from results — reconciled exactly against the
    driver-level total in sql/validation_checks.sql.
    """
    season = int(season)
    if round_ is None:
        latest = schema.query(
            f"SELECT max(round) AS r FROM {schema.CONSTRUCTOR_STANDINGS} WHERE season = %s",
            (season,),
        )
        round_ = latest[0]["r"] if latest and latest[0]["r"] is not None else None
        if round_ is None:
            raise UnknownRaceError(f"No constructor standings for {season}")
    round_ = int(round_)

    rows = schema.query(
        f"""
        SELECT championship_position, constructor_id, constructor_name,
               constructor_nationality, cumulative_points, cumulative_wins,
               gap_to_leader, points_gained_in_round, position_change_vs_prev_round
        FROM {schema.CONSTRUCTOR_STANDINGS}
        WHERE season = %s AND round = %s
        ORDER BY championship_position""",
        (season, round_),
    )
    if not rows:
        raise UnknownRaceError(f"No constructor standings for {season} round {round_}")
    return {
        "season": season,
        "round": round_,
        "standings": rows,
        "leader": rows[0]["constructor_name"] if rows else None,
    }


# --------------------------------------------------------------------------
# Writes — the agent takes real actions, not just reads
# --------------------------------------------------------------------------


def add_watchlist(
    entity_type: str, entity_ref: str, note: str | None = None, user_id: str = "default"
) -> dict:
    """Track a driver, constructor or circuit. Idempotent."""
    entity_type = str(entity_type).strip().lower()
    if entity_type not in ("driver", "constructor", "circuit"):
        raise ValueError("entity_type must be 'driver', 'constructor' or 'circuit'")
    if not str(entity_ref).strip():
        raise ValueError("entity_ref must be a non-empty string")

    resolved = str(entity_ref).strip()
    if entity_type == "driver":
        # Resolve before storing so the watchlist holds a real driver rather
        # than whatever the user typed.
        resolved = resolve_driver(resolved)["driver_name"]

    rows = schema.returning(
        f"""INSERT INTO {schema.WATCHLIST} (user_id, entity_type, entity_ref, note)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_id, entity_type, entity_ref)
            DO UPDATE SET note = COALESCE(EXCLUDED.note, {schema.WATCHLIST}.note)
            RETURNING id, entity_type, entity_ref, note, added_at""",
        (user_id, entity_type, resolved, note),
    )
    return {"written": True, "action": "add_to_watchlist", "row": rows[0]}


def get_watchlist(user_id: str = "default") -> dict:
    rows = schema.query(
        f"""SELECT id, entity_type, entity_ref, note, added_at
            FROM {schema.WATCHLIST} WHERE user_id = %s
            ORDER BY added_at DESC""",
        (user_id,),
    )
    return {"user_id": user_id, "count": len(rows), "items": rows}


def log_prediction(
    season: int,
    round_or_name,
    prediction: str,
    confidence: str = "medium",
    rationale: str | None = None,
    user_id: str = "default",
) -> dict:
    """Record a prediction against a specific race."""
    if not str(prediction).strip():
        raise ValueError("prediction must be a non-empty string")
    confidence = str(confidence).strip().lower()
    if confidence not in ("low", "medium", "high"):
        raise ValueError("confidence must be 'low', 'medium' or 'high'")

    race = resolve_race(season, round_or_name)
    rows = schema.returning(
        f"""INSERT INTO {schema.PREDICTIONS}
                (user_id, season, round, prediction, confidence, rationale)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id, season, round, prediction, confidence, rationale, created_at""",
        (user_id, race["season"], race["round"], str(prediction).strip(), confidence, rationale),
    )
    row = dict(rows[0])
    row["race_name"] = race["race_name"]
    return {"written": True, "action": "log_prediction", "row": row}


def save_note(season: int, round_or_name, note: str, user_id: str = "default") -> dict:
    """Save a free-text analyst note against a race."""
    if not str(note).strip():
        raise ValueError("note must be a non-empty string")
    race = resolve_race(season, round_or_name)
    rows = schema.returning(
        f"""INSERT INTO {schema.NOTES} (user_id, season, round, note)
            VALUES (%s, %s, %s, %s)
            RETURNING id, season, round, note, created_at""",
        (user_id, race["season"], race["round"], str(note).strip()),
    )
    row = dict(rows[0])
    row["race_name"] = race["race_name"]
    return {"written": True, "action": "save_race_note", "row": row}


def get_notes(
    season: int | None = None, round_: int | None = None, user_id: str = "default", limit: int = 20
) -> dict:
    # Every column must be table-qualified. Both f1_race_notes and f1_races
    # carry `season` and `round`, so an unqualified filter raises
    # "column reference is ambiguous" the moment a season is supplied - which
    # is exactly what the agent does when asked for one race's notes.
    clauses, params = ["n.user_id = %s"], [user_id]
    if season:
        clauses.append("n.season = %s")
        params.append(int(season))
    if round_ is not None:
        clauses.append("n.round = %s")
        params.append(int(round_))
    params.append(max(1, min(int(limit), MAX_RESULTS)))
    rows = schema.query(
        f"""SELECT n.id, n.season, n.round, r.race_name, n.note, n.created_at
            FROM {schema.NOTES} n
            LEFT JOIN {schema.RACES} r ON r.season = n.season AND r.round = n.round
            WHERE {" AND ".join(clauses)}
            ORDER BY n.created_at DESC LIMIT %s""",
        tuple(params),
    )
    return {"count": len(rows), "notes": rows}


def get_predictions(season: int | None = None, user_id: str = "default", limit: int = 20) -> dict:
    clauses, params = ["p.user_id = %s"], [user_id]
    if season:
        clauses.append("p.season = %s")
        params.append(int(season))
    params.append(max(1, min(int(limit), MAX_RESULTS)))
    rows = schema.query(
        f"""SELECT p.id, p.season, p.round, r.race_name, p.prediction,
                   p.confidence, p.rationale, p.created_at
            FROM {schema.PREDICTIONS} p
            LEFT JOIN {schema.RACES} r ON r.season = p.season AND r.round = p.round
            WHERE {" AND ".join(clauses)}
            ORDER BY p.created_at DESC LIMIT %s""",
        tuple(params),
    )
    return {"count": len(rows), "predictions": rows}


# --------------------------------------------------------------------------
# Deletes — same write path as add/log/save, callable from chat or the UI
# --------------------------------------------------------------------------
#
# The UI's delete buttons call these directly (see app.py's /api/*/delete
# routes), bypassing the LLM - deletion is a deterministic "remove row N"
# operation, not something that benefits from being phrased as a sentence and
# resolved by a model. The same functions are also exposed as MCP/agent tools
# below, so "remove Verstappen from my watchlist" still works from chat and
# still shows up in the trace and in the Change Data Feed loop like any other
# write. One implementation, two ways to trigger it - the project's existing
# pattern for everything else in this module.
#
# WHERE id = %s AND user_id = %s together, always: user_id scopes every delete
# to its owner, so one user can never delete another's row even if they guess
# an id. RETURNING id is how a "not found" is distinguished from "deleted".


def remove_from_watchlist(item_id: int, user_id: str = "default") -> dict:
    rows = schema.returning(
        f"""DELETE FROM {schema.WATCHLIST} WHERE id = %s AND user_id = %s
            RETURNING id, entity_type, entity_ref""",
        (int(item_id), user_id),
    )
    if not rows:
        raise ValueError(f"No watchlist entry {item_id} for this user")
    return {"written": True, "action": "remove_from_watchlist", "row": rows[0]}


def delete_prediction(item_id: int, user_id: str = "default") -> dict:
    rows = schema.returning(
        f"""DELETE FROM {schema.PREDICTIONS} WHERE id = %s AND user_id = %s
            RETURNING id, season, round, prediction""",
        (int(item_id), user_id),
    )
    if not rows:
        raise ValueError(f"No prediction {item_id} for this user")
    return {"written": True, "action": "delete_prediction", "row": rows[0]}


def delete_note(item_id: int, user_id: str = "default") -> dict:
    rows = schema.returning(
        f"""DELETE FROM {schema.NOTES} WHERE id = %s AND user_id = %s
            RETURNING id, season, round, note""",
        (int(item_id), user_id),
    )
    if not rows:
        raise ValueError(f"No note {item_id} for this user")
    return {"written": True, "action": "delete_note", "row": rows[0]}
