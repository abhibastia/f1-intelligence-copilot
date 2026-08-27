"""
Queries backing the frontend.

Read-only. Every write in this project goes through the agent's MCP tools -
that is the point of the write tools, and a UI that also wrote directly would
make it impossible to demonstrate that the agent, specifically, took an action.
The watchlist, predictions and notes shown here were all written by the agent.

Like the MCP server, everything here reads Lakebase and nothing reads Delta, so
rendering a page costs no Databricks compute.
"""

import logging

from f1lake import embedder, schema

logger = logging.getLogger("ui-data")

# The four races that make the project's central point: near-identical daily
# rainfall, opposite race outcomes. Pinned rather than computed so the panel
# always tells the same story, with the numbers themselves read live.
THESIS_RACES = [(2024, 16), (2025, 3), (2024, 21), (2025, 1)]


def corpus_stats() -> dict:
    return schema.query("""
        SELECT (SELECT count(*) FROM f1_races)               AS races,
               (SELECT count(*) FROM f1_documents)           AS documents,
               (SELECT count(*) FROM f1_embeddings)          AS embeddings,
               (SELECT count(*) FROM f1_race_weather)        AS weather,
               (SELECT count(*) FROM f1_race_weather WHERE was_wet) AS wet_races,
               (SELECT count(*) FROM f1_driver_performance)  AS results,
               (SELECT count(DISTINCT season) FROM f1_races) AS seasons
    """)[0]


def rain_vs_chaos() -> list[dict]:
    """Aggregate race outcomes at graduated rainfall thresholds.

    The headline finding is that this barely moves: DNF rate climbs only from
    12.5% to 15.7% between "any race" and "15 mm or more". Daily rainfall is a
    weak predictor of a chaotic race, which is the observation the whole
    narrative track exists to explain.
    """
    out = []
    for threshold in (0.0, 1.0, 5.0, 10.0, 15.0):
        row = schema.query("""
            SELECT count(DISTINCT (p.season, p.round)) AS races,
                   round(avg(CASE WHEN lower(p.dnf_flag::text) IN ('true','1')
                                  THEN 1.0 ELSE 0.0 END) * 100, 1) AS dnf_pct,
                   round(avg(ABS(NULLIF(p.positions_gained, 0)))::numeric, 2) AS pos_change
            FROM f1_driver_performance p
            JOIN f1_race_weather w ON w.season = p.season AND w.round = p.round
            WHERE w.precipitation_mm >= %s
        """, (threshold,))[0]
        out.append({"threshold": threshold, **row})
    return out


def thesis_races() -> list[dict]:
    """The four races that show daily rainfall failing as a proxy."""
    rows = []
    for season, rnd in THESIS_RACES:
        base = schema.query("""
            SELECT w.season, w.round, w.race_name, w.precipitation_mm, w.conditions,
                   round(avg(CASE WHEN lower(p.dnf_flag::text) IN ('true','1')
                                  THEN 1.0 ELSE 0.0 END) * 100, 1) AS dnf_pct,
                   round(avg(ABS(NULLIF(p.positions_gained, 0)))::numeric, 2) AS pos_change
            FROM f1_race_weather w
            JOIN f1_driver_performance p ON w.season = p.season AND w.round = p.round
            WHERE w.season = %s AND w.round = %s
            GROUP BY 1,2,3,4,5
        """, (season, rnd))
        if not base:
            continue
        row = dict(base[0])

        # Pull the sentence from the race report that mentions conditions. This
        # is the evidence that separates "it rained that day" from "it rained
        # during the race" - the distinction the rainfall column cannot make.
        quote = schema.query("""
            SELECT chunk_text FROM f1_embeddings
            WHERE season = %s AND round = %s
              AND (chunk_text ILIKE '%%wet track%%' OR chunk_text ILIKE '%%rainy condition%%'
                   OR chunk_text ILIKE '%%intermediate%%' OR chunk_text ILIKE '%%chance of rain%%')
            LIMIT 1
        """, (season, rnd))
        row["evidence"] = _sentence(quote[0]["chunk_text"]) if quote else None
        rows.append(row)
    return sorted(rows, key=lambda r: float(r["dnf_pct"] or 0))


def _sentence(text: str) -> str:
    """Extract the sentence mentioning conditions, so the panel quotes prose
    rather than dumping a whole chunk."""
    import re
    for m in re.finditer(r"[^.]*\b(wet track|rainy condition\w*|intermediate|chance of rain)\b[^.]*\.",
                         text, re.IGNORECASE):
        s = m.group(0).strip()
        if len(s) > 30:
            return (s[:240] + "…") if len(s) > 240 else s
    return (text[:200] + "…") if len(text) > 200 else text


def seasons() -> list[int]:
    return [r["season"] for r in schema.query(
        "SELECT DISTINCT season FROM f1_races ORDER BY season DESC")]


def season_races(season: int) -> list[dict]:
    """Every race in a season with its weather and the winner."""
    return schema.query("""
        SELECT r.season, r.round, r.race_name, r.race_date, r.circuit_name,
               r.circuit_country,
               w.conditions, w.precipitation_mm, w.temp_max, w.was_wet,
               (SELECT p.driver_name FROM f1_driver_performance p
                 WHERE p.season = r.season AND p.round = r.round
                   AND p.finish_position = 1 LIMIT 1) AS winner,
               (SELECT count(*) FROM f1_documents d
                 WHERE d.season = r.season AND d.round = r.round) AS report_sections
        FROM f1_races r
        LEFT JOIN f1_race_weather w ON w.season = r.season AND w.round = r.round
        WHERE r.season = %s
        ORDER BY r.round
    """, (int(season),))


def standings(season: int, limit: int = 10) -> list[dict]:
    """Final (latest available) championship standings for a season."""
    return schema.query("""
        SELECT championship_position, driver_name, constructor_name_as_of_race,
               cumulative_points, cumulative_wins
        FROM f1_championship
        WHERE season = %s AND round = (SELECT max(round) FROM f1_championship WHERE season = %s)
        ORDER BY championship_position LIMIT %s
    """, (int(season), int(season), int(limit)))


def search(query: str, top_k: int = 6, season: int | None = None) -> list[dict]:
    """Semantic search over race reports, each hit carrying its race's weather."""
    vector = embedder.to_pgvector(embedder.embed_query(query.strip()))
    params: list = [vector]
    where = ""
    if season:
        where = "WHERE e.season = %s"
        params.append(int(season))
    params += [vector, max(1, min(int(top_k), 20))]
    return schema.query(f"""
        SELECT e.season, e.round, d.race_name, d.race_date, e.section, d.url,
               e.chunk_text,
               1 - (e.embedding <=> %s::vector) AS similarity,
               w.was_wet, w.precipitation_mm, w.conditions
        FROM f1_embeddings e
        JOIN f1_documents d ON d.id = e.document_id
        LEFT JOIN f1_race_weather w ON w.season = e.season AND w.round = e.round
        {where}
        ORDER BY e.embedding <=> %s::vector
        LIMIT %s
    """, tuple(params))


def agent_analytics() -> dict:
    """Per-tool analytics materialised from the Change Data Feed.

    Read from Lakebase rather than from the Delta table the CDF job writes:
    querying Delta per page render would spend a SQL warehouse query every time
    someone loads the page, which on Free Edition's daily quota is exactly the
    behaviour that kills a demo. The Delta table is the analytical record; this
    aggregate mirrors it for serving.

    `cdf_materialised` reports whether the CDF job has run, so the page can say
    "not yet materialised" instead of implying the loop does not exist.
    """
    tools = schema.query(f"""
        SELECT tool_name,
               count(*)                                              AS call_count,
               count(*) FILTER (WHERE is_write)                      AS write_count,
               count(*) FILTER (WHERE outcome <> 'ok')               AS error_count,
               round(avg(duration_ms))                               AS avg_duration_ms,
               count(DISTINCT session_id)                            AS sessions,
               max(called_at)                                        AS last_called_at
        FROM {schema.TOOL_CALLS}
        WHERE tool_name NOT LIKE 'test\\_%%' ESCAPE '\\'
        GROUP BY tool_name ORDER BY call_count DESC""")
    totals = schema.query(f"""
        SELECT count(*) AS calls,
               count(*) FILTER (WHERE is_write)        AS writes,
               count(*) FILTER (WHERE outcome <> 'ok') AS errors,
               count(DISTINCT session_id)              AS sessions
        FROM {schema.TOOL_CALLS}
        WHERE tool_name NOT LIKE 'test\\_%%' ESCAPE '\\'""")[0]
    return {"tools": tools, "totals": totals}


def recent_sessions(limit: int = 6) -> list[dict]:
    """The last few conversations, each with the calls it made in order.

    The per-tool aggregate above says `get_race_weather` was called 31 times at
    an average of 240 ms. True, and impossible to picture. What a reader wants
    to know is what one conversation actually did: which tools, in what order,
    how long each took and whether it worked. That is the same Change Data Feed
    data at the grain it is legible at.

    Grouped by session rather than listed flat because the ordering within a
    conversation is the interesting part - a weather call followed by two
    searches is a visible reasoning path, where the same three rows shuffled
    together with other sessions' calls are noise.
    """
    rows = schema.query(f"""
        WITH recent AS (
            SELECT session_id,
                   max(called_at)                          AS last_at,
                   count(*)                                AS calls,
                   sum(duration_ms)                        AS total_ms,
                   count(*) FILTER (WHERE is_write)        AS writes,
                   count(*) FILTER (WHERE outcome <> 'ok') AS errors
            FROM {schema.TOOL_CALLS}
            WHERE tool_name NOT LIKE 'test\\_%%' ESCAPE '\\'
            GROUP BY session_id
            ORDER BY max(called_at) DESC
            LIMIT %s
        )
        SELECT c.session_id, c.called_at, c.tool_name, c.is_write, c.outcome,
               c.summary, c.duration_ms,
               r.last_at, r.calls, r.total_ms, r.writes, r.errors
        FROM {schema.TOOL_CALLS} c
        JOIN recent r ON r.session_id = c.session_id
        WHERE c.tool_name NOT LIKE 'test\\_%%' ESCAPE '\\'
        ORDER BY r.last_at DESC, c.id""", (max(1, min(int(limit), 20)),))

    sessions: list[dict] = []
    for row in rows:
        if not sessions or sessions[-1]["session_id"] != row["session_id"]:
            sessions.append({
                "session_id": row["session_id"],
                "last_at": row["last_at"], "calls": row["calls"],
                "total_ms": row["total_ms"], "writes": row["writes"],
                "errors": row["errors"], "steps": [],
            })
        sessions[-1]["steps"].append({
            "tool_name": row["tool_name"], "is_write": row["is_write"],
            "outcome": row["outcome"], "summary": row["summary"],
            "duration_ms": row["duration_ms"],
        })
    return sessions


def strategy_races(season: int, limit: int = 12) -> list[dict]:
    """Races ranked by how much strategy varied — where stop counts differed
    most between drivers, which is where the interesting decisions were."""
    # Aggregate stints PER DRIVER, not stint_number.
    #
    # The first version used min/max(stint_number) - the ordinal within one
    # driver's race - so the minimum was always 1 and every race displayed a
    # spread of "1-5". A column identical on every row is worse than no column:
    # it looks like data and carries none. What matters is how many stints each
    # driver ran, so the counts are computed first and then compared.
    return schema.query("""
        WITH per_driver AS (
            SELECT season, round, driver_id, count(*) AS stints
            FROM f1_stints GROUP BY 1, 2, 3
        )
        SELECT s.season, s.round, r.race_name, w.was_wet, w.precipitation_mm,
               count(*)                                          AS drivers,
               round(avg(s.stints)::numeric, 2)                   AS avg_stints,
               max(s.stints)                                      AS max_stints,
               min(s.stints)                                      AS min_stints,
               (SELECT round(avg(p.duration_s)::numeric, 2) FROM f1_pit_stops p
                 WHERE p.season = s.season AND p.round = s.round
                   AND p.duration_s IS NOT NULL AND p.duration_s < 120) AS avg_stop_s
        FROM per_driver s
        JOIN f1_races r ON r.season = s.season AND r.round = s.round
        LEFT JOIN f1_race_weather w ON w.season = s.season AND w.round = s.round
        WHERE s.season = %s
        GROUP BY 1,2,3,4,5
        ORDER BY (max(s.stints) - min(s.stints)) DESC, s.round
        LIMIT %s""", (int(season), int(limit)))


def agent_activity() -> dict:
    """Everything the agent has written. Proof that the write tools work."""
    return {
        "watchlist": schema.query("""
            SELECT entity_type, entity_ref, note, added_at
            FROM f1_watchlist ORDER BY added_at DESC LIMIT 20"""),
        "predictions": schema.query("""
            SELECT p.season, p.round, r.race_name, p.prediction, p.confidence,
                   p.rationale, p.created_at
            FROM f1_predictions p
            LEFT JOIN f1_races r ON r.season = p.season AND r.round = p.round
            ORDER BY p.created_at DESC LIMIT 20"""),
        "notes": schema.query("""
            SELECT n.season, n.round, r.race_name, n.note, n.created_at
            FROM f1_race_notes n
            LEFT JOIN f1_races r ON r.season = n.season AND r.round = n.round
            ORDER BY n.created_at DESC LIMIT 20"""),
    }
