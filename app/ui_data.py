"""
Queries backing the frontend.

Mostly read-only. Everything the assistant can create (watchlist entries,
predictions, notes) is still created only through chat, going through the
agent's MCP tools - that's what makes it demonstrable that the agent,
specifically, took the action. Removing one of those rows is the one direct
write the UI makes on its own (see app.py's /api/*/delete routes): deletion is
a deterministic "remove row N" operation with no upside to routing through an
LLM, and it calls the exact same f1_broker functions the matching agent tools
(remove_from_watchlist, delete_prediction, delete_note) call - one
implementation, reachable from chat or from a button.

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
    """Everything the agent has written. Proof that the write tools work.

    `id` is selected on every row so the UI's delete buttons can target one
    row - DELETE ... WHERE id = %s AND user_id = %s in f1_broker, the same
    write path chat-driven removal uses (see app.py's /api/*/delete routes).
    """
    return {
        "watchlist": schema.query("""
            SELECT id, entity_type, entity_ref, note, added_at
            FROM f1_watchlist ORDER BY added_at DESC LIMIT 20"""),
        "predictions": schema.query("""
            SELECT p.id, p.season, p.round, r.race_name, p.prediction, p.confidence,
                   p.rationale, p.created_at
            FROM f1_predictions p
            LEFT JOIN f1_races r ON r.season = p.season AND r.round = p.round
            ORDER BY p.created_at DESC LIMIT 20"""),
        "notes": schema.query("""
            SELECT n.id, n.season, n.round, r.race_name, n.note, n.created_at
            FROM f1_race_notes n
            LEFT JOIN f1_races r ON r.season = n.season AND r.round = n.round
            ORDER BY n.created_at DESC LIMIT 20"""),
    }
