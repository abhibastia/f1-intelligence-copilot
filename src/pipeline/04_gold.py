"""Gold — business-ready marts.

Both marts join facts to the SCD-2 driver dimension **as of the race date**, so
a 2024 result for a driver who changed teams mid-season is attributed to the
team they actually drove for that weekend, not to their current one. That
as-of join is the reason the SCD-2 dimension exists; a current-row-only join
would silently rewrite history.

Dimensions are kept wide on purpose: every filter the dashboard offers has to
exist as a column here, and it is far easier to aggregate further in a query
than to recover a dimension that was aggregated away.
"""

from pyspark import pipelines as dp

# Catalog comes from the pipeline `configuration` block. No default on
# purpose: a fallback of "f1" means a prod pipeline whose configuration is
# missing or misspelled writes silently into the dev catalog instead of
# failing, and nothing downstream can tell the difference afterwards.
CATALOG = spark.conf.get("f1.catalog")
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"

# Attach each fact row to the driver version that was current on race day.
# f-string: this fragment is interpolated into the queries below, and a nested
# {SILVER} in a plain string would be injected as literal text, not substituted.
_AS_OF_DRIVER = f"""
    LEFT JOIN {SILVER}.dim_driver d
           ON d.driver_id = f.driver_id
          AND f.race_date >= d.__START_AT
          AND (d.__END_AT IS NULL OR f.race_date < d.__END_AT)
"""


@dp.materialized_view(
    name=f"{GOLD}.driver_performance",
    comment=(
        "Driver performance per race: grid, finish, positions gained, points, "
        "reliability and fastest-lap flags. Grain: driver x race."
    ),
    table_properties={"quality": "gold"},
    cluster_by=["season", "round"],
)
def driver_performance():
    return spark.sql(f"""
        SELECT
            f.season,
            f.round,
            f.race_date,
            f.race_name,
            r.circuit_id,
            r.circuit_name,
            r.circuit_country,
            f.driver_id,
            f.driver_name,
            f.driver_code,
            f.driver_nationality,
            d.constructor_id            AS constructor_id_as_of_race,
            d.constructor_name          AS constructor_name_as_of_race,
            f.constructor_name          AS constructor_name_reported,
            f.grid_position,
            f.position                  AS finish_position,
            f.position_text,
            f.positions_gained,
            f.points                    AS race_points,
            -- Sprint points count toward the championship. Summing race points
            -- alone leaves 13 of 24 drivers short of their official 2024 total.
            COALESCE(sp.sprint_points, 0)               AS sprint_points,
            f.points + COALESCE(sp.sprint_points, 0)    AS total_points,
            sp.sprint_position,
            sp.sprint_grid_position,
            sp.sprint_position IS NOT NULL              AS is_sprint_weekend,
            f.laps_completed,
            f.status,
            f.dnf_flag,
            f.is_fastest_lap,
            f.fastest_lap_rank,
            f.fastest_lap_time,
            q.quali_position,
            q.quali_stage_reached,
            q.q3_millis,
            CASE WHEN q.quali_position IS NOT NULL AND f.position IS NOT NULL
                 THEN q.quali_position - f.position END AS quali_to_finish_delta,
            f.position = 1              AS is_win,
            f.position <= 3             AS is_podium,
            f.points > 0                AS is_points_finish
        FROM {SILVER}.fact_result f
        {_AS_OF_DRIVER}
        LEFT JOIN {SILVER}.dim_race r
               ON r.season = f.season AND r.round = f.round
        LEFT JOIN {SILVER}.fact_qualifying q
               ON q.season = f.season AND q.round = f.round AND q.driver_id = f.driver_id
        LEFT JOIN {SILVER}.fact_sprint_result sp
               ON sp.season = f.season AND sp.round = f.round AND sp.driver_id = f.driver_id
    """)


@dp.materialized_view(
    name=f"{GOLD}.championship_progression",
    comment=(
        "Driver championship state after every round: cumulative points, position, "
        "gap to leader, and round-over-round movement. Grain: driver x round."
    ),
    table_properties={"quality": "gold"},
    cluster_by=["season"],
)
def championship_progression():
    return spark.sql(f"""
        WITH standings AS (
            SELECT
                s.season,
                s.round,
                r.race_date,
                r.race_name,
                r.circuit_country,
                s.driver_id,
                s.driver_name,
                s.championship_position,
                s.cumulative_points,
                s.cumulative_wins,
                MAX(s.cumulative_points) OVER (PARTITION BY s.season, s.round)
                    AS leader_points,
                LAG(s.cumulative_points) OVER (
                    PARTITION BY s.season, s.driver_id ORDER BY s.round
                ) AS prev_cumulative_points,
                LAG(s.championship_position) OVER (
                    PARTITION BY s.season, s.driver_id ORDER BY s.round
                ) AS prev_championship_position
            FROM {SILVER}.fact_driver_standing s
            LEFT JOIN {SILVER}.dim_race r
                   ON r.season = s.season AND r.round = s.round
        )
        SELECT
            f.season,
            f.round,
            f.race_date,
            f.race_name,
            f.circuit_country,
            f.driver_id,
            f.driver_name,
            d.constructor_id            AS constructor_id_as_of_race,
            d.constructor_name          AS constructor_name_as_of_race,
            f.championship_position,
            f.cumulative_points,
            f.cumulative_wins,
            f.leader_points - f.cumulative_points        AS gap_to_leader,
            f.cumulative_points - COALESCE(f.prev_cumulative_points, 0)
                                                         AS points_gained_in_round,
            f.prev_championship_position - f.championship_position
                                                         AS position_change_vs_prev_round,
            f.championship_position = 1                  AS is_championship_leader
        FROM standings f
        {_AS_OF_DRIVER}
    """)


@dp.materialized_view(
    name=f"{GOLD}.race_conditions",
    comment=(
        "Measured weather joined to what actually happened on track. Grain: "
        "race. Answers whether rain made a race chaotic — and, when the answer "
        "is no, gives the numbers to say so."
    ),
    table_properties={"quality": "gold"},
    cluster_by=["season", "round"],
)
def race_conditions():
    """One row per race, whether or not it has weather.

    LEFT JOIN, deliberately. An INNER JOIN would silently drop every race
    inside the ERA5 publication lag, so the mart would quietly cover fewer
    races than the season has and nothing would say why. `weather_available`
    makes the gap a fact you can filter on instead of an absence you have to
    notice.

    The outcome columns are aggregated from the results fact rather than read
    from a standings table, so retirement rate and positions gained are
    computed on the same grain as the weather they sit beside.
    """
    return spark.sql(f"""
        WITH outcomes AS (
            SELECT
                season,
                round,
                COUNT(*)                                          AS drivers_classified,
                SUM(CASE WHEN dnf_flag THEN 1 ELSE 0 END)         AS retirements,
                ROUND(100.0 * AVG(CASE WHEN dnf_flag THEN 1.0 ELSE 0.0 END), 1)
                                                                  AS retirement_rate_pct,
                ROUND(AVG(ABS(positions_gained)), 2)              AS avg_abs_positions_changed,
                MAX(CASE WHEN finish_position = 1 THEN driver_name END)     AS winner,
                MAX(CASE WHEN finish_position = 1 THEN grid_position END)   AS winner_grid_position,
                MAX(CASE WHEN finish_position = 1
                         THEN constructor_name_as_of_race END)              AS winning_constructor
            FROM {GOLD}.driver_performance
            GROUP BY season, round
        )
        SELECT
            r.season,
            r.round,
            r.race_date,
            r.race_name,
            r.circuit_id,
            r.circuit_name,
            r.circuit_country,

            -- Weather. NULL here means no published observation, not fair weather.
            w.season IS NOT NULL          AS weather_available,
            w.precipitation_mm,
            w.rain_mm,
            w.temp_max_c,
            w.temp_min_c,
            w.wind_max_kmh,
            w.conditions,
            w.was_wet,

            -- Outcome on the same grain.
            o.drivers_classified,
            o.retirements,
            o.retirement_rate_pct,
            o.avg_abs_positions_changed,
            o.winner,
            o.winner_grid_position,
            o.winning_constructor,

            -- The comparison the mart exists for. A race can be flagged wet by
            -- rainfall and still run dry: a daily total cannot tell rain that
            -- fell overnight from rain that fell during the race. Naming the
            -- source of the flag keeps that readable downstream.
            CASE
                WHEN w.season IS NULL      THEN 'no observation'
                WHEN w.was_wet             THEN 'flagged wet by rainfall'
                ELSE                            'under the wet threshold'
            END                            AS rainfall_verdict
        FROM {SILVER}.dim_race r
        LEFT JOIN {SILVER}.fact_race_weather w
               ON w.season = r.season AND w.round = r.round
        LEFT JOIN outcomes o
               ON o.season = r.season AND o.round = r.round
        WHERE r.race_date <= current_date()
    """)


@dp.materialized_view(
    name=f"{GOLD}.race_strategy",
    comment=(
        "Pit-stop strategy per driver per race: stops, derived stints, service "
        "times, and how that compared with the rest of the field. Grain: "
        "driver x race. Answers whether a result was decided in the pit lane."
    ),
    table_properties={"quality": "gold"},
    cluster_by=["season", "round"],
)
def race_strategy():
    """Stints are derived, not sourced.

    Nothing publishes stints; they follow from the stops. A driver who stopped
    twice ran three stints, so stints = stops + 1 for anyone who finished. That
    identity is the whole reason pit stops are worth ingesting: it turns a list
    of events into the shape of a race.

    The field context matters as much as the driver's own numbers. "Two stops"
    means nothing alone; "two stops when everyone else made three" is the story.
    field_modal_stops and strategy_vs_field carry that so a dashboard tile does
    not have to re-derive it and risk disagreeing with the next tile.

    Drivers with no stop at all are included with zero: a one-stint race is a
    strategy, and dropping those rows would understate how often it happens.
    """
    return spark.sql(f"""
        WITH per_driver AS (
            SELECT
                season,
                round,
                driver_id,
                COUNT(*)                                            AS stops,
                COUNT(*) + 1                                        AS stints,
                MIN(lap)                                            AS first_stop_lap,
                MAX(lap)                                            AS last_stop_lap,
                -- Only real service stops are timed. A 35-minute red-flag
                -- stoppage is a true duration and a meaningless average.
                ROUND(AVG(CASE WHEN is_service_stop THEN duration_s END), 2)
                                                                    AS avg_service_stop_s,
                ROUND(MIN(CASE WHEN is_service_stop THEN duration_s END), 2)
                                                                    AS fastest_stop_s,
                ROUND(SUM(CASE WHEN is_service_stop THEN duration_s END), 2)
                                                                    AS total_service_time_s,
                SUM(CASE WHEN NOT is_service_stop THEN 1 ELSE 0 END) AS stoppages
            FROM {SILVER}.fact_pit_stop
            GROUP BY season, round, driver_id
        ),
        field AS (
            -- The most common stop count in each race, and the spread. A race
            -- where everyone stopped twice was not decided in the pit lane; a
            -- race spanning one to four stops probably was.
            SELECT
                season,
                round,
                MIN(stops)                                          AS field_min_stops,
                MAX(stops)                                          AS field_max_stops,
                ROUND(AVG(stops), 2)                                AS field_avg_stops,
                MODE(stops)                                         AS field_modal_stops
            FROM per_driver
            GROUP BY season, round
        )
        SELECT
            p.season,
            p.round,
            d.race_date,
            d.race_name,
            d.circuit_id,
            d.circuit_name,
            p.driver_id,
            dp.driver_name,
            dp.constructor_name_as_of_race,

            p.stops,
            p.stints,
            p.first_stop_lap,
            p.last_stop_lap,
            p.avg_service_stop_s,
            p.fastest_stop_s,
            p.total_service_time_s,
            p.stoppages,

            f.field_min_stops,
            f.field_max_stops,
            f.field_avg_stops,
            f.field_modal_stops,
            f.field_max_stops - f.field_min_stops                    AS field_stop_spread,
            CASE
                WHEN p.stops < f.field_modal_stops THEN 'fewer stops than the field'
                WHEN p.stops > f.field_modal_stops THEN 'more stops than the field'
                ELSE 'same as the field'
            END                                                     AS strategy_vs_field,

            dp.grid_position,
            dp.finish_position,
            dp.positions_gained,
            dp.dnf_flag,
            dp.total_points
        FROM per_driver p
        JOIN field f
          ON f.season = p.season AND f.round = p.round
        JOIN {SILVER}.dim_race d
          ON d.season = p.season AND d.round = p.round
        -- LEFT: a driver can appear in the stop feed and be missing from the
        -- results mart (withdrawn after the formation lap). Better a strategy
        -- row with no outcome than a silently dropped stop.
        LEFT JOIN {GOLD}.driver_performance dp
          ON dp.season = p.season AND dp.round = p.round AND dp.driver_id = p.driver_id
    """)


@dp.materialized_view(
    name=f"{GOLD}.lap_pace",
    comment=(
        "Race pace per driver: clean-lap median, best lap, consistency and laps "
        "led, with pace expressed relative to the race winner. Grain: driver x "
        "race. Answers who was actually fast, as distinct from who finished "
        "ahead."
    ),
    table_properties={"quality": "gold"},
    cluster_by=["season", "round"],
)
def lap_pace():
    """Pace measured on clean laps only.

    A raw lap-time average is close to meaningless: an in-lap, an out-lap and
    every lap behind a safety car are all "laps", and a driver who pitted three
    times looks slow for reasons that have nothing to do with speed. Filtering
    to laps within 107% of that driver's own best in that race — the same
    threshold F1 uses for qualifying — removes the traffic and the pit cycles
    while keeping genuine racing laps, including the slow ones.

    The threshold is per driver, not per race, so a backmarker is judged
    against their own pace rather than the leader's.

    Consistency is the standard deviation of those clean laps. Two drivers can
    share a median and be doing very different jobs; the one with the tighter
    spread is the one managing tyres rather than taking lumps out of them.
    """
    return spark.sql(f"""
        WITH driver_best AS (
            SELECT season, round, driver_id, MIN(lap_time_s) AS best_lap_s
            FROM {SILVER}.fact_lap
            GROUP BY season, round, driver_id
        ),
        clean AS (
            SELECT
                l.season,
                l.round,
                l.driver_id,
                l.lap,
                l.lap_time_s,
                l.position
            FROM {SILVER}.fact_lap l
            JOIN driver_best b
              ON b.season = l.season AND b.round = l.round AND b.driver_id = l.driver_id
            WHERE l.lap_time_s <= b.best_lap_s * 1.07
        ),
        per_driver AS (
            SELECT
                c.season,
                c.round,
                c.driver_id,
                COUNT(*)                                          AS clean_laps,
                ROUND(MEDIAN(c.lap_time_s), 3)                    AS median_clean_lap_s,
                ROUND(MIN(c.lap_time_s), 3)                       AS best_lap_s,
                ROUND(STDDEV_SAMP(c.lap_time_s), 3)               AS consistency_s
            FROM clean c
            GROUP BY c.season, c.round, c.driver_id
        ),
        all_laps AS (
            SELECT
                season,
                round,
                driver_id,
                COUNT(*)                                          AS laps_recorded,
                SUM(CASE WHEN position = 1 THEN 1 ELSE 0 END)     AS laps_led
            FROM {SILVER}.fact_lap
            GROUP BY season, round, driver_id
        ),
        -- The reference is the fastest median in the race, not the winner's:
        -- a winner who controlled the race from the front is often not the
        -- quickest car on the day, and calling their pace 0% would hide that.
        reference AS (
            SELECT season, round, MIN(median_clean_lap_s) AS reference_median_s
            FROM per_driver
            GROUP BY season, round
        )
        SELECT
            p.season,
            p.round,
            d.race_date,
            d.race_name,
            d.circuit_id,
            d.circuit_name,
            p.driver_id,
            dp.driver_name,
            dp.constructor_name_as_of_race,

            a.laps_recorded,
            p.clean_laps,
            a.laps_led,
            p.median_clean_lap_s,
            p.best_lap_s,
            p.consistency_s,
            r.reference_median_s,
            ROUND(100.0 * (p.median_clean_lap_s - r.reference_median_s)
                        / r.reference_median_s, 3)                AS pace_deficit_pct,

            dp.grid_position,
            dp.finish_position,
            dp.positions_gained,
            dp.dnf_flag,
            -- The comparison the mart exists for: fast and finished ahead is
            -- unremarkable, fast and finished behind is a story about strategy,
            -- reliability or traffic.
            CASE
                WHEN dp.finish_position IS NULL THEN 'did not classify'
                WHEN p.median_clean_lap_s = r.reference_median_s THEN 'fastest on the day'
                ELSE CONCAT(CAST(ROUND(100.0 * (p.median_clean_lap_s - r.reference_median_s)
                                 / r.reference_median_s, 2) AS STRING), '% off the pace')
            END                                                   AS pace_summary
        FROM per_driver p
        JOIN all_laps a
          ON a.season = p.season AND a.round = p.round AND a.driver_id = p.driver_id
        JOIN reference r
          ON r.season = p.season AND r.round = p.round
        JOIN {SILVER}.dim_race d
          ON d.season = p.season AND d.round = p.round
        LEFT JOIN {GOLD}.driver_performance dp
          ON dp.season = p.season AND dp.round = p.round AND dp.driver_id = p.driver_id
    """)


@dp.materialized_view(
    name=f"{GOLD}.constructor_standings",
    comment=(
        "Constructor championship state after every round: cumulative points, "
        "position, gap to leader, and round-over-round movement. Grain: "
        "constructor x round. The team-level mirror of championship_progression."
    ),
    table_properties={"quality": "gold"},
    cluster_by=["season"],
)
def constructor_standings():
    """Read from the standings endpoint, not summed from results.

    Both routes give the same answer — verified, zero mismatches across every
    team and season — but they are not equally good. Summing driver results
    reproduces the number; reading the published standings *is* the number, and
    keeping the independent endpoint in Gold is what lets the two be reconciled
    against each other at all. Collapse them into one source and the check
    becomes a tautology.

    That reconciliation is check 14 in sql/validation_checks.sql, and it is the
    constructor-side equivalent of check 1: two endpoints that were parsed,
    deduplicated and joined separately agreeing to the point.
    """
    return spark.sql(f"""
        WITH standings AS (
            SELECT
                s.season,
                s.round,
                r.race_date,
                r.race_name,
                r.circuit_country,
                s.constructor_id,
                s.constructor_name,
                s.constructor_nationality,
                s.championship_position,
                s.cumulative_points,
                s.cumulative_wins,
                MAX(s.cumulative_points) OVER (PARTITION BY s.season, s.round)
                    AS leader_points,
                LAG(s.cumulative_points) OVER (
                    PARTITION BY s.season, s.constructor_id ORDER BY s.round
                ) AS prev_cumulative_points,
                LAG(s.championship_position) OVER (
                    PARTITION BY s.season, s.constructor_id ORDER BY s.round
                ) AS prev_championship_position
            FROM {SILVER}.fact_constructor_standing s
            LEFT JOIN {SILVER}.dim_race r
                   ON r.season = s.season AND r.round = s.round
        )
        SELECT
            season,
            round,
            race_date,
            race_name,
            circuit_country,
            constructor_id,
            constructor_name,
            constructor_nationality,
            championship_position,
            cumulative_points,
            cumulative_wins,
            leader_points - cumulative_points          AS gap_to_leader,
            cumulative_points - COALESCE(prev_cumulative_points, 0)
                                                       AS points_gained_in_round,
            prev_championship_position - championship_position
                                                       AS position_change_vs_prev_round,
            championship_position = 1                  AS is_championship_leader
        FROM standings
    """)
