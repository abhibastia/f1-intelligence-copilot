-- Correctness checks. Run these before trusting the dashboard.
--
-- Check 1 is the important one: it reconciles two INDEPENDENT Jolpica endpoints
-- against each other (results + sprint versus driverStandings). Agreement is
-- real evidence the flattening, deduplication, and joins are correct — not a
-- comparison against a number someone typed in from memory.
--
-- Verified against the landed 2024 data before the pipeline was built:
--   race points only     → 13 of 24 drivers mismatch
--   race + sprint points →  0 of 24 drivers mismatch

-- ─────────────────────────────────────────────────────────────────────────
-- 1. Championship reconciliation. MUST return zero rows.
-- ─────────────────────────────────────────────────────────────────────────
WITH earned AS (
  SELECT season, driver_id, SUM(total_points) AS points_from_results
  FROM f1.gold.driver_performance
  GROUP BY season, driver_id
),
final_round AS (
  SELECT season, MAX(round) AS last_round
  FROM f1.gold.championship_progression
  GROUP BY season
),
official AS (
  SELECT c.season, c.driver_id, c.cumulative_points AS points_from_standings
  FROM f1.gold.championship_progression c
  JOIN final_round f ON f.season = c.season AND f.last_round = c.round
)
SELECT
  o.season,
  o.driver_id,
  e.points_from_results,
  o.points_from_standings,
  e.points_from_results - o.points_from_standings AS delta
FROM official o
JOIN earned e ON e.season = o.season AND e.driver_id = o.driver_id
WHERE ABS(e.points_from_results - o.points_from_standings) > 1e-9
ORDER BY ABS(delta) DESC;


-- ─────────────────────────────────────────────────────────────────────────
-- 2. SCD-2 history exists. MUST return rows.
--    An empty result means dim_driver has no versioning and MVP item 3 is
--    NOT met, regardless of the pattern being implemented.
-- ─────────────────────────────────────────────────────────────────────────
SELECT
  driver_id,
  driver_name,
  constructor_id,
  constructor_name,
  __START_AT,
  __END_AT
FROM f1.silver.dim_driver
WHERE __END_AT IS NOT NULL
ORDER BY driver_id, __START_AT;


-- ─────────────────────────────────────────────────────────────────────────
-- 3. Every driver has exactly one current row in the SCD-2 dimension.
--    MUST return zero rows.
-- ─────────────────────────────────────────────────────────────────────────
SELECT driver_id, COUNT(*) AS current_rows
FROM f1.silver.dim_driver
WHERE __END_AT IS NULL
GROUP BY driver_id
HAVING COUNT(*) <> 1;


-- ─────────────────────────────────────────────────────────────────────────
-- 4. Deduplication worked: no duplicate natural keys anywhere in Silver.
--    MUST return zero rows. A non-zero count here means the open round landed
--    multiple snapshots and the dedupe window failed.
-- ─────────────────────────────────────────────────────────────────────────
SELECT 'fact_result' AS table_name, season, round, driver_id, COUNT(*) AS n
FROM f1.silver.fact_result GROUP BY 1,2,3,4 HAVING COUNT(*) > 1
UNION ALL
SELECT 'fact_qualifying', season, round, driver_id, COUNT(*)
FROM f1.silver.fact_qualifying GROUP BY 1,2,3,4 HAVING COUNT(*) > 1
UNION ALL
SELECT 'fact_driver_standing', season, round, driver_id, COUNT(*)
FROM f1.silver.fact_driver_standing GROUP BY 1,2,3,4 HAVING COUNT(*) > 1
UNION ALL
SELECT 'fact_sprint_result', season, round, driver_id, COUNT(*)
FROM f1.silver.fact_sprint_result GROUP BY 1,2,3,4 HAVING COUNT(*) > 1;


-- ─────────────────────────────────────────────────────────────────────────
-- 5. Gold row counts and season coverage — a quick sanity read.
-- ─────────────────────────────────────────────────────────────────────────
SELECT
  season,
  COUNT(DISTINCT round)      AS rounds,
  COUNT(DISTINCT driver_id)  AS drivers,
  COUNT(*)                   AS rows,
  SUM(total_points)          AS points_awarded,
  SUM(CASE WHEN dnf_flag THEN 1 ELSE 0 END) AS dnfs
FROM f1.gold.driver_performance
GROUP BY season
ORDER BY season;


-- ─────────────────────────────────────────────────────────────────────────
-- 6. Quarantine census. NOT expected to be empty — three sources reject rows
--    on real Jolpica data, and the counts are the point:
--      lap             ~69  plausible_lap_time — laps outside 40-300 s, i.e.
--                           red-flag and safety-car-delta laps. Dropping them
--                           is what keeps the pace mart about racing.
--      driver_standing  ~8  position_present — standings rows the feed
--                           published without a championship position.
--      pit_stop         ~2  duration_parsed — stops published with an empty
--                           duration. Check 11 proves those are the same two
--                           stops that leave a gap in the stop numbering.
--    A number moving a long way from these is the signal; zero everywhere
--    would mean the expectations stopped being evaluated.
-- ─────────────────────────────────────────────────────────────────────────
SELECT 'race'                 AS source, COUNT(*) AS quarantined FROM f1.silver.quarantine_race
UNION ALL SELECT 'result',                COUNT(*) FROM f1.silver.quarantine_result
UNION ALL SELECT 'qualifying',            COUNT(*) FROM f1.silver.quarantine_qualifying
UNION ALL SELECT 'sprint_result',         COUNT(*) FROM f1.silver.quarantine_sprint_result
UNION ALL SELECT 'driver_standing',       COUNT(*) FROM f1.silver.quarantine_driver_standing
UNION ALL SELECT 'constructor_standing',  COUNT(*) FROM f1.silver.quarantine_constructor_standing
UNION ALL SELECT 'race_weather',          COUNT(*) FROM f1.silver.quarantine_race_weather
UNION ALL SELECT 'pit_stop',              COUNT(*) FROM f1.silver.quarantine_pit_stop
UNION ALL SELECT 'lap',                   COUNT(*) FROM f1.silver.quarantine_lap
ORDER BY quarantined DESC;


-- ─────────────────────────────────────────────────────────────────────────
-- 7. The as-of join is doing real work: drivers whose Gold constructor differs
--    across the season. Should list mid-season team changes.
-- ─────────────────────────────────────────────────────────────────────────
SELECT season, driver_id, driver_name,
       COUNT(DISTINCT constructor_id_as_of_race) AS teams_in_season,
       COLLECT_SET(constructor_name_as_of_race)  AS teams
FROM f1.gold.driver_performance
GROUP BY season, driver_id, driver_name
HAVING COUNT(DISTINCT constructor_id_as_of_race) > 1
ORDER BY season, driver_id;


-- ─────────────────────────────────────────────────────────────────────────
-- 8. Deduplication at the finer grains. MUST return zero rows.
--
--    Check 4 covers the facts keyed on (season, round, driver_id). Laps and
--    pit stops are keyed one level deeper, and both had a specific reason to
--    duplicate: lap pages split mid-lap, so the same lap number arrives twice
--    with different drivers, and the open round is re-pulled on every run.
--    Deduplicating on the lap element instead of (lap, driver) would look
--    correct here and silently drop half a lap of timing.
-- ─────────────────────────────────────────────────────────────────────────
SELECT 'fact_lap' AS table_name, season, round, lap AS lap_or_stop, driver_id, COUNT(*) AS n
FROM f1.silver.fact_lap
GROUP BY 1,2,3,4,5 HAVING COUNT(*) > 1
UNION ALL
SELECT 'fact_pit_stop', season, round, stop_number, driver_id, COUNT(*)
FROM f1.silver.fact_pit_stop
GROUP BY 1,2,3,4,5 HAVING COUNT(*) > 1;


-- ─────────────────────────────────────────────────────────────────────────
-- 9. Lap timings reconcile with the results endpoint. MUST return zero rows.
--
--    Same idea as check 1, one grain down: `laps` and `results` are separate
--    Jolpica endpoints, so the last lap a driver was timed on should be the
--    number of laps the results credit them with. Agreement is evidence the
--    three-level explode and the deduplication are correct.
--
--    Two exemptions, both real semantics rather than tolerance for error:
--
--    Disqualified drivers are excluded. The results endpoint zeroes a
--    disqualified driver's lap count, but the laps they actually drove were
--    still timed — five drivers here, every one of them status 'Disqualified'.
--    Reconciling those to zero would mean deleting laps that happened.
--
--    A one-lap difference is allowed. A lapped runner is classified on the
--    lap the leader finished, so the timing feed holds one more lap than the
--    results credit. Anything beyond one lap is a flattening bug.
-- ─────────────────────────────────────────────────────────────────────────
WITH timed AS (
  SELECT season, round, driver_id, MAX(lap) AS last_timed_lap
  FROM f1.silver.fact_lap
  GROUP BY season, round, driver_id
)
SELECT
  r.season, r.round, r.driver_name, r.status,
  r.laps_completed,
  t.last_timed_lap,
  t.last_timed_lap - r.laps_completed AS delta
FROM f1.gold.driver_performance r
JOIN timed t
  ON t.season = r.season AND t.round = r.round AND t.driver_id = r.driver_id
WHERE r.status <> 'Disqualified'
  AND ABS(t.last_timed_lap - r.laps_completed) > 1
ORDER BY ABS(delta) DESC;


-- ─────────────────────────────────────────────────────────────────────────
-- 10. Every pit stop happened inside the race. MUST return zero rows.
--
--     Cross-endpoint again: the race distance comes from the laps feed, the
--     stops from the pit stop feed. A stop on lap 0, or past the chequered
--     flag, means a round number was mis-parsed and stops were attributed to
--     the wrong race — the failure mode the expectations cannot see, because
--     each row is individually plausible.
-- ─────────────────────────────────────────────────────────────────────────
WITH race_distance AS (
  SELECT season, round, MAX(lap) AS last_lap
  FROM f1.silver.fact_lap
  GROUP BY season, round
)
SELECT p.season, p.round, p.driver_id, p.stop_number, p.lap, rd.last_lap
FROM f1.silver.fact_pit_stop p
JOIN race_distance rd
  ON rd.season = p.season AND rd.round = p.round
WHERE p.lap < 1 OR p.lap > rd.last_lap;


-- ─────────────────────────────────────────────────────────────────────────
-- 11. Every gap in the stop numbering is accounted for. MUST return zero rows.
--
--     Stop numbers should run 1..n per driver per race, and for two driver-
--     races they do not: the highest stop number is larger than the number of
--     stops kept. That is not a lost row — both stops were published with an
--     empty `duration` and the `duration_parsed` expectation moved them to
--     quarantine, where check 6 counts them.
--
--     So the invariant worth asserting is not "no gaps" but "no unexplained
--     gaps": missing stop numbers must equal quarantined stops for that
--     driver-race. A gap without a matching quarantine row means the stop
--     vanished somewhere between Bronze and Silver.
--
--     Consequence worth knowing: gold.race_strategy derives stints as
--     stops + 1 from the rows that survived, so those two driver-races
--     understate stints by one. That is the honest number given a stop with
--     no duration, not a defect to paper over.
-- ─────────────────────────────────────────────────────────────────────────
WITH gaps AS (
  SELECT season, round, driver_id,
         COUNT(*)          AS stops_kept,
         MAX(stop_number)  AS highest_stop_number
  FROM f1.silver.fact_pit_stop
  GROUP BY season, round, driver_id
  HAVING MAX(stop_number) <> COUNT(*)
),
dropped AS (
  SELECT season, round, driver_id, COUNT(*) AS quarantined
  FROM f1.silver.quarantine_pit_stop
  GROUP BY season, round, driver_id
)
SELECT g.season, g.round, g.driver_id,
       g.stops_kept, g.highest_stop_number,
       COALESCE(d.quarantined, 0) AS quarantined
FROM gaps g
LEFT JOIN dropped d
  ON d.season = g.season AND d.round = g.round AND d.driver_id = g.driver_id
WHERE g.highest_stop_number - g.stops_kept <> COALESCE(d.quarantined, 0);


-- ─────────────────────────────────────────────────────────────────────────
-- 12. gold.lap_pace internal arithmetic. MUST return zero rows.
--
--     The pace mart filters, aggregates and then compares against a per-race
--     reference, and each step has an invariant that must survive it: clean
--     laps are a subset of recorded laps, a driver cannot lead more laps than
--     they ran, a median of a set cannot be faster than its minimum, and the
--     reference is the fastest median in the race so no deficit can be
--     negative. Any of these turning up means the 107% filter or the
--     reference join is wrong — which a row count would never reveal.
-- ─────────────────────────────────────────────────────────────────────────
SELECT season, round, driver_name,
       laps_recorded, clean_laps, laps_led,
       best_lap_s, median_clean_lap_s, pace_deficit_pct
FROM f1.gold.lap_pace
WHERE clean_laps > laps_recorded
   OR laps_led   > laps_recorded
   OR median_clean_lap_s < best_lap_s
   OR pace_deficit_pct   < 0
   OR clean_laps < 1;


-- ─────────────────────────────────────────────────────────────────────────
-- 13. Lap and pit stop coverage. MUST return zero rows.
--
--     Every round that produced a result must also have laps and stops. This
--     is the check that would have caught the landing zone holding results
--     for a round whose laps were never uploaded: the pipeline runs green,
--     the marts are internally consistent, and the pace layer is simply
--     missing a race — visible as an absence, which nothing else here tests.
-- ─────────────────────────────────────────────────────────────────────────
WITH raced AS (
  SELECT DISTINCT season, round FROM f1.silver.fact_result
)
SELECT r.season, r.round,
       EXISTS (SELECT 1 FROM f1.silver.fact_lap l
                WHERE l.season = r.season AND l.round = r.round) AS has_laps,
       EXISTS (SELECT 1 FROM f1.silver.fact_pit_stop p
                WHERE p.season = r.season AND p.round = r.round) AS has_pit_stops
FROM raced r
WHERE NOT EXISTS (SELECT 1 FROM f1.silver.fact_lap l
                   WHERE l.season = r.season AND l.round = r.round)
   OR NOT EXISTS (SELECT 1 FROM f1.silver.fact_pit_stop p
                   WHERE p.season = r.season AND p.round = r.round)
ORDER BY r.season, r.round;


-- ─────────────────────────────────────────────────────────────────────────
-- 14. Constructor championship reconciliation. MUST return zero rows.
--
--     The constructor-side equivalent of check 1, and it works for the same
--     reason: two independent Jolpica endpoints, parsed and deduplicated
--     separately, are made to agree. gold.constructor_standings reads the
--     published constructorStandings endpoint; the comparison sums driver
--     results by the team the driver actually drove for that weekend.
--
--     The as-of-race attribution is what makes this non-trivial. A driver who
--     changes team mid-season carries their points with them in the standings,
--     and only an as-of-race join reproduces that — a current-team join would
--     credit every point they scored all year to whoever they drive for now.
-- ─────────────────────────────────────────────────────────────────────────
WITH final_round AS (
  SELECT season, MAX(round) AS last_round
  FROM f1.gold.constructor_standings
  GROUP BY season
),
official AS (
  SELECT c.season, c.constructor_id, c.constructor_name,
         c.cumulative_points AS points_from_standings
  FROM f1.gold.constructor_standings c
  JOIN final_round f ON f.season = c.season AND f.last_round = c.round
),
earned AS (
  SELECT season, constructor_id_as_of_race AS constructor_id,
         SUM(total_points) AS points_from_results
  FROM f1.gold.driver_performance
  WHERE constructor_id_as_of_race IS NOT NULL
  GROUP BY season, constructor_id_as_of_race
)
SELECT
  o.season,
  o.constructor_name,
  e.points_from_results,
  o.points_from_standings,
  e.points_from_results - o.points_from_standings AS delta
FROM official o
LEFT JOIN earned e ON e.season = o.season AND e.constructor_id = o.constructor_id
WHERE ABS(COALESCE(e.points_from_results, 0) - o.points_from_standings) > 1e-9
ORDER BY ABS(delta) DESC;
