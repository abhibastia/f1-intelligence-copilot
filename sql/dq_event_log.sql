-- Data-quality metrics from the Lakeflow pipeline event log.
--
-- The pipeline publishes its event log to f1.gold.pipeline_event_log (set via
-- the `event_log` block in the pipeline config). Expectation results arrive as
-- JSON inside the `details` column under flow_progress.data_quality.expectations.
--
-- These queries are the evidence for MVP item 5: data quality is not just
-- enforced, it is measurable.

-- ─────────────────────────────────────────────────────────────────────────
-- 1. Expectation pass/fail counts per dataset per run.
--    This is the query the dashboard DQ tile is built on.
-- ─────────────────────────────────────────────────────────────────────────
WITH expectations AS (
  SELECT
    origin.update_id                                   AS update_id,
    timestamp                                          AS event_time,
    origin.flow_name                                   AS dataset_name,
    explode(
      from_json(
        get_json_object(details, '$.flow_progress.data_quality.expectations'),
        'ARRAY<STRUCT<name STRING, dataset STRING, passed_records BIGINT, failed_records BIGINT>>'
      )
    )                                                  AS expectation
  FROM f1.gold.pipeline_event_log
  WHERE event_type = 'flow_progress'
    AND get_json_object(details, '$.flow_progress.data_quality.expectations') IS NOT NULL
)
SELECT
  update_id,
  dataset_name,
  expectation.name                                     AS expectation_name,
  SUM(expectation.passed_records)                      AS passed_records,
  SUM(expectation.failed_records)                      AS failed_records,
  ROUND(
    100.0 * SUM(expectation.failed_records)
      / NULLIF(SUM(expectation.passed_records) + SUM(expectation.failed_records), 0),
    3
  )                                                    AS failure_rate_pct,
  MAX(event_time)                                      AS last_seen_at
FROM expectations
GROUP BY update_id, dataset_name, expectation.name
ORDER BY failed_records DESC, dataset_name, expectation_name;


-- ─────────────────────────────────────────────────────────────────────────
-- 2. Run history: did the pipeline succeed, and how long did it take?
-- ─────────────────────────────────────────────────────────────────────────
SELECT
  origin.update_id                                     AS update_id,
  MIN(timestamp)                                       AS started_at,
  MAX(timestamp)                                       AS ended_at,
  ROUND((UNIX_TIMESTAMP(MAX(timestamp)) - UNIX_TIMESTAMP(MIN(timestamp))) / 60.0, 1)
                                                       AS duration_minutes,
  MAX(CASE WHEN event_type = 'update_progress'
           THEN get_json_object(details, '$.update_progress.state') END)
                                                       AS final_state
FROM f1.gold.pipeline_event_log
GROUP BY origin.update_id
ORDER BY started_at DESC
LIMIT 20;


-- ─────────────────────────────────────────────────────────────────────────
-- 3. Rows written per dataset per run — pairs with the quarantine counts to
--    show what was accepted versus what was rejected.
-- ─────────────────────────────────────────────────────────────────────────
SELECT
  origin.update_id                                     AS update_id,
  origin.flow_name                                     AS dataset_name,
  MAX(get_json_object(details, '$.flow_progress.metrics.num_output_rows'))
    ::BIGINT                                           AS rows_written,
  MAX(timestamp)                                       AS completed_at
FROM f1.gold.pipeline_event_log
WHERE event_type = 'flow_progress'
  AND get_json_object(details, '$.flow_progress.metrics.num_output_rows') IS NOT NULL
GROUP BY origin.update_id, origin.flow_name
ORDER BY completed_at DESC, dataset_name;


-- ─────────────────────────────────────────────────────────────────────────
-- 4. Errors, with the real exception message.
--    The top-level message only says "Update X is FAILED" — useless on its own.
-- ─────────────────────────────────────────────────────────────────────────
SELECT
  timestamp                                            AS event_time,
  origin.update_id                                     AS update_id,
  origin.flow_name                                     AS dataset_name,
  message,
  get_json_object(details, '$.flow_progress.status')   AS flow_status
FROM f1.gold.pipeline_event_log
WHERE level = 'ERROR'
ORDER BY timestamp DESC
LIMIT 50;
