\set ON_ERROR_STOP on
\pset pager off
SET search_path=trading,public;

DO $$
DECLARE
  missing text[];
BEGIN
  SELECT array_agg(table_name)
  INTO missing
  FROM unnest(ARRAY[
    'model_registry',
    'decision_log',
    'broker_fills',
    'kill_switch_state'
  ]) AS required(table_name)
  WHERE to_regclass(required.table_name) IS NULL;

  IF missing IS NOT NULL THEN
    RAISE EXCEPTION 'restore_sanity_missing_tables=%', missing;
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM kill_switch_state
    WHERE scope = 'global'
      AND key = 'global'
      AND enabled = 1
  ) THEN
    RAISE EXCEPTION 'restore_sanity_kill_switch_not_tripped';
  END IF;
END
$$;

SELECT 'model_registry' AS check_name, COUNT(*) AS row_count
FROM model_registry;

SELECT 'decision_log_recent_24h' AS check_name, COUNT(*) AS row_count
FROM decision_log
WHERE ts_ms >= ((EXTRACT(EPOCH FROM now())::bigint - 86400) * 1000);

SELECT 'broker_fills_recent_24h' AS check_name, COUNT(*) AS row_count
FROM broker_fills
WHERE ts_ms >= ((EXTRACT(EPOCH FROM now())::bigint - 86400) * 1000);

SELECT 'kill_switch_global_enabled' AS check_name, COUNT(*) AS row_count
FROM kill_switch_state
WHERE scope = 'global'
  AND key = 'global'
  AND enabled = 1;

\echo restore_sanity_pass
