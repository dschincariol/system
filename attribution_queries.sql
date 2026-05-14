-- attribution_queries.sql
-- Trade Attribution Ledger: query examples (production)

-- 1) Latest attribution rows (executed + suppressed)
SELECT
  ts_ms,
  source_alert_id,
  symbol,
  pnl,
  fees,
  slippage_bps,
  suppression_reason
FROM trade_attribution_ledger
ORDER BY ts_ms DESC, id DESC
LIMIT 200;

-- 2) Every dollar: reconcile pnl_attribution vs ledger (should be 0 or fail-closed)
SELECT COUNT(1) AS orphan_pnl_rows
FROM pnl_attribution p
LEFT JOIN trade_attribution_ledger t
  ON p.ts_ms = t.ts_ms
 AND p.source_alert_id = t.source_alert_id
 AND p.symbol = t.symbol
WHERE t.id IS NULL;

-- 3) PnL by model_name (best-effort; requires model_json.model_name present)
SELECT
  COALESCE(json_extract(model_json, '$.model_name'), 'unknown') AS model_name,
  SUM(COALESCE(pnl,0)) AS total_pnl,
  COUNT(1) AS n
FROM trade_attribution_ledger
WHERE suppression_reason IS NULL
GROUP BY model_name
ORDER BY total_pnl DESC;

-- 4) PnL by social regime (best-effort; requires regime_vector_json.regime present)
SELECT
  COALESCE(UPPER(json_extract(regime_vector_json, '$.regime')), 'UNKNOWN') AS regime,
  SUM(COALESCE(pnl,0)) AS total_pnl,
  COUNT(1) AS n
FROM trade_attribution_ledger
WHERE suppression_reason IS NULL
GROUP BY regime
ORDER BY total_pnl DESC;

-- 5) Execution aggressiveness impact (best-effort; requires decision_json.aggressiveness)
SELECT
  COALESCE(UPPER(json_extract(decision_json, '$.aggressiveness')), 'UNKNOWN') AS aggressiveness,
  SUM(COALESCE(pnl,0)) AS total_pnl,
  COUNT(1) AS n
FROM trade_attribution_ledger
WHERE suppression_reason IS NULL
GROUP BY aggressiveness
ORDER BY total_pnl DESC;

-- 6) Suppression counts (why signals were blocked)
SELECT
  suppression_reason,
  COUNT(1) AS n
FROM trade_attribution_ledger
WHERE suppression_reason IS NOT NULL
GROUP BY suppression_reason
ORDER BY n DESC;

-- 7) PnL decomposition sanity (latest snapshot)
SELECT
  ts_ms,
  SUM(ABS(COALESCE(residual_pnl,0))) AS sum_abs_residual,
  SUM(ABS(COALESCE(realized_pnl,0))) AS sum_abs_realized,
  CASE
    WHEN SUM(ABS(COALESCE(realized_pnl,0))) > 0
    THEN SUM(ABS(COALESCE(residual_pnl,0))) / SUM(ABS(COALESCE(realized_pnl,0)))
    ELSE NULL
  END AS abs_residual_ratio
FROM pnl_decomposition
WHERE ts_ms = (SELECT MAX(ts_ms) FROM pnl_decomposition)
GROUP BY ts_ms;
