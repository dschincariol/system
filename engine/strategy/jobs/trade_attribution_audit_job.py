"""
FILE: trade_attribution_audit_job.py

Runs audit checks over trade attribution and PnL decomposition tables.
"""

import json
import os
from engine.runtime.storage import connect, init_db
from engine.execution.trade_attribution_ledger import attribution_completeness_snapshot

RESIDUAL_ABS_PNL_MAX = float(os.environ.get("RESIDUAL_ABS_PNL_MAX", "50.0"))
RESIDUAL_ABS_RATIO_MAX = float(os.environ.get("RESIDUAL_ABS_RATIO_MAX", "0.25"))
RESIDUAL_REALIZED_EPS = float(os.environ.get("RESIDUAL_REALIZED_EPS", "1.0"))
AUTHORITATIVE_MODEL_MIN_RATIO = float(os.environ.get("AUTHORITATIVE_MODEL_MIN_RATIO", "0.95"))


def main() -> int:
    init_db()
    con = connect(readonly=True)
    try:
        report = {"ok": True, "checks": {}}

        # 1) Orphan PnL rows (must be 0)
        orphan = con.execute(
            """
            SELECT COUNT(1)
            FROM pnl_attribution p
            LEFT JOIN trade_attribution_ledger t
              ON p.ts_ms = t.ts_ms
             AND p.source_alert_id = t.source_alert_id
             AND COALESCE(NULLIF(TRIM(p.model_id), ''), 'baseline') = COALESCE(NULLIF(TRIM(t.model_id), ''), 'baseline')
             AND p.symbol = t.symbol
            WHERE t.id IS NULL
            """
        ).fetchone()[0]
        orphan = int(orphan or 0)
        report["checks"]["orphan_pnl_rows"] = orphan
        if orphan > 0:
            report["ok"] = False
            report["checks"]["orphan_pnl_fail"] = True

        # 2) Latest residual invariant (same logic as runtime check)
        snap = con.execute("SELECT MAX(ts_ms) FROM pnl_decomposition").fetchone()[0]
        snap = int(snap or 0)
        report["checks"]["pnl_decomposition_latest_ts_ms"] = snap

        if snap > 0:
            r = con.execute(
                """
                SELECT
                  COUNT(1),
                  SUM(ABS(COALESCE(residual_pnl,0))),
                  SUM(ABS(COALESCE(realized_pnl,0)))
                FROM pnl_decomposition
                WHERE ts_ms=?
                """,
                (int(snap),),
            ).fetchone()

            n = int(r[0] or 0)
            sum_abs_resid = float(r[1] or 0.0)
            sum_abs_realized = float(r[2] or 0.0)

            ratio = None
            if sum_abs_realized >= float(RESIDUAL_REALIZED_EPS):
                ratio = float(sum_abs_resid) / float(sum_abs_realized)

            report["checks"]["residual_n"] = n
            report["checks"]["residual_sum_abs"] = sum_abs_resid
            report["checks"]["realized_sum_abs"] = sum_abs_realized
            report["checks"]["residual_ratio"] = ratio
            report["checks"]["residual_abs_max"] = float(RESIDUAL_ABS_PNL_MAX)
            report["checks"]["residual_ratio_max"] = float(RESIDUAL_ABS_RATIO_MAX)

            if sum_abs_resid > float(RESIDUAL_ABS_PNL_MAX):
                report["ok"] = False
                report["checks"]["residual_abs_fail"] = True

            if ratio is not None and ratio > float(RESIDUAL_ABS_RATIO_MAX):
                report["ok"] = False
                report["checks"]["residual_ratio_fail"] = True

        # 3) Missing attribution fields for executed rows (best-effort quality bar)
        miss = con.execute(
            """
            SELECT
              SUM(CASE WHEN model_json IS NULL OR LENGTH(model_json) <= 2 THEN 1 ELSE 0 END) AS miss_model,
              SUM(CASE WHEN regime_vector_json IS NULL OR LENGTH(regime_vector_json) <= 2 THEN 1 ELSE 0 END) AS miss_regime,
              SUM(CASE WHEN execution_policy_json IS NULL OR LENGTH(execution_policy_json) <= 2 THEN 1 ELSE 0 END) AS miss_policy
            FROM trade_attribution_ledger
            WHERE suppression_reason IS NULL
              AND pnl IS NOT NULL
            """
        ).fetchone()

        miss_model = int((miss[0] or 0) if miss else 0)
        miss_regime = int((miss[1] or 0) if miss else 0)
        miss_policy = int((miss[2] or 0) if miss else 0)

        report["checks"]["missing_model_json_executed"] = miss_model
        report["checks"]["missing_regime_vector_executed"] = miss_regime
        report["checks"]["missing_execution_policy_executed"] = miss_policy

        # Soft fail (keep as warning unless you want hard fail)
        if miss_model > 0 or miss_regime > 0 or miss_policy > 0:
            report["checks"]["attribution_completeness_warning"] = True

        try:
            completeness = attribution_completeness_snapshot(limit=5000)
        except Exception as e:
            completeness = {"ok": False, "error": str(e)}
        report["checks"]["attribution_completeness"] = completeness
        authoritative_ratio = float((completeness or {}).get("authoritative_model_present_ratio") or 0.0)
        report["checks"]["authoritative_model_min_ratio"] = float(AUTHORITATIVE_MODEL_MIN_RATIO)
        if authoritative_ratio < float(AUTHORITATIVE_MODEL_MIN_RATIO):
            report["ok"] = False
            report["checks"]["authoritative_model_ratio_fail"] = True

        print(json.dumps(report, indent=2, sort_keys=True))
        return 0 if report["ok"] else 2
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
"""
FILE: trade_attribution_audit_job.py

Job entrypoint wrapper for trade attribution audit tasks.
"""
