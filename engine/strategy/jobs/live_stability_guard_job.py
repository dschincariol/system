# FILE: live_stability_guard_job.py
# NEW FILE (CREATE)

"""
Live Stability Guard

Hard protection independent of strategy metrics.

Guards:
  - Rolling equity drawdown
  - Max daily loss
  - Turnover spike
  - Slippage drift

On breach:
  set_execution_armed(0)
  set_execution_mode("paper")
"""

import json
import os
import sys
import time

from engine.runtime.storage import connect, init_db
from engine.execution.execution_mode import set_execution_mode, set_execution_armed

MAX_DD = float(os.environ.get("LIVE_MAX_DRAWDOWN", "0.25"))
MAX_DAILY_LOSS = float(os.environ.get("LIVE_MAX_DAILY_LOSS", "0.05"))
MAX_TURNOVER = float(os.environ.get("LIVE_MAX_TURNOVER", "2.0"))
MAX_SLIPPAGE_DRIFT = float(os.environ.get("LIVE_MAX_SLIPPAGE_DRIFT", "0.02"))


def _real_model_ids(con):
    try:
        rows = con.execute(
            """
            SELECT DISTINCT COALESCE(NULLIF(TRIM(model_id), ''), 'baseline')
            FROM execution_orders
            WHERE COALESCE(json_extract(extra_json, '$.execution_target'), 'real') = 'real'
            """
        ).fetchall() or []
    except Exception:
        rows = []
    mids = [str((r or [None])[0] or "").strip() for r in rows if str((r or [None])[0] or "").strip()]
    return mids or ["baseline"]

def _now_ms():
    return int(time.time() * 1000)

def _print(x):
    sys.stdout.write(json.dumps(x, sort_keys=True) + "\n")
    sys.stdout.flush()

def main():
    con = connect()
    try:
        init_db()

        rows = con.execute(
            "SELECT ts_ms, equity FROM equity_history ORDER BY ts_ms ASC"
        ).fetchall() or []

        if not rows:
            _print({"ok": True, "status": "no_equity_data"})
            return 0

        eq = [float(r[1]) for r in rows]
        peak = None
        max_dd = 0.0
        for v in eq:
            if peak is None or v > peak:
                peak = v
            dd = (peak - v) / peak if peak and peak > 0 else 0.0
            max_dd = max(max_dd, dd)

        # daily loss: use the latest canonical attribution snapshot for today
        today = int(_now_ms() // 86400000)
        real_model_ids = _real_model_ids(con)
        ph = ",".join("?" for _ in real_model_ids)
        day_snap = con.execute(
            f"""
            SELECT MAX(ts_ms)
            FROM pnl_attribution
            WHERE ts_ms >= ?
              AND COALESCE(NULLIF(TRIM(model_id), ''), 'baseline') IN ({ph})
            """,
            tuple([today * 86400000] + list(real_model_ids)),
        ).fetchone()
        day_snap_ts_ms = int((day_snap or [0])[0] or 0)
        day_pnl = 0.0
        if day_snap_ts_ms > 0:
            day_rows = con.execute(
                f"""
                SELECT pnl,
                       COALESCE(realized_pnl, 0.0),
                       COALESCE(unrealized_pnl, 0.0),
                       COALESCE(fees, 0.0),
                       extra_json
                FROM pnl_attribution
                WHERE ts_ms = ?
                  AND COALESCE(NULLIF(TRIM(model_id), ''), 'baseline') IN ({ph})
                """,
                tuple([int(day_snap_ts_ms)] + list(real_model_ids)),
            ).fetchall() or []
            for pnl, realized_pnl, unrealized_pnl, fees, extra_json in day_rows:
                slippage_cost = 0.0
                if extra_json:
                    try:
                        parsed = json.loads(extra_json)
                        if isinstance(parsed, dict):
                            slippage_cost = float(parsed.get("slippage_cost") or 0.0)
                    except Exception:
                        slippage_cost = 0.0
                total_pnl = (
                    float(realized_pnl or 0.0)
                    + float(unrealized_pnl or 0.0)
                    - float(fees or 0.0)
                    - float(slippage_cost)
                )
                day_pnl += float(total_pnl)

        # turnover
        turn_rows = con.execute(
            f"""
            SELECT delta_weight
            FROM portfolio_orders
            WHERE ts_ms >= ?
              AND COALESCE(NULLIF(TRIM(model_id), ''), 'baseline') IN ({ph})
            """,
            tuple([today * 86400000] + list(real_model_ids)),
        ).fetchall() or []
        turnover = sum(abs(float(r[0] or 0.0)) for r in turn_rows)

        # slippage drift
        slip_rows = con.execute(
            """
            SELECT f.expected_px, f.fill_px
            FROM execution_fills f
            LEFT JOIN execution_orders o
              ON o.client_order_id = f.client_order_id
            WHERE f.ts_ms >= ?
              AND COALESCE(json_extract(o.extra_json, '$.execution_target'), 'real') = 'real'
            """,
            (today * 86400000,),
        ).fetchall() or []
        drift = 0.0
        if slip_rows:
            diffs = []
            for e, f in slip_rows:
                if e and f:
                    diffs.append(abs(float(f) - float(e)) / float(e))
            drift = sum(diffs) / len(diffs) if diffs else 0.0

        breach = (
            max_dd > MAX_DD
            or abs(day_pnl) > MAX_DAILY_LOSS
            or turnover > MAX_TURNOVER
            or drift > MAX_SLIPPAGE_DRIFT
        )

        if breach:
            set_execution_armed(0, actor="stability_guard", reason="risk_breach")
            set_execution_mode("paper", actor="stability_guard", reason="risk_breach")

        _print({
            "ok": True,
            "breach": breach,
            "max_dd": max_dd,
            "daily_pnl": day_pnl,
            "daily_pnl_snapshot_ts_ms": day_snap_ts_ms,
            "turnover": turnover,
            "slippage_drift": drift
        })
        return 0

    except Exception as e:
        _print({"ok": False, "error": str(e)})
        return 2
    finally:
        con.close()

if __name__ == "__main__":
    raise SystemExit(main())
