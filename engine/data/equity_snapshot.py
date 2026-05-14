"""
FILE: equity_snapshot.py

Data subsystem module for `equity_snapshot`.
"""

# dev_core/equity_snapshot.py
import logging
import time
from typing import Optional
from engine.runtime.equity_drift import (
    emit_equity_drift_alerts,
    get_latest_broker_equity,
    sync_equity_drift_from_history,
)
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect, init_db

LOG = get_logger("engine.data.equity_snapshot")


def snapshot_equity(ts_ms: Optional[int] = None) -> bool:
    init_db()
    if ts_ms is None:
        ts_ms = int(time.time() * 1000)

    con = connect()
    try:
        # This is a thin snapshot helper, not a PnL/accounting engine. It copies
        # the latest broker equity into a history table for trend monitoring.
        eq, _ = get_latest_broker_equity(con)
        if eq is None:
            return False

        con.execute(
            "INSERT OR REPLACE INTO equity_history(ts_ms, equity) VALUES (?,?)",
            (int(ts_ms), float(eq)),
        )
        # Keep the historical broker-vs-backtest drift series in step with the
        # canonical equity snapshots so dashboard/guard readers see real data.
        drift_sync = sync_equity_drift_from_history(con, upto_ts_ms=int(ts_ms))
        con.commit()
        try:
            emit_equity_drift_alerts(current_row=dict(drift_sync.get("latest_row") or {}))
        except Exception as e:
            log_failure(
                LOG,
                event="equity_snapshot_alert_emit_failed",
                code="EQUITY_SNAPSHOT_ALERT_EMIT_FAILED",
                message="Equity drift alert emission failed.",
                error=e,
                level=logging.WARNING,
                component="engine.data.equity_snapshot",
                persist=False,
            )
        return True
    except Exception as e:
        log_failure(
            LOG,
            event="equity_snapshot_failed",
            code="EQUITY_SNAPSHOT_FAILED",
            message="Equity snapshot failed.",
            error=e,
            level=logging.WARNING,
            component="engine.data.equity_snapshot",
            persist=False,
        )
        return False
    finally:
        con.close()
