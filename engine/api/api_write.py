"""
FILE: api_write.py

HTTP/API handlers for write endpoints.
"""

"""
Write-only API layer.

All DB mutations previously inside dashboard_server.py now live here.
No supervisor logic.
No runtime orchestration.
Pure DB mutations.
"""

import json
import time

from engine.runtime.failure_diagnostics import failure_response
from engine.runtime.logging import get_logger
from engine.runtime.storage import run_write_txn

LOG = get_logger("api.write")

# ============================================================
# ALERT ACK / RESOLVE
# ============================================================

def ack_alert(alert_id: int, who: str = "", source: str = ""):
    def _txn(con):
        # Alert acknowledgements are persisted as explicit write-side records
        # rather than mutating the original alert row.
        con.execute(
            """
            INSERT OR REPLACE INTO alert_acks
            (alert_id, acked_ts_ms, acked_by, source)
            VALUES (?,?,?,?)
            """,
            (
                int(alert_id),
                int(time.time() * 1000),
                str(who or ""),
                str(source or ""),
            ),
        )

    run_write_txn(_txn)
    return {"ok": True}


def resolve_alert(alert_id: int, who: str = "", reason: str = "", source: str = ""):
    def _txn(con):
        con.execute(
            """
            INSERT OR IGNORE INTO alert_resolutions
            (alert_id, resolved_ts_ms, resolved_by, reason, source)
            VALUES (?,?,?,?,?)
            """,
            (
                int(alert_id),
                int(time.time() * 1000),
                str(who or ""),
                str(reason or ""),
                str(source or ""),
            ),
        )

    run_write_txn(_txn)
    return {"ok": True}

# ============================================================
# JOB HISTORY
# ============================================================

def write_job_event(job_name: str, event: str, detail: dict | None = None):
    from engine.runtime.locks import write_job_history

    try:
        # This preserves the write-side boundary: API code delegates job-history
        # writes to the runtime lock/history subsystem.
        write_job_history(
            job_name=job_name,
            event=event,
            detail=json.dumps(detail or {}, separators=(",", ":"), sort_keys=True),
        )
        return {"ok": True}
    except Exception as e:
        out = failure_response(
            LOG,
            event="api_write_job_event_failed",
            code="API_WRITE_JOB_EVENT_FAILED",
            message="Failed to write job history event.",
            error=e,
            component="engine.api.api_write",
            extra={
                "job_name": str(job_name or ""),
                "event_name": str(event or ""),
            },
        )
        out["job_name"] = str(job_name or "")
        out["event_name"] = str(event or "")
        return out


# ============================================================
# PROMOTION GUARD
# ============================================================

def set_promotion_enabled(value: str):
    from engine.strategy.promotion_guard import set_guard

    # API writes normalize inputs to the narrow guard contract.
    v = "1" if str(value) == "1" else "0"
    set_guard("promotion_enabled", v)
    return {"ok": True, "promotion_enabled": v}
