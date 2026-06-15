"""
FILE: repair_trade_attribution_history.py

Job entrypoint wrapper for historical trade attribution repair.
"""

import json
import os
import logging

from engine.execution.execution_ledger import (
    rebuild_historical_pnl_attribution,
    repair_execution_order_model_identity,
)
from engine.execution.trade_attribution_ledger import (
    attribution_completeness_snapshot,
    rebuild_historical_trade_attribution,
)
from engine.runtime.runtime_meta import meta_set
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import (
    acquire_job_lock,
    init_db,
    put_job_heartbeat,
    release_job_lock,
    touch_job_lock,
)

JOB_NAME = "repair_trade_attribution_history"
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "300"))
REPAIR_LIMIT = int(os.environ.get("ATTRIBUTION_REPAIR_EXECUTION_ORDER_LIMIT", "50000"))
SNAPSHOT_LIMIT = int(os.environ.get("ATTRIBUTION_REPAIR_SNAPSHOT_LIMIT", "200"))
MAX_SNAPSHOT_AGE_MS = int(os.environ.get("ATTRIBUTION_REPAIR_MAX_SNAPSHOT_AGE_MS", str(90 * 24 * 60 * 60 * 1000)))
LOG = get_logger("engine.execution.jobs.repair_trade_attribution_history")


def main() -> int:
    now_ms = int(__import__("time").time() * 1000)
    repair = repair_execution_order_model_identity(limit=int(REPAIR_LIMIT))
    pnl_rebuild = rebuild_historical_pnl_attribution(
        limit_snapshots=int(SNAPSHOT_LIMIT),
        max_snapshot_age_ms=int(MAX_SNAPSHOT_AGE_MS),
        lookback_orders=int(REPAIR_LIMIT),
    )
    rebuild = rebuild_historical_trade_attribution(
        limit_snapshots=int(SNAPSHOT_LIMIT),
        max_snapshot_age_ms=int(MAX_SNAPSHOT_AGE_MS),
    )
    completeness = attribution_completeness_snapshot(limit=max(5000, int(REPAIR_LIMIT)))
    payload = {
        "ok": bool(repair.get("ok")) and bool(pnl_rebuild.get("ok")) and bool(rebuild.get("ok")) and bool(completeness.get("ok")),
        "ts_ms": int(now_ms),
        "repair": repair,
        "pnl_rebuild": pnl_rebuild,
        "rebuild": rebuild,
        "completeness": completeness,
    }
    try:
        meta_set(
            "trade_attribution_historical_repair",
            json.dumps(payload, separators=(",", ":"), sort_keys=True),
        )
    except Exception as e:
        log_failure(
            LOG,
            event="repair_trade_attribution_history_meta_set_failed",
            code="REPAIR_TRADE_ATTRIBUTION_HISTORY_META_SET_FAILED",
            message="Trade attribution historical repair meta_set failed.",
            error=e,
            level=logging.WARNING,
            component="engine.execution.jobs.repair_trade_attribution_history",
            persist=False,
        )
    LOG.info(
        "repair_trade_attribution_history_result result=%s",
        json.dumps(payload, indent=2, sort_keys=True),
    )
    return 0 if bool(payload.get("ok")) else 2


if __name__ == "__main__":
    init_db()

    if not acquire_job_lock(JOB_NAME, OWNER, PID, stale_after_s=LOCK_STALE_AFTER_S):
        raise SystemExit(main())

    try:
        touch_job_lock(JOB_NAME, OWNER, PID)
        put_job_heartbeat(
            JOB_NAME,
            OWNER,
            PID,
            extra_json=json.dumps({"phase": "start"}, separators=(",", ":"), sort_keys=True),
        )
        rc = int(main() or 0)
        touch_job_lock(JOB_NAME, OWNER, PID)
        put_job_heartbeat(
            JOB_NAME,
            OWNER,
            PID,
            extra_json=json.dumps({"phase": "done", "rc": rc}, separators=(",", ":"), sort_keys=True),
        )
        raise SystemExit(rc)
    finally:
        release_job_lock(JOB_NAME, OWNER, PID)
