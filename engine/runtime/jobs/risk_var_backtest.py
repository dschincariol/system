"""Runtime job for VaR/CVaR model exception backtesting."""

from __future__ import annotations

import json
import os

from engine.risk.var_backtesting import run_var_backtest
from engine.runtime.storage import (
    acquire_job_lock,
    init_db,
    put_job_heartbeat,
    release_job_lock,
    touch_job_lock,
)


JOB_NAME = "risk_var_backtest"
OWNER = os.environ.get("JOB_OWNER", os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")))
PID = os.getpid()
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))


def main() -> int:
    init_db()
    if not acquire_job_lock(JOB_NAME, OWNER, PID, stale_after_s=LOCK_STALE_AFTER_S):
        print(json.dumps({"ok": True, "status": "locked_out", "job": JOB_NAME}, sort_keys=True))
        return 0

    try:
        touch_job_lock(JOB_NAME, OWNER, PID)
        put_job_heartbeat(
            JOB_NAME,
            OWNER,
            PID,
            extra_json=json.dumps({"phase": "start"}, separators=(",", ":"), sort_keys=True),
        )
        result = run_var_backtest(
            limit=int(os.environ.get("VAR_BACKTEST_MAX_FORECASTS", "100") or 100),
            rolling_window=int(os.environ.get("VAR_BACKTEST_ROLLING_WINDOW", "250") or 250),
        )
        touch_job_lock(JOB_NAME, OWNER, PID)
        put_job_heartbeat(
            JOB_NAME,
            OWNER,
            PID,
            extra_json=json.dumps(
                {"phase": "done", "status": result.get("status"), "written": result.get("written", 0)},
                separators=(",", ":"),
                sort_keys=True,
            ),
        )
        print(json.dumps({"job": JOB_NAME, **result}, separators=(",", ":"), sort_keys=True, default=str))
        return 0 if bool(result.get("ok", False)) else 1
    finally:
        release_job_lock(JOB_NAME, OWNER, PID)


if __name__ == "__main__":
    raise SystemExit(main())
