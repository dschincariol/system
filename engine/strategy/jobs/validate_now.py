"""
FILE: validate_now.py

Job entrypoint for immediate validation-score and model-metric computation.
"""

import json
import os

from engine.runtime.storage import (
    acquire_job_lock,
    init_db,
    put_job_heartbeat,
    release_job_lock,
    touch_job_lock,
)
from engine.strategy.validation import (
    init_validation_db,
    compute_validation_scores,
    compute_model_metrics,
    get_validation_scores,
    get_model_metrics,
)

JOB_NAME = "validate_now"
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))


def main() -> int:
    init_validation_db()
    groups = compute_validation_scores()
    print("validated_groups =", groups)

    m_groups = compute_model_metrics(model_name="default", err_threshold=1.0, n_bins=10)
    print("metrics_groups =", m_groups)

    for sym, h, mae, rmse, n, ts in get_validation_scores():
        print(f"{sym} h={h} MAE={mae:.4f} RMSE={rmse:.4f} n={n}")

    for r in get_model_metrics(model_name="default"):
        m = (r.get("metrics") or {})
        print(
            f'{r["symbol"]} h={r["horizon_s"]} '
            f'R2={float(m.get("r2",0.0)):.3f} '
            f'DirAcc={float(m.get("direction_acc",0.0)):.3f} '
            f'ECE={float(m.get("ece",0.0)):.3f} '
            f'AvgConf={float(m.get("avg_conf",0.0)):.3f} '
            f'n={int(r["n"])}'
        )
    return 0


if __name__ == "__main__":
    init_db()

    if not acquire_job_lock(JOB_NAME, OWNER, PID, stale_after_s=LOCK_STALE_AFTER_S):
        raise SystemExit(0)

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
"""
FILE: validate_now.py

Job entrypoint wrapper for immediate validation scoring.
"""
