"""
FILE: guards.py

Runtime subsystem module for `guards`.
"""

# engine/runtime/guards.py
"""
Runtime Guards:
- Auto champion rollback
- Equity drift classification helpers
"""

import os
import logging
import time

from engine.runtime.equity_drift import (
    classify_equity_diff as _classify_equity_diff,
    detect_sustained_equity_drift as _detect_sustained_equity_drift,
)
from engine.runtime.storage import connect as _db_connect
from engine.model_registry import get_stage_latest
from engine.runtime.logging import get_logger
from engine.strategy.model_config import primary_active_model_name

LOG = get_logger("runtime_guards")


MODEL_NAME = primary_active_model_name() or "embed_regressor"


def _table_exists(con, table_name: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (str(table_name),),
    ).fetchone()
    return bool(row)


# ---------------------------------------------------
# AUTO ROLLBACK LOOP
# ---------------------------------------------------

def auto_rollback_loop(rollback_fn, write_job_history_fn):
    """
    rollback_fn: callable that executes rollback and returns dict
    write_job_history_fn: persistence hook
    This loop is intentionally conservative: it only reacts to sustained
    validation degradation, not one noisy bad point.
    """
    bad_streak = 0
    consecutive_failures = 0
    poll_s = float(os.environ.get("AUTO_ROLLBACK_POLL_S", "30"))
    max_consecutive_failures = int(os.environ.get("AUTO_ROLLBACK_MAX_CONSECUTIVE_FAILURES", "20"))

    while True:
        try:
            time.sleep(poll_s)

            champ = get_stage_latest(MODEL_NAME, stage="champion")
            if not champ:
                bad_streak = 0
                consecutive_failures = 0
                continue

            champ_rmse = champ.get("rmse")
            if champ_rmse is None:
                bad_streak = 0
                consecutive_failures = 0
                continue

            window = int(os.environ.get("AUTO_ROLLBACK_WINDOW", "100"))
            sustained = int(os.environ.get("AUTO_ROLLBACK_SUSTAINED", "3"))
            rmse_mult = float(os.environ.get("AUTO_ROLLBACK_RMSE_MULT", "1.10"))
            min_n = int(os.environ.get("AUTO_ROLLBACK_MIN_N", "20"))

            conn = _db_connect()
            try:
                rows = conn.execute(
                    """
                    SELECT CAST(json_extract(metrics_json, '$.rmse') AS REAL) AS rmse, n
                    FROM model_metrics
                    WHERE model_name = ?
                    ORDER BY ts_ms DESC
                    LIMIT ?
                    """,
                    (MODEL_NAME, window),
                ).fetchall()
            finally:
                conn.close()

            if not rows:
                bad_streak = 0
                consecutive_failures = 0
                continue

            rmse_w = 0.0
            n_tot = 0
            for r in rows:
                rmse_val = r[0]
                n_val = r[1]
                if rmse_val is None or n_val is None:
                    continue
                rmse_w += float(rmse_val) * float(n_val)
                n_tot += int(n_val)

            if n_tot < min_n:
                bad_streak = 0
                consecutive_failures = 0
                continue

            cur_rmse = rmse_w / max(1, n_tot)

            if cur_rmse >= champ_rmse * rmse_mult:
                bad_streak += 1
            else:
                bad_streak = 0

            if bad_streak >= sustained:
                try:
                    # Rollback is triggered only after the sustained window is
                    # breached, and the action is written to job history so the
                    # promotion/rollback trail remains auditable.
                    result = rollback_fn()
                    write_job_history_fn(
                        job_name="auto_rollback",
                        event="rollback",
                        detail=f"rollback executed: {result}",
                        exit_code=None,
                    )
                    LOG.log(
                        logging.WARNING,
                        "AUTO_ROLLBACK_EXECUTED",
                        extra={"model_name": MODEL_NAME, "result": str(result)},
                    )
                except Exception:
                    LOG.exception("AUTO_ROLLBACK_FAILED", extra={"model_name": MODEL_NAME})
                finally:
                    bad_streak = 0

            consecutive_failures = 0

        except Exception:
            consecutive_failures += 1
            LOG.exception(
                "AUTO_ROLLBACK_LOOP_ERROR",
                extra={
                    "model_name": MODEL_NAME,
                    "consecutive_failures": consecutive_failures,
                    "max_consecutive_failures": max_consecutive_failures,
                },
            )
            bad_streak = 0
            if consecutive_failures >= max_consecutive_failures:
                # Escalate if the guard itself is unhealthy; silently spinning
                # forever would hide the fact that rollback protection is dead.
                raise RuntimeError(
                    f"auto_rollback_loop_failure_limit_exceeded consecutive_failures={consecutive_failures}"
                )
            time.sleep(min(max(1.0, poll_s), 30.0))
            LOG.log(
                logging.WARNING,
                "AUTO_ROLLBACK_LOOP_RETRY",
                extra={
                    "event": "AUTO_ROLLBACK_LOOP_RETRY",
                    "extra_json": {
                        "model_name": MODEL_NAME,
                        "consecutive_failures": consecutive_failures,
                    },
                },
            )
            continue


# ---------------------------------------------------
# EQUITY DRIFT CLASSIFICATION
# ---------------------------------------------------

def detect_sustained_equity_drift(
    con,
    window: int,
    min_warn: int,
    min_crit: int,
):
    """
    Returns: "CRIT", "WARN", or None.
    This is a classifier helper only; policy responses happen elsewhere.
    """
    return _detect_sustained_equity_drift(
        con,
        window=int(window),
        min_warn=int(min_warn),
        min_crit=int(min_crit),
    )


def classify_equity_diff(
    diff_pct: float,
    diff_abs: float,
    warn_pct: float,
    crit_pct: float,
    warn_abs: float,
    crit_abs: float,
):
    return _classify_equity_diff(
        diff_pct,
        diff_abs,
        warn_pct=warn_pct,
        crit_pct=crit_pct,
        warn_abs=warn_abs,
        crit_abs=crit_abs,
    )
