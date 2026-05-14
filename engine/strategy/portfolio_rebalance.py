"""
FILE: portfolio_rebalance.py

Human-readable purpose:
Supervisor-run job that executes the portfolio rebalance calculation and emits
paper-trading or intent-level results. It wraps portfolio computation with job
locking, heartbeat handling, and runtime health/risk checks.

Runs portfolio rebalance (paper-trading intent only).

Outputs:
- summary JSON to stdout (captured in dashboard console)
"""

import time
import json
import os
import logging
from typing import Tuple, Any

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import (
    connect,
    init_db,
    acquire_job_lock,
    release_job_lock,
    touch_job_lock,
    put_job_heartbeat,
)

from engine.strategy.portfolio import (
    compute_rebalance,
    get_portfolio_snapshot,
)
from engine.execution.kill_switch import execution_allowed, activate
from engine.strategy.model_v2 import get_current_regime
from engine.strategy.rules_engine import evaluate_rules
from engine.strategy.regime_size import regime_capital_scale
from engine.runtime.health import get_health_snapshot

# ------            -- ------------------------------------------------------
# Job / runtime config
# ------            -- ------------------------------------------------------

JOB_NAME = "portfolio_rebalance"
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()

REBALANCE_INTERVAL_S = int(os.environ.get("REBALANCE_INTERVAL_S", "60"))
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))
HEARTBEAT_EVERY_S = float(os.environ.get("HEARTBEAT_EVERY_S", "15.0"))

# Hard risk circuit breakers (fail-closed)
MAX_DRAWDOWN_PCT = float(os.environ.get("MAX_DRAWDOWN_PCT", "0.15"))      # 15%
MAX_DAILY_LOSS_PCT = float(os.environ.get("MAX_DAILY_LOSS_PCT", "0.05"))  # 5%
MIN_CONFIDENCE = float(os.environ.get("MIN_EXEC_CONFIDENCE", "0.25"))

# Opportunity-weighted allocation knobs (bounded, convex)
OPP_CONVEX_POWER = float(os.environ.get("OPP_CONVEX_POWER", "2.0"))
OPP_MIN_CAP = float(os.environ.get("OPP_MIN_CAP", "0.0"))
OPP_MAX_CAP = float(os.environ.get("OPP_MAX_CAP", "1.0"))
MAX_SINGLE_POSITION_WEIGHT = float(os.environ.get("MAX_SINGLE_POSITION_WEIGHT", "0.15"))

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [portfolio_rebalance] %(message)s",
)
LOG = get_logger("engine.strategy.portfolio_rebalance")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(event: str, code: str, error: BaseException, *, warn_key: str | None = None, **extra: Any) -> None:
    if warn_key and warn_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=event,
        code=code,
        message=event,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.portfolio_rebalance",
        extra=extra or None,
        persist=False,
    )
    if warn_key:
        _WARNED_NONFATAL_KEYS.add(warn_key)


def _is_preflight_smoke() -> bool:
    return os.environ.get("PREFLIGHT_SMOKE", "0") == "1"

# ------            -- ------------------------------------------------------
# Risk state (fail-closed)
# ------            -- ------------------------------------------------------

def _risk_state(con) -> Tuple[bool, str]:
    """
    Returns (ok: bool, reason: str)
    """
    # Rebalance runs fail-closed: if risk state cannot be validated, the job
    # should not invent a permissive answer.
    try:
        rows = con.execute(
            """
            SELECT ts_ms, equity, ret
            FROM portfolio_bt_points
            ORDER BY ts_ms ASC
            """
        ).fetchall()

        if not rows:
            return True, "ok"

        peak = None
        max_drawdown_pct = 0.0

        for _, equity, _ in rows:
            eq = float(equity or 0.0)
            if eq <= 0.0:
                continue
            if peak is None or eq > peak:
                peak = eq
            if peak and peak > 0.0:
                dd = (float(peak) - float(eq)) / float(peak)
                if dd > max_drawdown_pct:
                    max_drawdown_pct = float(dd)

        if max_drawdown_pct >= MAX_DRAWDOWN_PCT:
            return False, f"max_drawdown_exceeded pct={max_drawdown_pct:.3f}"

        row3 = con.execute(
            """
            SELECT COALESCE(SUM(ret), 0.0)
            FROM portfolio_bt_points
            WHERE ts_ms >= ?
            """,
            (int((time.time() - 86400) * 1000),),
        ).fetchone()
        daily_ret = float(row3[0] or 0.0)

        if daily_ret <= -MAX_DAILY_LOSS_PCT:
            return False, f"max_daily_loss_exceeded ret={daily_ret:.3f}"

        return True, "ok"

    except Exception as e:
        _warn_nonfatal(
            "portfolio_rebalance_risk_state_error",
            "PORTFOLIO_REBALANCE_RISK_STATE_ERROR",
            e,
            warn_key="portfolio_rebalance_risk_state_error",
        )
        return False, f"risk_state_error {e}"

# ------            -- ------------------------------------------------------
# Main loop
# ------            -- ------------------------------------------------------

def main() -> int:
    init_db()

    if not acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
        if _is_preflight_smoke():
            print(
                json.dumps(
                    {
                        "status": "lock_held",
                        "reason": "job_lock_held",
                        "job_name": JOB_NAME,
                        "ts_ms": int(time.time() * 1000),
                    },
                    indent=2,
                )
            )
            return 0
        logging.error("another instance is holding the job lock; exiting")
        return 2

    try:
        con = connect()
        try:
            try:
                touch_job_lock(JOB_NAME, OWNER, PID)
                put_job_heartbeat(
                    JOB_NAME,
                    OWNER,
                    PID,
                    extra_json=json.dumps(
                        {"interval_s": REBALANCE_INTERVAL_S},
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                )
            except Exception as exc:
                _warn_nonfatal(
                    "portfolio_rebalance_heartbeat_failed",
                    "PORTFOLIO_REBALANCE_HEARTBEAT_FAILED",
                    exc,
                    warn_key="portfolio_rebalance_heartbeat_failed",
                )

            # ---            -- ------------------------------------------------------
            # Phase 5/6/7: rules engine (global + per-symbol halts)
            # ---            -- ------------------------------------------------------
            try:
                evaluate_rules()
            except Exception as exc:
                _warn_nonfatal(
                    "portfolio_rebalance_evaluate_rules_failed",
                    "PORTFOLIO_REBALANCE_EVALUATE_RULES_FAILED",
                    exc,
                    warn_key="portfolio_rebalance_evaluate_rules_failed",
                )

            # Current regime (best-effort)
            regime = None

            try:
                regime = str(get_current_regime("SPY") or "").strip()
            except Exception as exc:
                _warn_nonfatal(
                    "portfolio_rebalance_current_regime_failed",
                    "PORTFOLIO_REBALANCE_CURRENT_REGIME_FAILED",
                    exc,
                    warn_key="portfolio_rebalance_current_regime_failed",
                )
                regime = None

            # Kill switch gate (fail-closed for this run)
            allow, ks_reason, ks_meta = execution_allowed(con=con, symbol=None, regime=regime)
            if not allow:
                out = {
                    "status": "blocked",
                    "reason": str(ks_reason or "kill_switch_block"),
                    "meta": ks_meta or {},
                    "ts_ms": int(time.time() * 1000),
                }
                print(json.dumps(out, indent=2))
                return 0

            # Health gate (auto-kill global on failure)
            health = get_health_snapshot()
            if not health.get("ok", False):
                try:
                    activate(
                        "global",
                        "global",
                        reason="auto_health_failure",
                        actor="system",
                        meta={"health": health, "job": JOB_NAME},
                        action="AUTO",
                        con=con,
                    )
                except Exception as exc:
                    _warn_nonfatal(
                        "portfolio_rebalance_health_activate_failed",
                        "PORTFOLIO_REBALANCE_HEALTH_ACTIVATE_FAILED",
                        exc,
                        warn_key="portfolio_rebalance_health_activate_failed",
                    )
                out = {
                    "status": "blocked",
                    "reason": "health_degraded",
                    "health": health,
                    "ts_ms": int(time.time() * 1000),
                }
                print(json.dumps(out, indent=2))
                return 0

            # Risk gate (auto-kill global on failure)
            ok, reason = _risk_state(con)
            if not ok:
                try:
                    activate(
                        "global",
                        "global",
                        reason=f"auto_risk_gate:{reason}",
                        actor="system",
                        meta={"job": JOB_NAME},
                        action="AUTO",
                        con=con,
                    )
                except Exception as exc:
                    _warn_nonfatal(
                        "portfolio_rebalance_risk_activate_failed",
                        "PORTFOLIO_REBALANCE_RISK_ACTIVATE_FAILED",
                        exc,
                        warn_key="portfolio_rebalance_risk_activate_failed",
                    )
                out = {
                    "status": "blocked",
                    "reason": str(reason),
                    "ts_ms": int(time.time() * 1000),
                }
                print(json.dumps(out, indent=2))
                return 0

            # Compute rebalance (strategy-only; writes portfolio_orders)
            rebalance_res = compute_rebalance()

            # Regime capital scaling (best-effort, deterministic)
            try:
                regime_info = regime_capital_scale(con)
            except Exception as exc:
                _warn_nonfatal(
                    "portfolio_rebalance_regime_capital_scale_failed",
                    "PORTFOLIO_REBALANCE_REGIME_CAPITAL_SCALE_FAILED",
                    exc,
                    warn_key="portfolio_rebalance_regime_capital_scale_failed",
                )
                regime_info = {"ok": False}

            # Snapshot portfolio state after rebalance write
            snapshot = get_portfolio_snapshot(limit_orders=30)

            out = {
                "status": "ok" if bool((rebalance_res or {}).get("ok", False)) else "blocked",
                "rebalance": rebalance_res,
                "regime": regime_info,
                "portfolio": snapshot,
                "ts_ms": int(time.time() * 1000),
            }
            print(json.dumps(out, indent=2))
            return 0

        finally:
            con.close()

    except Exception:
        logging.exception("portfolio rebalance failed")
        raise

    finally:
        try:
            release_job_lock(JOB_NAME, OWNER, PID)
        except Exception as exc:
            _warn_nonfatal(
                "portfolio_rebalance_release_job_lock_failed",
                "PORTFOLIO_REBALANCE_RELEASE_JOB_LOCK_FAILED",
                exc,
                warn_key="portfolio_rebalance_release_job_lock_failed",
            )

# ------            -- ------------------------------------------------------
# Ensure lock release on shutdown
# ------            -- ------------------------------------------------------

if __name__ == "__main__":
    try:
        raise SystemExit(main())
    finally:
        try:
            release_job_lock(JOB_NAME, OWNER, PID)
        except Exception as exc:
            _warn_nonfatal(
                "portfolio_rebalance_shutdown_release_job_lock_failed",
                "PORTFOLIO_REBALANCE_SHUTDOWN_RELEASE_JOB_LOCK_FAILED",
                exc,
                warn_key="portfolio_rebalance_shutdown_release_job_lock_failed",
            )
