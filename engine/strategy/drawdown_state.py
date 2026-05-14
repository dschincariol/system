"""
FILE: drawdown_state.py

Read helpers for portfolio drawdown state. This module computes current
drawdown from persisted equity history and exposes a simple short-horizon
velocity proxy for higher-level risk controls.
"""

import logging
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect, init_db

LOG = get_logger("strategy.drawdown_state")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="strategy_drawdown_state_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.drawdown_state",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def get_current_drawdown(con=None) -> float:
    """
    Compute drawdown from equity_history: dd = 1 - equity/peak.
    Returns 0.0 if history missing.
    """
    init_db()
    owns = False
    if con is None:
        con = connect()
        owns = True
    try:
        rows = con.execute(
            "SELECT equity FROM equity_history ORDER BY ts_ms ASC"
        ).fetchall()
        if not rows or len(rows) < 5:
            return 0.0

        peak = 0.0
        cur = 0.0
        for (eq,) in rows:
            try:
                e = float(eq or 0.0)
            except Exception as err:
                _warn_nonfatal(
                    "DRAWDOWN_STATE_EQUITY_PARSE_FAILED",
                    err,
                    once_key="equity_parse",
                    value=repr(eq)[:120],
                )
                continue
            if e > peak:
                peak = e
            cur = e

        if peak <= 0:
            return 0.0
        dd = 1.0 - (cur / peak)
        if dd < 0.0:
            dd = 0.0
        if dd > 1.0:
            dd = 1.0
        return float(dd)
    finally:
        if owns:
            con.close()

def get_drawdown_velocity(con):
    try:
        rows = con.execute(
            """
            SELECT drawdown
            FROM equity_snapshots
            ORDER BY ts_ms DESC
            LIMIT 5
            """
        ).fetchall()
        if not rows or len(rows) < 2:
            return 0.0
        latest = float(rows[0][0] or 0.0)
        prev = float(rows[1][0] or 0.0)
        return abs(latest - prev)
    except Exception as e:
        _warn_nonfatal(
            "DRAWDOWN_STATE_GET_DRAWDOWN_VELOCITY_FAILED",
            e,
            once_key="drawdown_velocity",
        )
        return 0.0
