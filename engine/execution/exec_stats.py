"""
FILE: exec_stats.py

Execution subsystem module for `exec_stats`.
"""

# dev_core/exec_stats.py
"""
Execution performance stats from labels_exec.

labels_exec schema:
  symbol, ts_ms, realized, net_z, total_cost_bps, ...
"""

import time
import logging
from typing import Dict, Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect
from engine.runtime.state_cache import cache_get_or_load

LOG = get_logger("execution.exec_stats")

_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="execution_exec_stats_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.execution.exec_stats",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _since_ms(lookback_days: int) -> int:
    return int(time.time() * 1000) - int(lookback_days) * 86400 * 1000


def get_exec_winrate_global(con=None, lookback_days: int = 30) -> Optional[float]:
    owns = False
    if con is None:
        con = connect()
        owns = True
    try:
        def _load() -> Optional[float]:
            # These stats are dashboard/governance summaries over realized labels,
            # not real-time execution permission checks.
            since = _since_ms(lookback_days)
            rows = con.execute(
                """
                SELECT net_z
                FROM labels_exec
                WHERE ts_ms >= ?
                  AND realized = 1
                  AND net_z IS NOT NULL
                """,
                (int(since),),
            ).fetchall()

            if not rows:
                return None

            wins = 0
            n = 0
            for (z,) in rows:
                try:
                    zz = float(z)
                except Exception as e:
                    _warn_nonfatal(
                        "EXEC_STATS_WINRATE_ROW_PARSE_FAILED",
                        e,
                        once_key="winrate_row_parse",
                        value=repr(z)[:120],
                    )
                    continue
                n += 1
                if zz > 0:
                    wins += 1

            if n <= 0:
                return None
            return float(wins) / float(n)

        return cache_get_or_load("execution_stats", f"winrate_global:{int(lookback_days)}", _load, ttl_s=1.0)
    finally:
        if owns:
            con.close()


def get_exec_stats_by_symbol(con=None, lookback_days: int = 30) -> Dict[str, Dict[str, float]]:
    owns = False
    if con is None:
        con = connect()
        owns = True
    try:
        def _load() -> Dict[str, Dict[str, float]]:
            since = _since_ms(lookback_days)
            rows = con.execute(
                """
                SELECT symbol, net_z, total_cost_bps
                FROM labels_exec
                WHERE ts_ms >= ?
                  AND realized = 1
                  AND net_z IS NOT NULL
                """,
                (int(since),),
            ).fetchall()

            buckets: Dict[str, Dict[str, float]] = {}
            for sym, net_z, cost_bps in rows or []:
                s = str(sym or "").strip().upper()
                if not s:
                    continue
                try:
                    z = float(net_z)
                except Exception as e:
                    _warn_nonfatal(
                        "EXEC_STATS_SYMBOL_ROW_PARSE_FAILED",
                        e,
                        once_key=f"symbol_row_parse:{s}",
                        symbol=str(s),
                        net_z=repr(net_z)[:120],
                    )
                    continue
                try:
                    c = float(cost_bps) if cost_bps is not None else 0.0
                except Exception:
                    c = 0.0

                b = buckets.setdefault(s, {"n": 0.0, "wins": 0.0, "sum_z": 0.0, "sum_cost": 0.0})
                b["n"] += 1.0
                b["sum_z"] += float(z)
                b["sum_cost"] += float(c)
                if z > 0:
                    b["wins"] += 1.0

            out: Dict[str, Dict[str, float]] = {}
            for s, b in buckets.items():
                n = float(b.get("n", 0.0))
                if n <= 0:
                    continue
                out[s] = {
                    "n": float(n),
                    "winrate": float(b.get("wins", 0.0)) / float(n),
                    "avg_net_z": float(b.get("sum_z", 0.0)) / float(n),
                    "avg_cost_bps": float(b.get("sum_cost", 0.0)) / float(n),
                }
            return out

        return cache_get_or_load("execution_stats", f"by_symbol:{int(lookback_days)}", _load, ttl_s=1.0)
    finally:
        if owns:
            con.close()

# ============================================================
# TSE SUPPORT FUNCTIONS
# ============================================================

def get_false_positive_streak(con):
    # Trade suppression and guard logic uses this as a compact recent-failure
    # signal rather than recomputing full outcome history each time.
    try:
        rows = con.execute(
            """
            SELECT was_false_positive
            FROM execution_outcomes
            ORDER BY ts_ms DESC
            LIMIT 50
            """
        ).fetchall()
        if not rows:
            return 0
        streak = 0
        for r in rows:
            if int(r[0]) == 1:
                streak += 1
            else:
                break
        return int(streak)
    except Exception as e:
        _warn_nonfatal(
            "EXEC_STATS_FALSE_POSITIVE_STREAK_FAILED",
            e,
            once_key="false_positive_streak",
        )
        return 0
