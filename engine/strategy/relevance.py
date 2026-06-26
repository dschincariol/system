"""
FILE: relevance.py

Cached and timeout-bounded access to learned relevance statistics. This module
exists mainly so API/UI consumers can fetch relevance diagnostics safely.
"""

import logging
import os
import time
import threading

from engine.api.degradation import degraded_empty_read, is_missing_table_error
from engine.runtime.failure_diagnostics import failure_response, log_failure
from engine.runtime.logging import get_logger

# -------------            -- ------------------------------------------------------
# RELEVANCE STATS CONFIG (moved from dashboard_server.py)
# -------------            -- ------------------------------------------------------

ENABLE_RELEVANCE_STATS = os.environ.get("ENABLE_RELEVANCE_STATS", "1") == "1"
RELEVANCE_STATS_CACHE_TTL_S = int(os.environ.get("RELEVANCE_STATS_CACHE_TTL_S", "60"))
RELEVANCE_STATS_TIMEOUT_S = float(os.environ.get("RELEVANCE_STATS_TIMEOUT_S", "5.0"))

# -------------            -- ------------------------------------------------------
# RELEVANCE STATS CACHE + TIMEOUT (moved from dashboard_server.py)
# -------------            -- ------------------------------------------------------

_relevance_cache = {
    "ts": 0.0,
    "value": None,
}
LOG = get_logger("strategy.relevance")


def _labels_table_state() -> dict:
    state = {"table_present": None, "usable_rows": None}
    con = None
    try:
        from engine.api.internal_access import db_connect

        con = db_connect(readonly=True)
        try:
            con.execute("SELECT 1 FROM labels LIMIT 1").fetchone()
            state["table_present"] = True
        except Exception as e:
            log_failure(
                LOG,
                event="strategy_relevance_labels_table_state_probe_failed",
                code="STRATEGY_RELEVANCE_LABELS_TABLE_STATE_PROBE_FAILED",
                message=str(e),
                error=e,
                level=logging.WARNING,
                component="engine.strategy.relevance",
                include_health=False,
                persist=False,
            )
            if is_missing_table_error(e, "labels"):
                state["table_present"] = False
                state["usable_rows"] = 0
                return state
            return state

        try:
            row = con.execute("SELECT COUNT(*) FROM labels WHERE impact_z IS NOT NULL").fetchone()
            state["usable_rows"] = int((row or [0])[0] or 0)
        except Exception:
            state["usable_rows"] = None
    except Exception as e:
        log_failure(
            LOG,
            event="strategy_relevance_labels_table_state_failed",
            code="STRATEGY_RELEVANCE_LABELS_TABLE_STATE_FAILED",
            message=str(e),
            error=e,
            level=logging.WARNING,
            component="engine.strategy.relevance",
            include_health=False,
            persist=False,
        )
        return state
    finally:
        try:
            if con is not None:
                con.close()
        except Exception:
            LOG.debug("relevance_labels_table_state_close_failed", exc_info=True)
    return state


def _empty_relevance_stats_payload(*, cached: bool, stats: object) -> dict:
    table_state = _labels_table_state()
    table_present = table_state.get("table_present")
    usable_rows = table_state.get("usable_rows")
    if table_present is False:
        reason = "labels_table_missing"
    elif table_present is True and int(usable_rows or 0) <= 0:
        reason = "relevance_stats_no_labels_yet"
    else:
        reason = "relevance_stats_empty"
    return degraded_empty_read(
        reason,
        source="labels",
        table_present=(bool(table_present) if table_present is not None else None),
        count=0,
        cached=bool(cached),
        stats=stats if isinstance(stats, dict) else {},
        usable_label_rows=usable_rows,
    )


def _compute_relevance_stats_with_timeout(timeout_s: float):
    """
    Runs learn_relevance_stats() with a hard timeout.
    Prevents UI hangs if learner stalls.
    """
    result = {}
    error = {}

    def _runner():
        try:
            from engine.api.internal_access import learn_relevance_stats
            result["value"] = learn_relevance_stats()
        except Exception as e:
            error["error"] = str(e)

    t = threading.Thread(target=_runner, daemon=True)
    t.start()
    t.join(timeout_s)

    if t.is_alive():
        raise TimeoutError(f"learn_relevance_stats timed out after {timeout_s}s")

    if "error" in error:
        raise RuntimeError(error["error"])

    return result.get("value")


def get_relevance_stats():
    """
    Learned relevance stats used by predictor + alerts.
    Safe, read-only diagnostic endpoint.
    Includes:
      - env gate
      - TTL cache
      - hard timeout
    """
    if not ENABLE_RELEVANCE_STATS:
        return {
            "ok": False,
            "error": "relevance stats disabled (ENABLE_RELEVANCE_STATS=0)",
        }

    now = time.time()

    # Cache prevents repeated expensive reads from surfacing as UI latency.
    if (
        _relevance_cache["value"] is not None
        and (now - _relevance_cache["ts"]) < RELEVANCE_STATS_CACHE_TTL_S
    ):
        cached_stats = _relevance_cache["value"]
        if not cached_stats:
            return _empty_relevance_stats_payload(cached=True, stats=cached_stats)
        return {
            "ok": True,
            "cached": True,
            "stats": cached_stats,
        }

    try:
        stats = _compute_relevance_stats_with_timeout(
            RELEVANCE_STATS_TIMEOUT_S
        )
        _relevance_cache["value"] = stats
        _relevance_cache["ts"] = now
        if not stats:
            return _empty_relevance_stats_payload(cached=False, stats=stats)
        return {
            "ok": True,
            "cached": False,
            "stats": stats,
        }
    except Exception as e:
        out = failure_response(
            LOG,
            event="strategy_relevance_stats_failed",
            code="STRATEGY_RELEVANCE_STATS_FAILED",
            message="Failed to compute relevance stats.",
            error=e,
            component="engine.strategy.relevance",
            log_level=logging.ERROR,
            persist=False,
        )
        out["cached"] = False
        return out
