"""
FILE: relevance.py

Cached and timeout-bounded access to learned relevance statistics. This module
exists mainly so API/UI consumers can fetch relevance diagnostics safely.
"""

import logging
import os
import time
import threading

from engine.runtime.failure_diagnostics import failure_response
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
        return {
            "ok": True,
            "cached": True,
            "stats": _relevance_cache["value"],
        }

    try:
        stats = _compute_relevance_stats_with_timeout(
            RELEVANCE_STATS_TIMEOUT_S
        )
        _relevance_cache["value"] = stats
        _relevance_cache["ts"] = now
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
