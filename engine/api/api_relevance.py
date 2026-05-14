"""
FILE: api_relevance.py

HTTP/API handlers for relevance endpoints.
"""

# engine/api/api_relevance.py
from __future__ import annotations

import logging

from engine.runtime.failure_diagnostics import failure_response
from engine.runtime.logging import get_logger
from engine.strategy.relevance import get_relevance_stats

LOG = get_logger("api.relevance")


def api_get_relevance_stats(_parsed, _ctx=None):
    try:
        # Thin pass-through endpoint; business logic stays in strategy.relevance.
        out = get_relevance_stats()
        if isinstance(out, dict):
            out.setdefault("ok", True)
            return out
        return {"ok": True, "data": out}
    except Exception as e:
        return failure_response(
            LOG,
            event="api_relevance_stats_failed",
            code="API_RELEVANCE_STATS_FAILED",
            message="Failed to load relevance stats.",
            error=e,
            component="engine.api.api_relevance",
            ctx=_ctx,
            log_level=logging.ERROR,
        )
