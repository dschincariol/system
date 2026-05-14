"""
FILE: cache_warm.py

Runtime subsystem module for `cache_warm`.
"""

import logging
import threading

from engine.execution.execution_mode import get_execution_mode
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.lifecycle_state import get_state as get_lifecycle_state
from engine.runtime.logging import get_logger
from engine.runtime.risk_state import get_state as get_risk_state
from engine.runtime.runtime_meta import meta_get


LOG = get_logger("engine.runtime.cache_warm")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(event: str, code: str, error: BaseException, *, warn_key: str | None = None, **extra: object) -> None:
    if warn_key and warn_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=event,
        code=code,
        message=event,
        error=error,
        level=logging.WARNING,
        component="engine.runtime.cache_warm",
        extra=extra or None,
        persist=False,
    )
    if warn_key:
        _WARNED_NONFATAL_KEYS.add(warn_key)


def _warm_once() -> None:
    # Cache warm is intentionally best-effort: it preloads the most commonly
    # polled status keys so dashboards do less synchronous DB work after boot.
    try:
        get_execution_mode()
    except Exception as exc:
        _warn_nonfatal(
            "cache_warm_execution_mode_failed",
            "CACHE_WARM_EXECUTION_MODE_FAILED",
            exc,
            warn_key="cache_warm_execution_mode_failed",
        )

    for key, default in (
        ("portfolio_risk_block", "0"),
        ("execution_pause", "0"),
        ("capital_mode", "normal"),
        ("monte_carlo_risk_info", ""),
    ):
        try:
            get_risk_state(key, default)
        except Exception as exc:
            _warn_nonfatal(
                "cache_warm_risk_state_failed",
                "CACHE_WARM_RISK_STATE_FAILED",
                exc,
                key=str(key),
                warn_key=f"cache_warm_risk_state_failed:{key}",
            )

    for key, default in (
        ("lifecycle_state", "BOOTING"),
        ("lifecycle_detail", ""),
        ("first_price_ts_ms", ""),
        ("schema_version", ""),
        ("last_clean_shutdown_ts_ms", ""),
    ):
        try:
            meta_get(key, default)
        except Exception as exc:
            _warn_nonfatal(
                "cache_warm_meta_get_failed",
                "CACHE_WARM_META_GET_FAILED",
                exc,
                key=str(key),
                warn_key=f"cache_warm_meta_get_failed:{key}",
            )

    for key, default in (
        ("last_strategy_name", ""),
        ("last_strategy_switch_ts_ms", ""),
        ("last_rebalance_ts_ms", ""),
        ("last_rebalance_exec_id", ""),
        ("last_drawdown", ""),
        ("peak_gross_weight", ""),
        ("live_drawdown", ""),
        ("strategy_champion", ""),
        ("last_strategy_promotion_ts_ms", ""),
        ("last_strategy_governance_ts_ms", ""),
        ("last_strategy_validation", ""),
    ):
        try:
            meta_get(key, default)
        except Exception as exc:
            _warn_nonfatal(
                "cache_warm_strategy_meta_get_failed",
                "CACHE_WARM_STRATEGY_META_GET_FAILED",
                exc,
                key=str(key),
                warn_key=f"cache_warm_strategy_meta_get_failed:{key}",
            )

    try:
        get_lifecycle_state()
    except Exception as exc:
        _warn_nonfatal(
            "cache_warm_lifecycle_state_failed",
            "CACHE_WARM_LIFECYCLE_STATE_FAILED",
            exc,
            warn_key="cache_warm_lifecycle_state_failed",
        )


def warm_cache_async() -> None:
    try:
        # Fire-and-forget warmup avoids slowing the main bootstrap path.
        t = threading.Thread(target=_warm_once, name="state-cache-warm", daemon=True)
        t.start()
    except Exception as exc:
        _warn_nonfatal(
            "cache_warm_async_start_failed",
            "CACHE_WARM_ASYNC_START_FAILED",
            exc,
            warn_key="cache_warm_async_start_failed",
        )
