"""
FILE: strategy_selector.py

Chooses which portfolio strategy implementation is active.

This module supports explicit operator selection and a simple auto-selection
mode that reads persisted strategy metrics and applies a cooldown before
switching the live choice.
"""

import json
import os
import time
import logging
from typing import Optional, Tuple, Dict, Any

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger

PORTFOLIO_STRATEGY = os.environ.get("PORTFOLIO_STRATEGY", "baseline").strip().lower()
PORTFOLIO_STRATEGY_SWITCH_COOLDOWN_S = int(os.environ.get("PORTFOLIO_STRATEGY_SWITCH_COOLDOWN_S", "3600"))

STRATEGY_WINDOW_DAYS = int(os.environ.get("STRATEGY_WINDOW_DAYS", "30"))
STRATEGY_METRICS_MAX_AGE_S = int(os.environ.get("STRATEGY_METRICS_MAX_AGE_S", "86400"))  # 24h

KNOWN_STRATEGIES = ("baseline", "conservative")
LOG = get_logger("strategy.strategy_selector")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="strategy_selector_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.strategy_selector",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)

def _now_ms() -> int:
    return int(time.time() * 1000)

def _get_meta(con, key: str) -> Optional[str]:
    from engine.runtime.state_cache import cache_get, cache_set

    key_s = str(key)
    cached = cache_get("portfolio_meta", key_s)
    if cached is not None:
        return str(cached) if cached is not None else None

    row = con.execute("SELECT value FROM portfolio_meta WHERE key=?", (key_s,)).fetchone()
    value = str(row[0]) if row and row[0] is not None else None
    cache_set("portfolio_meta", key_s, value, ttl_s=3600.0)
    return value

def _set_meta(con, key: str, value: str) -> None:
    from engine.runtime.state_cache import cache_invalidate_namespace, cache_set

    key_s = str(key)
    value_s = str(value)

    con.execute(
        """
        INSERT INTO portfolio_meta(key, value) VALUES(?,?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """,
        (key_s, value_s),
    )
    cache_set("portfolio_meta", key_s, value_s, ttl_s=3600.0)
    cache_invalidate_namespace("portfolio_snapshot")

def _load_strategy_metrics(con, window_days: int) -> Dict[str, Dict[str, Any]]:
    """
    Returns dict[strategy_name] = {"ts_ms":..., "metrics":{...}}.
    Safe if the metrics table is missing during early bootstrap.
    """
    try:
        rows = con.execute(
            """
            SELECT strategy_name, ts_ms, metrics_json
            FROM strategy_metrics
            WHERE window_days=?
            """,
            (int(window_days),),
        ).fetchall()
    except Exception as e:
        _warn_nonfatal(
            "STRATEGY_SELECTOR_LOAD_METRICS_FAILED",
            e,
            once_key="load_metrics",
            window_days=int(window_days),
        )
        return {}

    out: Dict[str, Dict[str, Any]] = {}
    for name, ts_ms, mj in rows or []:
        try:
            metrics = json.loads(mj) if mj else {}
            out[str(name).strip().lower()] = {"ts_ms": int(ts_ms), "metrics": metrics}
        except Exception as e:
            _warn_nonfatal(
                "STRATEGY_SELECTOR_METRICS_PARSE_FAILED",
                e,
                once_key="metrics_parse",
                name=repr(name)[:120],
            )
            continue
    return out

def _pick_best_from_metrics(metrics_by_name: Dict[str, Dict[str, Any]], now_ms: int) -> str:
    """
    Deterministic selection:
      1) highest net_calmar
      2) highest sharpe_simple
      3) lowest turnover_avg
    Ignores stale rows.
    """
    best_name = "baseline"
    best_key: Optional[Tuple[float, float, float]] = None

    stale_cutoff = int(now_ms) - int(max(0, STRATEGY_METRICS_MAX_AGE_S)) * 1000

    for name in KNOWN_STRATEGIES:
        r = metrics_by_name.get(name)
        if not r:
            continue
        ts_ms = int(r.get("ts_ms") or 0)
        if ts_ms <= 0 or ts_ms < stale_cutoff:
            continue

        m = r.get("metrics") or {}
        net_calmar = float(m.get("net_calmar", 0.0) or 0.0)
        sharpe = float(m.get("sharpe_simple", 0.0) or 0.0)
        turnover = float(m.get("turnover_avg", 0.0) or 0.0)

        # The tuple ordering keeps selection deterministic with no extra tie-break pass.
        key = (net_calmar, sharpe, -turnover)

        if best_key is None or key > best_key:
            best_key = key
            best_name = name

    return best_name

def choose_strategy_name(con, now_ms: int) -> str:
    """
    Returns chosen strategy name.
    Enforces switch cooldown using portfolio_meta.
    """
    requested = (PORTFOLIO_STRATEGY or "baseline").strip().lower()
    if requested not in ("baseline", "conservative", "auto"):
        requested = "baseline"

    if requested == "auto":
        metrics_by_name = _load_strategy_metrics(con, window_days=int(STRATEGY_WINDOW_DAYS))
        desired = _pick_best_from_metrics(metrics_by_name, now_ms=int(now_ms))
    else:
        desired = requested

    last_name = (_get_meta(con, "last_strategy_name") or "").strip().lower() or "baseline"
    last_switch = _get_meta(con, "last_strategy_switch_ts_ms")
    last_switch_ms = 0
    if last_switch:
        try:
            last_switch_ms = int(last_switch)
        except Exception:
            last_switch_ms = 0

    # Cooldown avoids flapping when strategies trade places on similar metrics.
    if desired != last_name:
        cooldown_ms = int(max(0, PORTFOLIO_STRATEGY_SWITCH_COOLDOWN_S)) * 1000
        if cooldown_ms > 0 and (int(now_ms) - int(last_switch_ms)) < cooldown_ms:
            return last_name

        _set_meta(con, "last_strategy_name", desired)
        _set_meta(con, "last_strategy_switch_ts_ms", str(int(now_ms)))
        return desired

    # Seed the metadata once so later cooldown decisions have a stable baseline.
    if not last_switch_ms:
        _set_meta(con, "last_strategy_name", last_name)
        _set_meta(con, "last_strategy_switch_ts_ms", str(int(now_ms)))

    return last_name

def load_strategy_module(name: str):
    """
    Returns the selected strategy module exposing
    `build_desired(alerts, now_ms) -> desired dict`.
    """
    n = (name or "baseline").strip().lower()
    if n == "conservative":
        from engine.strategy.models import conservative as mod
        return mod
    from engine.strategy.models import baseline as mod
    return mod
