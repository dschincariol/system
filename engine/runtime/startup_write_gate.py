"""Gate noncritical SQLite writes until the first market-data tick lands."""

from __future__ import annotations

import logging
import os
from typing import Any, Dict

from engine.runtime.state_cache import cache_get, cache_set
from engine.runtime.storage import connect_ro

_DEFER_ENABLED = str(os.environ.get("STARTUP_NONCRITICAL_WRITE_DEFER_ENABLED", "1")).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
_DEFER_CACHE_TTL_S = max(
    0.05,
    float(os.environ.get("STARTUP_NONCRITICAL_WRITE_DEFER_CACHE_TTL_S", "0.5") or 0.5),
)
_DEFER_WAIT_S = max(
    0.05,
    float(os.environ.get("STARTUP_NONCRITICAL_WRITE_DEFER_WAIT_S", "0.5") or 0.5),
)
_CACHE_NS = "startup_noncritical_write_gate"
_LOG = logging.getLogger("engine.runtime.startup_write_gate")


def _cache_key() -> str:
    return str(os.environ.get("DB_PATH") or "").strip() or "<default>"


def _read_runtime_meta_gate_state() -> Dict[str, Any]:
    state: Dict[str, Any] = {
        "enabled": bool(_DEFER_ENABLED),
        "defer": False,
        "reason": "disabled",
        "first_price_ts_ms": "",
        "warmup_started_ts_ms": "",
        "lifecycle_state": "",
    }
    if not _DEFER_ENABLED:
        return state

    con = None
    rows = []
    try:
        con = connect_ro()
        rows = con.execute(
            """
            SELECT key, value
            FROM runtime_meta
            WHERE key IN ('first_price_ts_ms', 'warmup_started_ts_ms', 'lifecycle_state')
            """
        ).fetchall() or []
    except Exception as exc:
        state["reason"] = f"meta_read_failed:{type(exc).__name__}"
        return state
    finally:
        try:
            if con is not None:
                con.close()
        except Exception as close_err:
            _LOG.log(
                logging.WARNING,
                "startup_write_gate_connection_close_failed error=%s",
                f"{type(close_err).__name__}: {close_err}",
            )

    values = {str(row[0] or ""): str(row[1] or "") for row in rows}
    first_price_ts_ms = str(values.get("first_price_ts_ms") or "").strip()
    warmup_started_ts_ms = str(values.get("warmup_started_ts_ms") or "").strip()
    lifecycle_state = str(values.get("lifecycle_state") or "").strip().upper()

    state.update(
        {
            "first_price_ts_ms": first_price_ts_ms,
            "warmup_started_ts_ms": warmup_started_ts_ms,
            "lifecycle_state": lifecycle_state,
        }
    )

    if lifecycle_state in {"SHUTTING_DOWN", "SHUTDOWN", "KILL_SWITCH"}:
        state["reason"] = f"lifecycle_terminal:{lifecycle_state.lower()}"
        return state
    if first_price_ts_ms:
        state["reason"] = "first_price_seen"
        return state
    if lifecycle_state in {"BOOTING", "WARMING", "WARMING_UP"}:
        state["defer"] = True
        state["reason"] = "awaiting_first_price_tick"
        return state
    if warmup_started_ts_ms:
        state["defer"] = True
        state["reason"] = "awaiting_first_price_tick"
        return state

    state["reason"] = "startup_markers_absent"
    return state


def startup_noncritical_write_gate_state(*, force_refresh: bool = False) -> Dict[str, Any]:
    key = _cache_key()
    if not force_refresh:
        cached = cache_get(_CACHE_NS, key)
        if isinstance(cached, dict):
            return dict(cached)
    state = _read_runtime_meta_gate_state()
    cache_set(_CACHE_NS, key, dict(state), ttl_s=float(_DEFER_CACHE_TTL_S))
    return dict(state)


def should_defer_noncritical_startup_write() -> bool:
    return bool(startup_noncritical_write_gate_state().get("defer"))


def noncritical_startup_write_wait_s() -> float:
    return float(_DEFER_WAIT_S)
