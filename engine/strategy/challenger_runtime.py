"""Runtime bridge that feeds live shadow evidence into challenger competition.

The challenger runtime listens to market and strategy events, records shadow
orders for non-live candidates, refreshes marketplace snapshots, and publishes a
compact competition summary for operator and governance readers.
"""

import json
import logging
import threading
import time
from typing import Any, Dict

from engine.runtime.event_bus import subscribe_event
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.runtime_meta import meta_set
from engine.strategy.champion_manager import current_competition_snapshot
from engine.strategy.model_marketplace import (
    publish_marketplace_snapshot,
    record_shadow_order,
)
from engine.strategy.universe_selector import select_active_universe

_LOCK = threading.RLock()
_STARTED = False
_LAST_PRICE_BY_SYMBOL: Dict[str, float] = {}
_LAST_TS_BY_SYMBOL: Dict[str, int] = {}
_WARNED_NONFATAL_KEYS: set[str] = set()
LOG = get_logger("strategy.challenger_runtime")


def _now_ms() -> int:
    return int(time.time() * 1000)


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="strategy_challenger_runtime_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.challenger_runtime",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception as e:
        _warn_nonfatal("CHALLENGER_RUNTIME_SAFE_FLOAT_FAILED", e, once_key="safe_float", value=repr(v)[:120])
        return float(default)


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception as e:
        _warn_nonfatal("CHALLENGER_RUNTIME_SAFE_INT_FAILED", e, once_key="safe_int", value=repr(v)[:120])
        return int(default)


def _extract_payload(event: Dict[str, Any]) -> Dict[str, Any]:
    payload = event.get("payload")
    return payload if isinstance(payload, dict) else {}


def _publish_runtime_meta() -> None:
    active_symbols = select_active_universe(limit=25)
    snap = current_competition_snapshot(active_symbols=active_symbols)
    meta_set(
        "competition_runtime", json.dumps(snap, separators=(",", ":"), sort_keys=True)
    )


def _on_price_tick(event: Dict[str, Any]) -> None:
    payload = _extract_payload(event)
    symbol = str(payload.get("symbol") or "").upper().strip()
    if not symbol:
        return

    price = payload.get("price")
    ts_ms = _safe_int(payload.get("ts_ms") or event.get("ts_ms") or _now_ms())

    if price in (None, ""):
        return
    fpx = _safe_float(price, 0.0)
    if fpx <= 0.0:
        return

    with _LOCK:
        _LAST_PRICE_BY_SYMBOL[symbol] = fpx
        _LAST_TS_BY_SYMBOL[symbol] = int(ts_ms)


def _on_strategy_signal(event: Dict[str, Any]) -> None:
    payload = _extract_payload(event)
    symbol = str(payload.get("symbol") or "").upper().strip()
    if not symbol:
        return

    model_name = str(
        payload.get("model_name")
        or payload.get("strategy_name")
        or payload.get("strategy")
        or "default_challenger"
    ).strip()

    signal = str(payload.get("signal") or payload.get("side") or "").strip().lower()
    confidence = _safe_float(payload.get("confidence"), 0.0)
    horizon_s = _safe_int(payload.get("horizon_s"), 0)
    regime = str(payload.get("regime") or "global").strip() or "global"
    source_alert_id = payload.get("source_alert_id")

    with _LOCK:
        ref_price = _LAST_PRICE_BY_SYMBOL.get(symbol)
        last_ts_ms = _LAST_TS_BY_SYMBOL.get(symbol, 0)

    if ref_price is None:
        return

    if signal in ("buy", "long", "bullish", "1"):
        side = "buy"
    elif signal in ("sell", "short", "bearish", "-1"):
        side = "sell"
    else:
        side = "hold"

    qty = 0.0 if side == "hold" else max(1.0, round(confidence * 10.0, 4))

    record_shadow_order(
        model_name=model_name,
        symbol=symbol,
        side=side,
        qty=qty,
        ref_price=float(ref_price),
        confidence=float(confidence),
        horizon_s=int(horizon_s),
        regime=regime,
        meta={
            "event_ts_ms": int(event.get("ts_ms") or _now_ms()),
            "last_price_ts_ms": int(last_ts_ms),
            "signal_ref_price": float(ref_price),
            "signal_ref_ts_ms": int(last_ts_ms),
            "source_alert_id": (
                _safe_int(source_alert_id) if source_alert_id is not None else None
            ),
            "signal": str(signal or ""),
        },
    )

    publish_marketplace_snapshot(active_symbols=select_active_universe(limit=25))
    _publish_runtime_meta()


def start_challenger_runtime() -> Dict[str, Any]:
    global _STARTED

    with _LOCK:
        if _STARTED:
            snap = current_competition_snapshot(
                active_symbols=select_active_universe(limit=25)
            )
            return {"ok": True, "started": False, "snapshot": snap}
        _STARTED = True

    # Price ticks establish the reference prices used to mark shadow orders,
    # while strategy signals create the challenger observations those prices are
    # later evaluated against.
    subscribe_event("price_tick", _on_price_tick)
    subscribe_event("strategy_signal", _on_strategy_signal)

    snap = current_competition_snapshot(active_symbols=select_active_universe(limit=25))
    meta_set(
        "competition_runtime", json.dumps(snap, separators=(",", ":"), sort_keys=True)
    )
    return {"ok": True, "started": True, "snapshot": snap}
