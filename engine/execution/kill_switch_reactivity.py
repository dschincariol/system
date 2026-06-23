"""Low-latency kill-switch waits for execution slice boundaries.

Execution authority remains in :func:`engine.execution.kill_switch.execution_allowed`.
This helper only makes sleeps interruptible in-process and bounds cross-process
reaction by polling that authority at a short interval.
"""

from __future__ import annotations

import os
import threading
import time
from typing import Any, Dict, Optional, Tuple

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.metrics import emit_timing
from engine.runtime.storage import connect

LOG = get_logger("engine.execution.kill_switch_reactivity")
_WARNED_NONFATAL_KEYS: set[str] = set()
_WAKE_EVENT = threading.Event()
_LOCAL_KILL_ACTIVE = False
_LAST_KILL_EVENT_TS_MS = 0
_LAST_REACTION_METRIC: Dict[Tuple[str, str, str, str], int] = {}
_REACTION_METRIC_LOCK = threading.Lock()


def _now_ms() -> int:
    return int(time.time() * 1000)


def _reaction_bound_s() -> float:
    try:
        return max(0.05, min(1.0, float(os.environ.get("EXEC_KILL_REACTION_BOUND_S", "1.0") or 1.0)))
    except Exception:
        return 1.0


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=30,
        component="engine.execution.kill_switch_reactivity",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def notify_kill_switch_state_changed(*, enabled: bool, ts_ms: Optional[int] = None) -> None:
    """Wake local slice sleeps after a kill-switch state change."""
    global _LAST_KILL_EVENT_TS_MS, _LOCAL_KILL_ACTIVE
    _LOCAL_KILL_ACTIVE = bool(enabled)
    if bool(enabled):
        _LAST_KILL_EVENT_TS_MS = int(ts_ms or _now_ms())
    _WAKE_EVENT.set()


def _local_kill_block(
    *,
    symbol: Optional[str] = None,
    broker: str = "",
    component: str = "engine.execution",
    stage: str = "slice_boundary",
) -> Tuple[bool, str, Dict[str, Any]] | None:
    """Return a local fail-closed block after an in-process kill notification."""

    if not bool(_LOCAL_KILL_ACTIVE) or int(_LAST_KILL_EVENT_TS_MS or 0) <= 0:
        return None
    latency_ms = emit_kill_reaction_latency(
        activation_ts_ms=int(_LAST_KILL_EVENT_TS_MS),
        broker=str(broker or ""),
        symbol=str(symbol or ""),
        component=str(component or "engine.execution"),
        stage=str(stage or "slice_boundary"),
    )
    meta: Dict[str, Any] = {
        "scope": "global",
        "key": "global",
        "kill_activation_ts_ms": int(_LAST_KILL_EVENT_TS_MS),
        "local_kill_notification": True,
    }
    if latency_ms is not None:
        meta["kill_reaction_latency_ms"] = int(latency_ms)
    return False, "kill_switch_local_notification", meta


def _latest_active_kill_ts_ms(*, con=None, symbol: Optional[str] = None, model_id: Optional[str] = None) -> int:
    own = False
    if con is None:
        con = connect(readonly=True)
        own = True
    try:
        clauses = []
        sym = str(symbol or "").strip().upper()
        if sym:
            clauses.append(("symbol", sym))
        mid = str(model_id or "").strip()
        if mid:
            clauses.append(("model", mid))

        latest = int(_LAST_KILL_EVENT_TS_MS or 0)
        try:
            row = con.execute(
                """
                SELECT updated_ts_ms
                FROM kill_switch_state
                WHERE scope='global' AND enabled=1
                ORDER BY updated_ts_ms DESC
                LIMIT 1
                """
            ).fetchone()
        except Exception:
            row = None
        if row and row[0] is not None:
            latest = max(int(latest), int(row[0] or 0))

        for scope, key in clauses:
            try:
                row = con.execute(
                    """
                    SELECT updated_ts_ms
                    FROM kill_switch_state
                    WHERE scope=? AND key=? AND enabled=1
                    ORDER BY updated_ts_ms DESC
                    LIMIT 1
                    """,
                    (str(scope), str(key)),
                ).fetchone()
            except Exception:
                row = None
            if row and row[0] is not None:
                latest = max(int(latest), int(row[0] or 0))
        return int(latest)
    finally:
        if own:
            try:
                con.close()
            except Exception as e:
                _warn_nonfatal("KILL_SWITCH_REACTIVITY_CLOSE_FAILED", e, once_key="latest_active_close")


def emit_kill_reaction_latency(
    *,
    activation_ts_ms: int,
    broker: str = "",
    symbol: str = "",
    component: str = "engine.execution",
    stage: str = "slice_boundary",
    now_ms: Optional[int] = None,
) -> Optional[int]:
    """Emit ``kill_reaction_latency_ms`` once per activation/stage tuple."""
    activation = int(activation_ts_ms or 0)
    if activation <= 0:
        return None
    observed = int(now_ms or _now_ms())
    latency_ms = max(0, int(observed - activation))
    key = (str(broker or ""), str(symbol or ""), str(component or ""), str(stage or ""))
    with _REACTION_METRIC_LOCK:
        if _LAST_REACTION_METRIC.get(key) == activation:
            return int(latency_ms)
        _LAST_REACTION_METRIC[key] = activation
    emit_timing(
        "kill_reaction_latency_ms",
        int(latency_ms),
        component=str(component or "engine.execution"),
        broker=(str(broker) if broker else None),
        symbol=(str(symbol).upper().strip() if symbol else None),
        extra_tags={"stage": str(stage or "slice_boundary")},
    )
    return int(latency_ms)


def execution_allowed_with_reaction(
    *,
    con=None,
    symbol: Optional[str] = None,
    regime: Optional[str] = None,
    model_id: Optional[str] = None,
    broker: str = "",
    component: str = "engine.execution",
    stage: str = "slice_boundary",
) -> Tuple[bool, str, Dict[str, Any]]:
    """Call the canonical kill gate and emit reaction latency when blocked."""
    from engine.execution.kill_switch import execution_allowed

    allowed, reason, meta = execution_allowed(con=con, symbol=symbol, regime=regime, model_id=model_id)
    if not bool(allowed):
        activation_ts_ms = _latest_active_kill_ts_ms(con=con, symbol=symbol, model_id=model_id)
        latency_ms = emit_kill_reaction_latency(
            activation_ts_ms=activation_ts_ms,
            broker=str(broker or ""),
            symbol=str(symbol or ""),
            component=str(component or "engine.execution"),
            stage=str(stage or "slice_boundary"),
        )
        meta_out = dict(meta or {})
        if activation_ts_ms > 0:
            meta_out["kill_activation_ts_ms"] = int(activation_ts_ms)
        if latency_ms is not None:
            meta_out["kill_reaction_latency_ms"] = int(latency_ms)
        return False, str(reason or "kill_switch_block"), meta_out
    return True, str(reason or "ok"), dict(meta or {})


def wait_with_kill_interrupt(
    *,
    delay_s: float,
    con=None,
    symbol: Optional[str] = None,
    regime: Optional[str] = None,
    model_id: Optional[str] = None,
    broker: str = "",
    component: str = "engine.execution",
    stage: str = "slice_sleep",
) -> Tuple[bool, str, Dict[str, Any]]:
    """Sleep up to ``delay_s`` but re-check kill state within the configured bound."""
    delay = max(0.0, float(delay_s or 0.0))
    local_block = _local_kill_block(
        symbol=symbol,
        broker=broker,
        component=component,
        stage=stage,
    )
    if local_block is not None:
        return local_block
    allowed, reason, meta = execution_allowed_with_reaction(
        con=con,
        symbol=symbol,
        regime=regime,
        model_id=model_id,
        broker=broker,
        component=component,
        stage=stage,
    )
    if not allowed or delay <= 0.0:
        return bool(allowed), str(reason), dict(meta or {})

    deadline = time.monotonic() + float(delay)
    bound = _reaction_bound_s()
    while True:
        remaining = float(deadline - time.monotonic())
        if remaining <= 0.0:
            break
        woke = _WAKE_EVENT.wait(timeout=min(float(bound), max(0.0, remaining)))
        if woke:
            _WAKE_EVENT.clear()
            local_block = _local_kill_block(
                symbol=symbol,
                broker=broker,
                component=component,
                stage=stage,
            )
            if local_block is not None:
                return local_block
        allowed, reason, meta = execution_allowed_with_reaction(
            con=con,
            symbol=symbol,
            regime=regime,
            model_id=model_id,
            broker=broker,
            component=component,
            stage=stage,
        )
        if not allowed:
            return False, str(reason), dict(meta or {})
    return True, "ok", {}
