from __future__ import annotations

"""Shared emergency controls for live-capital execution."""

import os
import time
from typing import Any, Dict, Optional


DISABLE_LIVE_EXECUTION_ENV = "DISABLE_LIVE_EXECUTION"
DISABLE_LIVE_EXECUTION_REASON = "disable_live_execution_env"
PRELIVE_RECONCILE_ENV = "EXECUTION_PRELIVE_RECONCILE"
PRELIVE_RECONCILE_DISABLED_REASON = "prelive_reconcile_disabled_for_live"
PRELIVE_RECONCILE_BREAK_GLASS_ENV = "EXECUTION_PRELIVE_RECONCILE_BREAK_GLASS"
PRELIVE_RECONCILE_BREAK_GLASS_ACTOR_ENV = "EXECUTION_PRELIVE_RECONCILE_BREAK_GLASS_ACTOR"
PRELIVE_RECONCILE_BREAK_GLASS_REASON_ENV = "EXECUTION_PRELIVE_RECONCILE_BREAK_GLASS_REASON"
PRELIVE_RECONCILE_BREAK_GLASS_ACCEPTED_REASON = "prelive_reconcile_break_glass_accepted"
PRELIVE_RECONCILE_BREAK_GLASS_AUDIT_FAILED_REASON = "prelive_reconcile_break_glass_audit_failed"

_TRUTHY_VALUES = {"1", "true", "t", "yes", "y", "on"}
_FALSEY_VALUES = {"0", "false", "f", "no", "n", "off", "none", "null"}
_PLACEHOLDER_VALUES = {
    "changeme",
    "change-me",
    "default",
    "dummy",
    "example",
    "none",
    "null",
    "placeholder",
    "sample",
    "tbd",
    "test",
    "todo",
    "unset",
}


def env_flag_truthy(name: str, default: bool = False) -> bool:
    """Return True for fail-closed env flags.

    Explicit false-like values are false. Any other non-empty value is treated
    as true so misspellings on emergency controls fail closed.
    """

    raw = os.environ.get(str(name))
    if raw is None:
        return bool(default)
    value = str(raw).strip().lower()
    if value == "":
        return bool(default)
    if value in _FALSEY_VALUES:
        return False
    return True


def _env_flag_strict_true(name: str, default: bool = False) -> bool:
    raw = os.environ.get(str(name))
    if raw is None:
        return bool(default)
    value = str(raw).strip().lower()
    if value == "":
        return bool(default)
    return value in _TRUTHY_VALUES


def _normalize_mode(value: Any = None) -> str:
    return str(value if value is not None else os.environ.get("ENGINE_MODE", "safe") or "safe").strip().lower() or "safe"


def _present_non_placeholder(value: Any) -> bool:
    text = str(value if value is not None else "").strip()
    if not text:
        return False
    lowered = text.lower().replace("_", "-").replace(" ", "")
    return lowered not in _PLACEHOLDER_VALUES


def _prelive_reconcile_enabled() -> bool:
    raw = os.environ.get(PRELIVE_RECONCILE_ENV)
    if raw is None or str(raw).strip() == "":
        return True
    return str(raw).strip().lower() in _TRUTHY_VALUES


def live_execution_disabled() -> bool:
    return env_flag_truthy(DISABLE_LIVE_EXECUTION_ENV, False)


def disabled_live_execution_gate(
    *,
    source: str,
    mode: str = "live",
    armed: Optional[int] = None,
    runtime_state: str = "",
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    raw = str(os.environ.get(DISABLE_LIVE_EXECUTION_ENV, "") or "")
    gate: Dict[str, Any] = {
        "ok": True,
        "ts_ms": int(time.time() * 1000),
        "mode": str(mode or "live"),
        "armed": armed,
        "allow_execution": False,
        "allow_execution_pipeline": False,
        "allow_simulation": False,
        "real_trading_allowed": False,
        "allowed": False,
        "reason": DISABLE_LIVE_EXECUTION_REASON,
        "source": str(source or "live_execution_control"),
        "severity": "CRITICAL",
        "severity_reasons": [DISABLE_LIVE_EXECUTION_REASON],
        "disable_live_execution": True,
        "env": {DISABLE_LIVE_EXECUTION_ENV: raw},
    }
    if runtime_state:
        gate["runtime_state"] = str(runtime_state)
    if extra:
        gate.update(dict(extra))
    return gate


def prelive_reconcile_policy_snapshot(
    *,
    engine_mode: Any = None,
    source: str = "engine.runtime.live_execution_control",
    broker: Optional[str] = None,
) -> Dict[str, Any]:
    """Validate the live-mode pre-live reconciliation safety contract."""

    mode = _normalize_mode(engine_mode)
    required = mode == "live"
    raw_reconcile = str(os.environ.get(PRELIVE_RECONCILE_ENV, "1") or "").strip()
    enabled = _prelive_reconcile_enabled()
    break_glass_raw = str(os.environ.get(PRELIVE_RECONCILE_BREAK_GLASS_ENV, "") or "").strip()
    break_glass_enabled = _env_flag_strict_true(PRELIVE_RECONCILE_BREAK_GLASS_ENV, False)
    actor = str(os.environ.get(PRELIVE_RECONCILE_BREAK_GLASS_ACTOR_ENV, "") or "").strip()
    break_glass_reason = str(os.environ.get(PRELIVE_RECONCILE_BREAK_GLASS_REASON_ENV, "") or "").strip()
    blockers: list[str] = []
    audit: Dict[str, Any] = {}

    if required and not enabled:
        if not break_glass_enabled:
            blockers.append(PRELIVE_RECONCILE_DISABLED_REASON)
        else:
            if not _present_non_placeholder(actor):
                blockers.append(f"{PRELIVE_RECONCILE_BREAK_GLASS_ACTOR_ENV} required")
            if not _present_non_placeholder(break_glass_reason):
                blockers.append(f"{PRELIVE_RECONCILE_BREAK_GLASS_REASON_ENV} required")
            if not blockers:
                audit = {
                    "flag": PRELIVE_RECONCILE_BREAK_GLASS_ENV,
                    "actor": actor,
                    "reason": break_glass_reason,
                    "source": str(source or "engine.runtime.live_execution_control"),
                    "broker": (str(broker) if broker is not None else ""),
                    "ts_ms": int(time.time() * 1000),
                }

    override = bool(required and (not enabled) and break_glass_enabled and audit and not blockers)
    reason = "ok"
    if blockers:
        reason = str(blockers[0])
    elif override:
        reason = PRELIVE_RECONCILE_BREAK_GLASS_ACCEPTED_REASON

    return {
        "ok": not blockers,
        "required": bool(required),
        "mode": mode,
        "enabled": bool(enabled),
        "reason": reason,
        "blockers": list(blockers),
        "override": bool(override),
        "audit": dict(audit),
        "source": str(source or "engine.runtime.live_execution_control"),
        "broker": (str(broker) if broker is not None else ""),
        "env": {
            PRELIVE_RECONCILE_ENV: raw_reconcile,
            PRELIVE_RECONCILE_BREAK_GLASS_ENV: break_glass_raw,
            PRELIVE_RECONCILE_BREAK_GLASS_ACTOR_ENV: actor,
            PRELIVE_RECONCILE_BREAK_GLASS_REASON_ENV: break_glass_reason,
        },
    }


def record_prelive_reconcile_break_glass_audit(
    *,
    source: str,
    broker: Optional[str] = None,
    snapshot: Optional[Dict[str, Any]] = None,
    correlation_id: Optional[str] = None,
) -> Dict[str, Any]:
    state = dict(
        snapshot
        if snapshot is not None
        else prelive_reconcile_policy_snapshot(source=source, broker=broker)
    )
    if not bool(state.get("override")):
        return {"ok": True, "audited": False}

    payload = {
        "control": PRELIVE_RECONCILE_ENV,
        "reason": PRELIVE_RECONCILE_BREAK_GLASS_ACCEPTED_REASON,
        "source": str(source or "engine.runtime.live_execution_control"),
        "broker": (str(broker) if broker is not None else str(state.get("broker") or "")),
        "policy": state,
    }
    try:
        from engine.runtime.event_log import append_event

        event_id = append_event(
            event_type="prelive_reconcile_break_glass",
            event_source=str(source or "engine.runtime.live_execution_control"),
            event_version=1,
            entity_type="execution_safety_control",
            entity_id=PRELIVE_RECONCILE_ENV,
            correlation_id=(str(correlation_id) if correlation_id is not None else None),
            payload=payload,
            best_effort=False,
        )
        return {"ok": True, "audited": True, "event_id": event_id, "payload": payload}
    except Exception as exc:
        return {
            "ok": False,
            "audited": False,
            "reason": PRELIVE_RECONCILE_BREAK_GLASS_AUDIT_FAILED_REASON,
            "error": str(exc),
            "payload": payload,
        }


def prelive_reconcile_policy_gate(
    *,
    source: str,
    engine_mode: Any = None,
    broker: Optional[str] = None,
    audit_override: bool = False,
    correlation_id: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    state = prelive_reconcile_policy_snapshot(
        engine_mode=engine_mode,
        source=source,
        broker=broker,
    )
    if not bool(state.get("ok")):
        return {
            "ok": False,
            "status": str(state.get("reason") or PRELIVE_RECONCILE_DISABLED_REASON),
            "reason": str(state.get("reason") or PRELIVE_RECONCILE_DISABLED_REASON),
            "broker": (str(broker) if broker is not None else ""),
            "fatal_reconcile": True,
            "prelive_reconcile_policy": state,
        }

    if bool(state.get("override")) and bool(audit_override):
        audit = record_prelive_reconcile_break_glass_audit(
            source=source,
            broker=broker,
            snapshot=state,
            correlation_id=correlation_id,
        )
        if not bool(audit.get("ok")):
            return {
                "ok": False,
                "status": PRELIVE_RECONCILE_BREAK_GLASS_AUDIT_FAILED_REASON,
                "reason": PRELIVE_RECONCILE_BREAK_GLASS_AUDIT_FAILED_REASON,
                "broker": (str(broker) if broker is not None else ""),
                "fatal_reconcile": True,
                "prelive_reconcile_policy": state,
                "audit": dict(audit),
            }
    return None


__all__ = [
    "DISABLE_LIVE_EXECUTION_ENV",
    "DISABLE_LIVE_EXECUTION_REASON",
    "PRELIVE_RECONCILE_ENV",
    "PRELIVE_RECONCILE_DISABLED_REASON",
    "PRELIVE_RECONCILE_BREAK_GLASS_ENV",
    "PRELIVE_RECONCILE_BREAK_GLASS_ACTOR_ENV",
    "PRELIVE_RECONCILE_BREAK_GLASS_REASON_ENV",
    "PRELIVE_RECONCILE_BREAK_GLASS_ACCEPTED_REASON",
    "PRELIVE_RECONCILE_BREAK_GLASS_AUDIT_FAILED_REASON",
    "disabled_live_execution_gate",
    "env_flag_truthy",
    "live_execution_disabled",
    "prelive_reconcile_policy_gate",
    "prelive_reconcile_policy_snapshot",
    "record_prelive_reconcile_break_glass_audit",
]
