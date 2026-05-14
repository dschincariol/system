"""
FILE: execution_barrier.py

Runtime subsystem module for `execution_barrier`.
"""

# engine/runtime/execution_barrier.py
import json
import logging
from dataclasses import dataclass
from typing import Dict, Any, Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger

LOG = get_logger("runtime.execution_barrier")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="runtime_execution_barrier_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.runtime.execution_barrier",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)

try:
    from engine.runtime.lifecycle_state import get_state as _get_lifecycle_state  # type: ignore
except Exception as e:
    _warn_nonfatal(
        "RUNTIME_EXECUTION_BARRIER_LIFECYCLE_STATE_IMPORT_FAILED",
        e,
        once_key="runtime_execution_barrier_lifecycle_state_import_failed",
    )
    _get_lifecycle_state = None  # type: ignore

try:
    from engine.runtime.risk_state import get_state as _get_risk_state  # type: ignore
except Exception as e:
    _warn_nonfatal(
        "RUNTIME_EXECUTION_BARRIER_RISK_STATE_IMPORT_FAILED",
        e,
        once_key="runtime_execution_barrier_risk_state_import_failed",
    )
    _get_risk_state = None  # type: ignore


@dataclass(frozen=True)
class ExecBarrierDecision:
    allowed: bool
    reason: str
    detail: Dict[str, Any]

def execution_barrier_decide(
    system_state: Dict[str, Any],
    kill_switches: Optional[Dict[str, Any]],
    execution_degraded: bool,
    portfolio_risk_gate: Optional[Dict[str, Any]] = None,
) -> ExecBarrierDecision:
    # This helper is the narrow fail-closed decision primitive. APIs can wrap
    # it, but actual execution should assume deny unless explicitly allowed.
    # Fail-closed defaults
    if not system_state or not isinstance(system_state, dict):
        if callable(_get_lifecycle_state):
            try:
                system_state = _get_lifecycle_state() or {}
            except Exception:
                system_state = {}
        if not system_state or not isinstance(system_state, dict):
            return ExecBarrierDecision(False, "system_state_missing", {})

    st = str(system_state.get("state") or "").strip().upper()
    if st == "WARMING":
        st = "WARMING_UP"
    if st == "SHUTTING_DOWN":
        st = "SHUTDOWN"
    if st == "KILL":
        st = "KILL_SWITCH"

    if "ok" in system_state:
        ok = bool(system_state.get("ok", False))
    else:
        ok = bool(st == "LIVE")

    # Lifecycle state is the first hard gate. Non-LIVE means the control plane
    # has not declared the runtime safe for execution.
    if st != "LIVE":
        return ExecBarrierDecision(False, "system_state_not_live", {"state": st, "ok": ok})

    if not ok:
        return ExecBarrierDecision(False, "system_state_not_ok", {"state": st})

    if kill_switches and isinstance(kill_switches, dict):
        # Any active kill switch is sufficient to block. This keeps local,
        # scoped kill signals from being accidentally ignored by higher layers.
        active = []
        for k, v in kill_switches.items():
            if isinstance(v, dict) and v.get("active"):
                active.append(k)
            elif v is True:
                active.append(k)
        if active:
            return ExecBarrierDecision(False, "kill_switch_active", {"active": active})

    if execution_degraded:
        return ExecBarrierDecision(False, "execution_degraded", {})

    if portfolio_risk_gate and isinstance(portfolio_risk_gate, dict):
        if portfolio_risk_gate.get("blocked"):
            return ExecBarrierDecision(False, "portfolio_risk_gate_block", portfolio_risk_gate)

    if callable(_get_risk_state):
        try:
            portfolio_risk_block = str(_get_risk_state("portfolio_risk_block", "0") or "0").strip()
            if portfolio_risk_block == "1":
                raw = str(_get_risk_state("portfolio_risk_info", "") or "")
                summary_raw = str(_get_risk_state("portfolio_risk_summary", "") or "")
                status = str(_get_risk_state("portfolio_risk_status", "") or "").strip()
                ts_ms = str(_get_risk_state("portfolio_risk_ts_ms", "0") or "0").strip()

                detail: Dict[str, Any] = {}
                if raw:
                    try:
                        parsed = json.loads(raw)
                        if isinstance(parsed, dict):
                            detail = parsed
                        else:
                            detail = {"raw": raw}
                    except Exception as e:
                        _warn_nonfatal(
                            "RUNTIME_EXECUTION_BARRIER_PORTFOLIO_RISK_INFO_PARSE_FAILED",
                            e,
                            once_key="runtime_execution_barrier_portfolio_risk_info_parse_failed",
                        )
                        detail = {"raw": raw}

                summary: Dict[str, Any] = {}
                if summary_raw:
                    try:
                        parsed = json.loads(summary_raw)
                        if isinstance(parsed, dict):
                            summary = parsed
                        else:
                            summary = {"raw": summary_raw}
                    except Exception as e:
                        _warn_nonfatal(
                            "RUNTIME_EXECUTION_BARRIER_PORTFOLIO_RISK_SUMMARY_PARSE_FAILED",
                            e,
                            once_key="runtime_execution_barrier_portfolio_risk_summary_parse_failed",
                        )
                        summary = {"raw": summary_raw}

                return ExecBarrierDecision(
                    False,
                    "portfolio_risk_block",
                    {
                        "status": str(status),
                        "ts_ms": int(ts_ms or "0"),
                        "summary": summary,
                        "info": detail,
                    },
                )
        except Exception as e:
            _warn_nonfatal(
                "RUNTIME_EXECUTION_BARRIER_PORTFOLIO_RISK_STATE_ERROR",
                e,
                once_key="runtime_execution_barrier_portfolio_risk_state_error",
            )
            return ExecBarrierDecision(False, "portfolio_risk_state_error", {})

    return ExecBarrierDecision(True, "ok", {"state": st})

# -------------------------------------------------------------------
# Snapshot adapter for health / API layer
# -------------------------------------------------------------------
# -------------------------------------------------------------------
# Snapshot adapter for health / API layer
# -------------------------------------------------------------------

def execution_gate_snapshot(
    system_state: Optional[Dict[str, Any]] = None,
    kill_switches: Optional[Dict[str, Any]] = None,
    execution_degraded: bool = False,
    portfolio_risk_gate: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Backward-compatible snapshot wrapper expected by API layer.
    Must never raise because health/status endpoints should report denial,
    not fail their own response path.
    """

    try:
        if not system_state and callable(_get_lifecycle_state):
            try:
                system_state = _get_lifecycle_state() or {}
            except Exception as e:
                _warn_nonfatal(
                    "RUNTIME_EXECUTION_BARRIER_LIFECYCLE_STATE_LOAD_FAILED",
                    e,
                    once_key="runtime_execution_barrier_lifecycle_state_load_failed",
                )
                system_state = {}

        decision = execution_barrier_decide(
            system_state=system_state or {},
            kill_switches=kill_switches,
            execution_degraded=execution_degraded,
            portfolio_risk_gate=portfolio_risk_gate,
        )

        return {
            "allowed": bool(decision.allowed),
            "reason": decision.reason,
            "detail": decision.detail,
            "ok": True,
        }

    except Exception as e:
        _warn_nonfatal(
            "RUNTIME_EXECUTION_BARRIER_GATE_SNAPSHOT_FAILED",
            e,
            once_key="runtime_execution_barrier_gate_snapshot_failed",
        )
        return {
            "allowed": False,
            "reason": f"execution_barrier_error: {e}",
            "detail": {},
            "ok": True,
        }
