from __future__ import annotations

"""Central fail-closed checks required before live trading can be enabled."""

import os
from typing import Any, Dict, Optional

from engine.api.auth_config import dashboard_api_token_issue
from engine.execution.broker_failover_policy import (
    broker_startup_preflight,
    live_broker_environment_contract,
)
from engine.runtime.backup_evidence import backup_restore_evidence_snapshot
from engine.runtime.live_execution_control import (
    DISABLE_LIVE_EXECUTION_REASON,
    env_flag_truthy,
    live_execution_disabled,
    prelive_reconcile_policy_snapshot,
)
from engine.runtime.platform import LOOPBACK_HOSTS, default_dashboard_host


_TRUTHY_VALUES = {"1", "true", "yes", "on"}
_FALSEY_VALUES = {"0", "false", "no", "off"}
DEFAULT_LIVE_CONFIRM_PHRASE = "I_UNDERSTAND_LIVE_TRADING"


def _env_bool(name: str, default: bool = False) -> bool:
    raw = str(os.environ.get(name, "")).strip().lower()
    if raw == "":
        return bool(default)
    return raw in _TRUTHY_VALUES


def _env_bool_snapshot(name: str, default: bool = False) -> tuple[bool, bool, str]:
    raw = str(os.environ.get(name, "")).strip()
    lowered = raw.lower()
    if lowered == "":
        return bool(default), False, raw
    if lowered in _TRUTHY_VALUES:
        return True, False, raw
    if lowered in _FALSEY_VALUES:
        return False, False, raw
    return bool(default), True, raw


def _normalize_mode(value: Any, default: str = "safe") -> str:
    mode = str(value or default).strip().lower() or default
    return mode if mode in {"safe", "paper", "shadow", "live", "dev", "development"} else default


def _confirmation_phrase() -> str:
    return DEFAULT_LIVE_CONFIRM_PHRASE


def live_confirmation_snapshot(
    *,
    engine_mode: Optional[str] = None,
    live_confirm: Optional[str] = None,
    require_confirmation: Optional[bool] = None,
) -> Dict[str, Any]:
    """Return the canonical live-confirmation contract.

    Live confirmation is intentionally not runtime-configurable. Operators arm
    it by setting LIVE_TRADING_CONFIRM to the built-in phrase in deployment
    configuration for the target host.
    """

    mode = _normalize_mode(engine_mode if engine_mode is not None else os.environ.get("ENGINE_MODE"), "safe")
    confirm = str(
        live_confirm
        if live_confirm is not None
        else os.environ.get("LIVE_TRADING_CONFIRM", "")
    ).strip()
    phrase = _confirmation_phrase()
    phrase_override = str(os.environ.get("LIVE_TRADING_CONFIRM_PHRASE") or "").strip()
    if require_confirmation is None:
        requested_required, invalid_required, raw_required = _env_bool_snapshot(
            "LIVE_TRADING_REQUIRE_CONFIRMATION",
            True,
        )
    else:
        requested_required = bool(require_confirmation)
        invalid_required = False
        raw_required = "1" if requested_required else "0"

    blockers: list[str] = []
    required = bool(mode == "live")
    if required:
        if invalid_required:
            blockers.append("live_trading_confirmation_requirement_invalid")
        if phrase_override:
            blockers.append("live_trading_confirmation_phrase_override_forbidden")
        if not requested_required:
            blockers.append("live_trading_confirmation_cannot_be_disabled")
        if confirm != phrase:
            blockers.append("live_trading_confirmation_required")

    blockers = list(dict.fromkeys(blockers))
    return {
        "ok": not blockers,
        "required": required,
        "mode": mode,
        "reason": "ok" if not blockers else blockers[0],
        "blockers": blockers,
        "expected_phrase": phrase,
        "configured": bool(confirm),
        "require_confirmation_raw": raw_required,
        "require_confirmation_requested": bool(requested_required),
        "require_confirmation_invalid": bool(invalid_required),
        "phrase_override_configured": bool(phrase_override),
    }


def assert_live_trading_confirmation(
    *,
    engine_mode: Optional[str] = None,
    live_confirm: Optional[str] = None,
    require_confirmation: Optional[bool] = None,
) -> Dict[str, Any]:
    state = live_confirmation_snapshot(
        engine_mode=engine_mode,
        live_confirm=live_confirm,
        require_confirmation=require_confirmation,
    )
    if not bool(state.get("ok")):
        blockers = ",".join(str(item) for item in list(state.get("blockers") or []))
        raise RuntimeError(f"live_trading_confirmation_failed:{blockers}")
    return state


def _execution_mode_state() -> Dict[str, Any]:
    try:
        from engine.cache.wrappers.execution_mode import read_execution_mode

        state = dict(read_execution_mode() or {})
        state.setdefault("source", "engine.cache.wrappers.execution_mode")
        return state
    except Exception as exc:
        return {
            "mode": "",
            "armed": None,
            "source": "unavailable",
            "error": str(exc),
        }


def _initial_kill_switch_hold_snapshot(*, engine_mode: str) -> Dict[str, Any]:
    raw = str(os.environ.get("KILL_SWITCH_GLOBAL", "") or "").strip()
    armed = bool(env_flag_truthy("KILL_SWITCH_GLOBAL", False))
    db_state = _execution_mode_state()
    db_mode = str(db_state.get("mode") or "").strip().lower()
    try:
        db_armed = int(db_state.get("armed") or 0) == 1
    except Exception:
        db_armed = False
    signed_off = bool(db_mode == "live" and db_armed)
    required = bool(str(engine_mode or "").strip().lower() == "live" and not signed_off)
    ok = bool((not required) or armed)
    reason = "ok" if ok else "kill_switch_global_initial_hold_required"
    return {
        "ok": ok,
        "required": required,
        "reason": reason,
        "armed": armed,
        "signed_off": signed_off,
        "db_execution_mode": db_state,
        "env": {"KILL_SWITCH_GLOBAL": raw},
    }


def _live_armed(state: Dict[str, Any]) -> bool:
    mode = str(state.get("mode") or "").strip().lower()
    try:
        armed = int(state.get("armed") or 0) == 1
    except Exception:
        armed = False
    return bool(mode == "live" and armed)


def _execution_arming_audit_snapshot(*, engine_mode: str) -> Dict[str, Any]:
    """Verify live arming came from the persisted audit-chain DB path."""

    mode = _normalize_mode(engine_mode, "safe")
    cached_state = _execution_mode_state()
    cached_live_armed = _live_armed(cached_state)
    required = bool(mode == "live" and cached_live_armed)
    blockers: list[str] = []
    db_state: Dict[str, Any] = {}
    audit_state: Dict[str, Any] = {}

    try:
        from engine.runtime.storage import connect

        con = connect(readonly=True)
        try:
            row = con.execute(
                "SELECT mode, armed, updated_ts_ms, actor, reason FROM execution_mode WHERE id=1"
            ).fetchone()
            if row:
                db_state = {
                    "mode": str(row[0] or ""),
                    "armed": int(row[1] or 0),
                    "updated_ts_ms": int(row[2] or 0),
                    "actor": str(row[3] or ""),
                    "reason": str(row[4] or ""),
                    "source": "engine.runtime.storage",
                }
            else:
                db_state = {
                    "mode": "",
                    "armed": 0,
                    "updated_ts_ms": 0,
                    "actor": "",
                    "reason": "missing",
                    "source": "engine.runtime.storage",
                }

            db_live_armed = _live_armed(db_state)
            required = bool(mode == "live" and (cached_live_armed or db_live_armed))
            if required and not db_live_armed:
                blockers.append("execution_mode_live_armed_not_from_db")

            if db_live_armed:
                audit_row = con.execute(
                    """
                    SELECT ts_ms, prev_mode, new_mode, actor, reason, prev_armed, new_armed, row_hash
                    FROM execution_mode_audit
                    WHERE new_mode='live' AND COALESCE(new_armed, 0)=1
                    ORDER BY ts_ms DESC
                    LIMIT 1
                    """
                ).fetchone()
                if not audit_row:
                    blockers.append("execution_mode_live_armed_audit_missing")
                else:
                    row_hash = audit_row[7]
                    audit_state = {
                        "ts_ms": int(audit_row[0] or 0),
                        "prev_mode": str(audit_row[1] or ""),
                        "new_mode": str(audit_row[2] or ""),
                        "actor": str(audit_row[3] or ""),
                        "reason": str(audit_row[4] or ""),
                        "prev_armed": int(audit_row[5] or 0),
                        "new_armed": int(audit_row[6] or 0),
                        "row_hash_present": bool(row_hash),
                    }
                    mismatches = []
                    if int(audit_state["ts_ms"]) != int(db_state.get("updated_ts_ms") or 0):
                        mismatches.append("ts_ms")
                    if str(audit_state["new_mode"]) != str(db_state.get("mode") or ""):
                        mismatches.append("mode")
                    if int(audit_state["new_armed"]) != int(db_state.get("armed") or 0):
                        mismatches.append("armed")
                    if str(audit_state["actor"]) != str(db_state.get("actor") or ""):
                        mismatches.append("actor")
                    if str(audit_state["reason"]) != str(db_state.get("reason") or ""):
                        mismatches.append("reason")
                    if not bool(audit_state["row_hash_present"]):
                        blockers.append("execution_mode_live_armed_audit_hash_missing")
                    if mismatches:
                        audit_state["mismatches"] = list(mismatches)
                        blockers.append("execution_mode_live_armed_audit_mismatch")
        finally:
            con.close()
    except Exception as exc:
        if required:
            blockers.append("execution_mode_live_armed_audit_unavailable")
        db_state.setdefault("source", "engine.runtime.storage")
        db_state["error"] = str(exc)

    blockers = list(dict.fromkeys(blockers))
    return {
        "ok": not blockers,
        "required": bool(required),
        "mode": mode,
        "reason": "ok" if not blockers else blockers[0],
        "blockers": blockers,
        "cached_execution_mode": dict(cached_state or {}),
        "db_execution_mode": dict(db_state or {}),
        "audit": dict(audit_state or {}),
    }


def live_environment_contract_snapshot(
    *,
    engine_mode: Optional[str] = None,
    execution_mode: Optional[str] = None,
    dashboard_host: Optional[str] = None,
    dashboard_api_token: Optional[str] = None,
    live_confirm: Optional[str] = None,
    require_dashboard_api_token: Optional[bool] = None,
    require_confirmation: Optional[bool] = None,
) -> Dict[str, Any]:
    """Return the live deployment env contract without backup/reconcile gates."""

    mode = _normalize_mode(engine_mode if engine_mode is not None else os.environ.get("ENGINE_MODE"), "safe")
    exec_mode = _normalize_mode(
        execution_mode if execution_mode is not None else os.environ.get("EXECUTION_MODE"),
        "safe",
    )
    token = str(
        dashboard_api_token
        if dashboard_api_token is not None
        else os.environ.get("DASHBOARD_API_TOKEN", "")
    ).strip()
    confirm = str(
        live_confirm
        if live_confirm is not None
        else os.environ.get("LIVE_TRADING_CONFIRM", "")
    ).strip()
    host = str(
        dashboard_host
        if dashboard_host is not None
        else os.environ.get("DASHBOARD_HOST", default_dashboard_host())
    ).strip() or default_dashboard_host()
    require_token = (
        bool(require_dashboard_api_token)
        if require_dashboard_api_token is not None
        else _env_bool("LIVE_TRADING_REQUIRE_DASHBOARD_API_TOKEN", True)
    )

    blockers: list[str] = []
    if mode == "live" and exec_mode != "live":
        blockers.append("execution_mode_live_required")

    broker_contract = live_broker_environment_contract(engine_mode=mode)
    if mode == "live" and not bool(broker_contract.get("ok")):
        blockers.extend(str(item) for item in list(broker_contract.get("blockers") or []))

    broker_preflight = broker_startup_preflight(engine_mode=mode)
    if mode == "live" and not bool(broker_preflight.get("ok")):
        blockers.extend(str(item) for item in list(broker_preflight.get("blockers") or []))

    initial_kill_switch_hold = _initial_kill_switch_hold_snapshot(engine_mode=mode)
    if mode == "live" and not bool(initial_kill_switch_hold.get("ok")):
        blockers.append(str(initial_kill_switch_hold.get("reason") or "kill_switch_global_initial_hold_required"))

    token_issue = dashboard_api_token_issue(token, strict=(mode == "live"))
    if host not in LOOPBACK_HOSTS and not token:
        blockers.append("dashboard_api_token_required_for_remote_bind")
    elif host not in LOOPBACK_HOSTS and token_issue:
        blockers.append(f"dashboard_api_token_invalid_for_remote_bind:{token_issue}")
    if mode == "live" and require_token:
        if not token:
            blockers.append("dashboard_api_token_required_for_live")
        elif token_issue:
            blockers.append(f"dashboard_api_token_invalid_for_live:{token_issue}")
    confirmation = live_confirmation_snapshot(
        engine_mode=mode,
        live_confirm=confirm,
        require_confirmation=require_confirmation,
    )
    if mode == "live" and not bool(confirmation.get("ok")):
        blockers.extend(str(item) for item in list(confirmation.get("blockers") or []))
    phrase = _confirmation_phrase()

    blockers = list(dict.fromkeys(blockers))
    return {
        "ok": not blockers,
        "mode": mode,
        "execution_mode": exec_mode,
        "required": mode == "live",
        "reason": "ok" if not blockers else blockers[0],
        "blockers": blockers,
        "dashboard_host": host,
        "dashboard_api_token_configured": bool(token),
        "dashboard_api_token_issue": token_issue,
        "confirmation_required": bool(confirmation.get("required")),
        "confirmation_phrase": phrase if bool(confirmation.get("required")) else "",
        "confirmation": dict(confirmation or {}),
        "broker_contract": dict(broker_contract or {}),
        "broker_preflight": dict(broker_preflight or {}),
        "initial_kill_switch_hold": dict(initial_kill_switch_hold or {}),
    }


def live_trading_preflight(
    *,
    engine_mode: Optional[str] = None,
    execution_mode: Optional[str] = None,
    dashboard_host: Optional[str] = None,
    dashboard_api_token: Optional[str] = None,
    live_confirm: Optional[str] = None,
    require_dashboard_api_token: Optional[bool] = None,
    require_confirmation: Optional[bool] = None,
) -> Dict[str, Any]:
    """Return the live-trading preflight state without mutating runtime state."""

    mode = _normalize_mode(engine_mode if engine_mode is not None else os.environ.get("ENGINE_MODE"), "safe")
    exec_mode = _normalize_mode(
        execution_mode if execution_mode is not None else os.environ.get("EXECUTION_MODE"),
        "safe",
    )
    token = str(
        dashboard_api_token
        if dashboard_api_token is not None
        else os.environ.get("DASHBOARD_API_TOKEN", "")
    ).strip()
    confirm = str(
        live_confirm
        if live_confirm is not None
        else os.environ.get("LIVE_TRADING_CONFIRM", "")
    ).strip()
    host = str(
        dashboard_host
        if dashboard_host is not None
        else os.environ.get("DASHBOARD_HOST", default_dashboard_host())
    ).strip() or default_dashboard_host()
    require_token = (
        bool(require_dashboard_api_token)
        if require_dashboard_api_token is not None
        else _env_bool("LIVE_TRADING_REQUIRE_DASHBOARD_API_TOKEN", True)
    )
    contract = live_environment_contract_snapshot(
        engine_mode=mode,
        execution_mode=exec_mode,
        dashboard_host=host,
        dashboard_api_token=token,
        live_confirm=confirm,
        require_dashboard_api_token=require_token,
        require_confirmation=require_confirmation,
    )
    blockers = list(contract.get("blockers") or [])
    if mode == "live" and live_execution_disabled():
        blockers.append(DISABLE_LIVE_EXECUTION_REASON)

    prelive_reconcile = prelive_reconcile_policy_snapshot(
        engine_mode=mode,
        source="engine.runtime.live_trading_preflight",
    )
    if mode == "live" and not bool(prelive_reconcile.get("ok")):
        blockers.extend(str(item) for item in list(prelive_reconcile.get("blockers") or []))

    backup_restore_evidence = backup_restore_evidence_snapshot(engine_mode=mode)
    if mode == "live" and not bool(backup_restore_evidence.get("ok")):
        blockers.extend(str(item) for item in list(backup_restore_evidence.get("blockers") or []))

    execution_arming_audit = _execution_arming_audit_snapshot(engine_mode=mode)
    if mode == "live" and not bool(execution_arming_audit.get("ok")):
        blockers.extend(str(item) for item in list(execution_arming_audit.get("blockers") or []))

    phrase = _confirmation_phrase()
    blockers = list(dict.fromkeys(blockers))

    return {
        "ok": not blockers,
        "mode": mode,
        "execution_mode": exec_mode,
        "required": mode == "live",
        "reason": "ok" if not blockers else blockers[0],
        "blockers": blockers,
        "dashboard_host": host,
        "dashboard_api_token_configured": bool(token),
        "confirmation_required": bool((contract.get("confirmation") or {}).get("required")),
        "confirmation_phrase": phrase if bool((contract.get("confirmation") or {}).get("required")) else "",
        "confirmation": dict(contract.get("confirmation") or {}),
        "deployment_contract": dict(contract or {}),
        "prelive_reconcile": dict(prelive_reconcile or {}),
        "broker_contract": dict(contract.get("broker_contract") or {}),
        "broker_preflight": dict(contract.get("broker_preflight") or {}),
        "initial_kill_switch_hold": dict(contract.get("initial_kill_switch_hold") or {}),
        "backup_restore_evidence": dict(backup_restore_evidence or {}),
        "execution_arming_audit": dict(execution_arming_audit or {}),
    }


def assert_dashboard_security_config(
    *,
    engine_mode: Optional[str] = None,
    dashboard_host: Optional[str] = None,
    dashboard_api_token: Optional[str] = None,
    live_confirm: Optional[str] = None,
) -> Dict[str, Any]:
    """Validate dashboard security settings and raise RuntimeError on failure."""

    state = live_trading_preflight(
        engine_mode=engine_mode,
        dashboard_host=dashboard_host,
        dashboard_api_token=dashboard_api_token,
        live_confirm=live_confirm,
    )
    if not bool(state.get("ok")):
        blockers = ",".join(str(x) for x in state.get("blockers") or [])
        raise RuntimeError(f"dashboard_security_preflight_failed:{blockers}")
    return state


__all__ = [
    "DEFAULT_LIVE_CONFIRM_PHRASE",
    "assert_live_trading_confirmation",
    "assert_dashboard_security_config",
    "live_confirmation_snapshot",
    "live_environment_contract_snapshot",
    "live_trading_preflight",
]
