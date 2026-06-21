"""Broker order-risk controls for shutdown and emergency operator actions."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
import uuid
from typing import Any, Dict, Mapping, Optional

from engine.execution.broker_failover_policy import (
    LIVE_BROKERS,
    canonical_broker_name,
    configured_failover_chain,
)
from engine.execution.order_command_boundary import (
    ensure_order_command_boundary_schema,
    record_order_event,
)
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect_rw_direct


LOG = get_logger("engine.execution.broker_shutdown_risk")

POLICY_ENV = "BROKER_SHUTDOWN_POLICY"
TIMEOUT_ENV = "BROKER_SHUTDOWN_TIMEOUT_S"
FLATTEN_MAX_SYMBOL_QTY_ENV = "BROKER_SHUTDOWN_FLATTEN_MAX_ABS_QTY_PER_SYMBOL"
FLATTEN_MAX_TOTAL_QTY_ENV = "BROKER_SHUTDOWN_FLATTEN_MAX_TOTAL_ABS_QTY"

OBSERVE_ONLY = "observe_only"
CANCEL_ONLY = "cancel_only"
FLATTEN_POSITIONS = "flatten_positions"
CANCEL_AND_FLATTEN = "cancel_and_flatten"

_POLICY_ALIASES = {
    "": "",
    "none": OBSERVE_ONLY,
    "observe": OBSERVE_ONLY,
    "observe_only": OBSERVE_ONLY,
    "observe-only": OBSERVE_ONLY,
    "cancel": CANCEL_ONLY,
    "cancel_only": CANCEL_ONLY,
    "cancel-only": CANCEL_ONLY,
    "cancel_open_orders": CANCEL_ONLY,
    "cancel-open-orders": CANCEL_ONLY,
    "cancel_orders": CANCEL_ONLY,
    "cancel-orders": CANCEL_ONLY,
    "flatten": FLATTEN_POSITIONS,
    "flatten_only": FLATTEN_POSITIONS,
    "flatten-only": FLATTEN_POSITIONS,
    "flatten_positions": FLATTEN_POSITIONS,
    "flatten-positions": FLATTEN_POSITIONS,
    "cancel_flatten": CANCEL_AND_FLATTEN,
    "cancel-flatten": CANCEL_AND_FLATTEN,
    "cancel_and_flatten": CANCEL_AND_FLATTEN,
    "cancel-and-flatten": CANCEL_AND_FLATTEN,
    "emergency_flatten": CANCEL_AND_FLATTEN,
    "emergency-flatten": CANCEL_AND_FLATTEN,
}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _warn_nonfatal(code: str, error: BaseException, **extra: Any) -> None:
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=30,
        component="engine.execution.broker_shutdown_risk",
        extra=extra or None,
        persist=False,
    )


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _safe_timeout(value: Any, default: float) -> float:
    try:
        parsed = float(value)
    except Exception:
        parsed = float(default)
    return max(0.1, min(120.0, float(parsed)))


def _normalize_mode(value: Any = None) -> str:
    return str(value if value is not None else os.environ.get("ENGINE_MODE", "safe") or "safe").strip().lower() or "safe"


def normalize_shutdown_policy(value: Any = None) -> str:
    raw = str(value if value is not None else os.environ.get(POLICY_ENV, "") or "").strip().lower()
    raw = raw.replace(" ", "_")
    return _POLICY_ALIASES.get(raw, raw)


def _policy_has_side_effects(policy: str) -> bool:
    return str(policy or "") in {CANCEL_ONLY, FLATTEN_POSITIONS, CANCEL_AND_FLATTEN}


def broker_shutdown_policy_snapshot(
    *,
    policy: Any = None,
    engine_mode: Any = None,
    require_explicit_live: bool = True,
) -> Dict[str, Any]:
    mode = _normalize_mode(engine_mode)
    raw_policy = str(policy if policy is not None else os.environ.get(POLICY_ENV, "") or "").strip()
    normalized = normalize_shutdown_policy(raw_policy)
    blockers: list[str] = []

    if mode == "live" and bool(require_explicit_live) and not raw_policy:
        blockers.append("broker_shutdown_policy_required_for_live")
    elif not raw_policy:
        normalized = OBSERVE_ONLY
    elif normalized not in {OBSERVE_ONLY, CANCEL_ONLY, FLATTEN_POSITIONS, CANCEL_AND_FLATTEN}:
        blockers.append("broker_shutdown_policy_invalid")

    return {
        "ok": not blockers,
        "required": bool(mode == "live"),
        "mode": mode,
        "raw_policy": raw_policy,
        "policy": normalized if normalized else "",
        "reason": "ok" if not blockers else blockers[0],
        "blockers": blockers,
        "side_effects_allowed": bool(not blockers and _policy_has_side_effects(normalized)),
        "env": {POLICY_ENV: raw_policy},
    }


def flatten_limit_snapshot() -> Dict[str, Any]:
    raw_symbol = str(os.environ.get(FLATTEN_MAX_SYMBOL_QTY_ENV, "") or "").strip()
    raw_total = str(os.environ.get(FLATTEN_MAX_TOTAL_QTY_ENV, "") or "").strip()
    blockers: list[str] = []
    symbol_limit = _safe_float(raw_symbol, 0.0)
    total_limit = _safe_float(raw_total, 0.0)
    if not raw_symbol or symbol_limit <= 0.0:
        blockers.append(f"{FLATTEN_MAX_SYMBOL_QTY_ENV}_required")
    if not raw_total or total_limit <= 0.0:
        blockers.append(f"{FLATTEN_MAX_TOTAL_QTY_ENV}_required")
    return {
        "ok": not blockers,
        "reason": "ok" if not blockers else blockers[0],
        "blockers": blockers,
        "max_abs_qty_per_symbol": float(symbol_limit),
        "max_total_abs_qty": float(total_limit),
        "env": {
            FLATTEN_MAX_SYMBOL_QTY_ENV: raw_symbol,
            FLATTEN_MAX_TOTAL_QTY_ENV: raw_total,
        },
    }


def _json_load_dict(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    try:
        data = json.loads(str(raw or "{}"))
    except Exception:
        return {}
    return dict(data) if isinstance(data, dict) else {}


def _claim_command(
    *,
    command_id: str,
    ts_ms: int,
    broker: str,
    mode: str,
    payload: Mapping[str, Any],
) -> Dict[str, Any]:
    con = connect_rw_direct()
    try:
        ensure_order_command_boundary_schema(con)
        payload_json = json.dumps(dict(payload or {}), separators=(",", ":"), sort_keys=True, default=str)
        cur = con.execute(
            """
            INSERT OR IGNORE INTO order_commands(
              command_id, ts_ms, updated_ts_ms, batch_id, payload_ts_ms, correlation_id,
              mode, broker, payload_source, status, real_order_count, shadow_order_count,
              blocked_order_count, command_json, result_json
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                str(command_id),
                int(ts_ms),
                int(ts_ms),
                None,
                int(ts_ms),
                str(command_id),
                str(mode),
                str(broker),
                "broker_shutdown_risk",
                "started",
                0,
                0,
                0,
                payload_json,
                None,
            ),
        )
        inserted = int(getattr(cur, "rowcount", 0) or 0) > 0
        if inserted:
            con.commit()
            return {"ok": True, "duplicate": False, "status": "started", "command_id": str(command_id)}
        row = con.execute(
            """
            SELECT status, result_json, command_json
            FROM order_commands
            WHERE command_id=?
            LIMIT 1
            """,
            (str(command_id),),
        ).fetchone()
        con.commit()
        if not row:
            return {"ok": False, "duplicate": True, "status": "duplicate_command_unreadable", "command_id": str(command_id)}
        result = _json_load_dict(row[1])
        command = _json_load_dict(row[2])
        return {
            "ok": True,
            "duplicate": True,
            "status": str(row[0] or "started"),
            "command_id": str(command_id),
            "result": result,
            "command": command,
        }
    except Exception:
        try:
            con.rollback()
        except Exception as rollback_error:
            _warn_nonfatal("BROKER_SHUTDOWN_COMMAND_ROLLBACK_FAILED", rollback_error)
        raise
    finally:
        try:
            con.close()
        except Exception as close_error:
            _warn_nonfatal("BROKER_SHUTDOWN_COMMAND_CLOSE_FAILED", close_error)


def _record_command_event(
    *,
    command_id: str,
    broker: str,
    mode: str,
    status: str,
    payload: Mapping[str, Any],
    event_type: str = "broker_shutdown_risk",
) -> None:
    record_order_event(
        ts_ms=_now_ms(),
        command_id=str(command_id),
        correlation_id=str(command_id),
        event_type=str(event_type),
        mode=str(mode or ""),
        broker=str(broker or ""),
        status=str(status or ""),
        payload=dict(payload or {}),
    )


def _resolve_broker(broker: Any = None) -> str:
    explicit = str(broker or "").strip()
    if explicit:
        return canonical_broker_name(explicit)
    for item in configured_failover_chain():
        normalized = canonical_broker_name(item)
        if normalized in LIVE_BROKERS:
            return normalized
    chain = configured_failover_chain()
    return canonical_broker_name(chain[0] if chain else os.environ.get("BROKER_NAME", "sim"))


def _adapter_module(broker: str):
    normalized = canonical_broker_name(broker)
    if normalized == "alpaca":
        from engine.execution import broker_alpaca_rest

        return broker_alpaca_rest
    if normalized == "ibkr":
        from engine.execution import broker_ibkr_gateway

        return broker_ibkr_gateway
    return None


def _remaining_timeout(deadline: float) -> float:
    return max(0.0, float(deadline - time.monotonic()))


def _timed_out(deadline: float) -> bool:
    return _remaining_timeout(deadline) <= 0.0


def list_open_orders_for_broker(*, broker: str, timeout_s: float) -> Dict[str, Any]:
    normalized = canonical_broker_name(broker)
    if normalized == "sim":
        return {"ok": True, "broker": "sim", "status": "no_live_broker", "orders": [], "open_order_count": 0}
    module = _adapter_module(normalized)
    if module is None or not callable(getattr(module, "list_open_orders", None)):
        return {"ok": False, "broker": normalized, "status": "broker_list_open_orders_unavailable"}
    orders = list(module.list_open_orders(timeout_s=float(timeout_s)) or [])
    return {
        "ok": True,
        "broker": normalized,
        "status": "open_orders_listed",
        "orders": orders,
        "open_order_count": int(len(orders)),
    }


def cancel_open_orders_for_broker(*, broker: str, timeout_s: float, command_id: str) -> Dict[str, Any]:
    normalized = canonical_broker_name(broker)
    if normalized == "sim":
        return {"ok": True, "broker": "sim", "status": "no_live_broker", "cancelled_n": 0, "failed_n": 0}
    module = _adapter_module(normalized)
    if module is None or not callable(getattr(module, "cancel_open_orders", None)):
        return {"ok": False, "broker": normalized, "status": "broker_cancel_open_orders_unavailable"}
    return dict(module.cancel_open_orders(timeout_s=float(timeout_s), command_id=str(command_id)) or {})


def _run_reconcile_gate(*, broker: str) -> Dict[str, Any]:
    try:
        from engine.execution.position_reconcile import pre_live_position_reconcile

        gate = dict(pre_live_position_reconcile(str(broker)) or {})
    except Exception as exc:
        return {
            "ok": False,
            "status": "prelive_reconcile_exception",
            "reason": "prelive_reconcile_exception",
            "broker": str(broker),
            "error": f"{type(exc).__name__}: {exc}",
        }
    if not bool(gate.get("ok")):
        return {
            "ok": False,
            "status": str(gate.get("status") or "prelive_reconcile_block"),
            "reason": str(gate.get("reason") or gate.get("status") or "prelive_reconcile_block"),
            "broker": str(broker),
            "reconcile": gate,
        }
    gate_status = str(gate.get("status") or "").strip().lower()
    if gate_status.startswith("skipped") or gate_status in {"disabled", "skipped_disabled"}:
        return {
            "ok": False,
            "status": "prelive_reconcile_required_for_flatten",
            "reason": "prelive_reconcile_required_for_flatten",
            "broker": str(broker),
            "reconcile": gate,
        }
    return {"ok": True, "status": "prelive_reconcile_ok", "broker": str(broker), "reconcile": gate}


def flatten_positions_for_broker(*, broker: str, timeout_s: float, command_id: str) -> Dict[str, Any]:
    normalized = canonical_broker_name(broker)
    if normalized == "sim":
        return {"ok": True, "broker": "sim", "status": "no_live_broker", "submitted_n": 0, "failed_n": 0}

    limits = flatten_limit_snapshot()
    if not bool(limits.get("ok")):
        return {
            "ok": False,
            "broker": normalized,
            "status": "flatten_limits_required",
            "reason": str(limits.get("reason") or "flatten_limits_required"),
            "flatten_limits": limits,
        }

    reconcile = _run_reconcile_gate(broker=normalized)
    if not bool(reconcile.get("ok")):
        return {
            "ok": False,
            "broker": normalized,
            "status": "flatten_reconcile_block",
            "reason": str(reconcile.get("reason") or "flatten_reconcile_block"),
            "reconcile": reconcile,
        }

    module = _adapter_module(normalized)
    if module is None or not callable(getattr(module, "flatten_positions", None)):
        return {"ok": False, "broker": normalized, "status": "broker_flatten_positions_unavailable"}
    return dict(
        module.flatten_positions(
            timeout_s=float(timeout_s),
            command_id=str(command_id),
            max_abs_qty_per_symbol=float(limits.get("max_abs_qty_per_symbol") or 0.0),
            max_total_abs_qty=float(limits.get("max_total_abs_qty") or 0.0),
        )
        or {}
    )


def handle_broker_shutdown_risk(
    *,
    policy: Any = None,
    broker: Any = None,
    engine_mode: Any = None,
    timeout_s: Optional[float] = None,
    command_id: Optional[str] = None,
    actor: str = "runtime",
    reason: str = "runtime_shutdown",
    source: str = "engine.runtime.shutdown",
    require_explicit_live_policy: bool = True,
) -> Dict[str, Any]:
    started_ms = _now_ms()
    mode = _normalize_mode(engine_mode)
    timeout = _safe_timeout(timeout_s if timeout_s is not None else os.environ.get(TIMEOUT_ENV, "10"), 10.0)
    deadline = time.monotonic() + float(timeout)
    normalized_broker = _resolve_broker(broker)
    policy_state = broker_shutdown_policy_snapshot(
        policy=policy,
        engine_mode=mode,
        require_explicit_live=bool(require_explicit_live_policy),
    )
    normalized_policy = str(policy_state.get("policy") or OBSERVE_ONLY)
    resolved_command_id = str(
        command_id
        or f"broker-risk-{normalized_policy}-{normalized_broker}-{started_ms}-{uuid.uuid4().hex[:12]}"
    )
    command_payload = {
        "policy": normalized_policy,
        "policy_state": policy_state,
        "broker": normalized_broker,
        "mode": mode,
        "actor": str(actor or "runtime"),
        "reason": str(reason or "runtime_shutdown"),
        "source": str(source or "engine.runtime.shutdown"),
        "timeout_s": float(timeout),
        "started_ts_ms": int(started_ms),
    }

    try:
        claim = _claim_command(
            command_id=resolved_command_id,
            ts_ms=started_ms,
            broker=normalized_broker,
            mode=mode,
            payload=command_payload,
        )
    except Exception as exc:
        return {
            "ok": False,
            "status": "broker_shutdown_command_audit_failed",
            "broker": normalized_broker,
            "policy": normalized_policy,
            "command_id": resolved_command_id,
            "error": f"{type(exc).__name__}: {exc}",
        }

    if bool(claim.get("duplicate")):
        existing = dict(claim.get("result") or {})
        if existing:
            existing.setdefault("duplicate", True)
            existing.setdefault("command_id", resolved_command_id)
            return existing
        return {
            "ok": False,
            "duplicate": True,
            "status": "duplicate_command_in_progress",
            "broker": normalized_broker,
            "policy": normalized_policy,
            "command_id": resolved_command_id,
        }

    try:
        _record_command_event(
            command_id=resolved_command_id,
            broker=normalized_broker,
            mode=mode,
            status="started",
            payload=command_payload,
            event_type="broker_shutdown_risk_started",
        )

        if not bool(policy_state.get("ok")):
            result = {
                "ok": False,
                "status": str(policy_state.get("reason") or "broker_shutdown_policy_required"),
                "broker": normalized_broker,
                "policy": normalized_policy,
                "command_id": resolved_command_id,
                "policy_state": policy_state,
                "duration_ms": int(_now_ms() - started_ms),
            }
            _record_command_event(
                command_id=resolved_command_id,
                broker=normalized_broker,
                mode=mode,
                status=str(result["status"]),
                payload=result,
            )
            return result

        observe: Dict[str, Any] = {}
        cancel: Dict[str, Any] = {}
        flatten: Dict[str, Any] = {}

        if _timed_out(deadline):
            raise TimeoutError("broker_shutdown_policy_timeout_before_observe")
        observe = list_open_orders_for_broker(
            broker=normalized_broker,
            timeout_s=max(0.1, _remaining_timeout(deadline)),
        )

        if normalized_policy in {CANCEL_ONLY, CANCEL_AND_FLATTEN}:
            if _timed_out(deadline):
                raise TimeoutError("broker_shutdown_policy_timeout_before_cancel")
            cancel = cancel_open_orders_for_broker(
                broker=normalized_broker,
                timeout_s=max(0.1, _remaining_timeout(deadline)),
                command_id=resolved_command_id,
            )
            if not bool(cancel.get("ok")):
                result = {
                    "ok": False,
                    "status": "broker_shutdown_cancel_failed",
                    "broker": normalized_broker,
                    "policy": normalized_policy,
                    "command_id": resolved_command_id,
                    "observe": observe,
                    "cancel": cancel,
                    "duration_ms": int(_now_ms() - started_ms),
                }
                _record_command_event(
                    command_id=resolved_command_id,
                    broker=normalized_broker,
                    mode=mode,
                    status=str(result["status"]),
                    payload=result,
                )
                return result

        if normalized_policy in {FLATTEN_POSITIONS, CANCEL_AND_FLATTEN}:
            if _timed_out(deadline):
                raise TimeoutError("broker_shutdown_policy_timeout_before_flatten")
            flatten = flatten_positions_for_broker(
                broker=normalized_broker,
                timeout_s=max(0.1, _remaining_timeout(deadline)),
                command_id=resolved_command_id,
            )
            if not bool(flatten.get("ok")):
                result = {
                    "ok": False,
                    "status": "broker_shutdown_flatten_failed",
                    "broker": normalized_broker,
                    "policy": normalized_policy,
                    "command_id": resolved_command_id,
                    "observe": observe,
                    "cancel": cancel,
                    "flatten": flatten,
                    "duration_ms": int(_now_ms() - started_ms),
                }
                _record_command_event(
                    command_id=resolved_command_id,
                    broker=normalized_broker,
                    mode=mode,
                    status=str(result["status"]),
                    payload=result,
                )
                return result

        result = {
            "ok": True,
            "status": "broker_shutdown_policy_applied",
            "broker": normalized_broker,
            "policy": normalized_policy,
            "command_id": resolved_command_id,
            "observe": observe,
            "cancel": cancel,
            "flatten": flatten,
            "duration_ms": int(_now_ms() - started_ms),
        }
        if normalized_policy == OBSERVE_ONLY:
            result["status"] = "broker_shutdown_observed"
        _record_command_event(
            command_id=resolved_command_id,
            broker=normalized_broker,
            mode=mode,
            status=str(result["status"]),
            payload=result,
        )
        return result
    except TimeoutError as exc:
        result = {
            "ok": False,
            "status": "broker_shutdown_timeout",
            "broker": normalized_broker,
            "policy": normalized_policy,
            "command_id": resolved_command_id,
            "error": str(exc),
            "timeout_s": float(timeout),
            "duration_ms": int(_now_ms() - started_ms),
        }
        _record_command_event(
            command_id=resolved_command_id,
            broker=normalized_broker,
            mode=mode,
            status="broker_shutdown_timeout",
            payload=result,
        )
        return result
    except Exception as exc:
        result = {
            "ok": False,
            "status": "broker_shutdown_exception",
            "broker": normalized_broker,
            "policy": normalized_policy,
            "command_id": resolved_command_id,
            "error": f"{type(exc).__name__}: {exc}",
            "duration_ms": int(_now_ms() - started_ms),
        }
        try:
            _record_command_event(
                command_id=resolved_command_id,
                broker=normalized_broker,
                mode=mode,
                status="broker_shutdown_exception",
                payload=result,
            )
        except Exception as audit_exc:
            _warn_nonfatal("BROKER_SHUTDOWN_EXCEPTION_AUDIT_FAILED", audit_exc)
        return result


def _json_print(payload: Mapping[str, Any]) -> int:
    print(json.dumps(dict(payload or {}), separators=(",", ":"), sort_keys=True, default=str), flush=True)
    return 0 if bool(payload.get("ok")) else 2


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run broker shutdown/emergency risk controls.")
    parser.add_argument("--policy", default=None)
    parser.add_argument("--broker", default=None)
    parser.add_argument("--engine-mode", default=None)
    parser.add_argument("--timeout-s", type=float, default=None)
    parser.add_argument("--command-id", default=None)
    parser.add_argument("--actor", default="operator")
    parser.add_argument("--reason", default="operator_broker_risk")
    parser.add_argument("--source", default="broker_shutdown_risk_cli")
    args = parser.parse_args(argv)
    if not args.command_id:
        seed = f"{time.time_ns()}:{args.policy}:{args.broker}:{args.actor}:{args.reason}"
        args.command_id = "broker-risk-cli-" + hashlib.sha256(seed.encode("utf-8")).hexdigest()[:16]
    result = handle_broker_shutdown_risk(
        policy=args.policy,
        broker=args.broker,
        engine_mode=args.engine_mode,
        timeout_s=args.timeout_s,
        command_id=args.command_id,
        actor=args.actor,
        reason=args.reason,
        source=args.source,
        require_explicit_live_policy=False,
    )
    return _json_print(result)


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "CANCEL_AND_FLATTEN",
    "CANCEL_ONLY",
    "FLATTEN_POSITIONS",
    "OBSERVE_ONLY",
    "POLICY_ENV",
    "TIMEOUT_ENV",
    "broker_shutdown_policy_snapshot",
    "cancel_open_orders_for_broker",
    "flatten_limit_snapshot",
    "flatten_positions_for_broker",
    "handle_broker_shutdown_risk",
    "list_open_orders_for_broker",
    "normalize_shutdown_policy",
]
