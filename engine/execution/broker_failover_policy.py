from __future__ import annotations

"""Shared live-broker failover and terminal-failure policy."""

import os
from typing import Any, Dict, Mapping, Optional, Sequence


SIM_BROKERS = {"sim", "paper", "sandbox"}
ALPACA_BROKERS = {"alpaca", "alpaca_rest"}
IBKR_BROKERS = {
    "ibkr",
    "interactivebrokers",
    "interactive_brokers",
    "ib_gateway",
    "ibgateway",
    "tws",
}
LIVE_BROKERS = ALPACA_BROKERS | IBKR_BROKERS

_CANONICAL_BROKER = {
    **{name: "sim" for name in SIM_BROKERS},
    **{name: "alpaca" for name in ALPACA_BROKERS},
    **{name: "ibkr" for name in IBKR_BROKERS},
}

NON_RETRYABLE_BROKER_STATUSES = {
    "missing_credentials",
    "credentials_missing",
    "credential_missing",
    "invalid_credentials",
    "credentials_invalid",
    "auth_failed",
    "authentication_failed",
    "authorization_failed",
    "alpaca_auth_failed",
    "broker_auth_failed",
    "ibkr_configuration_invalid",
    "invalid_order_ref",
    "needs_reconcile",
    "prelive_reconcile_exception",
    "prelive_reconcile_unavailable",
    "submission_reconcile_gate_unavailable",
    "submission_unrecorded",
}
NON_RETRYABLE_FAILURE_KINDS = {
    "auth",
    "authentication",
    "authorization",
    "credential",
    "credentials",
    "configuration",
}


def canonical_broker_name(name: Any) -> str:
    broker = str(name or "").strip().lower()
    return _CANONICAL_BROKER.get(broker, broker)


def configured_failover_chain(env: Optional[Mapping[str, str]] = None) -> list[str]:
    source = env if env is not None else os.environ
    raw = str(source.get("BROKER_FAILOVER", "") or "").strip()
    if raw:
        parts = [canonical_broker_name(part) for part in raw.split(",") if str(part or "").strip()]
        if parts:
            return parts

    name = str(source.get("BROKER_NAME", source.get("BROKER", "sim")) or "sim").strip()
    return [canonical_broker_name(name)] if name else ["sim"]


def _normalize_mode(value: Any = None) -> str:
    return str(value if value is not None else os.environ.get("ENGINE_MODE", "safe") or "safe").strip().lower() or "safe"


def validate_live_failover_chain(
    chain: Optional[Sequence[Any]] = None,
    *,
    engine_mode: Any = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    normalized = [canonical_broker_name(item) for item in list(chain if chain is not None else configured_failover_chain())]
    mode = _normalize_mode(engine_mode)
    blockers: list[str] = []
    live_broker = ""

    if mode == "live" and not bool(dry_run):
        live_entries: list[str] = []
        if not normalized:
            blockers.append("live_broker_required")
        for broker in normalized:
            if broker in LIVE_BROKERS:
                live_entries.append(broker)
                if not live_broker:
                    live_broker = broker
            elif broker == "sim" and live_entries:
                blockers.append("sim_after_live_broker_forbidden")
            elif broker == "sim":
                blockers.append("sim_broker_forbidden_in_live")
            else:
                blockers.append("unknown_broker_forbidden_in_live")
        if len(set(live_entries)) > 1:
            blockers.append("mixed_live_broker_chain_forbidden")
        if not live_entries:
            blockers.append("live_broker_required")

    return {
        "ok": not blockers,
        "status": "ok" if not blockers else "live_failover_chain_invalid",
        "reason": "ok" if not blockers else blockers[0],
        "engine_mode": mode,
        "dry_run": bool(dry_run),
        "chain": normalized,
        "blockers": blockers,
        "live_broker": live_broker if blockers else "",
    }


def live_broker_environment_contract(
    *,
    env: Optional[Mapping[str, str]] = None,
    engine_mode: Any = None,
    chain: Optional[Sequence[Any]] = None,
) -> Dict[str, Any]:
    """Validate the live broker env contract without touching broker APIs."""

    source = env if env is not None else os.environ
    mode = _normalize_mode(engine_mode)
    required = mode == "live"
    raw_broker = str(source.get("BROKER", "") or "").strip()
    raw_broker_name = str(source.get("BROKER_NAME", "") or "").strip()
    raw_live_broker = str(source.get("LIVE_BROKER", "") or "").strip()
    raw_intended = str(source.get("INTENDED_LIVE_BROKER", "") or "").strip()
    raw_expected = raw_live_broker or raw_intended
    raw_failover = str(source.get("BROKER_FAILOVER", "") or "").strip()
    broker = canonical_broker_name(raw_broker)
    broker_name = canonical_broker_name(raw_broker_name)
    expected = canonical_broker_name(raw_expected)
    normalized_chain = [
        canonical_broker_name(item)
        for item in list(chain if chain is not None else configured_failover_chain(source))
    ]
    chain_state = validate_live_failover_chain(
        normalized_chain,
        engine_mode=mode,
        dry_run=False,
    )
    blockers: list[str] = []

    if required:
        if not raw_broker:
            blockers.append("broker_required_for_live")
        elif broker not in LIVE_BROKERS:
            blockers.append("broker_must_be_live")

        if not raw_live_broker:
            blockers.append("live_broker_required_for_live")
        elif expected not in LIVE_BROKERS:
            blockers.append("live_broker_expected_invalid")
        elif broker and broker != expected:
            blockers.append("broker_mismatch_expected_live_broker")
        if raw_intended:
            intended = canonical_broker_name(raw_intended)
            if intended != expected:
                blockers.append("intended_live_broker_mismatch_live_broker")

        if not raw_broker_name:
            blockers.append("broker_name_required_for_live")
        elif broker_name not in LIVE_BROKERS:
            blockers.append("broker_name_must_be_live")
        elif broker and broker_name != broker:
            blockers.append("broker_name_mismatch_broker")
        elif expected in LIVE_BROKERS and broker_name != expected:
            blockers.append("broker_name_mismatch_expected_live_broker")

        if not raw_failover:
            blockers.append("broker_failover_required_for_live")

        if not bool(chain_state.get("ok")):
            blockers.extend(str(item) for item in list(chain_state.get("blockers") or []))

        if normalized_chain and broker in LIVE_BROKERS and normalized_chain[0] != broker:
            blockers.append("broker_failover_primary_mismatch")
        if normalized_chain and broker in LIVE_BROKERS and any(item != broker for item in normalized_chain):
            blockers.append("broker_failover_chain_mismatch")
        if normalized_chain and expected in LIVE_BROKERS and any(item != expected for item in normalized_chain):
            blockers.append("broker_failover_chain_mismatch_expected_live_broker")

    blockers = list(dict.fromkeys(blockers))
    return {
        "ok": not blockers,
        "required": bool(required),
        "status": "ok" if not blockers else "live_broker_contract_invalid",
        "reason": "ok" if not blockers else blockers[0],
        "engine_mode": mode,
        "broker": broker,
        "broker_name": broker_name,
        "expected_live_broker": expected,
        "chain": normalized_chain,
        "chain_policy": chain_state,
        "blockers": blockers,
        "env": {
            "BROKER": raw_broker,
            "BROKER_NAME": raw_broker_name,
            "LIVE_BROKER": raw_live_broker,
            "INTENDED_LIVE_BROKER": raw_intended,
            "BROKER_FAILOVER": raw_failover,
        },
    }


def terminal_broker_failure(
    *,
    broker: Any,
    status: str,
    failure_kind: str,
    detail: str = "",
    error: Any = None,
    extra: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "ok": False,
        "status": str(status or "broker_terminal_failure"),
        "broker": canonical_broker_name(broker),
        "failure_kind": str(failure_kind or "configuration"),
        "retryable": False,
        "stop_failover": True,
    }
    if detail:
        out["detail"] = str(detail)
    if error is not None:
        out["error"] = str(error)
    if extra:
        out.update(dict(extra))
    return out


def is_non_retryable_broker_result(res: Optional[Mapping[str, Any]]) -> bool:
    if not isinstance(res, Mapping) or bool(res.get("ok")):
        return False
    if bool(res.get("stop_failover")):
        return True
    status = str(res.get("status") or "").strip().lower()
    failure_kind = str(res.get("failure_kind") or "").strip().lower()
    if status in NON_RETRYABLE_BROKER_STATUSES:
        return True
    if failure_kind in NON_RETRYABLE_FAILURE_KINDS:
        return True
    code = str(res.get("http_status") or res.get("status_code") or "").strip()
    return code in {"401", "403"}


def broker_exception_terminal_failure(*, broker: Any, error: BaseException) -> Optional[Dict[str, Any]]:
    message = str(error or "")
    lowered = message.lower()
    type_name = type(error).__name__.lower()

    if any(token in type_name for token in ("credential", "authentication", "authorization", "auth")) or any(
        token in lowered
        for token in (
            "missing_credentials",
            "credentials missing",
            "missing_required_ibkr_env",
            "unauthorized",
            "forbidden",
            "authentication",
            "authorization",
            "401",
            "403",
        )
    ):
        status = "auth_failed" if any(token in lowered for token in ("unauthorized", "forbidden", "401", "403", "auth")) else "missing_credentials"
        failure_kind = "auth" if status == "auth_failed" else "credential"
        return terminal_broker_failure(
            broker=broker,
            status=status,
            failure_kind=failure_kind,
            detail="broker_credentials_or_auth_failed",
            error=message,
        )
    return None


def broker_startup_preflight(
    *,
    chain: Optional[Sequence[Any]] = None,
    engine_mode: Any = None,
    validate_reachability: Optional[bool] = None,
) -> Dict[str, Any]:
    mode = _normalize_mode(engine_mode)
    normalized = [canonical_broker_name(item) for item in list(chain if chain is not None else configured_failover_chain())]
    chain_state = validate_live_failover_chain(normalized, engine_mode=mode, dry_run=False)
    checks: list[Dict[str, Any]] = []
    blockers: list[str] = list(chain_state.get("blockers") or [])
    environment_contract: Dict[str, Any] = {}

    if mode == "live":
        environment_contract = live_broker_environment_contract(engine_mode=mode, chain=normalized)
        if not bool(environment_contract.get("ok")):
            blockers.extend(str(item) for item in list(environment_contract.get("blockers") or []))
            blockers = list(dict.fromkeys(blockers))
            return {
                "ok": False,
                "status": "broker_startup_preflight_failed",
                "reason": blockers[0] if blockers else "live_broker_contract_invalid",
                "engine_mode": mode,
                "chain": normalized,
                "chain_policy": chain_state,
                "environment_contract": environment_contract,
                "checks": checks,
                "blockers": blockers,
            }

        if "alpaca" in normalized:
            try:
                from engine.execution.broker_alpaca_rest import alpaca_credentials_status

                alpaca_state = dict(alpaca_credentials_status(require_live_endpoint=True) or {})
            except Exception as exc:
                alpaca_state = {
                    "ok": False,
                    "broker": "alpaca",
                    "status": "credential_check_failed",
                    "error": str(exc),
                }
            checks.append(alpaca_state)
            if not bool(alpaca_state.get("ok")):
                blockers.append(str(alpaca_state.get("status") or "alpaca_credentials_invalid"))

        if "ibkr" in normalized:
            reachability = (
                str(os.environ.get("LIVE_PREFLIGHT_VALIDATE_BROKER_REACHABILITY", "1")).strip().lower()
                not in {"0", "false", "no", "off"}
                if validate_reachability is None
                else bool(validate_reachability)
            )
            try:
                from engine.execution.broker_ibkr_gateway import ibkr_startup_preflight

                ibkr_state = dict(ibkr_startup_preflight(validate_reachability=reachability) or {})
            except Exception as exc:
                ibkr_state = {
                    "ok": False,
                    "broker": "ibkr",
                    "status": "ibkr_preflight_failed",
                    "error": str(exc),
                }
            checks.append(ibkr_state)
            if not bool(ibkr_state.get("ok")):
                blockers.extend(str(item) for item in list(ibkr_state.get("blockers") or []))
                if not ibkr_state.get("blockers"):
                    blockers.append(str(ibkr_state.get("status") or "ibkr_preflight_failed"))

    return {
        "ok": not blockers,
        "status": "ok" if not blockers else "broker_startup_preflight_failed",
        "reason": "ok" if not blockers else blockers[0],
        "engine_mode": mode,
        "chain": normalized,
        "chain_policy": chain_state,
        "environment_contract": environment_contract,
        "checks": checks,
        "blockers": list(dict.fromkeys(blockers)),
    }


__all__ = [
    "ALPACA_BROKERS",
    "IBKR_BROKERS",
    "LIVE_BROKERS",
    "NON_RETRYABLE_BROKER_STATUSES",
    "SIM_BROKERS",
    "broker_exception_terminal_failure",
    "broker_startup_preflight",
    "canonical_broker_name",
    "configured_failover_chain",
    "is_non_retryable_broker_result",
    "live_broker_environment_contract",
    "terminal_broker_failure",
    "validate_live_failover_chain",
]
