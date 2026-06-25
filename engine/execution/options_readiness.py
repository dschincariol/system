"""Options-instrument live readiness gates and shadow scaffolding.

The current broker adapters submit equity-style orders only. Options can be
used as shadow/paper intents for research, but live option order flow must fail
closed until a concrete broker adapter and all risk controls are present.
"""

from __future__ import annotations

import math
import os
import re
import time
from datetime import datetime, timezone
from importlib import import_module
from typing import Any, Callable, Iterable, Mapping, Optional, Sequence

try:
    from engine.data.options_instrument import parse_option_symbol  # type: ignore
except Exception:

    def parse_option_symbol(symbol: object):  # type: ignore
        return None


TRUTHY = {"1", "true", "t", "yes", "y", "on"}
FALSEY = {"0", "false", "f", "no", "n", "off"}
LIVE_BROKERS = {
    "alpaca",
    "alpaca_rest",
    "ibkr",
    "interactivebrokers",
    "interactive_brokers",
    "ib_gateway",
    "ibgateway",
    "tws",
    "tradier_options",
}
OPTIONS_MODES = {"disabled", "shadow", "paper", "live"}

# Keeping this list in code prevents an env-only toggle from enabling stock
# adapters to receive option contracts.
LIVE_OPTIONS_BROKER_ADAPTERS: frozenset[str] = frozenset({"tradier_options"})

CONTROL_FLAG_GROUPS: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    ("greeks", "options_live_greeks_gate_missing", ("OPTIONS_LIVE_GREEKS_READY", "OPTIONS_GREEKS_REQUIRED")),
    (
        "liquidity_filters",
        "options_live_liquidity_filters_missing",
        ("OPTIONS_LIVE_LIQUIDITY_FILTERS_READY", "OPTIONS_LIQUIDITY_FILTERS_ENABLED"),
    ),
    (
        "bid_ask_quality",
        "options_live_bid_ask_quality_missing",
        ("OPTIONS_LIVE_BID_ASK_QUALITY_READY", "OPTIONS_BID_ASK_QUALITY_ENABLED"),
    ),
    (
        "assignment_exercise",
        "options_live_assignment_exercise_missing",
        ("OPTIONS_LIVE_ASSIGNMENT_EXERCISE_READY", "OPTIONS_ASSIGNMENT_EXERCISE_HANDLING_ENABLED"),
    ),
    (
        "expiration_risk",
        "options_live_expiration_risk_missing",
        ("OPTIONS_LIVE_EXPIRATION_RISK_READY", "OPTIONS_EXPIRATION_RISK_ENABLED"),
    ),
    (
        "margin_impact",
        "options_live_margin_impact_missing",
        ("OPTIONS_LIVE_MARGIN_IMPACT_READY", "OPTIONS_MARGIN_IMPACT_ENABLED"),
    ),
    (
        "broker_support",
        "options_live_broker_support_missing",
        ("OPTIONS_LIVE_BROKER_SUPPORT_READY", "OPTIONS_BROKER_SUPPORT_ENABLED"),
    ),
    (
        "position_limits",
        "options_live_position_limits_missing",
        ("OPTIONS_LIVE_POSITION_LIMITS_READY", "OPTIONS_POSITION_LIMITS_ENABLED"),
    ),
    (
        "kill_switch_integration",
        "options_live_kill_switch_integration_missing",
        ("OPTIONS_LIVE_KILL_SWITCH_INTEGRATION_READY", "OPTIONS_KILL_SWITCH_INTEGRATION_ENABLED"),
    ),
)

NUMERIC_CONTROLS: tuple[tuple[str, str, float | None, float | None], ...] = (
    ("OPTIONS_MIN_OPEN_INTEREST", "options_live_min_open_interest_missing", 0.0, None),
    ("OPTIONS_MIN_VOLUME", "options_live_min_volume_missing", 0.0, None),
    ("OPTIONS_MAX_SPREAD_BPS", "options_live_max_spread_bps_missing", 0.0, None),
    ("OPTIONS_MIN_DTE_DAYS", "options_live_min_dte_missing", 0.0, None),
    ("OPTIONS_MAX_DTE_DAYS", "options_live_max_dte_missing", 0.0, None),
    ("OPTIONS_MAX_POSITION_CONTRACTS", "options_live_position_contract_limit_missing", 0.0, None),
    ("OPTIONS_MARGIN_IMPACT_MAX_FRACTION", "options_live_margin_limit_missing", 0.0, 1.0),
    ("OPTIONS_MAX_PORTFOLIO_DELTA_ABS", "options_live_portfolio_delta_limit_missing", 0.0, None),
    ("OPTIONS_MAX_PORTFOLIO_GAMMA_ABS", "options_live_portfolio_gamma_limit_missing", 0.0, None),
    ("OPTIONS_MAX_PORTFOLIO_VEGA_ABS", "options_live_portfolio_vega_limit_missing", 0.0, None),
)

_OCC_COMPACT_RE = re.compile(r"^[A-Z]{1,6}\d{6}[CP]\d{8}$")


def _env_text(*names: str, default: str = "") -> str:
    for name in names:
        raw = os.environ.get(str(name))
        if raw is not None and str(raw).strip():
            return str(raw).strip()
    return str(default).strip()


def _env_bool(*names: str, default: bool = False) -> bool:
    raw = _env_text(*names)
    if not raw:
        return bool(default)
    text = raw.strip().lower()
    if text in FALSEY:
        return False
    if text in TRUTHY:
        return True
    return bool(default)


def _safe_float(value: Any, default: float = math.nan) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    return float(out) if math.isfinite(out) else float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _dedupe(values: Iterable[Any]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in values:
        text = str(raw or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def options_instruments_mode() -> str:
    mode = _env_text("OPTIONS_INSTRUMENTS_MODE", "OPTIONS_AS_INSTRUMENTS_MODE", default="shadow").lower()
    return mode if mode in OPTIONS_MODES else "invalid"


def live_options_requested() -> bool:
    mode = options_instruments_mode()
    return mode == "live" or _env_bool(
        "OPTIONS_LIVE_ORDERS_ENABLED",
        "OPTIONS_ENABLE_LIVE_ORDERS",
        "OPTIONS_AS_INSTRUMENTS_LIVE",
        default=False,
    )


def _nested_dicts(order: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    out: list[Mapping[str, Any]] = [order]
    for key in ("option", "options", "option_contract", "contract", "instrument", "instrument_details"):
        child = order.get(key)
        if isinstance(child, Mapping):
            out.append(child)
    explain = order.get("explain")
    if isinstance(explain, Mapping):
        out.append(explain)
        for key in ("option", "options", "option_contract", "instrument", "model_intent"):
            child = explain.get(key)
            if isinstance(child, Mapping):
                out.append(child)
    return out


def _first_text(order: Mapping[str, Any], keys: Sequence[str]) -> str:
    for container in _nested_dicts(order):
        for key in keys:
            raw = container.get(key)
            if raw is None:
                continue
            text = str(raw).strip()
            if text:
                return text
    return ""


def _first_number(order: Mapping[str, Any], keys: Sequence[str]) -> float:
    for container in _nested_dicts(order):
        for key in keys:
            if key not in container or container.get(key) in (None, ""):
                continue
            value = _safe_float(container.get(key), math.nan)
            if math.isfinite(value):
                return float(value)
    return math.nan


def is_options_order(order: Any) -> bool:
    if not isinstance(order, Mapping):
        return False
    instrument_type = _first_text(order, ("instrument_type", "instrument", "security_type", "sec_type", "asset_class")).lower()
    if instrument_type in {"option", "options", "us_option", "option_contract", "derivative_option", "opt"}:
        return True
    contract_type = _first_text(order, ("contract_type", "option_type", "right", "put_call", "call_put")).lower()
    if contract_type in {"call", "put", "c", "p"} and (
        _first_text(order, ("expiration", "expiry", "expiration_date", "maturity"))
        or math.isfinite(_first_number(order, ("strike", "strike_price")))
    ):
        return True
    contract_symbol = _first_text(order, ("option_symbol", "option_contract", "contract", "occ_symbol", "local_symbol"))
    if contract_symbol and (
        _OCC_COMPACT_RE.match(contract_symbol.upper().replace(" ", ""))
        or parse_option_symbol(contract_symbol) is not None
    ):
        return True
    return False


def _normalize_contract_type(value: str) -> str:
    text = str(value or "").strip().lower()
    if text in {"c", "call"}:
        return "call"
    if text in {"p", "put"}:
        return "put"
    return text


def option_order_metadata(order: Mapping[str, Any]) -> dict[str, Any]:
    contract = _first_text(order, ("option_symbol", "option_contract", "contract", "occ_symbol", "local_symbol"))
    underlying = _first_text(order, ("underlying", "underlying_symbol", "root_symbol", "symbol"))
    expiration = _first_text(order, ("expiration", "expiry", "expiration_date", "maturity"))
    contract_type = _normalize_contract_type(_first_text(order, ("contract_type", "option_type", "right", "put_call", "call_put")))
    strike = _first_number(order, ("strike", "strike_price"))
    parsed_contract = parse_option_symbol(contract)
    if parsed_contract is not None:
        if not underlying:
            underlying = parsed_contract.underlying
        if not expiration:
            expiration = parsed_contract.expiry.isoformat()
        if not contract_type:
            contract_type = _normalize_contract_type(parsed_contract.right)
        if not math.isfinite(strike):
            strike = float(parsed_contract.strike)
    greeks = {
        "delta": _first_number(order, ("delta", "option_delta")),
        "gamma": _first_number(order, ("gamma", "option_gamma")),
        "theta": _first_number(order, ("theta", "option_theta")),
        "vega": _first_number(order, ("vega", "option_vega")),
    }
    return {
        "instrument_type": "option",
        "contract": contract,
        "underlying": underlying.upper() if underlying else "",
        "expiration": expiration,
        "contract_type": contract_type,
        "strike": (float(strike) if math.isfinite(strike) else None),
        "bid": _finite_or_none(_first_number(order, ("bid", "bid_px", "bid_price"))),
        "ask": _finite_or_none(_first_number(order, ("ask", "ask_px", "ask_price"))),
        "mid": _finite_or_none(_first_number(order, ("mid", "mid_px", "mark", "mark_price", "ref_price"))),
        "open_interest": _finite_or_none(_first_number(order, ("open_interest", "oi"))),
        "volume": _finite_or_none(_first_number(order, ("volume", "day_volume"))),
        "greeks": {key: _finite_or_none(value) for key, value in greeks.items()},
        "margin_impact_fraction": _finite_or_none(
            _first_number(order, ("margin_impact_fraction", "estimated_margin_fraction", "margin_fraction"))
        ),
        "assignment_exercise_policy": _first_text(order, ("assignment_exercise_policy", "exercise_policy", "assignment_policy")),
    }


def _finite_or_none(value: float) -> float | None:
    return float(value) if math.isfinite(value) else None


def _days_to_expiration(expiration: Any, *, now_ms: int | None = None) -> float | None:
    text = str(expiration or "").strip()
    if not text:
        return None
    try:
        dt = datetime.strptime(text[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except Exception:
        return None
    ref_ms = int(now_ms if now_ms is not None else time.time() * 1000)
    return float((dt.timestamp() * 1000.0 - float(ref_ms)) / 86_400_000.0)


def _numeric_control_snapshot() -> tuple[list[str], dict[str, Any]]:
    blockers: list[str] = []
    controls: dict[str, Any] = {}
    for name, blocker, minimum, maximum in NUMERIC_CONTROLS:
        raw = _env_text(name)
        value = _safe_float(raw, math.nan)
        valid = bool(raw and math.isfinite(value))
        if valid and minimum is not None and value <= float(minimum):
            valid = False
        if valid and maximum is not None and value > float(maximum):
            valid = False
        controls[name] = {
            "configured": bool(raw),
            "ok": bool(valid),
            "value": (float(value) if math.isfinite(value) else None),
        }
        if not valid:
            blockers.append(blocker if not raw else f"{blocker}:invalid")
    return blockers, controls


def _check_failed_blocker(blocker: str) -> str:
    text = str(blocker or "").strip()
    if text.endswith("_missing"):
        return f"{text[:-8]}_check_failed"
    return f"{text}_check_failed"


def _option_qty(order: Mapping[str, Any]) -> float:
    return abs(_first_number(order, ("contracts", "qty", "quantity", "order_qty", "target_contracts")))


def _option_side_sign(order: Mapping[str, Any]) -> float:
    raw_qty = _first_number(order, ("contracts", "qty", "quantity", "order_qty", "target_contracts"))
    if math.isfinite(raw_qty) and raw_qty < 0.0:
        return -1.0
    side = _first_text(order, ("tradier_side", "side", "action", "order_side")).strip().lower()
    if side in {"sell", "short", "sell_short", "sell_to_open", "sell_to_close"}:
        return -1.0
    return 1.0


def _option_multiplier(contract: Any) -> float | None:
    try:
        parsed = parse_option_symbol(contract)
        multiplier = float(getattr(parsed, "multiplier", 0.0) or 0.0) if parsed is not None else 0.0
        multiplier_source = str(getattr(parsed, "multiplier_source", "") or "").strip() if parsed is not None else ""
        specs_verified = bool(getattr(parsed, "contract_specs_verified", False)) if parsed is not None else False
        if multiplier > 0.0 and (specs_verified or multiplier_source):
            return float(multiplier)
        return None
    except Exception:
        return None


def _option_underlyings(orders: Sequence[Mapping[str, Any]]) -> list[str]:
    out: list[str] = []
    for order in list(orders or []):
        if not is_options_order(order):
            continue
        meta = option_order_metadata(order)
        underlying = str(meta.get("underlying") or "").upper().strip()
        if underlying:
            out.append(underlying)
    return _dedupe(out)


def _first_order_scope(orders: Sequence[Mapping[str, Any]]) -> dict[str, str]:
    for order in list(orders or []):
        if not is_options_order(order):
            continue
        meta = option_order_metadata(order)
        symbol = str(meta.get("contract") or _first_text(order, ("symbol", "option_symbol")) or "").upper().strip()
        return {
            "symbol": symbol,
            "regime": _first_text(order, ("regime", "market_regime")),
            "model_id": _first_text(order, ("model_id", "model", "strategy_id")),
        }
    return {"symbol": "", "regime": "", "model_id": ""}


def _option_order_greek_snapshot(orders: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    net_delta = 0.0
    net_gamma = 0.0
    net_theta = 0.0
    net_vega = 0.0
    gross_contracts = 0.0
    max_position_contracts = 0.0
    margin_impact_fraction = 0.0
    by_symbol: dict[str, Any] = {}
    missing_greeks: list[str] = []
    missing_multiplier: list[str] = []

    for order in list(orders or []):
        if not is_options_order(order):
            continue
        meta = option_order_metadata(order)
        contract = str(meta.get("contract") or _first_text(order, ("symbol", "option_symbol")) or "").upper().strip()
        if not contract:
            continue
        qty = _option_qty(order)
        if not math.isfinite(qty) or qty <= 0.0:
            continue
        signed_contracts = float(qty) * _option_side_sign(order)
        greeks = dict(meta.get("greeks") or {})
        if any(greeks.get(name) is None for name in ("delta", "gamma", "theta", "vega")):
            missing_greeks.append(contract)
            continue
        multiplier = _option_multiplier(meta.get("contract"))
        if multiplier is None:
            missing_multiplier.append(contract)
            continue
        delta = float(greeks["delta"])
        gamma = float(greeks["gamma"])
        theta = float(greeks["theta"])
        vega = float(greeks["vega"])
        margin_fraction = float(meta.get("margin_impact_fraction") or 0.0)

        delta_contribution = signed_contracts * delta * multiplier
        gamma_contribution = signed_contracts * gamma * multiplier
        theta_contribution = signed_contracts * theta * multiplier
        vega_contribution = signed_contracts * vega * multiplier
        net_delta += delta_contribution
        net_gamma += gamma_contribution
        net_theta += theta_contribution
        net_vega += vega_contribution
        gross_contracts += abs(signed_contracts)
        max_position_contracts = max(max_position_contracts, abs(signed_contracts))
        margin_impact_fraction += max(0.0, margin_fraction)
        by_symbol[contract] = {
            "signed_contracts": float(signed_contracts),
            "gross_contracts": float(abs(signed_contracts)),
            "contract_multiplier": float(multiplier),
            "delta": delta,
            "gamma": gamma,
            "theta": theta,
            "vega": vega,
            "net_delta": float(delta_contribution),
            "net_gamma": float(gamma_contribution),
            "net_theta": float(theta_contribution),
            "net_vega": float(vega_contribution),
            "margin_impact_fraction": float(max(0.0, margin_fraction)),
        }

    return {
        "net_delta": float(net_delta),
        "net_gamma": float(net_gamma),
        "net_theta": float(net_theta),
        "net_vega": float(net_vega),
        "gross_contracts": float(gross_contracts),
        "max_position_contracts": float(max_position_contracts),
        "margin_impact_fraction": float(margin_impact_fraction),
        "by_symbol": dict(sorted(by_symbol.items())),
        "missing_greeks": sorted(missing_greeks),
        "missing_multiplier": sorted(missing_multiplier),
        "enabled": True,
    }


def _portfolio_risk_predicate(kind: str, context: Mapping[str, Any]) -> tuple[bool, dict[str, Any]]:
    try:
        risk = import_module("engine.risk.portfolio_risk_engine")
        apply_entry = getattr(risk, "apply_portfolio_risk_engine", None)
        post_checks_fn = getattr(risk, "_post_constraint_checks", None)
    except Exception as exc:
        return False, {"provider": "portfolio_risk_engine", "available": False, "error": str(exc)}

    if not callable(apply_entry) or not callable(post_checks_fn):
        return False, {
            "provider": "portfolio_risk_engine",
            "available": False,
            "apply_entrypoint": callable(apply_entry),
            "post_constraint_checker": callable(post_checks_fn),
        }

    orders = [dict(order) for order in list(context.get("orders") or []) if isinstance(order, Mapping)]
    snapshot = _option_order_greek_snapshot(orders)
    checks = dict(post_checks_fn({"gross": 0.0, "net": 0.0, "options_greeks": snapshot}) or {})
    missing_greeks = list(snapshot.get("missing_greeks") or [])
    missing_multiplier = list(snapshot.get("missing_multiplier") or [])
    violations = dict(checks.get("options_greek_violations") or {})
    if kind == "greeks":
        ok = not missing_greeks and not missing_multiplier and bool(checks.get("options_greeks_within_cap", True))
    elif kind == "margin_impact":
        ok = "margin_impact_fraction" not in violations
    elif kind == "position_limits":
        ok = "max_position_contracts" not in violations
    else:
        ok = bool(checks.get("options_greeks_within_cap", True))
    return bool(ok), {
        "provider": "portfolio_risk_engine",
        "available": True,
        "entrypoint": "apply_portfolio_risk_engine",
        "checker": "_post_constraint_checks",
        "kind": str(kind),
        "checks": checks,
        "options_greeks": snapshot,
    }


def _storage_connect_readonly():
    storage = import_module("engine.runtime.storage")
    connect = getattr(storage, "connect", None)
    if not callable(connect):
        raise RuntimeError("storage_connect_unavailable")
    return connect(readonly=True)


def _data_quality_predicate(kind: str, context: Mapping[str, Any]) -> tuple[bool, dict[str, Any]]:
    try:
        dq = import_module("engine.data.options_data_quality")
        compute = getattr(dq, "compute_options_data_quality", None)
        is_ok = getattr(dq, "options_data_quality_ok", None)
    except Exception as exc:
        return False, {"provider": "options_data_quality", "available": False, "error": str(exc)}

    if not callable(compute) or not callable(is_ok):
        return False, {
            "provider": "options_data_quality",
            "available": False,
            "compute_entrypoint": callable(compute),
            "ok_helper": callable(is_ok),
        }

    orders = [dict(order) for order in list(context.get("orders") or []) if isinstance(order, Mapping)]
    symbols = _option_underlyings(orders) or None
    con = None
    try:
        con = _storage_connect_readonly()
        report = dict(compute(con, now_ms=int(context.get("now_ms") or time.time() * 1000), symbols=symbols) or {})
    except Exception as exc:
        return False, {
            "provider": "options_data_quality",
            "available": True,
            "kind": str(kind),
            "symbols": symbols or [],
            "error": str(exc),
        }
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:  # no-op-guard: allow - read-only DQ connection cleanup is best-effort.
                pass

    ok = bool(is_ok(report))
    if kind == "bid_ask_quality":
        providers = dict(report.get("providers") or {})
        completeness = [
            float((stats or {}).get("bid_ask_complete_fraction") or 0.0)
            for stats in providers.values()
            if isinstance(stats, Mapping)
        ]
        ok = bool(ok and completeness and min(completeness) > 0.0)
    return bool(ok), {
        "provider": "options_data_quality",
        "available": True,
        "kind": str(kind),
        "symbols": symbols or [],
        "report": report,
    }


def _lifecycle_predicate(kind: str, _context: Mapping[str, Any]) -> tuple[bool, dict[str, Any]]:
    try:
        lifecycle = import_module("engine.execution.options_lifecycle")
        evidence_fn = getattr(lifecycle, "lifecycle_readiness_evidence", None)
    except Exception as exc:
        return False, {"provider": "options_lifecycle", "available": False, "error": str(exc)}

    if not callable(evidence_fn):
        return False, {"provider": "options_lifecycle", "available": False, "entrypoint": False}

    try:
        evidence = dict(evidence_fn(os.environ) or {})
    except Exception as exc:
        return False, {"provider": "options_lifecycle", "available": True, "error": str(exc)}

    required_key = "assignment_exercise_model" if kind == "assignment_exercise" else "expiration_risk_model"
    ok = bool(evidence.get("implemented")) and bool(evidence.get("enabled")) and bool(evidence.get(required_key))
    return bool(ok), {
        "provider": "options_lifecycle",
        "available": True,
        "kind": str(kind),
        "required_evidence": required_key,
        "evidence": evidence,
    }


def _broker_support_predicate(_kind: str, context: Mapping[str, Any]) -> tuple[bool, dict[str, Any]]:
    broker_names = _broker_tokens(context.get("broker") or os.environ.get("BROKER") or os.environ.get("BROKER_NAME") or "")
    adapter_names = [name for name in broker_names if name in LIVE_OPTIONS_BROKER_ADAPTERS]
    missing_adapter = [name for name in broker_names if name in LIVE_BROKERS and name not in LIVE_OPTIONS_BROKER_ADAPTERS]
    if missing_adapter and not adapter_names:
        return True, {
            "provider": "options_broker_adapter_registry",
            "available": True,
            "broker_names": broker_names,
            "registered_adapters": sorted(LIVE_OPTIONS_BROKER_ADAPTERS),
            "specific_adapter_blocker": [f"options_live_broker_adapter_missing:{name}" for name in missing_adapter],
        }
    if not adapter_names:
        return False, {
            "provider": "options_broker_adapter_registry",
            "available": False,
            "broker_names": broker_names,
            "registered_adapters": sorted(LIVE_OPTIONS_BROKER_ADAPTERS),
        }
    importable: dict[str, bool] = {}
    errors: dict[str, str] = {}
    for adapter_name in adapter_names:
        try:
            module_name = "engine.execution.broker_tradier_options" if adapter_name == "tradier_options" else ""
            if not module_name:
                raise RuntimeError(f"options_adapter_module_unknown:{adapter_name}")
            module = import_module(module_name)
            importable[adapter_name] = callable(getattr(module, "apply_latest_portfolio_orders_live", None))
        except Exception as exc:
            importable[adapter_name] = False
            errors[adapter_name] = str(exc)
    return all(importable.values()), {
        "provider": "options_broker_adapter_registry",
        "available": True,
        "broker_names": broker_names,
        "registered_adapters": sorted(LIVE_OPTIONS_BROKER_ADAPTERS),
        "adapter_importable": importable,
        "errors": errors,
    }


def _kill_switch_predicate(_kind: str, context: Mapping[str, Any]) -> tuple[bool, dict[str, Any]]:
    try:
        kill_switch = import_module("engine.execution.kill_switch")
        execution_allowed = getattr(kill_switch, "execution_allowed", None)
    except Exception as exc:
        return False, {"provider": "kill_switch.execution_allowed", "available": False, "error": str(exc)}
    if not callable(execution_allowed):
        return False, {"provider": "kill_switch.execution_allowed", "available": False, "entrypoint": False}
    scope = _first_order_scope([dict(order) for order in list(context.get("orders") or []) if isinstance(order, Mapping)])
    try:
        allowed, reason, meta = execution_allowed(
            con=None,
            symbol=scope.get("symbol") or None,
            regime=scope.get("regime") or None,
            model_id=scope.get("model_id") or None,
        )
    except Exception as exc:
        return False, {
            "provider": "kill_switch.execution_allowed",
            "available": True,
            "scope": scope,
            "error": str(exc),
        }
    return bool(allowed), {
        "provider": "kill_switch.execution_allowed",
        "available": True,
        "scope": scope,
        "reason": str(reason or "ok"),
        "meta": dict(meta or {}),
    }


GatePredicate = Callable[[str, Mapping[str, Any]], tuple[bool, dict[str, Any]]]

_GATE_PREDICATES: dict[str, GatePredicate] = {
    "greeks": _portfolio_risk_predicate,
    "liquidity_filters": _data_quality_predicate,
    "bid_ask_quality": _data_quality_predicate,
    "assignment_exercise": _lifecycle_predicate,
    "expiration_risk": _lifecycle_predicate,
    "margin_impact": _portfolio_risk_predicate,
    "broker_support": _broker_support_predicate,
    "position_limits": _portfolio_risk_predicate,
    "kill_switch_integration": _kill_switch_predicate,
}


def _control_flag_snapshot(
    *,
    broker: Any = None,
    orders: Optional[Sequence[Mapping[str, Any]]] = None,
) -> tuple[list[str], dict[str, Any]]:
    blockers: list[str] = []
    controls: dict[str, Any] = {}
    context = {
        "broker": broker,
        "orders": [dict(order) for order in list(orders or []) if isinstance(order, Mapping)],
        "now_ms": int(time.time() * 1000),
    }
    for control, blocker, names in CONTROL_FLAG_GROUPS:
        enabled = _env_bool(*names, default=False)
        configured = any(bool(_env_text(name)) for name in names)
        real_ok = False
        detail: dict[str, Any] = {"evaluated": False, "reason": "env_flag_not_enabled"}
        if enabled:
            predicate = _GATE_PREDICATES.get(control)
            if callable(predicate):
                try:
                    real_ok, detail = predicate(control, context)
                    detail = dict(detail or {})
                    detail["evaluated"] = True
                except Exception as exc:
                    real_ok = False
                    detail = {"evaluated": True, "error": str(exc), "provider": control}
            else:
                real_ok = False
                detail = {"evaluated": True, "error": "gate_predicate_missing", "provider": control}
        controls[control] = {
            "ok": bool(enabled and real_ok),
            "configured": bool(configured),
            "env": list(names),
            "detail": detail,
        }
        if not enabled:
            blockers.append(blocker)
        elif not real_ok:
            blockers.append(_check_failed_blocker(blocker))
    return blockers, controls


def _order_control_blockers(order: Mapping[str, Any], index: int, *, now_ms: int | None = None) -> list[str]:
    meta = option_order_metadata(order)
    prefix = f"order_{int(index)}"
    blockers: list[str] = []

    if not meta.get("contract"):
        blockers.append(f"{prefix}:options_contract_missing")
    if not meta.get("underlying"):
        blockers.append(f"{prefix}:options_underlying_missing")
    if not meta.get("expiration"):
        blockers.append(f"{prefix}:options_expiration_missing")
    if meta.get("strike") is None or float(meta.get("strike") or 0.0) <= 0.0:
        blockers.append(f"{prefix}:options_strike_missing")
    if str(meta.get("contract_type") or "") not in {"call", "put"}:
        blockers.append(f"{prefix}:options_contract_type_missing")

    greeks = dict(meta.get("greeks") or {})
    for greek in ("delta", "gamma", "theta", "vega"):
        if greeks.get(greek) is None:
            blockers.append(f"{prefix}:options_greek_missing:{greek}")

    oi = meta.get("open_interest")
    vol = meta.get("volume")
    min_oi = _safe_float(_env_text("OPTIONS_MIN_OPEN_INTEREST"), math.nan)
    min_vol = _safe_float(_env_text("OPTIONS_MIN_VOLUME"), math.nan)
    if oi is None:
        blockers.append(f"{prefix}:options_open_interest_missing")
    elif math.isfinite(min_oi) and float(oi) < float(min_oi):
        blockers.append(f"{prefix}:options_open_interest_below_min")
    if vol is None:
        blockers.append(f"{prefix}:options_volume_missing")
    elif math.isfinite(min_vol) and float(vol) < float(min_vol):
        blockers.append(f"{prefix}:options_volume_below_min")

    bid = meta.get("bid")
    ask = meta.get("ask")
    if bid is None or ask is None or float(bid) <= 0.0 or float(ask) <= 0.0 or float(ask) < float(bid):
        blockers.append(f"{prefix}:options_bid_ask_invalid")
    else:
        mid = max(1e-12, (float(bid) + float(ask)) / 2.0)
        spread_bps = ((float(ask) - float(bid)) / mid) * 10_000.0
        max_spread = _safe_float(_env_text("OPTIONS_MAX_SPREAD_BPS"), math.nan)
        if math.isfinite(max_spread) and float(spread_bps) > float(max_spread):
            blockers.append(f"{prefix}:options_spread_too_wide")

    dte = _days_to_expiration(meta.get("expiration"), now_ms=now_ms)
    min_dte = _safe_float(_env_text("OPTIONS_MIN_DTE_DAYS"), math.nan)
    max_dte = _safe_float(_env_text("OPTIONS_MAX_DTE_DAYS"), math.nan)
    if dte is None:
        blockers.append(f"{prefix}:options_expiration_unparseable")
    else:
        if math.isfinite(min_dte) and float(dte) < float(min_dte):
            blockers.append(f"{prefix}:options_expiration_too_near")
        if math.isfinite(max_dte) and float(dte) > float(max_dte):
            blockers.append(f"{prefix}:options_expiration_too_far")

    if not meta.get("assignment_exercise_policy"):
        blockers.append(f"{prefix}:options_assignment_exercise_policy_missing")

    margin_fraction = meta.get("margin_impact_fraction")
    max_margin_fraction = _safe_float(_env_text("OPTIONS_MARGIN_IMPACT_MAX_FRACTION"), math.nan)
    if margin_fraction is None:
        blockers.append(f"{prefix}:options_margin_impact_missing")
    elif math.isfinite(max_margin_fraction) and float(margin_fraction) > float(max_margin_fraction):
        blockers.append(f"{prefix}:options_margin_impact_too_high")

    qty = abs(_first_number(order, ("contracts", "qty", "quantity", "order_qty")))
    max_contracts = _safe_float(_env_text("OPTIONS_MAX_POSITION_CONTRACTS"), math.nan)
    if not math.isfinite(qty) or qty <= 0.0:
        blockers.append(f"{prefix}:options_contract_quantity_missing")
    elif math.isfinite(max_contracts) and float(qty) > float(max_contracts):
        blockers.append(f"{prefix}:options_position_contract_limit_exceeded")

    return blockers


def _broker_tokens(broker: Any) -> list[str]:
    text = str(broker or "").replace(";", ",")
    return [part.strip().lower() for part in text.split(",") if part.strip()]


def _live_broker_context(broker: Any) -> bool:
    return any(token in LIVE_BROKERS for token in _broker_tokens(broker))


def live_options_readiness_snapshot(
    *,
    engine_mode: Any = None,
    execution_mode: Any = None,
    broker: Any = None,
    orders: Optional[Sequence[Mapping[str, Any]]] = None,
) -> dict[str, Any]:
    mode = options_instruments_mode()
    option_orders = [dict(order) for order in list(orders or []) if is_options_order(order)]
    live_context = (
        str(engine_mode if engine_mode is not None else os.environ.get("ENGINE_MODE", "")).strip().lower() == "live"
        or str(execution_mode if execution_mode is not None else os.environ.get("EXECUTION_MODE", "")).strip().lower() == "live"
        or _live_broker_context(broker if broker is not None else os.environ.get("BROKER") or os.environ.get("BROKER_NAME") or "")
    )
    requested = live_options_requested()
    required = bool(live_context and (requested or option_orders))
    blockers: list[str] = []

    if mode == "invalid":
        blockers.append("options_instruments_mode_invalid")

    if not required:
        return {
            "ok": not blockers,
            "required": False,
            "mode": mode,
            "live_requested": bool(requested),
            "live_context": bool(live_context),
            "option_order_count": int(len(option_orders)),
            "reason": "ok" if not blockers else blockers[0],
            "blockers": _dedupe(blockers),
            "shadow_only": mode in {"disabled", "shadow", "paper", "invalid"},
            "live_broker_adapters": sorted(LIVE_OPTIONS_BROKER_ADAPTERS),
        }

    if mode != "live" or not requested:
        blockers.append("options_live_orders_disabled_shadow_only")

    flag_blockers, flag_controls = _control_flag_snapshot(broker=broker, orders=option_orders)
    numeric_blockers, numeric_controls = _numeric_control_snapshot()
    blockers.extend(flag_blockers)
    blockers.extend(numeric_blockers)

    broker_names = _broker_tokens(broker if broker is not None else os.environ.get("BROKER") or os.environ.get("BROKER_NAME") or "")
    for broker_name in broker_names or ["unknown"]:
        if broker_name in LIVE_BROKERS and broker_name not in LIVE_OPTIONS_BROKER_ADAPTERS:
            blockers.append(f"options_live_broker_adapter_missing:{broker_name}")

    now_ms = int(time.time() * 1000)
    order_blockers: list[str] = []
    for index, order in enumerate(option_orders):
        order_blockers.extend(_order_control_blockers(order, index, now_ms=now_ms))
    blockers.extend(order_blockers)

    blockers = _dedupe(blockers)
    return {
        "ok": not blockers,
        "required": True,
        "mode": mode,
        "live_requested": bool(requested),
        "live_context": bool(live_context),
        "option_order_count": int(len(option_orders)),
        "reason": "ok" if not blockers else blockers[0],
        "blockers": blockers,
        "controls": flag_controls,
        "numeric_controls": numeric_controls,
        "order_blockers": order_blockers,
        "live_broker_adapters": sorted(LIVE_OPTIONS_BROKER_ADAPTERS),
    }


def live_options_order_block(
    orders: Optional[Sequence[Mapping[str, Any]]],
    *,
    broker: Any,
    dry_run: bool = False,
    engine_mode: Any = None,
    execution_mode: Any = None,
) -> dict[str, Any] | None:
    if bool(dry_run):
        return None
    if not any(is_options_order(order) for order in list(orders or [])):
        return None
    state = live_options_readiness_snapshot(
        engine_mode=engine_mode,
        execution_mode=execution_mode,
        broker=broker,
        orders=list(orders or []),
    )
    if bool(state.get("ok")):
        return None
    return {
        "ok": False,
        "status": "options_instruments_not_live_ready",
        "reason": str(state.get("reason") or "options_instruments_not_live_ready"),
        "broker": str(broker or "unknown"),
        "stop_failover": True,
        "retryable": False,
        "fatal_options_readiness": True,
        "options_readiness": state,
    }


def force_options_shadow_intent(intent: Mapping[str, Any], *, reason: str = "options_instruments_shadow_only") -> dict[str, Any]:
    out = dict(intent or {})
    if not is_options_order(out):
        return out
    metadata = option_order_metadata(out)
    out["instrument_type"] = "option"
    out["options_instrument"] = metadata
    out["execution_target"] = "shadow"
    out["options_live_block_reason"] = str(reason)
    explain = dict(out.get("explain") or {}) if isinstance(out.get("explain"), Mapping) else {}
    explain["execution_target"] = "shadow"
    explain["options_instrument"] = dict(metadata)
    explain["options_live_block_reason"] = str(reason)
    out["explain"] = explain
    decision = dict(out.get("decision") or {}) if isinstance(out.get("decision"), Mapping) else {}
    decision["downgraded_execution_target"] = "shadow"
    decision["reason"] = str(reason)
    out["decision"] = decision
    competition = dict(out.get("competition") or {}) if isinstance(out.get("competition"), Mapping) else {}
    competition.update({"allowed": False, "blocked": True, "reason": str(reason)})
    out["competition"] = competition
    return out


__all__ = [
    "force_options_shadow_intent",
    "is_options_order",
    "live_options_order_block",
    "live_options_readiness_snapshot",
    "option_order_metadata",
    "options_instruments_mode",
]
