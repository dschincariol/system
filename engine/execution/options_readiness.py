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
from typing import Any, Iterable, Mapping, Optional, Sequence


TRUTHY = {"1", "true", "t", "yes", "y", "on"}
FALSEY = {"0", "false", "f", "no", "n", "off"}
LIVE_BROKERS = {"alpaca", "alpaca_rest", "ibkr", "interactivebrokers", "interactive_brokers", "ib_gateway", "ibgateway", "tws"}
OPTIONS_MODES = {"disabled", "shadow", "paper", "live"}

# Empty until a live options order adapter is implemented and reviewed. Keeping
# this list in code prevents an env-only toggle from enabling stock adapters to
# receive option contracts.
LIVE_OPTIONS_BROKER_ADAPTERS: frozenset[str] = frozenset()

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
    if contract_symbol and _OCC_COMPACT_RE.match(contract_symbol.upper().replace(" ", "")):
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


def _control_flag_snapshot() -> tuple[list[str], dict[str, Any]]:
    blockers: list[str] = []
    controls: dict[str, Any] = {}
    for control, blocker, names in CONTROL_FLAG_GROUPS:
        enabled = _env_bool(*names, default=False)
        configured = any(bool(_env_text(name)) for name in names)
        controls[control] = {
            "ok": bool(enabled),
            "configured": bool(configured),
            "env": list(names),
        }
        if not enabled:
            blockers.append(blocker)
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

    flag_blockers, flag_controls = _control_flag_snapshot()
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
