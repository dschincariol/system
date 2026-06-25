"""Shadow-only option position lifecycle planning.

This module models reference-grade option lifecycle transitions for paper and
shadow simulation. It is not live order authority. American exercise and short
assignment are modeled only at or after expiry; optimal early exercise is
explicitly out of scope. Expiry intrinsic value is deterministic:

- call: ``max(0, underlying - strike)``
- put: ``max(0, strike - underlying)``

The value applied by broker simulation is intrinsic times absolute contracts
times the contract multiplier. Pin risk is deterministic as well: if the
underlying is within ``OPTIONS_PIN_RISK_BAND_ABS`` of the strike at expiry, the
planner emits ``PIN_RISK`` and does not guess an exercise/assignment outcome.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime, timezone
from enum import Enum
import math
import os
from typing import Any, Callable, Mapping, Sequence


class LifecycleState(str, Enum):
    OPEN = "OPEN"
    DTE_ROLL = "DTE_ROLL"
    DTE_AUTOCLOSE = "DTE_AUTOCLOSE"
    PIN_RISK = "PIN_RISK"
    EXPIRE_WORTHLESS = "EXPIRE_WORTHLESS"
    CASH_SETTLE = "CASH_SETTLE"
    EXERCISE = "EXERCISE"
    ASSIGN = "ASSIGN"


OPEN_TRANSITIONS = frozenset(
    {
        LifecycleState.DTE_ROLL,
        LifecycleState.DTE_AUTOCLOSE,
        LifecycleState.PIN_RISK,
        LifecycleState.EXPIRE_WORTHLESS,
        LifecycleState.CASH_SETTLE,
        LifecycleState.EXERCISE,
        LifecycleState.ASSIGN,
    }
)
INTERMEDIATE_TRANSITIONS = frozenset(
    {
        LifecycleState.EXPIRE_WORTHLESS,
        LifecycleState.CASH_SETTLE,
        LifecycleState.EXERCISE,
        LifecycleState.ASSIGN,
    }
)


@dataclass(frozen=True)
class LifecycleEvent:
    symbol: str
    event_type: str
    qty: float
    avg_px: float
    underlying: str
    underlying_px: float | None
    strike: float
    expiry: str
    right: str
    settlement: str
    multiplier: float
    dte: float | None
    intrinsic_per_contract: float
    intrinsic_value: float
    from_state: str = LifecycleState.OPEN.value
    to_state: str = ""
    target_symbol: str | None = None
    reason: str = ""
    warning: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    return float(out) if math.isfinite(out) else float(default)


def _safe_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def _env_get(env: Mapping[str, Any] | None, name: str, default: Any = None) -> Any:
    if env is not None and name in env:
        return env.get(name)
    return os.environ.get(name, default)


def _env_float(env: Mapping[str, Any] | None, name: str, default: float = 0.0) -> float:
    return _safe_float(_env_get(env, name, default), default)


def _metadata_dict(metadata: Any) -> dict[str, Any]:
    if metadata is None:
        return {}
    if isinstance(metadata, Mapping):
        return dict(metadata)
    if is_dataclass(metadata):
        return dict(asdict(metadata))
    if hasattr(metadata, "to_dict"):
        try:
            raw = metadata.to_dict()
            return dict(raw) if isinstance(raw, Mapping) else {}
        except Exception:
            return {}
    out: dict[str, Any] = {}
    for key in (
        "occ_symbol",
        "symbol",
        "underlying",
        "expiry",
        "expiration",
        "right",
        "contract_type",
        "strike",
        "multiplier",
        "settlement",
        "opt_settlement",
        "exercise_style",
    ):
        if hasattr(metadata, key):
            try:
                out[key] = getattr(metadata, key)
            except Exception:
                return {}
    return out


def _normalize_metadata(metadata: Any) -> dict[str, Any] | None:
    raw = _metadata_dict(metadata)
    if not raw:
        return None
    symbol = str(raw.get("occ_symbol") or raw.get("symbol") or "").upper().strip()
    underlying = str(raw.get("underlying") or raw.get("root_symbol") or "").upper().strip()
    expiry_raw = raw.get("expiry") or raw.get("expiration") or raw.get("expiration_date")
    right_raw = raw.get("right") or raw.get("contract_type") or raw.get("option_type")
    right = str(right_raw or "").upper().strip()
    if right == "CALL":
        right = "C"
    elif right == "PUT":
        right = "P"
    settlement = str(raw.get("settlement") or raw.get("opt_settlement") or "").upper().strip()
    strike = _safe_float(raw.get("strike"), float("nan"))
    multiplier = _safe_float(raw.get("multiplier"), float("nan"))
    expiry = _expiration_text(expiry_raw)
    if not underlying or right not in {"C", "P"} or not expiry:
        return None
    if not math.isfinite(strike) or strike <= 0.0 or not math.isfinite(multiplier) or multiplier <= 0.0:
        return None
    return {
        "symbol": symbol,
        "underlying": underlying,
        "expiry": expiry,
        "right": right,
        "settlement": settlement,
        "strike": float(strike),
        "multiplier": float(multiplier),
    }


def _expiration_text(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, "isoformat"):
        try:
            return str(value.isoformat())[:10]
        except Exception:
            return ""
    text = str(value).strip()
    return text[:10] if text else ""


def _days_to_expiration(expiration: str, ts_ms: int) -> float | None:
    try:
        expiry_dt = datetime.strptime(str(expiration), "%Y-%m-%d").replace(tzinfo=timezone.utc)
        out = ((expiry_dt.timestamp() * 1000.0) - float(ts_ms)) / 86400000.0
    except Exception:
        return None
    return float(out) if math.isfinite(out) else None


def _position_tuple(position: Any) -> tuple[str, float, float] | None:
    if isinstance(position, Mapping):
        symbol = str(position.get("symbol") or "").upper().strip()
        qty = _safe_float(position.get("qty"), 0.0)
        avg_px = _safe_float(position.get("avg_px"), 0.0)
        return (symbol, float(qty), float(avg_px)) if symbol else None
    if isinstance(position, Sequence) and not isinstance(position, (str, bytes)) and len(position) >= 3:
        symbol = str(position[0] or "").upper().strip()
        qty = _safe_float(position[1], 0.0)
        avg_px = _safe_float(position[2], 0.0)
        return (symbol, float(qty), float(avg_px)) if symbol else None
    return None


def _underlying_price(underlying_prices: Mapping[str, Any] | None, underlying: str) -> float | None:
    if not underlying_prices:
        return None
    raw = underlying_prices.get(str(underlying).upper()) or underlying_prices.get(str(underlying))
    if isinstance(raw, Mapping):
        raw = raw.get("price") or raw.get("px") or raw.get("underlying_px")
    out = _safe_float(raw, float("nan"))
    if not math.isfinite(out) or out <= 0.0:
        return None
    return float(out)


def _intrinsic_per_contract(right: str, underlying_px: float, strike: float) -> float:
    if str(right).upper() == "C":
        return max(0.0, float(underlying_px) - float(strike))
    if str(right).upper() == "P":
        return max(0.0, float(strike) - float(underlying_px))
    return 0.0


def _make_event(
    *,
    symbol: str,
    event_type: LifecycleState,
    qty: float,
    avg_px: float,
    meta: Mapping[str, Any],
    underlying_px: float | None,
    dte: float | None,
    reason: str,
    target_symbol: str | None = None,
    warning: str | None = None,
    details: dict[str, Any] | None = None,
) -> LifecycleEvent:
    right = str(meta.get("right") or "").upper().strip()
    strike = float(meta.get("strike") or 0.0)
    multiplier = float(meta.get("multiplier") or 0.0)
    intrinsic = 0.0
    if underlying_px is not None:
        intrinsic = _intrinsic_per_contract(right, float(underlying_px), float(strike))
    return LifecycleEvent(
        symbol=str(symbol).upper().strip(),
        event_type=event_type.value,
        qty=float(qty),
        avg_px=float(avg_px),
        underlying=str(meta.get("underlying") or "").upper().strip(),
        underlying_px=(float(underlying_px) if underlying_px is not None else None),
        strike=float(strike),
        expiry=str(meta.get("expiry") or ""),
        right=right,
        settlement=str(meta.get("settlement") or ""),
        multiplier=float(multiplier),
        dte=(float(dte) if dte is not None else None),
        intrinsic_per_contract=float(intrinsic),
        intrinsic_value=float(abs(float(qty)) * float(multiplier) * float(intrinsic)),
        to_state=event_type.value,
        target_symbol=target_symbol,
        reason=str(reason),
        warning=warning,
        details=dict(details or {}),
    )


def plan_option_lifecycle_events(
    positions: Sequence[Any],
    *,
    underlying_prices: Mapping[str, Any],
    now_ms: int,
    metadata_for: Callable[[str], Any],
    env: Mapping[str, Any] | None = None,
) -> list[LifecycleEvent]:
    """Plan lifecycle events without mutating state or raising."""

    events: list[LifecycleEvent] = []
    try:
        min_dte = _env_float(env, "OPTIONS_MIN_DTE_DAYS", 0.0)
        pin_band = max(0.0, _env_float(env, "OPTIONS_PIN_RISK_BAND_ABS", 0.0))
        lifecycle_mode = str(_env_get(env, "OPTIONS_LIFECYCLE_MODE", "shadow") or "shadow").strip().lower()
        roll_target_dte = _env_float(env, "OPTIONS_LIFECYCLE_ROLL_TARGET_DTE", 30.0)
        roll_target_symbol = str(_env_get(env, "OPTIONS_LIFECYCLE_ROLL_TARGET_SYMBOL", "") or "").upper().strip() or None

        for raw_position in list(positions or []):
            parsed = _position_tuple(raw_position)
            if parsed is None:
                continue
            symbol, qty, avg_px = parsed
            if abs(float(qty)) <= 1e-12:
                continue
            try:
                meta = _normalize_metadata(metadata_for(str(symbol)))
            except Exception:
                meta = None
            if not meta:
                continue
            dte = _days_to_expiration(str(meta["expiry"]), int(now_ms))
            if dte is None:
                continue
            underlying_px = _underlying_price(underlying_prices, str(meta["underlying"]))
            expired = float(dte) <= 0.0

            if expired:
                if underlying_px is None:
                    continue
                distance = abs(float(underlying_px) - float(meta["strike"]))
                if pin_band > 0.0 and distance <= pin_band + 1e-12:
                    events.append(
                        _make_event(
                            symbol=symbol,
                            event_type=LifecycleState.PIN_RISK,
                            qty=qty,
                            avg_px=avg_px,
                            meta=meta,
                            underlying_px=underlying_px,
                            dte=dte,
                            reason="underlying_within_pin_risk_band_at_expiry",
                            details={"pin_risk_band_abs": float(pin_band), "distance_to_strike": float(distance)},
                        )
                    )
                    continue

                intrinsic = _intrinsic_per_contract(str(meta["right"]), float(underlying_px), float(meta["strike"]))
                if intrinsic <= 0.0:
                    events.append(
                        _make_event(
                            symbol=symbol,
                            event_type=LifecycleState.EXPIRE_WORTHLESS,
                            qty=qty,
                            avg_px=avg_px,
                            meta=meta,
                            underlying_px=underlying_px,
                            dte=dte,
                            reason="expired_otm",
                        )
                    )
                    continue

                settlement = str(meta.get("settlement") or "").upper().replace("-", "_")
                if settlement in {"CASH", "CASH_SETTLED", "CASH_SETTLEMENT"}:
                    events.append(
                        _make_event(
                            symbol=symbol,
                            event_type=LifecycleState.CASH_SETTLE,
                            qty=qty,
                            avg_px=avg_px,
                            meta=meta,
                            underlying_px=underlying_px,
                            dte=dte,
                            reason="expired_itm_cash_settlement",
                        )
                    )
                elif qty > 0.0:
                    events.append(
                        _make_event(
                            symbol=symbol,
                            event_type=LifecycleState.EXERCISE,
                            qty=qty,
                            avg_px=avg_px,
                            meta=meta,
                            underlying_px=underlying_px,
                            dte=dte,
                            reason="expired_itm_long_reference_exercise",
                        )
                    )
                else:
                    events.append(
                        _make_event(
                            symbol=symbol,
                            event_type=LifecycleState.ASSIGN,
                            qty=qty,
                            avg_px=avg_px,
                            meta=meta,
                            underlying_px=underlying_px,
                            dte=dte,
                            reason="expired_itm_short_reference_assignment",
                        )
                    )
                continue

            if min_dte > 0.0 and float(dte) <= float(min_dte):
                if lifecycle_mode == "roll":
                    warning = None if roll_target_symbol else "roll_target_unavailable_close_only"
                    events.append(
                        _make_event(
                            symbol=symbol,
                            event_type=LifecycleState.DTE_ROLL,
                            qty=qty,
                            avg_px=avg_px,
                            meta=meta,
                            underlying_px=underlying_px,
                            dte=dte,
                            target_symbol=roll_target_symbol,
                            warning=warning,
                            reason="dte_below_min_roll",
                            details={"roll_target_dte": float(roll_target_dte)},
                        )
                    )
                else:
                    events.append(
                        _make_event(
                            symbol=symbol,
                            event_type=LifecycleState.DTE_AUTOCLOSE,
                            qty=qty,
                            avg_px=avg_px,
                            meta=meta,
                            underlying_px=underlying_px,
                            dte=dte,
                            reason="dte_below_min_autoclose",
                        )
                    )
    except Exception:
        return []
    return events


def lifecycle_readiness_evidence(env: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Return data-side lifecycle implementation evidence without enabling live options."""

    enabled = _safe_bool(_env_get(env, "OPTIONS_LIFECYCLE_ENABLED", "0"), False)
    return {
        "implemented": True,
        "enabled": bool(enabled),
        "mode": str(_env_get(env, "OPTIONS_LIFECYCLE_MODE", "shadow") or "shadow"),
        "shadow_only": True,
        "live_order_authority": False,
        "assignment_exercise_model": "reference_expiry_only",
        "expiration_risk_model": "deterministic_intrinsic_and_pin_risk",
        "pin_risk_policy": "emit_pin_risk_no_random_assignment",
        "readiness_gates_modified": False,
    }
