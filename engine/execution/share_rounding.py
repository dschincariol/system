"""Per-broker equity share rounding and minimum-notional policy."""

from __future__ import annotations

import math
import os
from typing import Any, Dict, Tuple


_EQUITY_ASSET_CLASSES = {"EQUITY", "US_EQUITY", "STOCK", "ETF"}
_FALSE_STRINGS = {"0", "false", "no", "off", "n"}
_TRUE_STRINGS = {"1", "true", "yes", "on", "y"}


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(str(name))
    if raw is None or str(raw).strip() == "":
        return bool(default)
    text = str(raw).strip().lower()
    if text in _TRUE_STRINGS:
        return True
    if text in _FALSE_STRINGS:
        return False
    return bool(default)


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(str(name))
    if raw is None or str(raw).strip() == "":
        return float(default)
    try:
        value = float(str(raw).strip())
    except Exception:
        return float(default)
    if not math.isfinite(value):
        return float(default)
    return float(value)


def _canonical_broker(broker: str) -> str:
    broker_name = str(broker or "").strip().lower()
    if broker_name in {"broker_sim", "sim", "paper"}:
        target = str(os.environ.get("EXEC_SIM_ROUNDING_BROKER", "ibkr") or "ibkr").strip().lower()
        return target or "ibkr"
    return broker_name or "ibkr"


def _broker_increment(raw_broker: str, effective_broker: str) -> float:
    broker_key = str(raw_broker or "").strip().upper()
    effective_key = str(effective_broker or "").strip().upper()
    defaults = {
        "ALPACA": 0.0,
        "IBKR": 1.0,
    }
    default = float(defaults.get(effective_key, 1.0))
    names = []
    if broker_key:
        names.append(f"EXEC_{broker_key}_SHARE_INCREMENT")
    if effective_key and effective_key != broker_key:
        names.append(f"EXEC_{effective_key}_SHARE_INCREMENT")
    for name in names:
        raw = os.environ.get(name)
        if raw is not None and str(raw).strip() != "":
            return max(0.0, _env_float(name, default))
    return max(0.0, float(default))


def equity_share_policy(broker: str) -> Dict[str, Any]:
    """Return the active equity share-rounding policy for a broker boundary."""

    raw_broker = str(broker or "").strip().lower() or "ibkr"
    effective_broker = _canonical_broker(raw_broker)
    increment = _broker_increment(raw_broker, effective_broker)
    return {
        "enabled": _env_bool("EXEC_USE_SHARE_ROUNDING", False),
        "broker": raw_broker,
        "effective_broker": effective_broker,
        "increment": float(increment),
        "share_increment": float(increment),
        "allow_fractional": bool(increment <= 0.0),
        "min_notional": max(0.0, _env_float("EXEC_EQUITY_MIN_NOTIONAL_USD", 1.0)),
        "min_notional_usd": max(0.0, _env_float("EXEC_EQUITY_MIN_NOTIONAL_USD", 1.0)),
        "drop_sub_min": _env_bool("EXEC_SHARE_ROUNDING_DROP_SUB_MIN_NOTIONAL", True),
        "drop_sub_min_notional": _env_bool("EXEC_SHARE_ROUNDING_DROP_SUB_MIN_NOTIONAL", True),
        "unknown_as_equity": _env_bool("EXEC_SHARE_ROUNDING_UNKNOWN_AS_EQUITY", True),
    }


def _finite_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    if not math.isfinite(out):
        return float(default)
    return float(out)


def _round_toward_zero_to_increment(qty: float, increment: float) -> float:
    if increment <= 0.0:
        return float(qty)
    sign = -1.0 if float(qty) < 0.0 else 1.0
    units = math.floor((abs(float(qty)) / float(increment)) + 1e-12)
    rounded = sign * float(units) * float(increment)
    return 0.0 if abs(rounded) < 1e-12 else float(rounded)


def _eligible_asset(asset_class: str, unknown_as_equity: bool) -> Tuple[bool, str]:
    asset = str(asset_class or "UNKNOWN").strip().upper() or "UNKNOWN"
    if asset == "FX":
        return False, "fx_passthrough"
    if asset in _EQUITY_ASSET_CLASSES:
        return True, "equity"
    if asset == "UNKNOWN" and bool(unknown_as_equity):
        return True, "unknown_as_equity"
    return False, "non_equity_passthrough"


def round_equity_qty(
    qty: float,
    px: float,
    *,
    broker: str,
    asset_class: str,
) -> Tuple[float, Dict[str, Any]]:
    """Round an equity order quantity for the broker boundary.

    Non-equity assets are returned unchanged. FX is an explicit pass-through;
    this helper does not own FX weight-to-lots conversion.
    """

    raw_qty = _finite_float(qty, 0.0)
    ref_px = _finite_float(px, 0.0)
    policy = equity_share_policy(broker)
    asset = str(asset_class or "UNKNOWN").strip().upper() or "UNKNOWN"
    eligible, eligibility_reason = _eligible_asset(asset, bool(policy["unknown_as_equity"]))

    audit: Dict[str, Any] = {
        "enabled": bool(policy["enabled"]),
        "broker": str(policy["broker"]),
        "effective_broker": str(policy["effective_broker"]),
        "asset_class": asset,
        "eligible": bool(eligible),
        "eligibility_reason": str(eligibility_reason),
        "share_increment": float(policy["share_increment"]),
        "increment": float(policy["share_increment"]),
        "allow_fractional": bool(policy["allow_fractional"]),
        "min_notional_usd": float(policy["min_notional_usd"]),
        "min_notional": float(policy["min_notional_usd"]),
        "drop_sub_min_notional": bool(policy["drop_sub_min_notional"]),
        "drop_sub_min": bool(policy["drop_sub_min_notional"]),
        "raw_qty": float(raw_qty),
        "rounded_qty": float(raw_qty),
        "price": float(ref_px),
        "notional_usd": (abs(float(raw_qty)) * float(ref_px) if ref_px > 0.0 else None),
        "notional": (abs(float(raw_qty)) * float(ref_px) if ref_px > 0.0 else None),
        "applied": False,
        "changed": False,
        "dropped": False,
        "reason": "disabled" if eligible else str(eligibility_reason),
    }

    if not eligible:
        return float(raw_qty), audit
    if not bool(policy["enabled"]):
        return float(raw_qty), audit

    rounded_qty = _round_toward_zero_to_increment(raw_qty, float(policy["share_increment"]))
    notional = abs(float(rounded_qty)) * float(ref_px) if ref_px > 0.0 else None
    reason = "unchanged"
    dropped = False
    if (
        bool(policy["drop_sub_min_notional"])
        and float(policy["min_notional_usd"]) > 0.0
        and abs(float(rounded_qty)) > 0.0
        and notional is not None
        and float(notional) < float(policy["min_notional_usd"])
    ):
        rounded_qty = 0.0
        notional = 0.0
        dropped = True
        reason = "dropped_min_notional"
    elif abs(float(rounded_qty)) < 1e-12 and abs(float(raw_qty)) >= 1e-12:
        reason = "rounded_to_zero"
    elif abs(float(rounded_qty) - float(raw_qty)) >= 1e-12:
        reason = "rounded"

    audit.update(
        {
            "rounded_qty": float(rounded_qty),
            "notional_usd": (float(notional) if notional is not None else None),
            "notional": (float(notional) if notional is not None else None),
            "applied": True,
            "changed": abs(float(rounded_qty) - float(raw_qty)) >= 1e-12,
            "dropped": bool(dropped),
            "reason": str(reason),
        }
    )
    return float(rounded_qty), audit


__all__ = ["equity_share_policy", "round_equity_qty"]
