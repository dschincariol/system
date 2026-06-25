"""Runtime sizing helpers for the EQUITY asset class.

These helpers never create broker orders and never give models leverage
authority. They operate on target weights after model intent construction and
before execution.
"""

from __future__ import annotations

import math
from typing import Any, Dict, Mapping


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
        if math.isfinite(out):
            return float(out)
    except Exception:
        return float(default)
    return float(default)


def _side_sign(side: Any) -> float:
    side_s = str(side or "FLAT").upper().strip()
    if side_s == "LONG":
        return 1.0
    if side_s == "SHORT":
        return -1.0
    return 0.0


def _signed_weight(row: Mapping[str, Any] | None) -> float:
    if not isinstance(row, Mapping):
        return 0.0
    weight = _safe_float(row.get("weight"), 0.0)
    if weight < 0.0:
        return float(weight)
    return float(abs(weight) * _side_sign(row.get("side", "FLAT")))


def _row_with_signed_weight(row: Mapping[str, Any] | None, signed_weight: float) -> Dict[str, Any]:
    out = dict(row or {})
    signed = float(signed_weight)
    out["weight"] = float(abs(signed))
    out["side"] = "LONG" if signed > 0.0 else ("SHORT" if signed < 0.0 else "FLAT")
    return out


def equity_deployable_base(
    account: Mapping[str, Any] | None,
    *,
    account_equity: float,
    mode: str,
    max_leverage: float,
) -> tuple[float, dict]:
    """Return the dollar notional ceiling for aggregate EQUITY exposure.

    ``account_equity`` is account NAV/equity, not the EQUITY asset class.
    ``compute_deployable_equity`` is consumed read-only to derive a conservative
    deployable base from ``equity``, ``cash``, and ``buying_power``. The returned
    base is a dollar notional ceiling; callers derive the gross-weight ceiling as
    ``min(max_leverage, base / account_equity)``.

    In cash mode, the normal ceiling is 1.0x account equity. In Reg-T mode, the
    normal ceiling is 2.0x when a valid buying-power source is available. Reg-T
    without buying power fails closed to a zero ceiling even for standalone
    callers; the risk engine also hard-blocks that condition before sizing.
    """

    eq = _safe_float(account_equity, 0.0)
    max_lev = _safe_float(max_leverage, 1.0)
    normalized_mode = str(mode or "cash").strip().lower().replace("-", "_") or "cash"
    if normalized_mode == "regt":
        normalized_mode = "reg_t"
    acct = dict(account or {})
    fields_available = [
        key
        for key in ("equity", "cash", "buying_power")
        if key in acct and acct.get(key) not in (None, "")
    ]
    reason = {
        "type": "equity_deployable_base",
        "mode": normalized_mode,
        "account_equity": float(eq),
        "max_leverage": float(max_lev),
        "account_fields_available": list(fields_available),
        "buying_power_missing": "buying_power" not in fields_available,
        "available": True,
    }
    if eq <= 0.0 or max_lev <= 0.0:
        reason["type"] = "account_data_missing"
        reason["available"] = False
        reason["unavailable_reason"] = "account_data_missing"
        reason["deployable_equity"] = 0.0
        reason["base"] = 0.0
        reason["allowed_gross_weight"] = 0.0
        return 0.0, reason

    if normalized_mode == "reg_t" and "buying_power" not in fields_available:
        reason["type"] = "equity_deployable_base_unavailable"
        reason["available"] = False
        reason["unavailable_reason"] = "equity_buying_power_unavailable"
        reason["requires_buying_power"] = True
        reason["deployable_equity"] = 0.0
        reason["base"] = 0.0
        reason["allowed_gross_weight"] = 0.0
        return 0.0, reason

    acct.setdefault("equity", float(eq))
    try:
        from engine.execution.deployable_capital import compute_deployable_equity

        deployable = _safe_float(compute_deployable_equity(acct, default_equity=float(eq)), float(eq))
    except Exception as exc:
        deployable = float(eq)
        reason["deployable_base_fallback"] = "account_equity"
        reason["deployable_error_type"] = type(exc).__name__

    base = max(0.0, float(deployable)) * float(max_lev)
    allowed = min(float(max_lev), (float(base) / float(eq) if eq > 0.0 else 0.0))
    reason.update(
        {
            "deployable_equity": float(deployable),
            "base": float(base),
            "allowed_gross_weight": float(max(0.0, allowed)),
        }
    )
    for key in ("cash", "buying_power"):
        if key in acct and acct.get(key) not in (None, ""):
            reason[key] = _safe_float(acct.get(key), 0.0)
    return float(base), reason


def clamp_equity_gross_to_leverage(
    rows_equity: Mapping[str, Mapping[str, Any]] | None,
    *,
    account_equity: float,
    allowed_gross_weight: float,
    mode: str | None = None,
) -> tuple[dict[str, dict], dict]:
    """Clamp aggregate EQUITY gross weight proportionally.

    This intentionally diverges from the FX leverage helper. FX target weight is
    itself a notional multiple, so FX can clamp per leg. Stock/ETF leverage is an
    aggregate relationship: combined EQUITY gross notional divided by the
    buying-power/account-equity base. A single 0.20 stock weight is not 0.20x
    leverage by itself.
    """

    rows = dict(rows_equity or {})
    allowed = _safe_float(allowed_gross_weight, float("nan"))
    eq = _safe_float(account_equity, 0.0)
    gross_pre = float(sum(abs(_signed_weight(row)) for row in rows.values()))
    base_reason = {
        "type": "equity_leverage_within_cap",
        "clamped": False,
        "gross_pre": float(gross_pre),
        "gross_post": float(gross_pre),
        "allowed_gross": (float(allowed) if math.isfinite(allowed) else None),
        "allowed_gross_weight": (float(allowed) if math.isfinite(allowed) else None),
        "account_equity": float(eq),
    }
    if mode is not None:
        base_reason["mode"] = str(mode)

    if not math.isfinite(allowed) or eq <= 0.0:
        reason = dict(base_reason)
        reason.update({"type": "account_data_missing", "clamped": False})
        return rows, reason
    if allowed < 0.0:
        reason = dict(base_reason)
        reason.update({"type": "account_data_missing", "clamped": False, "allowed_gross": float(allowed)})
        return rows, reason
    if gross_pre <= allowed + 1e-12 or gross_pre <= 0.0:
        return rows, base_reason

    scale = float(allowed / gross_pre) if gross_pre > 0.0 else 0.0
    clamped_rows: dict[str, dict] = {}
    for symbol, row in rows.items():
        clamped_rows[str(symbol)] = _row_with_signed_weight(row, _signed_weight(row) * scale)
    gross_post = float(sum(abs(_signed_weight(row)) for row in clamped_rows.values()))
    reason = dict(base_reason)
    reason.update(
        {
            "type": "equity_leverage_cap",
            "clamped": True,
            "gross_post": float(gross_post),
            "scale": float(scale),
        }
    )
    return clamped_rows, reason


__all__ = ["clamp_equity_gross_to_leverage", "equity_deployable_base"]
