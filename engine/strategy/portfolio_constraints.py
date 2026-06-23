"""Constraint helpers for portfolio construction."""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Tuple


WarnFn = Callable[..., None]


def apply_max_position_constraint(
    desired: Dict[str, Dict[str, Any]] | None,
    *,
    max_positions: int | None = None,
    eff_max_positions_fn: Callable[[], int],
) -> Dict[str, Dict[str, Any]]:
    desired_map = dict(desired or {})
    eff_max_pos = int(
        eff_max_positions_fn() if max_positions is None else max_positions
    )
    if eff_max_pos >= 0 and len(desired_map) > int(eff_max_pos):
        items = sorted(
            desired_map.items(),
            key=lambda kv: abs(float((kv[1] or {}).get("weight", 0.0))),
            reverse=True,
        )
        desired_map = dict(items[: int(eff_max_pos)])
    return desired_map


def symbol_cap(
    symbol: str,
    *,
    symbol_caps: Dict[str, Any],
    max_weight_per_symbol: float,
    warn_nonfatal: WarnFn | None = None,
) -> float:
    if symbol in symbol_caps:
        try:
            return float(symbol_caps[symbol])
        except Exception as exc:
            if warn_nonfatal is not None:
                warn_nonfatal(
                    "PORTFOLIO_SYMBOL_CAP_FAILED",
                    exc,
                    once_key=f"symbol_cap:{symbol}",
                    symbol=str(symbol),
                )
            return float(max_weight_per_symbol)
    return float(max_weight_per_symbol)


def apply_weight_caps(
    raw_weights: List[float],
    caps: List[float],
    target_total: float,
    *,
    normalize_nonnegative_weights_fn: Callable[[List[float]], List[float]],
    safe_float_fn: Callable[[Any, float], float],
) -> List[float]:
    n = len(raw_weights or [])
    if n <= 0:
        return []

    target_left = max(0.0, float(target_total))
    normalized = normalize_nonnegative_weights_fn(list(raw_weights or []))
    out = [0.0 for _ in range(n)]
    remaining = {idx for idx in range(n)}
    safe_caps = [max(0.0, safe_float_fn(cap, 0.0)) for cap in caps]

    while remaining and target_left > 1e-12:
        remaining_total = sum(float(normalized[idx]) for idx in remaining)
        if remaining_total <= 1e-12:
            equal_share = float(target_left) / float(len(remaining))
            for idx in list(remaining):
                out[idx] += min(float(safe_caps[idx]), float(equal_share))
            break

        clipped: List[int] = []
        assigned = 0.0
        for idx in list(remaining):
            desired = float(target_left) * (
                float(normalized[idx]) / float(remaining_total)
            )
            cap = float(safe_caps[idx])
            if desired >= cap - 1e-12:
                out[idx] += float(cap)
                assigned += float(cap)
                clipped.append(int(idx))

        if not clipped:
            for idx in list(remaining):
                desired = float(target_left) * (
                    float(normalized[idx]) / float(remaining_total)
                )
                out[idx] += min(float(safe_caps[idx]), float(desired))
            break

        target_left = max(0.0, float(target_left) - float(assigned))
        for idx in clipped:
            remaining.discard(int(idx))

    return [float(value) for value in out]


def apply_capital_at_risk_gate(
    desired: Dict[str, Dict],
    *,
    car_max: float,
    car_max_per_symbol: float,
    gross_cap: float,
    desired_symbol_fn: Callable[[Any, Dict[str, Any] | None], str],
    tradability_from_explain_fn: Callable[[str], Dict[str, float]],
    warn_nonfatal: WarnFn | None = None,
) -> Tuple[Dict[str, Dict], Dict]:
    meta = {
        "car_enabled": True,
        "car_max": float(car_max),
        "car_max_per_symbol": float(car_max_per_symbol),
    }
    if not desired:
        meta["car_scaled"] = False
        return desired, meta

    risks: Dict[str, Dict[str, Any]] = {}
    total_risk = 0.0

    for desired_key, target in desired.items():
        symbol = desired_symbol_fn(desired_key, target)
        if not symbol:
            continue
        weight = abs(float(target.get("weight", 0.0) or 0.0))
        tradability = tradability_from_explain_fn(target.get("explain_json", "{}"))
        expected_dd = float(tradability.get("expected_dd", 0.0) or 0.0)
        expected_dd = max(0.0, min(1.0, expected_dd))

        max_weight = None
        if float(car_max_per_symbol) > 0.0:
            denom = expected_dd if expected_dd > 0 else 1.0
            max_weight = float(car_max_per_symbol) / max(1e-9, denom)

        if max_weight is not None and weight > max_weight:
            new_weight = float(max_weight)
            if str(target.get("side")).upper() == "SHORT":
                target["weight"] = -float(new_weight)
            else:
                target["weight"] = float(new_weight)

            target.setdefault("reason", {})
            target["reason"]["car_symbol_cap"] = True
            target["reason"]["car_expected_dd"] = float(expected_dd)
            target["reason"]["car_symbol_max_w"] = float(max_weight)

        adjusted_weight = abs(float(target.get("weight", 0.0) or 0.0))
        risk = adjusted_weight * expected_dd
        risks[str(desired_key)] = {
            "symbol": str(symbol),
            "w": adjusted_weight,
            "expected_dd": expected_dd,
            "risk": risk,
        }
        total_risk += float(risk)

    meta["car_total_risk_before"] = float(total_risk)

    if float(car_max) > 0.0 and total_risk > float(car_max) and total_risk > 1e-9:
        scale = float(car_max) / float(total_risk)
        for symbol in list(desired.keys()):
            try:
                desired[symbol]["weight"] = float(
                    desired[symbol].get("weight", 0.0) or 0.0
                ) * float(scale)
                desired[symbol].setdefault("reason", {})
                desired[symbol]["reason"]["car_scale"] = float(scale)
            except Exception as exc:
                if warn_nonfatal is not None:
                    warn_nonfatal(
                        "PORTFOLIO_CAPITAL_AT_RISK_REASON_FAILED",
                        exc,
                        once_key="capital_at_risk_reason",
                    )
        meta["car_scaled"] = True
        meta["car_scale"] = float(scale)
    else:
        meta["car_scaled"] = False

    gross = sum(
        abs(float((value or {}).get("weight", 0.0) or 0.0))
        for value in desired.values()
    )
    if gross > float(gross_cap) and gross > 1e-9:
        scale_gross = float(gross_cap) / float(gross)
        for symbol in list(desired.keys()):
            weight0 = float(desired[symbol].get("weight", 0.0) or 0.0)
            sign = -1.0 if weight0 < 0 else 1.0
            desired[symbol]["weight"] = sign * abs(float(weight0) * float(scale_gross))
        meta["gross_renorm_after_car"] = True
        meta["gross_scale_after_car"] = float(scale_gross)

    meta["car_by_symbol"] = risks
    return desired, meta
