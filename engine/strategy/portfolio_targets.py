"""Target-construction helpers for portfolio construction."""

from __future__ import annotations

from typing import Any, Callable, Dict


def desired_weight(
    score: float,
    symbol: str,
    *,
    score_norm: float,
    gross_cap: float,
    symbol_cap_fn: Callable[[str], float],
    clamp_fn: Callable[[float, float, float], float],
) -> float:
    weight = (float(score) / float(score_norm)) * float(gross_cap)
    weight = clamp_fn(weight, 0.0, symbol_cap_fn(symbol))
    return float(weight)


def resolve_desired_weight(
    alert: Dict[str, Any],
    score: float,
    symbol: str,
    *,
    coerce_float_fn: Callable[[Any], float | None],
    desired_weight_fn: Callable[[float, str], float],
    symbol_cap_fn: Callable[[str], float],
    clamp_fn: Callable[[float, float, float], float],
) -> float:
    intent = (alert or {}).get("_model_intent") or {}
    target_weight = coerce_float_fn(intent.get("target_weight"))
    size_mult = coerce_float_fn(intent.get("size_mult"))

    if target_weight is not None:
        weight = abs(float(target_weight))
    else:
        weight = desired_weight_fn(score, symbol)

    if size_mult is not None:
        weight = float(weight) * max(0.0, float(size_mult))

    weight = clamp_fn(weight, 0.0, symbol_cap_fn(symbol))
    return float(weight)
