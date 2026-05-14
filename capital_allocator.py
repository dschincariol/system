"""Confidence-aware capital allocation for model competition.

This allocator keeps the existing allocation contract simple while replacing
static rank shaping with a signal that responds to:

- model scores and realized performance
- per-model and ensemble confidence
- drawdown-aware risk compression

The method returns normalized allocation fractions plus per-model diagnostics so
callers can preserve their current output schema and enrich it with allocator
telemetry.
"""

from __future__ import annotations

import math
import os
from statistics import median
from typing import Any, Dict, Iterable, Mapping

_EPSILON = 1e-12

DEFAULT_SCORE_WEIGHT = max(
    0.0,
    float(os.environ.get("CAPITAL_ALLOCATOR_SCORE_WEIGHT", "0.40") or 0.40),
)
DEFAULT_PERFORMANCE_WEIGHT = max(
    0.0,
    float(os.environ.get("CAPITAL_ALLOCATOR_PERFORMANCE_WEIGHT", "0.25") or 0.25),
)
DEFAULT_CONFIDENCE_WEIGHT = max(
    0.0,
    float(os.environ.get("CAPITAL_ALLOCATOR_CONFIDENCE_WEIGHT", "0.20") or 0.20),
)
DEFAULT_RISK_WEIGHT = max(
    0.0,
    float(os.environ.get("CAPITAL_ALLOCATOR_RISK_WEIGHT", "0.15") or 0.15),
)
DEFAULT_SOFTMAX_TEMP = max(
    1e-6,
    float(os.environ.get("CAPITAL_ALLOCATOR_SOFTMAX_TEMP", "0.35") or 0.35),
)
DEFAULT_SOFTMAX_MIX = max(
    0.0,
    min(1.0, float(os.environ.get("CAPITAL_ALLOCATOR_SOFTMAX_MIX", "0.15") or 0.15)),
)
DEFAULT_FLOOR = max(
    0.0,
    min(0.25, float(os.environ.get("CAPITAL_ALLOCATOR_MIN_MODEL_FLOOR", "0.02") or 0.02)),
)
DEFAULT_MAX_MODEL_ALLOCATION = max(
    0.0,
    min(1.0, float(os.environ.get("CAPITAL_ALLOCATOR_MAX_MODEL_ALLOCATION", "0.70") or 0.70)),
)
DEFAULT_ANCHOR_MIX = max(
    0.0,
    min(1.0, float(os.environ.get("CAPITAL_ALLOCATOR_ANCHOR_MIX", "0.30") or 0.30)),
)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    if not math.isfinite(out):
        return float(default)
    return float(out)


def _clip(value: Any, lo: float, hi: float, *, default: float | None = None) -> float:
    if default is None:
        default = lo
    numeric = _safe_float(value, default)
    return float(max(float(lo), min(float(hi), numeric)))


def _clip01(value: Any, *, default: float = 0.0) -> float:
    return _clip(value, 0.0, 1.0, default=default)


def _bounded_tanh_01(value: Any, scale: float) -> float:
    numeric = _safe_float(value, 0.0)
    effective_scale = max(float(scale), _EPSILON)
    return float(0.5 + (0.5 * math.tanh(float(numeric) / effective_scale)))


def _robust_scale(values: Iterable[Any], *, fallback: float = 1.0) -> float:
    cleaned = [
        abs(_safe_float(value, math.nan))
        for value in values
        if math.isfinite(_safe_float(value, math.nan))
    ]
    positive = [value for value in cleaned if value > _EPSILON]
    if positive:
        return float(max(median(positive), _EPSILON))
    return float(max(fallback, _EPSILON))


def _resolve_name(row: Mapping[str, Any]) -> str:
    for key in ("model_name", "name", "model", "model_id", "strategy_name", "strategy"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    return ""


def _normalize_nonnegative(weights: Mapping[str, Any]) -> Dict[str, float]:
    cleaned = {
        str(key): max(0.0, _safe_float(value, 0.0))
        for key, value in (weights or {}).items()
    }
    total = sum(cleaned.values())
    if total <= _EPSILON:
        return {key: 0.0 for key in cleaned.keys()}
    return {key: float(value) / float(total) for key, value in cleaned.items()}


def _softmax(weights: Mapping[str, float], *, temperature: float) -> Dict[str, float]:
    if not weights:
        return {}
    temp = max(float(temperature), 1e-6)
    max_value = max(float(value) for value in weights.values())
    exp_weights = {
        str(key): float(math.exp((float(value) - float(max_value)) / temp))
        for key, value in weights.items()
    }
    return _normalize_nonnegative(exp_weights)


def _waterfill_allocate(
    deployable: float,
    targets: Mapping[str, float],
    caps: Mapping[str, float],
) -> Dict[str, float]:
    deployable_capital = max(0.0, float(deployable))
    allocations = {str(name): 0.0 for name in targets.keys()}
    if deployable_capital <= _EPSILON or not allocations:
        return allocations

    active = {
        str(name)
        for name in allocations.keys()
        if max(0.0, _safe_float(caps.get(name), 0.0)) > _EPSILON
    }
    if not active:
        return allocations

    target_weights = _normalize_nonnegative(
        {name: max(0.0, _safe_float(targets.get(name), 0.0)) for name in active}
    )
    if sum(target_weights.values()) <= _EPSILON:
        target_weights = {name: 1.0 / float(len(active)) for name in active}

    remaining = min(
        float(deployable_capital),
        sum(max(0.0, _safe_float(caps.get(name), 0.0)) for name in allocations.keys()),
    )
    while remaining > _EPSILON and active:
        active_targets = _normalize_nonnegative(
            {name: max(0.0, _safe_float(target_weights.get(name), 0.0)) for name in active}
        )
        if sum(active_targets.values()) <= _EPSILON:
            equal = 1.0 / float(len(active))
            active_targets = {name: equal for name in active}

        spent = 0.0
        next_active: set[str] = set()
        for name, share in active_targets.items():
            room = max(0.0, float(caps[name]) - float(allocations[name]))
            fill = min(float(remaining) * float(share), room)
            allocations[name] += fill
            spent += fill
            if room - fill > _EPSILON:
                next_active.add(name)

        if spent <= _EPSILON:
            break
        remaining = max(0.0, float(remaining) - float(spent))
        active = next_active

    return allocations


def _coerce_signal_rows(signals: Iterable[Mapping[str, Any]] | Mapping[str, Any]) -> list[Dict[str, Any]]:
    if isinstance(signals, Mapping):
        iterable: list[Dict[str, Any]] = []
        for key, value in signals.items():
            if isinstance(value, Mapping):
                row = dict(value)
                row.setdefault("model_name", str(key))
            else:
                row = {"model_name": str(key), "score": value}
            iterable.append(row)
    else:
        iterable = [dict(item or {}) for item in (signals or [])]

    rows: list[Dict[str, Any]] = []
    for item in iterable:
        if not isinstance(item, Mapping):
            continue
        name = _resolve_name(item)
        if not name:
            continue
        meta = item.get("meta")
        meta_map = dict(meta) if isinstance(meta, Mapping) else {}
        rows.append(
            {
                "name": str(name),
                "score": _safe_float(item.get("score"), 0.0),
                "capital_score": _safe_float(item.get("capital_score"), 0.0),
                "net_pnl": _safe_float(item.get("net_pnl"), 0.0),
                "raw_weight": max(0.0, _safe_float(item.get("raw_weight"), 0.0)),
                "performance_score": _clip01(item.get("performance_score"), default=0.5),
                "stability_score": _clip01(item.get("stability_score"), default=0.5),
                "effective_stability_score": _clip01(
                    item.get("effective_stability_score"),
                    default=_clip01(item.get("stability_score"), default=0.5),
                ),
                "avg_confidence": _clip01(
                    item.get("avg_confidence", meta_map.get("avg_confidence")),
                    default=0.5,
                ),
                "max_drawdown": max(
                    0.0,
                    _safe_float(
                        item.get("max_drawdown", meta_map.get("max_drawdown")),
                        0.0,
                    ),
                ),
                "recent_regression_stability": _clip01(
                    item.get("recent_regression_stability"),
                    default=0.65,
                ),
                "slippage_stability": _clip01(item.get("slippage_stability"), default=0.75),
                "governance_multiplier": _clip(
                    item.get("governance_multiplier"),
                    0.10,
                    1.0,
                    default=1.0,
                ),
                "model_risk_limit_multiplier": _clip(
                    item.get("model_risk_limit_multiplier"),
                    0.10,
                    1.0,
                    default=1.0,
                ),
            }
        )
    return rows


def _resolve_confidence_inputs(
    model_confidence: Mapping[str, Any] | float | int | None,
    rows: list[Dict[str, Any]],
    *,
    prior_weights: Mapping[str, float],
) -> tuple[float, Dict[str, float]]:
    confidence_map: Dict[str, float] = {}
    ensemble_confidence: float | None = None

    if isinstance(model_confidence, Mapping):
        nested = None
        for key in ("models", "per_model", "model_confidence"):
            value = model_confidence.get(key)
            if isinstance(value, Mapping):
                nested = value
                break
        if isinstance(nested, Mapping):
            confidence_map = {
                str(key): _clip01(value, default=0.5)
                for key, value in nested.items()
            }
        ensemble_confidence = _safe_float(
            model_confidence.get("ensemble_confidence", model_confidence.get("ensemble")),
            math.nan,
        )
    elif model_confidence is not None:
        ensemble_confidence = _clip01(model_confidence, default=0.5)

    for row in rows:
        confidence_map.setdefault(str(row["name"]), _clip01(row.get("avg_confidence"), default=0.5))

    if not math.isfinite(_safe_float(ensemble_confidence, math.nan)):
        weighted_sum = 0.0
        total_weight = 0.0
        for name, confidence in confidence_map.items():
            weight = max(0.0, _safe_float(prior_weights.get(name), 0.0))
            if weight <= 0.0:
                continue
            weighted_sum += float(confidence) * float(weight)
            total_weight += float(weight)
        if total_weight > _EPSILON:
            ensemble_confidence = weighted_sum / total_weight
        elif confidence_map:
            ensemble_confidence = sum(confidence_map.values()) / float(len(confidence_map))
        else:
            ensemble_confidence = 0.5

    return _clip01(ensemble_confidence, default=0.5), confidence_map


def _resolve_risk_inputs(
    risk_metrics: Mapping[str, Any] | None,
) -> tuple[Dict[str, Dict[str, Any]], str, float]:
    metrics = dict(risk_metrics or {})
    per_model = metrics.get("models")
    if not isinstance(per_model, Mapping):
        per_model = metrics.get("per_model")
    per_model_map = {
        str(key): dict(value)
        for key, value in (per_model or {}).items()
        if isinstance(value, Mapping)
    }
    strategy = str(metrics.get("strategy") or "proportional").strip().lower() or "proportional"
    max_model_allocation = _clip(
        metrics.get("max_model_allocation"),
        0.0,
        1.0,
        default=DEFAULT_MAX_MODEL_ALLOCATION,
    )
    return per_model_map, strategy, max_model_allocation


class CapitalAllocator:
    """Allocate model capital using score, confidence, and drawdown-aware risk."""

    def __init__(
        self,
        *,
        score_weight: float = DEFAULT_SCORE_WEIGHT,
        performance_weight: float = DEFAULT_PERFORMANCE_WEIGHT,
        confidence_weight: float = DEFAULT_CONFIDENCE_WEIGHT,
        risk_weight: float = DEFAULT_RISK_WEIGHT,
        softmax_temp: float = DEFAULT_SOFTMAX_TEMP,
        softmax_mix: float = DEFAULT_SOFTMAX_MIX,
        min_model_floor: float = DEFAULT_FLOOR,
        max_model_allocation: float = DEFAULT_MAX_MODEL_ALLOCATION,
        anchor_mix: float = DEFAULT_ANCHOR_MIX,
    ) -> None:
        self.score_weight = max(0.0, float(score_weight))
        self.performance_weight = max(0.0, float(performance_weight))
        self.confidence_weight = max(0.0, float(confidence_weight))
        self.risk_weight = max(0.0, float(risk_weight))
        self.softmax_temp = max(1e-6, float(softmax_temp))
        self.softmax_mix = _clip(float(softmax_mix), 0.0, 1.0, default=DEFAULT_SOFTMAX_MIX)
        self.min_model_floor = _clip(float(min_model_floor), 0.0, 0.25, default=DEFAULT_FLOOR)
        self.max_model_allocation = _clip(
            float(max_model_allocation),
            0.0,
            1.0,
            default=DEFAULT_MAX_MODEL_ALLOCATION,
        )
        self.anchor_mix = _clip(float(anchor_mix), 0.0, 1.0, default=DEFAULT_ANCHOR_MIX)

    def allocate(
        self,
        signals: Iterable[Mapping[str, Any]] | Mapping[str, Any],
        model_confidence: Mapping[str, Any] | float | int | None,
        risk_metrics: Mapping[str, Any] | None,
    ) -> Dict[str, Any]:
        """Blend model scores, confidence, and risk controls into capital weights."""
        rows = _coerce_signal_rows(signals)
        if not rows:
            return {
                "allocations": {},
                "details": {},
                "ensemble_confidence": 0.0,
                "strategy": "proportional",
            }

        if len(rows) == 1:
            row = dict(rows[0])
            name = str(row["name"])
            ensemble_confidence, confidence_map = _resolve_confidence_inputs(
                model_confidence,
                rows,
                prior_weights={name: 1.0},
            )
            return {
                "allocations": {name: 1.0},
                "details": {
                    name: {
                        "allocation_fraction": 1.0,
                        "prior_weight": 1.0,
                        "intelligent_weight": 1.0,
                        "blended_target_weight": 1.0,
                        "allocation_cap": 1.0,
                        "avg_confidence": float(confidence_map.get(name, row.get("avg_confidence", 0.5))),
                        "ensemble_confidence": float(ensemble_confidence),
                        "score_component": _bounded_tanh_01(row.get("score"), 1.0),
                        "performance_component": _clip01(row.get("performance_score"), default=0.5),
                        "risk_component": 1.0,
                        "underperformance_penalty": 1.0,
                        "drawdown_health": 1.0,
                    }
                },
                "ensemble_confidence": float(ensemble_confidence),
                "strategy": "proportional",
            }

        raw_weights = {
            str(row["name"]): max(0.0, _safe_float(row.get("raw_weight"), 0.0))
            for row in rows
        }
        prior_weights = _normalize_nonnegative(raw_weights)
        if sum(prior_weights.values()) <= _EPSILON:
            equal = 1.0 / float(len(rows))
            prior_weights = {str(row["name"]): equal for row in rows}

        ensemble_confidence, confidence_map = _resolve_confidence_inputs(
            model_confidence,
            rows,
            prior_weights=prior_weights,
        )
        risk_map, strategy, max_model_allocation = _resolve_risk_inputs(risk_metrics)

        style_power = 1.0
        style_anchor_mix = self.anchor_mix
        softmax_temp = self.softmax_temp
        softmax_mix = self.softmax_mix
        floor_share = self.min_model_floor
        if strategy == "equal_weight":
            style_power = 0.90
            style_anchor_mix = max(style_anchor_mix, 0.45)
            softmax_temp = max(softmax_temp, 0.55)
            softmax_mix = min(softmax_mix, 0.08)
        elif strategy == "winner_take_most":
            style_power = 1.20
            style_anchor_mix = min(style_anchor_mix, 0.20)
            softmax_temp = min(softmax_temp, 0.28)
            softmax_mix = max(softmax_mix, 0.22)
            floor_share = min(floor_share, 0.01)

        score_scale = _robust_scale(
            [row.get("score") for row in rows] + [row.get("capital_score") for row in rows],
            fallback=1.0,
        )
        pnl_scale = _robust_scale([row.get("net_pnl") for row in rows], fallback=100.0)
        drawdown_scale = _robust_scale([row.get("max_drawdown") for row in rows], fallback=max(1.0, pnl_scale))

        intelligent_scores: Dict[str, float] = {}
        details: Dict[str, Dict[str, float]] = {}
        for row in rows:
            name = str(row["name"])
            risk_row = dict(risk_map.get(name) or {})
            model_conf = _clip01(
                confidence_map.get(name, row.get("avg_confidence")),
                default=_safe_float(row.get("avg_confidence"), 0.5),
            )
            blended_confidence = float((0.65 * model_conf) + (0.35 * float(ensemble_confidence)))

            score_component = _bounded_tanh_01(row.get("score"), score_scale)
            capital_component = _bounded_tanh_01(row.get("capital_score"), score_scale)
            performance_component = _clip01(row.get("performance_score"), default=0.5)
            prior_weight_component = _clip01(prior_weights.get(name), default=1.0 / float(len(rows)))
            signal_component = _clip01(
                (0.40 * score_component)
                + (0.20 * capital_component)
                + (0.25 * performance_component)
                + (0.15 * prior_weight_component),
                default=0.5,
            )

            net_pnl_component = _bounded_tanh_01(row.get("net_pnl"), pnl_scale)
            recent_regression_component = _clip01(
                risk_row.get("recent_regression_stability", row.get("recent_regression_stability")),
                default=0.65,
            )
            governance_component = _clip(
                risk_row.get("governance_multiplier", row.get("governance_multiplier")),
                0.10,
                1.0,
                default=1.0,
            )
            underperformance_penalty = _clip(
                0.15
                + (0.45 * net_pnl_component)
                + (0.20 * capital_component)
                + (0.10 * recent_regression_component)
                + (0.10 * governance_component),
                0.15,
                1.0,
                default=0.5,
            )

            max_drawdown = max(
                0.0,
                _safe_float(
                    risk_row.get("max_drawdown", row.get("max_drawdown")),
                    row.get("max_drawdown", 0.0),
                ),
            )
            drawdown_health = _clip(
                1.0 / (1.0 + (float(max_drawdown) / max(float(drawdown_scale), _EPSILON))),
                0.05,
                1.0,
                default=1.0,
            )
            effective_stability = _clip01(
                risk_row.get("effective_stability_score", row.get("effective_stability_score")),
                default=_safe_float(row.get("effective_stability_score"), 0.5),
            )
            slippage_stability = _clip01(
                risk_row.get("slippage_stability", row.get("slippage_stability")),
                default=_safe_float(row.get("slippage_stability"), 0.75),
            )
            model_risk_limit = _clip(
                risk_row.get("model_risk_limit_multiplier", row.get("model_risk_limit_multiplier")),
                0.10,
                1.0,
                default=_safe_float(row.get("model_risk_limit_multiplier"), 1.0),
            )
            risk_component = _clip(
                (0.40 * drawdown_health)
                + (0.30 * effective_stability)
                + (0.15 * slippage_stability)
                + (0.15 * model_risk_limit),
                0.05,
                1.0,
                default=0.5,
            )

            composite_component = (
                (self.score_weight * signal_component)
                + (self.performance_weight * performance_component)
                + (self.confidence_weight * blended_confidence)
                + (self.risk_weight * risk_component)
            )
            composite_component = _clip01(composite_component, default=0.5)
            confidence_multiplier = 0.50 + blended_confidence
            intelligent_score = max(
                _EPSILON,
                float(composite_component) ** float(style_power)
                * float(confidence_multiplier)
                * float(underperformance_penalty)
                * float(risk_component),
            )

            intelligent_scores[name] = float(intelligent_score)
            details[name] = {
                "avg_confidence": float(model_conf),
                "ensemble_confidence": float(ensemble_confidence),
                "blended_confidence": float(blended_confidence),
                "score_component": float(score_component),
                "capital_component": float(capital_component),
                "performance_component": float(performance_component),
                "prior_weight": float(prior_weights.get(name, 0.0)),
                "signal_component": float(signal_component),
                "underperformance_penalty": float(underperformance_penalty),
                "drawdown_health": float(drawdown_health),
                "risk_component": float(risk_component),
                "model_risk_limit_multiplier": float(model_risk_limit),
                "intelligent_score": float(intelligent_score),
            }

        intelligent_weights = _normalize_nonnegative(intelligent_scores)
        softmax_weights = _softmax(intelligent_scores, temperature=softmax_temp)
        blended_targets = _normalize_nonnegative(
            {
                name: (
                    ((1.0 - float(style_anchor_mix)) * float(intelligent_weights.get(name, 0.0)))
                    + (float(style_anchor_mix) * float(prior_weights.get(name, 0.0)))
                )
                for name in intelligent_scores.keys()
            }
        )
        target_weights = _normalize_nonnegative(
            {
                name: (
                    ((1.0 - float(softmax_mix)) * float(blended_targets.get(name, 0.0)))
                    + (float(softmax_mix) * float(softmax_weights.get(name, 0.0)))
                )
                for name in intelligent_scores.keys()
            }
        )

        floor_total = min(1.0, float(floor_share) * float(len(rows)))
        if floor_total > 0.0:
            residual = max(0.0, 1.0 - float(floor_total))
            target_weights = {
                name: float(floor_share) + (float(residual) * float(target_weights.get(name, 0.0)))
                for name in target_weights.keys()
            }

        equal_capacity = 1.0 / float(len(rows))
        base_cap = _clip(
            max_model_allocation or self.max_model_allocation,
            0.0,
            1.0,
            default=self.max_model_allocation,
        )
        caps: Dict[str, float] = {}
        for name, detail in details.items():
            cap_multiplier = _clip(
                0.35 + (0.65 * ((0.60 * detail["risk_component"]) + (0.40 * detail["drawdown_health"]))),
                max(equal_capacity, floor_share, 0.10),
                1.0,
                default=1.0,
            )
            caps[name] = float(min(base_cap, max(equal_capacity, float(base_cap) * float(cap_multiplier))))
            detail["allocation_cap"] = float(caps[name])
            detail["intelligent_weight"] = float(intelligent_weights.get(name, 0.0))
            detail["blended_target_weight"] = float(target_weights.get(name, 0.0))

        allocations = _waterfill_allocate(1.0, target_weights, caps)
        for name, value in allocations.items():
            details.setdefault(str(name), {})
            details[str(name)]["allocation_fraction"] = float(value)

        return {
            "allocations": allocations,
            "details": details,
            "ensemble_confidence": float(ensemble_confidence),
            "strategy": str(strategy),
        }


__all__ = ["CapitalAllocator"]
