"""Multi-model prediction aggregation utilities.

This module combines multiple model predictions using either weighted averaging
or directional voting. Per-model weights are derived from two signals:

1. Historical accuracy stored with the model metadata or registry metrics.
2. Recent performance signals when available, falling back to historical
   accuracy when recent telemetry is absent.
"""

from __future__ import annotations

import math
import os
from typing import Any, Dict, Mapping, Sequence

from engine.regime_detector import normalize_regime_state

DEFAULT_METHOD = str(os.environ.get("INFERENCE_ENSEMBLE_METHOD", "weighted_average") or "weighted_average").strip()
DEFAULT_HISTORICAL_WEIGHT = max(0.0, float(os.environ.get("ENSEMBLE_HISTORICAL_WEIGHT", "0.6") or 0.6))
DEFAULT_RECENT_WEIGHT = max(0.0, float(os.environ.get("ENSEMBLE_RECENT_WEIGHT", "0.4") or 0.4))
MIN_MEMBER_WEIGHT = max(0.0, min(1.0, float(os.environ.get("ENSEMBLE_MIN_MEMBER_WEIGHT", "0.05") or 0.05)))
RECENT_PNL_SCALE = max(1.0, float(os.environ.get("ENSEMBLE_RECENT_PNL_SCALE", "100.0") or 100.0))
DEFAULT_PERFORMANCE_WINDOW = max(1, int(os.environ.get("ENSEMBLE_PERFORMANCE_WINDOW", "64") or 64))
DEFAULT_PERFORMANCE_MIN_SAMPLES = max(
    1,
    int(os.environ.get("ENSEMBLE_PERFORMANCE_MIN_SAMPLES", "16") or 16),
)

_HISTORICAL_KEYS = (
    "historical_accuracy",
    "directional_accuracy",
    "directional_acc",
    "accuracy",
    "win_rate",
    "quality_score",
    "auc",
    "f1",
    "r2",
)
_RECENT_SCORE_KEYS = (
    "recent_performance",
    "recent_accuracy",
    "recent_directional_accuracy",
    "recent_directional_acc",
    "recent_win_rate",
    "recent_quality_score",
    "shadow_win_rate",
    "competition_score",
    "win_rate_stability",
    "recent_regression_stability",
)
_ERROR_KEYS = ("rmse", "mae", "mse", "loss", "drawdown", "max_drawdown")
_RECENT_PNL_KEYS = ("recent_total_pnl", "recent_net_pnl", "recent_pnl", "net_pnl", "pnl")


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    if not math.isfinite(out):
        return float(default)
    return float(out)


def _clip01(value: Any, default: float = 0.0) -> float:
    return float(max(0.0, min(1.0, _safe_float(value, default))))


def _sign(value: Any) -> float:
    numeric = _safe_float(value, 0.0)
    if numeric > 0.0:
        return 1.0
    if numeric < 0.0:
        return -1.0
    return 0.0


def _mapping_sources(member: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    performance_metrics = member.get("performance_metrics")
    metadata = member.get("metadata")
    return (
        member,
        performance_metrics if isinstance(performance_metrics, Mapping) else {},
        metadata if isinstance(metadata, Mapping) else {},
    )


def _first_metric_value(
    sources: Sequence[Mapping[str, Any]],
    keys: Sequence[str],
) -> float | None:
    for source in sources:
        for key in keys:
            if key in source:
                return _safe_float(source.get(key), 0.0)
    return None


def _normalize_error_like(value: Any) -> float:
    numeric = max(0.0, _safe_float(value, 0.0))
    return _clip01(1.0 / (1.0 + numeric), default=0.5)


def _normalize_recent_pnl(value: Any) -> float:
    numeric = _safe_float(value, 0.0)
    return _clip01(0.5 + (0.5 * math.tanh(float(numeric) / float(RECENT_PNL_SCALE))), default=0.5)


def _resolve_member_identity(member: Mapping[str, Any]) -> dict[str, str]:
    metadata = member.get("metadata")
    metadata_map = metadata if isinstance(metadata, Mapping) else {}
    return {
        "symbol": str(member.get("symbol") or metadata_map.get("symbol") or "").strip().upper(),
        "model_id": str(member.get("model_id") or metadata_map.get("model_id") or "").strip(),
        "model_name": str(member.get("model_name") or metadata_map.get("model_name") or "").strip(),
        "model_version": str(
            member.get("model_version")
            or member.get("version")
            or metadata_map.get("model_version")
            or metadata_map.get("version")
            or ""
        ).strip(),
    }


def _extract_regime_filter(member: Mapping[str, Any]) -> dict[str, Any]:
    symbol = _resolve_member_identity(member)["symbol"]
    for source in _mapping_sources(member):
        candidate = source.get("regime")
        if isinstance(candidate, Mapping):
            return normalize_regime_state(candidate, symbol=symbol)
    return normalize_regime_state(None, symbol=symbol)


def _has_specific_regime(regime: Mapping[str, Any]) -> bool:
    return any(
        str(regime.get(field) or "unknown") != "unknown"
        for field in ("volatility_regime", "trend_regime", "liquidity_regime")
    )


def _build_model_performance_queries(
    identity: Mapping[str, str],
    *,
    regime_filter: Mapping[str, Any] | None = None,
) -> list[tuple[str, tuple[Any, ...]]]:
    queries: list[tuple[str, tuple[Any, ...]]] = []
    symbol = str(identity.get("symbol") or "")
    model_id = str(identity.get("model_id") or "")
    model_name = str(identity.get("model_name") or "")
    model_version = str(identity.get("model_version") or "")

    extra_where = ""
    extra_params: tuple[Any, ...] = ()
    if regime_filter is not None and _has_specific_regime(regime_filter):
        extra_where = " AND volatility_regime=? AND trend_regime=? AND liquidity_regime=?"
        extra_params = (
            str(regime_filter.get("volatility_regime") or "unknown"),
            str(regime_filter.get("trend_regime") or "unknown"),
            str(regime_filter.get("liquidity_regime") or "unknown"),
        )

    if model_id and symbol:
        queries.append((f"model_id=? AND symbol=?{extra_where}", (str(model_id), str(symbol), *extra_params)))
    if model_name and model_version and symbol:
        queries.append(
            (
                f"model_name=? AND model_version=? AND symbol=?{extra_where}",
                (str(model_name), str(model_version), str(symbol), *extra_params),
            )
        )
    if model_id:
        queries.append((f"model_id=?{extra_where}", (str(model_id), *extra_params)))
    if model_name and model_version:
        queries.append((f"model_name=? AND model_version=?{extra_where}", (str(model_name), str(model_version), *extra_params)))
    if model_name:
        queries.append((f"model_name=?{extra_where}", (str(model_name), *extra_params)))
    return queries


def _load_model_performance_rows(
    member: Mapping[str, Any],
    *,
    limit: int = DEFAULT_PERFORMANCE_WINDOW,
) -> tuple[list[tuple[Any, ...]], str]:
    identity = _resolve_member_identity(member)
    if not identity["model_name"] and not identity["model_id"]:
        return [], "metric_fallback"
    regime_filter = _extract_regime_filter(member)

    try:
        from engine.runtime.storage import connect as _connect
    except Exception:
        return [], "metric_fallback"

    con = None
    try:
        con = _connect(readonly=True)
        seen: set[tuple[str, tuple[Any, ...]]] = set()
        query_groups = []
        if _has_specific_regime(regime_filter):
            query_groups.append((_build_model_performance_queries(identity, regime_filter=regime_filter), "model_performance_regime"))
        query_groups.append((_build_model_performance_queries(identity, regime_filter=None), "model_performance"))

        for queries, source in query_groups:
            for where_sql, params in queries:
                lookup_key = (str(where_sql), tuple(params))
                if lookup_key in seen:
                    continue
                seen.add(lookup_key)
                try:
                    rows = con.execute(
                        f"""
                        SELECT rolling_score, directional_accuracy, pnl_impact, error
                        FROM model_performance
                        WHERE {where_sql}
                        ORDER BY "time" DESC, id DESC
                        LIMIT ?
                        """,
                        tuple([*params, int(limit)]),
                    ).fetchall()
                except Exception:
                    continue
                if rows:
                    return list(rows), str(source)
    except Exception:
        return [], "metric_fallback"
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass  # no-op-guard: allow best-effort cleanup
    return [], "metric_fallback"


def resolve_historical_accuracy(member: Mapping[str, Any]) -> float:
    """Extract a stable historical-accuracy signal from one member payload."""
    sources = _mapping_sources(member)

    direct_value = _first_metric_value(sources, _HISTORICAL_KEYS)
    if direct_value is not None:
        return _clip01(direct_value, default=0.5)

    error_value = _first_metric_value(sources, _ERROR_KEYS)
    if error_value is not None:
        return _normalize_error_like(error_value)

    selection_metric_value = member.get("selection_metric_value")
    if selection_metric_value is not None:
        if bool(member.get("selection_metric_higher_is_better", True)):
            return _clip01(selection_metric_value, default=0.5)
        return _normalize_error_like(selection_metric_value)

    return 0.5


def resolve_recent_performance(member: Mapping[str, Any]) -> float:
    """Extract a recent-performance signal from one member payload."""
    sources = _mapping_sources(member)

    direct_value = _first_metric_value(sources, _RECENT_SCORE_KEYS)
    if direct_value is not None:
        return _clip01(direct_value, default=0.5)

    pnl_value = _first_metric_value(sources, _RECENT_PNL_KEYS)
    if pnl_value is not None:
        return _normalize_recent_pnl(pnl_value)

    return resolve_historical_accuracy(member)


def _estimate_metric_blend_weight(
    member: Mapping[str, Any],
    *,
    historical_weight: float = DEFAULT_HISTORICAL_WEIGHT,
    recent_weight: float = DEFAULT_RECENT_WEIGHT,
    min_weight: float = MIN_MEMBER_WEIGHT,
) -> float:
    historical_accuracy = resolve_historical_accuracy(member)
    recent_performance = resolve_recent_performance(member)
    total = max(0.0, float(historical_weight)) + max(0.0, float(recent_weight))
    if total <= 0.0:
        hist_share = 0.5
        recent_share = 0.5
    else:
        hist_share = max(0.0, float(historical_weight)) / total
        recent_share = max(0.0, float(recent_weight)) / total
    score = (hist_share * historical_accuracy) + (recent_share * recent_performance)
    return float(max(float(min_weight), _clip01(score, default=0.5)))


def resolve_member_weight(
    member: Mapping[str, Any],
    *,
    historical_weight: float = DEFAULT_HISTORICAL_WEIGHT,
    recent_weight: float = DEFAULT_RECENT_WEIGHT,
    min_weight: float = MIN_MEMBER_WEIGHT,
    performance_min_samples: int = DEFAULT_PERFORMANCE_MIN_SAMPLES,
    performance_window: int = DEFAULT_PERFORMANCE_WINDOW,
) -> dict[str, Any]:
    """Blend explicit weights, metrics, and recent performance into one weight payload."""
    explicit_weight = _safe_float(member.get("weight"), math.nan)
    if math.isfinite(explicit_weight) and explicit_weight > 0.0:
        return {
            "weight": float(max(float(min_weight), float(explicit_weight))),
            "weight_source": "explicit",
            "baseline_weight": None,
            "performance_score": None,
            "performance_samples": 0,
            "performance_stability": 1.0,
        }

    baseline_weight = _estimate_metric_blend_weight(
        member,
        historical_weight=historical_weight,
        recent_weight=recent_weight,
        min_weight=min_weight,
    )
    rows, performance_source = _load_model_performance_rows(member, limit=performance_window)
    if not rows:
        return {
            "weight": float(baseline_weight),
            "weight_source": "metric_fallback",
            "baseline_weight": float(baseline_weight),
            "performance_score": None,
            "performance_samples": 0,
            "performance_stability": 0.0,
        }

    rolling_values = [_safe_float(row[0], math.nan) for row in rows if row[0] is not None]
    directional_values = [_clip01(row[1], default=0.0) for row in rows]
    pnl_values = [_normalize_recent_pnl(row[2]) for row in rows]
    error_values = [_normalize_error_like(row[3]) for row in rows]

    if rolling_values:
        performance_score = sum(_clip01(value, default=0.5) for value in rolling_values) / len(rolling_values)
    else:
        performance_score = (
            (0.50 * (sum(directional_values) / max(1, len(directional_values))))
            + (0.30 * (sum(error_values) / max(1, len(error_values))))
            + (0.20 * (sum(pnl_values) / max(1, len(pnl_values))))
        )

    performance_score = _clip01(performance_score, default=baseline_weight)
    stability = _clip01(
        float(len(rows)) / float(max(1, int(performance_min_samples))),
        default=1.0,
    )
    weight = float(
        max(
            float(min_weight),
            _clip01(
                ((1.0 - float(stability)) * float(baseline_weight))
                + (float(stability) * float(performance_score)),
                default=baseline_weight,
            ),
        )
    )
    return {
        "weight": float(weight),
        "weight_source": str(performance_source or "model_performance"),
        "baseline_weight": float(baseline_weight),
        "performance_score": float(performance_score),
        "performance_samples": int(len(rows)),
        "performance_stability": float(stability),
    }


def estimate_model_weight(
    member: Mapping[str, Any],
    *,
    historical_weight: float = DEFAULT_HISTORICAL_WEIGHT,
    recent_weight: float = DEFAULT_RECENT_WEIGHT,
    min_weight: float = MIN_MEMBER_WEIGHT,
) -> float:
    """Estimate one model's ensemble weight from its reported diagnostics."""
    details = resolve_member_weight(
        member,
        historical_weight=historical_weight,
        recent_weight=recent_weight,
        min_weight=min_weight,
    )
    return float(details.get("weight") or min_weight)


def _normalize_member(
    member: Mapping[str, Any],
    *,
    historical_weight: float,
    recent_weight: float,
    min_weight: float,
) -> dict[str, Any] | None:
    prediction = _safe_float(member.get("prediction"), math.nan)
    confidence = _clip01(member.get("confidence"), default=0.0)
    if not math.isfinite(prediction):
        return None
    historical_accuracy = resolve_historical_accuracy(member)
    recent_performance = resolve_recent_performance(member)
    weight_details = resolve_member_weight(
        member,
        historical_weight=historical_weight,
        recent_weight=recent_weight,
        min_weight=min_weight,
    )
    weight = float(weight_details.get("weight") or min_weight)
    return {
        "model_name": str(member.get("model_name") or "").strip() or "unknown_model",
        "model_version": (str(member.get("model_version") or member.get("version") or "").strip() or None),
        "prediction": float(prediction),
        "confidence": float(confidence),
        "historical_accuracy": float(historical_accuracy),
        "recent_performance": float(recent_performance),
        "weight": float(weight),
        "weight_source": str(weight_details.get("weight_source") or "metric_fallback"),
        "performance_samples": int(weight_details.get("performance_samples") or 0),
        "performance_stability": float(weight_details.get("performance_stability") or 0.0),
        "performance_score": (
            float(weight_details["performance_score"])
            if weight_details.get("performance_score") is not None
            else None
        ),
        "baseline_weight": (
            float(weight_details["baseline_weight"])
            if weight_details.get("baseline_weight") is not None
            else None
        ),
    }


def _aggregate_confidence(members: Sequence[Mapping[str, Any]], agreement: float) -> float:
    total_weight = sum(_safe_float(member.get("weight"), 0.0) for member in members)
    if total_weight <= 0.0:
        return 0.0
    confidence = sum(
        _safe_float(member.get("confidence"), 0.0) * _safe_float(member.get("weight"), 0.0)
        for member in members
    ) / total_weight
    return _clip01((0.65 * confidence) + (0.35 * _clip01(agreement, default=0.0)), default=0.0)


def _combine_weighted_average(members: Sequence[Mapping[str, Any]]) -> tuple[float, float]:
    total_weight = sum(_safe_float(member.get("weight"), 0.0) for member in members)
    if total_weight <= 0.0:
        return 0.0, 0.0
    prediction = sum(
        _safe_float(member.get("prediction"), 0.0) * _safe_float(member.get("weight"), 0.0)
        for member in members
    ) / total_weight
    agreement = abs(
        sum(_sign(member.get("prediction")) * _safe_float(member.get("weight"), 0.0) for member in members)
    ) / total_weight
    return float(prediction), _aggregate_confidence(members, agreement)


def _combine_voting(members: Sequence[Mapping[str, Any]]) -> tuple[float, float]:
    total_weight = sum(_safe_float(member.get("weight"), 0.0) for member in members)
    if total_weight <= 0.0:
        return 0.0, 0.0

    vote_score = sum(_sign(member.get("prediction")) * _safe_float(member.get("weight"), 0.0) for member in members)
    if abs(vote_score) <= 1e-12:
        return 0.0, _aggregate_confidence(members, 0.0)

    winning_direction = 1.0 if vote_score > 0.0 else -1.0
    winners = [member for member in members if _sign(member.get("prediction")) == winning_direction]
    winner_weight = sum(_safe_float(member.get("weight"), 0.0) for member in winners)
    if winner_weight <= 0.0:
        return 0.0, _aggregate_confidence(members, 0.0)

    prediction = sum(
        _safe_float(member.get("prediction"), 0.0) * _safe_float(member.get("weight"), 0.0)
        for member in winners
    ) / winner_weight
    agreement = abs(vote_score) / total_weight
    return float(prediction), _aggregate_confidence(members, agreement)


class EnsembleEngine:
    """Combine member predictions with historical and recent-performance weighting."""

    def __init__(
        self,
        *,
        default_method: str = DEFAULT_METHOD,
        historical_weight: float = DEFAULT_HISTORICAL_WEIGHT,
        recent_weight: float = DEFAULT_RECENT_WEIGHT,
        min_member_weight: float = MIN_MEMBER_WEIGHT,
    ) -> None:
        self.default_method = str(default_method or "weighted_average").strip() or "weighted_average"
        self.historical_weight = max(0.0, float(historical_weight))
        self.recent_weight = max(0.0, float(recent_weight))
        self.min_member_weight = max(0.0, min(1.0, float(min_member_weight)))

    def estimate_model_weight(self, member: Mapping[str, Any]) -> float:
        """Estimate the normalized contribution weight for one ensemble member."""
        return estimate_model_weight(
            member,
            historical_weight=self.historical_weight,
            recent_weight=self.recent_weight,
            min_weight=self.min_member_weight,
        )

    def combine_predictions(
        self,
        members: Sequence[Mapping[str, Any]],
        *,
        method: str | None = None,
    ) -> Dict[str, Any]:
        """Aggregate normalized member payloads into one ensemble prediction."""
        normalized_members = [
            normalized
            for normalized in (
                _normalize_member(
                    member,
                    historical_weight=self.historical_weight,
                    recent_weight=self.recent_weight,
                    min_weight=self.min_member_weight,
                )
                for member in (members or [])
            )
            if normalized is not None
        ]
        if not normalized_members:
            raise ValueError("ensemble_members_missing")

        resolved_method = str(method or self.default_method or "weighted_average").strip().lower() or "weighted_average"
        if resolved_method == "weighted_average":
            final_prediction, aggregated_confidence = _combine_weighted_average(normalized_members)
        elif resolved_method == "voting":
            final_prediction, aggregated_confidence = _combine_voting(normalized_members)
        else:
            raise ValueError(f"unsupported_ensemble_method:{resolved_method}")

        total_weight = sum(_safe_float(member.get("weight"), 0.0) for member in normalized_members)
        return {
            "final_prediction": float(final_prediction),
            "aggregated_confidence": float(aggregated_confidence),
            "method": str(resolved_method),
            "ensemble_size": int(len(normalized_members)),
            "total_weight": float(total_weight),
            "members": list(normalized_members),
        }


DEFAULT_ENGINE = EnsembleEngine()


def combine_predictions(
    members: Sequence[Mapping[str, Any]],
    *,
    method: str | None = None,
) -> Dict[str, Any]:
    """Combine predictions with the process-wide default ensemble engine."""
    return DEFAULT_ENGINE.combine_predictions(members, method=method)


__all__ = [
    "EnsembleEngine",
    "DEFAULT_ENGINE",
    "combine_predictions",
    "estimate_model_weight",
    "resolve_historical_accuracy",
    "resolve_member_weight",
    "resolve_recent_performance",
]
