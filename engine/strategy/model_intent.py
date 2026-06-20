"""
FILE: model_intent.py

Canonical model-intent payload helpers shared by event-processing jobs.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, List, Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.strategy.conformal import conformal_mode, extract_conformal_payload
from engine.strategy.ood import extract_ood_payload

LOG = get_logger("strategy.model_intent")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="strategy_model_intent_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.model_intent",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        out = float(value)
    except Exception as e:
        _warn_nonfatal(
            "MODEL_INTENT_SAFE_FLOAT_FAILED",
            e,
            once_key="safe_float",
            value=repr(value)[:120],
        )
        return default
    if out != out:
        return default
    return out


def _safe_list(values: Any) -> List[str]:
    if not isinstance(values, Iterable) or isinstance(values, (str, bytes, dict)):
        return []
    out: List[str] = []
    seen = set()
    for value in values:
        item = str(value or "").strip()
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def infer_selected_features(explain: Dict[str, Any]) -> List[str]:
    if not isinstance(explain, dict):
        return []

    for key in ("selected_features", "features_used", "feature_names", "feature_ids", "feature_set"):
        vals = _safe_list(explain.get(key))
        if vals:
            return vals

    model_block = explain.get("model")
    if isinstance(model_block, dict):
        for key in ("selected_features", "features_used", "feature_names", "feature_ids", "feature_set"):
            vals = _safe_list(model_block.get(key))
            if vals:
                return vals

    return []


def build_model_intent(
    *,
    symbol: str,
    horizon_s: int,
    expected_z: float,
    confidence: float,
    explain: Optional[Dict[str, Any]] = None,
    regime: Optional[str] = None,
    universe_score: Optional[float] = None,
    should_trade: bool = True,
    timing: str = "enter_now",
    target_weight: Optional[float] = None,
    size_mult: Optional[float] = None,
    prediction_strength: Optional[float] = None,
) -> Dict[str, Any]:
    ex = dict(explain or {})
    z = float(expected_z)
    conf = float(confidence)
    strength = _safe_float(prediction_strength)
    if strength is None:
        strength = _safe_float(ex.get("prediction_strength"))
    if strength is None:
        strength = abs(z) * max(0.0, conf)
    score = max(0.0, float(strength))
    u_score = _safe_float(universe_score, score)
    prob = _safe_float(ex.get("probability"), conf)
    uncertainty = _safe_float(ex.get("uncertainty"), max(0.0, 1.0 - conf))
    epistemic_uncertainty = _safe_float(
        ex.get("epistemic_uncertainty", ex.get("ensemble_epistemic_uncertainty"))
    )
    aleatoric_uncertainty = _safe_float(ex.get("aleatoric_uncertainty"))
    predictive_uncertainty = _safe_float(ex.get("predictive_uncertainty"))
    uncertainty_ts_ms = _safe_float(
        ex.get("uncertainty_ts_ms", ex.get("model_ts_ms", ex.get("feature_ts_ms")))
    )
    conf_raw = _safe_float(ex.get("raw_confidence"), conf)
    side = "FLAT"
    if z > 0:
        side = "LONG"
    elif z < 0:
        side = "SHORT"

    intent: Dict[str, Any] = {
        "schema_version": 1,
        "symbol": str(symbol or "").upper().strip(),
        "horizon_s": int(horizon_s),
        "should_trade": bool(should_trade),
        "timing": str(timing or "enter_now"),
        "side": side,
        "expected_z": float(z),
        "confidence": float(conf),
        "probability": float(prob if prob is not None else conf),
        "uncertainty": float(uncertainty if uncertainty is not None else max(0.0, 1.0 - conf)),
        "confidence_raw": float(conf_raw if conf_raw is not None else conf),
        "prediction_strength": float(score),
        "score": float(score),
        "selection_score": float(score),
        "trade_score": float(score),
        "include_in_universe": bool(should_trade),
        "universe_score": float(u_score if u_score is not None else score),
        "selected_features": infer_selected_features(ex),
    }

    uncertainty_detail: Dict[str, Any] = {}
    if epistemic_uncertainty is not None:
        intent["epistemic_uncertainty"] = float(max(0.0, epistemic_uncertainty))
        uncertainty_detail["epistemic_uncertainty"] = float(max(0.0, epistemic_uncertainty))
    if aleatoric_uncertainty is not None:
        intent["aleatoric_uncertainty"] = float(max(0.0, aleatoric_uncertainty))
        uncertainty_detail["aleatoric_uncertainty"] = float(max(0.0, aleatoric_uncertainty))
    if predictive_uncertainty is not None:
        intent["predictive_uncertainty"] = float(max(0.0, predictive_uncertainty))
        uncertainty_detail["predictive_uncertainty"] = float(max(0.0, predictive_uncertainty))
    if uncertainty_ts_ms is not None and uncertainty_ts_ms > 0:
        intent["uncertainty_ts_ms"] = int(uncertainty_ts_ms)
        uncertainty_detail["ts_ms"] = int(uncertainty_ts_ms)
    if isinstance(ex.get("uncertainty_detail"), dict):
        uncertainty_detail.update(dict(ex.get("uncertainty_detail") or {}))
    if uncertainty_detail:
        intent["uncertainty_detail"] = uncertainty_detail

    if regime is not None:
        intent["regime"] = str(regime)

    if target_weight is not None:
        tw = _safe_float(target_weight)
        if tw is not None:
            intent["target_weight"] = float(tw)

    if size_mult is not None:
        sm = _safe_float(size_mult)
        if sm is not None:
            intent["size_mult"] = float(sm)
    else:
        sm = _safe_float(ex.get("size_mult"))
        if sm is not None:
            intent["size_mult"] = float(sm)

    ood_payload = extract_ood_payload(ex)
    if ood_payload:
        ood_score = _safe_float(ood_payload.get("ood_score", ood_payload.get("ood_distance")))
        if ood_score is not None:
            intent["ood_score"] = float(ood_score)
            intent["ood_distance"] = float(ood_score)
        ood_threshold = _safe_float(ood_payload.get("threshold", ood_payload.get("ood_threshold")))
        if ood_threshold is not None:
            intent["ood_threshold"] = float(ood_threshold)
        ood_hard = _safe_float(ood_payload.get("hard_threshold", ood_payload.get("ood_hard_threshold")))
        if ood_hard is not None:
            intent["ood_hard_threshold"] = float(ood_hard)
        violation_count = _safe_float(ood_payload.get("range_violation_count", ood_payload.get("ood_range_violation_count")))
        if violation_count is not None:
            intent["ood_range_violation_count"] = int(violation_count)

    conformal_payload = extract_conformal_payload(ex)
    if conformal_payload:
        interval_lower = _safe_float(conformal_payload.get("interval_lower", conformal_payload.get("lower")))
        interval_upper = _safe_float(conformal_payload.get("interval_upper", conformal_payload.get("upper")))
        interval_width = _safe_float(conformal_payload.get("interval_width"))
        conformal_confidence = _safe_float(conformal_payload.get("confidence"))
        conformal_size_mult = _safe_float(conformal_payload.get("size_mult"))
        excludes_zero = bool(conformal_payload.get("interval_excludes_zero"))
        intent["conformal"] = dict(conformal_payload)
        intent["interval_excludes_zero"] = bool(excludes_zero)
        intent["conformal_interval_excludes_zero"] = bool(excludes_zero)
        if interval_lower is not None:
            intent["conformal_interval_lower"] = float(interval_lower)
        if interval_upper is not None:
            intent["conformal_interval_upper"] = float(interval_upper)
        if interval_width is not None:
            intent["conformal_interval_width"] = float(interval_width)
        if conformal_confidence is not None:
            intent["conformal_confidence"] = float(max(0.0, min(1.0, conformal_confidence)))
        if conformal_size_mult is not None:
            intent["conformal_size_mult"] = float(max(0.0, min(1.0, conformal_size_mult)))
        if conformal_confidence is not None and conformal_mode() in {"gate", "gate_and_size"}:
            conf_c = float(max(0.0, min(1.0, conformal_confidence)))
            intent["confidence"] = float(conf_c)
            intent["probability"] = float(conf_c)
            intent["uncertainty"] = float(1.0 - conf_c)

    tradability = ex.get("tradability")
    if isinstance(tradability, dict):
        for key in ("expected_ret_net", "p_win", "expected_dd"):
            val = _safe_float(tradability.get(key))
            if val is not None:
                intent[key] = float(val)

    return intent


def is_canonical_model_intent(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    try:
        return int(value.get("schema_version") or 0) >= 1
    except Exception as e:
        _warn_nonfatal(
            "MODEL_INTENT_SCHEMA_VERSION_CHECK_FAILED",
            e,
            once_key="schema_version_check",
            value=repr(value)[:240],
        )
        return False
