"""Decision gate between ensemble output and execution.

This module is intentionally narrow: it decides whether a signal-derived
prediction should be allowed to reach real execution. The evaluation is
feature-flagged, fail-open when inputs are incomplete, and can be reused from
tests or higher-level orchestration code.
"""

from __future__ import annotations

import math
import os
from typing import Any, Dict, Mapping


def _env_flag(name: str, default: bool = False) -> bool:
    raw = str(os.environ.get(name, "1" if default else "0") or "").strip().lower()
    return raw not in {"", "0", "false", "no", "off"}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    if not math.isfinite(out):
        return float(default)
    return float(out)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _clip_confidence(value: Any, default: float = 0.0) -> float:
    raw = _safe_float(value, default)
    if not math.isfinite(raw):
        return float(default)
    return float(max(0.0, min(1.0, raw)))


def _normalize_risk(risk: Any) -> Dict[str, Any]:
    if isinstance(risk, Mapping):
        return dict(risk)
    if risk in (None, ""):
        return {}
    return {"risk_score": _safe_float(risk, 0.0)}


def _risk_increasing_action(risk: Mapping[str, Any]) -> bool:
    action = str(risk.get("action") or "").strip().upper()
    from_side = str(risk.get("from_side") or "").strip().upper()
    to_side = str(risk.get("to_side") or "").strip().upper()
    execution_target = str(risk.get("execution_target") or "").strip().lower()
    current_weight = abs(_safe_float(risk.get("current_weight"), 0.0))
    target_weight = abs(_safe_float(risk.get("target_weight"), 0.0))

    if execution_target == "shadow":
        return False
    if to_side == "FLAT" or target_weight <= 1e-12:
        return False
    if action in {"HOLD", "CLOSE", "DECREASE"}:
        return False
    if action in {"OPEN", "REVERSE"}:
        return True
    if action == "INCREASE":
        return target_weight > (current_weight + 1e-12)
    if from_side == "FLAT" and to_side in {"LONG", "SHORT"}:
        return True
    return target_weight > (current_weight + 1e-12)


class DecisionEngine:
    """Gate risk-increasing execution using prediction, confidence, and risk inputs."""

    def __init__(
        self,
        *,
        enabled: bool | None = None,
        min_confidence: float | None = None,
        min_abs_prediction: float | None = None,
        max_risk_score: float | None = None,
        max_expected_drawdown: float | None = None,
        max_market_stress: float | None = None,
        max_signal_age_s: int | None = None,
        max_open_positions: int | None = None,
        max_positions_per_symbol: int | None = None,
    ) -> None:
        self.enabled = _env_flag("DECISION_ENGINE_ENABLED", False) if enabled is None else bool(enabled)
        self.min_confidence = _clip_confidence(
            os.environ.get("DECISION_MIN_CONFIDENCE", os.environ.get("PORTFOLIO_MIN_CONF", "0.55"))
            if min_confidence is None
            else min_confidence,
            default=0.55,
        )
        self.min_abs_prediction = max(
            0.0,
            _safe_float(
                os.environ.get("DECISION_MIN_ABS_PREDICTION", os.environ.get("PORTFOLIO_MIN_ABS_Z", "0.75"))
                if min_abs_prediction is None
                else min_abs_prediction,
                0.75,
            ),
        )
        self.max_risk_score = max(
            0.0,
            _safe_float(
                os.environ.get("DECISION_MAX_RISK_SCORE", "1.0")
                if max_risk_score is None
                else max_risk_score,
                1.0,
            ),
        )
        self.max_expected_drawdown = max(
            0.0,
            _safe_float(
                os.environ.get(
                    "DECISION_MAX_EXPECTED_DRAWDOWN",
                    os.environ.get("PORTFOLIO_CAR_MAX_PER_SYMBOL", "0.03"),
                )
                if max_expected_drawdown is None
                else max_expected_drawdown,
                0.03,
            ),
        )
        self.max_market_stress = max(
            0.0,
            _safe_float(
                os.environ.get("DECISION_MAX_MARKET_STRESS", "0.85")
                if max_market_stress is None
                else max_market_stress,
                0.85,
            ),
        )
        self.max_signal_age_s = max(
            0,
            _safe_int(
                os.environ.get(
                    "DECISION_MAX_SIGNAL_AGE_S",
                    os.environ.get("EXECUTION_MAX_SIGNAL_AGE_S", "300"),
                )
                if max_signal_age_s is None
                else max_signal_age_s,
                300,
            ),
        )
        self.max_open_positions = max(
            0,
            _safe_int(
                os.environ.get("DECISION_MAX_OPEN_POSITIONS", os.environ.get("PORTFOLIO_MAX_POSITIONS", "3"))
                if max_open_positions is None
                else max_open_positions,
                3,
            ),
        )
        self.max_positions_per_symbol = max(
            0,
            _safe_int(
                os.environ.get("DECISION_MAX_POSITIONS_PER_SYMBOL", "0")
                if max_positions_per_symbol is None
                else max_positions_per_symbol,
                0,
            ),
        )

    def cache_token(self) -> str:
        """Return a stable cache key for the current decision thresholds."""
        return "|".join(
            [
                f"decision_enabled={int(self.enabled)}",
                f"decision_min_conf={self.min_confidence:.6f}",
                f"decision_min_abs_pred={self.min_abs_prediction:.6f}",
                f"decision_max_risk={self.max_risk_score:.6f}",
                f"decision_max_dd={self.max_expected_drawdown:.6f}",
                f"decision_max_stress={self.max_market_stress:.6f}",
                f"decision_max_age_s={int(self.max_signal_age_s)}",
                f"decision_max_open={int(self.max_open_positions)}",
                f"decision_max_symbol={int(self.max_positions_per_symbol)}",
            ]
        )

    def evaluate(self, prediction: Any, confidence: Any, risk: Any = None) -> Dict[str, Any]:
        """Return a structured execution decision with thresholds and block reasons."""
        risk_map = _normalize_risk(risk)
        prediction_value = _safe_float(prediction, math.nan)
        confidence_value = _clip_confidence(confidence, default=math.nan)
        prediction_available = math.isfinite(prediction_value)
        confidence_available = math.isfinite(confidence_value)
        risk_increasing = _risk_increasing_action(risk_map)

        result: Dict[str, Any] = {
            "enabled": bool(self.enabled),
            "execute": True,
            "reason": "ok",
            "reasons": [],
            "risk_increasing": bool(risk_increasing),
            "prediction": (float(prediction_value) if prediction_available else None),
            "confidence": (float(confidence_value) if confidence_available else None),
            "thresholds": {
                "min_confidence": float(self.min_confidence),
                "min_abs_prediction": float(self.min_abs_prediction),
                "max_risk_score": float(self.max_risk_score),
                "max_expected_drawdown": float(self.max_expected_drawdown),
                "max_market_stress": float(self.max_market_stress),
                "max_signal_age_s": int(self.max_signal_age_s),
                "max_open_positions": int(self.max_open_positions),
                "max_positions_per_symbol": int(self.max_positions_per_symbol),
            },
            "risk": dict(risk_map),
        }

        if not self.enabled:
            result["reason"] = "feature_flag_disabled"
            return result

        if not risk_increasing:
            result["reason"] = "pass_through_non_risk_increasing"
            return result

        if not prediction_available:
            result["reason"] = "prediction_missing_fail_open"
            return result

        if not confidence_available:
            result["reason"] = "confidence_missing_fail_open"
            return result

        reasons: list[str] = []

        if abs(float(prediction_value)) < float(self.min_abs_prediction):
            reasons.append("prediction_below_threshold")
        if float(confidence_value) < float(self.min_confidence):
            reasons.append("confidence_below_threshold")

        risk_score = _safe_float(
            risk_map.get("risk_score", risk_map.get("risk", risk_map.get("score", 0.0))),
            0.0,
        )
        if float(self.max_risk_score) > 0.0 and float(risk_score) > float(self.max_risk_score):
            reasons.append("risk_score_above_limit")

        expected_drawdown = _safe_float(
            risk_map.get("expected_drawdown", risk_map.get("expected_dd", 0.0)),
            0.0,
        )
        if float(self.max_expected_drawdown) > 0.0 and float(expected_drawdown) > float(self.max_expected_drawdown):
            reasons.append("expected_drawdown_above_limit")

        market_stress = _safe_float(risk_map.get("market_stress", 0.0), 0.0)
        if float(self.max_market_stress) > 0.0 and float(market_stress) > float(self.max_market_stress):
            reasons.append("market_stress_above_limit")

        signal_age_s = max(0.0, _safe_float(risk_map.get("signal_age_s", 0.0), 0.0))
        if int(self.max_signal_age_s) > 0 and float(signal_age_s) > float(self.max_signal_age_s):
            reasons.append("signal_stale")

        if risk_map.get("allow") is False or risk_map.get("risk_blocked") is True:
            reasons.append("risk_blocked")

        new_position = bool(risk_map.get("is_new_position"))
        if not new_position:
            from_side = str(risk_map.get("from_side") or "").strip().upper()
            to_side = str(risk_map.get("to_side") or "").strip().upper()
            new_position = from_side == "FLAT" and to_side in {"LONG", "SHORT"}

        open_positions = max(0, _safe_int(risk_map.get("open_positions"), 0))
        symbol_open_positions = max(0, _safe_int(risk_map.get("symbol_open_positions"), 0))

        if int(self.max_open_positions) > 0 and new_position and open_positions >= int(self.max_open_positions):
            reasons.append("open_position_limit")
        if (
            int(self.max_positions_per_symbol) > 0
            and new_position
            and symbol_open_positions >= int(self.max_positions_per_symbol)
        ):
            reasons.append("symbol_position_limit")

        if reasons:
            result["execute"] = False
            result["reasons"] = list(reasons)
            result["reason"] = "|".join(reasons)
        return result

    def should_execute(self, prediction: Any, confidence: Any, risk: Any = None) -> bool:
        """Return only the final execute flag from :meth:`evaluate`."""
        return bool(self.evaluate(prediction, confidence, risk).get("execute"))


DEFAULT_ENGINE = DecisionEngine()


def evaluate_decision(prediction: Any, confidence: Any, risk: Any = None) -> Dict[str, Any]:
    """Evaluate one execution decision with the process-wide default engine."""
    return DEFAULT_ENGINE.evaluate(prediction, confidence, risk)


def should_execute(prediction: Any, confidence: Any, risk: Any = None) -> bool:
    """Return whether the default decision engine would allow execution."""
    return DEFAULT_ENGINE.should_execute(prediction, confidence, risk)


__all__ = [
    "DecisionEngine",
    "DEFAULT_ENGINE",
    "evaluate_decision",
    "should_execute",
]
