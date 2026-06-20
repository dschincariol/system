"""Uncertainty-driven execution sizing gates.

This module consumes the uncertainty diagnostics already carried through
model-intent, conformal, and OOD payloads. It does not create trading intent;
it only tells the execution policy whether risk-increasing orders should be
logged, shrunk, or blocked.
"""

from __future__ import annotations

import math
import os
import time
from collections.abc import Mapping, Sequence
from typing import Any

import numpy as np

from engine.strategy.conformal import conformal_mode, extract_conformal_payload
from engine.strategy.ood import extract_ood_payload


EPS = 1.0e-12
LIVE_BROKERS = {"alpaca", "ibkr", "interactive_brokers"}
SAFE_BROKERS = {"", "unknown", "sim", "paper", "sandbox", "test", "mock"}
VALID_MODES = {"log_only", "research", "enforce"}
VALID_PRODUCTION_POLICIES = {"log_only", "shrink", "strict"}


def _safe_float(value: Any, default: float = 0.0) -> float:
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


def _clip01(value: Any, default: float = 0.0) -> float:
    return max(0.0, min(1.0, _safe_float(value, default)))


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(str(name))
    if raw in (None, ""):
        return float(default)
    return _safe_float(raw, default)


def _uncertainty_mode() -> str:
    mode = str(os.environ.get("UNCERTAINTY_SIZING_MODE", "log_only") or "log_only").strip().lower()
    if mode in {"off", "disabled"}:
        return "log_only"
    if mode in {"gate", "gate_and_size", "suppress"}:
        return "enforce"
    return mode if mode in VALID_MODES else "log_only"


def _production_policy() -> str:
    return str(os.environ.get("UNCERTAINTY_SIZING_PRODUCTION_POLICY", "") or "").strip().lower()


def _policy_is_valid(policy: str) -> bool:
    return str(policy or "").strip().lower() in VALID_PRODUCTION_POLICIES


def _is_live_context(*, execution_mode: str = "", broker: str = "") -> bool:
    modes = {
        str(execution_mode or "").strip().lower(),
        str(os.environ.get("EXECUTION_MODE", "") or "").strip().lower(),
        str(os.environ.get("ENGINE_MODE", "") or "").strip().lower(),
    }
    if "live" in modes:
        return True
    broker_name = str(broker or os.environ.get("BROKER") or os.environ.get("LIVE_BROKER") or "").strip().lower()
    if broker_name in LIVE_BROKERS:
        return True
    return bool(broker_name and broker_name not in SAFE_BROKERS)


def _candidate_payloads(payload: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    obj = dict(payload or {})
    candidates: list[dict[str, Any]] = [obj]
    for key in ("explain", "reason", "model_intent", "alpha_intent", "signal", "ensemble_output", "uncertainty"):
        value = obj.get(key)
        if isinstance(value, Mapping):
            candidates.append(dict(value))
    for candidate in list(candidates):
        for key in ("model_intent", "signal", "conformal", "uncertainty", "ensemble_output"):
            nested = candidate.get(key)
            if isinstance(nested, Mapping):
                candidates.append(dict(nested))
    return candidates


def _first_finite(candidates: Sequence[Any]) -> float | None:
    for value in candidates:
        out = _safe_float(value, math.nan)
        if math.isfinite(out):
            return float(out)
    return None


def _prediction_from_payload(payload: Mapping[str, Any] | None) -> float | None:
    candidates = _candidate_payloads(payload)
    return _first_finite(
        [
            candidate.get(key)
            for candidate in candidates
            for key in ("prediction", "predicted_z", "expected_z", "score", "value")
        ]
    )


def _timestamp_from_payload(payload: Mapping[str, Any] | None, conformal: Mapping[str, Any]) -> int | None:
    candidates = _candidate_payloads(payload)
    raw = _first_finite(
        [
            conformal.get("ts_ms"),
            conformal.get("as_of_ts_ms"),
            *[
                candidate.get(key)
                for candidate in candidates
                for key in ("uncertainty_ts_ms", "model_ts_ms", "feature_ts_ms")
            ],
        ]
    )
    if raw is None:
        raw = _first_finite(
            [
                candidate.get(key)
                for candidate in candidates
                for key in ("ts_ms", "signal_ts_ms")
            ]
        )
    if raw is None or raw <= 0:
        return None
    return int(raw)


def ensemble_epistemic_uncertainty(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    """Estimate normalized member-disagreement uncertainty for ensemble payloads."""

    obj = dict(payload or {})
    members = obj.get("ensemble_members")
    if not isinstance(members, Sequence) or isinstance(members, (str, bytes, bytearray)):
        ensemble = obj.get("ensemble_output")
        if isinstance(ensemble, Mapping):
            members = ensemble.get("members")
    if not isinstance(members, Sequence) or isinstance(members, (str, bytes, bytearray)):
        return {"available": False, "reason": "missing_ensemble_members"}

    predictions: list[float] = []
    weights: list[float] = []
    for member in members:
        if not isinstance(member, Mapping):
            continue
        pred = _safe_float(member.get("prediction"), math.nan)
        if not math.isfinite(pred):
            continue
        weight = max(0.0, _safe_float(member.get("weight"), 1.0))
        predictions.append(float(pred))
        weights.append(float(weight))
    if len(predictions) < 2:
        return {"available": False, "reason": "insufficient_ensemble_members", "n": int(len(predictions))}

    pred_arr = np.asarray(predictions, dtype=np.float64)
    weight_arr = np.asarray(weights, dtype=np.float64)
    if not np.isfinite(weight_arr).all() or float(np.sum(weight_arr)) <= EPS:
        weight_arr = np.ones_like(pred_arr, dtype=np.float64)
    weight_arr = weight_arr / float(np.sum(weight_arr))
    mean = float(np.sum(pred_arr * weight_arr))
    variance = float(np.sum(weight_arr * ((pred_arr - mean) ** 2)))
    std = float(math.sqrt(max(0.0, variance)))
    normalized = float(std / max(abs(mean), 1.0))
    return {
        "available": True,
        "method": "weighted_member_prediction_std",
        "n": int(len(predictions)),
        "mean_prediction": float(mean),
        "std_prediction": float(std),
        "epistemic_uncertainty": float(normalized),
    }


def extract_uncertainty_payload(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    obj = dict(payload or {})
    candidates = _candidate_payloads(obj)
    ensemble_uncertainty = ensemble_epistemic_uncertainty(obj)

    model_uncertainty = _first_finite(
        [
            candidate.get(key)
            for candidate in candidates
            for key in ("uncertainty", "model_uncertainty", "predictive_uncertainty")
        ]
    )
    epistemic_uncertainty = _first_finite(
        [
            candidate.get(key)
            for candidate in candidates
            for key in ("epistemic_uncertainty", "mc_dropout_uncertainty", "ensemble_epistemic_uncertainty")
        ]
    )
    aleatoric_uncertainty = _first_finite(
        [
            candidate.get(key)
            for candidate in candidates
            for key in ("aleatoric_uncertainty", "data_uncertainty")
        ]
    )
    if epistemic_uncertainty is None and bool(ensemble_uncertainty.get("available")):
        epistemic_uncertainty = _safe_float(ensemble_uncertainty.get("epistemic_uncertainty"), math.nan)
        if not math.isfinite(epistemic_uncertainty):
            epistemic_uncertainty = None

    out = {
        "available": any(value is not None for value in (model_uncertainty, epistemic_uncertainty, aleatoric_uncertainty)),
        "model_uncertainty": (float(model_uncertainty) if model_uncertainty is not None else None),
        "epistemic_uncertainty": (float(epistemic_uncertainty) if epistemic_uncertainty is not None else None),
        "aleatoric_uncertainty": (float(aleatoric_uncertainty) if aleatoric_uncertainty is not None else None),
        "ensemble_epistemic": dict(ensemble_uncertainty),
    }
    return out


def _wide_interval_multiplier(
    *,
    conformal: Mapping[str, Any],
    conformal_gate: Mapping[str, Any] | None,
    prediction: float | None,
) -> tuple[float, dict[str, Any]]:
    if not bool(conformal.get("available", conformal_gate.get("available", False) if conformal_gate else False)):
        return 1.0, {"applied": False, "reason": "conformal_unavailable"}
    if not bool(conformal.get("interval_excludes_zero", conformal_gate.get("interval_excludes_zero") if conformal_gate else False)):
        return 1.0, {"applied": False, "reason": "interval_crosses_zero"}

    direct = _safe_float(
        conformal.get(
            "size_mult",
            conformal.get("conformal_size_mult", (conformal_gate or {}).get("size_mult", 1.0)),
        ),
        1.0,
    )
    direct = max(0.0, min(1.0, float(direct)))

    width = _safe_float(
        conformal.get("interval_width", (conformal_gate or {}).get("interval_width")),
        0.0,
    )
    pred = prediction
    if pred is None:
        pred = _safe_float(conformal.get("prediction"), math.nan)
    rel_width = 0.0
    computed = 1.0
    threshold = max(EPS, _env_float("UNCERTAINTY_WIDE_INTERVAL_REL_WIDTH", 1.0))
    if pred is not None and math.isfinite(float(pred)) and width > 0.0:
        rel_width = float(width / max(abs(float(pred)), EPS))
        if rel_width > threshold:
            width_scale = max(EPS, _env_float("CONFORMAL_SIZE_WIDTH_SCALE", 1.0))
            min_mult = max(0.0, min(1.0, _env_float("UNCERTAINTY_WIDE_INTERVAL_MIN_MULT", 0.0)))
            computed = max(min_mult, min(1.0, abs(float(pred)) / max(width * width_scale, EPS)))

    mult = min(float(direct), float(computed))
    return float(mult), {
        "applied": bool(mult < 1.0),
        "size_mult": float(mult),
        "direct_size_mult": float(direct),
        "computed_size_mult": float(computed),
        "interval_width": float(width),
        "relative_width": float(rel_width),
        "threshold": float(threshold),
    }


def _uncertainty_multiplier(value: float | None, *, threshold_name: str, hard_name: str) -> tuple[float, bool]:
    if value is None:
        return 1.0, False
    threshold = max(0.0, _env_float(threshold_name, 0.70))
    hard_threshold = max(threshold + EPS, _env_float(hard_name, 0.95))
    numeric = max(0.0, _safe_float(value, 0.0))
    if numeric < threshold:
        return 1.0, False
    if numeric >= hard_threshold:
        return 0.0, True
    span = max(EPS, hard_threshold - threshold)
    return max(0.0, min(1.0, 1.0 - ((numeric - threshold) / span))), False


def uncertainty_gate_from_payload(
    payload: Mapping[str, Any] | None,
    *,
    conformal_gate: Mapping[str, Any] | None = None,
    ood_gate: Mapping[str, Any] | None = None,
    execution_mode: str = "",
    broker: str = "",
    risk_increasing: bool = True,
    now_ms: int | None = None,
) -> dict[str, Any]:
    mode = _uncertainty_mode()
    prod_policy = _production_policy()
    live = _is_live_context(execution_mode=execution_mode, broker=broker)
    policy_required = bool(live and risk_increasing)
    if policy_required and not _policy_is_valid(prod_policy):
        return {
            "enabled": True,
            "mode": str(mode),
            "production_policy": str(prod_policy),
            "production_policy_required": True,
            "live_context": True,
            "risk_increasing": bool(risk_increasing),
            "applied": True,
            "hard_block": True,
            "action": "HARD_BLOCK",
            "multiplier": 0.0,
            "reason": "production_policy_missing",
            "reasons": ["production_policy_missing"],
        }

    effective_policy = str(prod_policy if policy_required else "").strip().lower()
    if not effective_policy:
        effective_policy = "shrink" if mode == "enforce" else "log_only"

    conformal = extract_conformal_payload(payload)
    ood = extract_ood_payload(payload)
    uncertainty = extract_uncertainty_payload(payload)
    prediction = _prediction_from_payload(payload)
    now = int(now_ms if now_ms is not None else time.time() * 1000)
    ts_ms = _timestamp_from_payload(payload, conformal)
    max_age_ms = max(1, _safe_int(os.environ.get("UNCERTAINTY_MAX_AGE_MS"), 5 * 60 * 1000))
    age_ms = (int(now) - int(ts_ms)) if ts_ms is not None else None
    stale = bool(age_ms is not None and age_ms > max_age_ms)
    has_uncertainty = bool(
        bool(conformal.get("available"))
        or bool(ood.get("available"))
        or bool(uncertainty.get("available"))
    )
    missing = not has_uncertainty

    reasons: list[str] = []
    multiplier = 1.0
    hard_block = False
    factors: dict[str, Any] = {}

    if not risk_increasing:
        return {
            "enabled": True,
            "mode": str(mode),
            "production_policy": str(effective_policy),
            "production_policy_required": bool(policy_required),
            "live_context": bool(live),
            "risk_increasing": False,
            "applied": False,
            "hard_block": False,
            "action": "LOG_ONLY" if mode in {"log_only", "research"} else "NONE",
            "multiplier": 1.0,
            "reason": "risk_reducing_order",
            "reasons": ["risk_reducing_order"],
            "conformal": dict(conformal_gate or conformal),
            "ood_gate": dict(ood_gate or {}),
            "uncertainty": dict(uncertainty),
        }

    conformal_gate_mode = str((conformal_gate or {}).get("mode") or conformal.get("mode") or conformal_mode()).strip().lower()
    conformal_size_active = conformal_gate_mode == "gate_and_size"
    should_apply = bool(effective_policy in {"shrink", "strict"} or conformal_size_active)

    if bool(conformal.get("available")) and not bool(
        conformal.get("interval_excludes_zero", (conformal_gate or {}).get("interval_excludes_zero", True))
    ):
        reasons.append("interval_crosses_zero")
        if should_apply:
            hard_block = True
            multiplier = 0.0

    wide_mult, wide_meta = _wide_interval_multiplier(
        conformal=conformal,
        conformal_gate=conformal_gate,
        prediction=prediction,
    )
    factors["wide_interval"] = dict(wide_meta)
    if should_apply and wide_mult < 1.0 and not hard_block:
        reasons.append("wide_interval")
        multiplier *= float(wide_mult)

    max_uncertainty = max(
        [
            _safe_float(value, 0.0)
            for value in (
                uncertainty.get("model_uncertainty"),
                uncertainty.get("epistemic_uncertainty"),
                uncertainty.get("aleatoric_uncertainty"),
            )
            if value is not None
        ]
        or [0.0]
    )
    u_mult, u_hard = _uncertainty_multiplier(
        max_uncertainty,
        threshold_name="UNCERTAINTY_HIGH_THRESHOLD",
        hard_name="UNCERTAINTY_HARD_THRESHOLD",
    )
    factors["model_uncertainty"] = {
        "max_uncertainty": float(max_uncertainty),
        "size_mult": float(u_mult),
        "hard": bool(u_hard),
    }
    if max_uncertainty > 0.0 and u_mult < 1.0:
        reasons.append("high_model_uncertainty")
        if should_apply:
            multiplier *= float(u_mult)
    if bool(u_hard) and should_apply:
        hard_block = True
        multiplier = 0.0

    if missing:
        reasons.append("missing_uncertainty")
        if should_apply:
            if effective_policy == "strict":
                hard_block = True
                multiplier = 0.0
            else:
                multiplier *= max(0.0, min(1.0, _env_float("UNCERTAINTY_MISSING_SIZE_MULT", 0.0)))
    elif ts_ms is None:
        reasons.append("missing_uncertainty_timestamp")
        if should_apply and effective_policy == "strict":
            hard_block = True
            multiplier = 0.0
    elif stale:
        reasons.append("stale_uncertainty")
        if should_apply:
            if effective_policy == "strict":
                hard_block = True
                multiplier = 0.0
            else:
                multiplier *= max(0.0, min(1.0, _env_float("UNCERTAINTY_STALE_SIZE_MULT", 0.25)))

    multiplier = max(0.0, min(1.0, float(multiplier)))
    applied = bool(should_apply and (hard_block or multiplier < 1.0))
    if hard_block:
        action = "HARD_BLOCK"
    elif applied:
        action = "SIZE_COMPRESSION"
    elif reasons:
        action = "LOG_ONLY"
    else:
        action = "NONE"

    return {
        "enabled": True,
        "mode": str(mode),
        "production_policy": str(effective_policy),
        "production_policy_required": bool(policy_required),
        "live_context": bool(live),
        "risk_increasing": bool(risk_increasing),
        "applied": bool(applied),
        "hard_block": bool(hard_block),
        "action": str(action),
        "multiplier": float(multiplier if should_apply else 1.0),
        "raw_multiplier": float(multiplier),
        "reason": str(reasons[0] if reasons else ""),
        "reasons": reasons,
        "missing_uncertainty": bool(missing),
        "stale_uncertainty": bool(stale),
        "uncertainty_ts_ms": (int(ts_ms) if ts_ms is not None else None),
        "uncertainty_age_ms": (int(age_ms) if age_ms is not None else None),
        "max_age_ms": int(max_age_ms),
        "conformal": dict(conformal_gate or conformal),
        "ood_gate": dict(ood_gate or {}),
        "uncertainty": dict(uncertainty),
        "factors": factors,
    }


__all__ = [
    "VALID_PRODUCTION_POLICIES",
    "ensemble_epistemic_uncertainty",
    "extract_uncertainty_payload",
    "uncertainty_gate_from_payload",
]
