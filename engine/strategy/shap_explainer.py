"""Normalized prediction explainability helpers.

This module provides a single explanation payload contract across model
families. Real SHAP values are emitted for the LightGBM family via native
`pred_contrib` support; other families degrade to explicitly labeled fallback
payloads and never claim to be SHAP.
"""

from __future__ import annotations

import math
import os
from typing import Any, Dict, List, Optional, Sequence

import numpy as np

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger

LOG = get_logger("engine.strategy.shap_explainer")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="strategy_shap_explainer_nonfatal",
        code=code,
        message=code,
        error=error,
        level=30,
        component="engine.strategy.shap_explainer",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _env_flag(name: str, default: bool) -> bool:
    raw = str(os.environ.get(name, "1" if default else "0") or "").strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return bool(default)


def _safe_float(value: Any, default: Optional[float] = 0.0) -> Optional[float]:
    if value is None:
        return default
    if isinstance(value, str) and not value.strip():
        return default
    try:
        out = float(value)
    except Exception:
        return default
    if not math.isfinite(out):
        return default
    return float(out)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _normalize_feature_ids(values: Sequence[Any] | None) -> List[str]:
    out: List[str] = []
    seen = set()
    for raw in values or []:
        feature_id = str(raw or "").strip()
        if not feature_id or feature_id in seen:
            continue
        seen.add(feature_id)
        out.append(feature_id)
    return out


def _coerce_feature_map(feature_snapshot: Any) -> Dict[str, Any]:
    if isinstance(feature_snapshot, dict):
        nested = feature_snapshot.get("features")
        if isinstance(nested, dict):
            return dict(nested)
        if any(not isinstance(value, dict) for value in feature_snapshot.values()):
            return dict(feature_snapshot)
    return {}


def _fallback_context(feature_snapshot: Any) -> Dict[str, Any]:
    if not isinstance(feature_snapshot, dict):
        return {}
    raw = feature_snapshot.get("explain_context")
    context = dict(raw) if isinstance(raw, dict) else {}
    allowed_keys = (
        "fallback_knn",
        "horizon_s",
        "model_kind",
        "model_n",
        "prediction_source",
        "prior",
        "prior_only",
        "regime",
        "regime_at_trade",
        "regime_source",
        "requested_model_family",
        "serve_fallback",
    )
    return {
        str(key): context.get(key)
        for key in allowed_keys
        if key in context
    }


def shap_explanations_enabled() -> bool:
    """Return whether normalized explanation payloads are enabled."""
    return _env_flag("SHAP_EXPLANATIONS_ENABLED", False)


def shap_live_compute_enabled() -> bool:
    """Return whether live explanation computation is allowed at serve time."""
    return _env_flag("SHAP_LIVE_COMPUTE_ENABLED", False)


def shap_persist_explanations_enabled() -> bool:
    """Return whether explanation payloads should be persisted with predictions."""
    return _env_flag("SHAP_PERSIST_EXPLANATIONS", True)


def shap_top_k() -> int:
    """Return how many top contributing features to expose per explanation."""
    return max(1, _safe_int(os.environ.get("SHAP_TOP_K", "10"), 10))


def model_family_supports_shap(model_family: Any) -> bool:
    """Return whether the model family has native SHAP-style support."""
    family = str(model_family or "").strip().lower()
    return family == "gbm_regressor"


def _sorted_feature_rows(rows: Sequence[Dict[str, Any]], *, top_k: int) -> List[Dict[str, Any]]:
    normalized_rows: List[Dict[str, Any]] = []
    for raw in rows or []:
        if not isinstance(raw, dict):
            continue
        feature_id = str(raw.get("feature_id") or raw.get("name") or raw.get("feature") or "").strip()
        if not feature_id:
            continue
        attribution = _safe_float(
            raw.get("attribution"),
            _safe_float(raw.get("contribution"), _safe_float(raw.get("score"), 0.0)),
        )
        value = _safe_float(raw.get("value"), None)
        attr = float(attribution or 0.0)
        normalized_rows.append(
            {
                "feature_id": feature_id,
                "value": value,
                "attribution": attr,
                "abs_attribution": float(abs(attr)),
            }
        )
    normalized_rows.sort(
        key=lambda row: (
            -float(row.get("abs_attribution") or 0.0),
            str(row.get("feature_id") or ""),
        )
    )
    limited = normalized_rows[: max(0, int(top_k))]
    for idx, row in enumerate(limited, start=1):
        attr = float(row.get("attribution") or 0.0)
        row["rank"] = int(idx)
        row["direction"] = "positive" if attr > 0 else ("negative" if attr < 0 else "neutral")
    return limited


def normalize_explanation_payload(raw_explanation: Any, feature_ids: Sequence[Any] | None = None) -> Dict[str, Any]:
    """Normalize raw family-specific explanation data into the shared payload contract."""
    raw = dict(raw_explanation) if isinstance(raw_explanation, dict) else {}
    feature_schema = dict(raw.get("feature_schema") or {}) if isinstance(raw.get("feature_schema"), dict) else {}
    normalized_feature_ids = _normalize_feature_ids(
        raw.get("feature_ids") or feature_schema.get("feature_ids") or feature_ids
    )
    if not normalized_feature_ids:
        normalized_feature_ids = _normalize_feature_ids(feature_ids)

    model_family = str(raw.get("model_family") or raw.get("family") or "").strip()
    explanation_type = str(raw.get("explanation_type") or "unsupported").strip() or "unsupported"
    supports_shap = bool(raw.get("supports_shap")) if "supports_shap" in raw else model_family_supports_shap(model_family)
    is_shap = bool(raw.get("is_shap")) if "is_shap" in raw else explanation_type == "shap"

    rows = _sorted_feature_rows(
        raw.get("top_features") or raw.get("features") or [],
        top_k=max(1, _safe_int(raw.get("top_k"), len(raw.get("top_features") or raw.get("features") or []) or 10)),
    )
    diagnostics = dict(raw.get("diagnostics") or {}) if isinstance(raw.get("diagnostics"), dict) else {}
    base_value = _safe_float(raw.get("base_value"), None)
    available = bool(rows or diagnostics or base_value is not None or raw.get("available"))

    return {
        "available": bool(available),
        "base_value": base_value,
        "diagnostics": diagnostics,
        "explanation_type": explanation_type,
        "feature_count": int(len(normalized_feature_ids)),
        "feature_ids": list(normalized_feature_ids),
        "is_shap": bool(is_shap),
        "model_family": model_family,
        "supports_shap": bool(supports_shap),
        "top_features": list(rows),
    }


def _feature_value_proxy_payload(model_family: str, feature_snapshot: Any, *, top_k: int) -> Dict[str, Any]:
    feature_map = _coerce_feature_map(feature_snapshot)
    feature_ids = _normalize_feature_ids(
        (feature_snapshot or {}).get("feature_ids")
        if isinstance(feature_snapshot, dict)
        else feature_map.keys()
    )
    if not feature_ids:
        feature_ids = _normalize_feature_ids(feature_map.keys())

    rows = []
    for feature_id in feature_ids:
        value = _safe_float(feature_map.get(feature_id), 0.0)
        rows.append(
            {
                "feature_id": str(feature_id),
                "value": value,
                "attribution": float(value or 0.0),
            }
        )

    diagnostics = _fallback_context(feature_snapshot)
    diagnostics["proxy_basis"] = "raw_feature_value"
    diagnostics["provided_feature_count"] = int(len(feature_map))
    diagnostics["top_k"] = int(top_k)

    return normalize_explanation_payload(
        {
            "available": bool(rows or diagnostics),
            "diagnostics": diagnostics,
            "explanation_type": ("feature_value_proxy" if rows else "model_diagnostics_fallback"),
            "feature_ids": list(feature_ids),
            "features": rows,
            "is_shap": False,
            "model_family": str(model_family or ""),
            "supports_shap": False,
            "top_k": int(top_k),
        },
        feature_ids=feature_ids,
    )


def _gbm_shap_payload(model_blob: bytes, feature_snapshot: Any, *, top_k: int) -> Dict[str, Any]:
    from engine.strategy.gbm_regressor import load_gbm_model

    model, schema = load_gbm_model(model_blob)
    feature_ids = _normalize_feature_ids((schema or {}).get("feature_ids"))
    feature_map = _coerce_feature_map(feature_snapshot)
    values: List[float] = []
    missing: List[str] = []

    for feature_id in feature_ids:
        if feature_id in feature_map:
            values.append(float(_safe_float(feature_map.get(feature_id), 0.0) or 0.0))
        else:
            values.append(0.0)
            missing.append(str(feature_id))

    vector = np.asarray(values, dtype=np.float32).reshape(1, -1)
    raw = model.predict(vector, pred_contrib=True)
    contribs = np.asarray(raw, dtype=float).reshape(-1)
    if int(contribs.shape[0]) != int(len(feature_ids) + 1):
        raise ValueError(
            f"unexpected_gbm_pred_contrib_shape:{int(contribs.shape[0])}:{int(len(feature_ids) + 1)}"
        )

    diagnostics = {
        "feature_coverage": float((len(feature_ids) - len(missing)) / max(1, len(feature_ids))),
        "feature_set_tag": str((schema or {}).get("feature_set_tag") or ""),
        "missing_feature_ids": list(missing),
        "model_kind": "lightgbm",
        "provided_feature_count": int(len(feature_map)),
        "top_k": int(top_k),
    }

    rows = [
        {
            "feature_id": str(feature_id),
            "value": float(values[idx]),
            "attribution": float(contribs[idx]),
        }
        for idx, feature_id in enumerate(feature_ids)
    ]

    return normalize_explanation_payload(
        {
            "available": True,
            "base_value": float(contribs[-1]),
            "diagnostics": diagnostics,
            "explanation_type": "shap",
            "feature_ids": list(feature_ids),
            "features": rows,
            "is_shap": True,
            "model_family": "gbm_regressor",
            "supports_shap": True,
            "top_k": int(top_k),
        },
        feature_ids=feature_ids,
    )


def explain_prediction(
    model_family: Any,
    model_blob: Any,
    feature_snapshot: Any,
    top_k: int = 10,
) -> Dict[str, Any]:
    """Build a normalized explanation payload for one model prediction request."""
    family = str(model_family or "").strip().lower()
    top_n = max(1, int(top_k or 10))

    if family == "gbm_regressor" and model_blob:
        try:
            return _gbm_shap_payload(bytes(model_blob), feature_snapshot, top_k=top_n)
        except Exception as e:
            _warn_nonfatal(
                "SHAP_EXPLAINER_GBM_SHAP_FAILED",
                e,
                once_key=None,
                model_family=family,
                top_k=int(top_n),
            )
            fallback = _feature_value_proxy_payload(family, feature_snapshot, top_k=top_n)
            diagnostics = dict(fallback.get("diagnostics") or {})
            diagnostics["fallback_reason"] = f"gbm_shap_failed:{type(e).__name__}"
            fallback["diagnostics"] = diagnostics
            fallback["explanation_type"] = "feature_value_proxy"
            return fallback

    if family in {"embed_regressor", "temporal_predictor", "regime_stats"}:
        return _feature_value_proxy_payload(family, feature_snapshot, top_k=top_n)

    unsupported = normalize_explanation_payload(
        {
            "available": False,
            "diagnostics": _fallback_context(feature_snapshot),
            "explanation_type": "unsupported",
            "feature_ids": _normalize_feature_ids(
                (feature_snapshot or {}).get("feature_ids") if isinstance(feature_snapshot, dict) else []
            ),
            "is_shap": False,
            "model_family": family,
            "supports_shap": False,
            "top_k": int(top_n),
        }
    )
    return unsupported


__all__ = [
    "explain_prediction",
    "model_family_supports_shap",
    "normalize_explanation_payload",
    "shap_explanations_enabled",
    "shap_live_compute_enabled",
    "shap_persist_explanations_enabled",
    "shap_top_k",
]
