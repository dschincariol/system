"""Per-model out-of-distribution scoring for live feature vectors.

The implementation uses a kNN-style dissimilarity index rather than a
Mahalanobis distance. That is intentionally conservative for this repo's
mixed, sparse, opt-in feature schemas: it avoids brittle covariance inversion
and normalizes live distance by the model's own training self-distances.
"""

from __future__ import annotations

import math
import os
import time
from collections.abc import Mapping, Sequence
from typing import Any

import numpy as np


DEFAULT_MAX_REFERENCE_ROWS = int(os.environ.get("OOD_REFERENCE_MAX_ROWS", "2000"))
DEFAULT_K = int(os.environ.get("OOD_KNN_K", "5"))
EPS = 1.0e-9


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    return float(out) if math.isfinite(out) else float(default)


def _feature_values(features: Any, feature_ids: Sequence[str]) -> np.ndarray:
    ids = [str(fid) for fid in list(feature_ids or [])]
    if isinstance(features, Mapping):
        nested = features.get("features")
        values = dict(nested) if isinstance(nested, Mapping) else dict(features)
        return np.asarray([_safe_float(values.get(fid), 0.0) for fid in ids], dtype=np.float32)
    arr = np.asarray(features, dtype=np.float32).reshape(-1)
    if int(arr.size) != int(len(ids)):
        raise ValueError(f"ood_feature_count_mismatch:{int(arr.size)}:{int(len(ids))}")
    return np.nan_to_num(arr.astype(np.float32, copy=False), nan=0.0, posinf=0.0, neginf=0.0)


def _deterministic_sample_indices(n_rows: int, max_rows: int) -> np.ndarray:
    n = int(max(0, n_rows))
    m = int(max(1, min(max_rows, n)))
    if n <= m:
        return np.arange(n, dtype=int)
    return np.unique(np.linspace(0, n - 1, num=m, dtype=int))


def _robust_scale(values: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    arr = np.asarray(values, dtype=np.float32)
    median = np.nanmedian(arr, axis=0).astype(np.float32)
    mad = np.nanmedian(np.abs(arr - median.reshape(1, -1)), axis=0).astype(np.float32)
    p1 = np.nanpercentile(arr, 1.0, axis=0).astype(np.float32)
    p99 = np.nanpercentile(arr, 99.0, axis=0).astype(np.float32)
    mad = np.where(np.isfinite(mad) & (mad > EPS), mad, 1.0).astype(np.float32)
    median = np.nan_to_num(median, nan=0.0, posinf=0.0, neginf=0.0)
    p1 = np.nan_to_num(p1, nan=0.0, posinf=0.0, neginf=0.0)
    p99 = np.nan_to_num(p99, nan=0.0, posinf=0.0, neginf=0.0)
    return median, mad, p1, p99


def _standardize(matrix: np.ndarray, median: np.ndarray, mad: np.ndarray) -> np.ndarray:
    arr = np.asarray(matrix, dtype=np.float32)
    return np.nan_to_num((arr - median.reshape(1, -1)) / mad.reshape(1, -1), nan=0.0, posinf=0.0, neginf=0.0)


def _mean_knn_distance(row: np.ndarray, reference: np.ndarray, *, k: int) -> float:
    ref = np.asarray(reference, dtype=np.float32)
    if ref.ndim != 2 or int(ref.shape[0]) <= 0:
        return 0.0
    vec = np.asarray(row, dtype=np.float32).reshape(1, -1)
    diff = ref - vec
    distances = np.sqrt(np.mean(diff * diff, axis=1)).astype(np.float64)
    finite = distances[np.isfinite(distances)]
    if finite.size <= 0:
        return 0.0
    positive = finite[finite > EPS]
    candidates = positive if positive.size else finite
    kk = max(1, min(int(k or DEFAULT_K), int(candidates.size)))
    nearest = np.partition(candidates, kk - 1)[:kk]
    return float(np.mean(nearest))


def build_ood_profile(
    matrix: Any,
    feature_ids: Sequence[str],
    *,
    max_reference_rows: int | None = None,
    k: int | None = None,
) -> dict[str, Any]:
    """Build a serializable kNN dissimilarity profile from training features."""

    ids = [str(fid) for fid in list(feature_ids or []) if str(fid or "").strip()]
    arr = np.asarray(matrix, dtype=np.float32)
    if arr.ndim == 1:
        arr = arr.reshape(1, -1)
    if arr.ndim != 2 or int(arr.shape[1]) != int(len(ids)) or int(arr.shape[0]) <= 0:
        return {"enabled": False, "reason": "invalid_training_matrix", "feature_ids": ids}

    arr = np.nan_to_num(arr.astype(np.float32, copy=False), nan=0.0, posinf=0.0, neginf=0.0)
    median, mad, p1, p99 = _robust_scale(arr)
    sample_idx = _deterministic_sample_indices(
        int(arr.shape[0]),
        int(max_reference_rows if max_reference_rows is not None else DEFAULT_MAX_REFERENCE_ROWS),
    )
    reference_raw = arr[sample_idx]
    reference_std = _standardize(reference_raw, median, mad)
    kk = max(1, int(k if k is not None else DEFAULT_K))
    self_raw = np.asarray([
        _mean_knn_distance(reference_std[idx], reference_std, k=kk)
        for idx in range(int(reference_std.shape[0]))
    ], dtype=np.float64)
    positive = self_raw[np.isfinite(self_raw) & (self_raw > EPS)]
    scale = float(np.median(positive)) if positive.size else 1.0
    if scale <= EPS or not math.isfinite(scale):
        scale = 1.0
    self_scores = self_raw / float(scale)
    p95_score = float(np.nanpercentile(self_scores, 95.0)) if self_scores.size else 1.0
    p99_score = float(np.nanpercentile(self_scores, 99.0)) if self_scores.size else max(1.0, p95_score)
    if not math.isfinite(p95_score) or p95_score <= 0.0:
        p95_score = 1.0
    if not math.isfinite(p99_score) or p99_score <= 0.0:
        p99_score = max(1.0, p95_score)

    return {
        "enabled": True,
        "method": "knn_dissimilarity",
        "version": 1,
        "feature_ids": ids,
        "n_train": int(arr.shape[0]),
        "n_reference": int(reference_std.shape[0]),
        "k": int(kk),
        "max_reference_rows": int(max_reference_rows if max_reference_rows is not None else DEFAULT_MAX_REFERENCE_ROWS),
        "median": median.astype(float).tolist(),
        "mad": mad.astype(float).tolist(),
        "p1": p1.astype(float).tolist(),
        "p99": p99.astype(float).tolist(),
        "reference_standardized": reference_std.astype(np.float32).tolist(),
        "self_distance_scale": float(scale),
        "self_distance_p95_score": float(p95_score),
        "self_distance_p99_score": float(p99_score),
        "default_suppress_threshold": float(1.5 * p95_score),
        "default_hard_threshold": float(max(2.0 * p95_score, 1.5 * p99_score)),
    }


def summarize_ood_profile(profile: Mapping[str, Any] | None) -> dict[str, Any]:
    p = dict(profile or {})
    return {
        "enabled": bool(p.get("enabled")),
        "method": str(p.get("method") or ""),
        "version": int(p.get("version") or 0),
        "feature_count": int(len(list(p.get("feature_ids") or []))),
        "n_train": int(p.get("n_train") or 0),
        "n_reference": int(p.get("n_reference") or 0),
        "k": int(p.get("k") or 0),
        "self_distance_p95_score": _safe_float(p.get("self_distance_p95_score"), 0.0),
        "default_suppress_threshold": _safe_float(p.get("default_suppress_threshold"), 0.0),
        "default_hard_threshold": _safe_float(p.get("default_hard_threshold"), 0.0),
    }


def _threshold_from_env(name: str, default_value: float) -> float:
    raw = os.environ.get(str(name))
    if raw in (None, ""):
        return float(default_value)
    return max(0.0, _safe_float(raw, default_value))


def score_ood(
    profile: Mapping[str, Any] | None,
    features: Any,
    *,
    now_ms: int | None = None,
) -> dict[str, Any]:
    """Score one live feature vector against a persisted OOD profile."""

    start = time.perf_counter()
    p = dict(profile or {})
    if not bool(p.get("enabled")):
        return {"enabled": False, "available": False, "reason": str(p.get("reason") or "missing_profile")}
    ids = [str(fid) for fid in list(p.get("feature_ids") or []) if str(fid or "").strip()]
    try:
        values = _feature_values(features, ids).astype(np.float32)
        median = np.asarray(p.get("median") or [], dtype=np.float32)
        mad = np.asarray(p.get("mad") or [], dtype=np.float32)
        p1 = np.asarray(p.get("p1") or [], dtype=np.float32)
        p99 = np.asarray(p.get("p99") or [], dtype=np.float32)
        reference = np.asarray(p.get("reference_standardized") or [], dtype=np.float32)
        if any(int(arr.size) != int(len(ids)) for arr in (median, mad, p1, p99)):
            raise ValueError("ood_profile_feature_stat_mismatch")
        if reference.ndim != 2 or int(reference.shape[1]) != int(len(ids)):
            raise ValueError("ood_profile_reference_shape_mismatch")
        mad = np.where(np.isfinite(mad) & (mad > EPS), mad, 1.0).astype(np.float32)
        standardized = ((values - median) / mad).astype(np.float32)
        raw_distance = _mean_knn_distance(standardized, reference, k=int(p.get("k") or DEFAULT_K))
        scale = max(EPS, _safe_float(p.get("self_distance_scale"), 1.0))
        score = float(raw_distance / scale)

        lower = p1 - (2.0 * mad)
        upper = p99 + (2.0 * mad)
        violation_mask = (values < lower) | (values > upper)
        robust_z = np.abs((values - median) / mad)
        violations = []
        for idx, violated in enumerate(violation_mask.tolist()):
            if not bool(violated):
                continue
            violations.append(
                {
                    "feature_id": str(ids[idx]),
                    "value": float(values[idx]),
                    "lower": float(lower[idx]),
                    "upper": float(upper[idx]),
                    "robust_z": float(robust_z[idx]),
                }
            )
        threshold = _threshold_from_env("OOD_SUPPRESS_THRESHOLD", _safe_float(p.get("default_suppress_threshold"), 1.5))
        hard_threshold = _threshold_from_env(
            "OOD_HARD_THRESHOLD",
            max(float(threshold) + EPS, _safe_float(p.get("default_hard_threshold"), max(2.0 * threshold, threshold + 1.0))),
        )
        latency_ms = (time.perf_counter() - start) * 1000.0
        return {
            "enabled": True,
            "available": True,
            "method": str(p.get("method") or "knn_dissimilarity"),
            "version": int(p.get("version") or 1),
            "ts_ms": int(now_ms if now_ms is not None else time.time() * 1000),
            "ood_score": float(score),
            "ood_distance": float(score),
            "raw_distance": float(raw_distance),
            "threshold": float(threshold),
            "hard_threshold": float(hard_threshold),
            "training_p95_score": _safe_float(p.get("self_distance_p95_score"), 0.0),
            "range_violation": bool(violations),
            "range_violation_count": int(len(violations)),
            "range_violations": violations[:20],
            "max_feature_robust_z": float(np.max(robust_z)) if robust_z.size else 0.0,
            "latency_ms": float(latency_ms),
        }
    except Exception as exc:
        latency_ms = (time.perf_counter() - start) * 1000.0
        return {
            "enabled": True,
            "available": False,
            "reason": f"score_failed:{type(exc).__name__}",
            "error": str(exc),
            "latency_ms": float(latency_ms),
        }


def extract_ood_payload(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    """Find OOD diagnostics in common order/explain/model-intent shapes."""

    obj = dict(payload or {})
    candidates: list[Mapping[str, Any]] = [obj]
    explain = obj.get("explain")
    if isinstance(explain, Mapping):
        candidates.append(dict(explain))
        signal = explain.get("signal")
        if isinstance(signal, Mapping):
            candidates.append(dict(signal))
        model_intent = explain.get("model_intent")
        if isinstance(model_intent, Mapping):
            candidates.append(dict(model_intent))
        ood = explain.get("ood")
        if isinstance(ood, Mapping):
            candidates.append(dict(ood))
    model_intent = obj.get("model_intent")
    if isinstance(model_intent, Mapping):
        candidates.append(dict(model_intent))
    alpha_intent = obj.get("alpha_intent")
    if isinstance(alpha_intent, Mapping):
        candidates.append(dict(alpha_intent))

    for candidate in candidates:
        nested = candidate.get("ood")
        if isinstance(nested, Mapping):
            nested_dict = dict(nested)
            if nested_dict.get("ood_score") is not None or nested_dict.get("ood_distance") is not None:
                return nested_dict
        for key in ("ood_score", "ood_distance", "feature_ood_distance", "distance_to_train"):
            if candidate.get(key) is not None:
                score = _safe_float(candidate.get(key), 0.0)
                return {
                    "enabled": True,
                    "available": True,
                    "ood_score": float(score),
                    "ood_distance": float(score),
                    "threshold": _safe_float(candidate.get("ood_threshold"), 0.0),
                    "hard_threshold": _safe_float(candidate.get("ood_hard_threshold"), 0.0),
                    "range_violation_count": int(_safe_float(candidate.get("ood_range_violation_count"), 0.0)),
                }
    return {}


def ood_gate_from_payload(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    """Convert OOD diagnostics into an execution-policy gate decision."""

    mode = str(os.environ.get("OOD_MODE", "log_only") or "log_only").strip().lower()
    if mode not in {"log_only", "suppress"}:
        mode = "log_only"
    ood = extract_ood_payload(payload)
    if not ood:
        return {"enabled": False, "applied": False, "mode": mode, "multiplier": 1.0, "hard_block": False, "reason": "missing_ood_score"}

    score = max(0.0, _safe_float(ood.get("ood_score", ood.get("ood_distance")), 0.0))
    threshold = _safe_float(ood.get("threshold"), 0.0)
    hard_threshold = _safe_float(ood.get("hard_threshold"), 0.0)
    if threshold <= 0.0:
        threshold = _threshold_from_env("OOD_SUPPRESS_THRESHOLD", 1.5)
    if hard_threshold <= threshold:
        hard_threshold = _threshold_from_env("OOD_HARD_THRESHOLD", max(3.0, 2.0 * threshold))
    action = "NONE"
    multiplier = 1.0
    hard_block = False
    if score > threshold:
        action = "SIZE_COMPRESSION"
        span = max(EPS, float(hard_threshold) - float(threshold))
        multiplier = max(0.0, min(1.0, 1.0 - ((score - threshold) / span)))
    if score >= hard_threshold:
        action = "HARD_BLOCK"
        multiplier = 0.0
        hard_block = True
    applied = bool(mode == "suppress" and score > threshold)
    if mode != "suppress":
        multiplier = 1.0
        hard_block = False
        action = "LOG_ONLY" if score > threshold else "NONE"
    return {
        "enabled": True,
        "applied": bool(applied),
        "mode": str(mode),
        "source": "order_ood_score",
        "action": str(action),
        "ood_score": float(score),
        "ood_distance": float(score),
        "threshold": float(threshold),
        "hard_threshold": float(hard_threshold),
        "multiplier": float(multiplier),
        "hard_block": bool(hard_block),
        "range_violation_count": int(_safe_float(ood.get("range_violation_count"), 0.0)),
        "range_violation": bool(ood.get("range_violation")) or int(_safe_float(ood.get("range_violation_count"), 0.0)) > 0,
        "range_violations": list(ood.get("range_violations") or [])[:20] if isinstance(ood.get("range_violations"), list) else [],
    }
