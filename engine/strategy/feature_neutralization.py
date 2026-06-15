"""Feature neutralization utilities for prediction post-processing.

The neutralizer is a cross-sectional post-processor: it subtracts the linear
projection of same-timestamp predictions onto a configured set of risky
features. It never fetches features itself; callers must pass the persisted
feature snapshot already used for prediction.
"""

from __future__ import annotations

import math
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import numpy as np

DEFAULT_RISKY_FEATURE_IDS = [
    "tech.kama_slope",
    "price.momentum_5m",
    "price.momentum_1h",
    "price.momentum_1d",
    "tech.rv_20",
    "price.rv_20",
    "price.vol_std_20",
    "tech.har_rv_forecast_ratio",
]
DEFAULT_MIN_CROSS_SECTION = 8
DEFAULT_STRENGTH = 0.5
DEFAULT_RIDGE_LAMBDA = 1.0e-6
_EPS = 1.0e-12
_CORR_NORM_EPS = 1.0e-6


@dataclass(frozen=True)
class NeutralizationResult:
    symbols: list[str]
    raw_predictions: dict[str, float]
    neutralized_predictions: dict[str, float]
    applied: bool
    reason: str
    mode: str
    strength: float
    ridge_lambda: float
    feature_ids: list[str]
    usable_feature_ids: list[str]
    cross_section_n: int
    projection_norm: float
    exposure_before: dict[str, float]
    exposure_after: dict[str, float]

    def diagnostics(self) -> dict[str, Any]:
        return {
            "applied": bool(self.applied),
            "reason": str(self.reason),
            "mode": str(self.mode),
            "strength": float(self.strength),
            "ridge_lambda": float(self.ridge_lambda),
            "feature_ids": list(self.feature_ids),
            "usable_feature_ids": list(self.usable_feature_ids),
            "cross_section_n": int(self.cross_section_n),
            "projection_norm": float(self.projection_norm),
            "exposure_before": dict(self.exposure_before),
            "exposure_after": dict(self.exposure_after),
        }


def neutralize_mode(value: str | None = None) -> str:
    raw = str(os.environ.get("NEUTRALIZE_MODE", "off") if value is None else value).strip().lower()
    return raw if raw in {"off", "metrics_only", "serve"} else "off"


def neutralize_strength(value: Any = None) -> float:
    raw = os.environ.get("NEUTRALIZE_STRENGTH", str(DEFAULT_STRENGTH)) if value is None else value
    try:
        parsed = float(raw)
    except Exception:
        parsed = float(DEFAULT_STRENGTH)
    if not math.isfinite(parsed):
        parsed = float(DEFAULT_STRENGTH)
    return float(max(0.0, min(1.0, parsed)))


def neutralize_ridge_lambda(value: Any = None) -> float:
    raw = os.environ.get("NEUTRALIZE_RIDGE_LAMBDA", str(DEFAULT_RIDGE_LAMBDA)) if value is None else value
    try:
        parsed = float(raw)
    except Exception:
        parsed = float(DEFAULT_RIDGE_LAMBDA)
    if not math.isfinite(parsed):
        parsed = float(DEFAULT_RIDGE_LAMBDA)
    return float(max(0.0, parsed))


def neutralize_feature_ids(value: Any = None) -> list[str]:
    raw = os.environ.get("NEUTRALIZE_FEATURE_IDS", "") if value is None else value
    if isinstance(raw, str):
        ids = [part.strip() for part in raw.split(",") if part.strip()]
    else:
        try:
            ids = [str(part).strip() for part in list(raw or []) if str(part).strip()]
        except Exception:
            ids = []
    return list(dict.fromkeys(ids or list(DEFAULT_RISKY_FEATURE_IDS)))


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except Exception:
        return float(default)
    return float(parsed) if math.isfinite(parsed) else float(default)


def _corr(left: Sequence[float], right: Sequence[float]) -> float:
    x = np.asarray([_safe_float(value, np.nan) for value in left], dtype=np.float64)
    y = np.asarray([_safe_float(value, np.nan) for value in right], dtype=np.float64)
    mask = np.isfinite(x) & np.isfinite(y)
    if int(mask.sum()) < 2:
        return 0.0
    x = x[mask]
    y = y[mask]
    x = x - float(np.mean(x))
    y = y - float(np.mean(y))
    norm_x = float(np.linalg.norm(x))
    norm_y = float(np.linalg.norm(y))
    if norm_x <= _CORR_NORM_EPS or norm_y <= _CORR_NORM_EPS:
        return 0.0
    denom = float(norm_x * norm_y)
    if denom <= _EPS:
        return 0.0
    return float(max(-1.0, min(1.0, float(np.dot(x, y) / denom))))


def _standardize_matrix(raw: np.ndarray, feature_ids: Sequence[str]) -> tuple[np.ndarray, list[str]]:
    if raw.ndim != 2 or raw.shape[1] <= 0:
        return np.zeros((raw.shape[0] if raw.ndim == 2 else 0, 0), dtype=np.float64), []
    arr = np.asarray(raw, dtype=np.float64)
    usable_cols: list[int] = []
    usable_ids: list[str] = []
    columns: list[np.ndarray] = []
    for col_idx in range(arr.shape[1]):
        col = np.asarray(arr[:, col_idx], dtype=np.float64)
        finite = np.isfinite(col)
        if int(finite.sum()) < 2:
            continue
        mean = float(np.mean(col[finite]))
        filled = np.where(finite, col, mean)
        std = float(np.std(filled))
        if std <= _EPS:
            continue
        columns.append((filled - mean) / std)
        usable_cols.append(int(col_idx))
        usable_ids.append(str(list(feature_ids)[col_idx]))
    del usable_cols
    if not columns:
        return np.zeros((arr.shape[0], 0), dtype=np.float64), []
    return np.column_stack(columns).astype(np.float64, copy=False), usable_ids


def _projection(p: np.ndarray, F: np.ndarray, ridge_lambda: float) -> np.ndarray:
    if F.size <= 0 or F.shape[1] <= 0:
        return np.zeros_like(p, dtype=np.float64)
    gram = F.T @ F
    if float(ridge_lambda) > 0.0:
        gram = gram + (float(ridge_lambda) * np.eye(F.shape[1], dtype=np.float64))
    rhs = F.T @ p
    try:
        beta = np.linalg.solve(gram, rhs)
    except np.linalg.LinAlgError:
        beta = np.linalg.pinv(gram) @ rhs
    return np.asarray(F @ beta, dtype=np.float64).reshape(-1)


def neutralize_predictions(
    predictions: Mapping[str, Any],
    feature_snapshots: Mapping[str, Mapping[str, Any]],
    *,
    feature_ids: Sequence[str] | None = None,
    strength: float | None = None,
    ridge_lambda: float | None = None,
    mode: str | None = None,
    min_symbols: int = DEFAULT_MIN_CROSS_SECTION,
) -> NeutralizationResult:
    ids = neutralize_feature_ids(feature_ids)
    key_map = {str(key).upper().strip(): key for key in predictions.keys() if str(key or "").strip()}
    syms = list(key_map.keys())
    raw = {sym: _safe_float(predictions.get(key_map[sym]), 0.0) for sym in syms}
    mode_value = neutralize_mode(mode)
    strength_value = neutralize_strength(strength)
    ridge_value = neutralize_ridge_lambda(ridge_lambda)
    n = int(len(syms))

    def _identity(reason: str) -> NeutralizationResult:
        return NeutralizationResult(
            symbols=list(syms),
            raw_predictions=dict(raw),
            neutralized_predictions=dict(raw),
            applied=False,
            reason=str(reason),
            mode=str(mode_value),
            strength=float(strength_value),
            ridge_lambda=float(ridge_value),
            feature_ids=list(ids),
            usable_feature_ids=[],
            cross_section_n=int(n),
            projection_norm=0.0,
            exposure_before={},
            exposure_after={},
        )

    if mode_value == "off":
        return _identity("mode_off")
    if n < int(min_symbols):
        return _identity("cross_section_too_small")
    if not ids:
        return _identity("no_feature_ids")

    feature_rows: list[list[float]] = []
    lookup = {str(k).upper().strip(): dict(v or {}) for k, v in dict(feature_snapshots or {}).items()}
    for sym in syms:
        snap = dict(lookup.get(sym) or {})
        feature_rows.append([_safe_float(snap.get(fid), np.nan) for fid in ids])
    F_raw = np.asarray(feature_rows, dtype=np.float64)
    F, usable_ids = _standardize_matrix(F_raw, ids)
    if F.shape[1] <= 0:
        return _identity("no_usable_feature_columns")

    p = np.asarray([raw[sym] for sym in syms], dtype=np.float64)
    proj = _projection(p, F, ridge_value)
    neutral = p - (float(strength_value) * proj)
    neutral_map = {sym: float(neutral[idx]) for idx, sym in enumerate(syms)}
    exposure_before = {fid: _corr(p, F[:, idx]) for idx, fid in enumerate(usable_ids)}
    exposure_after = {fid: _corr(neutral, F[:, idx]) for idx, fid in enumerate(usable_ids)}
    return NeutralizationResult(
        symbols=list(syms),
        raw_predictions=dict(raw),
        neutralized_predictions=neutral_map,
        applied=True,
        reason="applied",
        mode=str(mode_value),
        strength=float(strength_value),
        ridge_lambda=float(ridge_value),
        feature_ids=list(ids),
        usable_feature_ids=list(usable_ids),
        cross_section_n=int(n),
        projection_norm=float(np.linalg.norm(proj)),
        exposure_before=exposure_before,
        exposure_after=exposure_after,
    )


def feature_neutral_ic(
    predictions: Sequence[float],
    realized_returns: Sequence[float],
    feature_rows: Sequence[Mapping[str, Any] | Sequence[Any]],
    *,
    feature_ids: Sequence[str] | None = None,
    ridge_lambda: float | None = None,
    min_symbols: int = DEFAULT_MIN_CROSS_SECTION,
) -> dict[str, Any]:
    ids = neutralize_feature_ids(feature_ids)
    prediction_values = list(predictions or [])
    realized_values = list(realized_returns or [])
    feature_values = list(feature_rows or [])
    n = min(len(prediction_values), len(realized_values), len(feature_values))
    pred = [_safe_float(value, 0.0) for value in prediction_values[:n]]
    realized = [_safe_float(value, 0.0) for value in realized_values[:n]]
    rows = feature_values[:n]
    snapshots: dict[str, dict[str, Any]] = {}
    pred_map: dict[str, float] = {}
    for idx, row in enumerate(rows):
        sym = f"row_{idx:06d}"
        pred_map[sym] = float(pred[idx])
        if isinstance(row, Mapping):
            snapshots[sym] = {fid: row.get(fid) for fid in ids}
        else:
            values = list(row or [])
            snapshots[sym] = {fid: (values[col] if col < len(values) else 0.0) for col, fid in enumerate(ids)}
    result = neutralize_predictions(
        pred_map,
        snapshots,
        feature_ids=ids,
        strength=1.0,
        ridge_lambda=ridge_lambda,
        mode="metrics_only",
        min_symbols=int(min_symbols),
    )
    neutral = [float(result.neutralized_predictions.get(f"ROW_{idx:06d}", pred[idx])) for idx in range(n)]
    raw_ic = _corr(pred, realized)
    fnc = _corr(neutral, realized)
    return {
        "applied": bool(result.applied),
        "reason": str(result.reason),
        "raw_ic": float(raw_ic),
        "fnc": float(fnc),
        "raw_minus_fnc": float(raw_ic - fnc),
        "n": int(n),
        "feature_ids": list(ids),
        "usable_feature_ids": list(result.usable_feature_ids),
        "projection_norm": float(result.projection_norm),
        "exposure_before": dict(result.exposure_before),
        "exposure_after": dict(result.exposure_after),
    }


def extract_feature_rows(payload: Any, feature_ids: Sequence[str] | None = None) -> list[dict[str, float]]:
    ids = neutralize_feature_ids(feature_ids)
    if payload is None:
        return []
    if isinstance(payload, Mapping):
        for key in ("neutralization_features", "feature_snapshots", "feature_values", "features_matrix", "risky_features"):
            if key in payload:
                return extract_feature_rows(payload.get(key), ids)
        if all(fid in payload for fid in ids):
            return [{fid: _safe_float(payload.get(fid), 0.0) for fid in ids}]
        return []
    out: list[dict[str, float]] = []
    try:
        iterable = list(payload)
    except Exception:
        return []
    for row in iterable:
        if isinstance(row, Mapping):
            nested = row.get("features") if isinstance(row.get("features"), Mapping) else row
            out.append({fid: _safe_float(dict(nested or {}).get(fid), 0.0) for fid in ids})
        else:
            values = list(row or [])
            out.append({fid: _safe_float(values[idx] if idx < len(values) else 0.0, 0.0) for idx, fid in enumerate(ids)})
    return out
