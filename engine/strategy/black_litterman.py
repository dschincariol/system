"""Numerically safe Black-Litterman helpers for expected-return blending.

This module is intentionally additive. It builds a prior equilibrium from a
covariance matrix, converts model outputs into absolute views, and produces a
posterior return vector that can be consumed by portfolio construction.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import math
import os
from typing import Any

import numpy as np

_EPS = 1e-8


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    if not math.isfinite(out):
        return float(default)
    return float(out)


def _clamp(value: float, lo: float, hi: float) -> float:
    return float(max(float(lo), min(float(hi), float(value))))


def _default_tau() -> float:
    return float(max(_EPS, _safe_float(os.environ.get("BLACK_LITTERMAN_TAU", "0.05"), 0.05)))


def _default_view_confidence() -> float:
    raw = _safe_float(os.environ.get("BLACK_LITTERMAN_VIEW_CONFIDENCE", "0.60"), 0.60)
    return float(_clamp(raw, 0.01, 0.99))


def _as_square_matrix(cov_matrix: Any) -> np.ndarray:
    raw = cov_matrix
    if isinstance(raw, Mapping):
        if "matrix" in raw:
            raw = raw.get("matrix")
        elif raw and all(isinstance(value, Mapping) for value in raw.values()):
            keys = [str(key) for key in raw.keys()]
            rows = []
            for key in keys:
                inner = raw.get(key) or {}
                rows.append([_safe_float((inner or {}).get(other), 0.0) for other in keys])
            raw = rows

    arr = np.asarray(raw, dtype=np.float64)
    if arr.ndim == 1:
        arr = np.diag(arr)
    if arr.ndim != 2 or arr.shape[0] != arr.shape[1]:
        return np.zeros((0, 0), dtype=np.float64)

    arr = np.nan_to_num(arr, nan=0.0, posinf=0.0, neginf=0.0)
    arr = 0.5 * (arr + arr.T)
    diag = np.maximum(np.diag(arr), _EPS)
    np.fill_diagonal(arr, diag)
    return arr


def _as_vector(values: Any, size: int) -> np.ndarray:
    arr = np.asarray(values, dtype=np.float64).reshape(-1)
    if arr.size >= size:
        out = arr[:size]
    else:
        out = np.zeros(size, dtype=np.float64)
        out[: arr.size] = arr
    return np.nan_to_num(out, nan=0.0, posinf=0.0, neginf=0.0)


def _extract_view_payload(payload: Any) -> dict[str, float] | None:
    default_confidence = _default_view_confidence()
    if isinstance(payload, (int, float)):
        prediction = _safe_float(payload, float("nan"))
        if not math.isfinite(prediction):
            return None
        return {
            "prediction": float(prediction),
            "confidence": float(default_confidence),
            "uncertainty": float(max(_EPS, 1.0 - default_confidence)),
        }

    if not isinstance(payload, Mapping):
        return None

    ensemble_output = payload.get("ensemble_output")
    ensemble_output = dict(ensemble_output or {}) if isinstance(ensemble_output, Mapping) else {}

    prediction = None
    for container in (payload, ensemble_output):
        for key in (
            "view_return",
            "adjusted_expected_ret_net",
            "expected_ret_net",
            "prediction",
            "blended_prediction",
            "expected_z",
            "predicted_z",
            "signal_z",
        ):
            if key not in container:
                continue
            value = _safe_float(container.get(key), float("nan"))
            if math.isfinite(value):
                prediction = float(value)
                break
        if prediction is not None:
            break

    if prediction is None and isinstance(payload.get("ensemble_members"), Sequence):
        members = []
        for member in list(payload.get("ensemble_members") or []):
            if not isinstance(member, Mapping):
                continue
            value = _safe_float(
                member.get("prediction", member.get("blended_prediction", member.get("expected_z"))),
                float("nan"),
            )
            if math.isfinite(value):
                members.append(float(value))
        if members:
            prediction = float(sum(members) / len(members))

    if prediction is None:
        return None

    confidence = None
    for container in (payload, ensemble_output):
        for key in ("view_confidence", "confidence", "probability", "blended_confidence"):
            if key not in container:
                continue
            value = _safe_float(container.get(key), float("nan"))
            if math.isfinite(value):
                confidence = float(_clamp(value, 0.01, 0.99))
                break
        if confidence is not None:
            break
    if confidence is None:
        confidence = float(default_confidence)

    uncertainty = None
    for container in (payload, ensemble_output):
        for key in ("view_uncertainty", "uncertainty"):
            if key not in container:
                continue
            value = _safe_float(container.get(key), float("nan"))
            if math.isfinite(value):
                uncertainty = float(max(_EPS, value))
                break
        if uncertainty is not None:
            break
    if uncertainty is None:
        uncertainty = float(max(_EPS, 1.0 - float(confidence)))

    return {
        "prediction": float(prediction),
        "confidence": float(confidence),
        "uncertainty": float(uncertainty),
    }


def compute_equilibrium_returns(cov_matrix: Any) -> np.ndarray:
    """Compute a stable equilibrium prior from covariance alone.

    When explicit market-cap weights are unavailable, we use inverse-volatility
    risk-parity weights as a neutral equilibrium anchor.
    """

    cov = _as_square_matrix(cov_matrix)
    if cov.size == 0:
        return np.zeros(0, dtype=np.float64)

    diag = np.maximum(np.diag(cov), _EPS)
    inv_vol = 1.0 / np.sqrt(diag)
    total = float(np.sum(inv_vol))
    if total <= _EPS:
        weights = np.full(cov.shape[0], 1.0 / float(cov.shape[0]), dtype=np.float64)
    else:
        weights = inv_vol / total

    equilibrium = cov @ weights
    return np.nan_to_num(equilibrium, nan=0.0, posinf=0.0, neginf=0.0)


def build_view_matrix(model_predictions: Any) -> dict[str, Any]:
    """Convert model outputs into absolute Black-Litterman views.

    Returns a dict with:
    - `assets`: full asset ordering
    - `view_assets`: assets with usable views
    - `matrix`: P
    - `vector`: Q
    - `confidence`: per-view confidence
    - `uncertainty`: per-view relative uncertainty
    """

    if isinstance(model_predictions, Mapping) and "matrix" in model_predictions and "vector" in model_predictions:
        matrix = np.asarray(model_predictions.get("matrix"), dtype=np.float64)
        vector = np.asarray(model_predictions.get("vector"), dtype=np.float64).reshape(-1)
        assets = [str(value) for value in list(model_predictions.get("assets") or [])]
        view_assets = [str(value) for value in list(model_predictions.get("view_assets") or assets[: vector.shape[0]])]
        confidence = np.asarray(model_predictions.get("confidence", []), dtype=np.float64).reshape(-1)
        uncertainty = np.asarray(model_predictions.get("uncertainty", []), dtype=np.float64).reshape(-1)
        return {
            "assets": assets,
            "view_assets": view_assets,
            "matrix": np.nan_to_num(matrix, nan=0.0, posinf=0.0, neginf=0.0),
            "vector": np.nan_to_num(vector, nan=0.0, posinf=0.0, neginf=0.0),
            "confidence": np.nan_to_num(confidence, nan=_default_view_confidence(), posinf=_default_view_confidence(), neginf=_default_view_confidence()),
            "uncertainty": np.nan_to_num(uncertainty, nan=max(_EPS, 1.0 - _default_view_confidence()), posinf=1.0, neginf=max(_EPS, 1.0 - _default_view_confidence())),
        }

    if isinstance(model_predictions, Mapping):
        assets = [str(key) for key in list(model_predictions.keys() or [])]
        rows = []
        vector = []
        confidence = []
        uncertainty = []
        view_assets = []
        for idx, asset in enumerate(assets):
            payload = _extract_view_payload(model_predictions.get(asset))
            if payload is None:
                continue
            row = np.zeros(len(assets), dtype=np.float64)
            row[idx] = 1.0
            rows.append(row)
            vector.append(float(payload["prediction"]))
            confidence.append(float(payload["confidence"]))
            uncertainty.append(float(payload["uncertainty"]))
            view_assets.append(str(asset))
        matrix = np.vstack(rows) if rows else np.zeros((0, len(assets)), dtype=np.float64)
        return {
            "assets": assets,
            "view_assets": view_assets,
            "matrix": matrix,
            "vector": np.asarray(vector, dtype=np.float64),
            "confidence": np.asarray(confidence, dtype=np.float64),
            "uncertainty": np.asarray(uncertainty, dtype=np.float64),
        }

    vector = np.asarray(([] if model_predictions is None else model_predictions), dtype=np.float64).reshape(-1)
    size = int(vector.shape[0])
    return {
        "assets": [str(idx) for idx in range(size)],
        "view_assets": [str(idx) for idx in range(size)],
        "matrix": np.eye(size, dtype=np.float64),
        "vector": np.nan_to_num(vector, nan=0.0, posinf=0.0, neginf=0.0),
        "confidence": np.full(size, _default_view_confidence(), dtype=np.float64),
        "uncertainty": np.full(size, max(_EPS, 1.0 - _default_view_confidence()), dtype=np.float64),
    }


def _coerce_uncertainty_matrix(
    uncertainty: Any,
    view_matrix: np.ndarray,
    tau_sigma: np.ndarray,
    view_count: int,
) -> np.ndarray:
    if view_count <= 0:
        return np.zeros((0, 0), dtype=np.float64)

    if uncertainty is None:
        base = np.full(view_count, max(_EPS, 1.0 - _default_view_confidence()), dtype=np.float64)
    else:
        arr = np.asarray(uncertainty, dtype=np.float64)
        if arr.ndim == 2 and arr.shape == (view_count, view_count):
            arr = np.nan_to_num(arr, nan=0.0, posinf=0.0, neginf=0.0)
            arr = 0.5 * (arr + arr.T)
            diag = np.maximum(np.diag(arr), _EPS)
            np.fill_diagonal(arr, diag)
            return arr

        arr = arr.reshape(-1)
        if arr.size == 0:
            base = np.full(view_count, max(_EPS, 1.0 - _default_view_confidence()), dtype=np.float64)
        elif arr.size == 1:
            base = np.full(view_count, max(_EPS, float(arr[0])), dtype=np.float64)
        else:
            base = np.full(view_count, max(_EPS, 1.0 - _default_view_confidence()), dtype=np.float64)
            base[: min(view_count, arr.size)] = [max(_EPS, float(v)) for v in arr[:view_count]]

    prior_view_var = np.diag(view_matrix @ tau_sigma @ view_matrix.T)
    prior_view_var = np.maximum(np.nan_to_num(prior_view_var, nan=_EPS, posinf=_EPS, neginf=_EPS), _EPS)
    omega_diag = np.maximum(prior_view_var * np.maximum(base, _EPS), _EPS)
    return np.diag(omega_diag.astype(np.float64))


def black_litterman_posterior(
    cov_matrix: Any,
    equilibrium_returns: Any,
    views: Any,
    uncertainty: Any,
) -> dict[str, Any]:
    """Blend equilibrium returns with model views using Black-Litterman."""

    cov = _as_square_matrix(cov_matrix)
    asset_count = int(cov.shape[0])
    if asset_count <= 0:
        empty = np.zeros(0, dtype=np.float64)
        return {
            "applied": False,
            "tau": float(_default_tau()),
            "assets": [],
            "view_assets": [],
            "posterior_returns": empty,
            "posterior_covariance": np.zeros((0, 0), dtype=np.float64),
            "equilibrium_returns": empty,
            "view_matrix": np.zeros((0, 0), dtype=np.float64),
            "view_returns": empty,
            "view_confidence": empty,
            "uncertainty": np.zeros((0, 0), dtype=np.float64),
            "fallback_reason": "invalid_covariance",
        }

    view_payload = build_view_matrix(views)
    view_matrix = np.asarray(view_payload.get("matrix"), dtype=np.float64)
    view_returns = np.asarray(view_payload.get("vector"), dtype=np.float64).reshape(-1)
    assets = [str(value) for value in list(view_payload.get("assets") or [])]
    view_assets = [str(value) for value in list(view_payload.get("view_assets") or [])]
    view_confidence = np.asarray(view_payload.get("confidence", []), dtype=np.float64).reshape(-1)

    if view_matrix.ndim != 2:
        view_matrix = np.zeros((0, asset_count), dtype=np.float64)
    if view_matrix.shape[1] < asset_count:
        padded = np.zeros((view_matrix.shape[0], asset_count), dtype=np.float64)
        padded[:, : view_matrix.shape[1]] = view_matrix
        view_matrix = padded
    elif view_matrix.shape[1] > asset_count:
        view_matrix = view_matrix[:, :asset_count]

    view_count = int(view_matrix.shape[0])
    prior = _as_vector(equilibrium_returns, asset_count)
    tau = float(_default_tau())

    if view_count <= 0 or view_returns.size <= 0:
        return {
            "applied": False,
            "tau": float(tau),
            "assets": assets,
            "view_assets": view_assets,
            "posterior_returns": prior,
            "posterior_covariance": cov,
            "equilibrium_returns": prior,
            "view_matrix": view_matrix,
            "view_returns": np.zeros(0, dtype=np.float64),
            "view_confidence": view_confidence,
            "uncertainty": np.zeros((0, 0), dtype=np.float64),
            "fallback_reason": "no_views",
        }

    tau_sigma = (tau * cov) + (np.eye(asset_count, dtype=np.float64) * _EPS)
    omega = _coerce_uncertainty_matrix(
        uncertainty if uncertainty is not None else view_payload.get("uncertainty"),
        view_matrix,
        tau_sigma,
        view_count=view_count,
    )

    middle = view_matrix @ tau_sigma @ view_matrix.T
    middle = middle + omega + (np.eye(view_count, dtype=np.float64) * _EPS)
    middle_inv = np.linalg.pinv(middle, hermitian=True)

    adjustment = tau_sigma @ view_matrix.T @ middle_inv @ (view_returns - (view_matrix @ prior))
    posterior_returns = np.nan_to_num(prior + adjustment, nan=0.0, posinf=0.0, neginf=0.0)

    posterior_covariance = cov + tau_sigma - (tau_sigma @ view_matrix.T @ middle_inv @ view_matrix @ tau_sigma)
    posterior_covariance = _as_square_matrix(posterior_covariance)

    return {
        "applied": True,
        "tau": float(tau),
        "assets": assets,
        "view_assets": view_assets,
        "posterior_returns": posterior_returns,
        "posterior_covariance": posterior_covariance,
        "equilibrium_returns": prior,
        "view_matrix": view_matrix,
        "view_returns": np.nan_to_num(view_returns, nan=0.0, posinf=0.0, neginf=0.0),
        "view_confidence": np.nan_to_num(view_confidence, nan=_default_view_confidence(), posinf=_default_view_confidence(), neginf=_default_view_confidence()),
        "uncertainty": omega,
    }
