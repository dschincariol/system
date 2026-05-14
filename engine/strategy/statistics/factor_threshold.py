"""Harvey-Liu-Zhu factor threshold with Newey-West HAC t-statistics.

New candidate factors must clear a conservative out-of-sample t-statistic
hurdle before entering the live feature registry. References: Newey, W. and
West, K. (1987), "A Simple, Positive Semi-definite, Heteroskedasticity and
Autocorrelation Consistent Covariance Matrix"; Harvey, C., Liu, Y. and Zhu, H.
(2016), "... and the Cross-Section of Expected Returns", Review of Financial
Studies.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from statistics import NormalDist
from typing import Iterable

import numpy as np

_NORMAL = NormalDist()
_EPS = 1e-15


@dataclass(frozen=True)
class FactorThresholdResult:
    """Structured result for a single candidate factor threshold test."""

    feature_id: str
    t_stat: float
    p_value: float
    threshold: float
    passed: bool
    n_obs: int
    lags: int
    beta: float
    standard_error: float

    def to_dict(self) -> dict:
        return {
            "feature_id": str(self.feature_id),
            "t_stat": float(self.t_stat),
            "p_value": float(self.p_value),
            "threshold": float(self.threshold),
            "passed": bool(self.passed),
            "n_obs": int(self.n_obs),
            "lags": int(self.lags),
            "beta": float(self.beta),
            "standard_error": float(self.standard_error),
        }


def _as_vector(values: Iterable[float], *, name: str) -> np.ndarray:
    arr = np.asarray(list([] if values is None else values), dtype=float)
    if arr.ndim != 1:
        arr = arr.reshape(-1)
    arr = arr[np.isfinite(arr)]
    if arr.size == 0:
        raise ValueError(f"{name}_must_have_at_least_one_finite_value")
    return arr.astype(float)


def _default_lags(n_obs: int) -> int:
    if n_obs <= 1:
        return 0
    return int(math.floor(4.0 * (float(n_obs) / 100.0) ** (2.0 / 9.0)))


def _two_sided_normal_p_value(t_stat: float) -> float:
    if not math.isfinite(float(t_stat)):
        return 0.0
    tail = 1.0 - float(_NORMAL.cdf(abs(float(t_stat))))
    return float(max(0.0, min(1.0, 2.0 * tail)))


def newey_west_ols(
    y: Iterable[float],
    x: Iterable[float] | None = None,
    *,
    lags: int | None = None,
    use_correction: bool = True,
    random_state: int = 42,
) -> dict:
    """Fit OLS and return Newey-West HAC covariance for intercept or slope."""

    _ = int(random_state)
    if x is None:
        y_arr = _as_vector(y, name="y")
        x_mat = np.ones((int(y_arr.size), 1), dtype=float)
        target_index = 0
    else:
        y_arr = np.asarray(list([] if y is None else y), dtype=float)
        x_arr = np.asarray(list([] if x is None else x), dtype=float)
        if y_arr.ndim != 1:
            y_arr = y_arr.reshape(-1)
        if x_arr.ndim != 1:
            x_arr = x_arr.reshape(-1)
        n = min(int(y_arr.size), int(x_arr.size))
        if n <= 1:
            raise ValueError("x_and_y_need_at_least_two_aligned_observations")
        y_arr = y_arr[:n]
        x_arr = x_arr[:n]
        finite = np.isfinite(y_arr) & np.isfinite(x_arr)
        y_arr = y_arr[finite]
        x_arr = x_arr[finite]
        if y_arr.size <= 1:
            raise ValueError("x_and_y_need_at_least_two_aligned_finite_observations")
        x_mat = np.column_stack([np.ones(int(y_arr.size), dtype=float), x_arr.astype(float)])
        target_index = 1

    n_obs = int(y_arr.size)
    if n_obs <= x_mat.shape[1]:
        raise ValueError("not_enough_observations_for_hac_ols")
    nlags = int(_default_lags(n_obs) if lags is None else max(0, int(lags)))
    nlags = min(nlags, max(0, n_obs - 1))

    xtx_inv = np.linalg.pinv(x_mat.T @ x_mat)
    beta = xtx_inv @ x_mat.T @ y_arr
    resid = y_arr - (x_mat @ beta)
    xu = x_mat * resid[:, None]

    scale = xu.T @ xu
    for lag in range(1, nlags + 1):
        weight = 1.0 - (float(lag) / float(nlags + 1))
        gamma = xu[lag:].T @ xu[:-lag]
        scale = scale + weight * (gamma + gamma.T)

    cov = xtx_inv @ scale @ xtx_inv
    if bool(use_correction):
        denom = max(1, n_obs - int(x_mat.shape[1]))
        cov = cov * (float(n_obs) / float(denom))

    return {
        "beta": beta.astype(float),
        "covariance": cov.astype(float),
        "residuals": resid.astype(float),
        "n_obs": int(n_obs),
        "lags": int(nlags),
        "target_index": int(target_index),
    }


def newey_west_t_statistic(
    y: Iterable[float],
    x: Iterable[float] | None = None,
    *,
    lags: int | None = None,
    parameter_index: int | None = None,
    use_correction: bool = True,
    random_state: int = 42,
) -> float:
    """Return the HAC t-statistic for a mean return or OLS slope."""

    fit = newey_west_ols(y, x, lags=lags, use_correction=use_correction, random_state=random_state)
    idx = int(fit["target_index"] if parameter_index is None else parameter_index)
    beta = np.asarray(fit["beta"], dtype=float)
    cov = np.asarray(fit["covariance"], dtype=float)
    variance = float(cov[idx, idx])
    if variance <= _EPS:
        value = float(beta[idx])
        if value > 0.0:
            return float("inf")
        if value < 0.0:
            return float("-inf")
        return 0.0
    return float(beta[idx] / math.sqrt(variance))


def harvey_liu_zhu_threshold_result(
    y: Iterable[float] | None = None,
    x: Iterable[float] | None = None,
    *,
    feature_id: str = "",
    t_stat: float | None = None,
    n_obs: int | None = None,
    lags: int | None = None,
    threshold: float = 3.0,
    use_correction: bool = True,
    random_state: int = 42,
) -> FactorThresholdResult:
    """Evaluate the Harvey-Liu-Zhu `|t| > 3.0` factor hurdle."""

    beta_value = 0.0
    stderr = 0.0
    resolved_lags = int(0 if lags is None else max(0, int(lags)))
    resolved_n = int(max(0, int(n_obs or 0)))
    if t_stat is None:
        if y is None:
            raise ValueError("y_or_t_stat_required")
        fit = newey_west_ols(y, x, lags=lags, use_correction=use_correction, random_state=random_state)
        idx = int(fit["target_index"])
        beta = np.asarray(fit["beta"], dtype=float)
        cov = np.asarray(fit["covariance"], dtype=float)
        variance = float(cov[idx, idx])
        beta_value = float(beta[idx])
        stderr = float(math.sqrt(max(0.0, variance)))
        resolved_lags = int(fit["lags"])
        resolved_n = int(fit["n_obs"])
        if stderr <= _EPS:
            t_value = float("inf") if beta_value > 0.0 else (float("-inf") if beta_value < 0.0 else 0.0)
        else:
            t_value = float(beta_value / stderr)
    else:
        t_value = float(t_stat)
        beta_value = float(t_value)
        stderr = 1.0

    hurdle = float(abs(float(threshold)))
    p_value = _two_sided_normal_p_value(t_value)
    return FactorThresholdResult(
        feature_id=str(feature_id or ""),
        t_stat=float(t_value),
        p_value=float(p_value),
        threshold=float(hurdle),
        passed=bool(abs(float(t_value)) > hurdle),
        n_obs=int(resolved_n),
        lags=int(resolved_lags),
        beta=float(beta_value),
        standard_error=float(stderr),
    )


__all__ = [
    "FactorThresholdResult",
    "harvey_liu_zhu_threshold_result",
    "newey_west_ols",
    "newey_west_t_statistic",
]
