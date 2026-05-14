"""Multivariate Granger causality diagnostics with HAC covariance."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

import numpy as np
from scipy.stats import f as f_dist


@dataclass(frozen=True)
class GrangerResult:
    """Result of a feature -> target Granger causality test."""

    p_value: float
    lag: int
    f_stat: float
    hac_lag: int
    n_obs: int
    bic: float

    @property
    def F(self) -> float:
        return float(self.f_stat)


def _series(data: Any, name: str) -> np.ndarray:
    try:
        value = data[name]
    except Exception as exc:
        raise KeyError(f"missing series {name!r}") from exc
    if hasattr(value, "to_numpy"):
        arr = value.to_numpy(dtype=float)
    else:
        arr = np.asarray(value, dtype=float)
    if arr.ndim != 1:
        arr = np.ravel(arr)
    return np.asarray(arr, dtype=float)


def _matrix(
    data: Any,
    *,
    cause: str,
    effect: str,
    controls: Sequence[str] | None,
) -> tuple[np.ndarray, list[str]]:
    names: list[str] = [str(effect), str(cause)]
    for control in controls or ():
        control_name = str(control)
        if control_name not in names:
            names.append(control_name)
    columns = [_series(data, name) for name in names]
    min_len = min(len(col) for col in columns)
    if min_len <= 0:
        raise ValueError("Granger input series are empty")
    values = np.column_stack([col[-min_len:] for col in columns])
    mask = np.isfinite(values).all(axis=1)
    values = values[mask]
    if values.shape[0] <= 2:
        raise ValueError("Granger test requires more observations")
    return values.astype(float, copy=False), names


def _lagged_design(values: np.ndarray, lag: int) -> tuple[np.ndarray, np.ndarray]:
    n_total, n_vars = values.shape
    n_rows = n_total - int(lag)
    if n_rows <= 0:
        raise ValueError("lag exceeds available observations")
    cols = [np.ones(n_rows, dtype=float)]
    for step in range(1, int(lag) + 1):
        cols.append(values[int(lag) - step : n_total - step, :])
    return np.column_stack(cols), values[int(lag) :, :]


def _logdet_psd(matrix: np.ndarray) -> float:
    mat = np.asarray(matrix, dtype=float)
    jitter = 1e-12
    for _ in range(8):
        sign, logdet = np.linalg.slogdet(mat + jitter * np.eye(mat.shape[0], dtype=float))
        if sign > 0 and math.isfinite(float(logdet)):
            return float(logdet)
        jitter *= 10.0
    eigvals = np.linalg.eigvalsh(mat)
    eigvals = np.maximum(eigvals, 1e-12)
    return float(np.sum(np.log(eigvals)))


def _var_bic(values: np.ndarray, lag: int) -> tuple[float, np.ndarray, np.ndarray]:
    x, y = _lagged_design(values, lag)
    n_rows, n_cols = x.shape
    n_vars = y.shape[1]
    if n_rows <= n_cols + 1:
        raise ValueError("not enough observations for lag")
    beta = np.linalg.pinv(x) @ y
    resid = y - x @ beta
    sigma = (resid.T @ resid) / max(1, n_rows)
    num_params = n_vars * n_cols
    bic = _logdet_psd(sigma) + math.log(float(n_rows)) * float(num_params) / float(n_rows)
    return float(bic), x, y


def _hac_covariance(x: np.ndarray, resid: np.ndarray, nw_lag: int) -> np.ndarray:
    xtx_inv = np.linalg.pinv(x.T @ x)
    scores = x * resid.reshape(-1, 1)
    meat = scores.T @ scores
    max_lag = max(0, min(int(nw_lag), scores.shape[0] - 1))
    for lag in range(1, max_lag + 1):
        weight = 1.0 - float(lag) / float(max_lag + 1)
        gamma = scores[lag:].T @ scores[:-lag]
        meat += weight * (gamma + gamma.T)
    return xtx_inv @ meat @ xtx_inv


def _newey_west_lag(n_obs: int) -> int:
    if n_obs <= 0:
        return 0
    return int(math.floor(4.0 * (float(n_obs) / 100.0) ** (2.0 / 9.0)))


def granger_causality(
    data: Mapping[str, Sequence[float]] | Any,
    *,
    cause: str,
    effect: str,
    controls: Sequence[str] | None = None,
    max_lag: int = 10,
) -> GrangerResult:
    """Test whether ``cause`` Granger-causes ``effect`` in a VAR.

    Lag length is selected by BIC over ``1..max_lag``. The reported statistic is
    the HAC-robust Wald statistic divided by the number of restrictions and
    evaluated against an F distribution.
    """

    if str(cause) == str(effect):
        raise ValueError("cause and effect must be different series")
    values, names = _matrix(data, cause=str(cause), effect=str(effect), controls=controls)
    n_total = values.shape[0]
    p_max = max(1, min(int(max_lag), n_total // 4, n_total - 3))
    candidates: list[tuple[float, int, np.ndarray, np.ndarray]] = []
    for lag in range(1, p_max + 1):
        try:
            bic, x, y = _var_bic(values, lag)
        except ValueError:
            continue
        candidates.append((float(bic), int(lag), x, y))
    if not candidates:
        raise ValueError("not enough observations for Granger lag selection")

    bic, lag, x, y_all = min(candidates, key=lambda item: (item[0], item[1]))
    y = y_all[:, 0]
    coef = np.linalg.pinv(x) @ y
    resid = y - x @ coef
    n_rows, n_cols = x.shape
    df_resid = max(1, n_rows - n_cols)
    hac_lag = _newey_west_lag(n_rows)
    cov = _hac_covariance(x, resid, hac_lag)

    cause_idx = names.index(str(cause))
    n_vars = values.shape[1]
    restricted_cols = [1 + (step - 1) * n_vars + cause_idx for step in range(1, lag + 1)]
    restricted = coef[restricted_cols]
    restricted_cov = cov[np.ix_(restricted_cols, restricted_cols)]
    wald = float(restricted.T @ np.linalg.pinv(restricted_cov) @ restricted)
    q = max(1, len(restricted_cols))
    f_stat = max(0.0, wald / float(q))
    p_value = float(f_dist.sf(f_stat, q, df_resid))
    if not math.isfinite(p_value):
        p_value = 1.0
    return GrangerResult(
        p_value=max(0.0, min(1.0, p_value)),
        lag=int(lag),
        f_stat=float(f_stat),
        hac_lag=int(hac_lag),
        n_obs=int(n_rows),
        bic=float(bic),
    )
