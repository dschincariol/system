"""Deflated Sharpe utilities for multiple backtest trials."""

from __future__ import annotations

import math
from dataclasses import dataclass
from statistics import NormalDist
from typing import Iterable

import numpy as np

_NORMAL = NormalDist()
_EULER_GAMMA = 0.5772156649015329
_EPS = 1e-12


@dataclass(frozen=True)
class DeflatedSharpeResult:
    raw_sharpe: float
    deflated_sharpe: float
    expected_max_sharpe: float
    p_value: float
    n_trials: int
    probability: float = 0.0
    z_score: float = 0.0
    sharpe_std: float = 0.0

    def to_dict(self) -> dict:
        return {
            "raw_sharpe": float(self.raw_sharpe),
            "deflated_sharpe": float(self.deflated_sharpe),
            "expected_max_sharpe": float(self.expected_max_sharpe),
            "p_value": float(self.p_value),
            "n_trials": int(self.n_trials),
            "probability": float(self.probability),
            "z_score": float(self.z_score),
            "sharpe_std": float(self.sharpe_std),
        }


def _clean(values: Iterable[float]) -> np.ndarray:
    arr = np.asarray([] if values is None else list(values), dtype=float).reshape(-1)
    return arr[np.isfinite(arr)]


def expected_max_sharpe(trial_sharpes: Iterable[float], n_trials: int | None = None) -> float:
    """Expected maximum Sharpe under repeated trials."""
    values = _clean(trial_sharpes)
    trials = int(max(1, n_trials if n_trials is not None else int(values.size)))
    if trials <= 1 or values.size <= 1:
        return 0.0

    mean_sr = float(values.mean())
    std_sr = float(values.std(ddof=1))
    if std_sr <= _EPS:
        return mean_sr

    p1 = min(1.0 - _EPS, max(_EPS, 1.0 - (1.0 / float(trials))))
    p2 = min(1.0 - _EPS, max(_EPS, 1.0 - (1.0 / (float(trials) * math.e))))
    return float(mean_sr + std_sr * (((1.0 - _EULER_GAMMA) * _NORMAL.inv_cdf(p1)) + (_EULER_GAMMA * _NORMAL.inv_cdf(p2))))


def deflated_sharpe_ratio(
    trial_sharpes: Iterable[float],
    *,
    realized_sharpe: float | None = None,
    n_trials: int | None = None,
    n_observations: int | None = None,
    skew: float = 0.0,
    kurtosis: float = 3.0,
) -> DeflatedSharpeResult:
    """Compute Bailey-de Prado style deflated Sharpe diagnostics.

    ``deflated_sharpe`` is kept as an adjusted Sharpe value for compatibility
    with existing callers and collapses to the raw Sharpe when there is only one
    trial. ``probability`` is the DSR probability and ``p_value`` is its upper
    tail.
    """
    values = _clean(trial_sharpes)
    trials = int(max(1, n_trials if n_trials is not None else int(values.size)))
    if values.size <= 0:
        return DeflatedSharpeResult(0.0, 0.0, 0.0, 1.0, trials, probability=0.0)

    raw = float(realized_sharpe if realized_sharpe is not None else float(np.max(values)))
    if trials <= 1:
        probability = float(_NORMAL.cdf(raw))
        return DeflatedSharpeResult(
            raw,
            raw,
            0.0,
            float(1.0 - probability),
            1,
            probability=probability,
            z_score=raw,
            sharpe_std=0.0,
        )

    expected_max = float(expected_max_sharpe(values, n_trials=trials))
    deflated = float(raw - expected_max)
    if n_observations is not None and int(n_observations) > 1:
        obs = int(n_observations)
        variance_term = 1.0 - (float(skew or 0.0) * raw) + (((float(kurtosis or 3.0) - 1.0) / 4.0) * (raw**2))
        scale = math.sqrt(max(_EPS, variance_term) / float(max(1, obs - 1)))
    else:
        scale = float(values.std(ddof=1)) if values.size > 1 else 1.0
        scale = max(scale, 1.0 / math.sqrt(float(max(1, trials))))
    z_score = deflated / max(scale, _EPS)
    probability = float(_NORMAL.cdf(z_score))
    p_value = float(1.0 - probability)
    return DeflatedSharpeResult(
        raw_sharpe=raw,
        deflated_sharpe=deflated,
        expected_max_sharpe=expected_max,
        p_value=max(0.0, min(1.0, p_value)),
        n_trials=trials,
        probability=max(0.0, min(1.0, probability)),
        z_score=float(z_score),
        sharpe_std=float(scale),
    )


__all__ = [
    "DeflatedSharpeResult",
    "deflated_sharpe_ratio",
    "expected_max_sharpe",
]
