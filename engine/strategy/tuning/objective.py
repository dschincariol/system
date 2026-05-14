"""Objective protocol and reusable objective builders for tuning."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Protocol

import numpy as np

from engine.strategy.tuning.catalog import suggest_params


class ObjectiveFactory(Protocol):
    def __call__(self, symbol: str, train: Any, valid: Any) -> Callable[[Any], float]:
        ...


@dataclass(frozen=True)
class ArrayRegressionData:
    x_train: np.ndarray
    y_train: np.ndarray
    x_valid: np.ndarray
    y_valid: np.ndarray


def negative_rmse(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    truth = np.asarray(y_true, dtype=float).reshape(-1)
    pred = np.asarray(y_pred, dtype=float).reshape(-1)
    if truth.size == 0 or pred.size != truth.size:
        return float("-inf")
    return -float(np.sqrt(np.mean((truth - pred) ** 2)))


def sharpe_like_score(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    truth = np.asarray(y_true, dtype=float).reshape(-1)
    pred = np.asarray(y_pred, dtype=float).reshape(-1)
    pnl = np.sign(pred) * truth
    if pnl.size < 2:
        return float("-inf")
    denom = float(np.std(pnl, ddof=1))
    if denom <= 1e-12:
        return 0.0
    return float(np.mean(pnl) / denom * np.sqrt(252.0))


def build_sklearn_regression_objective(
    *,
    model_family: str,
    data: ArrayRegressionData,
    estimator_factory: Callable[[dict[str, Any]], Any],
    scorer: Callable[[np.ndarray, np.ndarray], float] = sharpe_like_score,
) -> Callable[[Any], float]:
    def objective(trial) -> float:
        params = suggest_params(trial, model_family)
        model = estimator_factory(params)
        model.fit(data.x_train, data.y_train)
        pred = model.predict(data.x_valid)
        return float(scorer(data.y_valid, pred))

    return objective


def build_quadratic_smoke_objective(model_family: str) -> Callable[[Any], float]:
    """Deterministic objective used by tests and dry-run tuning smoke jobs."""

    defaults = suggest_params

    def objective(trial) -> float:
        params = defaults(trial, model_family)
        score = 0.0
        for idx, value in enumerate(params.values(), start=1):
            if isinstance(value, (int, float)):
                score -= (float(value) / float(idx + 1)) ** 2 * 1e-6
        return float(score)

    return objective
