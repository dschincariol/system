from __future__ import annotations

import numpy as np
from sklearn.linear_model import Ridge

from engine.strategy.ensemble.ridge_meta import RidgeStackEnsemble


def _coef_vector(model: RidgeStackEnsemble) -> np.ndarray:
    return np.asarray([model.weights_[family] for family in model.families_], dtype=float)


def test_unconstrained_ridge_is_deterministic_and_matches_sklearn():
    rng = np.random.default_rng(42)
    X = rng.normal(size=(96, 4))
    beta = np.asarray([0.35, -0.2, 0.6, -0.1], dtype=float)
    y = 0.25 + X @ beta + rng.normal(scale=0.02, size=X.shape[0])
    rows = [
        {
            "f0": float(X[idx, 0]),
            "f1": float(X[idx, 1]),
            "f2": float(X[idx, 2]),
            "f3": float(X[idx, 3]),
            "target": float(y[idx]),
        }
        for idx in range(X.shape[0])
    ]

    alpha = 0.41
    first = RidgeStackEnsemble(alpha=alpha, nonneg=False).fit(rows)
    second = RidgeStackEnsemble(alpha=alpha, nonneg=False).fit(rows)
    expected = Ridge(alpha=alpha, fit_intercept=True).fit(X, y)
    heldout = [
        {"f0": float(row[0]), "f1": float(row[1]), "f2": float(row[2]), "f3": float(row[3])}
        for row in X[:17]
    ]

    assert np.array_equal(_coef_vector(first), _coef_vector(second))
    assert first.intercept_ == second.intercept_
    assert first.to_dict() == second.to_dict()
    np.testing.assert_allclose(_coef_vector(first), expected.coef_, atol=1e-9, rtol=0.0)
    assert abs(first.intercept_ - float(expected.intercept_)) < 1e-9
    assert np.array_equal(first.predict(heldout), second.predict(heldout))
    np.testing.assert_allclose(first.predict(heldout), expected.predict(X[:17]), atol=1e-9, rtol=0.0)
