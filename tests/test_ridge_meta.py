import numpy as np
from sklearn.linear_model import Ridge
from sklearn.metrics import r2_score

from engine.strategy.ensemble.ridge_meta import RidgeStackEnsemble


def test_nonnegative_fit_honors_weight_constraint():
    rng = np.random.default_rng(7)
    rows = []
    for ts in range(120):
        p1 = float(rng.normal())
        p2 = float(rng.normal())
        target = 0.8 * p1 - 0.4 * p2 + float(rng.normal(scale=0.05))
        rows.extend(
            [
                {"symbol": "AAPL", "horizon": 3600, "family": "f1", "ts": ts, "prediction": p1, "target": target},
                {"symbol": "AAPL", "horizon": 3600, "family": "f2", "ts": ts, "prediction": p2, "target": target},
            ]
        )

    model = RidgeStackEnsemble(alpha=0.1, nonneg=True).fit(rows)

    assert model.n_train_obs_ == 120
    assert all(weight >= -1e-12 for weight in model.weights_.values())


def test_unconstrained_matches_sklearn_ridge_exactly():
    rng = np.random.default_rng(11)
    X = rng.normal(size=(80, 3))
    beta = np.asarray([0.4, -0.2, 0.7])
    y = 0.15 + X @ beta + rng.normal(scale=0.03, size=80)
    rows = [
        {"f0": float(X[i, 0]), "f1": float(X[i, 1]), "f2": float(X[i, 2]), "target": float(y[i])}
        for i in range(X.shape[0])
    ]

    actual = RidgeStackEnsemble(alpha=0.73, nonneg=False).fit(rows)
    expected = Ridge(alpha=0.73, fit_intercept=True).fit(X, y)

    np.testing.assert_allclose(
        [actual.weights_["f0"], actual.weights_["f1"], actual.weights_["f2"]],
        expected.coef_,
        atol=1e-9,
        rtol=0,
    )
    assert abs(actual.intercept_ - float(expected.intercept_)) < 1e-9


def test_uncorrelated_noisy_estimators_improve_r2_when_stacked():
    rng = np.random.default_rng(123)
    n_train = 2500
    n_test = 1200
    y = rng.normal(size=n_train + n_test)
    p1 = y + rng.normal(scale=3.0, size=n_train + n_test)
    p2 = y + rng.normal(scale=3.0, size=n_train + n_test)
    rows = []
    for ts in range(n_train):
        rows.extend(
            [
                {"symbol": "AAPL", "horizon": 3600, "family": "f1", "ts": ts, "prediction": float(p1[ts]), "target": float(y[ts])},
                {"symbol": "AAPL", "horizon": 3600, "family": "f2", "ts": ts, "prediction": float(p2[ts]), "target": float(y[ts])},
            ]
        )

    model = RidgeStackEnsemble(alpha=0.01, nonneg=True).fit(rows)
    heldout = [
        {"f1": float(p1[i]), "f2": float(p2[i])}
        for i in range(n_train, n_train + n_test)
    ]
    blended = model.predict(heldout)
    y_test = y[n_train:]

    r2_blend = r2_score(y_test, blended)
    r2_individual = max(r2_score(y_test, p1[n_train:]), r2_score(y_test, p2[n_train:]))
    assert r2_blend > r2_individual + 0.03
