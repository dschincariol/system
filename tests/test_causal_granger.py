from __future__ import annotations

import pytest

from engine.causal.granger import granger_causality


def _white_dgp(seed: int, *, n: int = 500, causal: bool) -> dict[str, list[float]]:
    import numpy as np

    rng = np.random.default_rng(seed)
    x = rng.normal(size=n)
    y = rng.normal(size=n)
    if causal:
        for idx in range(1, n):
            y[idx] += 0.9 * x[idx - 1]
    return {"x": x.tolist(), "y": y.tolist()}


def _causal_ar_dgp(seed: int, *, n: int = 80) -> dict[str, list[float]]:
    import numpy as np

    rng = np.random.default_rng(seed)
    x = np.zeros(n)
    y = np.zeros(n)
    ex = rng.normal(size=n)
    ey = rng.normal(size=n)
    for idx in range(1, n):
        x[idx] = 0.45 * x[idx - 1] + ex[idx]
        y[idx] = 0.35 * y[idx - 1] + 0.75 * x[idx - 1] + ey[idx]
    return {"x": x.tolist(), "y": y.tolist()}


def test_granger_deterministic_hand_checked_example() -> None:
    data = _causal_ar_dgp(7)

    first = granger_causality(data, cause="x", effect="y", max_lag=4)
    second = granger_causality(data, cause="x", effect="y", max_lag=4)

    assert second == first
    assert first.lag == 1
    assert first.hac_lag == 3
    assert first.n_obs == 79
    assert first.f_stat == pytest.approx(64.45218480039682, rel=1e-6)
    assert first.p_value == pytest.approx(9.721726948773076e-12, rel=1e-6)


def test_granger_rejection_rate_on_synthetic_causal_dgp() -> None:
    p_values = [
        granger_causality(_white_dgp(seed, causal=True), cause="x", effect="y", max_lag=10).p_value
        for seed in range(100)
    ]

    assert sum(p < 0.05 for p in p_values) >= 95


def test_granger_false_positive_rate_on_synthetic_noncausal_dgp() -> None:
    p_values = [
        granger_causality(_white_dgp(seed, causal=False), cause="x", effect="y", max_lag=10).p_value
        for seed in range(100)
    ]

    assert sum(p < 0.05 for p in p_values) <= 6
