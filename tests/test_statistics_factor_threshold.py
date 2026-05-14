from __future__ import annotations

import numpy as np
import pytest
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from engine.strategy.statistics.factor_threshold import (
    harvey_liu_zhu_threshold_result,
    newey_west_t_statistic,
)


def test_newey_west_t_stat_matches_statsmodels_cov_hac() -> None:
    sm = pytest.importorskip("statsmodels.api")
    sandwich = pytest.importorskip("statsmodels.stats.sandwich_covariance")

    rng = np.random.default_rng(7)
    n_obs = 1_000
    x = rng.normal(size=n_obs)
    y = 2.0 * x + rng.normal(size=n_obs)
    lags = 5

    fit = sm.OLS(y, sm.add_constant(x)).fit()
    cov = sandwich.cov_hac(fit, nlags=lags, use_correction=True)
    expected = float(fit.params[1] / np.sqrt(cov[1, 1]))

    actual = newey_west_t_statistic(y, x, lags=lags, use_correction=True)
    assert actual == pytest.approx(expected, abs=1e-6)


def test_hlz_accepts_strong_slope_and_rejects_noise() -> None:
    rng = np.random.default_rng(11)
    x = rng.normal(size=1_000)
    y = 2.0 * x + rng.normal(scale=0.5, size=1_000)

    strong = harvey_liu_zhu_threshold_result(y, x, feature_id="factor.strong", lags=5)
    assert strong.passed
    assert abs(strong.t_stat) > 3.0

    failures = 0
    for seed in range(20):
        local = np.random.default_rng(seed)
        x_noise = local.normal(size=1_000)
        y_noise = local.normal(size=1_000)
        result = harvey_liu_zhu_threshold_result(y_noise, x_noise, feature_id=f"factor.noise_{seed}", lags=5)
        failures += int(abs(result.t_stat) < 3.0)

    assert failures >= 19


def test_hac_pairs_nonfinite_x_y_observations_and_scores_negative_infinity() -> None:
    y = [1.0, float("nan"), 3.0, 8.0, 11.0]
    x = [1.0, 2.0, float("nan"), 4.0, 5.0]
    actual = newey_west_t_statistic(y, x, lags=0, use_correction=True)
    expected = newey_west_t_statistic([1.0, 8.0, 11.0], [1.0, 4.0, 5.0], lags=0, use_correction=True)
    assert actual == pytest.approx(expected, abs=1e-12)

    result = harvey_liu_zhu_threshold_result(t_stat=float("-inf"), feature_id="factor.negative")
    assert result.passed
    assert result.p_value == 0.0
