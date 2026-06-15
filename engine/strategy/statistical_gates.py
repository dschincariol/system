"""Statistical promotion gates for champion/challenger promotion decisions."""

from __future__ import annotations

import math
import os
import random
from statistics import NormalDist
from typing import Any, Iterable, Mapping

_NORMAL = NormalDist()
_EULER_MASCHERONI = 0.5772156649015329
_EPS = 1e-12


def _safe_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return bool(value)
    text = str(value or "").strip().lower()
    if not text:
        return bool(default)
    return text in {"1", "true", "yes", "y", "on"}


def _safe_int(value: Any, default: int = 0) -> int:
    if value is None:
        return int(default)
    if isinstance(value, str) and not value.strip():
        return int(default)
    try:
        return int(value)
    except Exception:
        return int(default)


def _safe_optional_int(value: Any, default: int | None = None) -> int | None:
    if value is None:
        return default
    if isinstance(value, str) and not value.strip():
        return default
    try:
        return int(value)
    except Exception:
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return float(default)
    if isinstance(value, str) and not value.strip():
        return float(default)
    try:
        out = float(value)
    except Exception:
        return float(default)
    return float(out) if math.isfinite(out) else float(default)


def _as_statistic(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _clean_returns(returns: Iterable[Any] | None) -> list[float]:
    out: list[float] = []
    for raw in list(returns or []):
        try:
            value = float(raw)
        except Exception:
            continue
        if math.isfinite(value):
            out.append(float(value))
    return out


def _clean_models_returns(models_returns: Mapping[str, Iterable[Any]] | None) -> dict[str, list[float]]:
    if not isinstance(models_returns, Mapping):
        return {}
    out: dict[str, list[float]] = {}
    for idx, (raw_name, raw_values) in enumerate(models_returns.items(), start=1):
        model_name = str(raw_name or "").strip() or f"model_{idx}"
        cleaned = _clean_returns(raw_values)
        if cleaned:
            out[model_name] = cleaned
    return out


def _sample_mean(values: list[float]) -> float:
    if not values:
        return 0.0
    return float(sum(values) / float(len(values)))


def _sample_std(values: list[float], mean: float | None = None) -> float:
    n = len(values)
    if n < 2:
        return 0.0
    mu = float(_sample_mean(values) if mean is None else mean)
    variance = sum((float(value) - mu) ** 2 for value in values) / float(n - 1)
    if variance <= _EPS:
        return 0.0
    return float(math.sqrt(variance))


def _sharpe_ratio(values: list[float], mean: float | None = None, sample_std: float | None = None) -> float:
    mu = float(_sample_mean(values) if mean is None else mean)
    sigma = float(_sample_std(values, mean=mu) if sample_std is None else sample_std)
    if sigma <= _EPS:
        if mu > 0.0:
            return float("inf")
        if mu < 0.0:
            return float("-inf")
        return 0.0
    return float(mu / sigma)


def _skew_kurtosis(values: list[float], mean: float | None = None, sample_std: float | None = None) -> tuple[float, float]:
    n = len(values)
    if n < 3:
        return 0.0, 3.0
    mu = float(_sample_mean(values) if mean is None else mean)
    sigma = float(_sample_std(values, mean=mu) if sample_std is None else sample_std)
    if sigma <= _EPS:
        return 0.0, 3.0
    z_scores = [(float(value) - mu) / sigma for value in values]
    skew = sum(z ** 3 for z in z_scores) / float(n)
    kurt = sum(z ** 4 for z in z_scores) / float(n)
    if not math.isfinite(skew):
        skew = 0.0
    if not math.isfinite(kurt):
        kurt = 3.0
    return float(skew), float(max(kurt, 1.0))


def _normal_ppf(probability: float) -> float:
    clipped = min(1.0 - 1e-12, max(1e-12, float(probability)))
    return float(_NORMAL.inv_cdf(clipped))


def _two_sided_p_value_from_t(t_statistic: float) -> float:
    if not math.isfinite(t_statistic):
        if t_statistic > 0.0:
            return 0.0
        return 1.0
    tail = 1.0 - float(_NORMAL.cdf(abs(float(t_statistic))))
    return float(max(0.0, min(1.0, 2.0 * tail)))


def _standard_error(values: list[float], mean: float | None = None, sample_std: float | None = None) -> float:
    n_obs = len(values)
    if n_obs < 2:
        return 0.0
    sigma = float(_sample_std(values, mean=mean) if sample_std is None else sample_std)
    if sigma <= _EPS:
        return 0.0
    return float(sigma / math.sqrt(float(n_obs)))


def _bootstrap_statistic(
    values: list[float],
    *,
    studentize: bool,
    positive_only: bool,
    standard_error: float | None = None,
) -> float:
    n_obs = len(values)
    mean_return = _sample_mean(values)
    if studentize:
        stderr = float(_standard_error(values, mean=mean_return) if standard_error is None else standard_error)
        if stderr <= _EPS:
            if mean_return > 0.0:
                statistic = float("inf")
            elif mean_return < 0.0:
                statistic = float("-inf")
            else:
                statistic = 0.0
        else:
            statistic = float(mean_return / stderr)
    else:
        statistic = float(mean_return * math.sqrt(float(max(1, n_obs))))
    if positive_only:
        statistic = float(max(0.0, statistic))
    return statistic


def _prepare_bootstrap_models(
    models_returns: Mapping[str, Iterable[Any]] | None,
    *,
    min_observations: int,
) -> tuple[list[dict[str, Any]], int, list[str]]:
    cleaned = _clean_models_returns(models_returns)
    prepared: list[dict[str, Any]] = []
    dropped: list[str] = []
    for model_name in sorted(cleaned):
        values = cleaned.get(model_name) or []
        if len(values) < max(2, int(min_observations or 2)):
            dropped.append(str(model_name))
            continue
        mean_return = _sample_mean(values)
        std_return = _sample_std(values, mean=mean_return)
        stderr = _standard_error(values, mean=mean_return, sample_std=std_return)
        prepared.append(
            {
                "model_name": str(model_name),
                "returns": list(values),
                "centered_returns": [float(value) - float(mean_return) for value in values],
                "n_observations": int(len(values)),
                "mean_return": float(mean_return),
                "std_return": float(std_return),
                "standard_error": float(stderr),
                "t_statistic": float(compute_t_statistic(values)),
            }
        )
    return prepared, int(len(cleaned)), dropped


def _bootstrap_test_result(
    *,
    bootstrap_result: Mapping[str, Any],
    alpha: float,
    statistic_label: str,
) -> dict[str, Any]:
    alpha = min(1.0, max(_EPS, _safe_float(alpha, 0.05)))
    diagnostics = dict(bootstrap_result or {})
    diagnostics["alpha"] = float(alpha)
    diagnostics["statistic_label"] = str(statistic_label)
    diagnostics["passed"] = True
    diagnostics["p_value"] = 1.0
    diagnostics["best_model_name"] = str(diagnostics.get("best_model_name") or "")
    diagnostics["best_model_statistic"] = _as_statistic(diagnostics.get("observed_max_statistic"), 0.0)
    if not bool(diagnostics.get("applied")):
        diagnostics["status"] = str(diagnostics.get("status") or "insufficient_models")
        diagnostics["passed"] = True
        return diagnostics

    observed = _as_statistic(diagnostics.get("observed_max_statistic"), 0.0)
    distribution = [_as_statistic(value, 0.0) for value in list(diagnostics.get("distribution") or [])]
    exceedances = sum(1 for value in distribution if float(value) >= float(observed))
    p_value = float((1.0 + float(exceedances)) / float(len(distribution) + 1))
    diagnostics["p_value"] = float(max(0.0, min(1.0, p_value)))
    diagnostics["passed"] = bool(observed > 0.0 and diagnostics["p_value"] <= float(alpha))
    diagnostics["status"] = "evaluated"
    return diagnostics


def bootstrap_performance_distribution(
    models_returns: Mapping[str, Iterable[Any]] | None,
    *,
    bootstrap_samples: int = 1000,
    seed: int | None = None,
    studentize: bool = False,
    positive_only: bool = False,
    min_models: int = 2,
    min_observations: int = 2,
) -> dict[str, Any]:
    """Bootstrap the max performance statistic across competing model return series."""
    samples = max(10, int(bootstrap_samples or 0))
    required_models = max(2, int(min_models or 2))
    required_observations = max(2, int(min_observations or 2))
    prepared, input_model_count, dropped_models = _prepare_bootstrap_models(
        models_returns,
        min_observations=required_observations,
    )
    model_statistics = {
        str(item["model_name"]): {
            "n_observations": int(item["n_observations"]),
            "mean_return": float(item["mean_return"]),
            "std_return": float(item["std_return"]),
            "standard_error": float(item["standard_error"]),
            "t_statistic": float(item["t_statistic"]) if math.isfinite(float(item["t_statistic"])) else item["t_statistic"],
            "observed_statistic": _as_statistic(
                _bootstrap_statistic(
                    list(item["returns"]),
                    studentize=bool(studentize),
                    positive_only=bool(positive_only),
                    standard_error=float(item["standard_error"]),
                ),
                0.0,
            ),
        }
        for item in prepared
    }
    diagnostics = {
        "applied": False,
        "status": "insufficient_models",
        "bootstrap_samples": int(samples),
        "seed": (None if seed is None else int(seed)),
        "studentized": bool(studentize),
        "positive_only": bool(positive_only),
        "input_model_count": int(input_model_count),
        "model_count": int(len(prepared)),
        "min_models": int(required_models),
        "min_observations": int(required_observations),
        "dropped_models": list(dropped_models),
        "model_statistics": model_statistics,
        "distribution": [],
        "best_model_name": "",
        "observed_max_statistic": 0.0,
    }
    if len(prepared) < required_models:
        return diagnostics

    observed_items = sorted(
        (
            (
                _as_statistic(stats.get("observed_statistic"), 0.0),
                str(model_name),
            )
            for model_name, stats in model_statistics.items()
        ),
        key=lambda item: (-float(item[0]), str(item[1])),
    )
    diagnostics["best_model_name"] = str(observed_items[0][1]) if observed_items else ""
    diagnostics["observed_max_statistic"] = float(observed_items[0][0]) if observed_items else 0.0

    rng = random.Random(seed)
    distribution: list[float] = []
    for _ in range(samples):
        max_stat = float("-inf")
        for item in prepared:
            centered_returns = list(item["centered_returns"])
            n_obs = int(item["n_observations"])
            sample = [float(centered_returns[rng.randrange(n_obs)]) for _ in range(n_obs)]
            statistic = _bootstrap_statistic(
                sample,
                studentize=bool(studentize),
                positive_only=bool(positive_only),
                standard_error=float(item["standard_error"]),
            )
            if statistic > max_stat:
                max_stat = float(statistic)
        distribution.append(float(max_stat if math.isfinite(max_stat) else 0.0))

    diagnostics["distribution"] = distribution
    diagnostics["applied"] = True
    diagnostics["status"] = "evaluated"
    return diagnostics


def white_reality_check(
    models_returns: Mapping[str, Iterable[Any]] | None,
    *,
    alpha: float = 0.05,
    bootstrap_samples: int = 1000,
    seed: int | None = None,
    min_models: int = 2,
    min_observations: int = 2,
) -> dict[str, Any]:
    """Run White's Reality Check over competing model return streams."""
    bootstrap = bootstrap_performance_distribution(
        models_returns,
        bootstrap_samples=bootstrap_samples,
        seed=seed,
        studentize=False,
        positive_only=False,
        min_models=min_models,
        min_observations=min_observations,
    )
    diagnostics = _bootstrap_test_result(
        bootstrap_result=bootstrap,
        alpha=alpha,
        statistic_label="white_reality_check",
    )
    best_model_name = str(diagnostics.get("best_model_name") or "")
    best_stats = dict((bootstrap.get("model_statistics") or {}).get(best_model_name) or {})
    diagnostics["best_mean_return"] = float(_safe_float(best_stats.get("mean_return"), 0.0))
    diagnostics["best_t_statistic"] = best_stats.get("t_statistic")
    return diagnostics


def spa_test(
    models_returns: Mapping[str, Iterable[Any]] | None,
    *,
    alpha: float = 0.05,
    bootstrap_samples: int = 1000,
    seed: int | None = None,
    min_models: int = 2,
    min_observations: int = 2,
) -> dict[str, Any]:
    """Run the Superior Predictive Ability test over competing model returns."""
    bootstrap = bootstrap_performance_distribution(
        models_returns,
        bootstrap_samples=bootstrap_samples,
        seed=seed,
        studentize=True,
        positive_only=True,
        min_models=min_models,
        min_observations=min_observations,
    )
    diagnostics = _bootstrap_test_result(
        bootstrap_result=bootstrap,
        alpha=alpha,
        statistic_label="spa_test",
    )
    best_model_name = str(diagnostics.get("best_model_name") or "")
    best_stats = dict((bootstrap.get("model_statistics") or {}).get(best_model_name) or {})
    diagnostics["best_mean_return"] = float(_safe_float(best_stats.get("mean_return"), 0.0))
    diagnostics["best_t_statistic"] = best_stats.get("t_statistic")
    return diagnostics


def _multiple_testing_validation(
    models_returns: Mapping[str, Iterable[Any]] | None,
    config: Mapping[str, Any],
) -> dict[str, Any]:
    enabled = bool(config.get("spa_test_enabled"))
    bootstrap_samples = max(10, _safe_int(config.get("spa_bootstrap_samples"), 1000))
    min_models = max(2, _safe_int(config.get("spa_min_models"), 3))
    min_observations = max(2, _safe_int(config.get("min_observations"), 2))
    alpha = min(1.0, max(_EPS, _safe_float(config.get("spa_alpha"), 0.05)))
    seed = _safe_optional_int(config.get("spa_seed"))
    diagnostics = {
        "enabled": bool(enabled),
        "applied": False,
        "status": "disabled",
        "passed": True,
        "blocking_test": "spa_test",
        "blocking_passed": True,
        "bootstrap_samples": int(bootstrap_samples),
        "min_models": int(min_models),
        "min_observations": int(min_observations),
        "alpha": float(alpha),
        "seed": (None if seed is None else int(seed)),
        "white_reality_check": {
            "enabled": bool(enabled),
            "applied": False,
            "status": "disabled",
            "passed": True,
        },
        "spa_test": {
            "enabled": bool(enabled),
            "applied": False,
            "status": "disabled",
            "passed": True,
        },
    }
    if not enabled:
        return diagnostics

    white = white_reality_check(
        models_returns,
        alpha=alpha,
        bootstrap_samples=bootstrap_samples,
        seed=seed,
        min_models=min_models,
        min_observations=min_observations,
    )
    spa = spa_test(
        models_returns,
        alpha=alpha,
        bootstrap_samples=bootstrap_samples,
        seed=seed,
        min_models=min_models,
        min_observations=min_observations,
    )
    diagnostics["white_reality_check"] = dict(white or {})
    diagnostics["spa_test"] = dict(spa or {})
    diagnostics["applied"] = bool(white.get("applied") or spa.get("applied"))
    diagnostics["blocking_passed"] = bool(spa.get("passed")) if bool(spa.get("applied")) else True
    diagnostics["passed"] = bool(
        (bool(white.get("passed")) if bool(white.get("applied")) else True)
        and bool(diagnostics["blocking_passed"])
    )
    if bool(diagnostics["applied"]):
        diagnostics["status"] = "evaluated"
    else:
        diagnostics["status"] = str(
            (spa.get("status") or white.get("status") or "insufficient_models")
        )
    diagnostics["candidate_models_available"] = max(
        _safe_int(white.get("input_model_count"), 0),
        _safe_int(spa.get("input_model_count"), 0),
    )
    diagnostics["candidate_models_considered"] = max(
        _safe_int(white.get("model_count"), 0),
        _safe_int(spa.get("model_count"), 0),
    )
    return diagnostics


def promotion_gate_config_from_env(config: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Load and normalize statistical promotion gate settings from env and overrides."""
    env_config = {
        "enabled": _safe_bool(os.environ.get("CHAMPION_PROMOTION_USE_STAT_GATE", "0"), False),
        "min_t_stat": _safe_float(os.environ.get("CHAMPION_PROMOTION_MIN_T_STAT", "3.0"), 3.0),
        "min_deflated_sharpe": _safe_float(
            os.environ.get("CHAMPION_PROMOTION_MIN_DEFLATED_SHARPE", "0.0"),
            0.0,
        ),
        "min_observations": max(
            1,
            _safe_int(os.environ.get("CHAMPION_PROMOTION_MIN_OBSERVATIONS", "50"), 50),
        ),
        "fdr_alpha": min(
            1.0,
            max(
                _EPS,
                _safe_float(os.environ.get("CHAMPION_PROMOTION_FDR_ALPHA", "0.05"), 0.05),
            ),
        ),
        "spa_test_enabled": _safe_bool(os.environ.get("SPA_TEST_ENABLED", "0"), False),
        "spa_min_models": max(2, _safe_int(os.environ.get("SPA_MIN_MODELS", "3"), 3)),
        "spa_bootstrap_samples": max(
            10,
            _safe_int(os.environ.get("SPA_BOOTSTRAP_SAMPLES", "1000"), 1000),
        ),
        "spa_alpha": min(
            1.0,
            max(
                _EPS,
                _safe_float(os.environ.get("SPA_ALPHA", "0.05"), 0.05),
            ),
        ),
        "spa_seed": _safe_optional_int(os.environ.get("SPA_TEST_SEED")),
    }
    overrides = dict(config or {})
    if not overrides:
        return env_config

    merged = dict(env_config)
    nested_spa = overrides.get("spa") if isinstance(overrides.get("spa"), Mapping) else {}
    if "enabled" in overrides or "use_gate" in overrides or "use_stat_gate" in overrides:
        merged["enabled"] = _safe_bool(
            overrides.get("enabled", overrides.get("use_gate", overrides.get("use_stat_gate"))),
            merged["enabled"],
        )
    if "min_t_stat" in overrides or "min_t" in overrides:
        merged["min_t_stat"] = _safe_float(
            overrides.get("min_t_stat", overrides.get("min_t")),
            merged["min_t_stat"],
        )
    if "min_deflated_sharpe" in overrides or "min_dsr" in overrides:
        merged["min_deflated_sharpe"] = _safe_float(
            overrides.get("min_deflated_sharpe", overrides.get("min_dsr")),
            merged["min_deflated_sharpe"],
        )
    if "min_observations" in overrides or "min_obs" in overrides:
        merged["min_observations"] = max(
            1,
            _safe_int(
                overrides.get("min_observations", overrides.get("min_obs")),
                merged["min_observations"],
            ),
        )
    if "fdr_alpha" in overrides or "alpha" in overrides:
        merged["fdr_alpha"] = min(
            1.0,
            max(
                _EPS,
                _safe_float(overrides.get("fdr_alpha", overrides.get("alpha")), merged["fdr_alpha"]),
            ),
        )
    if "spa_test_enabled" in overrides or "spa_enabled" in overrides or "use_spa_test" in overrides:
        merged["spa_test_enabled"] = _safe_bool(
            overrides.get(
                "spa_test_enabled",
                overrides.get("spa_enabled", overrides.get("use_spa_test")),
            ),
            merged["spa_test_enabled"],
        )
    if "spa_test_enabled" in nested_spa or "enabled" in nested_spa:
        merged["spa_test_enabled"] = _safe_bool(
            nested_spa.get("spa_test_enabled", nested_spa.get("enabled")),
            merged["spa_test_enabled"],
        )
    if "spa_min_models" in overrides or "min_models" in nested_spa:
        merged["spa_min_models"] = max(
            2,
            _safe_int(
                overrides.get("spa_min_models", nested_spa.get("min_models")),
                merged["spa_min_models"],
            ),
        )
    if "spa_bootstrap_samples" in overrides or "bootstrap_samples" in nested_spa:
        merged["spa_bootstrap_samples"] = max(
            10,
            _safe_int(
                overrides.get("spa_bootstrap_samples", nested_spa.get("bootstrap_samples")),
                merged["spa_bootstrap_samples"],
            ),
        )
    if "spa_alpha" in overrides or "alpha" in nested_spa:
        merged["spa_alpha"] = min(
            1.0,
            max(
                _EPS,
                _safe_float(overrides.get("spa_alpha", nested_spa.get("alpha")), merged["spa_alpha"]),
            ),
        )
    if "spa_seed" in overrides or "seed" in nested_spa:
        merged["spa_seed"] = _safe_optional_int(
            overrides.get("spa_seed", nested_spa.get("seed")),
            merged.get("spa_seed"),
        )
    return merged


def compute_t_statistic(returns) -> float:
    """Compute the one-sample t-statistic for a sequence of returns."""
    values = _clean_returns(returns)
    n_obs = len(values)
    if n_obs < 2:
        return 0.0
    mean_return = _sample_mean(values)
    std_return = _sample_std(values, mean=mean_return)
    if std_return <= _EPS:
        if mean_return > 0.0:
            return float("inf")
        if mean_return < 0.0:
            return float("-inf")
        return 0.0
    return float((mean_return / std_return) * math.sqrt(float(n_obs)))


def deflated_sharpe_ratio(sharpe, n_trials, n_obs, skew, kurt) -> float:
    """Estimate the deflated Sharpe probability after multiple-testing adjustment."""
    sr = float(sharpe)
    trials = max(1, int(n_trials or 1))
    obs = max(0, int(n_obs or 0))
    skewness = float(skew or 0.0)
    kurtosis = float(kurt or 3.0)

    if obs < 2:
        return 0.0
    if not math.isfinite(sr):
        return 1.0 if sr > 0.0 else 0.0

    variance_term = 1.0 - (skewness * sr) + (((kurtosis - 1.0) / 4.0) * (sr ** 2))
    variance_term = max(_EPS, float(variance_term))
    sr_std = math.sqrt(variance_term / float(max(1, obs - 1)))

    if trials <= 1:
        benchmark_sharpe = 0.0
    else:
        z_one = _normal_ppf(1.0 - (1.0 / float(trials)))
        z_two = _normal_ppf(1.0 - (1.0 / (float(trials) * math.e)))
        benchmark_sharpe = float(sr_std * (((1.0 - _EULER_MASCHERONI) * z_one) + (_EULER_MASCHERONI * z_two)))

    z_score = (float(sr) - float(benchmark_sharpe)) / float(max(sr_std, _EPS))
    out = float(_NORMAL.cdf(z_score))
    return float(max(0.0, min(1.0, out)))


def benjamini_hochberg_fdr(p_values, alpha=0.05) -> list[bool]:
    """Apply Benjamini-Hochberg FDR control and return accepted hypothesis flags."""
    cleaned = [min(1.0, max(0.0, _safe_float(value, 1.0))) for value in list(p_values or [])]
    m = len(cleaned)
    if m <= 0:
        return []
    target_alpha = min(1.0, max(0.0, _safe_float(alpha, 0.05)))
    ordered = sorted(enumerate(cleaned), key=lambda item: item[1])
    cutoff_rank = 0
    for rank, (_idx, p_value) in enumerate(ordered, start=1):
        if float(p_value) <= (target_alpha * float(rank) / float(m)):
            cutoff_rank = int(rank)
    accepted = [False] * m
    if cutoff_rank <= 0:
        return accepted
    for idx, _p_value in ordered[:cutoff_rank]:
        accepted[int(idx)] = True
    return accepted


def harvey_liu_zhu_threshold(n_trials) -> float:
    """Return a conservative Harvey-Liu-Zhu t-stat hurdle for the trial count."""
    # Conservative Gaussian multiple-testing proxy for the HLZ t-stat hurdle.
    trials = max(1, int(n_trials or 1))
    familywise_tail = 0.05 / float(2 * trials)
    return float(_normal_ppf(1.0 - familywise_tail))


def passes_promotion_gate(returns, n_competing_trials, config=None, models_returns=None) -> tuple[bool, dict]:
    """Evaluate one candidate model against the configured statistical promotion gates."""
    cfg = promotion_gate_config_from_env(config)
    values = _clean_returns(returns)
    n_obs = len(values)
    trials = max(1, int(n_competing_trials or 1))
    mean_return = _sample_mean(values)
    std_return = _sample_std(values, mean=mean_return)
    sharpe = _sharpe_ratio(values, mean=mean_return, sample_std=std_return)
    skew, kurt = _skew_kurtosis(values, mean=mean_return, sample_std=std_return)
    t_statistic = compute_t_statistic(values)
    deflated_sharpe = deflated_sharpe_ratio(sharpe, trials, n_obs, skew, kurt)
    p_value = _two_sided_p_value_from_t(t_statistic)
    threshold_t = max(
        float(cfg.get("min_t_stat") or 0.0),
        float(harvey_liu_zhu_threshold(trials)),
    )
    fdr_mask = benjamini_hochberg_fdr([p_value] + ([1.0] * max(0, trials - 1)), alpha=float(cfg.get("fdr_alpha") or 0.05))
    fdr_pass = bool(fdr_mask[0]) if fdr_mask else False
    enough_observations = n_obs >= int(cfg.get("min_observations") or 0)
    multiple_testing = _multiple_testing_validation(
        models_returns,
        {
            **cfg,
            "spa_test_enabled": (bool(cfg.get("spa_test_enabled")) and bool(cfg.get("enabled"))),
        },
    )

    diagnostics = {
        "enabled": bool(cfg.get("enabled")),
        "applied": bool(cfg.get("enabled")),
        "status": "disabled",
        "n_observations": int(n_obs),
        "n_competing_trials": int(trials),
        "mean_return": float(mean_return),
        "std_return": float(std_return),
        "sharpe": float(sharpe) if math.isfinite(sharpe) else sharpe,
        "skew": float(skew),
        "kurt": float(kurt),
        "t_statistic": float(t_statistic) if math.isfinite(t_statistic) else t_statistic,
        "deflated_sharpe": float(deflated_sharpe),
        "p_value": float(p_value),
        "threshold_t": float(threshold_t),
        "min_t_stat": float(cfg.get("min_t_stat") or 0.0),
        "min_deflated_sharpe": float(cfg.get("min_deflated_sharpe") or 0.0),
        "min_observations": int(cfg.get("min_observations") or 0),
        "fdr_alpha": float(cfg.get("fdr_alpha") or 0.05),
        "fdr_pass": bool(fdr_pass),
        "spa_test_enabled": bool(cfg.get("spa_test_enabled")),
        "spa_min_models": int(cfg.get("spa_min_models") or 0),
        "spa_bootstrap_samples": int(cfg.get("spa_bootstrap_samples") or 0),
        "spa_alpha": float(cfg.get("spa_alpha") or 0.05),
        "spa_seed": cfg.get("spa_seed"),
        "multiple_testing": dict(multiple_testing or {}),
        "white_reality_check": dict((multiple_testing or {}).get("white_reality_check") or {}),
        "spa_test": dict((multiple_testing or {}).get("spa_test") or {}),
        "spa_pass": bool((multiple_testing or {}).get("blocking_passed", True)),
        "t_pass": bool(
            (math.isfinite(t_statistic) or float(t_statistic) == float("inf"))
            and float(t_statistic) >= float(threshold_t)
        ),
        "dsr_pass": bool(float(deflated_sharpe) >= float(cfg.get("min_deflated_sharpe") or 0.0)),
        "passed": True,
    }

    if not bool(cfg.get("enabled")):
        diagnostics["applied"] = False
        diagnostics["status"] = "disabled"
        diagnostics["passed"] = True
        return True, diagnostics

    if not enough_observations:
        diagnostics["status"] = "insufficient_observations"
        diagnostics["passed"] = False
        return False, diagnostics

    passed = bool(
        diagnostics["t_pass"]
        and diagnostics["dsr_pass"]
        and diagnostics["fdr_pass"]
        and diagnostics["spa_pass"]
    )
    diagnostics["status"] = "evaluated"
    if diagnostics["t_pass"] and diagnostics["dsr_pass"] and diagnostics["fdr_pass"] and not diagnostics["spa_pass"]:
        diagnostics["status"] = "spa_test_failed"
    diagnostics["passed"] = bool(passed)
    return passed, diagnostics


__all__ = [
    "bootstrap_performance_distribution",
    "benjamini_hochberg_fdr",
    "compute_t_statistic",
    "deflated_sharpe_ratio",
    "harvey_liu_zhu_threshold",
    "passes_promotion_gate",
    "promotion_gate_config_from_env",
    "spa_test",
    "white_reality_check",
]
