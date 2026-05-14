"""White Reality Check via Politis-Romano stationary bootstrap.

The promotion gate tests whether a challenger has superior out-of-sample
performance relative to the incumbent after accounting for data-snooping across
candidate models. References: White, H. (2000), "A Reality Check for Data
Snooping", Econometrica; Politis, D. and Romano, J. (1994), "The Stationary
Bootstrap", Journal of the American Statistical Association.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence

import numpy as np

_EPS = 1e-15


@dataclass(frozen=True)
class RealityCheckResult:
    """Structured result for White's Reality Check."""

    test_name: str
    p_value: float
    observed_statistic: float
    bootstrap_distribution: np.ndarray
    bootstrap_samples: int
    alpha: float
    passed: bool
    best_model: str
    model_statistics: dict
    random_state: int
    average_block_length: float
    n_obs: int
    status: str

    def to_dict(self, *, include_distribution: bool = True) -> dict:
        payload = {
            "test_name": str(self.test_name),
            "p_value": float(self.p_value),
            "observed_statistic": float(self.observed_statistic),
            "bootstrap_samples": int(self.bootstrap_samples),
            "alpha": float(self.alpha),
            "passed": bool(self.passed),
            "best_model": str(self.best_model),
            "model_statistics": dict(self.model_statistics or {}),
            "random_state": int(self.random_state),
            "average_block_length": float(self.average_block_length),
            "n_obs": int(self.n_obs),
            "status": str(self.status),
        }
        if include_distribution:
            payload["bootstrap_distribution"] = [float(x) for x in self.bootstrap_distribution.tolist()]
        return payload


def _as_returns(values: Iterable[float], *, name: str) -> np.ndarray:
    arr = np.asarray(list([] if values is None else values), dtype=float)
    if arr.ndim != 1:
        arr = arr.reshape(-1)
    arr = arr[np.isfinite(arr)]
    if arr.size == 0:
        raise ValueError(f"{name}_must_have_finite_returns")
    return arr.astype(float)


def _sample_sharpe(values: np.ndarray) -> float:
    arr = np.asarray(values, dtype=float)
    if arr.size < 2:
        return 0.0
    mean = float(np.mean(arr))
    std = float(np.std(arr, ddof=1))
    if std <= _EPS:
        if mean > 0.0:
            return float("inf")
        if mean < 0.0:
            return float("-inf")
        return 0.0
    return float(mean / std)


def _studentized_mean(values: np.ndarray) -> float:
    arr = np.asarray(values, dtype=float)
    n = int(arr.size)
    if n < 2:
        return 0.0
    mean = float(np.mean(arr))
    std = float(np.std(arr, ddof=1))
    if std <= _EPS:
        if mean > 0.0:
            return float("inf")
        if mean < 0.0:
            return float("-inf")
        return 0.0
    return float(math.sqrt(float(n)) * mean / std)


def _sharpe_difference(candidate: np.ndarray, benchmark: np.ndarray) -> float:
    return float(_sample_sharpe(candidate) - _sample_sharpe(benchmark))


def stationary_bootstrap_indices(
    n_obs: int,
    *,
    random_state: int = 42,
    average_block_length: float | None = None,
) -> np.ndarray:
    """Draw one stationary-bootstrap index path of length `n_obs`."""

    n = int(max(0, int(n_obs)))
    if n <= 0:
        return np.asarray([], dtype=int)
    block = float(average_block_length or max(1.0, math.sqrt(float(n))))
    probability = min(1.0, max(_EPS, 1.0 / block))
    rng = np.random.default_rng(int(random_state))
    out = np.empty(n, dtype=int)
    out[0] = int(rng.integers(0, n))
    for idx in range(1, n):
        if float(rng.random()) < probability:
            out[idx] = int(rng.integers(0, n))
        else:
            out[idx] = int((out[idx - 1] + 1) % n)
    return out


def _bootstrap_index_matrix(
    n_obs: int,
    samples: int,
    *,
    random_state: int,
    average_block_length: float,
) -> np.ndarray:
    rng = np.random.default_rng(int(random_state))
    n = int(n_obs)
    probability = min(1.0, max(_EPS, 1.0 / float(average_block_length)))
    out = np.empty((int(samples), n), dtype=int)
    for sample_idx in range(int(samples)):
        out[sample_idx, 0] = int(rng.integers(0, n))
        for t in range(1, n):
            if float(rng.random()) < probability:
                out[sample_idx, t] = int(rng.integers(0, n))
            else:
                out[sample_idx, t] = int((out[sample_idx, t - 1] + 1) % n)
    return out


def _align_candidate_matrix(
    *,
    challenger_returns: Iterable[float] | None,
    champion_returns: Iterable[float] | None,
    candidate_returns: Mapping[str, Iterable[float]] | Sequence[Iterable[float]] | None,
    benchmark_returns: Iterable[float] | None,
) -> tuple[list[str], np.ndarray, np.ndarray]:
    candidates: list[tuple[str, np.ndarray]] = []
    if candidate_returns is not None:
        if isinstance(candidate_returns, Mapping):
            iterable = list(candidate_returns.items())
        else:
            iterable = [(f"candidate_{idx}", values) for idx, values in enumerate(candidate_returns, start=1)]
        for raw_label, raw_values in iterable:
            label = str(raw_label or f"candidate_{len(candidates) + 1}").strip() or f"candidate_{len(candidates) + 1}"
            candidates.append((label, _as_returns(raw_values, name=label)))
    elif challenger_returns is not None:
        candidates.append(("challenger", _as_returns(challenger_returns, name="challenger_returns")))

    if not candidates:
        raise ValueError("candidate_returns_required")

    if benchmark_returns is not None:
        benchmark = _as_returns(benchmark_returns, name="benchmark_returns")
    elif champion_returns is not None:
        benchmark = _as_returns(champion_returns, name="champion_returns")
    else:
        raise ValueError("champion_or_benchmark_returns_required")

    n = min([int(benchmark.size)] + [int(values.size) for _label, values in candidates])
    if n < 2:
        raise ValueError("at_least_two_aligned_oos_returns_required")
    labels = [label for label, _values in candidates]
    matrix = np.vstack([values[:n].astype(float) for _label, values in candidates])
    return labels, matrix, benchmark[:n].astype(float)


def white_reality_check(
    challenger_returns: Iterable[float] | None = None,
    champion_returns: Iterable[float] | None = None,
    *,
    candidate_returns: Mapping[str, Iterable[float]] | Sequence[Iterable[float]] | None = None,
    benchmark_returns: Iterable[float] | None = None,
    alpha: float = 0.05,
    bootstrap_samples: int = 10_000,
    average_block_length: float | None = None,
    random_state: int = 42,
) -> RealityCheckResult:
    """Run White's Reality Check on out-of-sample Sharpe differences."""

    level = float(min(1.0, max(0.0, float(alpha))))
    samples = int(max(1, int(bootstrap_samples or 0)))
    try:
        labels, candidate_matrix, benchmark = _align_candidate_matrix(
            challenger_returns=challenger_returns,
            champion_returns=champion_returns,
            candidate_returns=candidate_returns,
            benchmark_returns=benchmark_returns,
        )
    except Exception as exc:
        return RealityCheckResult(
            test_name="white_reality_check",
            p_value=1.0,
            observed_statistic=0.0,
            bootstrap_distribution=np.asarray([], dtype=float),
            bootstrap_samples=int(samples),
            alpha=float(level),
            passed=False,
            best_model="",
            model_statistics={},
            random_state=int(random_state),
            average_block_length=float(average_block_length or 0.0),
            n_obs=0,
            status=f"invalid_input:{type(exc).__name__}",
        )

    active = candidate_matrix - benchmark.reshape(1, -1)
    n_obs = int(active.shape[1])
    block = float(average_block_length or max(1.0, math.sqrt(float(n_obs))))

    observed_by_model = np.asarray(
        [_sharpe_difference(candidate_matrix[idx], benchmark) for idx in range(candidate_matrix.shape[0])],
        dtype=float,
    )
    observed_for_max = np.nan_to_num(observed_by_model, nan=-np.inf, posinf=np.inf, neginf=-np.inf)
    best_idx = int(np.argmax(observed_for_max))
    observed = float(observed_by_model[best_idx])

    active_means = np.mean(active, axis=1)
    centered_active = active - active_means.reshape(-1, 1)
    index_matrix = _bootstrap_index_matrix(
        n_obs,
        samples,
        random_state=int(random_state),
        average_block_length=float(block),
    )

    distribution = np.empty(samples, dtype=float)
    chunk_size = 1024
    for start in range(0, samples, chunk_size):
        stop = min(samples, start + chunk_size)
        idx = index_matrix[start:stop]
        benchmark_sample = benchmark[idx]
        active_sample = centered_active[:, idx]
        candidate_null = benchmark_sample.reshape(1, stop - start, n_obs) + active_sample
        benchmark_mean = np.mean(benchmark_sample, axis=1)
        benchmark_std = np.std(benchmark_sample, axis=1, ddof=1)
        candidate_mean = np.mean(candidate_null, axis=2)
        candidate_std = np.std(candidate_null, axis=2, ddof=1)
        with np.errstate(divide="ignore", invalid="ignore"):
            benchmark_sharpe = benchmark_mean / benchmark_std
            candidate_sharpe = candidate_mean / candidate_std
        benchmark_sharpe = np.where(
            benchmark_std <= _EPS,
            np.where(benchmark_mean > 0.0, np.inf, np.where(benchmark_mean < 0.0, -np.inf, 0.0)),
            benchmark_sharpe,
        )
        candidate_sharpe = np.where(
            candidate_std <= _EPS,
            np.where(candidate_mean > 0.0, np.inf, np.where(candidate_mean < 0.0, -np.inf, 0.0)),
            candidate_sharpe,
        )
        with np.errstate(invalid="ignore"):
            stats = candidate_sharpe - benchmark_sharpe.reshape(1, stop - start)
        stats = np.nan_to_num(stats, nan=-np.inf, posinf=np.inf, neginf=-np.inf)
        distribution[start:stop] = np.max(stats, axis=0)

    if not math.isfinite(observed):
        p_value = 1.0 / float(samples + 1) if observed > 0.0 else 1.0
    else:
        exceedances = int(np.sum(distribution >= float(observed)))
        p_value = float((1.0 + exceedances) / float(samples + 1))
    p_value = float(max(0.0, min(1.0, p_value)))

    model_statistics = {}
    for idx, label in enumerate(labels):
        model_statistics[str(label)] = {
            "active_mean": float(np.mean(active[idx])),
            "active_std": float(np.std(active[idx], ddof=1)) if n_obs > 1 else 0.0,
            "active_t_stat": float(_studentized_mean(active[idx])),
            "candidate_sharpe": float(_sample_sharpe(candidate_matrix[idx])),
            "benchmark_sharpe": float(_sample_sharpe(benchmark)),
            "sharpe_difference": float(observed_by_model[idx]),
        }

    passed = bool(float(observed) > 0.0 and p_value < level)
    return RealityCheckResult(
        test_name="white_reality_check",
        p_value=float(p_value),
        observed_statistic=float(observed),
        bootstrap_distribution=distribution.astype(float),
        bootstrap_samples=int(samples),
        alpha=float(level),
        passed=bool(passed),
        best_model=str(labels[best_idx]),
        model_statistics=model_statistics,
        random_state=int(random_state),
        average_block_length=float(block),
        n_obs=int(n_obs),
        status="evaluated",
    )


def hansen_spa_test(*args, **kwargs) -> RealityCheckResult:
    """Compatibility wrapper using the same stationary-bootstrap evidence path."""

    return white_reality_check(*args, **kwargs)


__all__ = [
    "RealityCheckResult",
    "hansen_spa_test",
    "stationary_bootstrap_indices",
    "white_reality_check",
]
