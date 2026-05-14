"""Multiple-hypothesis testing corrections for acceptance gates.

Implements Benjamini and Hochberg's false-discovery-rate procedure alongside
Bonferroni and Holm family-wise-error controls. Reference: Benjamini, Y. and
Hochberg, Y. (1995), "Controlling the False Discovery Rate: A Practical and
Powerful Approach to Multiple Testing", Journal of the Royal Statistical
Society Series B.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence

import numpy as np

_EPS = 1e-15


@dataclass(frozen=True)
class MultipleTestResult:
    """Structured result for a multiple-testing correction."""

    method: str
    alpha: float
    p_values: np.ndarray
    q_values: np.ndarray
    rejected: np.ndarray
    order: np.ndarray
    cutoff_p_value: float | None
    labels: tuple[str, ...]

    @property
    def n_tests(self) -> int:
        return int(self.p_values.size)

    @property
    def n_rejected(self) -> int:
        return int(np.sum(self.rejected))

    def to_dict(self) -> dict:
        return {
            "method": str(self.method),
            "alpha": float(self.alpha),
            "p_values": [float(x) for x in self.p_values.tolist()],
            "q_values": [float(x) for x in self.q_values.tolist()],
            "rejected": [bool(x) for x in self.rejected.tolist()],
            "order": [int(x) for x in self.order.tolist()],
            "cutoff_p_value": (None if self.cutoff_p_value is None else float(self.cutoff_p_value)),
            "labels": list(self.labels),
            "n_tests": int(self.n_tests),
            "n_rejected": int(self.n_rejected),
        }


def _as_p_values(p_values: Iterable[float]) -> np.ndarray:
    raw = np.asarray(list([] if p_values is None else p_values), dtype=float)
    if raw.ndim != 1:
        raw = raw.reshape(-1)
    raw = np.where(np.isfinite(raw), raw, 1.0)
    return np.clip(raw, 0.0, 1.0)


def _labels(labels: Sequence[str] | None, n: int) -> tuple[str, ...]:
    if labels is None:
        return tuple()
    out = tuple(str(label) for label in list(labels)[:n])
    if len(out) == n:
        return out
    return out + tuple("" for _ in range(n - len(out)))


def benjamini_hochberg(
    p_values: Iterable[float],
    *,
    q: float = 0.10,
    labels: Sequence[str] | None = None,
    random_state: int = 42,
) -> MultipleTestResult:
    """Apply the Benjamini-Hochberg FDR procedure and return adjusted q-values."""

    _ = int(random_state)
    p = _as_p_values(p_values)
    m = int(p.size)
    alpha = float(np.clip(float(q), 0.0, 1.0))
    if m == 0:
        empty_bool = np.asarray([], dtype=bool)
        empty_float = np.asarray([], dtype=float)
        empty_int = np.asarray([], dtype=int)
        return MultipleTestResult(
            method="benjamini_hochberg",
            alpha=alpha,
            p_values=empty_float,
            q_values=empty_float,
            rejected=empty_bool,
            order=empty_int,
            cutoff_p_value=None,
            labels=tuple(),
        )

    order = np.argsort(p, kind="mergesort")
    ranked = p[order]
    ranks = np.arange(1, m + 1, dtype=float)
    adjusted_sorted = ranked * float(m) / ranks
    adjusted_sorted = np.minimum.accumulate(adjusted_sorted[::-1])[::-1]
    adjusted_sorted = np.clip(adjusted_sorted, 0.0, 1.0)

    q_values = np.empty(m, dtype=float)
    q_values[order] = adjusted_sorted
    rejected = q_values <= (alpha + _EPS)

    cutoff = None
    passing = np.where(ranked <= (alpha * ranks / float(m) + _EPS))[0]
    if passing.size:
        cutoff = float(ranked[int(passing[-1])])

    return MultipleTestResult(
        method="benjamini_hochberg",
        alpha=alpha,
        p_values=p,
        q_values=q_values,
        rejected=rejected,
        order=order.astype(int),
        cutoff_p_value=cutoff,
        labels=_labels(labels, m),
    )


def bonferroni(
    p_values: Iterable[float],
    *,
    alpha: float = 0.05,
    labels: Sequence[str] | None = None,
    random_state: int = 42,
) -> MultipleTestResult:
    """Apply Bonferroni family-wise-error correction."""

    _ = int(random_state)
    p = _as_p_values(p_values)
    m = int(p.size)
    level = float(np.clip(float(alpha), 0.0, 1.0))
    order = np.argsort(p, kind="mergesort")
    q_values = np.clip(p * float(max(1, m)), 0.0, 1.0)
    rejected = q_values <= (level + _EPS)
    cutoff = float(np.max(p[rejected])) if bool(np.any(rejected)) else None
    return MultipleTestResult(
        method="bonferroni",
        alpha=level,
        p_values=p,
        q_values=q_values,
        rejected=rejected,
        order=order.astype(int),
        cutoff_p_value=cutoff,
        labels=_labels(labels, m),
    )


def holm(
    p_values: Iterable[float],
    *,
    alpha: float = 0.05,
    labels: Sequence[str] | None = None,
    random_state: int = 42,
) -> MultipleTestResult:
    """Apply Holm's step-down family-wise-error correction."""

    _ = int(random_state)
    p = _as_p_values(p_values)
    m = int(p.size)
    level = float(np.clip(float(alpha), 0.0, 1.0))
    if m == 0:
        return MultipleTestResult(
            method="holm",
            alpha=level,
            p_values=p,
            q_values=p,
            rejected=np.asarray([], dtype=bool),
            order=np.asarray([], dtype=int),
            cutoff_p_value=None,
            labels=tuple(),
        )

    order = np.argsort(p, kind="mergesort")
    ranked = p[order]
    factors = np.arange(m, 0, -1, dtype=float)
    adjusted_sorted = np.maximum.accumulate(ranked * factors)
    adjusted_sorted = np.clip(adjusted_sorted, 0.0, 1.0)

    q_values = np.empty(m, dtype=float)
    q_values[order] = adjusted_sorted
    rejected = q_values <= (level + _EPS)
    cutoff = float(np.max(p[rejected])) if bool(np.any(rejected)) else None
    return MultipleTestResult(
        method="holm",
        alpha=level,
        p_values=p,
        q_values=q_values,
        rejected=rejected,
        order=order.astype(int),
        cutoff_p_value=cutoff,
        labels=_labels(labels, m),
    )


def bh_fdr(
    p_values: Iterable[float],
    *,
    q: float = 0.10,
    labels: Sequence[str] | None = None,
    random_state: int = 42,
) -> MultipleTestResult:
    """Alias for :func:`benjamini_hochberg`."""

    return benjamini_hochberg(p_values, q=q, labels=labels, random_state=random_state)


__all__ = [
    "MultipleTestResult",
    "benjamini_hochberg",
    "bh_fdr",
    "bonferroni",
    "holm",
]
