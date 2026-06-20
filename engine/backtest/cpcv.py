"""Combinatorial purged cross-validation splitters.

The splitter follows the sklearn ``split(X, y=None, groups=None)`` shape while
also accepting label windows through either the constructor or ``groups``. A
label window is the interval over which a sample's target is observed; purging
removes train samples whose target interval overlaps a test interval.
"""

from __future__ import annotations
import logging

import itertools
import math
from dataclasses import dataclass
from typing import Any, Iterator, Sequence

import numpy as np


def _n_samples(X: Any) -> int:
    try:
        return int(len(X))
    except Exception:
        arr = np.asarray(X)
        if arr.ndim == 0:
            raise ValueError("X must contain at least one sample")
        return int(arr.shape[0])


def _as_1d(values: Any, *, n_samples: int, name: str) -> np.ndarray:
    arr = np.asarray(values, dtype=float).reshape(-1)
    if arr.size != int(n_samples):
        raise ValueError(f"{name} length {arr.size} does not match n_samples {n_samples}")
    return arr


def _sample_starts(X: Any, n_samples: int) -> np.ndarray:
    index = getattr(X, "index", None)
    if index is not None and len(index) == int(n_samples):
        try:
            return np.asarray(index, dtype=float).reshape(-1)
        except Exception:
            logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)
    return np.arange(int(n_samples), dtype=float)


def _resolve_label_windows(
    X: Any,
    n_samples: int,
    *,
    groups: Any = None,
    label_start_times: Any = None,
    label_end_times: Any = None,
    label_horizon: int | float = 0,
) -> tuple[np.ndarray, np.ndarray]:
    starts = _sample_starts(X, n_samples)
    ends: np.ndarray | None = None

    if label_start_times is not None:
        starts = _as_1d(label_start_times, n_samples=n_samples, name="label_start_times")
    if label_end_times is not None:
        ends = _as_1d(label_end_times, n_samples=n_samples, name="label_end_times")

    if groups is not None and ends is None:
        if isinstance(groups, dict):
            if groups.get("label_start") is not None:
                starts = _as_1d(groups.get("label_start"), n_samples=n_samples, name="groups.label_start")
            elif groups.get("start") is not None:
                starts = _as_1d(groups.get("start"), n_samples=n_samples, name="groups.start")
            if groups.get("label_end") is not None:
                ends = _as_1d(groups.get("label_end"), n_samples=n_samples, name="groups.label_end")
            elif groups.get("end") is not None:
                ends = _as_1d(groups.get("end"), n_samples=n_samples, name="groups.end")
        elif isinstance(groups, (tuple, list)) and len(groups) == 2:
            starts = _as_1d(groups[0], n_samples=n_samples, name="groups[0]")
            ends = _as_1d(groups[1], n_samples=n_samples, name="groups[1]")
        else:
            ends = _as_1d(groups, n_samples=n_samples, name="groups")

    if ends is None:
        horizon = float(label_horizon or 0.0)
        ends = starts + max(0.0, horizon)

    ends = np.maximum(np.asarray(ends, dtype=float), np.asarray(starts, dtype=float))
    return np.asarray(starts, dtype=float), np.asarray(ends, dtype=float)


def _contiguous_segments(indices: Sequence[int] | np.ndarray) -> list[tuple[int, int]]:
    arr = np.unique(np.asarray(indices, dtype=int).reshape(-1))
    if arr.size == 0:
        return []
    segments: list[tuple[int, int]] = []
    start = int(arr[0])
    prev = int(arr[0])
    for value in arr[1:]:
        value_i = int(value)
        if value_i != prev + 1:
            segments.append((start, prev))
            start = value_i
        prev = value_i
    segments.append((start, prev))
    return segments


def _embargo_count(embargo: int | float, n_samples: int) -> int:
    value = float(embargo or 0.0)
    if value <= 0.0:
        return 0
    if value < 1.0:
        return int(float(n_samples) * value)
    return int(value)


def embargo_indices(
    test_indices: Sequence[int] | np.ndarray,
    n_samples: int,
    embargo: int | float,
) -> np.ndarray:
    """Return sample positions excluded by the post-test embargo."""
    n = int(max(0, n_samples))
    width = _embargo_count(embargo, n)
    test = np.unique(np.asarray(test_indices, dtype=int).reshape(-1))
    if n <= 0 or test.size == 0 or width <= 0:
        return np.asarray([], dtype=int)

    out: list[int] = []
    for _start, end in _contiguous_segments(test):
        embargo_start = int(end) + 1
        embargo_end = min(n - 1, int(end) + int(width))
        if embargo_start <= embargo_end:
            out.extend(range(embargo_start, embargo_end + 1))
    return np.unique(np.asarray(out, dtype=int))


def purged_train_indices(
    train_indices: Sequence[int] | np.ndarray,
    test_indices: Sequence[int] | np.ndarray,
    *,
    label_start_times: Sequence[float] | np.ndarray | None = None,
    label_end_times: Sequence[float] | np.ndarray | None = None,
    label_horizon: int | float = 0,
    embargo: int | float = 0.0,
    n_samples: int | None = None,
) -> np.ndarray:
    """Remove train samples whose label windows overlap test windows."""
    train = np.unique(np.asarray(train_indices, dtype=int).reshape(-1))
    test = np.unique(np.asarray(test_indices, dtype=int).reshape(-1))
    if train.size == 0 or test.size == 0:
        return train.astype(int, copy=False)

    inferred_n = int(
        n_samples
        if n_samples is not None
        else max(int(train.max(initial=0)), int(test.max(initial=0))) + 1
    )
    dummy_X = np.arange(inferred_n, dtype=float)
    starts, ends = _resolve_label_windows(
        dummy_X,
        inferred_n,
        label_start_times=label_start_times,
        label_end_times=label_end_times,
        label_horizon=label_horizon,
    )

    keep = np.ones(train.shape[0], dtype=bool)
    test_starts = starts[test]
    test_ends = ends[test]
    for test_start, test_end in zip(test_starts, test_ends):
        overlaps = (starts[train] <= float(test_end)) & (ends[train] >= float(test_start))
        keep &= ~overlaps

    emb = embargo_indices(test, inferred_n, embargo)
    if emb.size:
        keep &= ~np.isin(train, emb)
    return np.sort(train[keep].astype(int, copy=False))


@dataclass
class CombinatorialPurgedKFold:
    """sklearn-style CPCV splitter with purging and embargo.

    Parameters
    ----------
    n_splits:
        Number of contiguous chronological groups.
    n_test_splits:
        Number of groups selected for each test combination.
    embargo:
        Fraction of total samples when ``0 < embargo < 1``; otherwise an
        absolute observation count.
    label_horizon:
        Fallback label horizon in sample units when explicit label end times
        are not supplied.
    """

    n_splits: int = 6
    n_test_splits: int = 2
    embargo: int | float = 0.0
    label_horizon: int | float = 0
    label_start_times: Sequence[float] | np.ndarray | None = None
    label_end_times: Sequence[float] | np.ndarray | None = None

    def __post_init__(self) -> None:
        if int(self.n_splits) < 2:
            raise ValueError("n_splits must be at least 2")
        if int(self.n_test_splits) < 1:
            raise ValueError("n_test_splits must be at least 1")
        if int(self.n_test_splits) >= int(self.n_splits):
            raise ValueError("n_test_splits must be smaller than n_splits")
        if float(self.embargo or 0.0) < 0.0:
            raise ValueError("embargo must be non-negative")

    def get_n_splits(self, X: Any = None, y: Any = None, groups: Any = None) -> int:
        del X, y, groups
        return int(math.comb(int(self.n_splits), int(self.n_test_splits)))

    def split(self, X: Any, y: Any = None, groups: Any = None) -> Iterator[tuple[np.ndarray, np.ndarray]]:
        del y
        n = _n_samples(X)
        if n < int(self.n_splits):
            raise ValueError("n_samples must be at least n_splits")

        starts, ends = _resolve_label_windows(
            X,
            n,
            groups=groups,
            label_start_times=self.label_start_times,
            label_end_times=self.label_end_times,
            label_horizon=self.label_horizon,
        )
        folds = [np.asarray(fold, dtype=int) for fold in np.array_split(np.arange(n, dtype=int), int(self.n_splits))]
        all_indices = np.arange(n, dtype=int)

        for combo in itertools.combinations(range(int(self.n_splits)), int(self.n_test_splits)):
            test_idx = np.sort(np.concatenate([folds[idx] for idx in combo]).astype(int, copy=False))
            raw_train = np.setdiff1d(all_indices, test_idx, assume_unique=True)
            train_idx = purged_train_indices(
                raw_train,
                test_idx,
                label_start_times=starts,
                label_end_times=ends,
                embargo=self.embargo,
                n_samples=n,
            )
            yield train_idx.astype(int, copy=False), test_idx.astype(int, copy=False)


__all__ = [
    "CombinatorialPurgedKFold",
    "embargo_indices",
    "purged_train_indices",
]
