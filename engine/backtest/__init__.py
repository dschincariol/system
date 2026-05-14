"""Backtesting utilities shared by operational runners and training jobs."""

from engine.backtest.cpcv import (
    CombinatorialPurgedKFold,
    embargo_indices,
    purged_train_indices,
)

__all__ = [
    "CombinatorialPurgedKFold",
    "embargo_indices",
    "purged_train_indices",
]
