from __future__ import annotations

from engine.strategy.feature_registry import expected_columns


def test_expected_columns_order_is_stable_across_repeated_calls() -> None:
    first = expected_columns()
    assert first
    for _ in range(100):
        assert expected_columns() == first
