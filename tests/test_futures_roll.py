from __future__ import annotations

import math

from engine.data.futures_roll import (
    build_ratio_adjusted_continuous,
    compute_roll_yield,
    detect_rolls,
)


def _bar(ts_ms: int, close: float, *, oi: float, volume: float) -> dict:
    return {
        "ts_ms": ts_ms,
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "open_interest": oi,
        "volume": volume,
    }


def test_detect_rolls_and_ratio_adjusted_continuous_preserves_returns() -> None:
    bars = {
        "ESM26": [
            _bar(1_000, 100.0, oi=500.0, volume=100.0),
            _bar(2_000, 101.0, oi=450.0, volume=90.0),
            _bar(3_000, 102.0, oi=400.0, volume=80.0),
        ],
        "ESU26": [
            _bar(1_000, 110.0, oi=300.0, volume=80.0),
            _bar(2_000, 111.0, oi=440.0, volume=89.0),
            _bar(3_000, 112.0, oi=420.0, volume=80.0),
            _bar(4_000, 113.0, oi=430.0, volume=85.0),
        ],
    }

    rolls = detect_rolls(bars)
    assert len(rolls) == 1
    roll = rolls[0]
    assert roll.root == "ES"
    assert roll.roll_ts_ms == 3_000
    assert roll.from_contract == "ESM26"
    assert roll.to_contract == "ESU26"
    assert math.isclose(roll.gap_ratio, 112.0 / 102.0)

    cont = build_ratio_adjusted_continuous(bars, rolls)
    assert [row.ts_ms for row in cont] == [1_000, 2_000, 3_000, 4_000]
    assert all(row.close > 0.0 for row in cont)
    assert [row.roll_flag for row in cont] == [False, False, True, False]
    assert [row.source_contract for row in cont] == ["ESM26", "ESM26", "ESU26", "ESU26"]

    returns = [cont[i].close / cont[i - 1].close - 1.0 for i in range(1, len(cont))]
    assert math.isclose(returns[0], 101.0 / 100.0 - 1.0, rel_tol=1e-12)
    assert math.isclose(returns[1], 102.0 / 101.0 - 1.0, rel_tol=1e-12)
    assert math.isclose(returns[2], 113.0 / 112.0 - 1.0, rel_tol=1e-12)

    raw_switch_return = 112.0 / 101.0 - 1.0
    assert not math.isclose(raw_switch_return, returns[1], rel_tol=1e-6)


def test_compute_roll_yield_sign_and_degenerate_inputs() -> None:
    assert compute_roll_yield(100.0, 110.0, 30.0) < 0.0
    assert compute_roll_yield(110.0, 100.0, 30.0) > 0.0
    assert compute_roll_yield(0.0, 100.0, 30.0) == 0.0
    assert compute_roll_yield(100.0, 0.0, 30.0) == 0.0
    assert compute_roll_yield(100.0, 110.0, 0.0) == 0.0


def test_empty_inputs_never_raise() -> None:
    assert detect_rolls({}) == []
    assert build_ratio_adjusted_continuous({}, []) == []
    assert compute_roll_yield(None, None, None) == 0.0
