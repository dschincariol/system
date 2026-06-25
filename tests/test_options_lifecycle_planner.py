from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from engine.execution.options_lifecycle import plan_option_lifecycle_events


def _ms(year: int, month: int, day: int) -> int:
    return int(datetime(year, month, day, tzinfo=timezone.utc).timestamp() * 1000)


@dataclass(frozen=True)
class _Meta:
    occ_symbol: str
    underlying: str
    expiry: str
    right: str
    strike: float
    multiplier: float = 100.0
    settlement: str = "physical"


class OptionsLifecyclePlannerTest(unittest.TestCase):
    def test_itm_call_expiry_emits_exercise_with_exact_intrinsic(self) -> None:
        meta = _Meta("SPY270115C00500000", "SPY", "2027-01-15", "C", 500.0)

        events = plan_option_lifecycle_events(
            [("SPY270115C00500000", 2.0, 4.0)],
            underlying_prices={"SPY": 512.0},
            now_ms=_ms(2027, 1, 16),
            metadata_for=lambda _symbol: meta,
            env={"OPTIONS_PIN_RISK_BAND_ABS": "0"},
        )

        self.assertEqual([event.event_type for event in events], ["EXERCISE"])
        self.assertAlmostEqual(events[0].intrinsic_per_contract, 12.0)
        self.assertAlmostEqual(events[0].intrinsic_value, 2400.0)

    def test_itm_cash_settled_call_expiry_emits_cash_settle(self) -> None:
        meta = _Meta("SPX270115C05000000", "SPX", "2027-01-15", "C", 5000.0, settlement="cash")

        events = plan_option_lifecycle_events(
            [("SPX270115C05000000", 1.0, 40.0)],
            underlying_prices={"SPX": 5033.0},
            now_ms=_ms(2027, 1, 16),
            metadata_for=lambda _symbol: meta,
            env={"OPTIONS_PIN_RISK_BAND_ABS": "0"},
        )

        self.assertEqual([event.event_type for event in events], ["CASH_SETTLE"])
        self.assertEqual(events[0].settlement, "CASH")
        self.assertAlmostEqual(events[0].intrinsic_per_contract, 33.0)
        self.assertAlmostEqual(events[0].intrinsic_value, 3300.0)

    def test_otm_put_expiry_emits_expire_worthless(self) -> None:
        meta = _Meta("SPY270115P00500000", "SPY", "2027-01-15", "P", 500.0)

        events = plan_option_lifecycle_events(
            [("SPY270115P00500000", 1.0, 2.0)],
            underlying_prices={"SPY": 505.0},
            now_ms=_ms(2027, 1, 16),
            metadata_for=lambda _symbol: meta,
            env={"OPTIONS_PIN_RISK_BAND_ABS": "0"},
        )

        self.assertEqual([event.event_type for event in events], ["EXPIRE_WORTHLESS"])
        self.assertEqual(events[0].intrinsic_value, 0.0)

    def test_min_dte_emits_autoclose(self) -> None:
        meta = _Meta("SPY270115C00500000", "SPY", "2027-01-15", "C", 500.0)

        events = plan_option_lifecycle_events(
            [("SPY270115C00500000", 1.0, 5.0)],
            underlying_prices={},
            now_ms=_ms(2027, 1, 14),
            metadata_for=lambda _symbol: meta,
            env={"OPTIONS_MIN_DTE_DAYS": "2", "OPTIONS_LIFECYCLE_MODE": "shadow"},
        )

        self.assertEqual([event.event_type for event in events], ["DTE_AUTOCLOSE"])
        self.assertEqual(events[0].reason, "dte_below_min_autoclose")

    def test_roll_mode_emits_roll_close_only_warning_when_target_missing(self) -> None:
        meta = _Meta("SPY270115C00500000", "SPY", "2027-01-15", "C", 500.0)

        events = plan_option_lifecycle_events(
            [("SPY270115C00500000", 1.0, 5.0)],
            underlying_prices={},
            now_ms=_ms(2027, 1, 14),
            metadata_for=lambda _symbol: meta,
            env={
                "OPTIONS_MIN_DTE_DAYS": "2",
                "OPTIONS_LIFECYCLE_MODE": "roll",
                "OPTIONS_LIFECYCLE_ROLL_TARGET_DTE": "30",
            },
        )

        self.assertEqual([event.event_type for event in events], ["DTE_ROLL"])
        self.assertEqual(events[0].warning, "roll_target_unavailable_close_only")

    def test_pin_risk_band_emits_pin_risk(self) -> None:
        meta = _Meta("SPY270115C00500000", "SPY", "2027-01-15", "C", 500.0)

        events = plan_option_lifecycle_events(
            [("SPY270115C00500000", 1.0, 5.0)],
            underlying_prices={"SPY": 500.25},
            now_ms=_ms(2027, 1, 16),
            metadata_for=lambda _symbol: meta,
            env={"OPTIONS_PIN_RISK_BAND_ABS": "1.0"},
        )

        self.assertEqual([event.event_type for event in events], ["PIN_RISK"])
        self.assertAlmostEqual(events[0].details["distance_to_strike"], 0.25)

    def test_missing_metadata_and_garbage_inputs_do_not_raise_or_mutate(self) -> None:
        positions = [
            ("BAD", 1.0, 1.0),
            {"symbol": "ALSO_BAD", "qty": "not-a-number", "avg_px": object()},
            object(),
        ]
        before = [positions[0], dict(positions[1])]

        events = plan_option_lifecycle_events(
            positions,
            underlying_prices={"SPY": 500.0},
            now_ms=_ms(2027, 1, 16),
            metadata_for=lambda _symbol: None,
            env={"OPTIONS_PIN_RISK_BAND_ABS": "1.0"},
        )

        self.assertEqual(events, [])
        self.assertEqual(positions[:2], before)


if __name__ == "__main__":
    unittest.main()
