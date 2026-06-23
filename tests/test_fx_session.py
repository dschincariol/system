from __future__ import annotations

import importlib
import os
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

pytestmark = pytest.mark.safety_critical


def _utc_ms(year: int, month: int, day: int, hour: int, minute: int = 0) -> int:
    return int(datetime(year, month, day, hour, minute, tzinfo=timezone.utc).timestamp() * 1000)


class FxSessionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fx_session = importlib.reload(importlib.import_module("engine.execution.fx_session"))

    def test_weekday_open_weekend_closed_and_rollover(self) -> None:
        open_state = self.fx_session.fx_session_state("EURUSD", _utc_ms(2026, 6, 24, 16))
        weekend_state = self.fx_session.fx_session_state("EURUSD", _utc_ms(2026, 6, 27, 16))
        rollover_state = self.fx_session.fx_session_state("EURUSD", _utc_ms(2026, 6, 24, 21, 30))

        self.assertEqual(open_state["session"], "open")
        self.assertTrue(bool(open_state["is_open"]))
        self.assertEqual(weekend_state["session"], "weekend_closed")
        self.assertFalse(bool(weekend_state["is_open"]))
        self.assertIsNotNone(weekend_state["next_open_ms"])
        self.assertEqual(rollover_state["session"], "rollover")
        self.assertTrue(bool(rollover_state["in_rollover_window"]))

    def test_dst_boundaries_use_new_york_clock_not_fixed_utc(self) -> None:
        # 2026-03-13 is in US daylight time: Friday 17:00 ET == 21:00 UTC.
        self.assertTrue(bool(self.fx_session.fx_session_state("EURUSD", _utc_ms(2026, 3, 13, 20, 59))["is_open"]))
        self.assertEqual(
            self.fx_session.fx_session_state("EURUSD", _utc_ms(2026, 3, 13, 21))["session"],
            "weekend_closed",
        )
        # 2026-01-02 is standard time: Friday 17:00 ET == 22:00 UTC.
        self.assertEqual(self.fx_session.fx_session_state("EURUSD", _utc_ms(2026, 1, 2, 21, 59))["session"], "open")
        self.assertEqual(
            self.fx_session.fx_session_state("EURUSD", _utc_ms(2026, 1, 2, 22))["session"],
            "weekend_closed",
        )

    def test_env_rollover_override_and_purity(self) -> None:
        with patch.dict(
            os.environ,
            {
                "FX_ROLLOVER_HOUR_ET": "16",
                "FX_ROLLOVER_START_MINUTE_ET": "0",
                "FX_ROLLOVER_DURATION_MINUTES": "30",
            },
            clear=False,
        ):
            ts = _utc_ms(2026, 6, 24, 20, 15)
            first = self.fx_session.fx_session_state("EURUSD", ts)
            second = self.fx_session.fx_session_state("EURUSD", ts)

        self.assertEqual(first, second)
        self.assertEqual(first["session"], "rollover")
        self.assertTrue(bool(first["in_rollover_window"]))

    def test_non_fx_symbols_are_pass_through_open(self) -> None:
        state = self.fx_session.fx_session_state("AAPL", _utc_ms(2026, 6, 27, 16))

        self.assertFalse(bool(state["is_fx"]))
        self.assertTrue(bool(state["is_open"]))
        self.assertEqual(state["session"], "open")
