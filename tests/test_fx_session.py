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

    def test_boundaries_use_new_york_clock_in_standard_and_daylight_time(self) -> None:
        # January standard time: 17:00 ET == 22:00 UTC.
        self.assertEqual(self.fx_session.fx_session_state("EURUSD", _utc_ms(2026, 1, 2, 21, 59))["session"], "open")
        self.assertEqual(
            self.fx_session.fx_session_state("EURUSD", _utc_ms(2026, 1, 2, 22))["session"],
            "weekend_closed",
        )
        self.assertEqual(
            self.fx_session.fx_session_state("EURUSD", _utc_ms(2026, 1, 4, 21, 59))["session"],
            "weekend_closed",
        )
        self.assertTrue(bool(self.fx_session.fx_session_state("EURUSD", _utc_ms(2026, 1, 4, 22))["is_open"]))

        # June daylight time: 17:00 ET == 21:00 UTC.
        self.assertEqual(self.fx_session.fx_session_state("EURUSD", _utc_ms(2026, 6, 26, 20, 59))["session"], "open")
        self.assertEqual(
            self.fx_session.fx_session_state("EURUSD", _utc_ms(2026, 6, 26, 21))["session"],
            "weekend_closed",
        )
        self.assertEqual(
            self.fx_session.fx_session_state("EURUSD", _utc_ms(2026, 6, 28, 20, 59))["session"],
            "weekend_closed",
        )
        self.assertTrue(bool(self.fx_session.fx_session_state("EURUSD", _utc_ms(2026, 6, 28, 21))["is_open"]))

    def test_no_fixed_utc_fallback_clock_remains(self) -> None:
        source = Path(self.fx_session.__file__).read_text(encoding="utf-8")

        self.assertNotIn("FX_WEEK_OPEN_HOUR_UTC", source)
        self.assertNotIn("FX_WEEK_CLOSE_HOUR_UTC", source)
        self.assertNotIn("_fallback_closed_utc", source)
        self.assertNotIn("_fallback_next_open_utc", source)

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
