from __future__ import annotations

import importlib
import os
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


NY = ZoneInfo("America/New_York")


def _ms_et(year: int, month: int, day: int, hour: int, minute: int = 0) -> int:
    return int(datetime(year, month, day, hour, minute, tzinfo=NY).timestamp() * 1000)


def _dt_utc(ts_ms: int) -> datetime:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)


class FxClockTests(unittest.TestCase):
    def setUp(self) -> None:
        for name in (
            "FX_WEEK_CLOSE_DAY_ET",
            "FX_WEEK_OPEN_DAY_ET",
            "FX_WEEK_CLOSE_HOUR_ET",
            "FX_WEEK_OPEN_HOUR_ET",
        ):
            os.environ.pop(name, None)
        self.fx_clock = importlib.reload(importlib.import_module("engine.data.prices.fx_clock"))

    def test_market_closed_boundaries(self) -> None:
        self.assertFalse(self.fx_clock.fx_market_closed(_ms_et(2026, 1, 9, 16, 59)))
        self.assertTrue(self.fx_clock.fx_market_closed(_ms_et(2026, 1, 9, 17, 0)))
        self.assertTrue(self.fx_clock.fx_market_closed(_ms_et(2026, 1, 10, 12, 0)))
        self.assertTrue(self.fx_clock.fx_market_closed(_ms_et(2026, 1, 11, 16, 59)))
        self.assertFalse(self.fx_clock.fx_market_closed(_ms_et(2026, 1, 11, 17, 0)))

    def test_forward_eval_counts_open_market_time(self) -> None:
        start = _ms_et(2026, 1, 9, 16, 30)
        target = self.fx_clock.fx_forward_eval_ms(start, 60 * 60 * 1000)
        self.assertEqual(target, _ms_et(2026, 1, 11, 17, 30))

    def test_window_gap_detection(self) -> None:
        self.assertTrue(
            self.fx_clock.fx_window_spans_closed_gap(
                _ms_et(2026, 1, 9, 16, 30),
                _ms_et(2026, 1, 9, 17, 30),
            )
        )
        self.assertFalse(
            self.fx_clock.fx_window_spans_closed_gap(
                _ms_et(2026, 1, 7, 10, 0),
                _ms_et(2026, 1, 7, 11, 0),
            )
        )

    def test_env_override_boundaries(self) -> None:
        os.environ["FX_WEEK_CLOSE_HOUR_ET"] = "16"
        os.environ["FX_WEEK_OPEN_HOUR_ET"] = "18"
        self.addCleanup(os.environ.pop, "FX_WEEK_CLOSE_HOUR_ET", None)
        self.addCleanup(os.environ.pop, "FX_WEEK_OPEN_HOUR_ET", None)
        self.assertTrue(self.fx_clock.fx_market_closed(_ms_et(2026, 1, 9, 16, 0)))
        self.assertTrue(self.fx_clock.fx_market_closed(_ms_et(2026, 1, 11, 17, 30)))
        self.assertFalse(self.fx_clock.fx_market_closed(_ms_et(2026, 1, 11, 18, 0)))

    def test_uses_real_new_york_dst_offsets(self) -> None:
        standard_close = _ms_et(2026, 1, 9, 17, 0)
        dst_close = _ms_et(2026, 3, 13, 17, 0)
        dst_reopen = _ms_et(2026, 3, 8, 17, 0)
        self.assertEqual(_dt_utc(standard_close).hour, 22)
        self.assertEqual(_dt_utc(dst_close).hour, 21)
        self.assertEqual(_dt_utc(dst_reopen).hour, 21)
        self.assertFalse(self.fx_clock.fx_market_closed(dst_reopen))


if __name__ == "__main__":
    unittest.main()
