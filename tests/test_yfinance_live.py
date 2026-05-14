from __future__ import annotations

import importlib
import os
import time
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


class YFinanceLiveTests(unittest.TestCase):
    def setUp(self) -> None:
        self._env = dict(os.environ)
        self.mod = importlib.import_module("engine.data.live_prices.yfinance_live")
        self.mod = importlib.reload(self.mod)
        self.mod._YFINANCE_BATCH_CURSOR = 0

    def tearDown(self) -> None:
        os.environ.clear()
        os.environ.update(self._env)

    def test_fetch_last_prices_works_without_yfinance_package_for_chart_path(self) -> None:
        chart = {
            "meta": {"regularMarketPrice": 123.45},
            "indicators": {"quote": [{"close": [123.45], "volume": [1000]}]},
        }

        with patch.object(self.mod, "yf", None):
            with patch.object(self.mod, "_fetch_chart_json", return_value=chart):
                out = self.mod.fetch_last_prices_yf({"SPY": "SPY"})

        self.assertIn("SPY", out)
        self.assertAlmostEqual(float(out["SPY"]["price"]), 123.45, places=6)
        self.assertEqual(str(out["SPY"]["source"]), "yfinance")

    def test_fetch_last_prices_limits_large_batches_and_rotates_symbols(self) -> None:
        os.environ["YFINANCE_LIVE_BATCH_SIZE"] = "2"
        os.environ["YFINANCE_LIVE_MAX_WORKERS"] = "1"
        os.environ["YFINANCE_LIVE_PRIORITY_TICKERS"] = ""

        def _fake_chart(symbol: str, *, interval: str, range_: str, deadline_monotonic: float | None = None):
            return {
                "meta": {"regularMarketPrice": float(len(symbol))},
                "indicators": {"quote": [{"close": [float(len(symbol))], "volume": [10]}]},
            }

        ticker_map = {"AAA": "AAA", "BBBB": "BBBB", "CCCCC": "CCCCC"}

        with patch.object(self.mod, "yf", None):
            with patch.object(self.mod, "_fetch_chart_json", side_effect=_fake_chart):
                out1 = self.mod.fetch_last_prices_yf(ticker_map)
                out2 = self.mod.fetch_last_prices_yf(ticker_map)

        self.assertLessEqual(len(out1), 2)
        self.assertLessEqual(len(out2), 2)
        self.assertNotEqual(set(out1.keys()), set(out2.keys()))
        self.assertEqual(set(out1.keys()) | set(out2.keys()), {"AAA", "BBBB", "CCCCC"})

    def test_fetch_last_prices_returns_partial_results_when_batch_deadline_expires(self) -> None:
        os.environ["YFINANCE_LIVE_BATCH_SIZE"] = "2"
        os.environ["YFINANCE_LIVE_MAX_WORKERS"] = "2"
        os.environ["YFINANCE_LIVE_BATCH_TIMEOUT_S"] = "0.25"
        os.environ["YFINANCE_LIVE_PRIORITY_TICKERS"] = ""

        def _fake_fetch(sym: str, ticker_symbol: str, now_ms: int, deadline_monotonic: float | None = None):
            if sym == "SLOW":
                time.sleep(0.6)
            else:
                time.sleep(0.005)
            return sym, {"ts_ms": int(now_ms), "price": float(len(ticker_symbol)), "source": "yfinance"}

        started = time.perf_counter()
        with patch.object(self.mod, "_fetch_symbol_last_price", side_effect=_fake_fetch):
            out = self.mod.fetch_last_prices_yf({"FAST": "FAST", "SLOW": "SLOW"})
        elapsed = time.perf_counter() - started

        self.assertIn("FAST", out)
        self.assertNotIn("SLOW", out)
        self.assertLess(elapsed, 0.45)


if __name__ == "__main__":
    unittest.main()
