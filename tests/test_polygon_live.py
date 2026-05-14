from __future__ import annotations

import importlib
import os
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


class _FakeResponse:
    def __init__(self, payload: dict, status_code: int = 200, url: str = "https://api.polygon.io/mock") -> None:
        self._payload = dict(payload or {})
        self.status_code = int(status_code)
        self.url = str(url)

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"http_{self.status_code}:{self.url}")

    def json(self) -> dict:
        return dict(self._payload)


class PolygonLiveTests(unittest.TestCase):
    def setUp(self) -> None:
        self._env = dict(os.environ)
        os.environ["POLYGON_API_KEY"] = "test-key"
        os.environ["POLYGON_SNAPSHOT_BATCH_SIZE"] = "100"
        self.mod = importlib.import_module("engine.data.live_prices.polygon_live")
        self.mod = importlib.reload(self.mod)

    def tearDown(self) -> None:
        os.environ.clear()
        os.environ.update(self._env)

    def test_fetch_last_prices_prefers_batch_snapshot_endpoint(self) -> None:
        provider = self.mod.PolygonPriceProvider()
        calls: list[tuple[str, dict | None, float | None]] = []

        def _fake_get(url: str, params=None, timeout=None):
            calls.append((str(url), dict(params or {}), timeout))
            if str(url).endswith("/v2/snapshot/locale/us/markets/stocks/tickers"):
                return _FakeResponse(
                    {
                        "tickers": [
                            {
                                "ticker": "SPY",
                                "updated": 1776260536489356286,
                                "min": {"c": 696.175, "v": 157291, "t": 1776260460000},
                                "day": {"c": 696.165, "v": 3882954},
                                "prevDay": {"c": 694.46, "v": 63480529},
                            },
                            {
                                "ticker": "QQQ",
                                "updated": 1776260536489356286,
                                "min": {"c": 601.25, "v": 81234, "t": 1776260460000},
                                "day": {"c": 601.2, "v": 1322333},
                                "prevDay": {"c": 599.77, "v": 18233445},
                            },
                            {
                                "ticker": "AAPL",
                                "updated": 1776260535472575444,
                                "min": {"c": 259.69, "v": 123885, "t": 1776260460000},
                                "day": {"c": 259.69, "v": 3192156},
                                "prevDay": {"c": 258.83, "v": 48370710},
                            },
                        ]
                    },
                    url=str(url),
                )
            raise AssertionError(f"unexpected polygon endpoint: {url}")

        provider.session.get = _fake_get  # type: ignore[method-assign]

        out = provider.fetch_last_prices({"SPY": "SPY", "QQQ": "QQQ", "AAPL": "AAPL"})

        self.assertEqual(set(out.keys()), {"SPY", "QQQ", "AAPL"})
        self.assertAlmostEqual(float(out["SPY"]["price"]), 696.175, places=6)
        self.assertEqual(str(out["SPY"]["source"]), "polygon_snapshot")
        self.assertEqual(len(calls), 1)
        self.assertTrue(calls[0][0].endswith("/v2/snapshot/locale/us/markets/stocks/tickers"))
        self.assertEqual(str((calls[0][1] or {}).get("tickers") or ""), "AAPL,QQQ,SPY")


if __name__ == "__main__":
    unittest.main()
