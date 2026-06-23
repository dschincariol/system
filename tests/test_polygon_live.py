from __future__ import annotations

import importlib
import json
import os
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


class _FakeResponse:
    def __init__(
        self,
        payload: dict,
        status_code: int = 200,
        url: str = "https://api.polygon.io/mock",
        *,
        content: bytes | None = None,
        forbid_json: bool = False,
    ) -> None:
        self._payload = dict(payload or {})
        self.status_code = int(status_code)
        self.url = str(url)
        self.content = content
        self._forbid_json = bool(forbid_json)

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"http_{self.status_code}:{self.url}")

    def json(self) -> dict:
        if self._forbid_json:
            raise AssertionError("Response.json() must not be used when raw content is available")
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

    def test_batch_snapshot_decodes_response_bytes_with_runtime_json_codec(self) -> None:
        provider = self.mod.PolygonPriceProvider()
        calls: list[tuple[str, dict | None, float | None]] = []
        decode_payload_types: list[str] = []
        original_loads = self.mod._json_loads

        def recording_loads(payload):
            decode_payload_types.append(type(payload).__name__)
            return original_loads(payload)

        def _fake_get(url: str, params=None, timeout=None):
            calls.append((str(url), dict(params or {}), timeout))
            return _FakeResponse(
                {},
                url=str(url),
                content=json.dumps(
                    {
                        "tickers": [
                            {
                                "ticker": "SPY",
                                "updated": 1776260536489356286,
                                "lastTrade": {"p": 451.25, "s": 10, "t": 1776260536489356286},
                                "lastQuote": {"bp": 451.2, "ap": 451.3, "t": 1776260536489356286},
                            }
                        ]
                    },
                    separators=(",", ":"),
                ).encode("utf-8"),
                forbid_json=True,
            )

        self.mod._json_loads = recording_loads
        provider.session.get = _fake_get  # type: ignore[method-assign]
        try:
            out = provider.fetch_last_prices({"SPY": "SPY"})
        finally:
            self.mod._json_loads = original_loads

        self.assertEqual(decode_payload_types, ["bytes"])
        self.assertEqual(len(calls), 1)
        self.assertAlmostEqual(float(out["SPY"]["price"]), 451.25, places=6)
        self.assertAlmostEqual(float(out["SPY"]["spread"]), 0.10, places=6)
        self.assertEqual(float(out["SPY"]["volume"]), 10.0)

    def test_batch_entitlement_failure_does_not_probe_per_symbol_endpoints(self) -> None:
        provider = self.mod.PolygonPriceProvider()
        calls: list[tuple[str, dict | None, float | None]] = []
        warnings: list[tuple[str, str, dict]] = []
        original_warn = self.mod._warn_nonfatal

        def _fake_warn(code: str, error: BaseException, **extra: object) -> None:
            warnings.append((str(code), str(error), dict(extra)))

        def _fake_get(url: str, params=None, timeout=None):
            calls.append((str(url), dict(params or {}), timeout))
            if str(url).endswith("/v2/snapshot/locale/us/markets/stocks/tickers"):
                return _FakeResponse(
                    {"status": "ERROR", "error": "not entitled to this endpoint"},
                    status_code=403,
                    url=str(url),
                )
            raise AssertionError(f"unexpected per-symbol polygon endpoint after entitlement failure: {url}")

        self.mod._warn_nonfatal = _fake_warn
        provider.session.get = _fake_get  # type: ignore[method-assign]
        try:
            out = provider.fetch_last_prices({"SPY": "SPY", "QQQ": "QQQ", "AAPL": "AAPL"})
        finally:
            self.mod._warn_nonfatal = original_warn

        self.assertEqual(out, {})
        self.assertEqual(len(calls), 1)
        self.assertEqual([row[0] for row in warnings], ["POLYGON_LIVE_BATCH_SNAPSHOT_ENTITLEMENT_FAILED"])
        self.assertNotIn("/v2/last/trade/", calls[0][0])
        self.assertNotIn("/v2/last/nbbo/", calls[0][0])

    def test_successful_batch_partial_miss_does_not_repeat_three_requests_per_missing_symbol(self) -> None:
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
                            }
                        ]
                    },
                    url=str(url),
                )
            raise AssertionError(f"unexpected per-symbol polygon endpoint for batch miss: {url}")

        provider.session.get = _fake_get  # type: ignore[method-assign]

        out = provider.fetch_last_prices({"SPY": "SPY", "QQQ": "QQQ", "AAPL": "AAPL"})

        self.assertEqual(set(out.keys()), {"SPY"})
        self.assertEqual(len(calls), 1)
        joined_urls = "\n".join(row[0] for row in calls)
        self.assertNotIn("/v2/last/trade/", joined_urls)
        self.assertNotIn("/v2/last/nbbo/", joined_urls)
        self.assertNotIn("/v2/aggs/ticker/", joined_urls)

    def test_unsupported_batch_uses_single_snapshot_fallback_without_last_trade_nbbo(self) -> None:
        provider = self.mod.PolygonPriceProvider()
        calls: list[tuple[str, dict | None, float | None]] = []

        def _fake_get(url: str, params=None, timeout=None):
            calls.append((str(url), dict(params or {}), timeout))
            if str(url).endswith("/v2/snapshot/locale/us/markets/stocks/tickers"):
                return _FakeResponse({"status": "ERROR", "error": "not found"}, status_code=404, url=str(url))
            if str(url).endswith("/v2/snapshot/locale/us/markets/stocks/tickers/SPY"):
                return _FakeResponse(
                    {
                        "ticker": {
                            "ticker": "SPY",
                            "updated": 1776260536489356286,
                            "min": {"c": 696.175, "v": 157291, "t": 1776260460000},
                            "lastQuote": {"p": 696.17, "P": 696.18, "t": 1776260536489356286},
                        }
                    },
                    url=str(url),
                )
            raise AssertionError(f"unexpected polygon endpoint for unsupported batch fallback: {url}")

        provider.session.get = _fake_get  # type: ignore[method-assign]

        out = provider.fetch_last_prices({"SPY": "SPY"})

        self.assertEqual(set(out.keys()), {"SPY"})
        self.assertAlmostEqual(float(out["SPY"]["price"]), 696.175, places=6)
        self.assertAlmostEqual(float(out["SPY"]["spread"]), 0.01, places=6)
        self.assertEqual(len(calls), 2)
        joined_urls = "\n".join(row[0] for row in calls)
        self.assertIn("/v2/snapshot/locale/us/markets/stocks/tickers/SPY", joined_urls)
        self.assertNotIn("/v2/last/trade/", joined_urls)
        self.assertNotIn("/v2/last/nbbo/", joined_urls)


if __name__ == "__main__":
    unittest.main()
