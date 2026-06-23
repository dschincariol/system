from __future__ import annotations

import importlib
import os
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


class FakeNetworkError(Exception):
    pass


class FakeRateLimitExceeded(Exception):
    pass


class FakeAuthenticationError(Exception):
    pass


class FakeBadSymbol(Exception):
    pass


def _fake_ccxt(exchange_cls):
    return SimpleNamespace(
        binance=exchange_cls,
        NetworkError=FakeNetworkError,
        RequestTimeout=FakeNetworkError,
        ExchangeNotAvailable=FakeNetworkError,
        DDoSProtection=FakeNetworkError,
        RateLimitExceeded=FakeRateLimitExceeded,
        AuthenticationError=FakeAuthenticationError,
        PermissionDenied=FakeAuthenticationError,
        AccountSuspended=FakeAuthenticationError,
        BadSymbol=FakeBadSymbol,
    )


class CCXTLiveTests(unittest.TestCase):
    def setUp(self) -> None:
        self.metrics_env = patch.dict(os.environ, {"RUNTIME_METRICS_ENABLED": "0"})
        self.metrics_env.start()
        self.ccxt_env = patch.dict(
            os.environ,
            {
                "CCXT_API_KEY": "",
                "CCXT_OPTIONS_JSON": "",
                "CCXT_PASSWORD": "",
                "CCXT_SANDBOX": "",
                "CCXT_SECRET": "",
                "CCXT_TIMEOUT_MS": "",
            },
        )
        self.ccxt_env.start()
        self.mod = importlib.import_module("engine.data.live_prices.ccxt_live")
        self.mod = importlib.reload(self.mod)
        self.mod._WARNED_NONFATAL_KEYS.clear()
        self.mod._clear_exchange_cache_for_tests()

    def tearDown(self) -> None:
        self.mod._clear_exchange_cache_for_tests()
        self.ccxt_env.stop()
        self.metrics_env.stop()

    def _captured_metric_values(self, metrics, name: str) -> list[object]:
        return [value for metric, value, _kwargs in metrics if metric == name]

    def _captured_cycle_paths(self, metrics) -> list[str]:
        return [
            str(kwargs.get("extra_tags", {}).get("path"))
            for metric, _value, kwargs in metrics
            if metric == "ccxt_live_fetch_cycles"
        ]

    def _captured_metric_tags(self, metrics, name: str) -> list[dict]:
        return [dict(kwargs.get("extra_tags", {})) for metric, _value, kwargs in metrics if metric == name]

    def test_fetch_last_prices_reuses_exchange_and_prefers_fetch_tickers(self) -> None:
        class BatchExchange:
            instances = []

            def __init__(self, config):
                self.config = dict(config)
                self.has = {"fetchTickers": True}
                self.load_markets_calls = 0
                self.fetch_tickers_calls = []
                self.fetch_ticker_calls = []
                self.closed = False
                self.__class__.instances.append(self)

            def load_markets(self):
                self.load_markets_calls += 1

            def fetch_tickers(self, markets):
                self.fetch_tickers_calls.append(list(markets))
                return {
                    "BTC/USDT": {"symbol": "BTC/USDT", "last": 100.0, "bid": 99.5, "ask": 100.5, "baseVolume": 7},
                    "ETH/USDT": {"symbol": "ETH/USDT", "last": 200.0, "bid": 199.0, "ask": 201.0, "baseVolume": 11},
                }

            def fetch_ticker(self, market):
                self.fetch_ticker_calls.append(str(market))
                raise AssertionError("fetch_ticker should not be used when batch data is complete")

            def close(self):
                self.closed = True

        self.mod.ccxt = _fake_ccxt(BatchExchange)
        metrics = []
        timings = []

        with patch.object(
            self.mod,
            "emit_counter",
            side_effect=lambda metric, value=1, **kwargs: metrics.append((metric, value, kwargs)),
        ):
            with patch.object(
                self.mod,
                "emit_timing",
                side_effect=lambda metric, value, **kwargs: timings.append((metric, value, kwargs)),
            ):
                out1 = self.mod.fetch_last_prices_ccxt(" Binance ", {"BTC": "BTC/USDT", "ETH": "ETH/USDT"})
                out2 = self.mod.fetch_last_prices_ccxt("binance", {"BTC": "BTC/USDT", "ETH": "ETH/USDT"})

        self.assertEqual(len(BatchExchange.instances), 1)
        exchange = BatchExchange.instances[0]
        self.assertEqual(exchange.config, {"enableRateLimit": True})
        self.assertEqual(exchange.load_markets_calls, 1)
        self.assertEqual(exchange.fetch_tickers_calls, [["BTC/USDT", "ETH/USDT"], ["BTC/USDT", "ETH/USDT"]])
        self.assertEqual(exchange.fetch_ticker_calls, [])
        self.assertEqual(set(out1), {"BTC", "ETH"})
        self.assertEqual(set(out2), {"BTC", "ETH"})
        self.assertEqual(
            set(out1["BTC"]),
            {"ts_ms", "price", "bid", "ask", "spread", "volume", "source"},
        )
        self.assertEqual(out1["BTC"]["source"], "ccxt")
        self.assertAlmostEqual(float(out1["BTC"]["spread"]), 1.0)
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_exchange_cache_misses"), [1])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_exchange_cache_hits"), [1])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_markets_loads"), [1])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_markets_reuses"), [1])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_market_cache_reloads"), [1])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_market_cache_hits"), [1])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fetch_tickers_attempts"), [1, 1])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fetch_tickers_successes"), [1, 1])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fetch_tickers_failures"), [])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fetch_tickers_calls"), [1, 1])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fetch_tickers_markets"), [2, 2])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fetch_tickers_rows"), [2, 2])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fallback_fetches"), [])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fetch_tickers_partial_misses"), [])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_failed_symbols"), [])
        self.assertEqual(self._captured_cycle_paths(metrics), ["batch_only", "batch_only"])
        self.assertEqual(
            [metric for metric, _value, _kwargs in timings],
            [
                "ccxt_live_markets_load_latency_ms",
                "ccxt_live_fetch_tickers_latency_ms",
                "ccxt_live_fetch_tickers_latency_ms",
            ],
        )
        self.assertTrue(all(float(value) >= 0.0 for _metric, value, _kwargs in timings))
        for metric, _value, kwargs in metrics:
            if str(metric).startswith("ccxt_live_"):
                self.assertEqual(kwargs.get("component"), "engine.data.live_prices.ccxt_live")
                self.assertEqual(kwargs.get("provider"), "ccxt")
                extra_tags = kwargs.get("extra_tags", {})
                self.assertEqual(extra_tags.get("exchange"), "binance")
                self.assertEqual(extra_tags.get("exchange_id"), "binance")
                if "path" in extra_tags:
                    self.assertEqual(extra_tags.get("supports_batch"), "1")

    def test_fetch_last_prices_falls_back_for_missing_batch_rows(self) -> None:
        class PartialBatchExchange:
            instances = []

            def __init__(self, _config):
                self.has = {"fetchTickers": True}
                self.fetch_tickers_calls = []
                self.fetch_ticker_calls = []
                self.__class__.instances.append(self)

            def load_markets(self):
                return None

            def fetch_tickers(self, markets):
                self.fetch_tickers_calls.append(list(markets))
                return {
                    "BTC/USDT": {"symbol": "BTC/USDT", "last": 100.0, "bid": 99.0, "ask": 101.0, "baseVolume": 5}
                }

            def fetch_ticker(self, market):
                self.fetch_ticker_calls.append(str(market))
                return {"symbol": str(market), "last": 200.0, "bid": 198.0, "ask": 202.0, "baseVolume": 9}

        self.mod.ccxt = _fake_ccxt(PartialBatchExchange)
        metrics = []

        with patch.object(
            self.mod,
            "emit_counter",
            side_effect=lambda metric, value=1, **kwargs: metrics.append((metric, value, kwargs)),
        ):
            out = self.mod.fetch_last_prices_ccxt("binance", {"BTC": "BTC/USDT", "ETH": "ETH/USDT"})

        exchange = PartialBatchExchange.instances[0]
        self.assertEqual(exchange.fetch_tickers_calls, [["BTC/USDT", "ETH/USDT"]])
        self.assertEqual(exchange.fetch_ticker_calls, ["ETH/USDT"])
        self.assertEqual(set(out), {"BTC", "ETH"})
        self.assertAlmostEqual(float(out["ETH"]["price"]), 200.0)
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fetch_tickers_calls"), [1])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fetch_tickers_attempts"), [1])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fetch_tickers_successes"), [1])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fetch_tickers_failures"), [])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fetch_tickers_markets"), [2])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fetch_tickers_rows"), [1])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fetch_tickers_partial_misses"), [1])
        self.assertEqual(
            [tags.get("reason") for tags in self._captured_metric_tags(metrics, "ccxt_live_fetch_tickers_partial_misses")],
            ["missing_symbol"],
        )
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fetch_tickers_missing_symbols"), [1])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fallback_fetches"), [1])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fallback_rows"), [1])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_failed_symbols"), [])
        self.assertEqual(self._captured_cycle_paths(metrics), ["batch_with_fallback"])

    def test_fetch_last_prices_falls_back_for_invalid_batch_rows(self) -> None:
        class InvalidBatchExchange:
            instances = []

            def __init__(self, _config):
                self.has = {"fetchTickers": True}
                self.fetch_tickers_calls = []
                self.fetch_ticker_calls = []
                self.__class__.instances.append(self)

            def load_markets(self):
                return None

            def fetch_tickers(self, markets):
                self.fetch_tickers_calls.append(list(markets))
                return {
                    "BTC/USDT": {"symbol": "BTC/USDT", "last": None, "bid": 99.0, "ask": 101.0},
                    "ETH/USDT": {"symbol": "ETH/USDT", "last": 200.0, "bid": 198.0, "ask": 202.0},
                }

            def fetch_ticker(self, market):
                self.fetch_ticker_calls.append(str(market))
                return {"symbol": str(market), "last": 101.0, "bid": 100.0, "ask": 102.0, "baseVolume": 3}

        self.mod.ccxt = _fake_ccxt(InvalidBatchExchange)
        metrics = []

        with patch.object(
            self.mod,
            "emit_counter",
            side_effect=lambda metric, value=1, **kwargs: metrics.append((metric, value, kwargs)),
        ):
            out = self.mod.fetch_last_prices_ccxt("binance", {"BTC": "BTC/USDT", "ETH": "ETH/USDT"})

        exchange = InvalidBatchExchange.instances[0]
        self.assertEqual(exchange.fetch_tickers_calls, [["BTC/USDT", "ETH/USDT"]])
        self.assertEqual(exchange.fetch_ticker_calls, ["BTC/USDT"])
        self.assertEqual(set(out), {"BTC", "ETH"})
        self.assertAlmostEqual(float(out["BTC"]["price"]), 101.0)
        self.assertAlmostEqual(float(out["ETH"]["price"]), 200.0)
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fetch_tickers_rows"), [1])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fetch_tickers_partial_misses"), [1])
        self.assertEqual(
            [tags.get("reason") for tags in self._captured_metric_tags(metrics, "ccxt_live_fetch_tickers_partial_misses")],
            ["invalid_row"],
        )
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fetch_tickers_missing_symbols"), [])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fallback_fetches"), [1])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fallback_rows"), [1])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_failed_symbols"), [])
        self.assertEqual(self._captured_cycle_paths(metrics), ["batch_with_fallback"])

    def test_fetch_last_prices_uses_per_symbol_fallback_when_batch_unsupported(self) -> None:
        class SingleTickerExchange:
            instances = []

            def __init__(self, _config):
                self.has = {"fetchTickers": False}
                self.load_markets_calls = 0
                self.fetch_ticker_calls = []
                self.__class__.instances.append(self)

            def load_markets(self):
                self.load_markets_calls += 1

            def fetch_ticker(self, market):
                self.fetch_ticker_calls.append(str(market))
                return {"symbol": str(market), "last": 10.0, "bid": 9.0, "ask": 11.0, "baseVolume": 1}

        self.mod.ccxt = _fake_ccxt(SingleTickerExchange)
        metrics = []

        with patch.object(
            self.mod,
            "emit_counter",
            side_effect=lambda metric, value=1, **kwargs: metrics.append((metric, value, kwargs)),
        ):
            self.mod.fetch_last_prices_ccxt("binance", {"BTC": "BTC/USDT", "ETH": "ETH/USDT"})
            self.mod.fetch_last_prices_ccxt("binance", {"BTC": "BTC/USDT", "ETH": "ETH/USDT"})

        self.assertEqual(len(SingleTickerExchange.instances), 1)
        exchange = SingleTickerExchange.instances[0]
        self.assertEqual(exchange.load_markets_calls, 1)
        self.assertEqual(exchange.fetch_ticker_calls, ["BTC/USDT", "ETH/USDT", "BTC/USDT", "ETH/USDT"])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_exchange_cache_misses"), [1])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_exchange_cache_hits"), [1])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_markets_loads"), [1])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_markets_reuses"), [1])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fetch_tickers_calls"), [])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fetch_tickers_attempts"), [])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fetch_tickers_successes"), [])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fetch_tickers_failures"), [])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fetch_tickers_unsupported"), [1, 1])
        self.assertEqual(
            [tags.get("reason") for tags in self._captured_metric_tags(metrics, "ccxt_live_fetch_tickers_unsupported")],
            ["unsupported", "unsupported"],
        )
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fallback_fetches"), [2, 2])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fallback_rows"), [2, 2])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_failed_symbols"), [])
        self.assertEqual(self._captured_cycle_paths(metrics), ["fallback_only", "fallback_only"])
        cycle_tags = [
            kwargs.get("extra_tags", {})
            for metric, _value, kwargs in metrics
            if metric == "ccxt_live_fetch_cycles"
        ]
        self.assertEqual([tags.get("supports_batch") for tags in cycle_tags], ["0", "0"])

    def test_fetch_last_prices_fallback_failure_preserves_partial_good_results_and_counts_failed_symbols(self) -> None:
        class PartiallyFailingFallbackExchange:
            def __init__(self, _config):
                self.has = {"fetchTickers": False}
                self.fetch_ticker_calls = []

            def load_markets(self):
                return None

            def fetch_ticker(self, market):
                self.fetch_ticker_calls.append(str(market))
                if str(market) == "BAD/USDT":
                    raise ValueError("bad market")
                return {"symbol": str(market), "last": 10.0, "bid": 9.0, "ask": 11.0, "baseVolume": 1}

        self.mod.ccxt = _fake_ccxt(PartiallyFailingFallbackExchange)
        metrics = []

        with patch.object(
            self.mod,
            "emit_counter",
            side_effect=lambda metric, value=1, **kwargs: metrics.append((metric, value, kwargs)),
        ):
            out = self.mod.fetch_last_prices_ccxt(
                "binance",
                {"BTC": "BTC/USDT", "BAD": "BAD/USDT", "ETH": "ETH/USDT"},
            )

        self.assertEqual(set(out), {"BTC", "ETH"})
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fallback_fetches"), [3])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fallback_rows"), [2])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_failed_symbols"), [1])
        self.assertEqual(self._captured_cycle_paths(metrics), ["fallback_only"])

    def test_fetch_last_prices_strips_symbols_and_markets_before_exchange_calls(self) -> None:
        class NormalizingExchange:
            instances = []

            def __init__(self, _config):
                self.has = {"fetchTickers": True}
                self.fetch_tickers_calls = []
                self.__class__.instances.append(self)

            def load_markets(self):
                return None

            def fetch_tickers(self, markets):
                self.fetch_tickers_calls.append(list(markets))
                return {
                    "BTC/USDT": {"symbol": "BTC/USDT", "last": 100.0, "bid": 99.0, "ask": 101.0, "baseVolume": 5}
                }

        self.mod.ccxt = _fake_ccxt(NormalizingExchange)

        out = self.mod.fetch_last_prices_ccxt("binance", {" BTC ": " BTC/USDT "})

        exchange = NormalizingExchange.instances[0]
        self.assertEqual(exchange.fetch_tickers_calls, [["BTC/USDT"]])
        self.assertEqual(set(out), {"BTC"})
        self.assertEqual(out["BTC"]["source"], "ccxt")

    def test_fetch_last_prices_falls_back_after_batch_exception(self) -> None:
        class BatchExceptionExchange:
            instances = []

            def __init__(self, _config):
                self.has = {"fetchTickers": True}
                self.fetch_tickers_calls = []
                self.fetch_ticker_calls = []
                self.closed = False
                self.__class__.instances.append(self)

            def load_markets(self):
                return None

            def fetch_tickers(self, markets):
                self.fetch_tickers_calls.append(list(markets))
                raise ValueError("temporary malformed batch response")

            def fetch_ticker(self, market):
                self.fetch_ticker_calls.append(str(market))
                return {"symbol": str(market), "last": 50.0, "bid": 49.0, "ask": 51.0, "baseVolume": 4}

            def close(self):
                self.closed = True

        self.mod.ccxt = _fake_ccxt(BatchExceptionExchange)
        metrics = []

        with patch.object(
            self.mod,
            "emit_counter",
            side_effect=lambda metric, value=1, **kwargs: metrics.append((metric, value, kwargs)),
        ):
            out = self.mod.fetch_last_prices_ccxt("binance", {"BTC": "BTC/USDT", "ETH": "ETH/USDT"})

        exchange = BatchExceptionExchange.instances[0]
        self.assertEqual(exchange.fetch_tickers_calls, [["BTC/USDT", "ETH/USDT"]])
        self.assertEqual(exchange.fetch_ticker_calls, ["BTC/USDT", "ETH/USDT"])
        self.assertFalse(exchange.closed)
        self.assertEqual(set(out), {"BTC", "ETH"})
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fetch_tickers_calls"), [1])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fetch_tickers_attempts"), [1])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fetch_tickers_successes"), [])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fetch_tickers_failures"), [1])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fetch_tickers_cycle_failures"), [1])
        self.assertEqual(
            [tags.get("reason") for tags in self._captured_metric_tags(metrics, "ccxt_live_fetch_tickers_cycle_failures")],
            ["nonfatal"],
        )
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fetch_tickers_rows"), [0])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fetch_tickers_partial_misses"), [])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fallback_fetches"), [2])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fallback_rows"), [2])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_failed_symbols"), [])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_exchange_cache_evictions"), [])
        self.assertEqual(self._captured_cycle_paths(metrics), ["batch_failed"])

    def test_stale_exchange_error_evicts_cached_exchange(self) -> None:
        class FlakyBatchExchange:
            instances = []

            def __init__(self, _config):
                self.index = len(self.__class__.instances)
                self.has = {"fetchTickers": True}
                self.closed = False
                self.__class__.instances.append(self)

            def load_markets(self):
                return None

            def fetch_tickers(self, _markets):
                if self.index == 0:
                    raise FakeNetworkError("stale exchange session")
                return {
                    "BTC/USDT": {"symbol": "BTC/USDT", "last": 101.0, "bid": 100.0, "ask": 102.0, "baseVolume": 2}
                }

            def fetch_ticker(self, _market):
                raise AssertionError("stale batch errors should not keep using the same exchange")

            def close(self):
                self.closed = True

        self.mod.ccxt = _fake_ccxt(FlakyBatchExchange)
        metrics = []

        with patch.object(
            self.mod,
            "emit_counter",
            side_effect=lambda metric, value=1, **kwargs: metrics.append((metric, value, kwargs)),
        ):
            first = self.mod.fetch_last_prices_ccxt("binance", {"BTC": "BTC/USDT"})
            second = self.mod.fetch_last_prices_ccxt("binance", {"BTC": "BTC/USDT"})

        self.assertEqual(first, {})
        self.assertEqual(set(second), {"BTC"})
        self.assertEqual(len(FlakyBatchExchange.instances), 2)
        self.assertTrue(FlakyBatchExchange.instances[0].closed)
        self.assertFalse(FlakyBatchExchange.instances[1].closed)
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_exchange_cache_misses"), [1, 1])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_exchange_cache_hits"), [])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_exchange_cache_evictions"), [1])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fetch_tickers_attempts"), [1, 1])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fetch_tickers_successes"), [1])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fetch_tickers_failures"), [1])

    def test_bad_symbol_batch_error_invalidates_market_cache_without_recreating_exchange(self) -> None:
        class BadSymbolBatchExchange:
            instances = []

            def __init__(self, _config):
                self.has = {"fetchTickers": True}
                self.load_markets_calls = 0
                self.fetch_tickers_calls = []
                self.fetch_ticker_calls = []
                self.__class__.instances.append(self)

            def load_markets(self):
                self.load_markets_calls += 1

            def fetch_tickers(self, markets):
                self.fetch_tickers_calls.append(list(markets))
                if len(self.fetch_tickers_calls) == 1:
                    raise FakeBadSymbol("market list stale")
                return {
                    "BTC/USDT": {"symbol": "BTC/USDT", "last": 101.0, "bid": 100.0, "ask": 102.0, "baseVolume": 6}
                }

            def fetch_ticker(self, market):
                self.fetch_ticker_calls.append(str(market))
                return {"symbol": str(market), "last": 100.0, "bid": 99.0, "ask": 101.0, "baseVolume": 5}

        self.mod.ccxt = _fake_ccxt(BadSymbolBatchExchange)
        metrics = []

        with patch.object(
            self.mod,
            "emit_counter",
            side_effect=lambda metric, value=1, **kwargs: metrics.append((metric, value, kwargs)),
        ):
            first = self.mod.fetch_last_prices_ccxt("binance", {"BTC": "BTC/USDT"})
            second = self.mod.fetch_last_prices_ccxt("binance", {"BTC": "BTC/USDT"})

        self.assertEqual(len(BadSymbolBatchExchange.instances), 1)
        exchange = BadSymbolBatchExchange.instances[0]
        self.assertEqual(exchange.load_markets_calls, 2)
        self.assertEqual(exchange.fetch_tickers_calls, [["BTC/USDT"], ["BTC/USDT"]])
        self.assertEqual(exchange.fetch_ticker_calls, ["BTC/USDT"])
        self.assertEqual(set(first), {"BTC"})
        self.assertEqual(set(second), {"BTC"})
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_market_cache_invalidations"), [1])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_market_cache_reloads"), [1, 1])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_exchange_cache_misses"), [1])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_exchange_cache_hits"), [1])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fetch_tickers_failures"), [1])
        self.assertEqual(self._captured_metric_values(metrics, "ccxt_live_fetch_tickers_successes"), [1])

    def test_per_symbol_failure_telemetry_is_bounded(self) -> None:
        class FailingFallbackExchange:
            def __init__(self, _config):
                self.has = {"fetchTickers": False}
                self.fetch_ticker_calls = []

            def load_markets(self):
                return None

            def fetch_ticker(self, market):
                self.fetch_ticker_calls.append(str(market))
                raise ValueError(f"bad ticker {market}")

        self.mod.ccxt = _fake_ccxt(FailingFallbackExchange)
        events = []

        def _capture_log_failure(_logger, **kwargs):
            events.append(dict(kwargs))

        markets = {f"SYM{i}": f"SYM{i}/USDT" for i in range(12)}
        with patch.object(self.mod, "log_failure", side_effect=_capture_log_failure):
            out = self.mod.fetch_last_prices_ccxt("binance", markets)

        codes = [str(event.get("code")) for event in events]
        self.assertEqual(out, {})
        self.assertEqual(codes.count("CCXT_LIVE_TICKER_FETCH_FAILED"), self.mod._MAX_FAILURE_TELEMETRY_EVENTS)
        self.assertEqual(codes.count("CCXT_LIVE_FAILURE_TELEMETRY_SUPPRESSED"), 1)

    def test_cached_exchange_preserves_timeout_credentials_options_rate_limit_and_sandbox(self) -> None:
        class ConfiguredExchange:
            instances = []

            def __init__(self, config):
                self.config = dict(config)
                self.has = {"fetchTickers": False}
                self.load_markets_calls = 0
                self.fetch_ticker_calls = []
                self.sandbox_calls = []
                self.__class__.instances.append(self)

            def set_sandbox_mode(self, enabled):
                self.sandbox_calls.append(bool(enabled))

            def load_markets(self):
                self.load_markets_calls += 1

            def fetch_ticker(self, market):
                self.fetch_ticker_calls.append(str(market))
                return {"symbol": str(market), "last": 123.0, "bid": 122.0, "ask": 124.0, "baseVolume": 8}

        self.mod.ccxt = _fake_ccxt(ConfiguredExchange)

        with patch.dict(
            os.environ,
            {
                "CCXT_API_KEY": "api-key",
                "CCXT_OPTIONS_JSON": '{"defaultType": "spot", "adjustForTimeDifference": true}',
                "CCXT_PASSWORD": "passphrase",
                "CCXT_SANDBOX": "1",
                "CCXT_SECRET": "secret-key",
                "CCXT_TIMEOUT_MS": "2500",
            },
        ):
            out1 = self.mod.fetch_last_prices_ccxt("binance", {"BTC": "BTC/USDT"})
            out2 = self.mod.fetch_last_prices_ccxt("binance", {"ETH": "ETH/USDT"})

        self.assertEqual(len(ConfiguredExchange.instances), 1)
        exchange = ConfiguredExchange.instances[0]
        self.assertEqual(
            exchange.config,
            {
                "apiKey": "api-key",
                "enableRateLimit": True,
                "options": {"adjustForTimeDifference": True, "defaultType": "spot"},
                "password": "passphrase",
                "secret": "secret-key",
                "timeout": 2500,
            },
        )
        self.assertEqual(exchange.sandbox_calls, [True])
        self.assertEqual(exchange.load_markets_calls, 1)
        self.assertEqual(exchange.fetch_ticker_calls, ["BTC/USDT", "ETH/USDT"])
        self.assertEqual(set(out1), {"BTC"})
        self.assertEqual(set(out2), {"ETH"})

    def test_exchange_cache_key_redacts_secret_config_values(self) -> None:
        key1 = self.mod._exchange_cache_key(
            "binance",
            {
                "apiKey": "live-public-key",
                "secret": "live-secret-value",
                "enableRateLimit": True,
                "timeout": 1000,
            },
        )
        key2 = self.mod._exchange_cache_key(
            "binance",
            {
                "apiKey": "different-public-key",
                "secret": "different-secret-value",
                "enableRateLimit": True,
                "timeout": 1000,
            },
        )
        key3 = self.mod._exchange_cache_key(
            "binance",
            {
                "apiKey": "live-public-key",
                "secret": "live-secret-value",
                "enableRateLimit": True,
                "timeout": 1000,
            },
            sandbox_enabled=True,
        )

        self.assertNotIn("live-public-key", key1)
        self.assertNotIn("live-secret-value", key1)
        self.assertNotIn("different-public-key", key2)
        self.assertNotIn("different-secret-value", key2)
        self.assertIn("enableRateLimit", key1)
        self.assertIn("timeout", key1)
        self.assertNotEqual(key1, key2)
        self.assertNotEqual(key1, key3)


if __name__ == "__main__":
    unittest.main()
