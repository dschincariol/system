from __future__ import annotations

import importlib
import os
import sys
import time
import unittest
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


class YFinanceLiveTests(unittest.TestCase):
    def setUp(self) -> None:
        self._env = dict(os.environ)
        os.environ.setdefault("RUNTIME_METRICS_ENABLED", "0")
        existing = sys.modules.get("engine.data.live_prices.yfinance_live")
        if existing is not None and hasattr(existing, "shutdown_yfinance_resources"):
            existing.shutdown_yfinance_resources(wait=True, cancel_futures=True)
        self.mod = importlib.import_module("engine.data.live_prices.yfinance_live")
        self.mod = importlib.reload(self.mod)
        self.mod.reset_yfinance_resources_for_tests()

    def tearDown(self) -> None:
        self.mod.reset_yfinance_resources_for_tests()
        os.environ.clear()
        os.environ.update(self._env)

    def _download_frame(self, tickers: list[str], *, missing: set[str] | None = None):
        import pandas as pd

        missing = set(missing or set())
        columns = []
        values = []
        for field in ("Close", "Volume"):
            for ticker in tickers:
                if ticker in missing:
                    continue
                columns.append((field, ticker))
                values.append(float(len(ticker)) if field == "Close" else float(len(ticker) * 100))
        return pd.DataFrame(
            [values],
            columns=pd.MultiIndex.from_tuples(columns),
            index=pd.to_datetime(["2026-06-21T14:30:00Z"]),
        )

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

    def test_fetch_last_prices_uses_download_batch_without_default_64_symbol_cap(self) -> None:
        os.environ.pop("YFINANCE_LIVE_BATCH_SIZE", None)
        os.environ["YFINANCE_LIVE_PRIORITY_TICKERS"] = ""
        ticker_map = {f"SYM{i:02d}": f"SYM{i:02d}" for i in range(70)}
        calls = []

        class FakeYF:
            @staticmethod
            def download(**kwargs):
                calls.append(dict(kwargs))
                return self._download_frame(list(kwargs["tickers"]))

        with patch.object(self.mod, "yf", FakeYF):
            with patch.object(self.mod, "_fetch_chart_json") as chart_fetch:
                out = self.mod.fetch_last_prices_yf(ticker_map)

        self.assertEqual(set(out), set(ticker_map))
        self.assertEqual(len(calls), 1)
        self.assertEqual(len(calls[0]["tickers"]), 70)
        self.assertEqual(calls[0]["period"], "1d")
        self.assertEqual(calls[0]["interval"], "1m")
        self.assertFalse(calls[0]["threads"])
        chart_fetch.assert_not_called()

    def test_fetch_last_prices_uses_configured_batch_size_as_chunk_without_dropping_symbols(self) -> None:
        os.environ["YFINANCE_LIVE_BATCH_SIZE"] = "2"
        os.environ["YFINANCE_LIVE_PRIORITY_TICKERS"] = ""
        calls = []
        sessions = []

        class FakeYF:
            @staticmethod
            def download(**kwargs):
                tickers = list(kwargs["tickers"])
                calls.append(tickers)
                sessions.append(kwargs.get("session"))
                return self._download_frame(tickers)

        ticker_map = {"AAA": "AAA", "BBBB": "BBBB", "CCCCC": "CCCCC"}

        with patch.object(self.mod, "yf", FakeYF):
            with patch.object(self.mod, "_fetch_chart_json") as chart_fetch:
                out = self.mod.fetch_last_prices_yf(ticker_map)

        self.assertEqual(set(out), {"AAA", "BBBB", "CCCCC"})
        self.assertEqual(calls, [["AAA", "BBBB"], ["CCCCC"]])
        self.assertEqual(len({id(session) for session in sessions}), 1)
        self.assertIsNotNone(sessions[0])
        chart_fetch.assert_not_called()

    def test_fetch_last_prices_limits_fallback_large_batches_and_rotates_symbols(self) -> None:
        os.environ["YFINANCE_LIVE_FALLBACK_SYMBOL_LIMIT"] = "2"
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

    def test_fetch_last_prices_reports_exact_symbols_skipped_by_fallback_cap(self) -> None:
        os.environ["YFINANCE_LIVE_FALLBACK_SYMBOL_LIMIT"] = "2"
        os.environ["YFINANCE_LIVE_MAX_WORKERS"] = "1"
        os.environ["YFINANCE_LIVE_PRIORITY_TICKERS"] = ""

        def _fake_chart(symbol: str, *, interval: str, range_: str, deadline_monotonic: float | None = None):
            return {
                "meta": {"regularMarketPrice": float(len(symbol))},
                "indicators": {"quote": [{"close": [float(len(symbol))], "volume": [10]}]},
            }

        ticker_map = {
            "AAA": "AAA",
            "BBBB": "BBBB",
            "CCCCC": "CCCCC",
            "DD": "DD",
        }

        with patch.object(self.mod, "yf", None):
            with patch.object(self.mod, "_fetch_chart_json", side_effect=_fake_chart):
                with patch.object(self.mod.LOG, "log") as log_method:
                    with patch.object(self.mod, "emit_counter") as emit_counter:
                        with patch.object(self.mod, "emit_gauge") as emit_gauge:
                            out = self.mod.fetch_last_prices_yf(ticker_map)

        self.assertEqual(set(out), {"AAA", "BBBB"})
        self.assertEqual(log_method.call_count, 1)
        warning_extra = dict(log_method.call_args.kwargs["extra"])
        self.assertEqual(warning_extra["event"], "yfinance_live_symbol_selection_limited")
        self.assertEqual(warning_extra["limit"], 2)
        self.assertTrue(warning_extra["degraded"])
        self.assertEqual(warning_extra["degraded_reason"], "configured_fallback_symbol_limit")
        self.assertEqual(warning_extra["skipped_symbols"], ["CCCCC", "DD"])

        emit_counter.assert_called_once()
        counter_args, counter_kwargs = emit_counter.call_args
        self.assertEqual(counter_args[:2], ("yfinance_live_symbol_limit_skipped", 2))
        self.assertEqual(counter_kwargs["provider"], "yfinance")
        self.assertEqual(counter_kwargs["extra_tags"]["reason"], "configured_fallback_symbol_limit")
        self.assertEqual(counter_kwargs["extra_tags"]["limit"], 2)
        self.assertTrue(counter_kwargs["extra_tags"]["degraded"])

        emit_gauge.assert_called_once()
        gauge_args, gauge_kwargs = emit_gauge.call_args
        self.assertEqual(gauge_args[:2], ("yfinance_live_degraded", 1))
        self.assertEqual(gauge_kwargs["provider"], "yfinance")
        self.assertEqual(gauge_kwargs["extra_tags"]["reason"], "configured_fallback_symbol_limit")

    def test_fetch_last_prices_falls_back_to_chart_for_download_misses(self) -> None:
        os.environ.pop("YFINANCE_LIVE_BATCH_SIZE", None)
        os.environ["YFINANCE_LIVE_MAX_WORKERS"] = "4"
        os.environ["YFINANCE_LIVE_PRIORITY_TICKERS"] = ""
        calls = []

        class FakeYF:
            @staticmethod
            def download(**kwargs):
                tickers = list(kwargs["tickers"])
                calls.append(tickers)
                return self._download_frame(tickers, missing={"QQQ"})

        chart = {
            "meta": {"regularMarketPrice": 456.78},
            "indicators": {"quote": [{"close": [456.78], "volume": [2000]}]},
        }

        with patch.object(self.mod, "yf", FakeYF):
            with patch.object(self.mod, "_fetch_chart_json", return_value=chart) as chart_fetch:
                with patch.object(self.mod, "_log_partial_batch_failure") as partial_log:
                    out = self.mod.fetch_last_prices_yf({"SPY": "SPY", "QQQ": "QQQ"})

        self.assertEqual(calls, [["SPY", "QQQ"]])
        self.assertIn("SPY", out)
        self.assertIn("QQQ", out)
        self.assertAlmostEqual(float(out["QQQ"]["price"]), 456.78, places=6)
        partial_log.assert_called_once()
        _, partial_kwargs = partial_log.call_args
        self.assertEqual(partial_kwargs["missing"], 1)
        self.assertEqual(partial_kwargs["missing_symbols"], ["QQQ"])
        chart_fetch.assert_called_once()
        _, kwargs = chart_fetch.call_args
        self.assertEqual(kwargs["range_"], "1d")
        self.assertEqual(int(self.mod._YFINANCE_EXECUTOR_MAX_WORKERS), 4)

    def test_fetch_last_prices_does_not_drop_symbols_past_old_cap_when_chunking(self) -> None:
        os.environ["YFINANCE_LIVE_BATCH_SIZE"] = "10"
        os.environ["YFINANCE_LIVE_PRIORITY_TICKERS"] = ""
        ticker_map = {f"SYM{i:02d}": f"SYM{i:02d}" for i in range(70)}
        calls = []

        class FakeYF:
            @staticmethod
            def download(**kwargs):
                tickers = list(kwargs["tickers"])
                calls.append(tickers)
                return self._download_frame(tickers)

        with patch.object(self.mod, "yf", FakeYF):
            with patch.object(self.mod, "_fetch_chart_json") as chart_fetch:
                out = self.mod.fetch_last_prices_yf(ticker_map)

        self.assertEqual(set(out), set(ticker_map))
        self.assertEqual(len(calls), 7)
        self.assertTrue(all(len(chunk) <= 10 for chunk in calls))
        chart_fetch.assert_not_called()

    def test_fetch_last_prices_reports_batch_timeout_without_chart_fallback(self) -> None:
        os.environ["YFINANCE_LIVE_BATCH_SIZE"] = "2"
        os.environ["YFINANCE_LIVE_BATCH_TIMEOUT_S"] = "0.25"
        os.environ["YFINANCE_LIVE_PRIORITY_TICKERS"] = ""

        class FakeYF:
            @staticmethod
            def download(**kwargs):
                raise AssertionError("download should be hidden behind the fake executor")

        class FakeFuture:
            def __init__(self) -> None:
                self.cancelled = False

            def cancel(self) -> bool:
                self.cancelled = True
                return True

        class FakeExecutor:
            futures: list[FakeFuture] = []

            def __init__(self, max_workers: int, thread_name_prefix: str) -> None:
                self.max_workers = max_workers
                self.thread_name_prefix = thread_name_prefix

            def submit(self, fn, *args, **kwargs):
                del fn, args, kwargs
                future = FakeFuture()
                self.futures.append(future)
                return future

            def shutdown(self, wait: bool = True, cancel_futures: bool = False) -> None:
                del wait, cancel_futures

        def _fake_wait(futures, timeout: float):
            del timeout
            return set(), set(futures)

        with patch.object(self.mod, "yf", FakeYF):
            with patch.object(self.mod, "ThreadPoolExecutor", FakeExecutor):
                with patch.object(self.mod, "wait", side_effect=_fake_wait):
                    with patch.object(self.mod, "_fetch_chart_json") as chart_fetch:
                        with patch.object(self.mod, "_log_partial_fetch_timeout") as timeout_log:
                            out = self.mod.fetch_last_prices_yf({"SLOW": "SLOW"})

        self.assertEqual(out, {})
        self.assertEqual(len(FakeExecutor.futures), 1)
        self.assertTrue(FakeExecutor.futures[0].cancelled)
        chart_fetch.assert_not_called()
        timeout_log.assert_called_once()

    def test_fetch_last_prices_returns_partial_results_when_batch_deadline_expires(self) -> None:
        os.environ["YFINANCE_LIVE_BATCH_SIZE"] = "2"
        os.environ["YFINANCE_LIVE_MAX_WORKERS"] = "2"
        os.environ["YFINANCE_LIVE_BATCH_TIMEOUT_S"] = "0.25"
        os.environ["YFINANCE_LIVE_BATCH_ENABLED"] = "0"
        os.environ["YFINANCE_LIVE_PRIORITY_TICKERS"] = ""

        class FakeFuture:
            def __init__(self, result=None, *, pending: bool = False) -> None:
                self._result = result
                self.pending = bool(pending)
                self.cancelled = False

            def cancel(self) -> bool:
                self.cancelled = True
                return True

            def result(self):
                return self._result

        class FakeExecutor:
            futures: list[FakeFuture] = []

            def __init__(self, max_workers: int, thread_name_prefix: str) -> None:
                self.max_workers = max_workers
                self.thread_name_prefix = thread_name_prefix

            def submit(self, fn, sym: str, ticker_symbol: str, now_ms: int, deadline_monotonic: float | None = None):
                del fn, deadline_monotonic
                future = FakeFuture(
                    None if sym == "SLOW" else (
                        sym,
                        {"ts_ms": int(now_ms), "price": float(len(ticker_symbol)), "source": "yfinance"},
                    ),
                    pending=(sym == "SLOW"),
                )
                self.futures.append(future)
                return future

            def shutdown(self, wait: bool = True, cancel_futures: bool = False) -> None:
                del wait, cancel_futures

        def _fake_wait(futures, timeout: float):
            del timeout
            done = {future for future in futures if not future.pending}
            not_done = {future for future in futures if future.pending}
            return done, not_done

        with patch.object(self.mod, "ThreadPoolExecutor", FakeExecutor):
            with patch.object(self.mod, "wait", side_effect=_fake_wait):
                out = self.mod.fetch_last_prices_yf({"FAST": "FAST", "SLOW": "SLOW"})

        self.assertIn("FAST", out)
        self.assertNotIn("SLOW", out)
        pending_futures = [future for future in FakeExecutor.futures if future.pending]
        self.assertEqual(len(pending_futures), 1)
        self.assertTrue(pending_futures[0].cancelled)

    def test_repeated_download_cycles_reuse_session_and_executor_resources(self) -> None:
        os.environ["YFINANCE_LIVE_MAX_WORKERS"] = "3"
        os.environ["YFINANCE_LIVE_PRIORITY_TICKERS"] = ""
        ticker_map = {"AAA": "AAA", "BBBB": "BBBB"}
        sessions = []

        class FakeYF:
            @staticmethod
            def download(**kwargs):
                sessions.append(kwargs.get("session"))
                return self._download_frame(list(kwargs["tickers"]))

        with patch.object(self.mod, "yf", FakeYF):
            out1 = self.mod.fetch_last_prices_yf(ticker_map)
            session_id = id(self.mod._HTTP_SESSION)
            executor_id = id(self.mod._YFINANCE_EXECUTOR)
            out2 = self.mod.fetch_last_prices_yf(ticker_map)

        self.assertEqual(set(out1), set(ticker_map))
        self.assertEqual(set(out2), set(ticker_map))
        self.assertEqual(len(sessions), 2)
        self.assertEqual({id(session) for session in sessions}, {session_id})
        self.assertEqual(id(self.mod._HTTP_SESSION), session_id)
        self.assertEqual(id(self.mod._YFINANCE_EXECUTOR), executor_id)
        self.assertGreaterEqual(int(self.mod._HTTP_SESSION_POOL_SIZE), 32)
        self.assertEqual(int(self.mod._YFINANCE_EXECUTOR_MAX_WORKERS), 3)

    def test_repeated_fallback_cycles_reuse_executor_instance(self) -> None:
        os.environ["YFINANCE_LIVE_BATCH_ENABLED"] = "0"
        os.environ["YFINANCE_LIVE_MAX_WORKERS"] = "2"
        os.environ["YFINANCE_LIVE_PRIORITY_TICKERS"] = ""

        class FakeFuture:
            def __init__(self, result) -> None:
                self._result = result
                self.cancelled = False

            def cancel(self) -> bool:
                self.cancelled = True
                return True

            def result(self):
                return self._result

        class FakeExecutor:
            instances = []

            def __init__(self, max_workers: int, thread_name_prefix: str) -> None:
                self.max_workers = max_workers
                self.thread_name_prefix = thread_name_prefix
                self.shutdown_calls = 0
                self.submitted = 0
                self.instances.append(self)

            def submit(self, fn, *args, **kwargs):
                self.submitted += 1
                return FakeFuture(fn(*args, **kwargs))

            def shutdown(self, wait: bool = True, cancel_futures: bool = False) -> None:
                del wait, cancel_futures
                self.shutdown_calls += 1

        def _fake_wait(futures, timeout: float):
            del timeout
            return set(futures), set()

        def _fake_symbol(sym: str, ticker_symbol: str, now_ms: int, deadline_monotonic: float | None = None):
            del deadline_monotonic
            return sym, {"ts_ms": int(now_ms), "price": float(len(ticker_symbol)), "source": "yfinance"}

        with patch.object(self.mod, "ThreadPoolExecutor", FakeExecutor):
            with patch.object(self.mod, "wait", side_effect=_fake_wait):
                with patch.object(self.mod, "_fetch_symbol_last_price", side_effect=_fake_symbol):
                    out1 = self.mod.fetch_last_prices_yf({"AAA": "AAA", "BBBB": "BBBB"})
                    out2 = self.mod.fetch_last_prices_yf({"AAA": "AAA", "BBBB": "BBBB"})

        self.assertEqual(set(out1), {"AAA", "BBBB"})
        self.assertEqual(set(out2), {"AAA", "BBBB"})
        self.assertEqual(len(FakeExecutor.instances), 1)
        self.assertEqual(FakeExecutor.instances[0].max_workers, 2)
        self.assertEqual(FakeExecutor.instances[0].submitted, 4)
        self.assertEqual(FakeExecutor.instances[0].shutdown_calls, 0)
        self.mod.shutdown_yfinance_resources(wait=True, cancel_futures=True)
        self.assertEqual(FakeExecutor.instances[0].shutdown_calls, 1)
        self.assertIsNone(self.mod._YFINANCE_EXECUTOR)

    def test_shared_fallback_executor_does_not_leak_threads_after_reset(self) -> None:
        os.environ["YFINANCE_LIVE_BATCH_ENABLED"] = "0"
        os.environ["YFINANCE_LIVE_MAX_WORKERS"] = "2"
        os.environ["YFINANCE_LIVE_PRIORITY_TICKERS"] = ""

        def _fake_symbol(sym: str, ticker_symbol: str, now_ms: int, deadline_monotonic: float | None = None):
            del deadline_monotonic
            return sym, {"ts_ms": int(now_ms), "price": float(len(ticker_symbol)), "source": "yfinance"}

        with patch.object(self.mod, "_fetch_symbol_last_price", side_effect=_fake_symbol):
            for _ in range(5):
                out = self.mod.fetch_last_prices_yf({"AAA": "AAA", "BBBB": "BBBB", "CC": "CC"})
                self.assertEqual(set(out), {"AAA", "BBBB", "CC"})

        executor = self.mod._YFINANCE_EXECUTOR
        self.assertIsNotNone(executor)
        threads = set(getattr(executor, "_threads", set()))
        self.assertLessEqual(len(threads), 2)
        self.mod.reset_yfinance_resources_for_tests()

        deadline = time.time() + 2.0
        while time.time() < deadline and any(thread.is_alive() for thread in threads):
            time.sleep(0.01)
        self.assertFalse(any(thread.is_alive() for thread in threads))
        self.assertIsNone(self.mod._YFINANCE_EXECUTOR)
        self.assertIsNone(self.mod._HTTP_SESSION)


if __name__ == "__main__":
    unittest.main()
