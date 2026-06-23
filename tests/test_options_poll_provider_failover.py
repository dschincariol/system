"""Regression tests for options provider cooldown failover."""

from __future__ import annotations

import importlib
import os
import sys
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


def _tradier_success_rows():
    return {
        "rows": [
            {
                "expiry": "2026-04-17",
                "strike": 500.0,
                "call_put": "C",
                "iv": 0.2,
                "open_interest": 10,
                "volume": 5,
            }
        ]
    }


def _polygon_success_contracts():
    return (
        [
            {
                "ts_ms": 1_000_000,
                "underlying": "SPY",
                "contract": "O:SPY260417C00500000",
                "expiration": "2026-04-17",
                "contract_type": "call",
                "strike": 500.0,
                "iv": 0.2,
                "open_interest": 10,
                "volume": 5,
                "source": "polygon",
            }
        ],
        None,
    )


def _bulk_write_counts(_con, *, polygon_rows=None, tradier_rows=None):
    polygon_n = len(list(polygon_rows or []))
    tradier_n = len(list(tradier_rows or []))
    return {"polygon_rows": polygon_n, "tradier_rows": tradier_n, "raw_rows": polygon_n + tradier_n}


class _FakeConnection:
    def close(self) -> None:
        return None

    def commit(self) -> None:
        return None

    def executemany(self, _sql, _seq_of_params):
        return None


class OptionsPollProviderFailoverTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = Path(self.tmp.name) / "options_provider_failover.db"
        self._env_keys = [
            "DB_PATH",
            "ENGINE_SUPERVISED",
            "TRADIER_API_TOKEN",
            "POLYGON_API_KEY",
            "OPTIONS_PROVIDER_CHAIN",
            "OPTIONS_CRITICAL_SYMBOLS",
            "OPTIONS_SYMBOL_FAILURE_THRESHOLD",
            "OPTIONS_PROVIDER_RATE_LIMIT_BASE_COOLDOWN_S",
            "OPTIONS_PROVIDER_RATE_LIMIT_MAX_COOLDOWN_S",
            "RUNTIME_METRICS_BUFFER_ENABLED",
        ]
        self._old_env = {key: os.environ.get(key) for key in self._env_keys}
        os.environ["DB_PATH"] = str(self.db_path)
        os.environ["ENGINE_SUPERVISED"] = "1"
        os.environ["TRADIER_API_TOKEN"] = "token"
        os.environ["POLYGON_API_KEY"] = "key"
        os.environ["OPTIONS_PROVIDER_CHAIN"] = "tradier,polygon"
        os.environ["OPTIONS_CRITICAL_SYMBOLS"] = "SPY"
        os.environ["OPTIONS_SYMBOL_FAILURE_THRESHOLD"] = "3"
        os.environ["OPTIONS_PROVIDER_RATE_LIMIT_BASE_COOLDOWN_S"] = "60"
        os.environ["OPTIONS_PROVIDER_RATE_LIMIT_MAX_COOLDOWN_S"] = "1800"
        os.environ["RUNTIME_METRICS_BUFFER_ENABLED"] = "0"

        _reload_modules("engine.runtime.db_guard")

    def tearDown(self) -> None:
        for key, value in self._old_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        self.tmp.cleanup()

    def test_rate_limited_provider_is_skipped_until_retry_after_expires(self) -> None:
        (options_poll,) = _reload_modules("engine.data.options_poll")
        options_poll.provider_cooldowns.clear()
        options_poll.provider_rate_limit_counts.clear()
        options_poll.provider_cooldown_reasons.clear()

        base_s = 1_900_000_000.0
        metric_calls = []
        rate_limit = options_poll.TradierFetchError(
            "rate_limited",
            kind="rate_limit",
            status_code=429,
            retry_after_s=30,
        )

        def _capture_metric(metric, value_num=None, value_text=None, tags=None, ts_ms=None):
            metric_calls.append(
                {
                    "metric": metric,
                    "value_num": value_num,
                    "value_text": value_text,
                    "tags": dict(tags or {}),
                    "ts_ms": ts_ms,
                }
            )

        with ExitStack() as stack:
            stack.enter_context(patch("engine.data.options_poll.connect", return_value=_FakeConnection()))
            stack.enter_context(patch("engine.data.options_poll.get_active_symbols", return_value=["SPY"]))
            stack.enter_context(
                patch("engine.data.options_poll._load_symbol_states", return_value={"SPY": {"disabled_until_ts_ms": 0}})
            )
            stack.enter_context(patch("engine.data.options_poll._record_symbol_success", return_value={}))
            stack.enter_context(patch("engine.data.options_poll._write_options_bulk_rows", side_effect=_bulk_write_counts))
            stack.enter_context(patch("engine.data.options_poll._write_options_snapshot_event", return_value=None))
            stack.enter_context(patch("engine.data.options_poll.checkpoint_if_due", return_value=None))
            stack.enter_context(
                patch(
                    "engine.data.options_poll.write_runtime_metric",
                    side_effect=_capture_metric,
                )
            )
            tradier_mock = stack.enter_context(
                patch(
                    "engine.data.options_poll.fetch_options_chain",
                    side_effect=[rate_limit, _tradier_success_rows()],
                )
            )
            polygon_mock = stack.enter_context(
                patch(
                    "engine.data.options_poll.fetch_options_chain_snapshot",
                    return_value=_polygon_success_contracts(),
                )
            )

            def _run_at(now_s: float):
                with patch("engine.data.options_poll.time.time", return_value=now_s):
                    return options_poll._run_once(["tradier", "polygon"])

            first = _run_at(base_s)

            self.assertEqual(tradier_mock.call_count, 1)
            self.assertEqual(polygon_mock.call_count, 1)
            self.assertAlmostEqual(
                float(options_poll.provider_cooldowns["tradier"]),
                base_s + 30.0,
            )
            self.assertEqual(
                int(first["provider_status"]["tradier"]["cooldown_remaining_s"]),
                30,
            )
            self.assertEqual(first["meta"]["symbol_status"]["SPY"]["provider"], "polygon")

            second = _run_at(base_s + 5.0)

            self.assertEqual(tradier_mock.call_count, 1)
            self.assertEqual(polygon_mock.call_count, 2)
            self.assertEqual(int(second["provider_status"]["tradier"]["skipped_symbols"]), 1)
            self.assertGreater(
                float(second["meta"]["provider_cooldowns"]["tradier"]["remaining_s"]),
                24.0,
            )
            self.assertEqual(second["meta"]["symbol_status"]["SPY"]["provider"], "polygon")

            third = _run_at(base_s + 35.0)

        self.assertEqual(tradier_mock.call_count, 2)
        self.assertEqual(polygon_mock.call_count, 2)
        self.assertNotIn("tradier", options_poll.provider_cooldowns)
        self.assertEqual(third["meta"]["symbol_status"]["SPY"]["provider"], "tradier")

        self.assertTrue(
            any(
                row["metric"] == "options.provider.cooldown_remaining_s"
                and float(row["value_num"] or 0.0) == 30.0
                and row["tags"].get("provider") == "tradier"
                for row in metric_calls
            )
        )


if __name__ == "__main__":
    unittest.main()
