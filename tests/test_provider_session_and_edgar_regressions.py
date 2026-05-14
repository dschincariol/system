from __future__ import annotations

import importlib
import os
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


class _AuthNeverReadyProbe:
    provider_name = "probe"

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._desired = set()
        self._subscribed = set()
        self._connected = False
        self._authenticated = False
        self._reconnect_count = 0

    def replace_desired_symbols(self, symbols):
        with self._lock:
            self._desired = {str(x) for x in (symbols or [])}

    def desired_symbols(self):
        with self._lock:
            return set(self._desired)

    def subscribed_symbols(self):
        with self._lock:
            return set(self._subscribed)

    def telemetry_snapshot(self):
        with self._lock:
            now = int(time.time() * 1000)
            return {
                "connected": bool(self._connected),
                "authenticated": bool(self._authenticated),
                "connection_state": "connected" if self._connected else "disconnected",
                "desired_symbol_count": len(self._desired),
                "subscribed_symbol_count": len(self._subscribed),
                "last_msg_age_ms": 0,
                "last_connect_ts_ms": now,
                "last_heartbeat_ts_ms": now,
                "capabilities": {"authentication": "api_key", "streaming": True, "polling": False},
            }

    def note_reconnecting(self, _reason=None):
        with self._lock:
            self._connected = False
            self._authenticated = False

    def note_error(self, _error):
        return None

    def increment_reconnect_count(self):
        with self._lock:
            self._reconnect_count += 1

    def close(self):
        with self._lock:
            self._connected = False
            self._authenticated = False

    def connect(self):
        with self._lock:
            self._connected = True

    def authenticate(self):
        with self._lock:
            self._authenticated = False

    def detect_capabilities(self):
        return self.telemetry_snapshot()["capabilities"]

    def subscribe(self, symbols):
        with self._lock:
            if self._connected and self._authenticated:
                self._subscribed |= {str(x) for x in (symbols or [])}

    def unsubscribe(self, symbols):
        with self._lock:
            self._subscribed -= {str(x) for x in (symbols or [])}

    def apply_rate_limit(self, _operation="request"):
        return None

    def heartbeat(self):
        return self.telemetry_snapshot()

    def snapshot(self):
        return {}

    def perform_gap_fill(self, *_args, **_kwargs):
        return {}


class ProviderSessionAndEdgarRegressionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.prev_env = {
            "DB_PATH": os.environ.get("DB_PATH"),
            "TRADING_LOGS": os.environ.get("TRADING_LOGS"),
            "TRADING_DATA": os.environ.get("TRADING_DATA"),
            "ENGINE_MODE": os.environ.get("ENGINE_MODE"),
            "PROVIDER_SUBSCRIBE_VERIFY_S": os.environ.get("PROVIDER_SUBSCRIBE_VERIFY_S"),
        }
        os.environ["DB_PATH"] = str(Path(self.tmp.name) / "regressions.db")
        os.environ["TRADING_LOGS"] = str(Path(self.tmp.name) / "logs")
        os.environ["TRADING_DATA"] = str(Path(self.tmp.name) / "data")
        os.environ["ENGINE_MODE"] = "safe"
        os.environ["PROVIDER_SUBSCRIBE_VERIFY_S"] = "0.1"
        _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
            "engine.runtime.runtime_meta",
            "engine.data.provider_sessions.session_manager",
            "engine.data.sec.edgar_live",
        )

    def tearDown(self) -> None:
        try:
            (storage,) = _reload_modules("engine.runtime.storage")
            storage.close_pooled_connections()
        finally:
            for key, value in self.prev_env.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value
            self.tmp.cleanup()

    def test_provider_manager_does_not_mark_connected_when_auth_never_completes(self) -> None:
        (session_manager,) = _reload_modules("engine.data.provider_sessions.session_manager")
        probe = _AuthNeverReadyProbe()
        manager = session_manager.ProviderSessionManager(
            probe,
            provider_name="probe",
            heartbeat_interval_s=0.01,
            reconnect_base_s=0.01,
            reconnect_max_s=0.01,
            max_reconnect_attempts=1,
        )
        try:
            manager.ensure_subscriptions({"SPY"})
            deadline = time.time() + 1.0
            while time.time() < deadline:
                telemetry = manager.provider_telemetry()
                if str(telemetry.get("manager_state") or "") == "failed":
                    break
                time.sleep(0.02)

            telemetry = manager.provider_telemetry()
            self.assertEqual(str(telemetry.get("manager_state") or ""), "failed")
            self.assertFalse(bool(telemetry.get("connected")))
            self.assertFalse(bool(telemetry.get("authenticated")))
            self.assertEqual(probe.subscribed_symbols(), set())
        finally:
            manager.close()

    def test_ticker_to_cik_supports_sec_fields_data_payload(self) -> None:
        (edgar_live,) = _reload_modules("engine.data.sec.edgar_live")
        payload = {
            "fields": ["cik", "name", "ticker", "exchange"],
            "data": [
                [320193, "Apple Inc.", "AAPL", "Nasdaq"],
                [1045810, "NVIDIA CORP", "NVDA", "Nasdaq"],
            ],
        }
        with patch.object(edgar_live, "_load_ticker_map", return_value=payload):
            self.assertEqual(edgar_live.ticker_to_cik("aapl"), "0000320193")
            self.assertEqual(edgar_live.ticker_to_cik("NVDA"), "0001045810")

    def test_options_polygon_redacts_api_key_from_errors(self) -> None:
        (options_polygon,) = _reload_modules("engine.data.options.options_polygon")
        secret = "topsecret_polygon_key"

        class _BoomSession:
            def get(self, *_args, **_kwargs):
                raise requests.HTTPError(
                    "403 Client Error: Forbidden for url: "
                    f"https://api.polygon.io/v3/snapshot/options/SPY?apiKey={secret}&limit=250"
                )

        with (
            patch.object(options_polygon, "_POLYGON_KEY", secret),
            patch.object(options_polygon, "_get_session", return_value=_BoomSession()),
        ):
            contracts, err = options_polygon.fetch_options_chain_snapshot("SPY")

        self.assertEqual(contracts, [])
        self.assertIsInstance(err, str)
        self.assertNotIn(secret, err)
        self.assertIn("apiKey=REDACTED", err)

    def test_execution_degradation_snapshot_tolerates_missing_table(self) -> None:
        (execution_analytics_engine,) = _reload_modules("engine.execution.execution_analytics_engine")
        con = sqlite3.connect(":memory:")
        try:
            summary = execution_analytics_engine.get_execution_degradation_snapshot(con)
        finally:
            con.close()
        self.assertEqual(summary["n"], 0)
        self.assertEqual(summary["mean_slippage"], 0.0)
        self.assertEqual(summary["mean_latency"], 0.0)

    def test_snapshot_model_features_load_symbols_tolerates_missing_price_quotes(self) -> None:
        (snapshot_model_features,) = _reload_modules("engine.data.jobs.snapshot_model_features")
        con = sqlite3.connect(":memory:")
        try:
            with (
                patch.object(snapshot_model_features, "get_active_symbols", return_value=[]),
                patch.object(snapshot_model_features, "_table_exists", return_value=False),
            ):
                symbols = snapshot_model_features._load_symbols(con, limit=10)
        finally:
            con.close()

        self.assertEqual(symbols, [])
