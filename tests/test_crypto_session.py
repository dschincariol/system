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


def _maintenance_disabled_env() -> dict[str, str]:
    return {
        "CRYPTO_MAINTENANCE_ENABLED": "0",
        "CRYPTO_MAINTENANCE_START_UTC": "",
        "CRYPTO_MAINTENANCE_START_HOUR_UTC": "",
        "CRYPTO_MAINTENANCE_START_MINUTE_UTC": "",
        "CRYPTO_MAINTENANCE_DURATION_MINUTES": "0",
        "CRYPTO_MAINTENANCE_SYMBOLS": "",
    }


class CryptoSessionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.crypto_session = importlib.reload(importlib.import_module("engine.execution.crypto_session"))

    def test_crypto_is_open_on_weekday_and_weekend(self) -> None:
        weekday = _utc_ms(2026, 6, 24, 16)
        weekend = _utc_ms(2026, 6, 27, 16)

        with patch.dict(os.environ, _maintenance_disabled_env(), clear=False):
            weekday_state = self.crypto_session.crypto_session_state("BTC", weekday)
            weekend_state = self.crypto_session.crypto_session_state("BTC", weekend)

        self.assertTrue(bool(weekday_state["is_crypto"]))
        self.assertEqual(weekday_state["session"], "open")
        self.assertTrue(bool(weekday_state["is_open"]))
        self.assertIsNone(weekday_state["next_open_ms"])
        self.assertEqual(weekend_state["session"], "open")
        self.assertTrue(bool(weekend_state["is_open"]))
        self.assertFalse(bool(weekend_state["in_maintenance_window"]))

    def test_weekend_timestamp_that_fx_closes_stays_open_for_crypto(self) -> None:
        ts = _utc_ms(2026, 6, 27, 16)
        fx_session = importlib.reload(importlib.import_module("engine.execution.fx_session"))

        with patch.dict(os.environ, _maintenance_disabled_env(), clear=False):
            crypto_state = self.crypto_session.crypto_session_state("BTC", ts)
            fx_state = fx_session.fx_session_state("EURUSD", ts)

        self.assertEqual(fx_state["session"], "weekend_closed")
        self.assertFalse(bool(fx_state["is_open"]))
        self.assertEqual(crypto_state["session"], "open")
        self.assertTrue(bool(crypto_state["is_open"]))

    def test_configured_maintenance_window_closes_crypto_temporarily(self) -> None:
        ts = _utc_ms(2026, 6, 24, 16, 15)
        expected_next_open = _utc_ms(2026, 6, 24, 16, 30)
        env = {
            **_maintenance_disabled_env(),
            "CRYPTO_MAINTENANCE_ENABLED": "1",
            "CRYPTO_MAINTENANCE_START_HOUR_UTC": "16",
            "CRYPTO_MAINTENANCE_START_MINUTE_UTC": "0",
            "CRYPTO_MAINTENANCE_DURATION_MINUTES": "30",
        }

        with patch.dict(os.environ, env, clear=False):
            first = self.crypto_session.crypto_session_state("BTC", ts)
            second = self.crypto_session.crypto_session_state("BTC", ts)
            after_window = self.crypto_session.crypto_session_state("BTC", _utc_ms(2026, 6, 24, 16, 31))

        self.assertEqual(first, second)
        self.assertEqual(first["session"], "maintenance")
        self.assertFalse(bool(first["is_open"]))
        self.assertTrue(bool(first["in_maintenance_window"]))
        self.assertEqual(first["next_open_ms"], expected_next_open)
        self.assertEqual(after_window["session"], "open")
        self.assertTrue(bool(after_window["is_open"]))

    def test_maintenance_symbol_filter_is_respected(self) -> None:
        env = {
            **_maintenance_disabled_env(),
            "CRYPTO_MAINTENANCE_ENABLED": "1",
            "CRYPTO_MAINTENANCE_START_UTC": "16:00",
            "CRYPTO_MAINTENANCE_DURATION_MINUTES": "30",
            "CRYPTO_MAINTENANCE_SYMBOLS": "ETH",
        }

        with patch.dict(os.environ, env, clear=False):
            btc_state = self.crypto_session.crypto_session_state("BTC", _utc_ms(2026, 6, 24, 16, 15))
            eth_state = self.crypto_session.crypto_session_state("ETH", _utc_ms(2026, 6, 24, 16, 15))

        self.assertEqual(btc_state["session"], "open")
        self.assertTrue(bool(btc_state["is_open"]))
        self.assertEqual(eth_state["session"], "maintenance")
        self.assertFalse(bool(eth_state["is_open"]))

    def test_non_crypto_passes_through_and_normalization_returns_bare_root(self) -> None:
        state = self.crypto_session.crypto_session_state("AAPL", _utc_ms(2026, 6, 27, 16))

        self.assertFalse(bool(state["is_crypto"]))
        self.assertEqual(state["session"], "open")
        self.assertEqual(self.crypto_session.normalize_crypto_symbol("BTC/USD"), "BTC")
        self.assertEqual(self.crypto_session.normalize_crypto_symbol("ETHUSDT"), "ETH")

    def test_feature_session_flags_are_crypto_247_without_changing_equity_or_fx(self) -> None:
        feature_registry = importlib.reload(importlib.import_module("engine.strategy.feature_registry"))
        ts = _utc_ms(2026, 6, 24, 23)
        legacy_flags = (0.0, 0.0, 0.0)

        self.assertEqual(feature_registry._session_flags(ts), legacy_flags)
        self.assertEqual(feature_registry._session_flags(ts, asset_class="EQUITY"), legacy_flags)
        self.assertEqual(feature_registry._session_flags(ts, asset_class="FX"), legacy_flags)
        self.assertEqual(feature_registry._session_flags(ts, asset_class="CRYPTO"), (1.0, 1.0, 1.0))

