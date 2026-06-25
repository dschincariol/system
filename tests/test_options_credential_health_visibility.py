from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from engine.data._credentials import clear_data_credential_cache
from engine.runtime import health


class OptionsCredentialHealthVisibilityTests(unittest.TestCase):
    def setUp(self) -> None:
        clear_data_credential_cache()

    def tearDown(self) -> None:
        clear_data_credential_cache()

    def _patch_credentials(self, value: str):
        return patch("engine.data._credentials.get_data_credential", return_value=value)

    def test_no_credentials_is_benign_and_visible(self) -> None:
        with patch.dict(
            os.environ,
            {
                "POLYGON_API_KEY": "",
                "POLYGON_KEY": "",
                "POLYGON_API_KEY_FILE": "",
                "TRADIER_API_TOKEN": "",
                "TRADIER_API_TOKEN_FILE": "",
                "TS_SECRETS_PROVIDER": "",
                "TS_ENV": "",
            },
            clear=False,
        ), self._patch_credentials(""), patch.object(health, "get_pipeline_status", return_value=None):
            snapshot = health._options_ingestion_snapshot(1_700_000_000_000)

        self.assertFalse(snapshot["available"])
        self.assertFalse(snapshot["degraded"])
        self.assertTrue(snapshot["ok"])
        self.assertEqual(snapshot["detail"], "options_provider_unconfigured")
        self.assertFalse(snapshot["credentials_configured"])

    def test_credentials_configured_and_stale_chain_is_degraded_visible(self) -> None:
        now_ms = 1_700_000_000_000
        status = {
            "ok": False,
            "last_ingested_ts_ms": now_ms - int((health.HEALTH_OPTIONS_MAX_AGE_S + 60) * 1000),
            "meta": {
                "fresh_symbols": [],
                "cached_symbols": ["SPY"],
                "failed_symbols": ["QQQ"],
                "disabled_symbols": [],
                "critical_symbols": [],
                "critical_unavailable_symbols": [],
                "symbol_status": {},
            },
        }

        with self._patch_credentials("token"), patch.object(health, "get_pipeline_status", return_value=status):
            snapshot = health._options_ingestion_snapshot(now_ms)

        self.assertTrue(snapshot["credentials_configured"])
        self.assertTrue(snapshot["degraded"])
        self.assertFalse(snapshot["ok"])
        self.assertEqual(snapshot["status"], "degraded")
        self.assertEqual(snapshot["detail"], "options_credentials_configured_but_chain_stale")

    def test_credentials_configured_and_fresh_chain_is_ok(self) -> None:
        now_ms = 1_700_000_000_000
        status = {
            "ok": True,
            "last_ingested_ts_ms": now_ms - 30_000,
            "meta": {
                "fresh_symbols": ["SPY"],
                "cached_symbols": [],
                "failed_symbols": [],
                "disabled_symbols": [],
                "critical_symbols": [],
                "critical_unavailable_symbols": [],
                "symbol_status": {},
            },
        }

        with self._patch_credentials("token"), patch.object(health, "get_pipeline_status", return_value=status):
            snapshot = health._options_ingestion_snapshot(now_ms)

        self.assertTrue(snapshot["credentials_configured"])
        self.assertFalse(snapshot["degraded"])
        self.assertTrue(snapshot["ok"])
        self.assertEqual(snapshot["status"], "ok")

    def test_existing_snapshot_keys_still_present(self) -> None:
        with self._patch_credentials(""), patch.object(health, "get_pipeline_status", return_value=None):
            snapshot = health._options_ingestion_snapshot(1_700_000_000_000)

        for key in (
            "fresh_symbols",
            "cached_symbols",
            "failed_symbols",
            "disabled_symbols",
            "critical_symbols",
            "critical_unavailable_symbols",
        ):
            self.assertIn(key, snapshot)
            self.assertIsInstance(snapshot[key], list)
        self.assertIn("max_age_s", snapshot)
        self.assertIsInstance(snapshot["max_age_s"], float)
        self.assertIn("stale", snapshot)
        self.assertIn("failed", snapshot)


if __name__ == "__main__":
    unittest.main()
