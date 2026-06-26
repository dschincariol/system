from __future__ import annotations

import importlib
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_router():
    return importlib.reload(importlib.import_module("engine.execution.broker_router"))


class BrokerRouterDegradedProbeFailClosedTests(unittest.TestCase):
    def setUp(self) -> None:
        self.router = _reload_router()

    def test_execution_health_reader_is_bound_on_import(self) -> None:
        self.assertIsNotNone(self.router._read_execution_health)
        self.assertTrue(callable(self.router._read_execution_health))

    def test_missing_execution_health_reader_returns_warning_active_snapshot(self) -> None:
        with patch.object(self.router, "_read_execution_health", None):
            out = self.router._execution_degraded_from_cache()

        self.assertIs(out["active"], True)
        self.assertEqual(out["severity"], "WARNING")
        self.assertIn("execution_health_reader_unavailable", out["reason_codes"])

    def test_raising_execution_health_reader_returns_warning_active_snapshot(self) -> None:
        def _raise() -> dict:
            raise RuntimeError("execution health read failed in test")

        with patch.object(self.router, "_read_execution_health", _raise):
            out = self.router._execution_degraded_from_cache()

        self.assertIs(out["active"], True)
        self.assertEqual(out["severity"], "WARNING")
        self.assertIn("execution_health_read_failed", out["reason_codes"])

    def test_unprimed_or_empty_execution_health_is_not_degraded(self) -> None:
        for value in (None, {}):
            with self.subTest(value=value), patch.object(
                self.router,
                "_read_execution_health",
                lambda value=value: value,
            ):
                out = self.router._execution_degraded_from_cache()

            self.assertIs(out["active"], False)

    def test_critical_execution_health_preserves_existing_critical_mapping(self) -> None:
        with patch.object(self.router, "_read_execution_health", lambda: {"state": "critical"}):
            out = self.router._execution_degraded_from_cache()

        self.assertIs(out["active"], True)
        self.assertEqual(out["severity"], "CRITICAL")
        self.assertEqual(out["reason"], "execution_health_critical")
        self.assertIn("execution_health_critical", out["reason_codes"])

    def test_safe_default_live_gate_still_blocks_when_degraded_probe_warns(self) -> None:
        with patch.dict(os.environ, {}, clear=True), patch.object(
            self.router,
            "_read_execution_health",
            None,
        ):
            block = self.router._execution_gate_or_block(dry_run=False)

        self.assertIsInstance(block, dict)
        self.assertIn(
            block.get("status"),
            {
                "execution_blocked",
                "execution_blocked_gate_unavailable",
                "execution_blocked_gate_providers_missing",
            },
        )


if __name__ == "__main__":
    unittest.main()
