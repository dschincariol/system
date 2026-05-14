from __future__ import annotations

import unittest
from pathlib import Path
from types import SimpleNamespace

from engine.api import server


REPO_ROOT = Path(__file__).resolve().parents[1]


class ApiServerContractTests(unittest.TestCase):
    def test_run_server_uses_dashboard_control_plane_runner(self) -> None:
        seen = {}

        def _runner():
            seen["called"] = True
            return {"ok": True}

        result = server.run_server(dashboard_module=SimpleNamespace(_run_dashboard_control_plane=_runner))

        self.assertEqual(result, {"ok": True})
        self.assertTrue(seen.get("called"))

    def test_run_server_requires_dashboard_control_plane_runner(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "dashboard_control_plane_runner_unavailable"):
            server.run_server(dashboard_module=SimpleNamespace())

    def test_start_system_imports_authoritative_api_server_entrypoint(self) -> None:
        text = (REPO_ROOT / "start_system.py").read_text(encoding="utf-8")

        self.assertIn("from engine.api.server import run_server as _rs", text)
        self.assertNotIn("from dashboard_server import run_server as _rs", text)


if __name__ == "__main__":
    unittest.main()
