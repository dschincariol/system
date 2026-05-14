from __future__ import annotations

import unittest
from types import SimpleNamespace

from engine.runtime.dashboard_runtime_boot import launch_post_bind_runtime_threads, run_post_bind_boot_safe


class DashboardRuntimeBootTests(unittest.TestCase):
    def test_launch_post_bind_runtime_threads_uses_dashboard_launcher(self) -> None:
        calls = []
        handler_ctx = {"ctx": True}

        def _start_background_thread(name, target, args=()):
            calls.append((name, target, args))
            return name

        dashboard_module = SimpleNamespace(
            _start_background_thread=_start_background_thread,
            _prewarm_health_cache=lambda _ctx: None,
        )

        launch_post_bind_runtime_threads(dashboard_module, handler_ctx)

        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0][0], "health_cache_prewarm")
        self.assertEqual(calls[0][2], (handler_ctx,))
        self.assertEqual(calls[1][0], "post_bind_boot")
        self.assertIs(calls[1][1], run_post_bind_boot_safe)
        self.assertEqual(calls[1][2], (dashboard_module, handler_ctx))


if __name__ == "__main__":
    unittest.main()
