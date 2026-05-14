from __future__ import annotations

import importlib
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


class AllocationQuarantineTests(unittest.TestCase):
    def test_legacy_allocation_engine_fails_closed(self) -> None:
        module = importlib.import_module("allocation_engine")

        self.assertIsNone(module.DEFAULT_ENGINE)
        with self.assertRaises(RuntimeError) as ctx:
            module.AllocationEngine()
        self.assertIn("quarantined", str(ctx.exception).lower())
        self.assertIn("portfolio", str(ctx.exception).lower())

        with self.assertRaises(RuntimeError) as fn_ctx:
            module.allocate_capital(1000.0, [])
        self.assertIn("blocked_entrypoint=allocate_capital", str(fn_ctx.exception))

    def test_legacy_hrp_allocator_fails_closed(self) -> None:
        module = importlib.import_module("engine.strategy.hrp_allocator")

        with self.assertRaises(RuntimeError) as ctx:
            module.hrp_optimize_desired(None, {}, gross_cap=1.0, lookback=20)
        self.assertIn("quarantined", str(ctx.exception).lower())
        self.assertIn("portfolio", str(ctx.exception).lower())


if __name__ == "__main__":
    unittest.main()
