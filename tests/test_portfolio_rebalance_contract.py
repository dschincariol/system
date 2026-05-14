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


def _reload_module():
    import engine.strategy.portfolio_rebalance as portfolio_rebalance

    return importlib.reload(portfolio_rebalance)


class PortfolioRebalanceContractTests(unittest.TestCase):
    def test_main_returns_zero_when_preflight_smoke_sees_lock_held(self) -> None:
        portfolio_rebalance = _reload_module()
        with patch.dict(os.environ, {"PREFLIGHT_SMOKE": "1"}, clear=False):
            with patch.object(portfolio_rebalance, "init_db"):
                with patch.object(portfolio_rebalance, "acquire_job_lock", return_value=False):
                    with patch("builtins.print") as print_mock:
                        rc = portfolio_rebalance.main()

        self.assertEqual(rc, 0)
        printed = "".join(str(arg) for arg in print_mock.call_args.args)
        self.assertIn('"status": "lock_held"', printed)

    def test_main_returns_two_when_lock_held_outside_preflight_smoke(self) -> None:
        portfolio_rebalance = _reload_module()
        with patch.dict(os.environ, {"PREFLIGHT_SMOKE": "0"}, clear=False):
            with patch.object(portfolio_rebalance, "init_db"):
                with patch.object(portfolio_rebalance, "acquire_job_lock", return_value=False):
                    rc = portfolio_rebalance.main()

        self.assertEqual(rc, 2)


if __name__ == "__main__":
    unittest.main()
