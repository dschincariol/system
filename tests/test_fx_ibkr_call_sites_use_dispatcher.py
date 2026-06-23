from __future__ import annotations

import importlib
import inspect
import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

pytestmark = pytest.mark.safety_critical


class FxIbkrDispatcherCallSiteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.gateway = importlib.reload(importlib.import_module("engine.execution.broker_ibkr_gateway"))

    def test_order_construction_sites_use_symbol_dispatcher(self) -> None:
        source = inspect.getsource(self.gateway)

        self.assertEqual(source.count("contract = _mk_contract_for_symbol(symbol)"), 4)
        self.assertNotIn("contract = _mk_stock_contract(symbol)", source)

    def test_dry_run_preview_does_not_connect_or_place_orders(self) -> None:
        orders = [{"symbol": "EURUSD", "qty": 1.0, "side": "BUY"}]
        fake_con = Mock()
        fake_con.close = Mock()

        with patch.object(self.gateway, "connect", return_value=fake_con), \
            patch.object(self.gateway, "apply_alpha_lifecycle", return_value=(orders, {})), \
            patch.object(self.gateway, "get_state", return_value="0"), \
            patch.object(self.gateway, "execution_allowed", return_value=(True, "", {})), \
            patch.object(self.gateway, "compute_deployable_equity_from_env", return_value=100_000.0), \
            patch.object(self.gateway, "_connect_ib", side_effect=AssertionError("dry run must not connect to IBKR")):
            result = self.gateway.apply_latest_portfolio_orders_live(
                dry_run=True,
                override_orders=orders,
                override_order_id=42,
                override_ts_ms=1_700_000_000_000,
            )

        self.assertTrue(bool(result["ok"]))
        self.assertEqual(result["status"], "dry_run_preview")
