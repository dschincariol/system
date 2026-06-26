from __future__ import annotations

import importlib
import os
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

pytestmark = pytest.mark.safety_critical


class FxIbkrContractConstructionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.gateway = importlib.reload(importlib.import_module("engine.execution.broker_ibkr_gateway"))

    def test_fx_contract_uses_cash_idealpro_base_quote(self) -> None:
        contract = self.gateway._mk_fx_contract("EURUSD")

        self.assertEqual(contract.secType, "CASH")
        self.assertEqual(contract.exchange, "IDEALPRO")
        self.assertEqual(contract.symbol, "EUR")
        self.assertEqual(contract.currency, "USD")

    def test_contract_dispatcher_preserves_equity_stock_contracts(self) -> None:
        fx_contract = self.gateway._mk_contract_for_symbol("EURUSD")
        equity_contract = self.gateway._mk_contract_for_symbol("AAPL")

        self.assertEqual(fx_contract.secType, "CASH")
        self.assertEqual(fx_contract.exchange, "IDEALPRO")
        self.assertEqual(equity_contract.secType, "STK")
        self.assertEqual(equity_contract.exchange, "SMART")
        self.assertEqual(equity_contract.symbol, "AAPL")
        self.assertEqual(equity_contract.currency, "USD")

    def test_fx02_parser_is_consulted_for_dispatch(self) -> None:
        from engine.data import fx_instrument

        parsed = SimpleNamespace(
            base_ccy="GBP",
            quote_ccy="USD",
            instrument_kind="fx_spot",
        )
        with patch.object(fx_instrument, "parse_fx_symbol", return_value=parsed) as parser:
            self.assertTrue(self.gateway._is_fx_symbol("GBPUSD"))
            contract = self.gateway._mk_contract_for_symbol("GBPUSD")

        parser.assert_called()
        self.assertEqual(contract.secType, "CASH")
        self.assertEqual(contract.symbol, "GBP")
        self.assertEqual(contract.currency, "USD")

    def test_non_pairs_are_not_fx_contracts(self) -> None:
        self.assertFalse(self.gateway._is_fx_symbol("AAPL"))
        self.assertFalse(self.gateway._is_fx_symbol("GOOGLE"))
        self.assertEqual(self.gateway._mk_contract_for_symbol("AAPL").secType, "STK")

    def test_direct_live_fx_submit_blocks_when_fx_live_flag_unset(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("FX_LIVE_TRADING_ENABLED", None)
            with patch.object(
                self.gateway,
                "_real_trading_gate",
                return_value={"ok": True, "real_trading_allowed": True},
            ), patch.object(
                self.gateway,
                "_prelive_reconcile_or_block",
                return_value=None,
            ), patch.object(
                self.gateway,
                "_ibkr_credentials_block",
                return_value=None,
            ), patch.object(
                self.gateway,
                "record_broker_action_audit",
                Mock(side_effect=AssertionError("FX submit audit should not run when FX live is disabled")),
            ), patch.object(
                self.gateway,
                "_connect_ib",
                Mock(side_effect=AssertionError("IBKR connection should not open when FX live is disabled")),
            ):
                result = self.gateway.submit_market_order("EURUSD", 1000.0, "fx_live_disabled_test")

        self.assertFalse(bool(result["ok"]))
        self.assertEqual(result["status"], "fx_live_trading_disabled_by_default")
        self.assertEqual(result["reason"], "fx_live_trading_disabled_by_default")
        self.assertEqual(result["broker"], "ibkr")
        self.assertTrue(bool(result["stop_failover"]))
        self.assertEqual(result["env"]["FX_LIVE_TRADING_ENABLED"], "0")
