from __future__ import annotations

import importlib
import sys
import unittest
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

pytestmark = pytest.mark.safety_critical


class CryptoIbkrContractConstructionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.gateway = importlib.reload(importlib.import_module("engine.execution.broker_ibkr_gateway"))

    def test_crypto_contract_uses_ibkr_paxos_base_quote(self) -> None:
        contract = self.gateway._mk_crypto_contract("BTC")

        self.assertEqual(contract.secType, "CRYPTO")
        self.assertEqual(contract.exchange, "PAXOS")
        self.assertEqual(contract.symbol, "BTC")
        self.assertEqual(contract.currency, "USD")

    def test_contract_dispatcher_routes_crypto_fx_and_equity(self) -> None:
        crypto_contract = self.gateway._mk_contract_for_symbol("BTC")
        fx_contract = self.gateway._mk_contract_for_symbol("EURUSD")
        equity_contract = self.gateway._mk_contract_for_symbol("AAPL")

        self.assertEqual(crypto_contract.secType, "CRYPTO")
        self.assertEqual(crypto_contract.exchange, "PAXOS")
        self.assertEqual(crypto_contract.symbol, "BTC")
        self.assertEqual(crypto_contract.currency, "USD")
        self.assertEqual(fx_contract.secType, "CASH")
        self.assertEqual(fx_contract.exchange, "IDEALPRO")
        self.assertEqual(equity_contract.secType, "STK")
        self.assertEqual(equity_contract.exchange, "SMART")

    def test_crypto_symbol_predicate_is_asset_map_bound(self) -> None:
        self.assertTrue(self.gateway._is_crypto_symbol("BTC"))
        self.assertTrue(self.gateway._is_crypto_symbol("BTC/USD"))
        self.assertFalse(self.gateway._is_crypto_symbol("EURUSD"))
        self.assertFalse(self.gateway._is_crypto_symbol("AAPL"))

    def test_normalize_crypto_symbol_returns_bare_root(self) -> None:
        self.assertEqual(self.gateway.normalize_crypto_symbol("BTC/USD"), "BTC")
        self.assertEqual(self.gateway.normalize_crypto_symbol("ETHUSDT"), "ETH")

    def test_all_local_crypto_normalizers_share_asset_map_matrix(self) -> None:
        broker_router = importlib.reload(importlib.import_module("engine.execution.broker_router"))
        crypto_costs = importlib.reload(importlib.import_module("engine.execution.crypto_costs"))
        crypto_session = importlib.reload(importlib.import_module("engine.execution.crypto_session"))
        crypto_sizing = importlib.reload(importlib.import_module("engine.strategy.crypto_sizing"))
        asset_map = importlib.reload(importlib.import_module("engine.data.asset_map"))
        normalizers = {
            "broker_ibkr_gateway": self.gateway.normalize_crypto_symbol,
            "broker_router": broker_router.normalize_crypto_symbol,
            "crypto_costs": crypto_costs.normalize_crypto_symbol,
            "crypto_session": crypto_session.normalize_crypto_symbol,
            "crypto_sizing": crypto_sizing.normalize_crypto_symbol,
        }
        positive_cases = {
            "BTC": "BTC",
            "BTCUSD": "BTC",
            "BTC/USD": "BTC",
            "BTC/USDT:USDT": "BTC",
            "ETHUSDT": "ETH",
            "SOLUSDC": "SOL",
            "XBTUSD": "BTC",
        }

        for raw_symbol, expected_root in positive_cases.items():
            for name, normalize in normalizers.items():
                root = normalize(raw_symbol)
                self.assertEqual(root, expected_root, f"{name} disagreed for {raw_symbol}")
                self.assertEqual(asset_map.asset_class_for_symbol(root), "CRYPTO", f"{name} produced non-crypto root")

        for raw_symbol in ("EURUSD", "AAPL"):
            for name, normalize in normalizers.items():
                root = normalize(raw_symbol)
                self.assertNotEqual(asset_map.asset_class_for_symbol(root), "CRYPTO", f"{name} misclassified {raw_symbol}")

    def test_xbtusd_routes_and_classifies_as_crypto_everywhere(self) -> None:
        broker_router = importlib.reload(importlib.import_module("engine.execution.broker_router"))
        crypto_costs = importlib.reload(importlib.import_module("engine.execution.crypto_costs"))
        crypto_session = importlib.reload(importlib.import_module("engine.execution.crypto_session"))
        crypto_sizing = importlib.reload(importlib.import_module("engine.strategy.crypto_sizing"))

        contract = self.gateway._mk_crypto_contract("XBTUSD")

        self.assertEqual(contract.symbol, "BTC")
        self.assertEqual(contract.currency, "USD")
        self.assertTrue(self.gateway._is_crypto_symbol("XBTUSD"))
        self.assertTrue(broker_router._is_crypto_order_symbol("XBTUSD"))
        self.assertTrue(broker_router._batch_has_crypto([{"symbol": "XBTUSD"}]))
        self.assertTrue(crypto_costs.is_crypto_symbol("XBTUSD"))
        self.assertTrue(crypto_session._is_crypto_symbol("XBTUSD"))
        sizing_instrument = crypto_sizing._crypto_instrument(None, "XBTUSD")
        self.assertIsNotNone(sizing_instrument)
        self.assertEqual(sizing_instrument["symbol"], "BTC")

        for raw_symbol in ("EURUSD", "AAPL"):
            self.assertFalse(self.gateway._is_crypto_symbol(raw_symbol))
            self.assertFalse(broker_router._is_crypto_order_symbol(raw_symbol))
            self.assertFalse(crypto_costs.is_crypto_symbol(raw_symbol))
            self.assertFalse(crypto_session._is_crypto_symbol(raw_symbol))
            self.assertIsNone(crypto_sizing._crypto_instrument(None, raw_symbol))
