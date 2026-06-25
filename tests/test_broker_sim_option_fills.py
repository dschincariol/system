from __future__ import annotations

import importlib
import os
import sys
import tempfile
import time
import unittest
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


OPTION_CONTRACT = "SPY270115C00500000"


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


@dataclass(frozen=True)
class _OptionMeta:
    occ_symbol: str
    underlying: str
    multiplier: float


class BrokerSimOptionFillTests(unittest.TestCase):
    ENV_KEYS = (
        "DB_PATH",
        "TS_TESTING",
        "TS_STORAGE_BACKEND",
        "BROKER_START_CASH",
        "BROKER_START_EQUITY",
        "BROKER_SPREAD_BPS",
        "BROKER_SLIPPAGE_BPS",
        "BROKER_FEE_BPS",
        "BROKER_IMPACT_ALPHA",
        "BROKER_MAX_TRADE_PCT_EQUITY",
        "BROKER_CHUNK_PCT",
        "BROKER_LATENCY_MS",
        "BROKER_LATENCY_SLEEP",
        "BROKER_ALLOW_MARGIN",
        "BROKER_OPTION_MAX_QUOTE_AGE_MS",
        "BROKER_MAX_PRICE_AGE_MS",
        "OPTIONS_SIM_MARGIN_UNDERLYING_FRACTION",
        "EXEC_PORTFOLIO_TOTAL_EXPOSURE_CAP",
        "EXEC_PORTFOLIO_DIRECTION_CONCENTRATION_CAP",
        "EXEC_PORTFOLIO_SYMBOL_CONCENTRATION_CAP",
        "PORTFOLIO_GROSS_CAP",
        "PORTFOLIO_RISK_MAX_GROSS",
        "PORTFOLIO_RISK_MAX_NET",
        "DEPLOYABLE_EQUITY_MODE",
        "DEPLOYABLE_BP_FACTOR",
        "DEPLOYABLE_CASH_FACTOR",
        "DEPLOYABLE_EQUITY_FACTOR",
    )

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = Path(self.tmp.name) / "broker_sim_options.db"
        self._env_backup = {key: os.environ.get(key) for key in self.ENV_KEYS}

        os.environ["DB_PATH"] = str(self.db_path)
        os.environ["TS_TESTING"] = "1"
        os.environ["TS_STORAGE_BACKEND"] = "sqlite"
        os.environ["BROKER_START_CASH"] = "100000"
        os.environ["BROKER_START_EQUITY"] = "100000"
        os.environ["BROKER_SPREAD_BPS"] = "0"
        os.environ["BROKER_SLIPPAGE_BPS"] = "0"
        os.environ["BROKER_FEE_BPS"] = "0"
        os.environ["BROKER_IMPACT_ALPHA"] = "0"
        os.environ["BROKER_MAX_TRADE_PCT_EQUITY"] = "1.0"
        os.environ["BROKER_CHUNK_PCT"] = "1.0"
        os.environ["BROKER_LATENCY_MS"] = "0"
        os.environ["BROKER_LATENCY_SLEEP"] = "0"
        os.environ["BROKER_ALLOW_MARGIN"] = "1"
        os.environ["BROKER_OPTION_MAX_QUOTE_AGE_MS"] = "3600000"
        os.environ["BROKER_MAX_PRICE_AGE_MS"] = "3600000"
        os.environ["OPTIONS_SIM_MARGIN_UNDERLYING_FRACTION"] = "0.20"
        os.environ["EXEC_PORTFOLIO_TOTAL_EXPOSURE_CAP"] = "10.0"
        os.environ["EXEC_PORTFOLIO_DIRECTION_CONCENTRATION_CAP"] = "10.0"
        os.environ["EXEC_PORTFOLIO_SYMBOL_CONCENTRATION_CAP"] = "10.0"
        os.environ["PORTFOLIO_GROSS_CAP"] = "10.0"
        os.environ["PORTFOLIO_RISK_MAX_GROSS"] = "10.0"
        os.environ["PORTFOLIO_RISK_MAX_NET"] = "10.0"
        os.environ["DEPLOYABLE_EQUITY_MODE"] = "equity"
        os.environ["DEPLOYABLE_BP_FACTOR"] = "1.0"
        os.environ["DEPLOYABLE_CASH_FACTOR"] = "1.0"
        os.environ["DEPLOYABLE_EQUITY_FACTOR"] = "1.0"

        self.storage, _deployable_capital, self.broker_sim, self.execution_ledger = _reload_modules(
            "engine.runtime.storage",
            "engine.execution.deployable_capital",
            "engine.execution.broker_sim",
            "engine.execution.execution_ledger",
        )
        self.storage.init_db()
        self.broker_sim.init_broker_db()
        self.execution_ledger.init_execution_ledger()
        self.now_ms = int(time.time() * 1000)

    def tearDown(self) -> None:
        try:
            (storage,) = _reload_modules("engine.runtime.storage")
            storage.close_pooled_connections()
        except Exception:
            pass

        for key, value in self._env_backup.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        self.tmp.cleanup()

    def _seed_price(self, symbol: str, px: float, *, ts_ms: int | None = None) -> None:
        con = self.storage.connect()
        try:
            con.execute(
                """
                INSERT INTO prices(ts_ms, symbol, price, px, source)
                VALUES(?,?,?,?,?)
                ON CONFLICT(symbol, ts_ms) DO UPDATE SET
                  price=excluded.price,
                  px=excluded.px,
                  source=excluded.source
                """,
                (int(ts_ms or self.now_ms), str(symbol), float(px), float(px), "test"),
            )
            con.commit()
        finally:
            con.close()

    def _seed_option_quote(
        self,
        contract: str = OPTION_CONTRACT,
        *,
        bid: float = 4.5,
        ask: float = 5.5,
        ts_ms: int | None = None,
    ) -> None:
        con = self.storage.connect()
        try:
            con.execute(
                """
                INSERT INTO options_chain_v2(
                  ts_ms, underlying, contract, expiration, contract_type, strike,
                  iv, open_interest, volume, bid, ask, delta, gamma, theta, vega, source
                )
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(contract, ts_ms) DO UPDATE SET
                  bid=excluded.bid,
                  ask=excluded.ask,
                  source=excluded.source
                """,
                (
                    int(ts_ms or self.now_ms),
                    "SPY",
                    str(contract),
                    "2027-01-15",
                    "call",
                    500.0,
                    None,
                    1000.0,
                    100.0,
                    float(bid),
                    float(ask),
                    None,
                    None,
                    None,
                    None,
                    "test",
                ),
            )
            con.commit()
        finally:
            con.close()

    def _apply_one_order(self, order: dict, *, order_id: int = 1) -> dict:
        with patch("engine.execution.kill_switch.execution_allowed", return_value=(True, None, None)):
            with patch.object(self.broker_sim, "get_execution_liquidity_snapshot", return_value={}):
                with patch.object(self.broker_sim, "_earnings_proximity_decay", return_value=0.0):
                    with patch.object(self.broker_sim, "_get_factor_feature_asof", return_value=0.0):
                        with patch.object(self.broker_sim, "_prime_broker_order_state_after_commit", return_value=None):
                            return self.broker_sim.apply_new_portfolio_orders(
                                dry_run=False,
                                override_orders=[dict(order)],
                                override_order_id=int(order_id),
                                override_ts_ms=int(self.now_ms),
                            )

    def _fetch_fill(self, symbol: str):
        con = self.storage.connect(readonly=True)
        try:
            return con.execute(
                """
                SELECT qty, px, contract_multiplier, option_quote_source, option_margin_debit, note
                FROM broker_fills
                WHERE symbol=?
                ORDER BY id DESC
                LIMIT 1
                """,
                (str(symbol),),
            ).fetchone()
        finally:
            con.close()

    def _fetch_position_qty(self, symbol: str) -> float | None:
        con = self.storage.connect(readonly=True)
        try:
            row = con.execute("SELECT qty FROM broker_positions WHERE symbol=?", (str(symbol),)).fetchone()
        finally:
            con.close()
        return None if row is None else float(row[0])

    def _fetch_cash(self) -> float:
        con = self.storage.connect(readonly=True)
        try:
            columns = {str(row[1]) for row in con.execute("PRAGMA table_info(broker_account)").fetchall()}
            if "id" in columns:
                row = con.execute("SELECT cash FROM broker_account WHERE id=1").fetchone()
            else:
                row = con.execute(
                    """
                    SELECT cash
                    FROM broker_account
                    ORDER BY COALESCE(updated_ts_ms, ts_ms, 0) DESC, ts_ms DESC
                    LIMIT 1
                    """
                ).fetchone()
            return float(row[0])
        finally:
            con.close()

    def test_long_option_fill_uses_chain_ask_multiplier_and_chain_mtm(self) -> None:
        self._seed_price("SPY", 500.0)
        self._seed_price(OPTION_CONTRACT, 999.0)
        self._seed_option_quote(bid=4.5, ask=5.5)

        result = self._apply_one_order(
            {"source_order_id": 501, "symbol": OPTION_CONTRACT, "to_side": "LONG", "to_weight": 0.01},
            order_id=501,
        )

        self.assertTrue(result.get("ok"), result)
        self.assertEqual(result.get("status"), "applied")
        self.assertEqual(self._fetch_position_qty(OPTION_CONTRACT), 2.0)

        fill = self._fetch_fill(OPTION_CONTRACT)
        self.assertIsNotNone(fill)
        qty, px, multiplier, quote_source, margin_debit, note = fill
        self.assertEqual(float(qty), 2.0)
        self.assertAlmostEqual(float(px), 5.5, places=6)
        self.assertAlmostEqual(float(multiplier), 100.0, places=6)
        self.assertIn("options_chain_v2:SPY270115C00500000", str(quote_source))
        self.assertIsNone(margin_debit)
        self.assertNotIn("option_sim_margin_reference", str(note))
        self.assertAlmostEqual(self._fetch_cash(), 98900.0, places=6)

        mtm = self.broker_sim.broker_equity_at(self.now_ms, include_prices=True)
        self.assertTrue(mtm.get("ok"), mtm)
        self.assertEqual(mtm.get("missing_prices"), [])
        self.assertAlmostEqual(float(mtm["equity"]), 99900.0, places=6)
        self.assertEqual(len(mtm["positions"]), 1)
        self.assertAlmostEqual(float(mtm["positions"][0]["px"]), 5.0, places=6)
        self.assertAlmostEqual(float(mtm["positions"][0]["notional"]), 1000.0, places=6)
        self.assertAlmostEqual(float(mtm["positions"][0]["contract_multiplier"]), 100.0, places=6)

    def test_option_without_chain_quote_does_not_fill_or_mtm_from_underlying_prices(self) -> None:
        self._seed_price("SPY", 500.0)
        self._seed_price(OPTION_CONTRACT, 999.0)

        result = self._apply_one_order(
            {"source_order_id": 502, "symbol": OPTION_CONTRACT, "to_side": "LONG", "to_weight": 0.01},
            order_id=502,
        )

        self.assertTrue(result.get("ok"), result)
        self.assertEqual(result.get("status"), "no_changes")
        self.assertIsNone(self._fetch_position_qty(OPTION_CONTRACT))

        con = self.storage.connect()
        try:
            con.execute(
                "INSERT INTO broker_positions(symbol, qty, avg_px, updated_ts_ms) VALUES(?,?,?,?)",
                (OPTION_CONTRACT, 1.0, 1.0, self.now_ms),
            )
            con.commit()
        finally:
            con.close()

        mtm = self.broker_sim.broker_equity_at(self.now_ms, include_prices=True)
        self.assertIn(OPTION_CONTRACT, mtm.get("missing_prices") or [])
        self.assertEqual(mtm.get("positions"), [])

    def test_option_sizing_uses_metadata_multiplier_not_hardcoded_100(self) -> None:
        self._seed_price("SPY", 500.0)
        self._seed_option_quote(bid=4.5, ask=5.5)
        meta = _OptionMeta(occ_symbol=OPTION_CONTRACT, underlying="SPY", multiplier=50.0)

        with patch.object(self.broker_sim, "_option_contract_meta", return_value=meta):
            result = self._apply_one_order(
                {"source_order_id": 503, "symbol": OPTION_CONTRACT, "to_side": "LONG", "to_weight": 0.01},
                order_id=503,
            )

        self.assertTrue(result.get("ok"), result)
        self.assertEqual(self._fetch_position_qty(OPTION_CONTRACT), 4.0)
        fill = self._fetch_fill(OPTION_CONTRACT)
        self.assertIsNotNone(fill)
        self.assertEqual(float(fill[0]), 4.0)
        self.assertAlmostEqual(float(fill[2]), 50.0, places=6)
        self.assertAlmostEqual(self._fetch_cash(), 98900.0, places=6)

    def test_short_option_records_reference_margin_debit(self) -> None:
        self._seed_price("SPY", 500.0)
        self._seed_option_quote(bid=4.5, ask=5.5)

        result = self._apply_one_order(
            {"source_order_id": 504, "symbol": OPTION_CONTRACT, "to_side": "SHORT", "qty": -1.0},
            order_id=504,
        )

        self.assertTrue(result.get("ok"), result)
        self.assertEqual(self._fetch_position_qty(OPTION_CONTRACT), -1.0)
        fill = self._fetch_fill(OPTION_CONTRACT)
        self.assertIsNotNone(fill)
        qty, px, multiplier, quote_source, margin_debit, note = fill
        self.assertEqual(float(qty), -1.0)
        self.assertAlmostEqual(float(px), 4.5, places=6)
        self.assertAlmostEqual(float(multiplier), 100.0, places=6)
        self.assertIn("options_chain_v2:SPY270115C00500000", str(quote_source))
        self.assertAlmostEqual(float(margin_debit), 10500.0, places=6)
        self.assertIn("option_sim_margin_reference", str(note))
        self.assertAlmostEqual(self._fetch_cash(), 89950.0, places=6)

    def test_non_option_equity_and_fx_paths_keep_plain_accounting(self) -> None:
        self._seed_price("AAPL", 100.0)
        self._seed_price("EURUSD", 1.1)

        result = self._apply_one_order(
            {"source_order_id": 505, "symbol": "AAPL", "to_side": "LONG", "to_weight": 0.01},
            order_id=505,
        )

        self.assertTrue(result.get("ok"), result)
        self.assertEqual(self._fetch_position_qty("AAPL"), 10.0)
        fill = self._fetch_fill("AAPL")
        self.assertIsNotNone(fill)
        qty, px, multiplier, quote_source, margin_debit, _note = fill
        self.assertEqual(float(qty), 10.0)
        self.assertAlmostEqual(float(px), 100.0, places=6)
        self.assertIsNone(multiplier)
        self.assertIsNone(quote_source)
        self.assertIsNone(margin_debit)
        self.assertAlmostEqual(self._fetch_cash(), 99000.0, places=6)

        result = self._apply_one_order(
            {"source_order_id": 506, "symbol": "EURUSD", "to_side": "LONG", "to_weight": 0.001},
            order_id=506,
        )

        self.assertTrue(result.get("ok"), result)
        self.assertAlmostEqual(float(self._fetch_position_qty("EURUSD") or 0.0), 100.0 / 1.1, places=6)
        fill = self._fetch_fill("EURUSD")
        self.assertIsNotNone(fill)
        qty, px, multiplier, quote_source, margin_debit, _note = fill
        self.assertAlmostEqual(float(qty), 100.0 / 1.1, places=6)
        self.assertAlmostEqual(float(px), 1.1, places=6)
        self.assertIsNone(multiplier)
        self.assertIsNone(quote_source)
        self.assertIsNone(margin_debit)
        self.assertAlmostEqual(self._fetch_cash(), 98900.0, places=6)

        mtm = self.broker_sim.broker_equity_at(self.now_ms, include_prices=True)
        self.assertEqual(mtm.get("missing_prices"), [])
        self.assertAlmostEqual(float(mtm["equity"]), 100000.0, places=6)


if __name__ == "__main__":
    unittest.main()
