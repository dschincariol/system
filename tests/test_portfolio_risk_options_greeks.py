from __future__ import annotations

from contextlib import contextmanager
import importlib
import os
from pathlib import Path
import sqlite3
import sys
import unittest
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


CALL_CONTRACT = "SPY270115C00500000"
PUT_CONTRACT = "QQQ270115P00300000"


_ENV_KEYS = {
    "PORTFOLIO_USE_RISK_ENGINE",
    "PORTFOLIO_RISK_MAX_GROSS",
    "PORTFOLIO_RISK_MAX_NET",
    "PORTFOLIO_RISK_MAX_SYMBOL_GROSS",
    "PORTFOLIO_RISK_USE_ASSET_CLASS_BUDGETS",
    "PORTFOLIO_RISK_USE_STRATEGY_BUDGETS",
    "PORTFOLIO_RISK_USE_VOL_CAPS",
    "PORTFOLIO_RISK_USE_CORR_CLUSTERS",
    "PORTFOLIO_RISK_USE_FX_LEVERAGE_CAPS",
    "PORTFOLIO_RISK_USE_FUTURES_MARGIN_CAPS",
    "PORTFOLIO_RISK_USE_MONTE_CARLO",
    "PORTFOLIO_RISK_USE_ALPHA_DECAY_THROTTLE",
    "PORTFOLIO_RISK_VOL_TARGET",
    "PORTFOLIO_RISK_USE_OPTIONS_GREEK_LIMITS",
    "OPTIONS_MAX_POSITION_CONTRACTS",
    "OPTIONS_MARGIN_IMPACT_MAX_FRACTION",
    "OPTIONS_MAX_PORTFOLIO_DELTA_ABS",
    "OPTIONS_MAX_PORTFOLIO_GAMMA_ABS",
    "OPTIONS_MAX_PORTFOLIO_VEGA_ABS",
}

_BASE_ENV = {
    "PORTFOLIO_USE_RISK_ENGINE": "1",
    "PORTFOLIO_RISK_MAX_GROSS": "0",
    "PORTFOLIO_RISK_MAX_NET": "0",
    "PORTFOLIO_RISK_MAX_SYMBOL_GROSS": "0",
    "PORTFOLIO_RISK_USE_ASSET_CLASS_BUDGETS": "0",
    "PORTFOLIO_RISK_USE_STRATEGY_BUDGETS": "0",
    "PORTFOLIO_RISK_USE_VOL_CAPS": "0",
    "PORTFOLIO_RISK_USE_CORR_CLUSTERS": "0",
    "PORTFOLIO_RISK_USE_FX_LEVERAGE_CAPS": "0",
    "PORTFOLIO_RISK_USE_FUTURES_MARGIN_CAPS": "0",
    "PORTFOLIO_RISK_USE_MONTE_CARLO": "0",
    "PORTFOLIO_RISK_USE_ALPHA_DECAY_THROTTLE": "0",
    "PORTFOLIO_RISK_VOL_TARGET": "0",
    "PORTFOLIO_RISK_USE_OPTIONS_GREEK_LIMITS": "1",
}


class _DrawdownOk:
    ok = True
    drawdown = 0.0
    reason_code = "ok"

    def to_dict(self) -> dict[str, object]:
        return {"ok": True, "drawdown": 0.0, "reason_code": "ok"}


@contextmanager
def _risk_module(extra_env: dict[str, str] | None = None):
    previous = {key: os.environ.get(key) for key in _ENV_KEYS}
    try:
        for key in _ENV_KEYS:
            os.environ.pop(key, None)
        os.environ.update(_BASE_ENV)
        os.environ.update(extra_env or {})
        name = "engine.risk.portfolio_risk_engine"
        module = importlib.import_module(name)
        yield importlib.reload(module)
    finally:
        for key in _ENV_KEYS:
            os.environ.pop(key, None)
            if previous[key] is not None:
                os.environ[key] = str(previous[key])


def _memory_db() -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.execute(
        """
        CREATE TABLE broker_account(
          id INTEGER PRIMARY KEY,
          updated_ts_ms INTEGER,
          equity REAL
        )
        """
    )
    con.execute("INSERT INTO broker_account(id, updated_ts_ms, equity) VALUES(1, 1, 100000.0)")
    con.execute(
        """
        CREATE TABLE options_chain_v2(
          contract TEXT NOT NULL,
          ts_ms INTEGER NOT NULL,
          delta REAL,
          gamma REAL,
          theta REAL,
          vega REAL,
          PRIMARY KEY(contract, ts_ms)
        )
        """
    )
    con.execute(
        "INSERT INTO options_chain_v2(contract, ts_ms, delta, gamma, theta, vega) VALUES(?,?,?,?,?,?)",
        (CALL_CONTRACT, 1000, 0.5, 0.02, -0.01, 0.12),
    )
    con.execute(
        "INSERT INTO options_chain_v2(contract, ts_ms, delta, gamma, theta, vega) VALUES(?,?,?,?,?,?)",
        (PUT_CONTRACT, 1000, -0.4, 0.03, -0.02, 0.20),
    )
    con.execute(
        """
        CREATE TABLE portfolio_risk_snapshots(
          ts_ms INTEGER PRIMARY KEY,
          gross REAL NOT NULL,
          net REAL NOT NULL,
          vol_proxy REAL,
          drawdown REAL,
          blocked INTEGER NOT NULL,
          info_json TEXT
        )
        """
    )
    con.execute(
        """
        CREATE TABLE risk_events(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER NOT NULL,
          trigger_type TEXT NOT NULL,
          reason TEXT,
          equity REAL,
          drawdown_pct REAL,
          var_pct REAL,
          concentration REAL,
          positions INTEGER,
          metadata_json TEXT
        )
        """
    )
    con.commit()
    return con


def _two_leg_book() -> dict[str, dict[str, object]]:
    return {
        CALL_CONTRACT: {"symbol": CALL_CONTRACT, "weight": 10.0, "side": "LONG", "contracts": 10.0},
        PUT_CONTRACT: {"symbol": PUT_CONTRACT, "weight": 5.0, "side": "SHORT", "contracts": 5.0},
    }


def _run_engine(module, con: sqlite3.Connection, desired: dict[str, dict[str, object]]):
    with (
        patch.object(module, "evaluate_current_drawdown", return_value=_DrawdownOk()),
        patch.object(module, "set_state", return_value=None),
        patch.object(module, "record_risk_block", return_value=None),
        patch.object(module, "_apply_portfolio_vol_target", side_effect=lambda _con, rows, _info: rows),
    ):
        return module.apply_portfolio_risk_engine(con, desired, {}, 123456)


class PortfolioRiskOptionsGreeksTest(unittest.TestCase):
    def test_options_greek_snapshot_scales_signed_contracts_by_multiplier(self) -> None:
        con = _memory_db()
        with _risk_module() as risk:
            snapshot = risk._options_greek_snapshot(con, _two_leg_book())

        self.assertAlmostEqual(snapshot["net_delta"], 700.0)
        self.assertAlmostEqual(snapshot["net_gamma"], 5.0)
        self.assertAlmostEqual(snapshot["net_theta"], 0.0)
        self.assertAlmostEqual(snapshot["net_vega"], 20.0)
        self.assertAlmostEqual(snapshot["gross_contracts"], 15.0)
        self.assertAlmostEqual(snapshot["max_position_contracts"], 10.0)
        self.assertEqual(snapshot["missing_greeks"], [])

    def test_delta_cap_proportionally_downsizes_before_hard_block(self) -> None:
        con = _memory_db()
        with _risk_module({"OPTIONS_MAX_PORTFOLIO_DELTA_ABS": "350"}) as risk:
            out, info = _run_engine(risk, con, _two_leg_book())

        self.assertFalse(info["blocked"])
        self.assertTrue(info["options_delta_cap_scaled"])
        self.assertAlmostEqual(info["options_delta_cap_scale"], 0.5)
        self.assertAlmostEqual(info["options_greeks_post"]["net_delta"], 350.0)
        self.assertAlmostEqual(info["options_greeks_post"]["net_gamma"], 2.5)
        self.assertAlmostEqual(out[CALL_CONTRACT]["weight"], 5.0)
        self.assertAlmostEqual(out[CALL_CONTRACT]["contracts"], 5.0)
        self.assertEqual(out[PUT_CONTRACT]["side"], "SHORT")
        self.assertAlmostEqual(out[PUT_CONTRACT]["weight"], 2.5)
        self.assertAlmostEqual(out[PUT_CONTRACT]["contracts"], 2.5)
        self.assertTrue(info["post_checks"]["options_greeks_within_cap"])

    def test_gamma_cap_blocks_with_greek_specific_reason(self) -> None:
        con = _memory_db()
        with _risk_module({"OPTIONS_MAX_PORTFOLIO_GAMMA_ABS": "4"}) as risk:
            _out, info = _run_engine(risk, con, _two_leg_book())

        self.assertTrue(info["blocked"])
        self.assertEqual(info["block_reason"]["type"], "options_greek_limit_breached")
        self.assertFalse(info["post_checks"]["options_greeks_within_cap"])
        self.assertIn("gamma", info["post_checks"]["options_greek_violations"])
        self.assertAlmostEqual(info["options_greeks_post"]["net_gamma"], 5.0)

    def test_empty_option_caps_fail_open_with_snapshot(self) -> None:
        con = _memory_db()
        with _risk_module() as risk:
            _out, info = _run_engine(risk, con, _two_leg_book())

        self.assertFalse(info["blocked"])
        self.assertAlmostEqual(info["options_greeks_post"]["net_delta"], 700.0)
        self.assertTrue(info["post_checks"]["options_greeks_within_cap"])
        self.assertNotIn("options_greek_violations", info["post_checks"])

    def test_non_option_keys_match_when_greek_limits_enabled_or_disabled(self) -> None:
        desired = {
            "AAPL": {"symbol": "AAPL", "weight": 0.2, "side": "LONG"},
            "EURUSD": {"symbol": "EURUSD", "weight": 0.1, "side": "LONG"},
        }

        with _risk_module({"PORTFOLIO_RISK_USE_OPTIONS_GREEK_LIMITS": "0"}) as risk:
            _out_disabled, disabled = _run_engine(risk, _memory_db(), dict(desired))
        with _risk_module({"PORTFOLIO_RISK_USE_OPTIONS_GREEK_LIMITS": "1"}) as risk:
            _out_enabled, enabled = _run_engine(risk, _memory_db(), dict(desired))

        self.assertEqual(disabled["final_gross"], enabled["final_gross"])
        self.assertEqual(disabled["final_net"], enabled["final_net"])
        self.assertEqual(
            disabled["post_checks"]["gross_within_cap"],
            enabled["post_checks"]["gross_within_cap"],
        )
        self.assertEqual(
            disabled["post_checks"]["asset_class_within_cap"],
            enabled["post_checks"]["asset_class_within_cap"],
        )
        self.assertEqual(enabled["options_greeks_post"]["net_delta"], 0.0)
        self.assertTrue(enabled["post_checks"]["options_greeks_within_cap"])

    def test_margin_impact_cap_blocks_above_cap(self) -> None:
        con = _memory_db()
        desired = {
            CALL_CONTRACT: {
                "symbol": CALL_CONTRACT,
                "weight": 1.0,
                "side": "LONG",
                "contracts": 1.0,
                "margin_impact_fraction": 0.30,
            }
        }

        with _risk_module({"OPTIONS_MARGIN_IMPACT_MAX_FRACTION": "0.25"}) as risk:
            _out, info = _run_engine(risk, con, desired)

        self.assertTrue(info["blocked"])
        self.assertEqual(info["block_reason"]["type"], "options_greek_limit_breached")
        self.assertIn("margin_impact_fraction", info["post_checks"]["options_greek_violations"])
        self.assertAlmostEqual(info["options_greeks_post"]["margin_impact_fraction"], 0.30)


if __name__ == "__main__":
    unittest.main()
