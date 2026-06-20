from __future__ import annotations

import importlib
import json
import os
import sqlite3
import sys
import tempfile
import time
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _create_l2_table(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS market_microstructure_signals (
          ts_ms INTEGER NOT NULL,
          symbol TEXT NOT NULL,
          provider TEXT NOT NULL,
          mid_px REAL,
          bid_px REAL,
          ask_px REAL,
          bid_sz REAL,
          ask_sz REAL,
          spread_bps REAL,
          spread_z REAL,
          spread_widening REAL,
          order_book_imbalance REAL,
          trade_buy_volume REAL,
          trade_sell_volume REAL,
          trade_aggressor_imbalance REAL,
          composite_score REAL,
          details_json TEXT,
          PRIMARY KEY(symbol, provider, ts_ms)
        )
        """
    )


def _insert_l2_rows(
    con: sqlite3.Connection,
    *,
    now_ms: int,
    n: int,
    symbol: str = "AAPL",
    bid_sz: float = 120.0,
    ask_sz: float = 80.0,
) -> None:
    _create_l2_table(con)
    for idx in range(n):
        ts_ms = int(now_ms - ((n - idx - 1) * 100))
        con.execute(
            """
            INSERT OR REPLACE INTO market_microstructure_signals(
              ts_ms, symbol, provider, mid_px, bid_px, ask_px, bid_sz, ask_sz,
              spread_bps, spread_z, spread_widening, order_book_imbalance,
              trade_buy_volume, trade_sell_volume, trade_aggressor_imbalance,
              composite_score, details_json
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                ts_ms,
                symbol,
                "unit",
                100.0,
                99.98,
                100.02,
                bid_sz,
                ask_sz,
                4.0,
                0.5,
                0.2,
                0.45,
                1000.0,
                300.0,
                0.55,
                0.30,
                "{}",
            ),
        )
    con.commit()


def test_reactive_lob_context_models_queue_crossing_impact_and_adverse_selection() -> None:
    from engine.execution.lob_simulation import build_reactive_lob_simulation

    con = sqlite3.connect(":memory:")
    now_ms = int(time.time() * 1000)
    _insert_l2_rows(con, now_ms=now_ms, n=5, bid_sz=100.0, ask_sz=70.0)

    passive = build_reactive_lob_simulation(
        con,
        symbol="AAPL",
        side="BUY",
        qty=90.0,
        mid_px=100.0,
        order_type="LIMIT",
        aggressiveness="PASSIVE",
        ts_ms=now_ms,
        latency_ms=120,
    )
    assert passive["applied"] is True
    assert passive["spread_crossed"] is False
    assert passive["queue_position_pct"] > 0.0
    assert passive["partial_fill_cap"] < 1.0
    assert passive["adverse_selection_bps"] > 0.0
    assert passive["market_impact_bps"] > 0.0

    market = build_reactive_lob_simulation(
        con,
        symbol="AAPL",
        side="BUY",
        qty=90.0,
        mid_px=100.0,
        order_type="MARKET",
        aggressiveness="AGGRESSIVE",
        ts_ms=now_ms,
        latency_ms=80,
    )
    assert market["applied"] is True
    assert market["spread_crossed"] is True
    assert market["fill_probability_mult"] == 1.0
    assert market["sweep_levels"] >= 2


def test_shadow_deeplob_signal_blocks_until_readiness_evidence_is_present(monkeypatch) -> None:
    from engine.execution.lob_simulation import shadow_deeplob_execution_signal

    con = sqlite3.connect(":memory:")
    now_ms = int(time.time() * 1000)
    _insert_l2_rows(con, now_ms=now_ms, n=2)
    monkeypatch.setenv("EXEC_LOB_DEEPLOB_SHADOW_ENABLED", "1")
    monkeypatch.setenv("EXEC_LOB_MIN_L2_ROWS", "3")
    monkeypatch.setenv("EXEC_LOB_MIN_CALIBRATION_FILLS", "1")
    monkeypatch.setenv("EXEC_LOB_FEATURE_WINDOW_N", "3")

    blocked = shadow_deeplob_execution_signal(con, symbol="AAPL", side="BUY", ts_ms=now_ms, latency_ms=120)
    assert blocked["blocked"] is True
    assert "l2_rows_insufficient" in blocked["blockers"]
    assert blocked["shadow_only"] is True

    _insert_l2_rows(con, now_ms=now_ms, n=4)
    con.execute(
        """
        CREATE TABLE broker_fills (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER,
          symbol TEXT,
          qty REAL,
          px REAL,
          explain_json TEXT
        )
        """
    )
    con.execute(
        "INSERT INTO broker_fills(ts_ms, symbol, qty, px, explain_json) VALUES(?,?,?,?,?)",
        (
            now_ms,
            "AAPL",
            10.0,
            100.0,
            json.dumps({"lob_simulation": {"applied": True, "market_impact_bps": 1.2, "adverse_selection_bps": 0.4}}),
        ),
    )
    con.commit()

    signal = shadow_deeplob_execution_signal(con, symbol="AAPL", side="BUY", ts_ms=now_ms, latency_ms=120)
    assert signal["ok"] is True
    assert signal["shadow_only"] is True
    assert signal["signal_type"] == "execution_timing_adverse_selection"
    assert signal["constraints"]["portfolio_selection_allowed"] is False
    assert "target_weight" not in signal
    assert "portfolio_action" not in signal


def test_live_preflight_lob_snapshot_blocks_enabled_shadow_path_without_evidence(monkeypatch) -> None:
    import engine.runtime.live_trading_preflight as live_trading_preflight

    con = sqlite3.connect(":memory:")
    monkeypatch.setenv("EXEC_LOB_DEEPLOB_SHADOW_ENABLED", "1")
    monkeypatch.setenv("EXEC_LOB_MIN_L2_ROWS", "1")
    monkeypatch.setenv("EXEC_LOB_MIN_CALIBRATION_FILLS", "1")

    with patch("engine.runtime.storage.connect", return_value=con):
        state = live_trading_preflight.lob_deeplob_shadow_readiness_snapshot(engine_mode="live")

    assert state["enabled"] is True
    assert state["ok"] is False
    assert "l2_data_missing" in state["blockers"]


def test_broker_sim_writes_lob_simulation_explain_metadata() -> None:
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as td:
        db_path = str(Path(td) / "lob_broker_sim.db")
        env = {
            "DB_PATH": db_path,
            "TS_STORAGE_BACKEND": "sqlite",
            "ENGINE_SUPERVISED": "1",
            "BROKER_START_CASH": "100000",
            "BROKER_START_EQUITY": "100000",
            "BROKER_MAX_TRADE_PCT_EQUITY": "1.0",
            "BROKER_CHUNK_PCT": "1.0",
            "BROKER_LATENCY_SLEEP": "0",
            "KILL_SWITCH_GLOBAL": "0",
            "TRADING_KILL_SWITCH": "0",
            "KILL_SWITCH": "0",
        }
        with patch.dict(os.environ, env, clear=False):
            import engine.runtime.storage as storage
            import engine.execution.broker_sim as broker_sim

            storage = importlib.reload(storage)
            broker_sim = importlib.reload(broker_sim)
            storage.init_db()

            now_ms = int(time.time() * 1000)
            con = storage.connect()
            try:
                con.execute(
                    "INSERT OR REPLACE INTO prices(ts_ms, symbol, price, px, source) VALUES(?,?,?,?,?)",
                    (now_ms, "AAPL", 100.0, 100.0, "unit"),
                )
                con.execute(
                    """
                    CREATE TABLE IF NOT EXISTS factor_features (
                      feature_id TEXT,
                      asof_ts INTEGER,
                      effective_ts INTEGER,
                      value REAL
                    )
                    """
                )
                _insert_l2_rows(con, now_ms=now_ms, n=5)
                con.commit()
            finally:
                con.close()

            with patch("engine.execution.kill_switch.execution_allowed", return_value=(True, "", {})):
                with patch.object(broker_sim, "_prime_broker_order_state_after_commit", return_value=None):
                    result = broker_sim.apply_new_portfolio_orders(
                        override_orders=[
                            {
                                "symbol": "AAPL",
                                "qty": 10.0,
                                "side": "BUY",
                                "source_order_id": 17,
                                "order_type": "LIMIT",
                                "aggressiveness": "PASSIVE",
                                "alpha_ttl_ms": 60_000,
                            }
                        ],
                        override_order_id=1701,
                        override_ts_ms=now_ms,
                        max_rows=1,
                    )
            assert result["ok"] is True
            assert result["fills_written"] > 0

            ro = storage.connect_ro_direct()
            try:
                row = ro.execute(
                    "SELECT explain_json FROM broker_fills WHERE symbol='AAPL' ORDER BY id DESC LIMIT 1"
                ).fetchone()
            finally:
                ro.close()
            assert row is not None
            explain = json.loads(row[0])
            assert explain["lob_simulation"]["applied"] is True
            assert "queue_position_pct" in explain["lob_simulation"]
            assert "spread_crossed" in explain["lob_simulation"]
            assert explain["lob_simulation"]["market_impact_bps"] >= 0.0
            assert explain["base_slippage_bps"] <= explain["slippage_bps"]
