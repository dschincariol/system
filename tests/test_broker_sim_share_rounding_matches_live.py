from __future__ import annotations

import importlib
import json
import time
import uuid
from contextlib import ExitStack
from typing import Any, Dict, Tuple
from unittest.mock import patch

import pytest


pytestmark = pytest.mark.safety_critical


@pytest.fixture()
def sim_runtime(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "eq07_share_rounding.db"))
    monkeypatch.setenv("BROKER_START_CASH", "100000")
    monkeypatch.setenv("BROKER_LATENCY_SLEEP", "0")
    monkeypatch.setenv("TS_TESTING", "1")
    monkeypatch.setenv("TS_STORAGE_BACKEND", "sqlite")
    monkeypatch.setenv("EXEC_SIM_ROUNDING_BROKER", "ibkr")
    monkeypatch.setenv("EXEC_IBKR_SHARE_INCREMENT", "1")
    monkeypatch.setenv("EXEC_EQUITY_MIN_NOTIONAL_USD", "1")

    import engine.runtime.storage as storage
    import engine.execution.broker_sim as broker_sim
    import engine.execution.execution_ledger as execution_ledger

    storage = importlib.reload(storage)
    broker_sim = importlib.reload(broker_sim)
    execution_ledger = importlib.reload(execution_ledger)
    storage.init_db()
    broker_sim.init_broker_db()
    execution_ledger.init_execution_ledger()
    try:
        yield storage, broker_sim
    finally:
        storage.close_pooled_connections()


def _seed_price(storage: Any, symbol: str, px: float, ts_ms: int) -> None:
    con = storage.connect()
    try:
        con.execute(
            """
            INSERT OR REPLACE INTO prices(ts_ms, symbol, price, px, source)
            VALUES(?,?,?,?,?)
            """,
            (int(ts_ms), str(symbol), float(px), float(px), "eq07-test"),
        )
        con.commit()
    finally:
        con.close()


def _seed_position(storage: Any, symbol: str, qty: float, avg_px: float, ts_ms: int) -> None:
    con = storage.connect()
    try:
        con.execute(
            """
            INSERT INTO broker_positions(symbol, qty, avg_px, updated_ts_ms)
            VALUES(?,?,?,?)
            ON CONFLICT(symbol) DO UPDATE SET
              qty=excluded.qty,
              avg_px=excluded.avg_px,
              updated_ts_ms=excluded.updated_ts_ms
            """,
            (str(symbol), float(qty), float(avg_px), int(ts_ms)),
        )
        con.commit()
    finally:
        con.close()


def _position_qty(storage: Any, symbol: str) -> float | None:
    con = storage.connect(readonly=True)
    try:
        row = con.execute("SELECT qty FROM broker_positions WHERE symbol=?", (str(symbol),)).fetchone()
        return None if row is None else float(row[0])
    finally:
        con.close()


def _fill_rows(storage: Any, symbol: str) -> list[Tuple[float, str]]:
    con = storage.connect(readonly=True)
    try:
        return [
            (float(row[0]), str(row[1] or ""))
            for row in con.execute(
                "SELECT qty, explain_json FROM broker_fills WHERE symbol=? ORDER BY ts_ms",
                (str(symbol),),
            ).fetchall()
        ]
    finally:
        con.close()


def _run_sim_order(
    broker_sim: Any,
    *,
    symbol: str,
    weight: float,
    order_id: int,
    ts_ms: int,
) -> Dict[str, Any]:
    order = {
        "source_order_id": int(order_id) + 1000,
        "symbol": str(symbol),
        "to_side": "LONG",
        "to_weight": float(weight),
        "source_alert_id": int(order_id) + 2000,
        "model_id": "eq07-test",
    }
    with ExitStack() as stack:
        stack.enter_context(patch("engine.execution.kill_switch.execution_allowed", return_value=(True, None, None)))
        stack.enter_context(patch.object(broker_sim, "get_execution_liquidity_snapshot", return_value={}))
        stack.enter_context(patch.object(broker_sim, "compute_deployable_equity", return_value=100000.0))
        stack.enter_context(patch.object(broker_sim, "_earnings_proximity_decay", return_value=0.0))
        stack.enter_context(patch.object(broker_sim, "_get_factor_feature_asof", return_value=0.0))
        stack.enter_context(patch.object(broker_sim, "_prime_broker_order_state_after_commit", return_value=None))
        stack.enter_context(
            patch.object(
                broker_sim,
                "_share_rounding_asset_class",
                side_effect=lambda sym: "FX" if str(sym).upper() == "EURUSD" else "EQUITY",
            )
        )
        return broker_sim.apply_new_portfolio_orders(
            dry_run=False,
            override_orders=[order],
            override_order_id=int(order_id),
            override_ts_ms=int(ts_ms),
        )


def test_sim_ibkr_mirror_rounds_flat_start_delta_to_live_helper(sim_runtime, monkeypatch) -> None:
    storage, broker_sim = sim_runtime
    monkeypatch.setenv("EXEC_USE_SHARE_ROUNDING", "1")
    ts_ms = int(time.time() * 1000)
    _seed_price(storage, "AAPL", 100.0, ts_ms)

    from engine.execution.share_rounding import round_equity_qty

    raw_target = (0.0124 * 100000.0) / 100.0
    expected_delta, helper_audit = round_equity_qty(
        raw_target,
        100.0,
        broker="ibkr",
        asset_class="EQUITY",
    )
    result = _run_sim_order(broker_sim, symbol="AAPL", weight=0.0124, order_id=9301, ts_ms=ts_ms)

    assert result["ok"] is True
    assert expected_delta == 12.0
    assert helper_audit["rounded_qty"] == 12.0
    assert _position_qty(storage, "AAPL") == expected_delta

    fills = _fill_rows(storage, "AAPL")
    assert fills
    explain_blob = json.dumps([json.loads(row[1]) for row in fills])
    assert '"share_rounding"' in explain_blob
    canary = f"EQ07_CANARY_{uuid.uuid4().hex}"
    assert canary not in json.dumps({"result": result, "fills": fills}, default=str)


def test_sim_mirrors_live_delta_rounding_for_fractional_nonflat_positions(sim_runtime, monkeypatch) -> None:
    storage, broker_sim = sim_runtime
    monkeypatch.setenv("EXEC_USE_SHARE_ROUNDING", "1")
    monkeypatch.setenv("EXEC_IBKR_SHARE_INCREMENT", "1")
    monkeypatch.delenv("EXEC_ALPACA_SHARE_INCREMENT", raising=False)
    ts_ms = int(time.time() * 1000)
    start_qty = 5.3
    raw_target = (0.0124 * 100000.0) / 100.0
    raw_delta = float(raw_target) - float(start_qty)

    from engine.execution.share_rounding import round_equity_qty

    expected_ibkr_delta, ibkr_audit = round_equity_qty(
        raw_delta,
        100.0,
        broker="ibkr",
        asset_class="EQUITY",
    )
    _seed_price(storage, "AAPL", 100.0, ts_ms)
    _seed_position(storage, "AAPL", start_qty, 100.0, ts_ms)
    monkeypatch.setenv("EXEC_SIM_ROUNDING_BROKER", "ibkr")
    result_ibkr = _run_sim_order(broker_sim, symbol="AAPL", weight=0.0124, order_id=9305, ts_ms=ts_ms)

    assert result_ibkr["ok"] is True
    assert ibkr_audit["raw_qty"] == pytest.approx(raw_delta)
    assert ibkr_audit["rounded_qty"] == 7.0
    assert _position_qty(storage, "AAPL") == pytest.approx(start_qty + expected_ibkr_delta)

    expected_alpaca_delta, alpaca_audit = round_equity_qty(
        raw_delta,
        100.0,
        broker="alpaca",
        asset_class="EQUITY",
    )
    _seed_price(storage, "MSFT", 100.0, ts_ms)
    _seed_position(storage, "MSFT", start_qty, 100.0, ts_ms)
    monkeypatch.setenv("EXEC_SIM_ROUNDING_BROKER", "alpaca")
    result_alpaca = _run_sim_order(broker_sim, symbol="MSFT", weight=0.0124, order_id=9306, ts_ms=ts_ms)

    assert result_alpaca["ok"] is True
    assert alpaca_audit["raw_qty"] == pytest.approx(raw_delta)
    assert alpaca_audit["rounded_qty"] == pytest.approx(raw_delta)
    assert _position_qty(storage, "MSFT") == pytest.approx(start_qty + expected_alpaca_delta)


def test_sim_sub_min_rounded_to_zero_produces_no_fill(sim_runtime, monkeypatch) -> None:
    storage, broker_sim = sim_runtime
    monkeypatch.setenv("EXEC_USE_SHARE_ROUNDING", "1")
    ts_ms = int(time.time() * 1000)
    _seed_price(storage, "AAPL", 100.0, ts_ms)

    result = _run_sim_order(broker_sim, symbol="AAPL", weight=0.0000005, order_id=9302, ts_ms=ts_ms)

    assert result["ok"] is True
    assert result["status"] == "no_changes"
    assert result["fills_written"] == 0
    assert _position_qty(storage, "AAPL") is None
    assert result["share_rounding_skipped"][0]["share_rounding"]["rounded_qty"] == 0.0


def test_sim_fx_quantity_is_unchanged_by_equity_share_rounding(sim_runtime, monkeypatch) -> None:
    storage, broker_sim = sim_runtime
    monkeypatch.setenv("EXEC_USE_SHARE_ROUNDING", "1")
    ts_ms = int(time.time() * 1000)
    _seed_price(storage, "EURUSD", 100.0, ts_ms)

    result = _run_sim_order(broker_sim, symbol="EURUSD", weight=0.0124, order_id=9303, ts_ms=ts_ms)

    assert result["ok"] is True
    assert _position_qty(storage, "EURUSD") == pytest.approx(12.4)


def test_sim_gate_off_preserves_legacy_fractional_position(sim_runtime, monkeypatch) -> None:
    storage, broker_sim = sim_runtime
    monkeypatch.setenv("EXEC_USE_SHARE_ROUNDING", "0")
    ts_ms = int(time.time() * 1000)
    _seed_price(storage, "AAPL", 100.0, ts_ms)

    result = _run_sim_order(broker_sim, symbol="AAPL", weight=0.0124, order_id=9304, ts_ms=ts_ms)

    assert result["ok"] is True
    assert _position_qty(storage, "AAPL") == pytest.approx(12.4)
