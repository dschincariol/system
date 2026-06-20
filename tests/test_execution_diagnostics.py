from __future__ import annotations

import json
import sqlite3
from urllib.parse import urlparse

from engine.api import api_ops, api_ops_handlers
from engine.execution.execution_diagnostics import build_execution_diagnostics


def _create_core_tables(con: sqlite3.Connection) -> None:
    con.executescript(
        """
        CREATE TABLE execution_orders (
          client_order_id TEXT PRIMARY KEY,
          broker TEXT,
          portfolio_orders_id INTEGER,
          source_alert_id INTEGER,
          symbol TEXT,
          qty REAL,
          submit_ts_ms INTEGER,
          status TEXT,
          extra_json TEXT
        );
        CREATE TABLE execution_fills (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          client_order_id TEXT,
          broker TEXT,
          symbol TEXT,
          portfolio_orders_id INTEGER,
          source_alert_id INTEGER,
          submit_ts_ms INTEGER,
          fill_ts_ms INTEGER,
          fill_qty REAL,
          fill_px REAL,
          expected_px REAL,
          mid_px REAL,
          spread_bps REAL,
          slippage_bps REAL,
          fill_latency_ms INTEGER,
          fees REAL,
          extra_json TEXT
        );
        CREATE TABLE execution_metrics (
          ts_ms INTEGER,
          client_order_id TEXT,
          broker TEXT,
          symbol TEXT,
          submit_qty REAL,
          filled_qty REAL,
          ref_px REAL,
          expected_px REAL,
          fill_px REAL,
          fill_vwap REAL,
          spread_bps REAL,
          slippage_bps REAL,
          fill_latency_ms INTEGER,
          fees REAL,
          m2m_pnl REAL
        );
        CREATE TABLE execution_policy_feedback (
          ts_ms INTEGER,
          client_order_id TEXT,
          broker TEXT,
          symbol TEXT,
          order_type TEXT,
          aggressiveness TEXT,
          execution_policy TEXT,
          entry_strategy TEXT,
          entry_delay_ms INTEGER,
          expected_slippage_bps REAL,
          realized_slippage_bps REAL,
          slippage_error_bps REAL,
          expected_fill_latency_ms REAL,
          realized_fill_latency_ms REAL,
          latency_error_ms REAL,
          fill_quality_score REAL,
          extra_json TEXT
        );
        CREATE TABLE execution_fill_quality (
          ts_ms INTEGER,
          client_order_id TEXT,
          broker TEXT,
          symbol TEXT,
          total_cost_bps REAL,
          spread_capture_bps REAL,
          extra_json TEXT
        );
        CREATE TABLE terminal_intent_rejections (
          id INTEGER PRIMARY KEY,
          ts_ms INTEGER,
          symbol TEXT,
          side TEXT,
          qty REAL,
          reason_code TEXT,
          reason TEXT,
          source TEXT,
          detail_json TEXT
        );
        CREATE TABLE trade_attribution_ledger (
          id INTEGER PRIMARY KEY,
          ts_ms INTEGER,
          source_alert_id INTEGER,
          symbol TEXT,
          suppression_reason TEXT,
          decision_json TEXT,
          order_id INTEGER
        );
        CREATE TABLE execution_policy_audit (
          id INTEGER PRIMARY KEY,
          ts_ms INTEGER,
          symbol TEXT,
          side TEXT,
          qty REAL,
          policy_json TEXT,
          decision_json TEXT,
          suppression_state TEXT,
          source_alert_id INTEGER,
          portfolio_orders_batch_id INTEGER
        );
        CREATE TABLE broker_fills (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER,
          symbol TEXT,
          qty REAL,
          px REAL,
          explain_json TEXT
        );
        CREATE TABLE market_microstructure_signals (
          ts_ms INTEGER NOT NULL,
          symbol TEXT NOT NULL,
          provider TEXT NOT NULL,
          mid_px REAL,
          bid_px REAL,
          ask_px REAL,
          bid_sz REAL,
          ask_sz REAL,
          spread_bps REAL,
          spread_widening REAL,
          order_book_imbalance REAL,
          trade_aggressor_imbalance REAL,
          composite_score REAL,
          details_json TEXT
        );
        """
    )


def _insert_l2(con: sqlite3.Connection, *, now_ms: int, age_ms: int = 500, rows: int = 3) -> None:
    for idx in range(rows):
        ts_ms = now_ms - age_ms - ((rows - idx - 1) * 100)
        con.execute(
            """
            INSERT INTO market_microstructure_signals(
              ts_ms, symbol, provider, mid_px, bid_px, ask_px, bid_sz, ask_sz,
              spread_bps, spread_widening, order_book_imbalance,
              trade_aggressor_imbalance, composite_score, details_json
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                ts_ms,
                "AAPL",
                "unit",
                100.0,
                99.99,
                100.01,
                120.0,
                110.0,
                2.0,
                0.1,
                0.2,
                0.1,
                0.3,
                "{}",
            ),
        )


def test_execution_diagnostics_serializes_tca_lob_slicing_and_outcomes(monkeypatch) -> None:
    now_ms = 1_800_000_000_000
    monkeypatch.setenv("EXEC_LOB_MIN_L2_ROWS", "2")
    monkeypatch.setenv("EXEC_LOB_MIN_CALIBRATION_FILLS", "1")
    monkeypatch.setenv("EXEC_LOB_DEEPLOB_SHADOW_ENABLED", "1")

    con = sqlite3.connect(":memory:")
    _create_core_tables(con)
    _insert_l2(con, now_ms=now_ms, rows=3)

    con.execute(
        "INSERT INTO execution_orders VALUES (?,?,?,?,?,?,?,?,?)",
        ("cid-1", "sim", 101, 201, "AAPL", 100.0, now_ms - 10_000, "submitted", "{}"),
    )
    con.execute(
        "INSERT INTO execution_fills(client_order_id, broker, symbol, portfolio_orders_id, source_alert_id, submit_ts_ms, fill_ts_ms, fill_qty, fill_px, expected_px, mid_px, spread_bps, slippage_bps, fill_latency_ms, fees, extra_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "cid-1",
            "sim",
            "AAPL",
            101,
            201,
            now_ms - 10_000,
            now_ms - 1_000,
            40.0,
            100.10,
            100.0,
            100.0,
            2.0,
            10.0,
            900,
            0.12,
            json.dumps({"implementation_shortfall_bps": 11.5, "vwap_px": 100.08}),
        ),
    )
    con.execute(
        "INSERT INTO execution_metrics VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            now_ms - 1_000,
            "cid-1",
            "sim",
            "AAPL",
            100.0,
            40.0,
            100.0,
            100.0,
            100.10,
            100.08,
            2.0,
            10.0,
            900,
            0.12,
            0.0,
        ),
    )
    con.execute(
        "INSERT INTO execution_policy_feedback VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            now_ms - 1_000,
            "cid-1",
            "sim",
            "AAPL",
            "LIMIT",
            "PASSIVE",
            "balanced",
            "working_limit",
            0,
            8.0,
            10.0,
            2.0,
            700.0,
            900.0,
            200.0,
            0.72,
            "{}",
        ),
    )
    con.execute(
        "INSERT INTO execution_fill_quality VALUES (?,?,?,?,?,?,?)",
        (now_ms - 1_000, "cid-1", "sim", "AAPL", 10.3, -8.0, "{}"),
    )
    con.execute(
        "INSERT INTO terminal_intent_rejections VALUES (?,?,?,?,?,?,?,?,?)",
        (7, now_ms - 2_000, "MSFT", "BUY", 10.0, "max_notional_exceeded", "Order exceeds max notional.", "terminal", "{}"),
    )
    con.execute(
        "INSERT INTO trade_attribution_ledger VALUES (?,?,?,?,?,?,?)",
        (9, now_ms - 3_000, 301, "TSLA", "max_position", json.dumps({"blocked_by": "max_position"}), 501),
    )
    con.execute(
        "INSERT INTO broker_fills(ts_ms, symbol, qty, px, explain_json) VALUES (?,?,?,?,?)",
        (
            now_ms - 1_000,
            "AAPL",
            40.0,
            100.10,
            json.dumps(
                {
                    "lob_simulation": {
                        "applied": True,
                        "market_impact_bps": 1.2,
                        "adverse_selection_bps": 0.4,
                    }
                }
            ),
        ),
    )
    learned_policy = {
        "learned_execution": {
            "enabled": True,
            "applied": True,
            "policy": "contextual_bandit_execution_slicer_v1",
            "action_id": "patient",
            "parameters": {"slice_pct": 0.1, "target_participation": 0.02, "slice_interval_ms": 400, "entry_delay_ms": 175},
            "context": {"spread_bps": 2.0, "slippage_bps": 10.0, "adverse_selection_bps": 0.4, "fill_risk": 0.1},
            "constraints": {"base_slice_pct": 0.2},
        }
    }
    con.execute(
        "INSERT INTO execution_policy_audit VALUES (?,?,?,?,?,?,?,?,?,?)",
        (12, now_ms - 1_500, "AAPL", "BUY", 100.0, json.dumps(learned_policy), "{}", "NONE", 201, 101),
    )

    payload = build_execution_diagnostics(con=con, now_ms=now_ms, limit=20)

    assert payload["ok"] is True
    assert payload["tca"]["by_symbol"][0]["symbol"] == "AAPL"
    assert payload["tca"]["by_symbol"][0]["avg_fill_quality_score"] == 0.72
    assert payload["tca"]["by_symbol"][0]["avg_implementation_shortfall_bps"] == 11.5
    assert payload["tca"]["by_symbol"][0]["avg_vwap_px"] == 100.08
    assert payload["order_flow"]["partial_fills"][0]["state"] == "partial"
    assert payload["order_flow"]["rejected_intents"][0]["reason_code"] == "max_notional_exceeded"
    assert payload["order_flow"]["suppressed_intents"][0]["reason_code"] == "max_position"
    assert {row["kind"] for row in payload["drilldowns"]} >= {"partial_fill", "rejected_intent", "suppressed_intent"}
    assert payload["lob"]["l2_feed"]["state"] == "fresh"
    assert payload["lob"]["deeplob"]["shadow_only"] is True
    assert payload["learned_slicing"]["authority"]["live_authority_granted"] is False
    assert payload["learned_slicing"]["selected_action_distribution"] == [{"action_id": "patient", "count": 1}]
    assert payload["learned_slicing"]["baseline_comparison"]["state"] == "available"


def test_execution_diagnostics_marks_l2_feed_stale(monkeypatch) -> None:
    now_ms = 1_800_000_000_000
    monkeypatch.setenv("EXEC_LOB_MIN_L2_ROWS", "1")
    monkeypatch.setenv("EXEC_LOB_MAX_L2_AGE_MS", "60000")

    con = sqlite3.connect(":memory:")
    _create_core_tables(con)
    _insert_l2(con, now_ms=now_ms, age_ms=120_000, rows=1)

    payload = build_execution_diagnostics(con=con, now_ms=now_ms, limit=5)

    assert payload["lob"]["l2_feed"]["state"] == "stale"
    assert "l2_stale" in payload["lob"]["warnings"]
    assert payload["inventory"]["summary"]["unavailable"] >= 1


def test_execution_diagnostics_api_route_and_handler_parse_query(monkeypatch) -> None:
    assert ("GET", "/api/execution/diagnostics", "api_get_execution_diagnostics") in api_ops.ROUTE_SPECS

    import engine.execution.execution_diagnostics as diagnostics_module

    def _fake_build_execution_diagnostics(**kwargs):
        return {"ok": True, "limit": kwargs["limit"], "symbol": kwargs["symbol"]}

    monkeypatch.setattr(diagnostics_module, "build_execution_diagnostics", _fake_build_execution_diagnostics)

    payload = api_ops_handlers.api_get_execution_diagnostics(
        urlparse("/api/execution/diagnostics?limit=7&symbol=aapl"),
        {},
    )

    assert payload == {"ok": True, "limit": 7, "symbol": "AAPL"}
