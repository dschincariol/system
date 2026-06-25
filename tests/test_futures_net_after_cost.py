from __future__ import annotations

import json
import math
import sqlite3
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _create_fills_table(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE execution_fills (
          id INTEGER PRIMARY KEY,
          fill_id TEXT,
          client_order_id TEXT,
          symbol TEXT,
          prediction_id INTEGER,
          source_alert_id INTEGER,
          fill_ts_ms INTEGER,
          fill_qty REAL,
          fill_px REAL,
          expected_px REAL,
          mid_px REAL,
          bid_px REAL,
          ask_px REAL,
          spread_bps REAL,
          slippage_bps REAL,
          fees REAL,
          raw_json TEXT,
          extra_json TEXT
        )
        """
    )


def test_futures_fill_notional_uses_multiplier_and_tick_roll_costs_round_trip() -> None:
    from engine.strategy.net_after_cost_labels import (
        build_net_after_cost_label,
        ensure_net_after_cost_labels_schema,
        load_execution_trace,
        upsert_net_after_cost_label,
    )

    con = sqlite3.connect(":memory:")
    try:
        _create_fills_table(con)
        con.execute(
            """
            INSERT INTO execution_fills(
              fill_id, client_order_id, symbol, prediction_id, source_alert_id, fill_ts_ms,
              fill_qty, fill_px, expected_px, mid_px, bid_px, ask_px, spread_bps,
              slippage_bps, fees, raw_json, extra_json
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            ("fut-fill-1", "fut-order-1", "ES.c.0", 10, 20, 1_000_000, 2.0, 5000.0, 4999.75, 5000.0, 4999.75, 5000.25, None, None, 4.0, "{}", "{}"),
        )
        trace = load_execution_trace(
            con,
            event_id=9001,
            symbol="ES.c.0",
            horizon_s=300,
            label_ts_ms=999_000,
            exit_ts_ms=1_300_000,
            prediction_id=10,
            source_alert_id=20,
        )
        assert trace["fill_count"] == 1
        assert math.isclose(float(trace["notional"]), 2.0 * 5000.0 * 50.0, rel_tol=1e-12)
        assert math.isclose(float(trace["futures_tick_slippage_cost"]), 25.0, rel_tol=1e-12)
        assert math.isclose(float(trace["slippage_bps"]), 25.0 / 500_000.0 * 10000.0, rel_tol=1e-12)

        artifact = build_net_after_cost_label(
            event_id=9001,
            symbol="ES.c.0",
            horizon_s=300,
            label_ts_ms=999_000,
            side=1,
            gross_return=0.0100,
            net_return=0.0100,
            realized_forward_return=0.0100,
            source="unit-test",
            realized=1,
            entry_ts_ms=1_000_000,
            exit_ts_ms=1_300_000,
            costs={"roll_cost_bps": 2.0, "carry_bps": 1.0},
            execution_trace=trace,
        )
        assert artifact["roll_cost_bps"] == 2.0
        assert artifact["carry_bps"] == 1.0
        assert artifact["total_cost_bps"] >= 3.0
        assert artifact["net_return"] < artifact["gross_return"]
        assert math.isclose(
            float(artifact["execution_cost_return"]),
            float(artifact["total_cost_bps"]) / 10000.0,
            rel_tol=1e-12,
        )

        ensure_net_after_cost_labels_schema(con)
        columns = {row[1] for row in con.execute("PRAGMA table_info(net_after_cost_labels)").fetchall()}
        assert {"roll_cost_bps", "carry_bps"}.issubset(columns)
        upsert_net_after_cost_label(con, artifact)
        row = con.execute(
            """
            SELECT roll_cost_bps, carry_bps, total_cost_bps, net_return, label_metadata_json
            FROM net_after_cost_labels
            WHERE event_id=9001 AND symbol='ES.c.0' AND horizon_s=300
            """
        ).fetchone()
        assert row is not None
        assert float(row[0]) == 2.0
        assert float(row[1]) == 1.0
        assert float(row[2]) == artifact["total_cost_bps"]
        assert float(row[3]) == artifact["net_return"]
        metadata = json.loads(row[4])
        assert metadata["cost_evidence"]["futures_cost_available"] is True
        assert metadata["futures_costs"]["roll_cost_bps"] == 2.0
    finally:
        con.close()


def test_non_futures_net_after_cost_labels_remain_without_futures_costs() -> None:
    from engine.strategy.net_after_cost_labels import build_net_after_cost_label

    artifact = build_net_after_cost_label(
        event_id=9002,
        symbol="AAPL",
        horizon_s=300,
        label_ts_ms=1_000_000,
        side=1,
        gross_return=0.020,
        net_return=0.018,
        realized_forward_return=0.020,
        source="unit-test",
        realized=1,
        costs={"fees_bps": 2.0, "slippage_bps": 4.0, "spread_bps": 5.0, "total_cost_bps": 11.0},
        execution_trace={"notional": 1000.0, "fees_cost": 0.2, "slippage_cost": 0.4, "spread_cost": 0.5},
    )

    assert artifact["roll_cost_bps"] == 0.0
    assert artifact["carry_bps"] == 0.0
    assert artifact["net_return"] == 0.018
    assert round(float(artifact["execution_cost_return"]), 6) == 0.002
