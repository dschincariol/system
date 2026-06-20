from __future__ import annotations

import importlib
import json
import sqlite3
import sys
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _create_net_labels_table(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE net_after_cost_labels (
          event_id INTEGER,
          prediction_id INTEGER,
          source_alert_id INTEGER,
          symbol TEXT,
          horizon_s INTEGER,
          label_ts_ms INTEGER,
          entry_ts_ms INTEGER,
          exit_ts_ms INTEGER,
          computed_at_ts_ms INTEGER,
          model_name TEXT,
          model_id TEXT,
          model_version TEXT,
          model_family TEXT,
          regime TEXT,
          confidence REAL,
          confidence_raw REAL,
          confidence_metadata_json TEXT,
          side INTEGER,
          realized INTEGER,
          gross_return REAL,
          realized_forward_return REAL,
          execution_cost_return REAL,
          net_return REAL,
          fees_bps REAL,
          slippage_bps REAL,
          spread_bps REAL,
          borrow_bps REAL,
          financing_bps REAL,
          total_cost_bps REAL,
          source TEXT,
          order_count INTEGER,
          fill_count INTEGER,
          label_metadata_json TEXT
        )
        """
    )


def _insert_label(
    con: sqlite3.Connection,
    *,
    event_id: int,
    base_ts: int,
    age_ms: int,
    net_return: float,
    target_weight: float = 0.08,
) -> None:
    confidence_meta = {
        "liquidity_regime": "liquid",
        "volatility_regime": "low",
        "predicted_z": 1.2,
    }
    label_meta = {
        "factor_group": "momentum",
        "target_weight": float(target_weight),
        "execution_trace": {"notional": 80_000.0},
    }
    con.execute(
        """
        INSERT INTO net_after_cost_labels(
          event_id, prediction_id, source_alert_id, symbol, horizon_s,
          label_ts_ms, entry_ts_ms, exit_ts_ms, computed_at_ts_ms,
          model_name, model_id, model_version, model_family, regime,
          confidence, confidence_raw, confidence_metadata_json, side, realized,
          gross_return, realized_forward_return, execution_cost_return, net_return,
          fees_bps, slippage_bps, spread_bps, borrow_bps, financing_bps,
          total_cost_bps, source, order_count, fill_count, label_metadata_json
        )
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            int(event_id),
            int(event_id + 1000),
            int(event_id + 2000),
            "AAPL",
            300,
            int(base_ts),
            int(base_ts + age_ms),
            int(base_ts + 300_000),
            int(base_ts + 300_001),
            "patchtst_v1",
            "patchtst:aapl",
            "v1",
            "patchtst",
            "risk_on",
            0.8,
            0.8,
            json.dumps(confidence_meta),
            1,
            1,
            float(net_return) + 0.001,
            float(net_return) + 0.001,
            0.001,
            float(net_return),
            1.0,
            2.0,
            1.5,
            0.0,
            0.0,
            3.0,
            "broker_fills_v2",
            1,
            1,
            json.dumps(label_meta),
        ),
    )


def _insert_manual_estimate(
    learned_alpha,
    con: sqlite3.Connection,
    *,
    now_ms: int,
    payload: dict,
    half_life_ms: int,
    max_useful_age_ms: int,
    capacity_estimate: float,
    crowding_penalty: float,
    size_multiplier: float,
    block_signal: int = 0,
) -> None:
    learned_alpha.ensure_schema(con)
    dims = learned_alpha.cohort_dimensions_from_payload(payload)
    key = learned_alpha._cohort_key(dims)
    con.execute(
        f"""
        INSERT INTO {learned_alpha.RUNS_TABLE}(id, ts_ms, lookback_days, min_samples, age_bucket_ms, params_json, metrics_json)
        VALUES (?,?,?,?,?,?,?)
        """,
        (1, int(now_ms), 90, 1, 60_000, "{}", "{}"),
    )
    con.execute(
        f"""
        INSERT INTO {learned_alpha.ESTIMATES_TABLE}(
          run_id, ts_ms, cohort_key, cohort_level, model_family, symbol, regime,
          liquidity_bucket, spread_bucket, volatility_bucket, factor_group,
          n_obs, mean_realized_edge, positive_rate, half_life_ms,
          max_useful_age_ms, capacity_estimate, crowding_penalty,
          size_multiplier, block_signal, detail_json
        )
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            1,
            int(now_ms),
            key,
            "exact",
            dims["model_family"],
            dims["symbol"],
            dims["regime"],
            dims["liquidity_bucket"],
            dims["spread_bucket"],
            dims["volatility_bucket"],
            dims["factor_group"],
            12,
            0.01,
            0.75,
            int(half_life_ms),
            int(max_useful_age_ms),
            float(capacity_estimate),
            float(crowding_penalty),
            float(size_multiplier),
            int(block_signal),
            "{}",
        ),
    )
    con.commit()


def test_training_persists_learned_half_life_capacity_and_crowding_estimates() -> None:
    learned_alpha = importlib.reload(importlib.import_module("engine.strategy.learned_alpha_decay"))
    con = sqlite3.connect(":memory:")
    try:
        _create_net_labels_table(con)
        base_ts = learned_alpha._now_ms() - 1_000_000
        for idx, (age_ms, edge) in enumerate(
            [
                (0, 0.040),
                (60_000, 0.025),
                (120_000, 0.010),
                (180_000, -0.004),
                (240_000, -0.010),
            ],
            start=1,
        ):
            _insert_label(con, event_id=idx, base_ts=base_ts, age_ms=age_ms, net_return=edge)

        result = learned_alpha.train_learned_alpha_decay(
            con,
            lookback_days=10,
            min_samples=2,
            age_bucket_ms=60_000,
            now_ms=base_ts + 1_000_000,
        )

        assert result["ok"] is True
        assert result["estimates"] >= 2
        estimate = learned_alpha.load_learned_alpha_estimate(
            con,
            {
                "model_family": "patchtst",
                "symbol": "AAPL",
                "regime": "risk_on",
                "liquidity_bucket": "liquid",
                "spread_bps": 1.5,
                "volatility_bucket": "low",
                "factor_group": "momentum",
            },
        )
        assert estimate["available"] is True
        assert estimate["cohort_level"] == "exact"
        assert int(estimate["half_life_ms"]) >= 60_000
        assert int(estimate["max_useful_age_ms"]) == 180_000
        assert float(estimate["capacity_estimate"]) > 0.0
        assert 0.0 <= float(estimate["crowding_penalty"]) <= 1.0
        assert 0.0 <= float(estimate["size_multiplier"]) <= 1.0

        edge_rows = con.execute(
            f"SELECT COUNT(1) FROM {learned_alpha.AGE_EDGES_TABLE} WHERE run_id=?",
            (int(result["run_id"]),),
        ).fetchone()
        assert int(edge_rows[0]) >= 5
    finally:
        con.close()


def test_execution_policy_blocks_risk_increasing_order_beyond_learned_max_age() -> None:
    learned_alpha = importlib.reload(importlib.import_module("engine.strategy.learned_alpha_decay"))
    epe = importlib.reload(importlib.import_module("engine.execution.execution_policy_engine"))
    con = sqlite3.connect(":memory:")
    now_ms = learned_alpha._now_ms()
    order = {
        "symbol": "AAPL",
        "qty": 1.0,
        "side": "BUY",
        "signal_ts_ms": now_ms - 120_000,
        "alpha_ttl_ms": 300_000,
        "alpha_half_life_ms": 300_000,
        "model_family": "patchtst",
        "model_id": "patchtst:aapl",
        "regime": "risk_on",
        "liquidity_bucket": "liquid",
        "spread_bps": 1.5,
        "volatility_bucket": "low",
        "factor_group": "momentum",
        "confidence": 0.9,
        "expected_z": 1.5,
        "source_order_id": 77,
    }
    _insert_manual_estimate(
        learned_alpha,
        con,
        now_ms=now_ms,
        payload=order,
        half_life_ms=30_000,
        max_useful_age_ms=60_000,
        capacity_estimate=0.10,
        crowding_penalty=0.10,
        size_multiplier=0.80,
    )

    suppressions: list[dict] = []

    def _record_suppression(**kwargs):
        suppressions.append(dict(kwargs))

    with patch.object(epe, "init_db", return_value=None):
        with patch.object(epe, "execution_allowed", return_value=(True, "", {})):
            with patch.object(
                epe,
                "evaluate_trade_suppression",
                return_value={"state": "NONE", "action": "NONE", "size_mult": 1.0, "throttle_mult": 1.0, "hard_block": False},
            ):
                with patch.object(epe, "update_capital_preservation_mode", return_value={}):
                    with patch.object(epe, "get_state", return_value="normal"):
                        with patch.object(epe, "live_ai_order_guard", return_value={"ok": True, "required": False}):
                            with patch.object(epe, "log_suppression", side_effect=_record_suppression):
                                shaped = epe.apply_execution_policy(
                                    [order],
                                    con=con,
                                    actor="test",
                                    mode="paper",
                                    broker="sim",
                                    initialize_storage=False,
                                    now_ms=now_ms,
                                )

    assert shaped == []
    assert suppressions
    assert suppressions[-1]["suppression_reason"] == "learned_alpha_stale"
    assert suppressions[-1]["decision_json"]["blocked_by"] == "learned_alpha_decay"
    finally_con = con
    finally_con.close()


def test_position_sizing_applies_learned_capacity_multiplier_and_blocks_bad_cohort() -> None:
    sizing = importlib.reload(importlib.import_module("engine.strategy.position_sizing"))
    with patch.object(sizing, "get_state", return_value="normal"):
        with patch.object(sizing, "regime_compat_multiplier", return_value={"mult": 1.0}):
            reduced = sizing.position_from_signal(
                2.0,
                0.8,
                learned_alpha_estimate={
                    "available": True,
                    "run_id": 1,
                    "cohort_key": "patchtst|AAPL|risk_on|liquid|tight|low|momentum",
                    "size_multiplier": 0.5,
                    "capacity_estimate": 0.05,
                    "crowding_penalty": 0.2,
                    "block_signal": False,
                },
            )
            blocked = sizing.position_from_signal(
                2.0,
                0.8,
                learned_alpha_estimate={
                    "available": True,
                    "run_id": 1,
                    "cohort_key": "patchtst|AAPL|risk_on|liquid|tight|low|momentum",
                    "size_multiplier": 0.0,
                    "capacity_estimate": 0.0,
                    "crowding_penalty": 0.95,
                    "block_signal": True,
                },
            )

    assert reduced["direction"] == "LONG"
    assert round(float(reduced["notional_frac"]), 6) == 0.08
    assert reduced["learned_alpha"]["size_multiplier"] == 0.5
    assert blocked["direction"] == "FLAT"
    assert blocked["reason"] == "learned_alpha_blocked"


def test_champion_gate_blocks_low_capacity_learned_alpha_cohort() -> None:
    learned_alpha = importlib.reload(importlib.import_module("engine.strategy.learned_alpha_decay"))
    champion_manager = importlib.reload(importlib.import_module("engine.strategy.champion_manager"))
    con = sqlite3.connect(":memory:")
    now_ms = learned_alpha._now_ms()
    candidate = {
        "model_name": "patchtst_v1",
        "model_id": "patchtst:aapl",
        "model_family": "patchtst",
        "symbol": "AAPL",
        "regime": "risk_on",
        "meta": {
            "liquidity_bucket": "liquid",
            "volatility_bucket": "low",
            "factor_group": "momentum",
        },
    }
    _insert_manual_estimate(
        learned_alpha,
        con,
        now_ms=now_ms,
        payload={**candidate, **candidate["meta"], "spread_bucket": "unknown"},
        half_life_ms=30_000,
        max_useful_age_ms=60_000,
        capacity_estimate=0.001,
        crowding_penalty=0.90,
        size_multiplier=0.0,
        block_signal=1,
    )

    gate = champion_manager._learned_alpha_candidate_gate(con, candidate)

    assert gate["available"] is True
    assert gate["allowed"] is False
    assert gate["reason"] == "learned_alpha_gate_blocked"
    con.close()
