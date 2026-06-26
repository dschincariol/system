from __future__ import annotations

import json
import sqlite3

import pytest

from engine.rl.offline_dataset import (
    OfflineDatasetConfig,
    RiskSensitiveRewardConfig,
    build_offline_rl_dataset,
)
from engine.rl.offline_policy import (
    BehaviorCloningPolicy,
    OfflinePolicyConfig,
    ensure_optional_family_available,
    evaluate_offline_policy_ope,
    train_behavior_cloning_policy,
)
from engine.rl.offline_shadow import log_offline_shadow_decisions
from engine.runtime.live_ai_safety import live_rl_policy_snapshot
from engine.strategy.ope_gate import ensure_ope_schema


DAY_MS = 86_400_000


def _con(tmp_path):
    con = sqlite3.connect(tmp_path / "offline_rl.sqlite")
    con.executescript(
        """
        CREATE TABLE model_feature_snapshots (
          symbol TEXT NOT NULL,
          ts_ms INTEGER NOT NULL,
          feature_set_tag TEXT NOT NULL,
          snapshot_version INTEGER NOT NULL DEFAULT 1,
          feature_ids_json TEXT NOT NULL,
          vector_json TEXT NOT NULL,
          features_json TEXT NOT NULL,
          source_timestamps_json TEXT NOT NULL,
          availability_json TEXT NOT NULL DEFAULT '{}',
          created_ts_ms INTEGER NOT NULL,
          PRIMARY KEY(symbol, ts_ms, feature_set_tag)
        );
        CREATE TABLE portfolio_orders (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER NOT NULL,
          model_id TEXT NOT NULL DEFAULT 'baseline',
          symbol TEXT NOT NULL,
          action TEXT NOT NULL,
          from_side TEXT NOT NULL,
          to_side TEXT NOT NULL,
          from_weight REAL NOT NULL,
          to_weight REAL NOT NULL,
          delta_weight REAL NOT NULL,
          source_alert_id INTEGER,
          prediction_id INTEGER,
          explain_json TEXT
        );
        CREATE TABLE prices (
          ts_ms INTEGER NOT NULL,
          symbol TEXT NOT NULL,
          price REAL,
          px REAL,
          source TEXT,
          PRIMARY KEY(symbol, ts_ms)
        );
        CREATE TABLE execution_fills (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          client_order_id TEXT NOT NULL,
          portfolio_orders_id INTEGER,
          symbol TEXT,
          fill_ts_ms INTEGER NOT NULL,
          fill_qty REAL NOT NULL,
          fill_px REAL NOT NULL,
          fees REAL,
          commission REAL,
          slippage_bps REAL
        );
        """
    )
    ensure_ope_schema(con)
    return con


def _insert_snapshot(con, *, symbol: str, ts_ms: int, value: float, source_ts: int) -> None:
    con.execute(
        """
        INSERT INTO model_feature_snapshots(
          symbol, ts_ms, feature_set_tag, feature_ids_json, vector_json,
          features_json, source_timestamps_json, created_ts_ms
        )
        VALUES (?,?,?,?,?,?,?,?)
        """,
        (
            symbol,
            ts_ms,
            "default",
            json.dumps(["f1"]),
            json.dumps([value]),
            json.dumps({"f1": value}),
            json.dumps({"f1": source_ts}),
            ts_ms,
        ),
    )


def _insert_order(con, *, ts_ms: int, to_weight: float, from_weight: float = 0.0) -> int:
    cur = con.execute(
        """
        INSERT INTO portfolio_orders(
          ts_ms, model_id, symbol, action, from_side, to_side,
          from_weight, to_weight, delta_weight, explain_json
        )
        VALUES (?,?,?,?,?,?,?,?,?,?)
        """,
        (
            ts_ms,
            "behavior_model",
            "AAA",
            "REBALANCE",
            "LONG" if from_weight > 0 else "FLAT",
            "LONG" if to_weight > 0 else "FLAT",
            abs(float(from_weight)),
            abs(float(to_weight)),
            float(to_weight) - float(from_weight),
            json.dumps({"behavior_propensity": 1.0}),
        ),
    )
    return int(cur.lastrowid)


def _insert_price(con, *, ts_ms: int, px: float) -> None:
    con.execute(
        "INSERT INTO prices(ts_ms, symbol, price, px, source) VALUES (?,?,?,?,?)",
        (ts_ms, "AAA", px, px, "unit"),
    )


def test_offline_dataset_enforces_pit_alignment_and_reward_math(tmp_path):
    con = _con(tmp_path)
    try:
        _insert_snapshot(con, symbol="AAA", ts_ms=900, value=0.25, source_ts=900)
        good_order_id = _insert_order(con, ts_ms=1_000, to_weight=0.5)
        _insert_price(con, ts_ms=1_000, px=100.0)
        _insert_price(con, ts_ms=1_000 + DAY_MS, px=110.0)
        con.execute(
            """
            INSERT INTO execution_fills(
              client_order_id, portfolio_orders_id, symbol, fill_ts_ms,
              fill_qty, fill_px, fees, commission, slippage_bps
            )
            VALUES (?,?,?,?,?,?,?,?,?)
            """,
            ("unit-fill", good_order_id, "AAA", 1_100, 1.0, 100.0, 0.0, 0.0, 0.0),
        )

        _insert_snapshot(con, symbol="AAA", ts_ms=1_900, value=0.75, source_ts=2_500)
        _insert_order(con, ts_ms=2_000, to_weight=0.25, from_weight=0.5)
        _insert_price(con, ts_ms=2_000, px=100.0)
        _insert_price(con, ts_ms=2_000 + DAY_MS, px=101.0)

        dataset = build_offline_rl_dataset(
            con,
            OfflineDatasetConfig(
                universe=["AAA"],
                feature_ids=["f1"],
                horizon_ms=DAY_MS,
                max_w=1.0,
                leverage_cap=1.0,
                min_rows=1,
                reward=RiskSensitiveRewardConfig(
                    fallback_cost_bps=0.0,
                    drawdown_penalty=0.0,
                    turnover_penalty=0.0,
                    slippage_penalty=0.0,
                    concentration_penalty=0.0,
                    cvar_penalty=0.0,
                ),
            ),
        )

        assert dataset.diagnostics["rows"] == 1
        assert dataset.diagnostics["pit_rejected"] == 1
        row = dataset.transitions[0]
        assert row.observation[0] == pytest.approx(0.25)
        assert row.action == pytest.approx((0.5,))
        assert row.reward == pytest.approx(0.05)
        assert row.meta["reward"]["net_pnl_after_costs"] == pytest.approx(0.05)
        assert "portfolio_orders" in row.source_ids[-1]
    finally:
        con.close()


def test_behavior_cloning_policy_artifact_and_optional_dependency_absence(tmp_path):
    con = _con(tmp_path)
    try:
        _insert_snapshot(con, symbol="AAA", ts_ms=900, value=0.25, source_ts=900)
        _insert_order(con, ts_ms=1_000, to_weight=0.5)
        _insert_price(con, ts_ms=1_000, px=100.0)
        _insert_price(con, ts_ms=1_000 + DAY_MS, px=110.0)
        dataset = build_offline_rl_dataset(
            con,
            OfflineDatasetConfig(
                universe=["AAA"],
                feature_ids=["f1"],
                horizon_ms=DAY_MS,
                max_w=1.0,
                leverage_cap=1.0,
                min_rows=1,
                reward=RiskSensitiveRewardConfig(concentration_penalty=0.0, cvar_penalty=0.0),
            ),
        )
        policy = train_behavior_cloning_policy(
            dataset,
            OfflinePolicyConfig(max_w=1.0, leverage_cap=1.0, ridge_l2=0.0),
        )
        artifact_dir = policy.save(tmp_path / "policy")
        loaded = BehaviorCloningPolicy.load(artifact_dir)

        assert loaded.dataset_hash == dataset.dataset_hash
        assert loaded.predict(dataset.transitions[0].observation)[0] == pytest.approx(0.5)
        with pytest.raises(Exception, match="requires optional dependencies"):
            ensure_optional_family_available(
                "iql",
                import_fn=lambda name: (_ for _ in ()).throw(ImportError(name)),
            )
    finally:
        con.close()


def test_offline_ope_persistence_and_lcb_blocking(tmp_path):
    con = _con(tmp_path)
    try:
        _insert_snapshot(con, symbol="AAA", ts_ms=900, value=-0.25, source_ts=900)
        _insert_order(con, ts_ms=1_000, to_weight=1.0)
        _insert_price(con, ts_ms=1_000, px=100.0)
        _insert_price(con, ts_ms=1_000 + DAY_MS, px=95.0)
        dataset = build_offline_rl_dataset(
            con,
            OfflineDatasetConfig(
                universe=["AAA"],
                feature_ids=["f1"],
                horizon_ms=DAY_MS,
                max_w=1.0,
                leverage_cap=1.0,
                min_rows=1,
                reward=RiskSensitiveRewardConfig(
                    fallback_cost_bps=0.0,
                    drawdown_penalty=0.0,
                    turnover_penalty=0.0,
                    slippage_penalty=0.0,
                    concentration_penalty=0.0,
                    cvar_penalty=0.0,
                ),
            ),
        )

        passed, payload = evaluate_offline_policy_ope(
            dataset,
            con=con,
            policy=None,
            config={
                "min_obs": 1,
                "min_effective_n": 1.0,
                "min_support": 1.0,
                "max_standard_error": 1.0,
                "max_ci_width": 2.0,
                "min_policy_value_lower_bound": 0.0,
                "lookback_ms": 0,
            },
        )

        assert passed is False
        assert payload["status"] == "confidence_bound_breached"
        assert payload["effective_n"] == pytest.approx(1.0)
        assert payload["support"] == pytest.approx(1.0)
        assert con.execute("SELECT COUNT(*) FROM policy_ope_observations").fetchone()[0] == 1
        evidence = con.execute("SELECT decision, reason FROM policy_ope_evidence").fetchone()
        assert evidence == ("fail", "confidence_bound_breached")
    finally:
        con.close()


def test_offline_shadow_logging_is_kill_switch_gated(tmp_path):
    con = _con(tmp_path)
    try:
        _insert_snapshot(con, symbol="AAA", ts_ms=900, value=0.25, source_ts=900)
        _insert_order(con, ts_ms=1_000, to_weight=0.5)
        _insert_price(con, ts_ms=1_000, px=100.0)
        _insert_price(con, ts_ms=1_000 + DAY_MS, px=110.0)
        dataset = build_offline_rl_dataset(
            con,
            OfflineDatasetConfig(
                universe=["AAA"],
                feature_ids=["f1"],
                horizon_ms=DAY_MS,
                max_w=1.0,
                leverage_cap=1.0,
                min_rows=1,
                reward=RiskSensitiveRewardConfig(concentration_penalty=0.0, cvar_penalty=0.0),
            ),
        )
        policy = train_behavior_cloning_policy(
            dataset,
            OfflinePolicyConfig(max_w=1.0, leverage_cap=1.0, ridge_l2=0.0),
        )
        transition = dataset.transitions[0]

        paused = log_offline_shadow_decisions(
            con=con,
            policy=policy,
            universe=transition.universe,
            observation=transition.observation,
            live_weights={"AAA": 0.1},
            ts_ms=12_345,
            kill_switch_fn=lambda _con: (False, "unit_test_kill", {}),
        )
        assert paused["status"] == "paused_kill_switch"
        assert con.execute("SELECT COUNT(*) FROM rl_shadow_decisions").fetchone()[0] == 0

        logged = log_offline_shadow_decisions(
            con=con,
            policy=policy,
            universe=transition.universe,
            observation=transition.observation,
            live_weights={"AAA": 0.1},
            ts_ms=12_345,
            kill_switch_fn=lambda _con: (True, "allowed", {}),
        )
        assert logged["status"] == "logged"
        row = con.execute(
            "SELECT symbol, live_weight, rl_weight, delta, meta_json FROM rl_shadow_decisions"
        ).fetchone()
        assert row[0] == "AAA"
        assert row[1] == pytest.approx(0.1)
        assert row[2] == pytest.approx(0.5)
        assert row[3] == pytest.approx(0.4)
        assert json.loads(row[4])["shadow_only"] is True
    finally:
        con.close()


def test_live_preflight_blocks_offline_rl_live_consumption(monkeypatch):
    monkeypatch.setenv("RL_OFFLINE_POLICY_CONSUME_LIVE", "1")
    snapshot = live_rl_policy_snapshot(engine_mode="live", execution_mode="live", broker="ibkr")
    assert snapshot["ok"] is False
    assert snapshot["active"] is True
    assert "live_rl_placeholder_policy_active" in snapshot["blockers"]
    assert snapshot["env"]["RL_OFFLINE_POLICY_CONSUME_LIVE"] == "1"
