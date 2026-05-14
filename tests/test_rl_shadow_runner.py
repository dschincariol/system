from __future__ import annotations

import sqlite3

import numpy as np
import pytest

from engine.rl.shadow_runner import RLShadowRunner, ShadowRunnerConfig, rows_for_snapshot


class FixedAgent:
    def predict(self, observation, deterministic=True):
        assert deterministic is True
        return np.asarray([0.20, -0.10], dtype=np.float32)


def test_shadow_runner_logs_expected_deltas(monkeypatch, tmp_path):
    db_path = tmp_path / "shadow.sqlite"

    def connect():
        return sqlite3.connect(db_path)

    def init_tables(con=None):
        owns = con is None
        db = con or connect()
        try:
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS rl_shadow_decisions(
                  ts INTEGER NOT NULL,
                  symbol TEXT NOT NULL,
                  live_weight REAL NOT NULL,
                  rl_weight REAL NOT NULL,
                  delta REAL NOT NULL,
                  obs_hash TEXT NOT NULL,
                  PRIMARY KEY(ts, symbol)
                );
                """
            )
            if owns:
                db.commit()
        finally:
            if owns:
                db.close()

    import engine.rl.shadow_runner as shadow_runner

    monkeypatch.setattr(shadow_runner.storage, "connect", connect)
    monkeypatch.setattr(shadow_runner.storage, "init_rl_portfolio_tables", init_tables, raising=False)

    config = ShadowRunnerConfig(
        universe=["AAA", "BBB"],
        max_w=0.5,
        leverage_cap=1.0,
        observation_fn=lambda universe, live_weights, ts: np.zeros(11, dtype=np.float32),
        kill_switch_fn=lambda con: (True, "allowed", {}),
    )
    result = RLShadowRunner(config, agent=FixedAgent()).run_once(
        live_decisions={"AAA": 0.05, "BBB": 0.10},
        ts_ms=123456,
    )

    assert result["ok"] is True
    assert result["rows"] == 2

    con = connect()
    try:
        rows = rows_for_snapshot(con, 123456)
    finally:
        con.close()

    assert [row["symbol"] for row in rows] == ["AAA", "BBB"]
    assert rows[0]["live_weight"] == pytest.approx(0.05)
    assert rows[0]["rl_weight"] == pytest.approx(0.20)
    assert rows[0]["delta"] == pytest.approx(0.15)
    assert rows[0]["obs_hash"] == result["obs_hash"]
    assert rows[1]["live_weight"] == pytest.approx(0.10)
    assert rows[1]["rl_weight"] == pytest.approx(-0.10)
    assert rows[1]["delta"] == pytest.approx(-0.20)
    assert rows[1]["obs_hash"] == result["obs_hash"]


def test_shadow_runner_pauses_when_kill_switch_blocks(monkeypatch, tmp_path):
    db_path = tmp_path / "shadow.sqlite"

    import engine.rl.shadow_runner as shadow_runner

    monkeypatch.setattr(shadow_runner.storage, "connect", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(
        shadow_runner.storage,
        "init_rl_portfolio_tables",
        lambda con=None: con.executescript(shadow_runner.RL_SHADOW_SCHEMA),
        raising=False,
    )

    config = ShadowRunnerConfig(
        universe=["AAA", "BBB"],
        observation_fn=lambda universe, live_weights, ts: np.zeros(11, dtype=np.float32),
        kill_switch_fn=lambda con: (False, "unit_test_kill", {"scope": "global"}),
    )
    result = RLShadowRunner(config, agent=FixedAgent()).run_once(live_decisions={"AAA": 0.0}, ts_ms=1)
    assert result["status"] == "paused_kill_switch"
    assert result["rows"] == 0
