from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from engine.api import api_system


def _connect(db_path: Path):
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    return con


def test_portfolio_risk_history_shape_has_multiple_timestamped_rows(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "risk.db"
    with _connect(db_path) as con:
        con.execute(
            """
            CREATE TABLE portfolio_risk_snapshots (
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
        con.executemany(
            "INSERT INTO portfolio_risk_snapshots VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                (1000, 0.30, 0.10, 0.010, 0.010, 0, "{}"),
                (2000, 0.40, -0.20, 0.012, 0.020, 1, '{"reason":"unit"}'),
                (3000, 0.50, 0.15, 0.014, 0.030, 0, "{}"),
            ],
        )

    from engine.runtime import risk_state, storage

    monkeypatch.setattr(storage, "connect", lambda readonly=True: _connect(db_path))
    monkeypatch.setattr(risk_state, "get_state_row", lambda key, default: ("{}", 3000))
    monkeypatch.setattr(
        risk_state,
        "get_state",
        lambda key, default="": {
            "portfolio_risk_status": "ok",
            "portfolio_risk_block": "0",
            "portfolio_risk_ts_ms": "3000",
        }.get(key, default),
    )

    payload = api_system.api_get_portfolio_risk(None)

    assert payload["ok"] is True
    assert len(payload["history"]) == 3
    assert [row["ts_ms"] for row in payload["history"]] == [3000, 2000, 1000]
    assert payload["history"][1]["blocked"] is True
    assert {"gross", "net", "drawdown", "blocked", "ts_ms"}.issubset(payload["history"][0])


def test_monte_carlo_shape_documents_missing_fan_input(monkeypatch) -> None:
    from engine.runtime import risk_state

    raw = json.dumps(
        {
            "ready": True,
            "status": "ok",
            "ts_ms": 3000,
            "simulations": 1500,
            "horizon": 10,
            "var_95": -0.01,
            "var_99": -0.02,
            "cvar_95": -0.03,
            "cvar_99": -0.04,
            "worst_simulated_drawdown": 0.05,
            "drawdown_percentiles": {"p95": 0.04, "p99": 0.045},
        }
    )
    monkeypatch.setattr(risk_state, "get_state_row", lambda key, default: (raw, 3000))
    monkeypatch.setattr(
        risk_state,
        "get_state",
        lambda key, default="": {
            "monte_carlo_risk_status": "idle",
            "monte_carlo_risk_pending": "0",
            "monte_carlo_risk_ts_ms": "3000",
        }.get(key, default),
    )

    payload = api_system.api_get_monte_carlo_risk(None)

    assert payload["ok"] is True
    assert payload["ready"] is True
    assert payload["chart_detail"]["mode"] == "summary"
    assert payload["chart_detail"]["has_fan"] is False
    assert any(row["field"] == "fan_chart" for row in payload["chart_detail"]["unavailable"])
    assert payload["cvar_95"] == -0.03


def test_monte_carlo_shape_reports_populated_fan_and_distribution(monkeypatch) -> None:
    from engine.runtime import risk_state

    raw = json.dumps(
        {
            "ready": True,
            "status": "ok",
            "ts_ms": 3000,
            "simulations": 1500,
            "horizon": 2,
            "var_95": -0.01,
            "cvar_95": -0.03,
            "fan": [
                {"step": 1, "p05": -0.02, "p50": 0.0, "p95": 0.02},
                {"step": 2, "p05": -0.04, "p50": 0.01, "p95": 0.05},
            ],
            "distribution": [
                {"bucket": "-4% to 0%", "value": -0.02, "count": 12, "probability": 0.24},
                {"bucket": "0% to 4%", "value": 0.02, "count": 38, "probability": 0.76},
            ],
        }
    )
    monkeypatch.setattr(risk_state, "get_state_row", lambda key, default: (raw, 3000))
    monkeypatch.setattr(
        risk_state,
        "get_state",
        lambda key, default="": {
            "monte_carlo_risk_status": "idle",
            "monte_carlo_risk_pending": "0",
            "monte_carlo_risk_ts_ms": "3000",
        }.get(key, default),
    )

    payload = api_system.api_get_monte_carlo_risk(None)

    assert payload["ok"] is True
    assert payload["chart_detail"] == {
        "mode": "fan_distribution",
        "has_distribution": True,
        "has_fan": True,
        "unavailable": [],
    }
    assert payload["fan"][1]["p95"] == 0.05
    assert payload["distribution"][0]["count"] == 12


def test_alpha_decay_shape_returns_latest_and_history(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "alpha.db"
    with _connect(db_path) as con:
        con.executescript(
            """
            CREATE TABLE alpha_decay_runtime_history (
              ts_ms INTEGER PRIMARY KEY,
              status TEXT,
              min_throttle_mult REAL,
              severe_count INTEGER,
              warn_count INTEGER,
              detail_json TEXT
            );
            CREATE TABLE alpha_decay_strategy_metrics (
              strategy_name TEXT,
              ts_ms INTEGER,
              window_days INTEGER,
              bucket_s INTEGER,
              rolling_sharpe REAL,
              half_life_buckets REAL,
              half_life_seconds REAL,
              structural_break_z REAL,
              severity TEXT,
              severity_score REAL,
              throttle_mult REAL,
              n_obs INTEGER,
              detail_json TEXT,
              PRIMARY KEY(strategy_name, ts_ms, window_days)
            );
            """
        )
        con.executemany(
            "INSERT INTO alpha_decay_runtime_history VALUES (?, ?, ?, ?, ?, ?)",
            [
                (1000, "ok", 1.0, 0, 0, "{}"),
                (2000, "warn", 0.7, 0, 1, "{}"),
            ],
        )
        con.executemany(
            "INSERT INTO alpha_decay_strategy_metrics VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("mean_reversion", 1000, 7, 3600, 0.42, 5.0, 18000.0, -0.2, "ok", 0.1, 1.0, 20, "{}"),
                ("mean_reversion", 2000, 7, 3600, 0.18, 2.0, 7200.0, -1.4, "warn", 0.4, 0.7, 20, "{}"),
            ],
        )

    from engine.runtime import storage

    monkeypatch.setattr(storage, "connect", lambda readonly=True: _connect(db_path))

    payload = api_system.api_get_alpha_decay(None)

    assert payload["ok"] is True
    assert payload["ready"] is True
    assert payload["runtime"]["status"] == "warn"
    assert len(payload["runtime_history"]) == 2
    assert len(payload["strategy_history"]) == 2
    assert payload["strategy_history"][0]["strategy"] == "mean_reversion"
    assert payload["strategy_history"][1]["rolling_sharpe"] == 0.18


def test_alpha_decay_shape_preserves_null_zero_and_nonzero_throttles(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "alpha-null-zero.db"
    with _connect(db_path) as con:
        con.executescript(
            """
            CREATE TABLE alpha_decay_runtime_history (
              ts_ms INTEGER PRIMARY KEY,
              status TEXT,
              min_throttle_mult REAL,
              severe_count INTEGER,
              warn_count INTEGER,
              detail_json TEXT
            );
            CREATE TABLE alpha_decay_strategy_metrics (
              strategy_name TEXT,
              ts_ms INTEGER,
              window_days INTEGER,
              bucket_s INTEGER,
              rolling_sharpe REAL,
              half_life_buckets REAL,
              half_life_seconds REAL,
              structural_break_z REAL,
              severity TEXT,
              severity_score REAL,
              throttle_mult REAL,
              n_obs INTEGER,
              detail_json TEXT,
              PRIMARY KEY(strategy_name, ts_ms, window_days)
            );
            """
        )
        con.executemany(
            "INSERT INTO alpha_decay_runtime_history VALUES (?, ?, ?, ?, ?, ?)",
            [
                (1000, "ok", None, 0, 0, "{}"),
                (2000, "severe", 0.0, 1, 0, "{}"),
                (3000, "warn", 0.7, 0, 1, "{}"),
            ],
        )
        con.executemany(
            "INSERT INTO alpha_decay_strategy_metrics VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("mean_reversion", 1000, 7, 3600, None, None, None, None, "ok", None, None, 0, "{}"),
                ("mean_reversion", 2000, 7, 3600, 0.0, 0.0, 0.0, 0.0, "severe", 0.0, 0.0, 8, "{}"),
                ("mean_reversion", 3000, 7, 3600, 0.18, 2.0, 7200.0, -1.4, "warn", 0.4, 0.7, 20, "{}"),
                ("blocked_alpha", 3000, 7, 3600, 0.0, 1.0, 3600.0, 0.0, "severe", 1.0, 0.0, 12, "{}"),
            ],
        )

    from engine.runtime import storage

    monkeypatch.setattr(storage, "connect", lambda readonly=True: _connect(db_path))

    payload = api_system.api_get_alpha_decay(None)

    assert payload["ok"] is True
    assert [row["min_throttle_mult"] for row in payload["runtime_history"]] == [None, 0.0, 0.7]
    mean_reversion_history = [
        row for row in payload["strategy_history"] if row["strategy"] == "mean_reversion"
    ]
    assert [row["rolling_sharpe"] for row in mean_reversion_history] == [None, 0.0, 0.18]
    assert [row["throttle_mult"] for row in mean_reversion_history] == [None, 0.0, 0.7]
    blocked_latest = next(row for row in payload["strategies"] if row["strategy"] == "blocked_alpha")
    assert blocked_latest["rolling_sharpe"] == 0.0
    assert blocked_latest["throttle_mult"] == 0.0


def test_alpha_decay_strategy_history_limit_is_per_strategy(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "alpha-per-strategy.db"
    with _connect(db_path) as con:
        con.executescript(
            """
            CREATE TABLE alpha_decay_strategy_metrics (
              strategy_name TEXT,
              ts_ms INTEGER,
              window_days INTEGER,
              bucket_s INTEGER,
              rolling_sharpe REAL,
              half_life_buckets REAL,
              half_life_seconds REAL,
              structural_break_z REAL,
              severity TEXT,
              severity_score REAL,
              throttle_mult REAL,
              n_obs INTEGER,
              detail_json TEXT,
              PRIMARY KEY(strategy_name, ts_ms, window_days)
            );
            """
        )
        rows = []
        for ts_ms in (1000, 2000, 3000, 4000, 5000):
            rows.append(("noisy_alpha", ts_ms, 7, 3600, 0.40 - (ts_ms / 10000.0), 5.0, 18000.0, 0.0, "ok", 0.1, 1.0, 20, "{}"))
        rows.extend(
            [
                ("quiet_alpha", 1500, 7, 3600, -0.20, 2.0, 7200.0, -1.5, "warn", 0.5, 0.6, 20, "{}"),
                ("quiet_alpha", 2500, 7, 3600, -0.30, 1.5, 5400.0, -2.0, "severe", 0.9, 0.2, 20, "{}"),
            ]
        )
        con.executemany(
            "INSERT INTO alpha_decay_strategy_metrics VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )

    from engine.runtime import storage

    monkeypatch.setattr(storage, "connect", lambda readonly=True: _connect(db_path))

    payload = api_system.api_get_alpha_decay({"limit": "2"})
    grouped = {}
    for row in payload["strategy_history"]:
        grouped.setdefault(row["strategy"], []).append(row["ts_ms"])

    assert payload["ok"] is True
    assert payload["strategy_history_limit_per_strategy"] == 2
    assert grouped["noisy_alpha"] == [4000, 5000]
    assert grouped["quiet_alpha"] == [1500, 2500]


def test_alpha_decay_runtime_state_preserves_zero_min_throttle(monkeypatch) -> None:
    from engine.runtime import alpha_decay_monitor

    captured: dict[str, str] = {}
    monkeypatch.setattr(alpha_decay_monitor, "set_risk_state", lambda key, value: captured.setdefault(key, value))
    monkeypatch.setattr(alpha_decay_monitor, "get_lifecycle_state", lambda: {"state": "LIVE", "detail": ""})
    monkeypatch.setattr(alpha_decay_monitor, "set_lifecycle_state", lambda *_args, **_kwargs: None)

    summary = alpha_decay_monitor.apply_alpha_decay_runtime_state(
        {
            "blocked_alpha": {
                "alpha_decay_severity": "severe",
                "alpha_decay_throttle_mult": 0.0,
                "alpha_decay_rolling_sharpe": 0.0,
                "alpha_decay_severity_score": 1.0,
            }
        },
        ts_ms=1234,
    )

    assert summary["min_throttle_mult"] == 0.0
    assert captured["alpha_decay_min_throttle_mult"] == "0.0"


def test_regime_history_shape_uses_decision_snapshots(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "regime.db"
    vector_a = {
        "SPY": {
            "ts_ms": 1000,
            "macro": {"risk_on": 0.8},
            "asset": {"etf_like": 1.0},
            "micro": {"momentum_dominant": 0.7},
            "confidence": {"macro": 0.8, "asset": 0.9, "micro": 0.7},
        }
    }
    vector_b = {
        "SPY": {
            "ts_ms": 2000,
            "macro": {"risk_off": 0.9},
            "asset": {"bocpd_cp_prob_5d": 0.6},
            "micro": {"liquidity_thin": 0.8},
            "confidence": {"macro": 0.6, "asset": 0.5, "micro": 0.4},
        }
    }
    with _connect(db_path) as con:
        con.execute(
            """
            CREATE TABLE trade_decision_snapshot (
              ts_ms INTEGER PRIMARY KEY,
              regime_vectors_json TEXT
            )
            """
        )
        con.executemany(
            "INSERT INTO trade_decision_snapshot VALUES (?, ?)",
            [(1000, json.dumps(vector_a)), (2000, json.dumps(vector_b))],
        )

    from engine.runtime import storage

    monkeypatch.setattr(storage, "connect", lambda readonly=True: _connect(db_path))
    monkeypatch.setattr(
        api_system,
        "api_get_regime_context",
        lambda parsed=None, ctx=None: {"ok": True, "ts_ms": 2000, "layers": {}},
    )

    payload = api_system.api_get_regime_history(None)

    assert payload["ok"] is True
    assert payload["ready"] is True
    assert [row["ts_ms"] for row in payload["rows"]] == [1000, 2000]
    assert payload["rows"][0]["layers"]["macro"]["label"] == "RISK_ON"
    assert payload["rows"][1]["layers"]["macro"]["label"] == "RISK_OFF"
