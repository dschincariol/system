from __future__ import annotations

import json
import shutil
import sqlite3
import subprocess
from pathlib import Path

import pytest

from engine.api.drift_explainer import build_drift_explainer_snapshot


REPO_ROOT = Path(__file__).resolve().parents[1]


def _init_schema(con: sqlite3.Connection) -> None:
    con.executescript(
        """
        CREATE TABLE equity_drift (
          ts_ms INTEGER PRIMARY KEY,
          broker_equity REAL NOT NULL,
          backtest_equity REAL NOT NULL,
          diff_equity REAL NOT NULL,
          diff_equity_pct REAL NOT NULL,
          level TEXT NOT NULL,
          reason TEXT,
          backtest_run_id INTEGER,
          backtest_ts_ms INTEGER,
          detail_json TEXT
        );
        CREATE TABLE model_drift (
          symbol TEXT NOT NULL,
          horizon_s INTEGER NOT NULL,
          ts_ms INTEGER NOT NULL,
          n INTEGER NOT NULL,
          mae REAL NOT NULL,
          baseline_mae REAL NOT NULL,
          drift_ratio REAL NOT NULL,
          PRIMARY KEY(symbol, horizon_s)
        );
        CREATE TABLE feature_distribution_drift (
          feature_id TEXT PRIMARY KEY,
          ts_ms INTEGER NOT NULL,
          recent_n INTEGER NOT NULL,
          baseline_n INTEGER NOT NULL,
          recent_mean REAL NOT NULL,
          baseline_mean REAL NOT NULL,
          baseline_std REAL NOT NULL,
          shift_z REAL NOT NULL,
          drift_score REAL NOT NULL,
          drift_flag INTEGER NOT NULL DEFAULT 0,
          meta_json TEXT
        );
        CREATE TABLE residual_distribution_drift (
          scope TEXT NOT NULL,
          symbol TEXT NOT NULL,
          ts_ms INTEGER NOT NULL,
          recent_n INTEGER NOT NULL,
          baseline_n INTEGER NOT NULL,
          recent_mean REAL NOT NULL,
          baseline_mean REAL NOT NULL,
          baseline_std REAL NOT NULL,
          shift_z REAL NOT NULL,
          abs_mean_recent REAL NOT NULL,
          abs_mean_base REAL NOT NULL,
          abs_shift_ratio REAL NOT NULL,
          drift_score REAL NOT NULL,
          drift_flag INTEGER NOT NULL DEFAULT 0,
          meta_json TEXT,
          PRIMARY KEY(scope, symbol)
        );
        CREATE TABLE risk_state (
          key TEXT PRIMARY KEY,
          value TEXT,
          updated_ts_ms INTEGER
        );
        CREATE TABLE runtime_meta (
          key TEXT PRIMARY KEY,
          value TEXT
        );
        CREATE TABLE drift_retrain_events (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          created_ts INTEGER NOT NULL,
          model_name TEXT NOT NULL DEFAULT '',
          family TEXT,
          trigger_type TEXT,
          trigger_metrics TEXT NOT NULL DEFAULT '{}',
          action_taken TEXT,
          cooldown_applied INTEGER NOT NULL DEFAULT 0,
          candidate_version TEXT,
          outcome_status TEXT,
          diagnostics TEXT NOT NULL DEFAULT '{}'
        );
        """
    )


def test_drift_explainer_handles_no_active_drift() -> None:
    now_ms = 1_700_000_000_000
    with sqlite3.connect(":memory:") as con:
        _init_schema(con)
        con.execute(
            """
            INSERT INTO equity_drift (
              ts_ms, broker_equity, backtest_equity, diff_equity,
              diff_equity_pct, level, reason, backtest_run_id, backtest_ts_ms, detail_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (now_ms - 1_000, 100_050.0, 100_000.0, 50.0, 0.0005, "OK", "within tolerance", 7, now_ms - 2_000, "{}"),
        )
        con.execute(
            "INSERT INTO model_drift VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("AAPL", 3600, now_ms - 1_000, 50, 0.20, 0.25, 0.8),
        )
        con.execute(
            "INSERT INTO feature_distribution_drift VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("price.rv_20", now_ms - 1_000, 200, 1000, 0.12, 0.10, 0.05, 0.4, 0.1, 0, "{}"),
        )
        con.execute(
            "INSERT INTO residual_distribution_drift VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("global", "__all__", now_ms - 1_000, 80, 320, 0.01, 0.01, 0.02, 0.1, 0.02, 0.02, 1.0, 0.05, 0, "{}"),
        )

        payload = build_drift_explainer_snapshot(con=con, now_ms=now_ms)

    assert payload["ok"] is True
    assert payload["status"]["state"] == "normal"
    assert payload["status"]["active"] is False
    assert payload["status"]["severity"] == "OK"
    assert any(row["source"] == "equity_drift" for row in payload["contributors"])
    assert any(item["field"] == "affected.models" for item in payload["unavailable"])


def test_drift_explainer_handles_active_drift_with_available_attribution() -> None:
    now_ms = 1_700_000_000_000
    with sqlite3.connect(":memory:") as con:
        _init_schema(con)
        con.execute(
            """
            INSERT INTO equity_drift (
              ts_ms, broker_equity, backtest_equity, diff_equity,
              diff_equity_pct, level, reason, backtest_run_id, backtest_ts_ms, detail_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (now_ms - 2_000, 112_000.0, 100_000.0, 12_000.0, 0.12, "CRIT", "equity diff exceeds CRIT threshold", 9, now_ms - 5_000, "{}"),
        )
        con.execute(
            "INSERT INTO model_drift VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("MSFT", 900, now_ms - 1_500, 50, 0.45, 0.20, 2.25),
        )
        con.execute(
            "INSERT INTO feature_distribution_drift VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("macro.credit_spread", now_ms - 1_000, 200, 1000, 2.2, 1.1, 0.3, 3.6, 0.9, 1, "{}"),
        )
        con.execute(
            "INSERT INTO residual_distribution_drift VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("symbol", "MSFT", now_ms - 1_000, 80, 320, -0.03, 0.01, 0.02, 2.1, 0.05, 0.02, 2.5, 0.85, 1, "{}"),
        )
        con.execute(
            "INSERT INTO risk_state VALUES (?, ?, ?)",
            (
                "monte_carlo_risk_info",
                json.dumps({"ready": True, "status": "ok", "ts_ms": now_ms - 1_000, "weights": {"MSFT": 0.6}}),
                now_ms - 1_000,
            ),
        )
        con.execute(
            "INSERT INTO drift_retrain_events(created_ts, model_name, family, trigger_type, trigger_metrics, action_taken, cooldown_applied, candidate_version, outcome_status, diagnostics) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (now_ms - 500, "ridge_msft", "ridge", "distribution_drift", json.dumps({"regime": "risk_off"}), "queued", 0, "v2", "pending", "{}"),
        )

        payload = build_drift_explainer_snapshot(con=con, now_ms=now_ms)

    assert payload["status"]["state"] == "active"
    assert payload["status"]["active"] is True
    assert payload["status"]["severity"] == "CRIT"
    assert payload["contributors"][0]["source"] in {"equity_drift", "feature_distribution_drift", "residual_distribution_drift"}
    assert any(row["symbol"] == "MSFT" for row in payload["affected"]["symbols"])
    assert payload["affected"]["models"] == [{"model": "ridge_msft", "source": "drift_retrain_events", "detail": "distribution_drift"}]
    assert payload["affected"]["regimes"] == [{"regime": "risk_off", "source": "drift_retrain_events", "detail": "ridge_msft"}]


def test_drift_explainer_view_model_handles_missing_contributors() -> None:
    node = shutil.which("node")
    if not node:
        pytest.skip("node executable is not available")

    helper_path = REPO_ROOT / "ui" / "portfolio.js"
    code = r"""
import { pathToFileURL } from "node:url";

const mod = await import(pathToFileURL(process.argv[1]).href);
const vm = mod.buildDriftExplainerViewModel({
  ok: true,
  status: {
    state: "unavailable",
    severity: "UNKNOWN",
    reason: "No drift source data is available.",
  },
  contributors: [],
  affected: {},
  unavailable: [
    { field: "top_contributing_features", reason: "No feature rows are available." },
    { field: "affected.models", reason: "No model attribution is available." },
  ],
});
console.log(JSON.stringify(vm));
"""

    result = subprocess.run(
        [node, "--input-type=module", "-e", code, str(helper_path)],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )
    assert result.returncode == 0, result.stderr or result.stdout
    parsed = json.loads(result.stdout)

    assert parsed["rows"] == []
    assert parsed["metaText"] == "Drift attribution unavailable"
    assert {note["field"] for note in parsed["notes"]} == {
        "top_contributing_features",
        "affected.models",
    }
    assert parsed["affected"][0]["value"] == "unavailable"
