from __future__ import annotations

import sqlite3
import threading
import urllib.request
from pathlib import Path

from engine.api.http_transport import _derive_response_status, build_handler, run_http_server
from engine.api import api_read_advanced, api_system
from engine.runtime import dashboard_weather_widgets

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_dashboard_root_serves_dashboard_when_static_root_is_ui(tmp_path: Path):
    ui_dir = tmp_path / "ui"
    ui_dir.mkdir()
    (ui_dir / "dashboard.html").write_text("<!doctype html><title>Dashboard OK</title>", encoding="utf-8")

    handler = build_handler([], {}, "", ctx={}, static_dir=str(ui_dir))
    httpd = run_http_server("127.0.0.1", 0, handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        port = int(httpd.server_address[1])
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=3) as resp:
            body = resp.read().decode("utf-8")
            assert resp.status == 200
            assert "Dashboard OK" in body
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_missing_portfolio_backtest_is_explicit_empty_state(monkeypatch, tmp_path: Path):
    db_path = tmp_path / "portfolio-empty.db"

    def connect():
        return sqlite3.connect(str(db_path))

    monkeypatch.setattr(api_read_advanced, "db_connect", connect)

    payload = api_read_advanced.get_latest_portfolio_backtest()
    assert payload["ok"] is False
    assert payload["run"] is None
    assert payload["meta"]["ready"] is False
    assert _derive_response_status(payload) == 200


def test_latest_portfolio_backtest_preserves_null_zero_and_nonzero_points(monkeypatch, tmp_path: Path):
    db_path = tmp_path / "portfolio-null-points.db"
    with sqlite3.connect(str(db_path)) as con:
        con.executescript(
            """
            CREATE TABLE portfolio_bt_runs (
              id INTEGER PRIMARY KEY,
              ts_ms INTEGER,
              start_ts_ms INTEGER,
              end_ts_ms INTEGER,
              metrics_json TEXT
            );
            CREATE TABLE portfolio_bt_points (
              run_id INTEGER,
              ts_ms INTEGER,
              ret REAL,
              equity REAL,
              drawdown REAL,
              detail_json TEXT
            );
            """
        )
        con.execute(
            "INSERT INTO portfolio_bt_runs VALUES (?, ?, ?, ?, ?)",
            (1, 4000, 1000, 3000, "{}"),
        )
        con.executemany(
            "INSERT INTO portfolio_bt_points VALUES (?, ?, ?, ?, ?, ?)",
            [
                (1, 1000, None, None, None, "{}"),
                (1, 2000, 0.0, 0.0, 0.0, "{}"),
                (1, 3000, 0.12, 1.12, -0.03, "{}"),
            ],
        )
        con.commit()

    monkeypatch.setattr(api_read_advanced, "db_connect", lambda: sqlite3.connect(str(db_path)))

    payload = api_read_advanced.get_latest_portfolio_backtest()

    assert payload["ok"] is True
    points = payload["run"]["points"]
    assert points[0]["ret"] is None
    assert points[0]["equity"] is None
    assert points[0]["drawdown"] is None
    assert points[1]["ret"] == 0.0
    assert points[1]["equity"] == 0.0
    assert points[1]["drawdown"] == 0.0
    assert points[2]["ret"] == 0.12
    assert points[2]["equity"] == 1.12
    assert points[2]["drawdown"] == -0.03


def test_weather_effect_tolerates_baseline_schema_without_spearman(monkeypatch, tmp_path: Path):
    db_path = tmp_path / "weather.db"
    with sqlite3.connect(str(db_path)) as con:
        con.execute(
            """
            CREATE TABLE model_weather_effect (
              key_type TEXT NOT NULL,
              key TEXT NOT NULL,
              horizon_s INTEGER NOT NULL,
              regime TEXT NOT NULL DEFAULT 'global',
              ts_ms INTEGER NOT NULL,
              base_rmse REAL,
              wx_rmse REAL,
              rmse_delta REAL,
              base_dir_acc REAL,
              wx_dir_acc REAL,
              dir_acc_delta REAL,
              n_eval INTEGER NOT NULL
            )
            """
        )
        con.execute(
            """
            INSERT INTO model_weather_effect(
              key_type, key, horizon_s, regime, ts_ms,
              base_rmse, wx_rmse, rmse_delta,
              base_dir_acc, wx_dir_acc, dir_acc_delta, n_eval
            )
            VALUES ('global', 'global', 3600, 'global', 1000, 1.2, 1.1, -0.1, 0.51, 0.53, 0.02, 42)
            """
        )
        con.commit()

    monkeypatch.setattr(dashboard_weather_widgets, "connect", lambda: sqlite3.connect(str(db_path)))

    payload = dashboard_weather_widgets.get_weather_effect_summary(ts_ms=2000)
    assert payload["meta"]["status"] == 200
    assert payload["meta"]["missing_columns"] == ["base_spearman", "spearman_delta", "wx_spearman"]
    assert payload["series"][0]["horizon_s"] == 3600
    assert payload["series"][0]["base_spearman"] is None
    assert payload["series"][0]["wx_spearman"] is None
    assert payload["series"][0]["spearman_delta"] is None


def test_supervisor_diagnostics_response_keeps_single_ok_field(monkeypatch):
    monkeypatch.setattr(
        api_system,
        "_build_system_snapshot",
        lambda _parsed, _ctx: {
            "ok": True,
            "status": "RUNNING",
            "state": "LIVE",
            "mode": "safe",
            "execution_mode": "safe",
            "execution_allowed": False,
            "services": {"ok": True},
            "graph": {"ok": True},
            "jobs": [{"name": "poll_prices", "running": True}],
            "ingestion": {"ok": True},
            "reasons": [],
            "health": {},
            "readiness": {},
            "timestamps": {},
        },
    )

    payload = api_system.api_get_supervisor_diagnostics({}, {})
    assert payload["ok"] is True
    assert payload["supervisor_diagnostics"]["ok"] is True
    assert payload["counts"]["running"] == 1


def test_operator_ui_locks_live_secret_and_admin_write_controls_for_monitoring():
    html = (REPO_ROOT / "boot" / "operator_ui.html").read_text(encoding="utf-8")

    assert 'id="modeLive"' in html and 'disabled aria-disabled="true"' in html
    assert "const OPERATOR_LIVE_MODE_UI_ENABLED = false;" in html
    assert "const OPERATOR_ADMIN_WRITES_ENABLED = false;" in html
    assert 'id="saveConfigBtn"' in html and "Configuration writes are disabled" in html
    assert 'id="factoryResetBtn"' in html and "Factory reset is disabled" in html
    assert 'id="secretKey"' in html and "disabled" in html
    assert 'id="secretValue"' in html and "disabled" in html
    assert 'id="saveSecretBtn"' in html and "Secret writes are disabled" in html
    assert 'if(!adminWritesEnabled())' in html
    assert 'if(mode === "live" && !liveModeUiEnabled())' in html


def test_operator_server_returns_degraded_monitoring_payloads_without_browser_5xx():
    server = (REPO_ROOT / "boot" / "operator_server.js").read_text(encoding="utf-8")
    bootstrap_block = server[
        server.index('app.get("/api/operator/bootstrap_counts"') :
        server.index("// --------------------------------------------\n// DB schema inspection")
    ]

    assert "function rowsFromOperatorPayload" in server
    assert 'return jsonState(res, payload, 200);' in bootstrap_block
    assert 'rowsFromOperatorPayload(fills.json, ["fills"])' in server
    assert 'rowsFromOperatorPayload(positions.json, ["positions"])' in server
