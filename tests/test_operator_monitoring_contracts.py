from __future__ import annotations

import json
import importlib
import sqlite3
import threading
import urllib.request
from pathlib import Path
from urllib.error import HTTPError

from engine.api.http_transport import _derive_response_status, build_handler, run_http_server
from engine.api import api_operator_handlers, api_ops, api_ops_handlers, api_read_advanced, api_system
from engine.data import weather_features
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


def _weather_handler(tmp_path: Path):
    route_specs = [route for route in api_ops.ROUTE_SPECS_OPS if str(route[1]).startswith("/api/weather/")]
    handlers = {
        "api_get_weather_snapshot": api_ops_handlers.api_get_weather_snapshot,
        "api_get_weather_alerts": api_ops_handlers.api_get_weather_alerts,
        "api_get_weather_effect": api_ops_handlers.api_get_weather_effect,
    }
    return build_handler(route_specs, handlers, "", ctx={}, static_dir=str(tmp_path))


def _get_json(httpd, path: str) -> tuple[int, dict]:
    with urllib.request.urlopen(f"http://127.0.0.1:{httpd.server_address[1]}{path}", timeout=3) as response:
        return int(response.status), json.loads(response.read().decode("utf-8"))


def test_weather_routes_degrade_on_cold_missing_tables(monkeypatch, tmp_path: Path):
    db_path = tmp_path / "weather-cold.db"
    monkeypatch.setattr(dashboard_weather_widgets, "connect", lambda: sqlite3.connect(str(db_path)))
    monkeypatch.setattr(weather_features, "connect", lambda: sqlite3.connect(str(db_path)))

    handler = _weather_handler(tmp_path)
    httpd = run_http_server("127.0.0.1", 0, handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        for path, expected_reason in (
            ("/api/weather/alerts", "weather_alerts_table_missing"),
            ("/api/weather/effect", "model_weather_effect_table_missing"),
            ("/api/weather/snapshot", "weather_forecast_region_daily_table_missing"),
            ("/api/weather/snapshot?symbol=QQQ", "weather_forecast_region_daily_table_missing"),
        ):
            status, payload = _get_json(httpd, path)
            assert status == 200
            assert payload["ok"] is True
            assert payload["error"] is None
            assert payload["meta"]["ready"] is False
            assert payload["meta"]["reason"] == expected_reason
            assert payload["meta"]["status"] == 200
            assert payload.get("reason_code") != "handler_exception"
            assert payload.get("detail") != "OperationalError"
            if path == "/api/weather/snapshot":
                assert payload["symbol"] == "SPY"
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=2)


def test_weather_routes_return_populated_payloads_when_tables_exist(monkeypatch, tmp_path: Path):
    db_path = tmp_path / "weather-populated.db"
    ts_ms = 1_700_000_000_000
    day_ts = (ts_ms // 86_400_000) * 86_400_000
    with sqlite3.connect(str(db_path)) as con:
        con.executescript(
            """
            CREATE TABLE weather_forecast_region_daily (
              provider TEXT NOT NULL,
              region_id TEXT NOT NULL,
              run_ts INTEGER NOT NULL,
              day_ts INTEGER NOT NULL,
              temp_mean_c REAL,
              hdd65 REAL,
              cdd65 REAL,
              wind_mean_mps REAL,
              precip_sum_mm REAL,
              spread REAL,
              source_uri TEXT
            );
            CREATE TABLE weather_alerts (
              provider TEXT NOT NULL,
              alert_id TEXT NOT NULL,
              issued_ts INTEGER NOT NULL,
              effective_ts INTEGER,
              expires_ts INTEGER,
              event TEXT,
              severity TEXT,
              urgency TEXT,
              certainty TEXT,
              area_desc TEXT,
              affected_regions TEXT,
              headline TEXT
            );
            CREATE TABLE model_weather_effect (
              key_type TEXT NOT NULL,
              key TEXT NOT NULL,
              horizon_s INTEGER NOT NULL,
              regime TEXT NOT NULL DEFAULT 'global',
              ts_ms INTEGER NOT NULL,
              base_rmse REAL,
              wx_rmse REAL,
              rmse_delta REAL,
              base_spearman REAL,
              wx_spearman REAL,
              spearman_delta REAL,
              n_eval INTEGER NOT NULL
            );
            """
        )
        con.execute(
            """
            INSERT INTO weather_forecast_region_daily(
              provider, region_id, run_ts, day_ts, temp_mean_c, hdd65, cdd65,
              wind_mean_mps, precip_sum_mm, spread, source_uri
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("open_meteo", "us_gulf", ts_ms - 10_000, day_ts, 24.0, 1.5, 2.5, 8.0, 12.0, 0.25, "fixture"),
        )
        con.execute(
            """
            INSERT INTO weather_alerts(
              provider, alert_id, issued_ts, effective_ts, expires_ts, event,
              severity, urgency, certainty, area_desc, affected_regions, headline
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "nws",
                "alert-1",
                ts_ms - 1_000,
                ts_ms - 1_000,
                ts_ms + 86_400_000,
                "Severe Thunderstorm Warning",
                "Severe",
                "Immediate",
                "Observed",
                "National",
                json.dumps(["us_gulf"]),
                "Storm warning",
            ),
        )
        con.execute(
            """
            INSERT INTO model_weather_effect(
              key_type, key, horizon_s, regime, ts_ms, base_rmse, wx_rmse,
              rmse_delta, base_spearman, wx_spearman, spearman_delta, n_eval
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("global", "global", 3600, "global", ts_ms - 500, 1.2, 0.9, -0.3, 0.4, 0.5, 0.1, 33),
        )
        con.commit()

    monkeypatch.setattr(dashboard_weather_widgets, "connect", lambda: sqlite3.connect(str(db_path)))
    monkeypatch.setattr(weather_features, "connect", lambda: sqlite3.connect(str(db_path)))

    handler = _weather_handler(tmp_path)
    httpd = run_http_server("127.0.0.1", 0, handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        snapshot_status, snapshot = _get_json(httpd, f"/api/weather/snapshot?symbol=SPY&ts_ms={ts_ms}")
        alerts_status, alerts = _get_json(httpd, f"/api/weather/alerts?ts_ms={ts_ms}")
        effect_status, effect = _get_json(httpd, f"/api/weather/effect?ts_ms={ts_ms}")
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=2)

    assert snapshot_status == alerts_status == effect_status == 200
    assert snapshot["meta"]["ready"] is True
    assert snapshot["symbol"] == "SPY"
    assert snapshot["wx"]["precip_7d"] > 0.0
    assert alerts["meta"]["ready"] is True
    assert alerts["active"][0]["alert_id"] == "alert-1"
    assert effect["meta"]["ready"] is True
    assert effect["series"][0]["horizon_s"] == 3600
    assert effect["series"][0]["spearman_delta"] == 0.1


def test_sqlite_bootstrap_creates_weather_tables(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "weather-bootstrap.db"))
    storage_sqlite = importlib.reload(importlib.import_module("engine.runtime.storage_sqlite"))
    storage_sqlite.init_db()
    with sqlite3.connect(str(storage_sqlite.DB_PATH)) as con:
        tables = {
            str(row[0])
            for row in con.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type='table'
                  AND (name LIKE 'weather_%' OR name='model_weather_effect')
                """
            ).fetchall()
        }

    assert {
        "model_weather_effect",
        "weather_alerts",
        "weather_forecast_region_daily",
        "weather_provider_health",
    }.issubset(tables)


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


def test_operator_institutional_check_degraded_route_returns_200_with_named_reasons():
    def readiness(_parsed=None, _ctx=None):
        return {
            "ok": False,
            "status": "DEGRADED",
            "ready": False,
            "reasons": ["storage_unavailable", "config_valid"],
            "production_validation": {"summary_reason": "storage_unavailable"},
        }

    def health(_parsed=None, _ctx=None):
        return {
            "ok": False,
            "status": "DEGRADED",
            "error": "health_snapshot_invalid",
        }

    handler_ctx = {"API_HANDLERS": {"api_get_readiness": readiness, "api_get_health": health}}
    payload = api_operator_handlers.api_get_operator_institutional_check(None, handler_ctx)

    assert payload["ok"] is False
    assert payload["pass"] is False
    assert payload["configValid"] is False
    assert payload["healthOk"] is False
    assert payload["meta"]["status"] == 200
    assert _derive_response_status(payload) == 200
    assert "readiness:storage_unavailable" in payload["reasons"]
    assert "readiness:config_valid" not in payload["reasons"]
    assert "health:health_snapshot_invalid" in payload["reasons"]

    failing_checks = [check for check in payload["checks"] if not check["ok"]]
    assert {check["name"] for check in failing_checks} == {"readiness", "health"}
    assert all(check["reason"] for check in failing_checks)
    assert all(check["reasons"] for check in failing_checks)

    handler = build_handler(
        [{"method": "GET", "path": "/api/operator/institutionalCheck", "handler": "api_get_operator_institutional_check"}],
        {"api_get_operator_institutional_check": api_operator_handlers.api_get_operator_institutional_check},
        "",
        ctx=handler_ctx,
        static_dir=str(REPO_ROOT / "ui"),
    )
    httpd = run_http_server("127.0.0.1", 0, handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        port = int(httpd.server_address[1])
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/operator/institutionalCheck", timeout=3) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            assert resp.status == 200
    finally:
        httpd.shutdown()
        httpd.server_close()

    assert body["ok"] is False
    assert body["error"] is None
    assert body["meta"]["status"] == 200
    assert "readiness:storage_unavailable" in body["reasons"]
    assert "readiness:config_valid" not in body["reasons"]
    assert "health:health_snapshot_invalid" in body["reasons"]


def test_operator_institutional_check_internal_fault_stays_actionable_500():
    def readiness(_parsed=None, _ctx=None):
        raise RuntimeError("readiness exploded")

    def health(_parsed=None, _ctx=None):
        return {"ok": True}

    handler_ctx = {"API_HANDLERS": {"api_get_readiness": readiness, "api_get_health": health}}
    payload = api_operator_handlers.api_get_operator_institutional_check(None, handler_ctx)

    assert payload["ok"] is False
    assert payload["sub_check"] == "readiness"
    assert payload["root_cause_code"] == "API_OPERATOR_HANDLERS_INSTITUTIONAL_CHECK_SUBCHECK_FAILED"
    assert payload["meta"]["status"] == 500
    assert payload["error"] != "request_failed"
    assert _derive_response_status(payload) == 500

    missing_handlers = api_operator_handlers.api_get_operator_institutional_check(None, {})
    assert missing_handlers["ok"] is False
    assert missing_handlers["error"] == "institutional_check_handlers_unavailable"
    assert missing_handlers["reason_code"] == "handler_resolution_failed"
    assert missing_handlers["meta"]["status"] == 500
    assert _derive_response_status(missing_handlers) == 500

    handler = build_handler(
        [{"method": "GET", "path": "/api/operator/institutionalCheck", "handler": "api_get_operator_institutional_check"}],
        {"api_get_operator_institutional_check": api_operator_handlers.api_get_operator_institutional_check},
        "",
        ctx=handler_ctx,
        static_dir=str(REPO_ROOT / "ui"),
    )
    httpd = run_http_server("127.0.0.1", 0, handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        port = int(httpd.server_address[1])
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/api/operator/institutionalCheck", timeout=3)
            raise AssertionError("internal fault unexpectedly returned HTTP 200")
        except HTTPError as exc:
            body = json.loads(exc.read().decode("utf-8"))
            assert exc.code == 500
    finally:
        httpd.shutdown()
        httpd.server_close()

    assert body["ok"] is False
    assert body["sub_check"] == "readiness"
    assert body["root_cause_code"] == "API_OPERATOR_HANDLERS_INSTITUTIONAL_CHECK_SUBCHECK_FAILED"
    assert body["meta"]["status"] == 500
    assert body["error"] != "request_failed"


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


def test_operator_ui_renders_structured_institutional_check_reasons():
    html = (REPO_ROOT / "boot" / "operator_ui.html").read_text(encoding="utf-8")

    assert "const structuredChecks = Array.isArray(j.checks) ? j.checks : [];" in html
    assert "code: name.toUpperCase()" in html
    assert "structuredChecks.length" in html


def test_operator_sidecar_institutional_check_has_structured_breakdown():
    server = (REPO_ROOT / "boot" / "operator_server.js").read_text(encoding="utf-8")
    block = server[
        server.index('"/api/operator/institutionalCheck"') :
        server.index('app.post("/api/operator/repairSchema"')
    ]

    assert "const checks = [" in block
    assert "const blockers = checks" in block
    assert "const reasons = [];" in block
    assert "checks," in block
    assert "blockers," in block
    assert "reasons," in block
