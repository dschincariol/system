from __future__ import annotations

import importlib
import json
import threading
import urllib.request
from pathlib import Path


def _reload_competition_stack(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "competition_view.db"))
    monkeypatch.setenv("TS_STORAGE_BACKEND", "sqlite")
    monkeypatch.setenv("TS_TESTING", "1")
    monkeypatch.setenv("TIMESCALE_ENABLED", "0")
    monkeypatch.setenv("SQLITE_LIVENESS_DB_ENABLED", "0")
    monkeypatch.setenv("SQLITE_LIVENESS_QUEUE_ENABLED", "0")

    storage_sqlite = importlib.reload(importlib.import_module("engine.runtime.storage_sqlite"))
    storage = importlib.reload(importlib.import_module("engine.runtime.storage"))
    runtime_meta = importlib.reload(importlib.import_module("engine.runtime.runtime_meta"))
    repository = importlib.reload(importlib.import_module("engine.strategy.model_competition.repository"))
    champion_manager = importlib.reload(importlib.import_module("engine.strategy.champion_manager"))
    api_system = importlib.reload(importlib.import_module("engine.api.api_system"))
    http_transport = importlib.reload(importlib.import_module("engine.api.http_transport"))
    return {
        "storage_sqlite": storage_sqlite,
        "storage": storage,
        "runtime_meta": runtime_meta,
        "repository": repository,
        "champion_manager": champion_manager,
        "api_system": api_system,
        "http_transport": http_transport,
    }


def _stub_system_snapshot() -> dict:
    return {
        "ok": False,
        "status": "DEGRADED",
        "state": "DEGRADED",
        "mode": "safe",
        "execution_mode": "safe",
        "execution_allowed": False,
        "reasons": ["competition_health_not_ready"],
        "health": {
            "competition": {
                "ok": False,
                "status": "warming_up",
                "reason": "warming_up",
            }
        },
        "system_state_detail": {"competition": {"active_symbols": ["SPY"]}},
    }


def _start_competition_server(stack: dict, tmp_path: Path, monkeypatch):
    api_system = stack["api_system"]
    http_transport = stack["http_transport"]
    monkeypatch.setattr(api_system, "_build_system_snapshot", lambda *_args, **_kwargs: _stub_system_snapshot())
    monkeypatch.setattr(http_transport, "emit_counter", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(http_transport, "emit_timing", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(http_transport, "deny_if_shutdown", lambda: None)

    handler_cls = http_transport.build_handler(
        ROUTE_SPECS=[
            ("GET", "/api/system/competition", "api_get_competition_view"),
            ("GET", "/api/operator/competition", "api_get_competition_view"),
        ],
        API_HANDLERS={"api_get_competition_view": api_system.api_get_competition_view},
        dashboard_api_token="competition-token",
        ctx={},
        static_dir=str(tmp_path),
    )
    httpd = http_transport.run_http_server("127.0.0.1", 0, handler_cls)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    return httpd, thread


def _get_json(base_url: str, path: str) -> tuple[int, dict]:
    request = urllib.request.Request(
        f"{base_url}{path}",
        headers={"X-API-Token": "competition-token"},
        method="GET",
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        return int(response.status), json.loads(response.read().decode("utf-8"))


def _seed_competition_ranking(storage) -> None:
    con = storage.connect()
    try:
        con.execute(
            """
            INSERT INTO model_competition_rankings(
              ranking_scope, model_name, rank, net_pnl, return_pct, max_drawdown,
              win_rate, trade_count, wins, losses, last_trade_ts_ms, source,
              updated_ts_ms, metrics_json
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "global",
                "seeded_champion_v1",
                1,
                42.5,
                2.5,
                1.25,
                0.75,
                8,
                6,
                2,
                123456789,
                "pytest_seed",
                123456999,
                json.dumps({"score": 9.5, "model_id": "seeded"}, separators=(",", ":"), sort_keys=True),
            ),
        )
        con.commit()
    finally:
        con.close()


def test_competition_view_aliases_return_200_for_cold_and_seeded_db(tmp_path, monkeypatch):
    stack = _reload_competition_stack(monkeypatch, tmp_path)
    stack["storage"].init_db()

    httpd, thread = _start_competition_server(stack, tmp_path, monkeypatch)
    base_url = f"http://127.0.0.1:{httpd.server_address[1]}"
    try:
        for path in ("/api/system/competition", "/api/operator/competition"):
            status, payload = _get_json(base_url, path)

            assert status == 200
            assert payload["ok"] is True
            assert payload["error"] is None
            runtime = payload["competition"]["runtime"]
            assert runtime["ok"] is True
            assert runtime["status"] == "empty"
            assert runtime["reason"] == "no_competition_data"
            assert runtime["rankings"] == []
            assert payload["competition"]["reason"] == "no_competition_data"
            assert payload["competition"]["rankings"] == []
            assert payload["competition"]["summary"]["reason"] == "no_competition_data"

        _seed_competition_ranking(stack["storage"])

        for path in ("/api/system/competition", "/api/operator/competition"):
            status, payload = _get_json(base_url, path)

            assert status == 200
            assert payload["ok"] is True
            runtime = payload["competition"]["runtime"]
            assert runtime["status"] == "ready"
            assert runtime["reason"] == ""
            assert runtime["rankings"][0]["model_name"] == "seeded_champion_v1"
            assert runtime["rankings"][0]["score"] == 9.5
            assert payload["competition"]["rankings"][0]["model_name"] == "seeded_champion_v1"
            assert payload["competition"]["summary"]["top_ranked_model_name"] == "seeded_champion_v1"
            assert payload["competition"]["summary"]["rankings"] == 1
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=2)
