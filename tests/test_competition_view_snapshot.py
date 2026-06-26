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


def _running_system_snapshot() -> dict:
    snapshot = _stub_system_snapshot()
    snapshot.update(
        {
            "ok": True,
            "status": "RUNNING",
            "state": "RUNNING",
            "reasons": ["baseline_reason"],
            "health": {
                "competition": {
                    "ok": True,
                    "replay_status": "ready",
                    "replay_age_s": 5,
                    "reasons": [],
                },
                "attribution": {
                    "ok": True,
                    "warning_row_count": 0,
                    "max_residual_share": 0.0,
                    "quality_status": "ok",
                },
            },
        }
    )
    return snapshot


def _stub_failure_response(_logger, **kwargs) -> dict:
    return {
        "ok": False,
        "error": str(kwargs.get("message") or ""),
        "root_cause_code": str(kwargs.get("code") or ""),
        "failure_scope": str(kwargs.get("event") or ""),
        "failure_type": type(kwargs.get("error")).__name__,
        "system_state_snapshot": {"stubbed": True},
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


def test_observability_snapshot_handlers_return_degraded_payloads(monkeypatch):
    api_system = importlib.import_module("engine.api.api_system")
    monkeypatch.setattr(api_system, "_build_system_snapshot", lambda *_args, **_kwargs: _running_system_snapshot())
    monkeypatch.setattr(api_system, "failure_response", _stub_failure_response)

    monkeypatch.setattr(
        api_system,
        "current_competition_snapshot",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("competition boom")),
    )
    payload = api_system.api_get_competition_view(None, ctx={})
    assert payload["ok"] is False
    assert payload["status"] == "DEGRADED"
    assert any(str(reason).startswith("competition_view_error:") for reason in payload["reasons"])
    assert payload["competition"] == {"ok": False, "error": "competition boom"}
    assert payload["root_cause_code"] == "API_SYSTEM_COMPETITION_VIEW_FAILED"

    monkeypatch.setattr(
        api_system,
        "meta_get",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("meta boom")),
    )
    payload = api_system.api_get_replay_freshness(None, ctx={})
    assert payload["ok"] is False
    assert payload["status"] == "DEGRADED"
    assert any(str(reason).startswith("replay_freshness_error:") for reason in payload["reasons"])
    assert payload["replay_freshness"] == {"ok": False, "error": "meta boom"}
    assert payload["root_cause_code"] == "API_SYSTEM_REPLAY_FRESHNESS_FAILED"

    payload = api_system.api_get_attribution_quality(None, ctx={})
    assert payload["ok"] is False
    assert payload["status"] == "DEGRADED"
    assert any(str(reason).startswith("attribution_quality_error:") for reason in payload["reasons"])
    assert payload["attribution_quality"] == {"ok": False, "error": "meta boom"}
    assert payload["root_cause_code"] == "API_SYSTEM_ATTRIBUTION_QUALITY_FAILED"


def test_observability_snapshot_handlers_keep_success_paths_truthy(monkeypatch):
    api_system = importlib.import_module("engine.api.api_system")
    monkeypatch.setattr(api_system, "_build_system_snapshot", lambda *_args, **_kwargs: _running_system_snapshot())
    monkeypatch.setattr(
        api_system,
        "current_competition_snapshot",
        lambda **_kwargs: {
            "ok": True,
            "status": "ready",
            "reason": "",
            "champion": {"model_name": "champion_v1", "symbol": "SPY", "horizon_s": 60},
            "champions": [{"model_name": "champion_v1"}],
            "ranking_champion": {"model_name": "ranked_v1"},
            "challengers": [{"model_name": "challenger_v1"}],
            "rankings": [{"model_name": "ranked_v1"}],
            "capital_plan": {"allocations": {"global": 1.0}},
            "replay_validation_status": {"status": "ready"},
            "self_critic": {"blocked_keys": []},
            "cycle_status": {"status": "ready"},
            "active_symbols": ["SPY"],
        },
    )

    meta_payloads = {
        "competition_replay_validation": {
            "models": {
                "ranked_v1": {
                    "approved": True,
                    "source": "pytest",
                    "window_end_ms": 123456,
                }
            }
        },
        "competition_replay_validation_status": {
            "status": "ready",
            "fresh": True,
            "stale": False,
            "updated_ts_ms": 123450,
            "model_count": 1,
        },
        "attribution_completeness": {
            "rows": 10,
            "authoritative_model_present": 10,
            "authoritative_model_present_ratio": 1.0,
            "regime_present_ratio": 1.0,
            "policy_present_ratio": 1.0,
        },
        "execution_order_model_identity_repair": {
            "rows_scanned": 5,
            "rows_updated": 1,
        },
        "trade_attribution_historical_repair": {
            "ok": True,
            "ts_ms": 123460,
        },
        "execution_poll_and_attrib_last": {
            "ts_ms": 123470,
        },
    }
    monkeypatch.setattr(
        api_system,
        "meta_get",
        lambda key, default="": json.dumps(meta_payloads.get(key, default if isinstance(default, dict) else {})),
    )

    competition = api_system.api_get_competition_view(None, ctx={})
    assert competition["ok"] is True
    assert competition["competition"]["summary"]["top_ranked_model_name"] == "ranked_v1"
    assert competition["competition"]["runtime"]["status"] == "ready"

    replay = api_system.api_get_replay_freshness(None, ctx={})
    assert replay["ok"] is True
    assert replay["replay_freshness"]["summary"]["approved_model_count"] == 1
    assert replay["replay_freshness"]["sources"] == {"pytest": 1}

    attribution = api_system.api_get_attribution_quality(None, ctx={})
    assert attribution["ok"] is True
    assert attribution["attribution_quality"]["summary"]["rows"] == 10
    assert attribution["attribution_quality"]["historical_repair"]["ok"] is True
