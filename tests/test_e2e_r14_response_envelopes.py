from __future__ import annotations

import importlib
import json
import sys
import threading
import time
from contextlib import contextmanager
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


TOKEN = "e2e-r14-dashboard-token-1234567890"


class _TestHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True


@contextmanager
def _http_server(handler_cls):
    server = _TestHTTPServer(("127.0.0.1", 0), handler_cls)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def _build_handler(*, routes, handlers, token: str = TOKEN, ctx=None, monkeypatch: pytest.MonkeyPatch):
    from engine.api import http_transport

    monkeypatch.setattr(http_transport, "emit_counter", lambda *args, **kwargs: None)
    monkeypatch.setattr(http_transport, "emit_timing", lambda *args, **kwargs: None)
    monkeypatch.setattr(http_transport, "_append_mutation_audit_event", lambda payload: None)
    monkeypatch.setattr(http_transport, "deny_if_shutdown", lambda: None)
    return http_transport.build_handler(
        ROUTE_SPECS=routes,
        API_HANDLERS=handlers,
        dashboard_api_token=token,
        ctx=ctx or {},
        static_dir=str(REPO_ROOT / "ui"),
    )


def _post_json(url: str, *, token: str = TOKEN, body: dict | None = None):
    request = Request(
        url,
        data=json.dumps(body or {}).encode("utf-8"),
        headers={"Content-Type": "application/json", "X-API-Token": token},
        method="POST",
    )
    with urlopen(request, timeout=5) as response:
        return response.status, json.loads(response.read().decode("utf-8"))


def _get_json(url: str):
    with urlopen(url, timeout=5) as response:
        return response.status, json.loads(response.read().decode("utf-8"))


def _error_json(exc: HTTPError) -> tuple[int, dict]:
    return exc.code, json.loads(exc.read().decode("utf-8"))


def _assert_envelope_matches_status(payload: dict, status: int) -> None:
    assert int((payload.get("meta") or {}).get("status") or 0) == int(status)
    assert bool(payload.get("ok")) is (int(status) < 400)
    if int(status) < 400:
        assert payload.get("error") is None
    else:
        assert str(payload.get("error") or "")


@pytest.fixture(autouse=True)
def _safe_http_env(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("ENV", "dev")
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("EXECUTION_MODE", "safe")
    monkeypatch.setenv("DASHBOARD_API_TOKEN", TOKEN)
    monkeypatch.delenv("TS_ENV", raising=False)
    monkeypatch.delenv("APP_ENV", raising=False)
    monkeypatch.delenv("PROD_LOCK", raising=False)


def test_runtime_watchdogs_degraded_body_keeps_success_envelope(monkeypatch: pytest.MonkeyPatch) -> None:
    from engine.api import api_system

    monkeypatch.setattr(
        api_system,
        "_cached_health_snapshot",
        lambda *, allow_sync_on_miss=True: {
            "ok": False,
            "prices": {"ok": False, "age_s": None},
            "ingestion_freshness": {},
            "events": {},
            "labels": {},
            "model": {},
            "job_summary": {},
            "jobs": {
                "ingestion_runtime": {
                    "running": True,
                    "heartbeat_ts_ms": 123456,
                    "heartbeat_age_s": 1.0,
                    "restart_count": 0,
                    "stale": False,
                }
            },
        },
    )
    handler_cls = _build_handler(
        routes=[("GET", "/api/operator/runtime_watchdogs", "api_get_runtime_watchdogs")],
        handlers={"api_get_runtime_watchdogs": api_system.api_get_runtime_watchdogs},
        token="",
        monkeypatch=monkeypatch,
    )

    with _http_server(handler_cls) as base_url:
        status, payload = _get_json(f"{base_url}/api/operator/runtime_watchdogs")

    assert status == 200
    _assert_envelope_matches_status(payload, 200)
    assert payload["watchdogs_ok"] is False
    assert "price_feed_not_ok" in payload["watchdog_reasons"]
    assert payload["pipeline_watchdog_state"]["ingestion_runtime"]["running"] is True
    assert "request_failed" not in json.dumps(payload, sort_keys=True)


def test_execution_arm_confirmation_refusal_uses_422_and_does_not_call_handler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict] = []

    def _handler(_parsed=None, body=None, _ctx=None):
        calls.append(dict(body or {}))
        return {"ok": True}

    handler_cls = _build_handler(
        routes=[("POST", "/api/operator/execution_arm", "api_post_operator_execution_arm")],
        handlers={"api_post_operator_execution_arm": _handler},
        monkeypatch=monkeypatch,
    )

    with _http_server(handler_cls) as base_url:
        with pytest.raises(HTTPError) as exc_info:
            _post_json(f"{base_url}/api/operator/execution_arm", body={"armed": 1})
        status, payload = _error_json(exc_info.value)

    assert calls == []
    assert status == 422
    _assert_envelope_matches_status(payload, 422)
    assert payload["error"] == "confirmation_required"
    assert payload["action_id"] == "operator.execution_arm"


@pytest.mark.parametrize(
    ("channel", "expected_status", "expected_error"),
    [
        ("webhook", 422, "channel_not_configured"),
        ("unknown-channel", 400, "unknown_channel"),
    ],
)
def test_notifications_test_refusals_are_4xx_and_do_not_deliver(
    channel: str,
    expected_status: int,
    expected_error: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from engine.api import api_ops_handlers
    from engine.runtime import alerts_notify

    delivered: list[str] = []
    recorded: list[dict] = []

    monkeypatch.setattr(alerts_notify, "_smtp_send_message", lambda **kwargs: delivered.append("smtp"))
    monkeypatch.setattr(alerts_notify, "_post_webhook_bytes", lambda **kwargs: delivered.append("webhook"))
    monkeypatch.setattr(alerts_notify, "_record_notification_test", lambda **kwargs: recorded.append(dict(kwargs)))
    monkeypatch.setattr(alerts_notify, "_load_latest_notification_tests", lambda: {})

    handler_cls = _build_handler(
        routes=[("POST", "/api/notifications/test", "api_post_notifications_test")],
        handlers={"api_post_notifications_test": api_ops_handlers.api_post_notifications_test},
        monkeypatch=monkeypatch,
    )

    with _http_server(handler_cls) as base_url:
        with pytest.raises(HTTPError) as exc_info:
            _post_json(f"{base_url}/api/notifications/test", body={"channel": channel})
        status, payload = _error_json(exc_info.value)

    assert delivered == []
    assert recorded and recorded[-1]["ok"] is False
    assert status == expected_status
    _assert_envelope_matches_status(payload, expected_status)
    assert payload["error"] == expected_error


def test_repair_schema_refuses_without_confirmation_via_transport_and_direct_handler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from engine.api import api_self_repair
    from engine.runtime.jobs import repair_schema

    runs: list[str] = []
    monkeypatch.setattr(repair_schema, "run", lambda: runs.append("run") or {"ok": True})

    direct = api_self_repair.api_post_repair_schema(None, None, {})
    assert direct["ok"] is False
    assert direct["error"] == "confirmation_required"
    assert direct["meta"]["status"] == 422
    assert runs == []

    handler_cls = _build_handler(
        routes=[("POST", "/api/repair_schema", "api_post_repair_schema")],
        handlers={"api_post_repair_schema": api_self_repair.api_post_repair_schema},
        monkeypatch=monkeypatch,
    )
    with _http_server(handler_cls) as base_url:
        with pytest.raises(HTTPError) as exc_info:
            _post_json(f"{base_url}/api/repair_schema")
        status, payload = _error_json(exc_info.value)

    assert runs == []
    assert status == 422
    _assert_envelope_matches_status(payload, 422)
    assert payload["error"] == "confirmation_required"
    assert payload["action_id"] == "operator.repair_schema"

    confirmed = api_self_repair.api_post_repair_schema(
        None,
        {"confirmation": "REPAIR_SCHEMA", "consequence_ack": True},
        {},
    )
    assert confirmed["ok"] is True
    assert runs == ["run"]


def test_market_session_reports_clock_open_separately_from_data_readiness(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dashboard_server = importlib.import_module("dashboard_server")
    price_read_router = importlib.import_module("engine.runtime.price_read_router")

    fixed_open = time.struct_time((2026, 6, 24, 10, 0, 0, 2, 175, -1))
    monkeypatch.setattr(dashboard_server.time, "localtime", lambda: fixed_open)
    monkeypatch.setattr(dashboard_server.time, "time", lambda: 1_783_000_800.0)
    monkeypatch.setattr(price_read_router, "fetch_price_rows", lambda *, symbol="", limit=1: [])

    payload = dashboard_server.api_get_market_session({"symbol": "SPY"}, {})

    assert payload["ok"] is True
    assert payload["state"] == "OPEN"
    assert payload["data_ready"] is False
    assert payload["data_reason"] == "no_price_rows"
    assert payload["data_symbol"] == "SPY"
    assert payload["meta"]["status"] == 200
    assert payload["meta"]["data_ready"] is False
