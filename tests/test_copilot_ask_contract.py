from __future__ import annotations

import importlib
import json
import sys
import threading
from contextlib import contextmanager
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from engine.api.http_transport import build_handler  # noqa: E402


_TOKEN = "copilot-contract-token-1234567890"


class _TestHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True


def _read_ok(_parsed=None, _body=None, _ctx=None):
    return {"ok": True, "state": "DEGRADED", "ready": False, "reasons": ["test_degraded"]}


def _install_copilot_read_stubs(monkeypatch: pytest.MonkeyPatch, dashboard_server) -> None:
    monkeypatch.setattr(
        dashboard_server,
        "API_HANDLERS",
        {
            "api_get_health": _read_ok,
            "api_get_readiness": _read_ok,
            "api_get_system_state": _read_ok,
            "api_get_execution_barrier": _read_ok,
            "api_get_market_stress": _read_ok,
            "api_get_training_status": _read_ok,
            "api_get_promotion_status": _read_ok,
        },
    )


@pytest.fixture()
def dashboard_server_module(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("DASHBOARD_ROUTE_CONTRACT_INTROSPECTION", "1")
    monkeypatch.setenv("DB_PATH", str(tmp_path / "copilot_contract.sqlite"))
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("EXECUTION_MODE", "sim")
    monkeypatch.setenv("TS_ENV", "test")
    monkeypatch.setenv("DASHBOARD_API_TOKEN", _TOKEN)
    dashboard_server = importlib.import_module("dashboard_server")
    _install_copilot_read_stubs(monkeypatch, dashboard_server)
    monkeypatch.setattr(dashboard_server, "COPILOT_LLM_ENDPOINT", "")
    monkeypatch.setattr(dashboard_server, "COPILOT_LLM_MODEL", "")
    return dashboard_server


@contextmanager
def _serve_copilot(dashboard_server):
    handler_cls = build_handler(
        ROUTE_SPECS=[
            {"method": "POST", "path": "/api/copilot/ask", "handler": "api_post_copilot_ask"},
        ],
        API_HANDLERS={"api_post_copilot_ask": dashboard_server.api_post_copilot_ask},
        dashboard_api_token=_TOKEN,
        ctx={},
        static_dir=str(REPO_ROOT / "ui"),
    )
    server = _TestHTTPServer(("127.0.0.1", 0), handler_cls)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def _post_copilot(server: _TestHTTPServer, payload) -> tuple[int, dict]:
    data = b"" if payload is None else json.dumps(payload).encode("utf-8")
    req = Request(
        f"http://127.0.0.1:{server.server_port}/api/copilot/ask",
        data=data,
        method="POST",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-API-Token": _TOKEN,
        },
    )
    try:
        with urlopen(req, timeout=5) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


def test_empty_copilot_body_returns_422_missing_question(dashboard_server_module) -> None:
    with _serve_copilot(dashboard_server_module) as server:
        status, payload = _post_copilot(server, None)

    assert status == 422
    assert payload["ok"] is False
    assert payload["error"] == "missing_question"
    assert payload["reason_code"] == "missing_question"
    assert payload["reason"]
    assert payload["answer"]
    assert payload["meta"]["status"] == 422


def test_copilot_without_llm_configuration_is_graceful_business_degraded(dashboard_server_module) -> None:
    with _serve_copilot(dashboard_server_module) as server:
        status, payload = _post_copilot(server, {"question": "why is the system degraded?"})

    assert status == 200
    assert payload["ok"] is False
    assert payload["error"] is None
    assert payload["reason_code"] == "copilot_llm_unconfigured"
    assert "no read-only model endpoint" in payload["answer"]
    assert payload["suggested_actions"]
    assert payload["meta"]["status"] == 200
    assert "request_failed" not in json.dumps(payload, sort_keys=True)


def test_copilot_configured_llm_without_answer_returns_structured_503(
    dashboard_server_module,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(dashboard_server_module, "COPILOT_LLM_ENDPOINT", "http://127.0.0.1:9/copilot")
    monkeypatch.setattr(dashboard_server_module, "_copilot_ask_llm", lambda _prompt_payload: "")

    with _serve_copilot(dashboard_server_module) as server:
        status, payload = _post_copilot(server, {"question": "why is the system degraded?"})

    assert status == 503
    assert payload["ok"] is False
    assert payload["error"] == "copilot_llm_unavailable"
    assert payload["reason_code"] == "copilot_llm_unavailable"
    assert payload["answer"]
    assert payload["suggested_actions"]
    assert payload["meta"]["status"] == 503


def test_copilot_handler_exception_returns_internal_server_error(
    dashboard_server_module,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _boom(_ctx=None):
        raise RuntimeError("secret-like-copilot-failure")

    monkeypatch.setattr(dashboard_server_module, "_copilot_server_context", _boom)

    with _serve_copilot(dashboard_server_module) as server:
        status, payload = _post_copilot(server, {"question": "why is the system degraded?"})

    assert status == 500
    assert payload["ok"] is False
    assert payload["error"] == "internal_server_error"
    assert payload["reason_code"] == "handler_exception"
    assert payload["detail"] == "RuntimeError"
    assert "secret-like-copilot-failure" not in json.dumps(payload, sort_keys=True)
