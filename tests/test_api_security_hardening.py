from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
from contextlib import contextmanager
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen

from engine.api.rate_limit import ApiRateLimiter


REPO_ROOT = Path(__file__).resolve().parents[1]


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return float(self.now)

    def advance(self, seconds: float) -> None:
        self.now += float(seconds)


@contextmanager
def _http_server(handler_cls):
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler_cls)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def _build_handler(*, routes, handlers, token="", ctx=None, static_dir: Path):
    import engine.api.http_transport as http_transport

    http_transport.emit_counter = lambda *args, **kwargs: None
    http_transport.emit_timing = lambda *args, **kwargs: None
    http_transport.deny_if_shutdown = lambda: None

    return http_transport.build_handler(
        ROUTE_SPECS=routes,
        API_HANDLERS=handlers,
        dashboard_api_token=token,
        ctx=ctx or {},
        static_dir=str(static_dir),
    )


def _post_json(
    url: str,
    *,
    token: str | None = None,
    body: dict | None = None,
    extra_headers: dict[str, str] | None = None,
):
    headers = {"Content-Type": "application/json"}
    if token is not None:
        headers["X-API-Token"] = str(token)
    headers.update(extra_headers or {})
    req = Request(
        url,
        data=json.dumps(body or {}).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    with urlopen(req, timeout=5) as response:
        return response.status, dict(response.headers), json.loads(response.read().decode("utf-8"))


def _emergency_confirmation() -> dict:
    return {
        "confirm": "KILL",
        "confirmation": "KILL",
        "consequence_ack": True,
        "confirmation_hold_ms": 3000,
        "actor": "security_test",
        "source": "pytest",
    }


def _enable_safe_dev_localhost_fallback(monkeypatch) -> None:
    monkeypatch.setenv("ENV", "dev")
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("EXECUTION_MODE", "safe")
    monkeypatch.setenv("TS_API_ALLOW_LOCALHOST_MUTATIONS_WITHOUT_TOKEN", "1")
    monkeypatch.delenv("TS_ENV", raising=False)
    monkeypatch.delenv("APP_ENV", raising=False)
    monkeypatch.delenv("PROD_LOCK", raising=False)


def test_production_import_refuses_unset_dashboard_token() -> None:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(REPO_ROOT)
    env["TS_ENV"] = "production"
    env.pop("DASHBOARD_API_TOKEN", None)

    result = subprocess.run(
        [sys.executable, "-c", "import engine.api.http_transport"],
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert result.returncode != 0
    assert "InsecureConfiguration" in result.stderr


def test_production_handler_refuses_missing_dashboard_token(tmp_path: Path, monkeypatch) -> None:
    import engine.api.http_transport as http_transport

    monkeypatch.setenv("ENV", "prod")
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("EXECUTION_MODE", "safe")
    monkeypatch.delenv("DASHBOARD_API_TOKEN", raising=False)

    try:
        _build_handler(
            routes=[("POST", "/api/operator/emergency_stop", "stop")],
            handlers={"stop": lambda *_args: {"ok": True}},
            token="",
            static_dir=tmp_path,
        )
        raise AssertionError("handler unexpectedly allowed missing production token")
    except http_transport.InsecureConfiguration as exc:
        assert "DASHBOARD_API_TOKEN must be set" in str(exc)


def test_live_handler_refuses_default_dashboard_token(tmp_path: Path, monkeypatch) -> None:
    import engine.api.http_transport as http_transport

    monkeypatch.setenv("ENV", "dev")
    monkeypatch.setenv("ENGINE_MODE", "live")
    monkeypatch.setenv("EXECUTION_MODE", "live")

    try:
        _build_handler(
            routes=[("POST", "/api/operator/emergency_stop", "stop")],
            handlers={"stop": lambda *_args: {"ok": True}},
            token="change-me",
            static_dir=tmp_path,
        )
        raise AssertionError("handler unexpectedly allowed default live token")
    except http_transport.InsecureConfiguration as exc:
        assert "placeholder/default" in str(exc)


def test_production_mutation_accepts_valid_dashboard_token(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("ENV", "prod")
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("EXECUTION_MODE", "safe")
    token = "production-token-1234567890"

    handler_cls = _build_handler(
        routes=[("POST", "/api/operator/emergency_stop", "stop")],
        handlers={"stop": lambda _parsed=None, _body=None, _ctx=None: {"ok": True}},
        token=token,
        static_dir=tmp_path,
    )

    with _http_server(handler_cls) as base_url:
        status, _headers, body = _post_json(
            f"{base_url}/api/operator/emergency_stop",
            token=token,
            body=_emergency_confirmation(),
        )

    assert status == 200
    assert body["ok"] is True


def test_high_impact_mutation_rejects_missing_confirmation_with_valid_token(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("ENV", "prod")
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("EXECUTION_MODE", "safe")
    token = "production-token-1234567890"

    handler_cls = _build_handler(
        routes=[("POST", "/api/operator/emergency_stop", "stop")],
        handlers={"stop": lambda _parsed=None, _body=None, _ctx=None: {"ok": True}},
        token=token,
        static_dir=tmp_path,
    )

    with _http_server(handler_cls) as base_url:
        try:
            _post_json(f"{base_url}/api/operator/emergency_stop", token=token)
            raise AssertionError("request unexpectedly succeeded")
        except HTTPError as exc:
            body = json.loads(exc.read().decode("utf-8"))
            assert exc.code == 422
            assert body["error"] == "confirmation_required"
            assert body["action_id"] == "operator.emergency_stop"
            assert body["required_token"] == "KILL"


def test_production_mutation_rejects_invalid_dashboard_token(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("ENV", "prod")
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("EXECUTION_MODE", "safe")
    token = "production-token-1234567890"

    handler_cls = _build_handler(
        routes=[("POST", "/api/operator/emergency_stop", "stop")],
        handlers={"stop": lambda _parsed=None, _body=None, _ctx=None: {"ok": True}},
        token=token,
        static_dir=tmp_path,
    )

    with _http_server(handler_cls) as base_url:
        try:
            _post_json(
                f"{base_url}/api/operator/emergency_stop",
                token="wrong-token",
            )
            raise AssertionError("request unexpectedly succeeded")
        except HTTPError as exc:
            body = json.loads(exc.read().decode("utf-8"))
            assert exc.code == 401
            assert body["error"] == "unauthorized"


def test_localhost_fallback_requires_explicit_safe_dev_mode(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("ENV", "dev")
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("EXECUTION_MODE", "safe")
    monkeypatch.delenv("PROD_LOCK", raising=False)
    monkeypatch.delenv("TS_API_ALLOW_LOCALHOST_MUTATIONS_WITHOUT_TOKEN", raising=False)

    handler_cls = _build_handler(
        routes=[("POST", "/api/operator/restart_feeds", "restart")],
        handlers={"restart": lambda _parsed=None, _body=None, _ctx=None: {"ok": True}},
        token="",
        static_dir=tmp_path,
    )

    with _http_server(handler_cls) as base_url:
        try:
            _post_json(f"{base_url}/api/operator/restart_feeds")
            raise AssertionError("request unexpectedly succeeded")
        except HTTPError as exc:
            body = json.loads(exc.read().decode("utf-8"))
            assert exc.code == 403
            assert body["error"] == "forbidden_localhost_fallback_disabled"

    _enable_safe_dev_localhost_fallback(monkeypatch)
    handler_cls = _build_handler(
        routes=[("POST", "/api/operator/restart_feeds", "restart")],
        handlers={"restart": lambda _parsed=None, _body=None, _ctx=None: {"ok": True}},
        token="",
        static_dir=tmp_path,
    )

    with _http_server(handler_cls) as base_url:
        status, _headers, body = _post_json(f"{base_url}/api/operator/restart_feeds")
        assert status == 200
        assert body["ok"] is True


def test_mutation_writes_audit_event(tmp_path: Path, monkeypatch) -> None:
    import engine.api.http_transport as http_transport

    events = []
    monkeypatch.setattr(http_transport, "_append_mutation_audit_event", lambda payload: events.append(dict(payload)))

    handler_cls = _build_handler(
        routes=[("POST", "/api/data_sources/test", "test_source")],
        handlers={"test_source": lambda _parsed=None, _body=None, _ctx=None: {"ok": True}},
        token="audit-token-1234567890",
        static_dir=tmp_path,
    )

    with _http_server(handler_cls) as base_url:
        status, _headers, body = _post_json(
            f"{base_url}/api/data_sources/test",
            token="audit-token-1234567890",
        )

    assert status == 200
    assert body["ok"] is True
    assert events
    assert events[-1]["outcome"] == "completed"
    assert events[-1]["path"] == "/api/data_sources/test"
    assert events[-1]["token_present"] is True
    assert events[-1]["auth_kind"] == "dashboard_api_token"


def test_audit_records_rejects_unauthorized_table_identifier(tmp_path: Path) -> None:
    from engine.api.api_dashboard_reads import api_get_audit_records

    handler_cls = _build_handler(
        routes=[("GET", "/api/audit/records", "api_get_audit_records")],
        handlers={"api_get_audit_records": api_get_audit_records},
        token="",
        static_dir=tmp_path,
    )

    bad_table = quote("portfolio_state;DROP TABLE x;", safe="")
    with _http_server(handler_cls) as base_url:
        try:
            urlopen(f"{base_url}/api/audit/records?table={bad_table}", timeout=5)
            raise AssertionError("request unexpectedly succeeded")
        except HTTPError as exc:
            body = json.loads(exc.read().decode("utf-8"))
            assert exc.code == 400
            assert body["reason"] == "unauthorized_table"
            assert body["error"] == "unauthorized_table"


def test_token_set_requires_token_even_from_localhost(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("PROD_LOCK", raising=False)
    handler_cls = _build_handler(
        routes=[("POST", "/api/operator/emergency_stop", "stop")],
        handlers={"stop": lambda _parsed=None, _body=None, _ctx=None: {"ok": True}},
        token="secret",
        static_dir=tmp_path,
    )

    with _http_server(handler_cls) as base_url:
        try:
            _post_json(f"{base_url}/api/operator/emergency_stop")
            raise AssertionError("request unexpectedly succeeded")
        except HTTPError as exc:
            body = json.loads(exc.read().decode("utf-8"))
            assert exc.code == 401
            assert body["error"] == "unauthorized"


def test_trusted_proxy_x_forwarded_for_disables_localhost_fallback(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _enable_safe_dev_localhost_fallback(monkeypatch)
    monkeypatch.setenv("TS_DASHBOARD_TRUSTED_PROXIES", "127.0.0.1/32")

    handler_cls = _build_handler(
        routes=[("POST", "/api/operator/restart_feeds", "restart")],
        handlers={"restart": lambda _parsed=None, _body=None, _ctx=None: {"ok": True}},
        token="",
        static_dir=tmp_path,
    )

    with _http_server(handler_cls) as base_url:
        try:
            _post_json(
                f"{base_url}/api/operator/restart_feeds",
                extra_headers={"X-Forwarded-For": "203.0.113.25"},
            )
            raise AssertionError("request unexpectedly succeeded")
        except HTTPError as exc:
            body = json.loads(exc.read().decode("utf-8"))
            assert exc.code == 403
            assert "localhost" in body["error"]


def test_destructive_http_rate_limit_returns_429_retry_after(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("PROD_LOCK", raising=False)
    clock = _Clock()
    limiter = ApiRateLimiter(
        token_limit_per_min=60,
        destructive_limit_per_min=1,
        clock=clock,
    )
    handler_cls = _build_handler(
        routes=[("POST", "/api/operator/emergency_stop", "stop")],
        handlers={"stop": lambda _parsed=None, _body=None, _ctx=None: {"ok": True}},
        token="secret",
        ctx={"API_RATE_LIMITER": limiter},
        static_dir=tmp_path,
    )

    with _http_server(handler_cls) as base_url:
        status, _headers, first = _post_json(
            f"{base_url}/api/operator/emergency_stop",
            token="secret",
            body=_emergency_confirmation(),
        )
        assert status == 200
        assert first["ok"] is True
        try:
            _post_json(f"{base_url}/api/operator/emergency_stop", token="secret")
            raise AssertionError("request unexpectedly succeeded")
        except HTTPError as exc:
            body = json.loads(exc.read().decode("utf-8"))
            assert exc.code == 429
            assert int(exc.headers["Retry-After"]) >= 1
            assert body["error"] == "rate_limit_exceeded"


def test_rate_limit_bucket_exhaustion() -> None:
    clock = _Clock()
    limiter = ApiRateLimiter(token_limit_per_min=2, clock=clock)

    assert limiter.check(token="alpha").allowed is True
    assert limiter.check(token="alpha").allowed is True
    denied = limiter.check(token="alpha")
    assert denied.allowed is False
    assert denied.retry_after_s >= 1


def test_rate_limit_burst_then_idle_refill() -> None:
    clock = _Clock()
    limiter = ApiRateLimiter(token_limit_per_min=2, clock=clock)

    assert limiter.check(token="alpha").allowed is True
    assert limiter.check(token="alpha").allowed is True
    assert limiter.check(token="alpha").allowed is False

    clock.advance(30.0)
    assert limiter.check(token="alpha").allowed is True


def test_rate_limit_distinct_tokens_have_distinct_buckets() -> None:
    clock = _Clock()
    limiter = ApiRateLimiter(token_limit_per_min=1, clock=clock)

    assert limiter.check(token="alpha").allowed is True
    assert limiter.check(token="alpha").allowed is False
    assert limiter.check(token="bravo").allowed is True
