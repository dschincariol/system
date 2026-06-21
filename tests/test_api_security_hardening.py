from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import types
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


def _get_json(
    url: str,
    *,
    token: str | None = None,
    extra_headers: dict[str, str] | None = None,
):
    headers = {}
    if token is not None:
        headers["X-API-Token"] = str(token)
    headers.update(extra_headers or {})
    req = Request(url, headers=headers, method="GET")
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


def _structured_confirmation(
    *,
    token: str,
    action_id: str,
    target: str = "pytest_target",
    reason: str = "pytest structured confirmation",
) -> dict:
    return {
        "confirm": token,
        "confirmation": token,
        "confirmation_token": token,
        "confirmation_method": "typed_phrase",
        "consequence_ack": True,
        "confirmation_hold_ms": 0,
        "actor": "security_test",
        "source": "pytest",
        "source_surface": "pytest",
        "reason": reason,
        "request_id": "pytest-request-id",
        "target": target,
        "action_id": action_id,
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
    env.pop("DASHBOARD_API_TOKEN_FILE", None)
    env.pop("DASHBOARD_API_TOKEN_SECRET", None)
    env.pop("TS_SECRETS_PROVIDER", None)
    env.pop("CREDENTIALS_DIRECTORY", None)
    env.pop("TS_DEV_SECRETS_DIR", None)

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


def test_production_auth_config_accepts_dashboard_token_secret(monkeypatch) -> None:
    from engine.api import auth_config

    monkeypatch.setenv("ENV", "prod")
    monkeypatch.setenv("PROD_LOCK", "1")
    monkeypatch.delenv("DASHBOARD_API_TOKEN", raising=False)
    monkeypatch.setenv("DASHBOARD_API_TOKEN_SECRET", "dashboard_api_token")

    def fake_load_secret(name: str) -> bytes:
        assert name == "dashboard_api_token"
        return b"production-token-1234567890"

    fake_loader = types.ModuleType("services.secrets.loader")
    fake_loader.load_secret = fake_load_secret
    monkeypatch.setitem(sys.modules, "services.secrets.loader", fake_loader)

    state = auth_config.validate_mutation_auth_config()

    assert state["ok"] is True
    assert state["dashboard_api_token_configured"] is True


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
    monkeypatch.setenv("DASHBOARD_API_TOKEN", token)

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
    monkeypatch.setenv("DASHBOARD_API_TOKEN", token)

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
    monkeypatch.setenv("DASHBOARD_API_TOKEN", "production-token-1234567890")
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
        status, _headers, body = _post_json(
            f"{base_url}/api/operator/restart_feeds",
            body=_structured_confirmation(
                token="RESTART_FEEDS",
                action_id="operator.restart_feeds",
                target="market_data_jobs",
            ),
        )
        assert status == 200
        assert body["ok"] is True


def test_operator_self_repair_rejects_missing_confirmation_with_valid_token(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("ENV", "prod")
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("EXECUTION_MODE", "safe")
    monkeypatch.setenv("DASHBOARD_API_TOKEN", "production-token-1234567890")
    token = "production-token-1234567890"
    handler_cls = _build_handler(
        routes=[("POST", "/api/operator/self_repair", "self_repair")],
        handlers={"self_repair": lambda _parsed=None, _body=None, _ctx=None: {"ok": True}},
        token=token,
        static_dir=tmp_path,
    )

    with _http_server(handler_cls) as base_url:
        try:
            _post_json(f"{base_url}/api/operator/self_repair", token=token)
            raise AssertionError("request unexpectedly succeeded")
        except HTTPError as exc:
            body = json.loads(exc.read().decode("utf-8"))
            assert exc.code == 422
            assert body["error"] == "confirmation_required"
            assert body["action_id"] == "operator.self_repair"
            assert body["required_token"] == "SYSTEM_FIX"


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


def _sensitive_get_routes_and_handlers():
    routes = [
        ("GET", "/api/system/config", "system_config"),
        ("GET", "/api/operator/logs", "operator_logs"),
        ("GET", "/api/operator/support_snapshot", "support_snapshot"),
        ("GET", "/api/terminal/positions", "terminal_positions"),
    ]
    handlers = {
        "system_config": lambda *_args: {
            "ok": True,
            "config": {
                "database_dsn": "postgresql://runtime:db-password@db.internal:5432/trading",
                "dashboard_api_token": "production-token-1234567890",
                "provider_api_key": "provider-key-abc",
                "authorization": "Bearer config-auth-token",
                "broker_account_id": "ACC-CONFIG-123",
                "env": "prod",
            },
        },
        "operator_logs": lambda *_args: {
            "ok": True,
            "text": (
                "Authorization: Bearer log-auth-token\n"
                "TIMESCALE_DSN=postgres://reader:reader-pass@timescale:5432/trading\n"
                "api_key=log-api-key broker_order_id=BRK-ORDER-123 account_id=ACC-LOG-456"
            ),
            "lines": [
                "password=hunter2",
                "Authorization: Bearer line-auth-token",
            ],
        },
        "support_snapshot": lambda *_args: {
            "ok": True,
            "diagnostics": {
                "env": {
                    "TS_PG_DSN": "postgres://app:pg-secret@pg:5432/trading",
                    "TRADIER_API_TOKEN": "tradier-secret",
                },
                "headers": {"Authorization": "Bearer support-auth-token"},
                "broker_account_id": "ACC-SUPPORT-789",
            },
        },
        "terminal_positions": lambda *_args: {
            "ok": True,
            "rows": [
                {
                    "symbol": "AAPL",
                    "qty": 5,
                    "avg_px": 190.0,
                    "account_id": "ACC-POS-999",
                    "broker_order_id": "BRK-POS-123",
                }
            ],
        },
    }
    return routes, handlers


def test_sensitive_gets_require_dashboard_token_in_production_and_redact(
    tmp_path: Path,
    monkeypatch,
) -> None:
    import engine.api.http_transport as http_transport

    monkeypatch.setenv("ENV", "prod")
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("EXECUTION_MODE", "safe")
    monkeypatch.setenv("DASHBOARD_API_TOKEN", "production-token-1234567890")
    monkeypatch.setenv("TS_PG_DSN", "postgres://envuser:env-pass@env-db:5432/trading")

    events = []
    monkeypatch.setattr(http_transport, "_append_mutation_audit_event", lambda payload: events.append(dict(payload)))

    routes, handlers = _sensitive_get_routes_and_handlers()
    handler_cls = _build_handler(
        routes=routes,
        handlers=handlers,
        token="production-token-1234567890",
        ctx={"API_RATE_LIMITER": ApiRateLimiter(token_limit_per_min=100, ip_limit_per_min=100)},
        static_dir=tmp_path,
    )

    with _http_server(handler_cls) as base_url:
        for method, path, _handler in routes:
            assert method == "GET"
            try:
                _get_json(f"{base_url}{path}")
                raise AssertionError(f"{path} unexpectedly allowed unauthenticated GET")
            except HTTPError as exc:
                body = json.loads(exc.read().decode("utf-8"))
                assert exc.code == 401
                assert body["error"] == "unauthorized"

            status, _headers, body = _get_json(
                f"{base_url}{path}",
                token="production-token-1234567890",
            )
            assert status == 200
            assert body["ok"] is True
            serialized = json.dumps(body, sort_keys=True)
            for leaked in (
                "db-password",
                "production-token-1234567890",
                "provider-key-abc",
                "config-auth-token",
                "log-auth-token",
                "reader-pass",
                "log-api-key",
                "hunter2",
                "line-auth-token",
                "pg-secret",
                "tradier-secret",
                "support-auth-token",
                "ACC-CONFIG-123",
                "ACC-LOG-456",
                "ACC-SUPPORT-789",
                "ACC-POS-999",
                "BRK-ORDER-123",
                "BRK-POS-123",
            ):
                assert leaked not in serialized
            assert "<redacted" in serialized

    protected_paths = {event["path"] for event in events if event.get("method") == "GET"}
    assert {route[1] for route in routes}.issubset(protected_paths)
    assert all(
        event.get("route_sensitivity") == "sensitive"
        for event in events
        if event.get("method") == "GET" and event.get("outcome") == "completed"
    )


def test_sensitive_get_rejects_query_string_token_in_production(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("ENV", "prod")
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("EXECUTION_MODE", "safe")
    token = "production-token-1234567890"

    routes, handlers = _sensitive_get_routes_and_handlers()
    handler_cls = _build_handler(
        routes=routes,
        handlers=handlers,
        token=token,
        static_dir=tmp_path,
    )

    with _http_server(handler_cls) as base_url:
        try:
            _get_json(f"{base_url}/api/system/config?token={token}")
            raise AssertionError("query-string token unexpectedly authenticated")
        except HTTPError as exc:
            body = json.loads(exc.read().decode("utf-8"))
            assert exc.code == 401
            assert body["error"] == "query_token_forbidden"


def test_sensitive_get_requires_dashboard_token_on_remote_bind(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("ENV", "dev")
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("EXECUTION_MODE", "safe")
    monkeypatch.setenv("DASHBOARD_HOST", "0.0.0.0")
    token = "remote-bind-token-1234567890"

    routes, handlers = _sensitive_get_routes_and_handlers()
    handler_cls = _build_handler(
        routes=routes,
        handlers=handlers,
        token=token,
        static_dir=tmp_path,
    )

    with _http_server(handler_cls) as base_url:
        try:
            _get_json(f"{base_url}/api/terminal/positions")
            raise AssertionError("remote-bind sensitive GET unexpectedly allowed no-token access")
        except HTTPError as exc:
            body = json.loads(exc.read().decode("utf-8"))
            assert exc.code == 401
            assert body["error"] == "unauthorized"

        status, _headers, body = _get_json(
            f"{base_url}/api/terminal/positions",
            token=token,
        )
        assert status == 200
        assert body["ok"] is True


def test_sensitive_gets_are_rate_limited_when_protected(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("ENV", "prod")
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("EXECUTION_MODE", "safe")
    token = "production-token-1234567890"
    clock = _Clock()
    limiter = ApiRateLimiter(token_limit_per_min=1, ip_limit_per_min=1, clock=clock)

    routes, handlers = _sensitive_get_routes_and_handlers()
    handler_cls = _build_handler(
        routes=routes,
        handlers=handlers,
        token=token,
        ctx={"API_RATE_LIMITER": limiter},
        static_dir=tmp_path,
    )

    with _http_server(handler_cls) as base_url:
        status, _headers, body = _get_json(f"{base_url}/api/operator/logs", token=token)
        assert status == 200
        assert body["ok"] is True
        try:
            _get_json(f"{base_url}/api/operator/logs", token=token)
            raise AssertionError("second protected GET unexpectedly bypassed rate limit")
        except HTTPError as exc:
            limited = json.loads(exc.read().decode("utf-8"))
            assert exc.code == 429
            assert int(exc.headers["Retry-After"]) >= 1
            assert limited["error"] == "rate_limit_exceeded"


def test_public_health_get_stays_unauthenticated_in_production(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("ENV", "prod")
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("EXECUTION_MODE", "safe")

    handler_cls = _build_handler(
        routes=[("GET", "/api/health", "health")],
        handlers={"health": lambda *_args: {"ok": True, "status": "RUNNING"}},
        token="production-token-1234567890",
        static_dir=tmp_path,
    )

    with _http_server(handler_cls) as base_url:
        status, _headers, body = _get_json(f"{base_url}/api/health")

    assert status == 200
    assert body["ok"] is True


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
