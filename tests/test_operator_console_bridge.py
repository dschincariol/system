import os
import json
import socket
import subprocess
import time
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest


os.environ.setdefault("TIMESCALE_ENABLED", "0")
os.environ.setdefault("FEATURE_STORE_ENABLED", "0")
os.environ.setdefault("FEATURE_STORE_INIT_ON_STARTUP", "0")


@pytest.fixture(autouse=True)
def _dashboard_token_file_for_bridge_tests(tmp_path, monkeypatch):
    token_file = tmp_path / "dashboard_api_token"
    token_file.write_text("dashboard-bridge-test-token-1234567890", encoding="utf-8")
    token_file.chmod(0o600)
    monkeypatch.delenv("DASHBOARD_API_TOKEN", raising=False)
    monkeypatch.setenv("DASHBOARD_API_TOKEN_FILE", str(token_file))
    monkeypatch.setenv("TRADING_SECRET_POLICY_REPO_ROOT", str(tmp_path))


class _FakeResponse:
    status = 200

    headers = {"Content-Type": "application/json"}

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def getcode(self):
        return self.status

    def read(self, *_args):
        return b'{"ok":true,"ts":123}'


class _TestHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True


class _BaseDashboardHandler(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        return

    def _send_fallback(self):
        body = b'{"ok":false,"error":"dashboard_fallback"}'
        self.send_response(404)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        self._send_fallback()

    def do_POST(self):
        self._send_fallback()


class _SidecarHandler(BaseHTTPRequestHandler):
    requests = []

    def log_message(self, *_args):
        return

    def _read_body(self):
        length = int(self.headers.get("Content-Length") or "0")
        return self.rfile.read(length) if length > 0 else b""

    def _send_json(self, payload, status=200, headers=None):
        body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("X-Sidecar-Test", "1")
        for name, value in dict(headers or {}).items():
            self.send_header(str(name), str(value))
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _handle(self):
        from urllib.parse import urlparse

        body = self._read_body()
        parsed = urlparse(self.path)
        record = {
            "method": self.command,
            "path": parsed.path,
            "query": parsed.query,
            "body": body.decode("utf-8", errors="replace"),
            "headers": dict(self.headers.items()),
        }
        type(self).requests.append(record)

        if parsed.path == "/api/operator/ping":
            self._send_json({"ok": True, "service": "mock_operator", "ts": 123})
            return
        if parsed.path == "/api/operator_summary":
            self._send_json({"ok": True, "summary": "mocked", "request": record})
            return
        if parsed.path == "/api/operator/health":
            self._send_json({"ok": True, "health": "green", "request": record})
            return
        if parsed.path == "/api/operator/config" and self.command == "POST":
            self._send_json({"ok": True, "saved": True, "request": record}, status=201)
            return

        self._send_json({"ok": False, "error": "not_found", "request": record}, status=404)

    def do_GET(self):
        self._handle()

    def do_POST(self):
        self._handle()


class _HealthProxyDashboardHandler(BaseHTTPRequestHandler):
    requests = []

    def log_message(self, *_args):
        return

    def _send_json(self, payload, status=200):
        body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        from urllib.parse import urlparse

        parsed = urlparse(self.path)
        record = {
            "method": self.command,
            "path": parsed.path,
            "query": parsed.query,
            "headers": dict(self.headers.items()),
        }
        type(self).requests.append(record)

        if parsed.path == "/api/health":
            self._send_json(
                {
                    "ok": True,
                    "ts_ms": 1_700_000_000_000,
                    "db": {"ok": True, "detail": "test_dashboard_health"},
                    "alert_lifecycle": {"ok": True},
                    "async_price_persistence": {"ok": True},
                }
            )
            return
        if parsed.path == "/api/operator/support_snapshot":
            self._send_json(
                {
                    "ok": True,
                    "mode": "quick",
                    "health": {"ok": True, "source": "test_dashboard_health"},
                    "runtime": {"ok": True},
                }
            )
            return

        self._send_json({"ok": False, "error": "not_found", "path": parsed.path}, status=404)


@contextmanager
def _http_server(handler_cls):
    server = _TestHTTPServer(("127.0.0.1", 0), handler_cls)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def _unused_local_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _read_json(url: str, *, method: str = "GET", body=None, headers=None):
    data = None
    request_headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body, separators=(",", ":")).encode("utf-8")
        request_headers["Content-Type"] = "application/json"
    request_headers.update(dict(headers or {}))
    req = Request(url, data=data, headers=request_headers, method=method)
    with urlopen(req, timeout=5) as response:
        payload = json.loads(response.read().decode("utf-8"))
        return int(response.status), dict(response.headers), payload


def _wait_for_operator_sidecar(proc, port: int):
    deadline = time.time() + 10
    last_error = None
    while time.time() < deadline:
        if proc.poll() is not None:
            out, err = proc.communicate(timeout=1)
            raise AssertionError(f"operator sidecar exited early code={proc.returncode}\nstdout={out}\nstderr={err}")
        try:
            status, _headers, payload = _read_json(f"http://127.0.0.1:{port}/api/operator/ping")
            if status == 200 and payload.get("ok") is True:
                return
        except Exception as exc:
            last_error = exc
            time.sleep(0.1)
    out, err = proc.communicate(timeout=1) if proc.poll() is not None else ("", "")
    raise AssertionError(f"operator sidecar did not start: {last_error}\nstdout={out}\nstderr={err}")


def _read_error_json(url: str, *, method: str = "GET", body=None, headers=None, timeout=10):
    data = None
    request_headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body, separators=(",", ":")).encode("utf-8")
        request_headers["Content-Type"] = "application/json"
    request_headers.update(dict(headers or {}))
    req = Request(url, data=data, headers=request_headers, method=method)
    try:
        with urlopen(req, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
            return int(response.status), dict(response.headers), payload
    except HTTPError as exc:
        payload = json.loads(exc.read().decode("utf-8"))
        return int(exc.code), dict(exc.headers), payload


def _build_operator_bridge_handler(*, token="", events=None, limiter=None, monkeypatch=None):
    import dashboard_server
    import engine.api.http_transport as http_transport

    setter = monkeypatch.setattr if monkeypatch is not None else setattr
    setter(http_transport, "emit_counter", lambda *args, **kwargs: None)
    setter(http_transport, "emit_timing", lambda *args, **kwargs: None)
    setter(http_transport, "deny_if_shutdown", lambda: None)
    if events is not None:
        setter(http_transport, "_append_mutation_audit_event", lambda payload: events.append(dict(payload)))

    base_handler = http_transport.build_handler(
        ROUTE_SPECS=[],
        API_HANDLERS={},
        dashboard_api_token=token,
        ctx={"API_RATE_LIMITER": limiter} if limiter is not None else {},
        static_dir=str(Path(__file__).resolve().parents[1] / "ui"),
    )
    return dashboard_server._wrap_operator_console_routes(base_handler)


def test_operator_sidecar_status_payload_reports_bridge_metadata(monkeypatch):
    import dashboard_server

    seen = {}

    def _fake_urlopen(req, timeout=None):
        seen["url"] = req.full_url
        seen["timeout"] = timeout
        return _FakeResponse()

    monkeypatch.setenv("OPERATOR_BIND_HOST", "0.0.0.0")
    monkeypatch.setenv("OPERATOR_PORT", "4555")
    monkeypatch.setattr(dashboard_server.urllib_request, "urlopen", _fake_urlopen)

    payload = dashboard_server._operator_sidecar_status_payload(timeout_s=0.25)

    assert payload["ok"] is True
    assert payload["reachable"] is True
    assert payload["base_url"] == "http://127.0.0.1:4555"
    assert payload["same_origin_url"] == "/operator/"
    assert payload["http_proxy_prefix"] == "/operator/api/"
    assert "Start or restart" in payload["action"]
    assert payload["websocket"]["proxy_enabled"] is False
    assert payload["websocket"]["direct_sidecar_required"] is True
    assert payload["websocket"]["ticket_endpoint"] == "/operator/ws_ticket"
    assert payload["websocket"]["path"] == "/ws/operator"
    assert payload["websocket"]["origin_checked"] is True
    assert "Sec-WebSocket-Protocol" in payload["websocket"]["auth"]
    assert "direct_url" not in payload["websocket"]
    assert "direct_url" not in payload
    assert seen["url"] == "http://127.0.0.1:4555/api/operator/ping"


def test_operator_bridge_ws_ticket_requires_dashboard_auth_and_does_not_expose_operator_token(monkeypatch):
    token = "dashboard-token-1234567890"
    sidecar_token = "operator-token-1234567890"
    handler_cls = _build_operator_bridge_handler(token=token, monkeypatch=monkeypatch)
    monkeypatch.setenv("OPERATOR_API_TOKEN", sidecar_token)

    with _http_server(handler_cls) as (base_url, _server):
        status, headers, payload = _read_json(
            f"{base_url}/operator/ws_ticket",
            headers={"X-API-Token": token},
        )
        try:
            _read_json(f"{base_url}/operator/ws_ticket")
            raise AssertionError("ticket request unexpectedly succeeded without dashboard token")
        except HTTPError as exc:
            denied_status = exc.code
            denied = json.loads(exc.read().decode("utf-8"))

    raw = json.dumps(payload)
    assert status == 200
    assert headers["X-Operator-Console-Bridge"] == "1"
    assert headers["Cache-Control"] == "no-store"
    assert payload["ok"] is True
    assert payload["protocol"] == "operator-ticket"
    assert payload["ticket"].count(".") == 1
    assert payload["origin"].startswith("http://127.0.0.1:")
    assert payload["expires_at_ms"] > int(time.time() * 1000)
    assert sidecar_token not in raw
    assert denied_status == 401
    assert denied["ok"] is False


def test_operator_bridge_serves_operator_ui_for_prefix_routes():
    import dashboard_server

    handler_cls = dashboard_server._wrap_operator_console_routes(_BaseDashboardHandler)

    with _http_server(handler_cls) as (base_url, _server):
        for path in ("/operator", "/operator/"):
            with urlopen(f"{base_url}{path}", timeout=5) as response:
                body = response.read().decode("utf-8")
                headers = dict(response.headers)

            assert response.status == 200
            assert headers["X-Operator-Console-Bridge"] == "1"
            assert "Trading Engine Operator Panel" in body
            assert "operatorBridgeUrl" in body


def test_operator_ui_prefix_logic_uses_same_origin_operator_api(tmp_path):
    root = Path(__file__).resolve().parents[1]
    html = (root / "boot" / "operator_ui.html").read_text(encoding="utf-8")
    start = html.index("const OPERATOR_BRIDGE_PREFIX =")
    end = html.index("let selectedMode", start)
    bridge_block = html[start:end]

    script = tmp_path / "operator_bridge_prefix_test.mjs"
    script.write_text(
        "\n".join(
            [
                "async function run(pathname) {",
                "  const location = { pathname, protocol: 'http:', host: '127.0.0.1:8000', hostname: '127.0.0.1', search: '?operator_token=operator-token-1234567890&dashboard_token=dashboard-token-1234567890' };",
                "  const store = new Map();",
                "  const window = { sessionStorage: { getItem: (k) => store.get(k) || '', setItem: (k, v) => store.set(k, String(v)) }, localStorage: { getItem: () => '', setItem: () => {} } };",
                bridge_block,
                "  const protocols = await operatorTelemetryWsProtocols();",
                "  const debug = await operatorTelemetryWsDebugInfo();",
                "  return {",
                "    prefix: OPERATOR_BRIDGE_PREFIX,",
                "    absolute: operatorBridgeUrl('/api/operator/status'),",
                "    summary: operatorBridgeUrl('/api/operator_summary'),",
                "    relative: operatorBridgeUrl('api/operator/start'),",
                "    passthrough: operatorBridgeUrl('/assets/app.js'),",
                "    ws: operatorTelemetryWsUrl(),",
                "    legacyWs: operatorTelemetryLegacyWsUrl(),",
                "    protocols,",
                "    debug",
                "  };",
                "}",
                "process.stdout.write(JSON.stringify({",
                "  bridged: await run('/operator/'),",
                "  nested: await run('/operator/deep/link'),",
                "  direct: await run('/')",
                "}));",
            ]
        ),
        encoding="utf-8",
    )

    result = subprocess.run(
        ["node", str(script)],
        cwd=str(root),
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["bridged"]["prefix"] == "/operator"
    assert payload["bridged"]["absolute"] == "/operator/api/operator/status"
    assert payload["bridged"]["summary"] == "/operator/api/operator_summary"
    assert payload["bridged"]["relative"] == "/operator/api/operator/start"
    assert payload["bridged"]["passthrough"] == "/assets/app.js"
    assert payload["bridged"]["ws"] == "ws://127.0.0.1:4001/ws/operator"
    assert payload["bridged"]["legacyWs"] == "ws://127.0.0.1:8000/operator/ws/operator?operator_token=operator-token-1234567890"
    assert payload["bridged"]["protocols"][0] == "operator-token"
    assert payload["bridged"]["protocols"][1].startswith("operator-token.")
    assert payload["bridged"]["debug"]["bridged"] is True
    assert payload["bridged"]["debug"]["tokenProtocol"] is True
    assert payload["bridged"]["debug"]["ticketProtocol"] is False
    assert payload["nested"]["absolute"] == "/operator/api/operator/status"
    assert payload["nested"]["ws"] == "ws://127.0.0.1:4001/ws/operator"
    assert payload["direct"]["prefix"] == ""
    assert payload["direct"]["absolute"] == "/api/operator/status"
    assert payload["direct"]["ws"] == "ws://127.0.0.1:8000/ws/operator"


def test_operator_status_route_reports_useful_sidecar_status(monkeypatch):
    import dashboard_server

    handler_cls = dashboard_server._wrap_operator_console_routes(_BaseDashboardHandler)
    _SidecarHandler.requests = []

    with _http_server(_SidecarHandler) as (_sidecar_url, sidecar):
        monkeypatch.setenv("OPERATOR_BIND_HOST", "127.0.0.1")
        monkeypatch.setenv("OPERATOR_PORT", str(sidecar.server_port))

        with _http_server(handler_cls) as (base_url, _server):
            status, headers, payload = _read_json(f"{base_url}/operator/status")

    assert status == 200
    assert headers["X-Operator-Console-Bridge"] == "1"
    assert payload["ok"] is True
    assert payload["reachable"] is True
    assert payload["service"] == "node_operator_sidecar"
    assert payload["same_origin_url"] == "/operator/"
    assert payload["http_proxy_prefix"] == "/operator/api/"
    assert payload["base_url"] == f"http://127.0.0.1:{sidecar.server_port}"
    assert payload["websocket"]["proxy_enabled"] is False
    assert payload["websocket"]["direct_sidecar_required"] is True
    assert payload["websocket"]["path"] == "/ws/operator"
    assert payload["websocket"]["origin_checked"] is True
    assert "Sec-WebSocket-Protocol" in payload["websocket"]["auth"]
    assert "direct_url" not in payload["websocket"]
    assert any(req["path"] == "/api/operator/ping" for req in _SidecarHandler.requests)


def test_dashboard_operator_ping_bridge_proxies_sidecar_ping(monkeypatch, tmp_path):
    import dashboard_server
    import engine.api.http_transport as http_transport

    monkeypatch.setattr(http_transport, "emit_counter", lambda *args, **kwargs: None)
    monkeypatch.setattr(http_transport, "emit_timing", lambda *args, **kwargs: None)
    monkeypatch.setattr(http_transport, "deny_if_shutdown", lambda: None)
    _SidecarHandler.requests = []

    with _http_server(_SidecarHandler) as (_sidecar_url, sidecar):
        monkeypatch.setenv("OPERATOR_BIND_HOST", "127.0.0.1")
        monkeypatch.setenv("OPERATOR_PORT", str(sidecar.server_port))
        handler_cls = http_transport.build_handler(
            ROUTE_SPECS=[("GET", "/api/operator/ping", "api_get_operator_ping")],
            API_HANDLERS={"api_get_operator_ping": dashboard_server.api_get_operator_ping},
            dashboard_api_token="",
            ctx={},
            static_dir=str(tmp_path),
        )
        with _http_server(handler_cls) as (base_url, _server):
            status, headers, payload = _read_json(f"{base_url}/api/operator/ping")

    assert status == 200
    assert headers["Content-Type"].startswith("application/json")
    assert payload["ok"] is True
    assert payload["service"] == "dashboard_operator_bridge"
    assert payload["operator"]["service"] == "mock_operator"
    assert payload["sidecar"]["reachable"] is True
    assert any(req["path"] == "/api/operator/ping" for req in _SidecarHandler.requests)


def test_operator_api_proxy_forwards_get_post_and_summary_alias(monkeypatch):
    events = []
    token = "dashboard-token-1234567890"
    sidecar_token = "operator-token-1234567890"
    handler_cls = _build_operator_bridge_handler(token=token, events=events, monkeypatch=monkeypatch)
    _SidecarHandler.requests = []

    with _http_server(_SidecarHandler) as (_sidecar_url, sidecar):
        monkeypatch.setenv("OPERATOR_BIND_HOST", "127.0.0.1")
        monkeypatch.setenv("OPERATOR_PORT", str(sidecar.server_port))
        monkeypatch.setenv("OPERATOR_API_TOKEN", sidecar_token)

        with _http_server(handler_cls) as (base_url, _server):
            status, headers, health = _read_json(
                f"{base_url}/operator/api/operator/health?panel=runtime",
                headers={"X-API-Token": token},
            )
            post_status, post_headers, saved = _read_json(
                f"{base_url}/operator/api/operator/config",
                method="POST",
                body={"mode": "safe"},
                headers={"X-API-Token": token},
            )
            summary_status, _summary_headers, summary = _read_json(
                f"{base_url}/operator/api/operator_summary",
                headers={"X-API-Token": token},
            )

    assert status == 200
    assert headers["X-Operator-Console-Bridge"] == "1"
    assert headers["X-Sidecar-Test"] == "1"
    assert health["ok"] is True
    assert health["request"]["path"] == "/api/operator/health"
    assert health["request"]["query"] == "panel=runtime"
    assert health["request"]["headers"]["X-Operator-Token"] == sidecar_token
    assert "X-API-Token" not in health["request"]["headers"]

    assert post_status == 201
    assert post_headers["X-Operator-Console-Bridge"] == "1"
    assert saved["ok"] is True
    assert saved["request"]["path"] == "/api/operator/config"
    assert json.loads(saved["request"]["body"]) == {"mode": "safe"}
    assert saved["request"]["headers"]["X-Operator-Token"] == sidecar_token
    assert "X-API-Token" not in saved["request"]["headers"]

    assert summary_status == 200
    assert summary["ok"] is True
    assert summary["request"]["path"] == "/api/operator_summary"
    assert summary["request"]["headers"]["X-Operator-Token"] == sidecar_token
    assert any(event["outcome"] == "completed" and event["path"] == "/api/operator/config" for event in events)


def test_operator_api_proxy_rejects_unauthenticated_post_before_sidecar(monkeypatch):
    events = []
    token = "dashboard-token-1234567890"
    handler_cls = _build_operator_bridge_handler(token=token, events=events, monkeypatch=monkeypatch)
    _SidecarHandler.requests = []

    with _http_server(_SidecarHandler) as (_sidecar_url, sidecar):
        monkeypatch.setenv("OPERATOR_BIND_HOST", "127.0.0.1")
        monkeypatch.setenv("OPERATOR_PORT", str(sidecar.server_port))
        monkeypatch.setenv("OPERATOR_API_TOKEN", "operator-token-1234567890")

        with _http_server(handler_cls) as (base_url, _server):
            req = Request(
                f"{base_url}/operator/api/operator/config",
                data=b'{"mode":"safe"}',
                headers={"Content-Type": "application/json", "Accept": "application/json"},
                method="POST",
            )
            try:
                urlopen(req, timeout=5)
                raise AssertionError("request unexpectedly succeeded")
            except HTTPError as exc:
                code = exc.code
                body = json.loads(exc.read().decode("utf-8"))

    assert code == 401
    assert body["error"] == "unauthorized"
    assert _SidecarHandler.requests == []
    assert any(event["outcome"] == "auth_denied" and event["path"] == "/api/operator/config" for event in events)


def test_operator_api_proxy_rejects_unauthenticated_get_before_sidecar(monkeypatch):
    token = "dashboard-token-1234567890"
    handler_cls = _build_operator_bridge_handler(token=token, monkeypatch=monkeypatch)
    _SidecarHandler.requests = []

    with _http_server(_SidecarHandler) as (_sidecar_url, sidecar):
        monkeypatch.setenv("OPERATOR_BIND_HOST", "127.0.0.1")
        monkeypatch.setenv("OPERATOR_PORT", str(sidecar.server_port))
        monkeypatch.setenv("OPERATOR_API_TOKEN", "operator-token-1234567890")

        with _http_server(handler_cls) as (base_url, _server):
            req = Request(
                f"{base_url}/operator/api/operator/config",
                headers={"Accept": "application/json"},
                method="GET",
            )
            try:
                urlopen(req, timeout=5)
                raise AssertionError("request unexpectedly succeeded")
            except HTTPError as exc:
                code = exc.code
                body = json.loads(exc.read().decode("utf-8"))

    assert code == 401
    assert body["error"] == "unauthorized"
    assert _SidecarHandler.requests == []


def test_operator_api_proxy_rate_limits_before_sidecar(monkeypatch):
    from engine.api.rate_limit import ApiRateLimiter

    token = "dashboard-token-1234567890"
    sidecar_token = "operator-token-1234567890"
    limiter = ApiRateLimiter(token_limit_per_min=1)
    handler_cls = _build_operator_bridge_handler(
        token=token,
        limiter=limiter,
        monkeypatch=monkeypatch,
    )
    _SidecarHandler.requests = []

    with _http_server(_SidecarHandler) as (_sidecar_url, sidecar):
        monkeypatch.setenv("OPERATOR_BIND_HOST", "127.0.0.1")
        monkeypatch.setenv("OPERATOR_PORT", str(sidecar.server_port))
        monkeypatch.setenv("OPERATOR_API_TOKEN", sidecar_token)

        with _http_server(handler_cls) as (base_url, _server):
            first_status, _first_headers, first = _read_json(
                f"{base_url}/operator/api/operator/config",
                method="POST",
                body={"mode": "safe"},
                headers={"X-API-Token": token},
            )
            req = Request(
                f"{base_url}/operator/api/operator/config",
                data=b'{"mode":"safe"}',
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "X-API-Token": token,
                },
                method="POST",
            )
            try:
                urlopen(req, timeout=5)
                raise AssertionError("request unexpectedly succeeded")
            except HTTPError as exc:
                code = exc.code
                body = json.loads(exc.read().decode("utf-8"))

    assert first_status == 201
    assert first["ok"] is True
    assert code == 429
    assert body["error"] == "rate_limit_exceeded"
    assert len(_SidecarHandler.requests) == 1


def test_operator_api_proxy_unavailable_is_graceful_and_actionable(monkeypatch):
    import dashboard_server

    handler_cls = dashboard_server._wrap_operator_console_routes(_BaseDashboardHandler)
    monkeypatch.setenv("OPERATOR_BIND_HOST", "127.0.0.1")
    monkeypatch.setenv("OPERATOR_PORT", str(_unused_local_port()))
    monkeypatch.setenv("OPERATOR_API_TOKEN", "operator-token-1234567890")

    with _http_server(handler_cls) as (base_url, _server):
        req = Request(f"{base_url}/operator/api/operator/health", headers={"Accept": "application/json"})
        try:
            urlopen(req, timeout=5)
            raise AssertionError("request unexpectedly succeeded")
        except HTTPError as exc:
            code = exc.code
            body = json.loads(exc.read().decode("utf-8"))
            headers = dict(exc.headers)

    assert code == 503
    assert headers["X-Operator-Console-Bridge"] == "1"
    assert body["ok"] is False
    assert body["error"] == "operator_sidecar_unavailable"
    assert "Start or restart" in body["action"]
    assert body["sidecar"]["ok"] is False
    assert body["sidecar"]["reachable"] is False
    assert body["sidecar"]["same_origin_url"] == "/operator/"
    assert body["sidecar"]["http_proxy_prefix"] == "/operator/api/"
    assert body["sidecar"]["websocket"]["proxy_enabled"] is False
    assert "detail" in body and body["detail"]


def test_operator_sidecar_proxy_health_uses_health_contract_and_keeps_snapshot_proxy(tmp_path):
    root = Path(__file__).resolve().parents[1]
    operator_port = _unused_local_port()
    operator_token = "operator-token-1234567890"
    _HealthProxyDashboardHandler.requests = []

    with _http_server(_HealthProxyDashboardHandler) as (dashboard_base, _dashboard):
        dashboard_port = dashboard_base.rsplit(":", 1)[1]
        env_path = tmp_path / "operator.env"
        env_path.write_text(
            "\n".join(
                [
                    f"DASHBOARD_BASE={dashboard_base}",
                    "DASHBOARD_HOST=127.0.0.1",
                    f"DASHBOARD_PORT={dashboard_port}",
                    f"DASHBOARD_API_TOKEN_FILE={os.environ['DASHBOARD_API_TOKEN_FILE']}",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        env = dict(os.environ)
        env.update(
            {
                "OPERATOR_BIND_HOST": "127.0.0.1",
                "OPERATOR_PORT": str(operator_port),
                "OPERATOR_API_TOKEN": operator_token,
                "OPERATOR_ENV_PATH": str(env_path),
                "OPERATOR_DATA_DIR": str(tmp_path / "operator-data"),
                "OPERATOR_AUTO_START": "0",
                "DASHBOARD_BASE": dashboard_base,
                "DASHBOARD_HOST": "127.0.0.1",
                "DASHBOARD_PORT": dashboard_port,
                "NODE_ENV": "test",
            }
        )

        proc = subprocess.Popen(
            ["node", "boot/operator_server.js"],
            cwd=str(root),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            _wait_for_operator_sidecar(proc, operator_port)

            status, _headers, health = _read_json(
                f"http://127.0.0.1:{operator_port}/api/operator/proxy/health",
                headers={"X-Operator-Token": operator_token},
            )
            snapshot_status, _snapshot_headers, snapshot = _read_json(
                f"http://127.0.0.1:{operator_port}/api/operator/support_snapshot?mode=quick",
                headers={"X-Operator-Token": operator_token},
            )
        finally:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.communicate(timeout=5)

    assert status == 200
    assert health["ok"] is True
    assert health["healthy"] is True
    assert health["status"] == 200
    assert health["body"]["ok"] is True
    assert health["body"]["ts_ms"] == 1_700_000_000_000
    assert health["body"]["db"]["ok"] is True
    assert health["body"]["alert_lifecycle"]["ok"] is True
    assert health["body"]["async_price_persistence"]["ok"] is True

    assert snapshot_status == 200
    assert snapshot["ok"] is True
    assert snapshot["mode"] == "quick"
    assert snapshot["health"]["ok"] is True

    assert any(req["path"] == "/api/health" for req in _HealthProxyDashboardHandler.requests)
    assert any(req["path"] == "/api/operator/support_snapshot" for req in _HealthProxyDashboardHandler.requests)


def test_operator_telemetry_websocket_accepts_trusted_token_protocol_and_rejects_bad_handshakes(tmp_path):
    root = Path(__file__).resolve().parents[1]
    operator_port = _unused_local_port()
    operator_token = "operator-token-1234567890"
    _HealthProxyDashboardHandler.requests = []

    with _http_server(_HealthProxyDashboardHandler) as (dashboard_base, _dashboard):
        dashboard_port = dashboard_base.rsplit(":", 1)[1]
        env_path = tmp_path / "operator.env"
        env_path.write_text(
            "\n".join(
                [
                    "DASHBOARD_HOST=127.0.0.1",
                    f"DASHBOARD_PORT={dashboard_port}",
                    f"DASHBOARD_API_TOKEN_FILE={os.environ['DASHBOARD_API_TOKEN_FILE']}",
                    "OPERATOR_ALLOWED_ORIGIN=http://127.0.0.1:8000",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        env = dict(os.environ)
        env.update(
            {
                "OPERATOR_BIND_HOST": "127.0.0.1",
                "OPERATOR_PORT": str(operator_port),
                "OPERATOR_API_TOKEN": operator_token,
                "OPERATOR_ENV_PATH": str(env_path),
                "OPERATOR_DATA_DIR": str(tmp_path / "operator-data"),
                "OPERATOR_AUTO_START": "0",
                "DASHBOARD_HOST": "127.0.0.1",
                "DASHBOARD_PORT": dashboard_port,
                "OPERATOR_ALLOWED_ORIGIN": "http://127.0.0.1:8000",
                "NODE_ENV": "test",
            }
        )

        proc = subprocess.Popen(
            ["node", "boot/operator_server.js"],
            cwd=str(root),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            _wait_for_operator_sidecar(proc, operator_port)
            script = tmp_path / "operator_ws_probe.cjs"
            script.write_text(
                f"""
const crypto = require("crypto");
const WebSocket = require({json.dumps(str(root / "node_modules" / "ws"))});
const url = "ws://127.0.0.1:{operator_port}/ws/operator";
const token = {json.dumps(operator_token)};
const trustedOrigin = "http://127.0.0.1:8000";

function wsTicket(origin) {{
  const payload = {{
    aud: "operator_ws",
    origin,
    exp_ms: Date.now() + 60000,
    iat_ms: Date.now(),
  }};
  const payloadB64 = Buffer.from(JSON.stringify(payload), "utf8").toString("base64url");
  const sig = crypto.createHmac("sha256", token).update(payloadB64).digest("base64url");
  return `${{payloadB64}}.${{sig}}`;
}}

function attempt(name, options) {{
  const protocols = options.protocols || [];
  const origin = options.origin || trustedOrigin;
  const expectFrame = !!options.expectFrame;
  return new Promise((resolve, reject) => {{
    const result = {{ name, upgradeStatus: null, rejectStatus: null, open: false, frame: null, error: null }};
    const ws = new WebSocket(url, protocols, {{ headers: {{ Origin: origin }} }});
    const timer = setTimeout(() => {{
      try {{ ws.terminate(); }} catch {{}}
      reject(new Error(`${{name}} timeout ${{JSON.stringify(result)}}`));
    }}, expectFrame ? 15000 : 5000);
    const done = () => {{
      clearTimeout(timer);
      try {{ ws.close(); }} catch {{}}
      resolve(result);
    }};
    ws.on("upgrade", (res) => {{
      result.upgradeStatus = res.statusCode;
    }});
    ws.on("open", () => {{
      result.open = true;
      if (!expectFrame) done();
    }});
    ws.on("message", (data) => {{
      result.frame = String(data);
      done();
    }});
    ws.on("unexpected-response", (_req, res) => {{
      result.rejectStatus = res.statusCode;
      done();
    }});
    ws.on("error", (err) => {{
      result.error = String((err && err.message) || err);
      if (!expectFrame) done();
    }});
  }});
}}

(async () => {{
  const accepted = await attempt("accepted", {{
    protocols: ["operator-ticket", `operator-ticket.${{wsTicket(trustedOrigin)}}`],
    expectFrame: true,
  }});
  const unauthenticated = await attempt("unauthenticated", {{
    protocols: [],
    expectFrame: false,
  }});
  const crossOrigin = await attempt("crossOrigin", {{
    protocols: ["operator-ticket", `operator-ticket.${{wsTicket(trustedOrigin)}}`],
    origin: "http://evil.example:8000",
    expectFrame: false,
  }});
  const payload = {{ accepted, unauthenticated, crossOrigin }};
  console.log(JSON.stringify(payload));
  if (accepted.upgradeStatus !== 101 || !accepted.open || !accepted.frame || !accepted.frame.includes("health_update")) {{
    process.exit(10);
  }}
  if (unauthenticated.rejectStatus !== 403 || unauthenticated.open) process.exit(11);
  if (crossOrigin.rejectStatus !== 403 || crossOrigin.open) process.exit(12);
}})().catch((err) => {{
  console.error(err && err.stack || err);
  process.exit(1);
}});
""",
                encoding="utf-8",
            )
            probe = subprocess.run(
                ["node", str(script)],
                cwd=str(root),
                capture_output=True,
                text=True,
                timeout=25,
            )
        finally:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.communicate(timeout=5)

    assert probe.returncode == 0, f"stdout={probe.stdout}\nstderr={probe.stderr}"
    payload = json.loads(probe.stdout)
    assert payload["accepted"]["upgradeStatus"] == 101
    assert payload["accepted"]["open"] is True
    frame = json.loads(payload["accepted"]["frame"])
    assert frame["type"] == "health_update"
    assert payload["unauthenticated"]["rejectStatus"] == 403
    assert payload["unauthenticated"]["open"] is False
    assert payload["crossOrigin"]["rejectStatus"] == 403
    assert payload["crossOrigin"]["open"] is False


def test_operator_websocket_bridge_returns_deferred_upgrade_response(monkeypatch):
    import dashboard_server

    handler_cls = dashboard_server._wrap_operator_console_routes(_BaseDashboardHandler)
    monkeypatch.setenv("OPERATOR_BIND_HOST", "127.0.0.1")
    monkeypatch.setenv("OPERATOR_PORT", "4556")

    with _http_server(handler_cls) as (base_url, _server):
        req = Request(
            f"{base_url}/operator/ws/operator",
            headers={"Accept": "application/json", "Upgrade": "websocket"},
        )
        try:
            urlopen(req, timeout=5)
            raise AssertionError("request unexpectedly succeeded")
        except HTTPError as exc:
            code = exc.code
            body = json.loads(exc.read().decode("utf-8"))
            headers = dict(exc.headers)

    assert code == 426
    assert headers["X-Operator-Console-Bridge"] == "1"
    assert headers["Upgrade"] == "websocket"
    assert body["ok"] is False
    assert body["error"] == "websocket_proxy_deferred"
    assert "direct sidecar WebSocket" in body["action"]
    assert "sidecar_ws_url" not in body
    assert "proxies HTTP only" in body["detail"]


def test_operator_sidecar_rejects_sensitive_get_without_operator_token_and_redacts_config(tmp_path):
    root = Path(__file__).resolve().parents[1]
    port = _unused_local_port()
    env_path = tmp_path / "operator.env"
    env_path.write_text(
        "\n".join(
            [
                "SAFE_SETTING=visible",
                "DASHBOARD_API_TOKEN=dashboard-secret-1234567890",
                "ALPACA_KEY_ID=alpaca-key-id-secret",
                "TS_PG_DSN=host=db user=trading password=pg-secret dbname=trading",
                "LIVE_CACHE_REDIS_URL=redis://:redis-secret@redis:6379/0",
                "DATA_SOURCE_MASTER_KEY=master-secret",
                "",
            ]
        ),
        encoding="utf-8",
    )
    env = dict(os.environ)
    env.update(
        {
            "OPERATOR_BIND_HOST": "127.0.0.1",
            "OPERATOR_PORT": str(port),
            "OPERATOR_API_TOKEN": "operator-token-1234567890",
            "OPERATOR_ENV_PATH": str(env_path),
            "OPERATOR_AUTO_START": "0",
            "DASHBOARD_BASE": "http://127.0.0.1:9",
            "DASHBOARD_HOST": "127.0.0.1",
            "DASHBOARD_PORT": "9",
            "NODE_ENV": "test",
        }
    )

    proc = subprocess.Popen(
        ["node", "boot/operator_server.js"],
        cwd=str(root),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.time() + 10
        last_error = None
        while time.time() < deadline:
            if proc.poll() is not None:
                out, err = proc.communicate(timeout=1)
                raise AssertionError(f"operator sidecar exited early code={proc.returncode}\nstdout={out}\nstderr={err}")
            try:
                status, _headers, payload = _read_json(f"http://127.0.0.1:{port}/api/operator/ping")
                if status == 200 and payload.get("ok") is True:
                    break
            except Exception as exc:
                last_error = exc
                time.sleep(0.1)
        else:
            out, err = proc.communicate(timeout=1) if proc.poll() is not None else ("", "")
            raise AssertionError(f"operator sidecar did not start: {last_error}\nstdout={out}\nstderr={err}")

        req = Request(f"http://127.0.0.1:{port}/api/operator/config", headers={"Accept": "application/json"})
        try:
            urlopen(req, timeout=5)
            raise AssertionError("request unexpectedly succeeded")
        except HTTPError as exc:
            code = exc.code
            body = json.loads(exc.read().decode("utf-8"))

        assert code == 403
        assert body["ok"] is False
        assert body["error"] == "operator_forbidden"
        assert body["reason_code"] == "operator_token_required"

        req = Request(f"http://127.0.0.1:{port}/api/operator/support_snapshot", headers={"Accept": "application/json"})
        try:
            urlopen(req, timeout=5)
            raise AssertionError("support snapshot request unexpectedly succeeded")
        except HTTPError as exc:
            snapshot_code = exc.code
            snapshot_body = json.loads(exc.read().decode("utf-8"))

        assert snapshot_code == 403
        assert snapshot_body["ok"] is False
        assert snapshot_body["error"] == "operator_forbidden"
        assert snapshot_body["reason_code"] == "operator_token_required"
        assert "X-Operator-Token" in snapshot_body["required_auth"]

        status, _headers, config = _read_json(
            f"http://127.0.0.1:{port}/api/operator/config",
            headers={"X-Operator-Token": "operator-token-1234567890"},
        )

        assert status == 200
        assert config["ok"] is True
        assert config["SAFE_SETTING"] == "visible"
        assert config["DASHBOARD_API_TOKEN"] == "***REDACTED***"
        assert config["ALPACA_KEY_ID"] == "***REDACTED***"
        assert config["TS_PG_DSN"] == "***REDACTED***"
        assert config["LIVE_CACHE_REDIS_URL"] == "***REDACTED***"
        assert config["DATA_SOURCE_MASTER_KEY"] == "***REDACTED***"
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.communicate(timeout=5)


def test_operator_sidecar_rejects_loopback_post_without_operator_token():
    root = Path(__file__).resolve().parents[1]
    port = _unused_local_port()
    env = dict(os.environ)
    env.update(
        {
            "OPERATOR_BIND_HOST": "127.0.0.1",
            "OPERATOR_PORT": str(port),
            "OPERATOR_API_TOKEN": "operator-token-1234567890",
            "OPERATOR_AUTO_START": "0",
            "DASHBOARD_BASE": "http://127.0.0.1:9",
            "DASHBOARD_HOST": "127.0.0.1",
            "DASHBOARD_PORT": "9",
            "NODE_ENV": "test",
        }
    )

    proc = subprocess.Popen(
        ["node", "boot/operator_server.js"],
        cwd=str(root),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.time() + 10
        last_error = None
        while time.time() < deadline:
            if proc.poll() is not None:
                out, err = proc.communicate(timeout=1)
                raise AssertionError(f"operator sidecar exited early code={proc.returncode}\nstdout={out}\nstderr={err}")
            try:
                status, _headers, payload = _read_json(f"http://127.0.0.1:{port}/api/operator/ping")
                if status == 200 and payload.get("ok") is True:
                    break
            except Exception as exc:
                last_error = exc
                time.sleep(0.1)
        else:
            out, err = proc.communicate(timeout=1) if proc.poll() is not None else ("", "")
            raise AssertionError(f"operator sidecar did not start: {last_error}\nstdout={out}\nstderr={err}")

        req = Request(
            f"http://127.0.0.1:{port}/api/operator/set_mode",
            data=b'{"mode":"safe"}',
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            method="POST",
        )
        try:
            urlopen(req, timeout=5)
            raise AssertionError("request unexpectedly succeeded")
        except HTTPError as exc:
            code = exc.code
            body = json.loads(exc.read().decode("utf-8"))

        assert code == 403
        assert body["ok"] is False
        assert body["error"] == "operator_forbidden"
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.communicate(timeout=5)


def test_operator_sidecar_rejects_missing_structured_confirmation_with_operator_token(tmp_path):
    root = Path(__file__).resolve().parents[1]
    port = _unused_local_port()
    operator_token = "operator-token-1234567890"
    operator_token_file = tmp_path / "operator_api_token"
    operator_token_file.write_text(operator_token, encoding="utf-8")
    operator_token_file.chmod(0o600)
    env = dict(os.environ)
    env.update(
        {
            "OPERATOR_BIND_HOST": "127.0.0.1",
            "OPERATOR_PORT": str(port),
            "OPERATOR_API_TOKEN": operator_token,
            "OPERATOR_API_TOKEN_FILE": str(operator_token_file),
            "OPERATOR_DATA_DIR": str(tmp_path / "operator-data"),
            "OPERATOR_AUTO_START": "0",
            "DASHBOARD_BASE": "http://127.0.0.1:9",
            "DASHBOARD_HOST": "127.0.0.1",
            "DASHBOARD_PORT": "9",
            "NODE_ENV": "test",
        }
    )

    proc = subprocess.Popen(
        ["node", "boot/operator_server.js"],
        cwd=str(root),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.time() + 10
        last_error = None
        while time.time() < deadline:
            if proc.poll() is not None:
                out, err = proc.communicate(timeout=1)
                raise AssertionError(f"operator sidecar exited early code={proc.returncode}\nstdout={out}\nstderr={err}")
            try:
                status, _headers, payload = _read_json(f"http://127.0.0.1:{port}/api/operator/ping")
                if status == 200 and payload.get("ok") is True:
                    break
            except Exception as exc:
                last_error = exc
                time.sleep(0.1)
        else:
            out, err = proc.communicate(timeout=1) if proc.poll() is not None else ("", "")
            raise AssertionError(f"operator sidecar did not start: {last_error}\nstdout={out}\nstderr={err}")

        for path, action_id, required_token in [
            ("/api/operator/factoryReset", "operator.factory_reset", "FACTORY_RESET"),
            ("/api/operator/start", "operator.start", "START_OPERATOR"),
            ("/api/operator/bootstrap", "operator.bootstrap", "BOOTSTRAP_OPERATOR"),
        ]:
            req = Request(
                f"http://127.0.0.1:{port}{path}",
                data=b"{}",
                    headers={
                        "Content-Type": "application/json",
                        "Accept": "application/json",
                        "X-Operator-Token": operator_token,
                    },
                method="POST",
            )
            try:
                urlopen(req, timeout=5)
                raise AssertionError(f"{path} unexpectedly succeeded")
            except HTTPError as exc:
                code = exc.code
                body = json.loads(exc.read().decode("utf-8"))

            assert code == 422
            assert body["ok"] is False
            assert body["error"] == "confirmation_required"
            assert body["action_id"] == action_id
            assert body["required_token"] == required_token
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.communicate(timeout=5)


def test_operator_start_and_restart_fs_faults_return_json_500_and_keep_sidecar_alive(tmp_path):
    root = Path(__file__).resolve().parents[1]
    port = _unused_local_port()
    dashboard_port = _unused_local_port()
    operator_token = "operator-token-1234567890"

    fake_python = tmp_path / "fake-python"
    fake_python.write_text(
        '#!/bin/sh\nif [ "$1" = "--version" ]; then echo "Python 3.11.0"; fi\nexit 0\n',
        encoding="utf-8",
    )
    fake_python.chmod(0o755)

    env_path = tmp_path / "operator.env"
    db_dir = tmp_path / "db"
    db_dir.mkdir()
    env_path.write_text(
        "\n".join(
            [
                "DASHBOARD_HOST=127.0.0.1",
                f"DASHBOARD_PORT={dashboard_port}",
                f"DASHBOARD_BASE=http://127.0.0.1:{dashboard_port}",
                f"DB_PATH={db_dir / 'trading.db'}",
                "AUTO_BOOT_DAEMONS=false",
                "OPERATOR_AUTORESTART=false",
                "DISABLE_LIVE_EXECUTION=1",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    blocked_parent = tmp_path / "blocked"
    blocked_parent.mkdir()
    blocked_parent.chmod(0o500)
    log_dir = blocked_parent / "runtime-logs"

    env = dict(os.environ)
    env.update(
        {
            "OPERATOR_BIND_HOST": "127.0.0.1",
            "OPERATOR_PORT": str(port),
            "OPERATOR_API_TOKEN": operator_token,
            "OPERATOR_ENV_PATH": str(env_path),
            "OPERATOR_DATA_DIR": str(tmp_path / "operator-data"),
            "OPERATOR_AUTO_START": "0",
            "OPERATOR_PYTHON": str(fake_python),
            "OPERATOR_PREFLIGHT_CACHE_TTL_MS": "0",
            "OPERATOR_VALIDATION_TIMEOUT_MS": "1000",
            "OPERATOR_START_REQUEST_TIMEOUT_MS": "5000",
            "TRADING_LOGS": str(log_dir),
            "LOG_DIR": str(log_dir),
            "DASHBOARD_HOST": "127.0.0.1",
            "DASHBOARD_PORT": str(dashboard_port),
            "DASHBOARD_BASE": f"http://127.0.0.1:{dashboard_port}",
            "NODE_ENV": "test",
        }
    )

    proc = subprocess.Popen(
        ["node", "boot/operator_server.js"],
        cwd=str(root),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    def confirmed_body(action_id, token):
        return {
            "mode": "safe",
            "confirmation": token,
            "confirmation_token": token,
            "confirm": token,
            "confirmation_method": "typed_phrase",
            "consequence_ack": True,
            "action_id": action_id,
            "actor": "pytest",
            "source_surface": "operator_sidecar_regression",
            "reason": "filesystem fault route guard test",
        }

    try:
        _wait_for_operator_sidecar(proc, port)

        for path, action_id, token in [
            ("/api/operator/start", "operator.start", "START_OPERATOR"),
            ("/api/operator/restart", "operator.restart", "RESTART_OPERATOR"),
        ]:
            status, _headers, payload = _read_error_json(
                f"http://127.0.0.1:{port}{path}",
                method="POST",
                body=confirmed_body(action_id, token),
                headers={"X-Operator-Token": operator_token},
            )

            assert status == 500
            assert payload["ok"] is False
            assert payload["error"] == "internal_server_error"
            assert "EACCES" in payload.get("detail", "") or "permission denied" in payload.get("detail", "").lower()
            assert proc.poll() is None

            ping_status, _headers, ping_payload = _read_json(f"http://127.0.0.1:{port}/api/operator/ping")
            assert ping_status == 200
            assert ping_payload["ok"] is True
    finally:
        blocked_parent.chmod(0o700)
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.communicate(timeout=5)


def test_operator_console_compatibility_files_exist():
    root = Path(__file__).resolve().parents[1]

    assert (root / "boot" / "operator_server.js").exists()
    assert (root / "boot" / "operator_ui.html").exists()
    assert (root / "ui" / "dashboard.html").exists()
