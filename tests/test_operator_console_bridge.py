import os
import json
import socket
import subprocess
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen


os.environ.setdefault("TIMESCALE_ENABLED", "0")
os.environ.setdefault("FEATURE_STORE_ENABLED", "0")
os.environ.setdefault("FEATURE_STORE_INIT_ON_STARTUP", "0")


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


def _read_json(url: str, *, method: str = "GET", body=None):
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body, separators=(",", ":")).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = Request(url, data=data, headers=headers, method=method)
    with urlopen(req, timeout=5) as response:
        payload = json.loads(response.read().decode("utf-8"))
        return int(response.status), dict(response.headers), payload


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
    assert seen["url"] == "http://127.0.0.1:4555/api/operator/ping"


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
    end = html.index("function operatorTelemetryWsUrl", start)
    bridge_block = html[start:end]

    script = tmp_path / "operator_bridge_prefix_test.mjs"
    script.write_text(
        "\n".join(
            [
                "function run(pathname) {",
                "  const location = { pathname, protocol: 'http:', host: '127.0.0.1:8000' };",
                "  const window = {};",
                bridge_block,
                "  return {",
                "    prefix: OPERATOR_BRIDGE_PREFIX,",
                "    absolute: operatorBridgeUrl('/api/operator/status'),",
                "    summary: operatorBridgeUrl('/api/operator_summary'),",
                "    relative: operatorBridgeUrl('api/operator/start'),",
                "    passthrough: operatorBridgeUrl('/assets/app.js')",
                "  };",
                "}",
                "process.stdout.write(JSON.stringify({",
                "  bridged: run('/operator/'),",
                "  nested: run('/operator/deep/link'),",
                "  direct: run('/')",
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
    assert payload["nested"]["absolute"] == "/operator/api/operator/status"
    assert payload["direct"]["prefix"] == ""
    assert payload["direct"]["absolute"] == "/api/operator/status"


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
    assert payload["websocket"]["direct_url"] == f"ws://127.0.0.1:{sidecar.server_port}/ws/operator"
    assert any(req["path"] == "/api/operator/ping" for req in _SidecarHandler.requests)


def test_operator_api_proxy_forwards_get_post_and_summary_alias(monkeypatch):
    import dashboard_server

    handler_cls = dashboard_server._wrap_operator_console_routes(_BaseDashboardHandler)
    _SidecarHandler.requests = []

    with _http_server(_SidecarHandler) as (_sidecar_url, sidecar):
        monkeypatch.setenv("OPERATOR_BIND_HOST", "127.0.0.1")
        monkeypatch.setenv("OPERATOR_PORT", str(sidecar.server_port))

        with _http_server(handler_cls) as (base_url, _server):
            status, headers, health = _read_json(
                f"{base_url}/operator/api/operator/health?panel=runtime"
            )
            post_status, post_headers, saved = _read_json(
                f"{base_url}/operator/api/operator/config",
                method="POST",
                body={"mode": "safe"},
            )
            summary_status, _summary_headers, summary = _read_json(
                f"{base_url}/operator/api/operator_summary"
            )

    assert status == 200
    assert headers["X-Operator-Console-Bridge"] == "1"
    assert headers["X-Sidecar-Test"] == "1"
    assert health["ok"] is True
    assert health["request"]["path"] == "/api/operator/health"
    assert health["request"]["query"] == "panel=runtime"

    assert post_status == 201
    assert post_headers["X-Operator-Console-Bridge"] == "1"
    assert saved["ok"] is True
    assert saved["request"]["path"] == "/api/operator/config"
    assert json.loads(saved["request"]["body"]) == {"mode": "safe"}

    assert summary_status == 200
    assert summary["ok"] is True
    assert summary["request"]["path"] == "/api/operator_summary"


def test_operator_api_proxy_unavailable_is_graceful_and_actionable(monkeypatch):
    import dashboard_server

    handler_cls = dashboard_server._wrap_operator_console_routes(_BaseDashboardHandler)
    monkeypatch.setenv("OPERATOR_BIND_HOST", "127.0.0.1")
    monkeypatch.setenv("OPERATOR_PORT", str(_unused_local_port()))

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
    assert body["sidecar_ws_url"] == "ws://127.0.0.1:4556/ws/operator"
    assert "HTTP only" in body["detail"]


def test_operator_console_compatibility_files_exist():
    root = Path(__file__).resolve().parents[1]

    assert (root / "boot" / "operator_server.js").exists()
    assert (root / "boot" / "operator_ui.html").exists()
    assert (root / "ui" / "dashboard.html").exists()
