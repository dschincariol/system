"""End-to-end LAN-relevant HTTP behavior for the dashboard transport.

Exercises the real ``build_handler`` / ``run_http_server`` stack on an ephemeral
loopback port and asserts:

* JSON API responses are gzip-compressed when the client advertises gzip and
  the payload is large enough (the main bandwidth win over a LAN link).
* Small payloads and clients that do not advertise gzip are left uncompressed.
* Static assets receive a revalidatable ``Cache-Control`` window; HTML shells
  are marked ``no-cache`` so a deploy is picked up immediately.
* A lightweight public health route responds.
"""

from __future__ import annotations

import gzip
import json
import sys
import threading
import unittest
import urllib.request
from pathlib import Path
from tempfile import TemporaryDirectory


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from engine.api.http_transport import build_handler, run_http_server


def _big_payload(_parsed=None, _ctx=None):
    # Comfortably above the default 1 KiB gzip threshold.
    return {"ok": True, "rows": [{"i": i, "label": "x" * 64} for i in range(200)]}


def _tiny_payload(_parsed=None, _ctx=None):
    return {"ok": True, "v": 1}


def _health(_parsed=None, _ctx=None):
    return {"ok": True, "status": "ALIVE"}


class DashboardHttpLanBehaviorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._tmp = TemporaryDirectory()
        static_dir = Path(cls._tmp.name)
        (static_dir / "app.js").write_text("export const x = 1;\n", encoding="utf-8")
        (static_dir / "page.html").write_text("<!doctype html><title>t</title>", encoding="utf-8")

        route_specs = [
            ("GET", "/api/bigjson", "big"),
            ("GET", "/api/tinyjson", "tiny"),
            ("GET", "/api/health", "health"),
        ]
        api_handlers = {"big": _big_payload, "tiny": _tiny_payload, "health": _health}

        handler_cls = build_handler(
            route_specs,
            api_handlers,
            dashboard_api_token="",
            ctx={},
            static_dir=str(static_dir),
        )
        cls._httpd = run_http_server("127.0.0.1", 0, handler_cls)
        cls._port = cls._httpd.server_address[1]
        cls._thread = threading.Thread(target=cls._httpd.serve_forever, daemon=True)
        cls._thread.start()

    @classmethod
    def tearDownClass(cls) -> None:
        try:
            cls._httpd.shutdown()
            cls._httpd.server_close()
        finally:
            cls._tmp.cleanup()

    def _get(self, path: str, headers: dict | None = None):
        url = f"http://127.0.0.1:{self._port}{path}"
        req = urllib.request.Request(url, headers=headers or {})
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, dict(resp.headers), resp.read()

    def test_large_json_is_gzip_compressed_when_accepted(self) -> None:
        status, headers, body = self._get(
            "/api/bigjson", headers={"Accept-Encoding": "gzip"}
        )
        self.assertEqual(status, 200)
        self.assertEqual(headers.get("Content-Encoding"), "gzip")
        self.assertIn("Accept-Encoding", headers.get("Vary", ""))
        decoded = json.loads(gzip.decompress(body).decode("utf-8"))
        self.assertTrue(decoded["ok"])
        self.assertEqual(len(decoded["rows"]), 200)

    def test_no_gzip_when_client_does_not_accept(self) -> None:
        status, headers, body = self._get("/api/bigjson", headers={"Accept-Encoding": "identity"})
        self.assertEqual(status, 200)
        self.assertIsNone(headers.get("Content-Encoding"))
        # Raw (uncompressed) JSON is directly parseable.
        self.assertTrue(json.loads(body.decode("utf-8"))["ok"])

    def test_small_json_is_not_compressed_even_when_accepted(self) -> None:
        status, headers, _ = self._get("/api/tinyjson", headers={"Accept-Encoding": "gzip"})
        self.assertEqual(status, 200)
        self.assertIsNone(headers.get("Content-Encoding"))

    def test_static_asset_gets_cache_control(self) -> None:
        status, headers, _ = self._get("/app.js")
        self.assertEqual(status, 200)
        cache = headers.get("Cache-Control", "")
        self.assertIn("max-age=", cache)
        self.assertIn("must-revalidate", cache)
        self.assertEqual(headers.get("X-Content-Type-Options"), "nosniff")

    def test_html_shell_is_no_cache(self) -> None:
        status, headers, _ = self._get("/page.html")
        self.assertEqual(status, 200)
        self.assertEqual(headers.get("Cache-Control"), "no-cache")

    def test_health_route_responds(self) -> None:
        status, _headers, body = self._get("/api/health")
        self.assertEqual(status, 200)
        self.assertTrue(json.loads(body.decode("utf-8"))["ok"])


if __name__ == "__main__":
    unittest.main()
