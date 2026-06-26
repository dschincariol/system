from __future__ import annotations

import json
import sys
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import urlopen


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from engine.api import http_transport
from engine.api.http_transport import _derive_response_status, _map_error_to_status


class _TestHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True


class HttpTransportStatusContractTests(unittest.TestCase):
    def test_runtime_state_payload_keeps_http_200_when_business_false(self) -> None:
        payload = {
            "ok": False,
            "status": "DEGRADED",
            "state": "UNKNOWN",
            "mode": "safe",
            "execution_mode": "safe",
            "execution_allowed": False,
            "reasons": ["health_not_ok"],
            "health": {"ok": False},
            "ingestion": {},
            "services": {},
            "readiness": {"ok": False},
            "timestamps": {"ts_ms": 1},
        }

        self.assertEqual(_derive_response_status(payload, default_status=200), 200)

    def test_reasoned_business_false_keeps_http_200(self) -> None:
        payload = {
            "ok": False,
            "reason": "warming_up",
            "reason_code": "WARMING_UP",
            "data": {"rows": []},
        }

        self.assertEqual(_derive_response_status(payload, default_status=200), 200)

    def test_business_false_response_preserves_reason_without_request_failed(self) -> None:
        def _business_degraded(_parsed=None, _body=None, _ctx=None):
            return {
                "ok": False,
                "reason": "warming_up",
                "reason_code": "WARMING_UP",
                "data": {"rows": []},
            }

        handler_cls = http_transport.build_handler(
            ROUTE_SPECS=[("GET", "/business_degraded", "api_business_degraded")],
            API_HANDLERS={"api_business_degraded": _business_degraded},
            dashboard_api_token="",
            ctx={},
            static_dir=str(REPO_ROOT / "ui"),
        )
        server = _TestHTTPServer(("127.0.0.1", 0), handler_cls)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with urlopen(f"http://127.0.0.1:{server.server_port}/business_degraded", timeout=5) as response:
                code = response.status
                payload = json.loads(response.read().decode("utf-8"))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

        self.assertEqual(code, 200)
        self.assertIs(payload["error"], None)
        self.assertEqual(payload["reason"], "warming_up")
        self.assertEqual(payload["reason_code"], "WARMING_UP")
        self.assertEqual(payload["meta"]["status"], 200)
        self.assertNotIn("request_failed", json.dumps(payload, sort_keys=True))

    def test_missing_input_maps_to_http_400(self) -> None:
        payload = {"ok": False, "error": "missing_name"}
        self.assertEqual(_derive_response_status(payload, default_status=200), 400)

    def test_unavailable_dependency_maps_to_http_503(self) -> None:
        payload = {"ok": False, "error": "jobs_manager_unavailable"}
        self.assertEqual(_derive_response_status(payload, default_status=200), 503)

    def test_timeout_maps_to_http_504(self) -> None:
        payload = {"ok": False, "error": "request_timeout"}
        self.assertEqual(_derive_response_status(payload, default_status=200), 504)

    def test_explicit_meta_status_wins(self) -> None:
        payload = {
            "ok": False,
            "error": "missing_name",
            "meta": {"status": 422},
        }
        self.assertEqual(_derive_response_status(payload, default_status=200), 422)

    def test_expected_business_refusals_map_to_4xx(self) -> None:
        cases = [
            ({"ok": False, "error": "execution_blocked"}, 403),
            ({"ok": False, "error": "pre_trade_rejected"}, 409),
            ({"ok": False, "error": "polygon_credentials_missing"}, 422),
            ({"ok": False, "error": "polygon_credentials_rejected"}, 401),
            ({"ok": False, "error": "polygon_entitlement_missing"}, 403),
        ]
        for payload, expected in cases:
            with self.subTest(payload=payload):
                self.assertEqual(_derive_response_status(payload, default_status=200), expected)

    def test_not_live_refusals_map_to_http_409_conflict(self) -> None:
        for error_code in [
            "execution_mode_not_live",
            "operator_execution_mode_not_live",
            "mode_not_live",
            "simulated_market_data_not_live",
            "mode_paper_not_live",
        ]:
            with self.subTest(error_code=error_code):
                self.assertEqual(_map_error_to_status(error_code), 409)

        self.assertEqual(
            _derive_response_status(
                {"ok": False, "error": "execution_mode_not_live", "execution_mode": {}},
                default_status=200,
            ),
            409,
        )

    def test_status_mapper_preserves_existing_contracts(self) -> None:
        cases = [
            ("unauthorized", 401),
            ("forbidden", 403),
            ("execution_blocked", 403),
            ("pre_trade_rejected", 409),
            ("unknown_endpoint", 404),
            ("deprecated_endpoint", 410),
            ("rate_limit_exceeded", 429),
            ("missing_credentials", 422),
            ("some_other_error", 500),
        ]
        for error_code, expected in cases:
            with self.subTest(error_code=error_code):
                self.assertEqual(_map_error_to_status(error_code), expected)

    def test_unexpected_handler_exception_still_returns_500(self) -> None:
        def _boom(_parsed=None, _body=None, _ctx=None):
            raise RuntimeError("secret-like-value-must-not-be-returned")

        handler_cls = http_transport.build_handler(
            ROUTE_SPECS=[("GET", "/boom", "api_boom")],
            API_HANDLERS={"api_boom": _boom},
            dashboard_api_token="",
            ctx={},
            static_dir=str(REPO_ROOT / "ui"),
        )
        server = _TestHTTPServer(("127.0.0.1", 0), handler_cls)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            try:
                urlopen(f"http://127.0.0.1:{server.server_port}/boom", timeout=5)
                raise AssertionError("request unexpectedly succeeded")
            except HTTPError as exc:
                code = exc.code
                payload = json.loads(exc.read().decode("utf-8"))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

        self.assertEqual(code, 500)
        self.assertEqual(payload["error"], "internal_server_error")
        self.assertEqual(payload["reason_code"], "handler_exception")
        self.assertEqual(payload["detail"], "RuntimeError")
        self.assertNotIn("secret-like-value", json.dumps(payload, sort_keys=True))


if __name__ == "__main__":
    unittest.main()
