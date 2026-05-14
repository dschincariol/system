from __future__ import annotations

import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from engine.api.http_transport import _derive_response_status


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


if __name__ == "__main__":
    unittest.main()
