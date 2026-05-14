from __future__ import annotations

import importlib
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


class ExternalServiceReadinessTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.prev_env: dict[str, str | None] = {}
        self._set_env("ARTIFACT_STORE_MIRROR_ROOT", str(Path(self.tmp.name) / "artifact_mirror"))

    def tearDown(self) -> None:
        for key, value in self.prev_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        self.tmp.cleanup()

    def _set_env(self, key: str, value: str | None) -> None:
        if key not in self.prev_env:
            self.prev_env[key] = os.environ.get(key)
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = str(value)

    def _load_module(self):
        import engine.runtime.external_service_readiness as readiness

        return importlib.reload(readiness)

    def test_no_relevant_external_services_returns_empty_summary(self) -> None:
        self._set_env("ARTIFACT_STORE_MIRROR_ROOT", None)
        readiness = self._load_module()

        summary = readiness.check_external_service_readiness()

        self.assertTrue(bool(summary.get("ok")))
        self.assertEqual(list(summary.get("errors") or []), [])
        self.assertEqual(list(summary.get("services") or []), [])

    def test_required_timescale_missing_dsn_fails(self) -> None:
        self._set_env("PREFLIGHT_REQUIRE_TIMESCALE", "1")
        self._set_env("TIMESCALE_DSN", None)
        self._set_env("TIMESCALE_PRICES_DSN", None)
        readiness = self._load_module()

        summary = readiness.check_external_service_readiness()

        self.assertFalse(bool(summary.get("ok")))
        self.assertTrue(any("timescale_primary required but DSN is missing" in item for item in summary.get("errors") or []))

    def test_explicit_redis_backend_reports_reachable_service(self) -> None:
        self._set_env("LIVE_CACHE_BACKEND", "redis")
        self._set_env("LIVE_CACHE_REDIS_URL", "redis://cache.local:6379/0")
        readiness = self._load_module()

        with patch.object(readiness, "_probe_redis", return_value=(True, None)):
            summary = readiness.check_external_service_readiness()

        services = {str(item.get("name")): item for item in list(summary.get("services") or []) if isinstance(item, dict)}
        redis_status = dict(services.get("live_cache_redis") or {})
        self.assertTrue(bool(summary.get("ok")))
        self.assertTrue(bool(redis_status.get("ok")))
        self.assertEqual(redis_status.get("target"), "cache.local:6379")
        self.assertTrue(any("ping ok" in item for item in list(redis_status.get("notes") or [])))

    def test_required_timescale_auth_probe_failure_fails(self) -> None:
        self._set_env("PREFLIGHT_REQUIRE_TIMESCALE", "1")
        self._set_env("TIMESCALE_DSN", "postgresql://trading:bad@db.local:5432/trading")
        self._set_env("TIMESCALE_PRICES_DSN", "postgresql://trading:bad@db.local:5432/trading")
        readiness = self._load_module()

        with patch.object(readiness, "_probe_postgres", return_value=(False, "authentication failed")):
            summary = readiness.check_external_service_readiness()

        self.assertFalse(bool(summary.get("ok")))
        self.assertTrue(any("authentication failed" in item for item in summary.get("errors") or []))

    def test_required_object_storage_needs_endpoint_credentials_and_mirror(self) -> None:
        self._set_env("PREFLIGHT_REQUIRE_OBJECT_STORAGE", "1")
        self._set_env("OBJECT_STORE_ENDPOINT", "http://minio.local:9000")
        self._set_env("OBJECT_STORE_BUCKET", "artifacts")
        self._set_env("OBJECT_STORE_ACCESS_KEY", "minio")
        self._set_env("OBJECT_STORE_SECRET_KEY", "secret")
        self._set_env("ARTIFACT_STORE_MIRROR_ROOT", None)
        readiness = self._load_module()

        with patch.object(readiness, "_probe_object_storage_bucket", return_value=(True, None)):
            summary = readiness.check_external_service_readiness()

        self.assertFalse(bool(summary.get("ok")))
        self.assertTrue(any("artifact_mirror_root missing" in item for item in summary.get("errors") or []))

    def test_required_object_storage_bucket_probe_failure_fails(self) -> None:
        self._set_env("PREFLIGHT_REQUIRE_OBJECT_STORAGE", "1")
        self._set_env("OBJECT_STORE_ENDPOINT", "http://minio.local:9000")
        self._set_env("OBJECT_STORE_BUCKET", "artifacts")
        self._set_env("OBJECT_STORE_ACCESS_KEY", "minio")
        self._set_env("OBJECT_STORE_SECRET_KEY", "secret")
        readiness = self._load_module()

        with patch.object(
            readiness,
            "_probe_object_storage_bucket",
            return_value=(False, "object storage credentials rejected"),
        ):
            summary = readiness.check_external_service_readiness()

        self.assertFalse(bool(summary.get("ok")))
        self.assertTrue(any("credentials rejected" in item for item in summary.get("errors") or []))


if __name__ == "__main__":
    unittest.main()
