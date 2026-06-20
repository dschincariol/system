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

    def test_explicit_redis_backend_resolves_password_secret_before_probe(self) -> None:
        self._set_env("LIVE_CACHE_BACKEND", "redis")
        self._set_env("LIVE_CACHE_REDIS_URL", "redis://cache.local:6379/0")
        self._set_env("LIVE_CACHE_REDIS_PASSWORD_SECRET", "redis_password")
        readiness = self._load_module()

        with patch.object(readiness, "_secret_text_from_env", return_value="redis-secret") as load_secret:
            with patch.object(readiness, "_probe_redis", return_value=(True, None)) as probe:
                summary = readiness.check_external_service_readiness()

        self.assertTrue(bool(summary.get("ok")))
        load_secret.assert_any_call(
            "LIVE_CACHE_REDIS_PASSWORD_SECRET",
            "TS_REDIS_PASSWORD_SECRET",
            "REDIS_PASSWORD_SECRET",
        )
        probe.assert_called_with("redis://:redis-secret@cache.local:6379/0", timeout_s=2.0)

    def test_required_timescale_auth_probe_failure_fails(self) -> None:
        self._set_env("PREFLIGHT_REQUIRE_TIMESCALE", "1")
        self._set_env("TIMESCALE_DSN", "postgresql://trading:bad@db.local:5432/trading")
        self._set_env("TIMESCALE_PRICES_DSN", "postgresql://trading:bad@db.local:5432/trading")
        readiness = self._load_module()

        with patch.object(readiness, "_probe_postgres", return_value=(False, "authentication failed")):
            summary = readiness.check_external_service_readiness()

        self.assertFalse(bool(summary.get("ok")))
        self.assertTrue(any("authentication failed" in item for item in summary.get("errors") or []))

    def test_required_timescale_url_with_inline_password_skips_secret_resolution(self) -> None:
        self._set_env("PREFLIGHT_REQUIRE_TIMESCALE", "1")
        self._set_env("TIMESCALE_DSN", "postgresql://trading:inline-pass@db.local:5432/trading")
        self._set_env("TIMESCALE_PRICES_DSN", None)
        self._set_env("TS_SECRETS_PROVIDER", "systemd-creds")
        self._set_env("CREDENTIALS_DIRECTORY", None)
        readiness = self._load_module()

        with patch.object(readiness, "_probe_postgres", return_value=(True, None)) as probe:
            summary = readiness.check_external_service_readiness()

        self.assertTrue(bool(summary.get("ok")))
        self.assertEqual(probe.call_count, 2)
        probe.assert_called_with(
            "postgresql://trading:inline-pass@db.local:5432/trading",
            timeout_s=2.0,
        )

    def test_required_timescale_resolves_passwordless_dsn_before_probe(self) -> None:
        self._set_env("PREFLIGHT_REQUIRE_TIMESCALE", "1")
        self._set_env("TIMESCALE_DSN", "host=db.local port=5432 user=trading dbname=trading")
        self._set_env("TIMESCALE_PRICES_DSN", None)
        readiness = self._load_module()

        with patch.object(
            readiness,
            "_postgres_probe_dsn",
            return_value="host=db.local port=5432 user=trading dbname=trading password=secret",
        ) as resolve:
            with patch.object(readiness, "_probe_postgres", return_value=(True, None)) as probe:
                summary = readiness.check_external_service_readiness()

        self.assertTrue(bool(summary.get("ok")))
        self.assertEqual(resolve.call_count, 2)
        probe.assert_called_with(
            "host=db.local port=5432 user=trading dbname=trading password=secret",
            timeout_s=2.0,
        )

    def test_required_timescale_credential_resolution_failure_fails(self) -> None:
        self._set_env("PREFLIGHT_REQUIRE_TIMESCALE", "1")
        self._set_env("TIMESCALE_DSN", "host=db.local port=5432 user=trading dbname=trading")
        self._set_env("TIMESCALE_PRICES_DSN", None)
        readiness = self._load_module()

        with patch.object(readiness, "_postgres_probe_dsn", side_effect=RuntimeError("missing credential")):
            with patch.object(readiness, "_probe_postgres", side_effect=AssertionError("probe should not run")):
                summary = readiness.check_external_service_readiness()

        self.assertFalse(bool(summary.get("ok")))
        self.assertTrue(any("credential resolution failed" in item for item in summary.get("errors") or []))
        self.assertTrue(any("missing credential" in item for item in summary.get("errors") or []))

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

    def test_required_object_storage_missing_credentials_skips_bucket_probe(self) -> None:
        self._set_env("PREFLIGHT_REQUIRE_OBJECT_STORAGE", "1")
        self._set_env("OBJECT_STORE_ENDPOINT", "http://minio.local:9000")
        self._set_env("OBJECT_STORE_BUCKET", None)
        self._set_env("OBJECT_STORE_ACCESS_KEY", None)
        self._set_env("OBJECT_STORE_SECRET_KEY", None)
        readiness = self._load_module()

        with patch.object(readiness, "_probe_object_storage_bucket", side_effect=AssertionError("probe should not run")):
            summary = readiness.check_external_service_readiness()

        self.assertFalse(bool(summary.get("ok")))
        self.assertTrue(any("bucket is missing" in item for item in summary.get("errors") or []))
        self.assertTrue(any("access key is missing" in item for item in summary.get("errors") or []))
        self.assertTrue(any("secret key is missing" in item for item in summary.get("errors") or []))

    def test_required_object_storage_resolves_secret_key_credential(self) -> None:
        self._set_env("PREFLIGHT_REQUIRE_OBJECT_STORAGE", "1")
        self._set_env("OBJECT_STORE_ENDPOINT", "http://minio.local:9000")
        self._set_env("OBJECT_STORE_BUCKET", "artifacts")
        self._set_env("OBJECT_STORE_ACCESS_KEY", "minio")
        self._set_env("OBJECT_STORE_SECRET_KEY", None)
        self._set_env("OBJECT_STORE_SECRET_KEY_SECRET", "object_store_secret_key")
        readiness = self._load_module()

        with patch.object(readiness, "_secret_text_from_env", return_value="object-secret") as load_secret:
            with patch.object(readiness, "_probe_object_storage_bucket", return_value=(True, None)) as probe:
                summary = readiness.check_external_service_readiness()

        self.assertTrue(bool(summary.get("ok")))
        load_secret.assert_called_with(
            "OBJECT_STORE_SECRET_KEY_SECRET",
            "MINIO_SECRET_KEY_SECRET",
            "AWS_SECRET_ACCESS_KEY_SECRET",
        )
        kwargs = dict(probe.call_args.kwargs)
        self.assertEqual(kwargs.get("secret_key"), "object-secret")

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
