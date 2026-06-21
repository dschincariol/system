from __future__ import annotations

import importlib
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


class LiveCacheTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.prev_env: dict[str, str | None] = {}
        self._set_env("DB_PATH", str(Path(self.tmp.name) / "live_cache.db"))
        self._set_env("ENGINE_SUPERVISED", "1")
        self._set_env("PRICE_CACHE_TTL_S", "3600")
        self._set_env("FEATURE_STORE_TTL_S", "3600")
        self._set_env("LIVE_CACHE_BACKEND", None)
        self._set_env("LIVE_CACHE_REDIS_URL", None)
        self._set_env("REDIS_URL", None)
        self._set_env("REDIS_CACHE_URL", None)

    def tearDown(self) -> None:
        try:
            (live_cache,) = _reload_modules("engine.runtime.live_cache")
            live_cache.close_live_cache()
        except Exception:
            pass
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

    def test_auto_backend_defaults_to_memory_when_redis_is_not_configured(self) -> None:
        (live_cache,) = _reload_modules("engine.runtime.live_cache")

        snapshot = live_cache.get_live_cache_snapshot()

        self.assertEqual(str(snapshot.get("resolved_backend") or ""), "memory")
        self.assertEqual(str(snapshot.get("requested_backend") or ""), "auto")
        self.assertFalse(bool(snapshot.get("degraded")))
        self.assertIsNone(snapshot.get("fallback_reason"))

    def test_explicit_redis_backend_falls_back_to_memory_when_dependency_is_unavailable(self) -> None:
        self._set_env("LIVE_CACHE_BACKEND", "redis")
        self._set_env("LIVE_CACHE_REDIS_URL", "redis://localhost:6379/0")
        (live_cache,) = _reload_modules("engine.runtime.live_cache")
        live_cache.close_live_cache()

        with patch.object(live_cache, "_redis", None), patch.object(
            live_cache, "_REDIS_IMPORT_ERROR", "ModuleNotFoundError:redis"
        ):
            snapshot = live_cache.get_live_cache_snapshot()

        self.assertEqual(str(snapshot.get("resolved_backend") or ""), "memory")
        self.assertEqual(str(snapshot.get("requested_backend") or ""), "redis")
        self.assertTrue(bool(snapshot.get("degraded")))
        self.assertIn("redis_dependency_unavailable", str(snapshot.get("fallback_reason") or ""))

    def test_redis_backend_resolves_password_secret(self) -> None:
        self._set_env("LIVE_CACHE_BACKEND", "redis")
        self._set_env("LIVE_CACHE_REDIS_URL", "redis://localhost:6379/0")
        self._set_env("LIVE_CACHE_REDIS_PASSWORD_SECRET", "redis_password")
        (live_cache,) = _reload_modules("engine.runtime.live_cache")

        with patch.object(live_cache, "_secret_text_from_env", return_value="secret"):
            config = live_cache.LiveCacheConfig.from_env()

        self.assertEqual(config.redis_url, "redis://:secret@localhost:6379/0")

    def test_redis_backend_resolves_password_file(self) -> None:
        secret_file = Path(self.tmp.name) / "redis_password"
        secret_file.write_text("file-secret", encoding="utf-8")
        secret_file.chmod(0o600)
        self._set_env("LIVE_CACHE_BACKEND", "redis")
        self._set_env("LIVE_CACHE_REDIS_URL", "redis://localhost:6379/0")
        self._set_env("LIVE_CACHE_REDIS_PASSWORD_FILE", str(secret_file))
        (live_cache,) = _reload_modules("engine.runtime.live_cache")

        config = live_cache.LiveCacheConfig.from_env()

        self.assertEqual(config.redis_url, "redis://:file-secret@localhost:6379/0")

    @pytest.mark.requires_redis
    def test_explicit_redis_live_cache_round_trip(self) -> None:
        redis_url = os.environ.get("TS_REDIS_URL") or os.environ.get("REDIS_URL") or "redis://127.0.0.1:6379/0"
        self._set_env("LIVE_CACHE_BACKEND", "redis")
        self._set_env("LIVE_CACHE_REDIS_URL", redis_url)
        self._set_env("LIVE_CACHE_REDIS_KEY_PREFIX", f"trading-system:test:{os.getpid()}:{int(time.time() * 1000)}")
        (live_cache,) = _reload_modules("engine.runtime.live_cache")
        live_cache.close_live_cache()
        backend = live_cache.get_live_cache()
        try:
            snapshot = live_cache.get_live_cache_snapshot()
            if str(snapshot.get("resolved_backend") or "") != "redis":
                raise unittest.SkipTest(f"redis live cache backend unavailable: {snapshot}")
            self.assertEqual(str(snapshot.get("resolved_backend") or ""), "redis")
            self.assertTrue(bool(snapshot.get("ok")), snapshot)

            self.assertTrue(
                backend.set_price_snapshot(
                    "ZZREDIS",
                    {"symbol": "ZZREDIS", "price": 123.45},
                    ttl_s=30,
                    snapshot_ts_ms=1000,
                )
            )
            self.assertEqual(float((backend.get_price_snapshot("ZZREDIS") or {}).get("price") or 0.0), 123.45)

            self.assertTrue(
                backend.set_feature_snapshot(
                    "ZZREDIS",
                    {"symbol": "ZZREDIS", "features": {"f": 1.0}},
                    ttl_s=30,
                    snapshot_ts_ms=1000,
                )
            )
            feature = backend.get_feature_snapshot("ZZREDIS") or {}
            self.assertEqual(dict(feature.get("features") or {}), {"f": 1.0})
        finally:
            backend.clear_price("ZZREDIS")
            backend.clear_feature("ZZREDIS")
            live_cache.close_live_cache()

    @pytest.mark.requires_postgres
    def test_price_and_feature_surfaces_report_live_cache_backend(self) -> None:
        storage, live_cache, price_cache, feature_store = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.live_cache",
            "engine.data.price_cache",
            "engine.data.feature_store",
        )
        storage.init_db()
        base_ts_ms = int(time.time() * 1000) - (30 * 60 * 1000)
        rows = [
            {
                "symbol": "AAPL",
                "ts_ms": int(base_ts_ms + (idx * 60_000)),
                "price": float(180.0 + idx),
                "volume": float(2_000 + (idx * 25)),
                "source": "unit_test",
            }
            for idx in range(25)
        ]

        price_cache.record_price_rows(rows)
        price_snapshot = price_cache.get_symbol_snapshot("AAPL", allow_db_recovery=False)
        stored = feature_store.store_features("AAPL", feature_store.compute_features("AAPL", price_snapshot))
        price_health = price_cache.get_cache_snapshot()
        feature_health = feature_store.get_feature_store_snapshot(timescale_snapshot={"enabled": False, "started": False, "queue_depth": 0})
        backend_snapshot = live_cache.get_live_cache_snapshot()

        self.assertGreater(int(stored.get("ts_ms") or 0), 0)
        self.assertEqual(str(backend_snapshot.get("resolved_backend") or ""), "memory")
        self.assertEqual(str(price_health.get("backend") or ""), "memory")
        self.assertEqual(str(((feature_health.get("cache") or {}).get("backend") or "")), "memory")

    def test_get_live_cache_logs_previous_backend_close_failures(self) -> None:
        (live_cache,) = _reload_modules("engine.runtime.live_cache")

        class _BrokenBackend(live_cache.MemoryLiveCache):
            def close(self) -> None:
                raise RuntimeError("close boom")

        previous = _BrokenBackend(requested_backend="memory")
        replacement = live_cache.MemoryLiveCache(requested_backend="memory")
        live_cache._LIVE_CACHE = previous
        live_cache._LIVE_CACHE_CONFIG_KEY = "stale"

        with patch.object(live_cache, "_warn_nonfatal") as warn_nonfatal:
            with patch.object(live_cache.LiveCacheConfig, "from_env", return_value=live_cache.LiveCacheConfig.from_env()):
                with patch.object(live_cache, "_build_live_cache", return_value=replacement):
                    resolved = live_cache.get_live_cache()

        self.assertIs(resolved, replacement)
        warn_nonfatal.assert_called_once()

    def test_close_live_cache_logs_backend_close_failures(self) -> None:
        (live_cache,) = _reload_modules("engine.runtime.live_cache")

        class _BrokenBackend(live_cache.MemoryLiveCache):
            def close(self) -> None:
                raise RuntimeError("close boom")

        live_cache._LIVE_CACHE = _BrokenBackend(requested_backend="memory")
        live_cache._LIVE_CACHE_CONFIG_KEY = "active"

        with patch.object(live_cache, "_warn_nonfatal") as warn_nonfatal:
            live_cache.close_live_cache()

        warn_nonfatal.assert_called_once()

    def test_base_live_cache_contract_is_safe_noop(self) -> None:
        (live_cache,) = _reload_modules("engine.runtime.live_cache")
        backend = live_cache._BaseLiveCache()

        backend.clear_price("AAPL")
        backend.clear_feature("AAPL")

        self.assertIsNone(backend.get_price_snapshot("AAPL"))
        self.assertIsNone(backend.get_feature_snapshot("AAPL"))
        self.assertFalse(backend.set_price_snapshot("AAPL", {"price": 1.0}, ttl_s=1.0, snapshot_ts_ms=1))
        self.assertFalse(backend.set_feature_snapshot("AAPL", {"x": 1.0}, ttl_s=1.0, snapshot_ts_ms=1))
        self.assertEqual(str(backend.get_snapshot().get("fallback_reason") or ""), "base_live_cache_no_backend")


if __name__ == "__main__":
    unittest.main()
