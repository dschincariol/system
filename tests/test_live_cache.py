from __future__ import annotations

import importlib
import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from typing import Any
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


class _FakeEvalshaRedis:
    def __init__(self) -> None:
        self.values: dict[str, bytes] = {}
        self.ttls: dict[str, int] = {}
        self.command_log: list[str] = []
        self.from_url_kwargs: dict[str, Any] = {}
        self.noscript_once = False
        self.ping_count = 0

    def script_load(self, script: str) -> str:
        self.command_log.append("script_load")
        assert "cmsgpack.unpack" in str(script)
        return "fake-sha"

    def _decode_current(self, current: bytes) -> dict[str, Any]:
        from engine.cache import codec

        try:
            decoded = codec.decode(current)
            return dict(decoded or {}) if isinstance(decoded, dict) else {}
        except Exception:
            text = current.decode("utf-8", errors="replace")
            decoded = json.loads(text)
            return dict(decoded or {}) if isinstance(decoded, dict) else {}

    def evalsha(self, sha: str, numkeys: int, *args: Any) -> list[int]:
        self.command_log.append("evalsha")
        assert sha == "fake-sha"
        if self.noscript_once:
            self.noscript_once = False
            raise RuntimeError("NOSCRIPT No matching script. Please use EVAL.")
        key_count = int(numkeys)
        keys = [str(key) for key in args[:key_count]]
        script_args = list(args[key_count:])
        ttl_s = int(script_args[0])
        results: list[int] = []
        assert len(script_args[1:]) == key_count * 2
        for idx, key in enumerate(keys):
            snapshot_ts_ms = int(script_args[1 + (idx * 2)])
            payload = script_args[2 + (idx * 2)]
            assert isinstance(payload, bytes)
            current = self.values.get(str(key))
            if current is not None:
                decoded = self._decode_current(current)
                stored_ts = int((decoded or {}).get("snapshot_ts_ms") or ((decoded or {}).get("payload") or {}).get("ts_ms") or 0)
                if stored_ts > int(snapshot_ts_ms):
                    results.append(0)
                    continue
            self.values[str(key)] = bytes(payload)
            self.ttls[str(key)] = int(ttl_s)
            results.append(1)
        return results

    def get(self, key: str) -> bytes | None:
        self.command_log.append("get")
        return self.values.get(str(key))

    def mget(self, keys: list[str]) -> list[bytes | None]:
        self.command_log.append("mget")
        return [self.values.get(str(key)) for key in keys]

    def delete(self, *keys: str) -> int:
        deleted = 0
        for key in keys:
            if str(key) in self.values:
                deleted += 1
            self.values.pop(str(key), None)
        return deleted

    def scan_iter(self, *, match: str, count: int):
        del count
        prefix = str(match).removesuffix("*")
        yield from [key for key in list(self.values) if str(key).startswith(prefix)]

    def ping(self) -> bool:
        self.ping_count += 1
        return True

    def close(self) -> None:
        self.command_log.append("close")

    def pipeline(self):  # pragma: no cover - this is asserted not to run
        raise AssertionError("redis live cache writes must use evalsha, not WATCH/MULTI")


class _FakeUnlinkPipeline:
    def __init__(self, redis_client: "_FakePipelinedDeleteRedis") -> None:
        self.redis_client = redis_client
        self.command = ""
        self.keys: list[str] = []

    def unlink(self, *keys: str) -> "_FakeUnlinkPipeline":
        self.command = "unlink"
        self.keys = [str(key) for key in keys]
        return self

    def delete(self, *keys: str) -> "_FakeUnlinkPipeline":
        self.command = "delete"
        self.keys = [str(key) for key in keys]
        return self

    def execute(self) -> list[int]:
        for key in self.keys:
            self.redis_client.values.pop(str(key), None)
        self.redis_client.pipeline_batches.append((str(self.command), list(self.keys)))
        return [1 for _key in self.keys]


class _FakeDeletePipeline:
    def __init__(self, redis_client: "_FakePipelinedDeleteRedis") -> None:
        self.redis_client = redis_client
        self.keys: list[str] = []

    def delete(self, *keys: str) -> "_FakeDeletePipeline":
        self.keys = [str(key) for key in keys]
        return self

    def execute(self) -> list[int]:
        for key in self.keys:
            self.redis_client.values.pop(str(key), None)
        self.redis_client.pipeline_batches.append(("delete", list(self.keys)))
        return [1 for _key in self.keys]


class _FakePipelinedDeleteRedis(_FakeEvalshaRedis):
    def __init__(self, *, supports_unlink: bool = True) -> None:
        super().__init__()
        self.supports_unlink = bool(supports_unlink)
        self.pipeline_batches: list[tuple[str, list[str]]] = []
        self.direct_delete_calls = 0

    def delete(self, *keys: str) -> int:
        self.direct_delete_calls += 1
        return super().delete(*keys)

    def pipeline(self, transaction: bool = False):
        assert transaction is False
        self.command_log.append("pipeline")
        if self.supports_unlink:
            return _FakeUnlinkPipeline(self)
        return _FakeDeletePipeline(self)


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

    def _redis_backend_with_fake(self, live_cache: Any, fake: _FakeEvalshaRedis):
        def _from_url(_url: str, **kwargs: Any) -> _FakeEvalshaRedis:
            fake.from_url_kwargs = dict(kwargs)
            return fake

        config = live_cache.LiveCacheConfig(
            backend="redis",
            redis_url="redis://localhost:6379/0",
            redis_key_prefix="trading-system:test-live-cache",
            connect_timeout_s=1.0,
            socket_timeout_s=1.0,
        )
        with patch.object(live_cache, "_redis", object()), patch.object(live_cache, "_redis_dependency_available", return_value=True):
            with patch.object(live_cache, "_redis_from_url", side_effect=_from_url):
                return live_cache.RedisLiveCache(config, requested_backend="redis")

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

    def test_explicit_redis_backend_falls_back_to_memory_when_msgpack_is_unavailable(self) -> None:
        self._set_env("LIVE_CACHE_BACKEND", "redis")
        self._set_env("LIVE_CACHE_REDIS_URL", "redis://localhost:6379/0")
        (live_cache,) = _reload_modules("engine.runtime.live_cache")
        live_cache.close_live_cache()

        with patch.object(live_cache, "_redis", object()), patch.object(live_cache, "_redis_dependency_available", return_value=True):
            with patch.object(live_cache._cache_codec, "msgpack_available", return_value=False):
                with patch.object(live_cache, "_redis_from_url", side_effect=AssertionError("redis should not initialize without msgpack")):
                    snapshot = live_cache.get_live_cache_snapshot()

        self.assertEqual(str(snapshot.get("resolved_backend") or ""), "memory")
        self.assertEqual(str(snapshot.get("requested_backend") or ""), "redis")
        self.assertTrue(bool(snapshot.get("degraded")))
        self.assertIn("msgpack_dependency_unavailable", str(snapshot.get("fallback_reason") or ""))

    def test_redis_live_cache_uses_evalsha_msgpack_and_rejects_older_snapshot(self) -> None:
        (live_cache,) = _reload_modules("engine.runtime.live_cache")
        fake = _FakeEvalshaRedis()
        backend = self._redis_backend_with_fake(live_cache, fake)

        self.assertFalse(bool(fake.from_url_kwargs.get("decode_responses")))
        self.assertTrue(
            backend.set_price_snapshot(
                "AAPL",
                {"symbol": "AAPL", "price": 101.0},
                ttl_s=30,
                snapshot_ts_ms=1000,
            )
        )
        self.assertEqual(fake.command_log, ["script_load", "evalsha"])
        key = "trading-system:test-live-cache:price:AAPL"
        from engine.cache import codec

        encoded = fake.values[key]
        self.assertIsInstance(encoded, bytes)
        decoded = codec.decode(encoded)
        self.assertEqual(int(decoded.get("snapshot_ts_ms") or 0), 1000)
        self.assertEqual(float((decoded.get("payload") or {}).get("price") or 0.0), 101.0)
        self.assertEqual(fake.ttls[key], 30)

        fake.command_log.clear()
        self.assertFalse(
            backend.set_price_snapshot(
                "AAPL",
                {"symbol": "AAPL", "price": 99.0},
                ttl_s=30,
                snapshot_ts_ms=999,
            )
        )
        self.assertEqual(fake.command_log, ["evalsha"])
        self.assertNotIn("get", fake.command_log)
        self.assertEqual(float((codec.decode(fake.values[key]).get("payload") or {}).get("price") or 0.0), 101.0)

        fake.command_log.clear()
        self.assertTrue(
            backend.set_price_snapshot(
                "AAPL",
                {"symbol": "AAPL", "price": 102.0},
                ttl_s=30,
                snapshot_ts_ms=1001,
            )
        )
        self.assertEqual(float((backend.get_price_snapshot("AAPL") or {}).get("price") or 0.0), 102.0)
        self.assertEqual(fake.command_log, ["evalsha", "get"])
        self.assertNotIn("pipeline", fake.command_log)
        metrics = backend.get_snapshot()
        self.assertEqual(str(metrics.get("redis_write_path") or ""), "evalsha_lua_msgpack")
        self.assertEqual(int(metrics.get("redis_evalsha_attempts") or 0), 3)
        self.assertEqual(int(metrics.get("redis_evalsha_results") or 0), 3)
        self.assertEqual(int(metrics.get("redis_write_rejected_older_count") or 0), 1)
        self.assertEqual(int(metrics.get("price_write_count") or 0), 2)

    def test_redis_live_cache_recovers_from_noscript_and_reports_reload_metric(self) -> None:
        (live_cache,) = _reload_modules("engine.runtime.live_cache")
        fake = _FakeEvalshaRedis()
        fake.noscript_once = True
        backend = self._redis_backend_with_fake(live_cache, fake)

        self.assertTrue(
            backend.set_price_snapshot(
                "MSFT",
                {"symbol": "MSFT", "price": 201.0},
                ttl_s=30,
                snapshot_ts_ms=2000,
            )
        )

        self.assertEqual(fake.command_log, ["script_load", "evalsha", "script_load", "evalsha"])
        metrics = backend.get_snapshot()
        self.assertEqual(int(metrics.get("redis_script_load_count") or 0), 2)
        self.assertEqual(int(metrics.get("redis_evalsha_attempts") or 0), 2)
        self.assertEqual(int(metrics.get("redis_evalsha_noscript_reloads") or 0), 1)
        self.assertEqual(int(metrics.get("redis_write_failure_count") or 0), 0)

    def test_redis_live_cache_write_path_metrics_cover_accepted_and_stale_without_get(self) -> None:
        (live_cache,) = _reload_modules("engine.runtime.live_cache")
        fake = _FakeEvalshaRedis()
        backend = self._redis_backend_with_fake(live_cache, fake)
        emitted: list[tuple[str, int, dict[str, Any]]] = []

        fake.command_log.clear()
        with patch.object(
            live_cache,
            "emit_counter",
            side_effect=lambda metric, value=1, **kwargs: emitted.append((str(metric), int(value), dict(kwargs))),
        ):
            self.assertTrue(
                backend.set_price_snapshot(
                    "AAPL",
                    {"symbol": "AAPL", "price": 101.0},
                    ttl_s=30,
                    snapshot_ts_ms=1000,
                )
            )
            self.assertFalse(
                backend.set_price_snapshot(
                    "AAPL",
                    {"symbol": "AAPL", "price": 99.0},
                    ttl_s=30,
                    snapshot_ts_ms=999,
                )
            )

        self.assertEqual(fake.command_log, ["evalsha", "evalsha"])
        self.assertNotIn("get", fake.command_log)
        self.assertEqual(
            [
                (metric, value, payload["extra_tags"])
                for metric, value, payload in emitted
            ],
            [
                (
                    "live_cache_redis_write_path_total",
                    1,
                    {"kind": "price", "mode": "evalsha_lua_msgpack", "result": "accepted"},
                ),
                (
                    "live_cache_redis_write_path_total",
                    1,
                    {"kind": "price", "mode": "evalsha_lua_msgpack", "result": "rejected_older"},
                ),
            ],
        )

    def test_redis_live_cache_keeps_legacy_json_compatibility_for_existing_keys(self) -> None:
        (live_cache,) = _reload_modules("engine.runtime.live_cache")
        fake = _FakeEvalshaRedis()
        backend = self._redis_backend_with_fake(live_cache, fake)
        key = "trading-system:test-live-cache:price:AAPL"
        fake.values[key] = json.dumps(
            {"snapshot_ts_ms": 1000, "payload": {"symbol": "AAPL", "price": 101.0}},
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")

        self.assertFalse(
            backend.set_price_snapshot(
                "AAPL",
                {"symbol": "AAPL", "price": 99.0},
                ttl_s=30,
                snapshot_ts_ms=999,
            )
        )
        self.assertEqual(float((backend.get_price_snapshot("AAPL") or {}).get("price") or 0.0), 101.0)

        self.assertTrue(
            backend.set_price_snapshot(
                "AAPL",
                {"symbol": "AAPL", "price": 102.0},
                ttl_s=30,
                snapshot_ts_ms=1001,
            )
        )
        self.assertEqual(float((backend.get_price_snapshot("AAPL") or {}).get("price") or 0.0), 102.0)

    def test_redis_live_cache_batches_multi_symbol_price_writes_in_one_evalsha(self) -> None:
        (live_cache,) = _reload_modules("engine.runtime.live_cache")
        fake = _FakeEvalshaRedis()
        backend = self._redis_backend_with_fake(live_cache, fake)

        results = backend.set_price_snapshots(
            {
                "AAPL": {"symbol": "AAPL", "price": 101.0},
                "MSFT": {"symbol": "MSFT", "price": 201.0},
                "NVDA": {"symbol": "NVDA", "price": 301.0},
            },
            ttl_s=45,
            snapshot_ts_ms_by_symbol={"AAPL": 1000, "MSFT": 1001, "NVDA": 1002},
        )

        self.assertEqual(results, {"AAPL": True, "MSFT": True, "NVDA": True})
        self.assertEqual(fake.command_log, ["script_load", "evalsha"])
        self.assertEqual(fake.command_log.count("evalsha"), 1)
        self.assertNotIn("get", fake.command_log)
        self.assertNotIn("pipeline", fake.command_log)
        metrics = backend.get_snapshot()
        self.assertEqual(int(metrics.get("redis_evalsha_attempts") or 0), 1)
        self.assertEqual(int(metrics.get("redis_evalsha_results") or 0), 3)
        self.assertEqual(int(metrics.get("price_write_count") or 0), 3)

    def test_redis_live_cache_clear_namespace_batches_unlink_pipeline(self) -> None:
        (live_cache,) = _reload_modules("engine.runtime.live_cache")
        fake = _FakePipelinedDeleteRedis()
        backend = self._redis_backend_with_fake(live_cache, fake)
        prefix = "trading-system:test-live-cache"
        total_price_keys = int(live_cache._REDIS_CLEAR_BATCH_SIZE * 2 + 17)
        for idx in range(total_price_keys):
            fake.values[f"{prefix}:price:SYM{idx}"] = b"x"
        fake.values[f"{prefix}:feature:SYM0"] = b"feature"
        fake.values["other-prefix:price:SYM0"] = b"other"

        fake.command_log.clear()
        backend.clear_price()

        self.assertFalse(any(str(key).startswith(f"{prefix}:price:") for key in fake.values))
        self.assertEqual(fake.values[f"{prefix}:feature:SYM0"], b"feature")
        self.assertEqual(fake.values["other-prefix:price:SYM0"], b"other")
        self.assertEqual(fake.direct_delete_calls, 0)
        self.assertEqual([command for command, _keys in fake.pipeline_batches], ["unlink", "unlink", "unlink"])
        self.assertEqual(sum(len(keys) for _command, keys in fake.pipeline_batches), total_price_keys)
        self.assertLessEqual(max(len(keys) for _command, keys in fake.pipeline_batches), live_cache._REDIS_CLEAR_BATCH_SIZE)

    def test_redis_live_cache_clear_namespace_falls_back_to_delete_pipeline(self) -> None:
        (live_cache,) = _reload_modules("engine.runtime.live_cache")
        fake = _FakePipelinedDeleteRedis(supports_unlink=False)
        backend = self._redis_backend_with_fake(live_cache, fake)
        prefix = "trading-system:test-live-cache"
        fake.values[f"{prefix}:feature:AAPL"] = b"a"
        fake.values[f"{prefix}:feature:MSFT"] = b"m"

        fake.command_log.clear()
        backend.clear_feature()

        self.assertEqual(fake.pipeline_batches, [("delete", [f"{prefix}:feature:AAPL", f"{prefix}:feature:MSFT"])])
        self.assertFalse(any(str(key).startswith(f"{prefix}:feature:") for key in fake.values))
        self.assertEqual(fake.direct_delete_calls, 0)

    def test_redis_live_cache_snapshot_health_check_is_interval_gated(self) -> None:
        (live_cache,) = _reload_modules("engine.runtime.live_cache")
        fake = _FakeEvalshaRedis()
        backend = self._redis_backend_with_fake(live_cache, fake)
        now = [500.0]

        with patch.object(live_cache.time, "monotonic", side_effect=lambda: float(now[0])):
            first = backend.get_snapshot()
            second = backend.get_snapshot()
            now[0] += live_cache._REDIS_HEALTH_CHECK_INTERVAL_S + 0.1
            third = backend.get_snapshot()

        self.assertTrue(bool(first.get("ok")))
        self.assertTrue(bool(second.get("ok")))
        self.assertTrue(bool(third.get("ok")))
        self.assertEqual(fake.ping_count, 2)

    def test_price_cache_record_price_rows_uses_batch_backend_for_multi_symbol_cycle(self) -> None:
        (price_cache,) = _reload_modules("engine.data.price_cache")

        class _BatchBackend:
            def __init__(self) -> None:
                self.command_log: list[str] = []
                self.payloads: dict[str, dict[str, Any]] = {
                    "AAPL": {
                        "symbol": "AAPL",
                        "source": "memory",
                        "recovered_from_db": False,
                        "points": [{"ts_ms": 900, "price": 99.0, "volume": 1.0}],
                    }
                }

            def get_price_snapshot(self, symbol: str) -> dict[str, Any] | None:
                self.command_log.append(f"get:{symbol}")
                return self.payloads.get(str(symbol))

            def set_price_snapshot(self, symbol: str, payload: dict[str, Any], **kwargs: Any) -> bool:
                del kwargs
                self.command_log.append(f"set:{symbol}")
                self.payloads[str(symbol)] = dict(payload)
                return True

            def get_price_snapshots(self, symbols: list[str]) -> dict[str, dict[str, Any] | None]:
                self.command_log.append("mget")
                return {str(symbol): self.payloads.get(str(symbol)) for symbol in symbols}

            def set_price_snapshots(
                self,
                snapshots: dict[str, dict[str, Any]],
                *,
                ttl_s: float,
                snapshot_ts_ms_by_symbol: dict[str, int],
            ) -> dict[str, bool]:
                del ttl_s, snapshot_ts_ms_by_symbol
                self.command_log.append("mset")
                for symbol, payload in snapshots.items():
                    self.payloads[str(symbol)] = dict(payload)
                return {str(symbol): True for symbol in snapshots}

            def clear_price(self, symbol: str | None = None) -> None:
                del symbol

        backend = _BatchBackend()
        rows = [
            {"symbol": "AAPL", "ts_ms": 1000, "price": 101.0, "volume": 10.0, "source": "unit"},
            {"symbol": "MSFT", "ts_ms": 1001, "price": 201.0, "volume": 20.0, "source": "unit"},
            {"symbol": "NVDA", "ts_ms": 1002, "price": 301.0, "volume": 30.0, "source": "unit"},
        ]

        with patch.object(price_cache, "_cache_backend", return_value=backend):
            written = price_cache.record_price_rows(rows)

        self.assertEqual(written, 3)
        self.assertEqual(backend.command_log, ["mget", "mset"])
        self.assertEqual([point["ts_ms"] for point in backend.payloads["AAPL"]["points"]], [900, 1000])
        self.assertEqual([point["ts_ms"] for point in backend.payloads["MSFT"]["points"]], [1001])
        self.assertFalse(any(command.startswith("get:") or command.startswith("set:") for command in backend.command_log))

    def test_price_cache_set_does_not_read_back_after_write(self) -> None:
        (price_cache,) = _reload_modules("engine.data.price_cache")

        class _Backend:
            def __init__(self) -> None:
                self.command_log: list[str] = []

            def set_price_snapshot(self, *args: Any, **kwargs: Any) -> bool:
                del args, kwargs
                self.command_log.append("set_price_snapshot")
                return True

            def get_price_snapshot(self, symbol: str) -> dict[str, Any] | None:
                del symbol
                self.command_log.append("get_price_snapshot")
                return None

        backend = _Backend()
        snapshot = price_cache.PriceSnapshot(
            symbol="AAPL",
            points=(price_cache.PricePoint(ts_ms=1000, price=101.0, volume=1.0),),
        )
        with patch.object(price_cache, "_cache_backend", return_value=backend):
            resolved = price_cache._set_cached_price_snapshot(snapshot)

        self.assertEqual(resolved, snapshot)
        self.assertEqual(backend.command_log, ["set_price_snapshot"])

    def test_price_cache_db_recovery_does_not_read_back_after_write(self) -> None:
        (price_cache,) = _reload_modules("engine.data.price_cache")
        snapshot = price_cache.PriceSnapshot(
            symbol="AAPL",
            points=(price_cache.PricePoint(ts_ms=1000, price=101.0, volume=1.0),),
            source="sqlite",
            recovered_from_db=True,
        )
        calls: list[str] = []

        def _record_price_rows(rows: Any) -> int:
            calls.append("record_price_rows")
            return len(list(rows or []))

        def _unexpected_get(symbol: str) -> None:
            calls.append(f"get:{symbol}")
            return None

        with patch.object(price_cache, "load_symbol_snapshot", return_value=snapshot):
            with patch.object(price_cache, "record_price_rows", side_effect=_record_price_rows):
                with patch.object(price_cache, "_get_cached_price_snapshot", side_effect=_unexpected_get):
                    resolved = price_cache._recover_symbol_from_db("AAPL")

        self.assertEqual(resolved, snapshot)
        self.assertEqual(calls, ["record_price_rows"])

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
