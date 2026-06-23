from __future__ import annotations

import importlib
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_pool_uses_env_url_and_pool_size(monkeypatch):
    redis_pool = importlib.reload(importlib.import_module("engine.cache.redis_pool"))
    circuit = importlib.reload(importlib.import_module("engine.cache.circuit"))
    calls = []

    class FakeClient:
        def ping(self):
            calls.append(("ping",))
            return True

    class FakeRedis:
        @staticmethod
        def from_url(url, **kwargs):
            calls.append(("from_url", url, kwargs))
            return FakeClient()

    monkeypatch.setenv("TS_REDIS_URL", "redis://127.0.0.1:6380/2")
    monkeypatch.setenv("TS_REDIS_POOL_SIZE", "7")
    monkeypatch.setattr(redis_pool, "redis", type("RedisModule", (), {"Redis": FakeRedis}))
    circuit.reset_global_circuit()
    redis_pool.reset_redis_pool()

    client = redis_pool.redis_pool()

    assert isinstance(client, FakeClient)
    assert calls[0][0] == "from_url"
    assert calls[0][1] == "redis://127.0.0.1:6380/2"
    assert calls[0][2]["decode_responses"] is False
    assert calls[0][2]["max_connections"] == 7
    assert calls[1] == ("ping",)


def test_pool_health_check_is_interval_gated(monkeypatch):
    redis_pool = importlib.reload(importlib.import_module("engine.cache.redis_pool"))
    circuit = importlib.reload(importlib.import_module("engine.cache.circuit"))
    calls = []
    now = [100.0]

    class FakeClient:
        def ping(self):
            calls.append(("ping",))
            return True

    class FakeRedis:
        @staticmethod
        def from_url(url, **kwargs):
            calls.append(("from_url", url, kwargs))
            return FakeClient()

    monkeypatch.setenv("TS_REDIS_URL", "redis://127.0.0.1:6380/2")
    monkeypatch.setenv("TS_REDIS_POOL_HEALTHCHECK_INTERVAL_S", "10")
    monkeypatch.setattr(redis_pool.time, "monotonic", lambda: float(now[0]))
    monkeypatch.setattr(redis_pool, "redis", type("RedisModule", (), {"Redis": FakeRedis}))
    circuit.reset_global_circuit()
    redis_pool.reset_redis_pool()

    first = redis_pool.redis_pool()
    for _idx in range(5):
        assert redis_pool.redis_pool() is first
    assert [call[0] for call in calls].count("from_url") == 1
    assert [call[0] for call in calls].count("ping") == 1

    now[0] += 10.1
    assert redis_pool.redis_pool() is first
    assert [call[0] for call in calls].count("from_url") == 1
    assert [call[0] for call in calls].count("ping") == 2


def test_pool_health_check_can_recover_stale_connection_after_interval(monkeypatch):
    redis_pool = importlib.reload(importlib.import_module("engine.cache.redis_pool"))
    circuit = importlib.reload(importlib.import_module("engine.cache.circuit"))
    calls = []
    now = [200.0]

    class FakeClient:
        def __init__(self):
            self.ping_results = [RuntimeError("stale socket"), True]

        def ping(self):
            calls.append(("ping",))
            result = self.ping_results.pop(0)
            if isinstance(result, BaseException):
                raise result
            return result

    class FakeRedis:
        @staticmethod
        def from_url(url, **kwargs):
            calls.append(("from_url", url, kwargs))
            return FakeClient()

    monkeypatch.setenv("TS_REDIS_URL", "redis://127.0.0.1:6380/2")
    monkeypatch.setenv("TS_REDIS_POOL_HEALTHCHECK_INTERVAL_S", "1")
    monkeypatch.setenv("TS_REDIS_CIRCUIT_FAILURES", "1")
    monkeypatch.setenv("TS_REDIS_CIRCUIT_COOLDOWN_S", "0")
    monkeypatch.setattr(redis_pool.time, "monotonic", lambda: float(now[0]))
    monkeypatch.setattr(redis_pool, "redis", type("RedisModule", (), {"Redis": FakeRedis}))
    circuit.reset_global_circuit()
    redis_pool.reset_redis_pool()

    client = redis_pool.redis_pool()
    assert circuit.cache_circuit().state == circuit.CircuitBreaker.OPEN

    now[0] += 0.5
    assert redis_pool.redis_pool() is client
    assert [call[0] for call in calls].count("ping") == 1

    now[0] += 0.6
    assert redis_pool.redis_pool() is client
    assert [call[0] for call in calls].count("ping") == 2
    assert circuit.cache_circuit().state == circuit.CircuitBreaker.CLOSED


def test_pool_resolves_password_secret(monkeypatch):
    redis_pool = importlib.reload(importlib.import_module("engine.cache.redis_pool"))

    monkeypatch.setenv("TS_REDIS_URL", "redis://127.0.0.1:6380/2")
    monkeypatch.setenv("TS_REDIS_PASSWORD_SECRET", "redis_password")
    monkeypatch.setattr(redis_pool, "_secret_text_from_env", lambda *names: "secret")

    assert redis_pool.redis_url() == "redis://:secret@127.0.0.1:6380/2"


def test_pool_resolves_password_file(monkeypatch, tmp_path):
    redis_pool = importlib.reload(importlib.import_module("engine.cache.redis_pool"))
    secret_file = tmp_path / "redis_password"
    secret_file.write_text("file-secret", encoding="utf-8")
    secret_file.chmod(0o600)

    monkeypatch.setenv("TS_REDIS_URL", "redis://127.0.0.1:6380/2")
    monkeypatch.setenv("TS_REDIS_PASSWORD_FILE", str(secret_file))

    assert redis_pool.redis_url() == "redis://:file-secret@127.0.0.1:6380/2"


def test_default_redis_url_is_linux_socket(monkeypatch):
    platform = importlib.reload(importlib.import_module("engine.runtime.platform"))

    monkeypatch.setattr(platform.sys, "platform", "linux")
    assert platform.default_redis_url() == "unix:///var/run/redis/trading.sock"
