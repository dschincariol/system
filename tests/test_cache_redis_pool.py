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
