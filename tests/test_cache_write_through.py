from __future__ import annotations

import importlib
import sys
import threading
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from engine.cache import codec


class FakeRedis:
    def __init__(self, *, fail_set: bool = False):
        self.fail_set = fail_set
        self.values = {}
        self.deleted = []
        self.get_count = 0
        self.set_calls = []

    def get(self, key):
        self.get_count += 1
        return self.values.get(key)

    def set(self, key, value, ex=None):
        if self.fail_set:
            raise RuntimeError("set failed")
        self.set_calls.append((key, value, ex))
        self.values[key] = (value, ex)
        return True

    def delete(self, key):
        self.deleted.append(key)
        self.values.pop(key, None)
        return 1


class FakePipeline:
    def __init__(self, redis):
        self.redis = redis
        self.commands = []

    def set(self, key, value, ex=None):
        self.commands.append((key, value, ex))
        return self

    def execute(self):
        for key, value, ex in self.commands:
            self.redis.values[key] = value
            self.redis.expiries[key] = ex
        self.redis.pipeline_commands.append(list(self.commands))
        return [True for _ in self.commands]


class PipelinedRedis:
    def __init__(self):
        self.values = {}
        self.expiries = {}
        self.deleted = []
        self.pipeline_commands = []
        self.get_count = 0

    def get(self, key):
        self.get_count += 1
        return self.values.get(key)

    def set(self, key, value, ex=None):
        self.values[key] = value
        self.expiries[key] = ex
        return True

    def pipeline(self, transaction=False):
        assert transaction is False
        return FakePipeline(self)

    def delete(self, key):
        self.deleted.append(key)
        self.values.pop(key, None)
        self.expiries.pop(key, None)
        return 1


class TxContext:
    def __init__(self, events):
        self.events = events
        self.con = object()

    def __enter__(self):
        self.events.append("begin")
        return self.con

    def __exit__(self, exc_type, exc, tb):
        self.events.append("rollback" if exc_type else "commit")
        return False


def test_write_through_persists_before_cache_set(monkeypatch):
    store = importlib.reload(importlib.import_module("engine.cache.store"))
    circuit = importlib.import_module("engine.cache.circuit")
    circuit.reset_global_circuit()
    events = []
    fake = FakeRedis()
    monkeypatch.setattr(store, "redis_pool", lambda: fake)
    monkeypatch.setattr(store.storage, "transaction", lambda: TxContext(events))

    def persist(_con):
        events.append("persist")

    store.write_through("k", b"v", persist=persist, ttl_s=9)

    assert events == ["begin", "persist", "commit"]
    assert fake.values["k"] == (b"v", 9)


def test_write_through_records_write_path_without_redis_readback(monkeypatch):
    store = importlib.reload(importlib.import_module("engine.cache.store"))
    circuit = importlib.import_module("engine.cache.circuit")
    circuit.reset_global_circuit()
    events = []
    metrics = []
    fake = FakeRedis()
    fake.values["k"] = codec.encode({"old": 1}, version=1)
    monkeypatch.setattr(store, "redis_pool", lambda: fake)
    monkeypatch.setattr(store.storage, "transaction", lambda: TxContext(events))
    monkeypatch.setattr(store, "emit_counter", lambda metric, value=1, **kwargs: metrics.append((metric, value, kwargs)))

    def persist(_con):
        events.append("persist")

    store.write_through("k", codec.encode({"new": 1}, version=2), persist=persist, ttl_s=9)

    assert events == ["begin", "persist", "commit"]
    assert fake.values["k"][1] == 9
    assert fake.get_count == 0
    assert metrics == [
        (
            "cache_write_through_path_total",
            1,
            {
                "component": "engine.cache.store",
                "extra_tags": {
                    "mode": "single_set",
                    "result": "success",
                    "key_count": "1",
                    "payload_version": "2",
                },
            },
        )
    ]


def test_write_through_rollback_does_not_touch_cache(monkeypatch):
    store = importlib.reload(importlib.import_module("engine.cache.store"))
    circuit = importlib.import_module("engine.cache.circuit")
    circuit.reset_global_circuit()
    events = []
    fake = FakeRedis()
    monkeypatch.setattr(store, "redis_pool", lambda: fake)
    monkeypatch.setattr(store.storage, "transaction", lambda: TxContext(events))

    def persist(_con):
        events.append("persist")
        raise RuntimeError("db failed")

    with pytest.raises(RuntimeError):
        store.write_through("k", b"v", persist=persist)

    assert events == ["begin", "persist", "rollback"]
    assert fake.values == {}
    assert fake.deleted == []


def test_cache_set_failure_invalidates_key(monkeypatch):
    store = importlib.reload(importlib.import_module("engine.cache.store"))
    circuit = importlib.import_module("engine.cache.circuit")
    circuit.reset_global_circuit()
    events = []
    fake = FakeRedis(fail_set=True)
    monkeypatch.setattr(store, "redis_pool", lambda: fake)
    monkeypatch.setattr(store.storage, "transaction", lambda: TxContext(events))

    store.write_through("k", b"v", persist=lambda _con: events.append("persist"))

    assert events == ["begin", "persist", "commit"]
    assert fake.deleted == ["k"]
    assert fake.get_count == 0


def test_write_through_can_compute_cache_value_after_commit(monkeypatch):
    store = importlib.reload(importlib.import_module("engine.cache.store"))
    circuit = importlib.import_module("engine.cache.circuit")
    circuit.reset_global_circuit()
    events = []
    fake = FakeRedis()
    monkeypatch.setattr(store, "redis_pool", lambda: fake)
    monkeypatch.setattr(store.storage, "transaction", lambda: TxContext(events))

    def persist(_con):
        events.append("persist")

    def value():
        events.append("value")
        return b"after"

    store.write_through("k", value, persist=persist)

    assert events == ["begin", "persist", "commit", "value"]
    assert fake.values["k"][0] == b"after"


def test_write_through_many_updates_all_keys_after_one_commit(monkeypatch):
    store = importlib.reload(importlib.import_module("engine.cache.store"))
    circuit = importlib.import_module("engine.cache.circuit")
    circuit.reset_global_circuit()
    events = []
    fake = FakeRedis()
    monkeypatch.setattr(store, "redis_pool", lambda: fake)
    monkeypatch.setattr(store.storage, "transaction", lambda: TxContext(events))

    payload = {"id": None}

    def persist(_con):
        events.append("persist")
        payload["id"] = 42

    store.write_through_many(
        lambda: {
            f"k:{payload['id']}": b"id",
            "k:latest": b"latest",
        },
        persist=persist,
        ttl_s=11,
    )

    assert events == ["begin", "persist", "commit"]
    assert fake.values == {
        "k:42": (b"id", 11),
        "k:latest": (b"latest", 11),
    }
    assert fake.get_count == 0


def test_write_through_many_pipelines_sets_without_readback(monkeypatch):
    store = importlib.reload(importlib.import_module("engine.cache.store"))
    circuit = importlib.import_module("engine.cache.circuit")
    circuit.reset_global_circuit()
    events = []
    metrics = []
    fake = PipelinedRedis()
    monkeypatch.setattr(store, "redis_pool", lambda: fake)
    monkeypatch.setattr(store.storage, "transaction", lambda: TxContext(events))
    monkeypatch.setattr(store, "_ttl_with_jitter", lambda ttl_s, _key: ttl_s)
    monkeypatch.setattr(store, "emit_counter", lambda metric, value=1, **kwargs: metrics.append((metric, value, kwargs)))

    payload_a = codec.encode({"a": 1}, version=3)
    payload_b = codec.encode({"b": 1}, version=3)

    def persist(_con):
        events.append("persist")

    store.write_through_many({"a": payload_a, "b": payload_b}, persist=persist, ttl_s=11)

    assert events == ["begin", "persist", "commit"]
    assert fake.get_count == 0
    assert fake.pipeline_commands == [[("a", payload_a, 11), ("b", payload_b, 11)]]
    assert fake.values == {"a": payload_a, "b": payload_b}
    assert fake.expiries == {"a": 11, "b": 11}
    assert metrics == [
        (
            "cache_write_through_path_total",
            1,
            {
                "component": "engine.cache.store",
                "extra_tags": {
                    "mode": "pipeline_set_many",
                    "result": "success",
                    "key_count": "2",
                    "payload_version": "3",
                },
            },
        )
    ]


def test_read_singleflight_rechecks_redis_after_lock(monkeypatch):
    store = importlib.reload(importlib.import_module("engine.cache.store"))
    circuit = importlib.import_module("engine.cache.circuit")
    circuit.reset_global_circuit()

    class ReadRedis:
        def __init__(self):
            self.values = {}
            self.expiries = {}
            self.get_count = 0
            self.condition = threading.Condition()

        def get(self, key):
            with self.condition:
                self.get_count += 1
                self.condition.notify_all()
            return self.values.get(key)

        def set(self, key, value, ex=None):
            self.values[key] = value
            self.expiries[key] = ex
            return True

        def wait_for_gets(self, count):
            with self.condition:
                return self.condition.wait_for(lambda: self.get_count >= count, timeout=2.0)

    fake = ReadRedis()
    monkeypatch.setattr(store, "redis_pool", lambda: fake)
    monkeypatch.setattr(store, "_ttl_with_jitter", lambda ttl_s, _key: ttl_s)

    loader_entered = threading.Event()
    release_loader = threading.Event()
    loader_calls = 0
    loader_guard = threading.Lock()
    results = []

    def loader():
        nonlocal loader_calls
        with loader_guard:
            loader_calls += 1
        loader_entered.set()
        assert release_loader.wait(2.0)
        return b"loaded"

    def worker():
        results.append(store.read("k", loader, ttl_s=9))

    t1 = threading.Thread(target=worker)
    t2 = threading.Thread(target=worker)
    t1.start()
    assert loader_entered.wait(2.0)
    t2.start()
    assert fake.wait_for_gets(3)
    release_loader.set()
    t1.join(2.0)
    t2.join(2.0)

    assert results == [b"loaded", b"loaded"]
    assert loader_calls == 1
    assert fake.values["k"] == b"loaded"
    assert fake.expiries["k"] == 9
    assert fake.get_count >= 4


def test_read_many_mget_loads_misses_once_and_pipelines_backfill(monkeypatch):
    store = importlib.reload(importlib.import_module("engine.cache.store"))
    circuit = importlib.import_module("engine.cache.circuit")
    circuit.reset_global_circuit()

    class BatchRedis:
        def __init__(self):
            self.values = {"hit": b"H"}
            self.expiries = {}
            self.mget_calls = []
            self.pipeline_commands = []

        def mget(self, keys):
            self.mget_calls.append(list(keys))
            return [self.values.get(key) for key in keys]

        def pipeline(self, transaction=False):
            assert transaction is False
            return FakePipeline(self)

    fake = BatchRedis()
    loader_calls = []
    monkeypatch.setattr(store, "redis_pool", lambda: fake)
    monkeypatch.setattr(store, "_ttl_with_jitter", lambda ttl_s, _key: ttl_s)

    def batch_loader(missing_keys):
        loader_calls.append(list(missing_keys))
        return {"miss1": b"M1", "miss2": b"M2"}

    out = store.read_many(["hit", "miss1", "miss2"], batch_loader, ttl_s=10)

    assert out == {"hit": b"H", "miss1": b"M1", "miss2": b"M2"}
    assert fake.mget_calls == [["hit", "miss1", "miss2"], ["miss1", "miss2"]]
    assert loader_calls == [["miss1", "miss2"]]
    assert fake.pipeline_commands == [[("miss1", b"M1", 10), ("miss2", b"M2", 10)]]
    assert fake.values["miss1"] == b"M1"
    assert fake.values["miss2"] == b"M2"


def test_ttl_jitter_spreads_expiries_within_bounds():
    store = importlib.reload(importlib.import_module("engine.cache.store"))

    values = {store._ttl_with_jitter(300, f"k:{idx}") for idx in range(64)}

    assert min(values) >= 270
    assert max(values) <= 330
    assert len(values) > 1
