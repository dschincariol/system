from __future__ import annotations

import importlib
import sys
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

    def get(self, key):
        return self.values.get(key)

    def set(self, key, value, ex=None):
        if self.fail_set:
            raise RuntimeError("set failed")
        self.values[key] = (value, ex)
        return True

    def delete(self, key):
        self.deleted.append(key)
        self.values.pop(key, None)
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


def test_write_through_records_version_sequence(monkeypatch):
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
    assert metrics[0][0] == "cache_write_through_lag_observed"
    assert metrics[0][2]["extra_tags"] == {
        "key": "k",
        "postgres_version": "2",
        "redis_pre_write_version": "1",
        "redis_post_write_version": "2",
    }


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
