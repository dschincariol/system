from __future__ import annotations

import importlib
import sqlite3
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class _SingleFlightRedis:
    def __init__(self) -> None:
        self.values: dict[str, bytes] = {}
        self.set_calls: list[tuple[str, bytes, int | None]] = []
        self._lock = threading.Lock()

    def get(self, key: str):
        with self._lock:
            return self.values.get(str(key))

    def set(self, key: str, value: bytes, ex: int | None = None):
        with self._lock:
            self.values[str(key)] = value
            self.set_calls.append((str(key), value, ex))
        return True


class _Pipeline:
    def __init__(self, redis: "_BatchRedis") -> None:
        self.redis = redis
        self.staged: list[tuple[str, bytes, int | None]] = []

    def set(self, key: str, value: bytes, ex: int | None = None):
        self.staged.append((str(key), value, ex))
        return self

    def execute(self):
        with self.redis._lock:
            self.redis.pipeline_execute_count += 1
            self.redis.pipeline_sets.extend(self.staged)
            for key, value, _ex in self.staged:
                self.redis.values[key] = value
        return [True for _ in self.staged]


class _BatchRedis:
    def __init__(self) -> None:
        self.values: dict[str, bytes] = {"k1": b"cached"}
        self.mget_calls: list[list[str]] = []
        self.pipeline_execute_count = 0
        self.pipeline_sets: list[tuple[str, bytes, int | None]] = []
        self._lock = threading.Lock()

    def mget(self, keys):
        cache_keys = [str(key) for key in keys]
        with self._lock:
            self.mget_calls.append(cache_keys)
            return [self.values.get(key) for key in cache_keys]

    def pipeline(self, transaction: bool = False):
        assert transaction is False
        return _Pipeline(self)


def _reload_store():
    store = importlib.reload(importlib.import_module("engine.cache.store"))
    circuit = importlib.import_module("engine.cache.circuit")
    circuit.reset_global_circuit()
    return store


def test_read_singleflight_loads_once_and_rechecks_redis(monkeypatch):
    store = _reload_store()
    fake = _SingleFlightRedis()
    metrics: list[tuple[str, int, dict[str, object]]] = []
    monkeypatch.setattr(store, "redis_pool", lambda: fake)
    monkeypatch.setattr(
        store,
        "emit_counter",
        lambda metric, value=1, **kwargs: metrics.append((metric, int(value), dict(kwargs))),
    )

    loader_calls = 0
    count_lock = threading.Lock()
    start = threading.Barrier(8)

    def loader() -> bytes:
        nonlocal loader_calls
        with count_lock:
            loader_calls += 1
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            if [name for name, _value, _kwargs in metrics].count("cache_singleflight_waits_total") >= 7:
                break
            time.sleep(0.001)
        return b"db"

    def worker() -> bytes | None:
        start.wait()
        return store.read("k", loader, ttl_s=100)

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _idx: worker(), range(8)))

    assert results == [b"db"] * 8
    assert loader_calls == 1
    assert fake.values["k"] == b"db"
    assert len(fake.set_calls) == 1
    assert 90 <= int(fake.set_calls[0][2] or 0) <= 110
    assert store._LOAD_LOCKS == {}
    assert [name for name, _value, _kwargs in metrics].count("cache_singleflight_wins_total") == 1
    assert [name for name, _value, _kwargs in metrics].count("cache_singleflight_waits_total") == 7


def test_read_singleflight_loader_failure_is_shared_with_waiters(monkeypatch):
    store = _reload_store()
    fake = _SingleFlightRedis()
    metrics: list[tuple[str, int, dict[str, object]]] = []
    monkeypatch.setattr(store, "redis_pool", lambda: fake)
    monkeypatch.setattr(
        store,
        "emit_counter",
        lambda metric, value=1, **kwargs: metrics.append((metric, int(value), dict(kwargs))),
    )

    loader_calls = 0
    count_lock = threading.Lock()
    start = threading.Barrier(8)

    def loader() -> bytes:
        nonlocal loader_calls
        with count_lock:
            loader_calls += 1
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            if [name for name, _value, _kwargs in metrics].count("cache_singleflight_waits_total") >= 7:
                break
            time.sleep(0.001)
        raise RuntimeError("db down")

    def worker() -> str:
        start.wait()
        try:
            store.read("k", loader, ttl_s=100)
        except RuntimeError as exc:
            return str(exc)
        return "unexpected-success"

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _idx: worker(), range(8)))

    assert results == ["db down"] * 8
    assert loader_calls == 1
    assert fake.values == {}
    assert fake.set_calls == []
    assert store._LOAD_LOCKS == {}
    assert [name for name, _value, _kwargs in metrics].count("cache_singleflight_wins_total") == 1
    assert [name for name, _value, _kwargs in metrics].count("cache_singleflight_waits_total") == 7
    failure_tags = [
        kwargs["extra_tags"]
        for name, _value, kwargs in metrics
        if name == "cache_singleflight_failures_total"
    ]
    assert failure_tags == [{"path": "read", "reason": "loader_exception"}]


def test_read_singleflight_lock_timeout_is_bounded_and_cleans_up(monkeypatch):
    store = _reload_store()
    fake = _SingleFlightRedis()
    metrics: list[tuple[str, int, dict[str, object]]] = []
    monkeypatch.setattr(store, "redis_pool", lambda: fake)
    monkeypatch.setenv("TS_REDIS_SINGLEFLIGHT_LOCK_TIMEOUT_S", "0.01")
    monkeypatch.setattr(store, "_ttl_with_jitter", lambda ttl_s, _key: ttl_s)
    monkeypatch.setattr(
        store,
        "emit_counter",
        lambda metric, value=1, **kwargs: metrics.append((metric, int(value), dict(kwargs))),
    )

    entry = store._get_load_lock("k")
    assert entry.lock.acquire(blocking=False)
    try:
        assert store.read("k", lambda: b"fallback", ttl_s=7) == b"fallback"
    finally:
        entry.lock.release()
        store._release_load_lock("k", entry)

    assert fake.values["k"] == b"fallback"
    assert fake.set_calls == [("k", b"fallback", 7)]
    assert store._LOAD_LOCKS == {}
    failure_tags = [
        kwargs["extra_tags"]
        for name, _value, kwargs in metrics
        if name == "cache_singleflight_failures_total"
    ]
    assert {"path": "read", "reason": "lock_timeout"} in failure_tags


def test_read_many_uses_mget_batch_loader_and_pipeline_backfill(monkeypatch):
    store = _reload_store()
    fake = _BatchRedis()
    metrics: list[tuple[str, int, dict[str, object]]] = []
    monkeypatch.setattr(store, "redis_pool", lambda: fake)
    monkeypatch.setattr(
        store,
        "emit_counter",
        lambda metric, value=1, **kwargs: metrics.append((metric, int(value), dict(kwargs))),
    )

    loader_calls: list[list[str]] = []

    def batch_loader(keys: list[str]):
        loader_calls.append(list(keys))
        return {"k2": b"db2", "k3": None}

    result = store.read_many(["k1", "k2", "k3"], batch_loader, ttl_s=100)

    assert result == {"k1": b"cached", "k2": b"db2", "k3": None}
    assert fake.mget_calls == [["k1", "k2", "k3"], ["k2", "k3"]]
    assert loader_calls == [["k2", "k3"]]
    assert fake.pipeline_execute_count == 1
    assert len(fake.pipeline_sets) == 1
    assert fake.pipeline_sets[0][0:2] == ("k2", b"db2")
    assert 90 <= int(fake.pipeline_sets[0][2] or 0) <= 110
    assert fake.values["k2"] == b"db2"
    assert "k3" not in fake.values
    assert store._LOAD_LOCKS == {}
    assert ("cache_singleflight_wins_total", 2, {"component": "engine.cache.store", "extra_tags": {"path": "read_many"}}) in metrics


def test_read_many_duplicate_keys_keep_first_seen_order_and_load_once(monkeypatch):
    store = _reload_store()
    fake = _BatchRedis()
    monkeypatch.setattr(store, "redis_pool", lambda: fake)

    loader_calls: list[list[str]] = []

    def batch_loader(keys: list[str]):
        loader_calls.append(list(keys))
        return {"k2": b"db2", "k3": b"db3"}

    result = store.read_many(["k2", "k1", "k2", "k3", "k1"], batch_loader, ttl_s=100)

    assert list(result.keys()) == ["k2", "k1", "k3"]
    assert result == {"k2": b"db2", "k1": b"cached", "k3": b"db3"}
    assert fake.mget_calls == [["k2", "k1", "k3"], ["k2", "k3"]]
    assert loader_calls == [["k2", "k3"]]
    assert [key for key, _value, _ttl in fake.pipeline_sets] == ["k2", "k3"]
    assert store._LOAD_LOCKS == {}


def test_read_many_loader_error_releases_locks_and_skips_backfill(monkeypatch):
    store = _reload_store()
    fake = _BatchRedis()
    monkeypatch.setattr(store, "redis_pool", lambda: fake)

    def failing_loader(_keys: list[str]):
        raise RuntimeError("db failed")

    with pytest.raises(RuntimeError, match="db failed"):
        store.read_many(["k2"], failing_loader, ttl_s=100)

    assert fake.pipeline_execute_count == 0
    assert store._LOAD_LOCKS == {}

    result = store.read_many(["k2"], lambda keys: {key: b"ok" for key in keys}, ttl_s=100)

    assert result == {"k2": b"ok"}
    assert fake.pipeline_execute_count == 1
    assert store._LOAD_LOCKS == {}


def test_feature_snapshots_latest_many_uses_store_read_many(monkeypatch):
    feature_snapshots = importlib.reload(importlib.import_module("engine.cache.wrappers.feature_snapshots"))
    cache_keys = importlib.import_module("engine.cache.keys")

    observed: dict[str, object] = {}

    def fake_read_many(keys, batch_loader, ttl_s=300):
        observed["keys"] = list(keys)
        observed["ttl_s"] = ttl_s
        loaded = dict(batch_loader(list(keys)) or {})
        return {str(key): loaded.get(str(key)) for key in keys}

    def fake_load_latest_many_result(symbols, feature_group):
        observed["symbols"] = list(symbols)
        observed["feature_group"] = feature_group
        return {
            str(symbol): {
                "symbol": str(symbol),
                "ts_ms": 1,
                "feature_set_tag": feature_group,
                "snapshot_version": 1,
                "feature_ids": ["f"],
                "vector": [1.0],
                "features": {"f": 1.0},
                "source_timestamps": {},
                "availability": {},
                "created_ts_ms": 1,
            }
            for symbol in symbols
        }, True

    monkeypatch.setattr(feature_snapshots.store, "read_many", fake_read_many)
    monkeypatch.setattr(feature_snapshots, "_load_latest_many_result", fake_load_latest_many_result)

    result = feature_snapshots.latest_many(["aapl", "MSFT"], "fg")

    assert observed["keys"] == [
        cache_keys.feature_snapshot("AAPL", "fg"),
        cache_keys.feature_snapshot("MSFT", "fg"),
    ]
    assert observed["ttl_s"] == feature_snapshots.FEATURE_SNAPSHOT_TTL_S
    assert observed["symbols"] == ["AAPL", "MSFT"]
    assert observed["feature_group"] == "fg"
    assert result["AAPL"]["features"]["f"] == 1.0
    assert result["MSFT"]["feature_ids"] == ["f"]


def test_feature_snapshots_load_latest_many_uses_one_database_query(monkeypatch):
    feature_snapshots = importlib.reload(importlib.import_module("engine.cache.wrappers.feature_snapshots"))
    con = sqlite3.connect(":memory:")
    con.execute(
        """
        CREATE TABLE model_feature_snapshots (
          symbol TEXT NOT NULL,
          ts_ms INTEGER NOT NULL,
          feature_set_tag TEXT NOT NULL,
          snapshot_version INTEGER NOT NULL,
          feature_ids_json TEXT NOT NULL,
          vector_json TEXT NOT NULL,
          features_json TEXT NOT NULL,
          source_timestamps_json TEXT NOT NULL,
          availability_json TEXT NOT NULL,
          created_ts_ms INTEGER NOT NULL
        )
        """
    )
    con.executemany(
        """
        INSERT INTO model_feature_snapshots(
          symbol, ts_ms, feature_set_tag, snapshot_version, feature_ids_json,
          vector_json, features_json, source_timestamps_json, availability_json,
          created_ts_ms
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("AAPL", 1000, "fg", 1, '["f"]', "[1.0]", '{"f":1.0}', "{}", "{}", 1001),
            ("AAPL", 900, "fg", 1, '["old"]', "[0.5]", '{"old":0.5}', "{}", "{}", 901),
            ("MSFT", 1100, "fg", 1, '["m"]', "[2.0]", '{"m":2.0}', "{}", "{}", 1101),
        ],
    )

    class CountingCon:
        def __init__(self, inner):
            self.inner = inner
            self.snapshot_queries = 0

        def execute(self, sql, params=()):
            if "FROM model_feature_snapshots" in str(sql):
                self.snapshot_queries += 1
            return self.inner.execute(sql, params)

        def close(self):
            return None

    counting = CountingCon(con)
    monkeypatch.setattr(feature_snapshots.storage, "connect", lambda readonly=True: counting)

    out = feature_snapshots._load_latest_many(["AAPL", "MSFT", "AAPL"], "fg")

    assert counting.snapshot_queries == 1
    assert sorted(out.keys()) == ["AAPL", "MSFT"]
    assert out["AAPL"]["ts_ms"] == 1000
    assert out["MSFT"]["features"] == {"m": 2.0}
