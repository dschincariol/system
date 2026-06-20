from __future__ import annotations

import ast
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from engine.cache import codec, keys
from engine.cache.wrappers import (
    _common as wrapper_common,
    broker_order_state,
    execution_health,
    execution_mode,
    feature_snapshots,
    kill_switch,
    position_baseline,
    strategy_allocations,
)


def test_cache_keys_honor_test_namespace(monkeypatch):
    monkeypatch.setenv("TS_REDIS_KEY_PREFIX", "unit_cache_namespace")

    assert keys.kill_switch("snapshot").startswith("unit_cache_namespace:")
    assert keys.execution_mode().startswith("unit_cache_namespace:")


def test_all_wrapper_reads_decode_loader_payloads(monkeypatch):
    monkeypatch.delenv("KILL_SWITCH_CACHE_TTL_S", raising=False)

    def fake_read(key, loader=None, *, ttl_s=None):
        assert ttl_s is not None
        return loader()

    for module in (
        broker_order_state,
        execution_health,
        execution_mode,
        feature_snapshots,
        kill_switch,
        position_baseline,
        strategy_allocations,
    ):
        monkeypatch.setattr(module.store, "read", fake_read)

    monkeypatch.setattr(kill_switch, "_load_snapshot", lambda: {"state": [{"scope": "global"}]})
    monkeypatch.setattr(execution_mode, "_load_mode", lambda: {"mode": "paper", "armed": 0})
    monkeypatch.setattr(execution_health, "_load_latest", lambda: {"state": "ok", "ts_ms": 1})
    monkeypatch.setattr(
        broker_order_state,
        "_load_order",
        lambda **_kwargs: {"source_order_id": 1, "symbol": "AAPL", "state": "FILLED"},
    )
    monkeypatch.setattr(position_baseline, "_load_baseline", lambda _broker: {"broker": "sim", "positions": {"AAPL": 1.0}})
    monkeypatch.setattr(strategy_allocations, "_load_latest", lambda _wd: {"window_days": 0, "allocations": {"s": 1.0}})
    monkeypatch.setattr(feature_snapshots, "_load_latest", lambda _sym, _fg: {"symbol": "AAPL", "feature_set_tag": "fg"})

    assert kill_switch.read_kill_switch()["state"][0]["scope"] == "global"
    assert execution_mode.read_execution_mode()["mode"] == "paper"
    assert execution_health.read_execution_health()["state"] == "ok"
    assert broker_order_state.read_broker_order_state(source_order_id=1, symbol="AAPL")["state"] == "FILLED"
    assert position_baseline.read_positions("sim") == {"AAPL": 1.0}
    assert strategy_allocations.read_strategy_allocations()["allocations"] == {"s": 1.0}
    assert feature_snapshots.latest("AAPL", "fg")["symbol"] == "AAPL"


def test_kill_switch_cache_uses_bounded_ttl_and_reloads_stale_snapshot(monkeypatch):
    monkeypatch.delenv("KILL_SWITCH_CACHE_TTL_S", raising=False)
    now_ms_value = {"value": 1_000_000}
    cache = {}
    ttl_values = []
    invalidated = []
    primed = []

    def fake_prime(key, value, *, ttl_s=None):
        primed.append((key, ttl_s))
        cache[key] = value

    def fake_read(key, loader=None, *, ttl_s=None):
        ttl_values.append(ttl_s)
        return cache.get(key)

    def fake_invalidate(key):
        invalidated.append(key)
        cache.pop(key, None)

    monkeypatch.setattr(kill_switch.store, "prime", fake_prime)
    monkeypatch.setattr(kill_switch.store, "read", fake_read)
    monkeypatch.setattr(kill_switch.store, "invalidate", fake_invalidate)
    monkeypatch.setattr(kill_switch, "now_ms", lambda: now_ms_value["value"])
    monkeypatch.setattr(kill_switch, "KILL_SWITCH_TTL_S", 30)
    monkeypatch.setattr(
        kill_switch,
        "_load_snapshot",
        lambda: {"state": [{"scope": "global", "key": "global", "enabled": 1}], "source": "unit_db"},
    )

    cache[keys.kill_switch("snapshot")] = codec.encode(
        {
            "state": [{"scope": "global", "key": "global", "enabled": 0}],
            "loaded_ts_ms": now_ms_value["value"] - 31_000,
            "source": "unit_old_cache",
            "max_age_ms": 30_000,
        },
        version=kill_switch.KILL_SWITCH_CODEC_VERSION,
    )

    state = kill_switch.read_kill_switch()

    assert state["state"][0]["enabled"] == 1
    assert state["loaded_ts_ms"] == now_ms_value["value"]
    assert state["max_age_ms"] == 30_000
    assert state["cache_status"] == "stale_reloaded"
    assert state["cache_fresh"] is True
    assert invalidated == [keys.kill_switch("snapshot")]
    assert primed == [(keys.kill_switch("snapshot"), 30)]
    assert ttl_values == [30]


def test_kill_switch_stale_cache_fails_closed_when_reload_unavailable(monkeypatch):
    monkeypatch.delenv("KILL_SWITCH_CACHE_TTL_S", raising=False)
    now_ms_value = {"value": 2_000_000}
    key = keys.kill_switch("snapshot")
    cache = {
        key: codec.encode(
            {
                "state": [{"scope": "global", "key": "global", "enabled": 0}],
                "loaded_ts_ms": now_ms_value["value"] - 60_000,
                "source": "unit_old_cache",
                "max_age_ms": 30_000,
            },
            version=kill_switch.KILL_SWITCH_CODEC_VERSION,
        )
    }

    monkeypatch.setattr(kill_switch.store, "read", lambda cache_key, loader=None, *, ttl_s=None: cache.get(cache_key))
    monkeypatch.setattr(kill_switch.store, "invalidate", lambda cache_key: cache.pop(cache_key, None))
    monkeypatch.setattr(kill_switch.store, "prime", lambda cache_key, value, *, ttl_s=None: cache.update({cache_key: value}))
    monkeypatch.setattr(kill_switch, "now_ms", lambda: now_ms_value["value"])
    monkeypatch.setattr(kill_switch, "_load_snapshot", lambda: kill_switch.fail_closed_snapshot("db down"))

    state = kill_switch.read_kill_switch()

    assert state["cache_status"] == "stale_fail_closed"
    row = state["state"][0]
    assert row["scope"] == "global"
    assert row["key"] == "provider_unavailable"
    assert row["enabled"] == 1
    assert row["reason"] == "kill_switch_provider_unavailable"


def test_kill_switch_read_fails_closed_when_provider_unavailable(monkeypatch):
    monkeypatch.delenv("KILL_SWITCH_CACHE_TTL_S", raising=False)

    def fake_read(key, loader=None, *, ttl_s=None):
        return loader()

    def fail_connect(*_args, **_kwargs):
        raise RuntimeError("kill switch db unavailable")

    monkeypatch.setattr(kill_switch.store, "read", fake_read)
    monkeypatch.setattr(kill_switch.storage, "connect", fail_connect)

    state = kill_switch.read_kill_switch()

    assert state["source"] == "engine.cache.wrappers.kill_switch:provider_unavailable"
    assert state["loaded_ts_ms"] > 0
    assert state["max_age_ms"] > 0
    row = state["state"][0]
    assert row["scope"] == "global"
    assert row["key"] == "provider_unavailable"
    assert row["enabled"] == 1
    assert row["reason"] == "kill_switch_provider_unavailable"


def test_kill_switch_provider_unavailable_source_wins_over_generic_error(monkeypatch):
    monkeypatch.delenv("KILL_SWITCH_CACHE_TTL_S", raising=False)
    now_ms_value = {"value": 3_000_000}
    payload = {
        "state": [
            {
                "scope": "global",
                "key": "provider_unavailable",
                "enabled": 1,
                "reason": "kill_switch_provider_unavailable",
                "actor": "unit",
                "meta": {"error": "db down"},
                "created_ts_ms": 0,
                "updated_ts_ms": now_ms_value["value"],
            }
        ],
        "loaded_ts_ms": now_ms_value["value"],
        "source": "engine.cache.wrappers.kill_switch:error",
        "max_age_ms": 30_000,
    }

    monkeypatch.setattr(kill_switch, "now_ms", lambda: now_ms_value["value"])
    monkeypatch.setattr(
        kill_switch.store,
        "read",
        lambda _key, _loader=None, *, ttl_s=None: codec.encode(
            payload,
            version=kill_switch.KILL_SWITCH_CODEC_VERSION,
        ),
    )

    state = kill_switch.read_kill_switch()

    assert state["source"] == "engine.cache.wrappers.kill_switch:provider_unavailable"
    assert state["cache_status"] == "fresh"
    assert state["state"][0]["key"] == "provider_unavailable"


def test_wrapper_codec_version_mismatch_invalidates_repopulates_and_emits_metric(monkeypatch):
    monkeypatch.delenv("KILL_SWITCH_CACHE_TTL_S", raising=False)
    key = keys.kill_switch("snapshot")
    cache = {
        key: codec.encode({"state": [{"scope": "global", "key": "global", "enabled": 0}]}, version=1)
    }
    deleted = []
    primed = []
    metrics = []
    loader_calls = {"count": 0}

    def fake_read(cache_key, loader=None, *, ttl_s=None):
        return cache.get(cache_key)

    def fake_invalidate(cache_key):
        deleted.append(cache_key)
        cache.pop(cache_key, None)

    def fake_prime(cache_key, value, *, ttl_s=None):
        primed.append((cache_key, codec.envelope_version(value), ttl_s))
        cache[cache_key] = value

    def fake_load_snapshot():
        loader_calls["count"] += 1
        return {"state": [{"scope": "global", "key": "global", "enabled": 1}]}

    def fake_emit_counter(metric, value=1, **kwargs):
        metrics.append((metric, value, kwargs))

    monkeypatch.setattr(kill_switch.store, "read", fake_read)
    monkeypatch.setattr(kill_switch.store, "invalidate", fake_invalidate)
    monkeypatch.setattr(kill_switch.store, "prime", fake_prime)
    monkeypatch.setattr(kill_switch, "_load_snapshot", fake_load_snapshot)
    monkeypatch.setattr(kill_switch, "KILL_SWITCH_CODEC_VERSION", 2)
    monkeypatch.setattr(wrapper_common, "emit_counter", fake_emit_counter)

    state = kill_switch.read_kill_switch()

    assert state["state"][0]["enabled"] == 1
    assert deleted == [key]
    assert primed == [(key, 2, 30)]
    assert loader_calls["count"] == 1
    assert metrics[0][0] == "codec_version_mismatch_count"
    assert metrics[0][2]["extra_tags"]["expected_version"] == 2


def test_keyspace_covers_all_hot_path_tables():
    assert keys.kill_switch("global", "global") == "trading:kill_switch_state:global:global"
    assert keys.execution_mode() == "trading:execution_mode:singleton"
    assert keys.execution_health() == "trading:execution_health_state:latest"
    assert keys.broker_order_state("source:1:AAPL") == "trading:broker_order_state:source:1:AAPL"
    assert keys.position_baseline("sim") == "trading:position_reconcile_baseline:sim"
    assert keys.strategy_allocations(0) == "trading:strategy_allocations:0"
    assert keys.feature_snapshot("aapl", "fg") == "trading:model_feature_snapshots:AAPL:fg"


def test_wrapper_write_payloads_are_encoded(monkeypatch):
    writes = []

    def fake_write_through(key, value, *, persist, ttl_s=None):
        writes.append((key, codec.decode(value), ttl_s))

    for module in (
        execution_mode,
        position_baseline,
        strategy_allocations,
        feature_snapshots,
    ):
        monkeypatch.setattr(module.store, "write_through", fake_write_through)
    monkeypatch.setattr(execution_mode, "read_execution_mode", lambda: {"mode": "paper", "armed": 0})

    execution_mode.set_execution_mode("paper")
    position_baseline.set_position_baseline("sim", {"AAPL": 2})
    strategy_allocations.set_strategy_allocations({"mean_reversion": 1.0})
    feature_snapshots.store_latest(
        {
            "symbol": "AAPL",
            "ts_ms": 1,
            "feature_set_tag": "fg",
            "feature_ids": [],
            "vector": [],
            "features": {},
            "source_timestamps": {},
            "availability": {},
            "created_ts_ms": 1,
        }
    )

    assert [item[0] for item in writes] == [
        keys.execution_mode(),
        keys.position_baseline("sim"),
        keys.strategy_allocations(0),
        keys.feature_snapshot("AAPL", "fg"),
    ]


def test_broker_order_state_write_updates_all_cache_keys(monkeypatch):
    writes = []

    def fake_write_through_many(entries, *, persist, ttl_s=None):
        class Cursor:
            lastrowid = 77

        class Tx:
            def execute(self, *_args, **_kwargs):
                return Cursor()

        persist(Tx())
        resolved = entries()
        writes.append((resolved, ttl_s))

    monkeypatch.setattr(broker_order_state.store, "write_through_many", fake_write_through_many)

    row = broker_order_state.set_broker_order_state(
        source_order_id=123,
        symbol="AAPL",
        state="FILLED",
    )

    cache_entries = writes[0][0]
    assert row["id"] == 77
    assert set(cache_entries) == {
        keys.broker_order_state("id:77"),
        keys.broker_order_state("source:123:AAPL"),
        keys.broker_order_state("latest:AAPL"),
    }


def test_legacy_execution_surfaces_read_through_wrappers(monkeypatch):
    legacy_mode = __import__("engine.execution.execution_mode", fromlist=["get_execution_mode"])
    legacy_kill = __import__("engine.execution.kill_switch", fromlist=["snapshot"])

    monkeypatch.setattr(execution_mode, "read_execution_mode", lambda: {"mode": "shadow", "armed": 0})
    monkeypatch.setattr(kill_switch, "read_kill_switch", lambda: {"state": [{"scope": "global", "key": "global"}]})

    assert legacy_mode.get_execution_mode()["mode"] == "shadow"
    assert legacy_kill.snapshot()["state"][0]["scope"] == "global"


def test_no_direct_redis_imports_or_get_set_calls_outside_cache_store():
    violations = []
    for path in (ROOT / "engine").rglob("*.py"):
        rel = path.relative_to(ROOT).as_posix()
        text = path.read_text(encoding="utf-8")
        tree = ast.parse(text)
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                module = ""
                if isinstance(node, ast.ImportFrom):
                    module = str(node.module or "")
                if (module == "redis" or module.startswith("redis.")) and not rel.startswith("engine/cache/"):
                    violations.append(f"{rel}:redis import")
            if not rel.startswith("engine/cache/") and isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                if node.func.attr in {"get", "set"}:
                    target = ast.unparse(node.func.value)
                    if "redis_pool" in target:
                        violations.append(f"{rel}:redis {node.func.attr}")
    assert violations == []


@pytest.mark.skipif(
    os.environ.get("TS_CACHE_REAL_INTEGRATION") != "1",
    reason="set TS_CACHE_REAL_INTEGRATION=1 with Redis and Postgres/Timescale configured",
)
@pytest.mark.requires_postgres
@pytest.mark.requires_redis
def test_real_redis_postgres_wrappers_integration():
    pytest.importorskip("redis")

    from engine.cache.redis_pool import redis_pool
    from engine.runtime import storage

    redis_pool().ping()
    storage.init_db()

    suffix = "cacheit"
    kill_switch.set_kill_switch(False, reason=suffix, actor="pytest", scope="global", key="global")
    assert isinstance(kill_switch.read_kill_switch().get("state"), list)

    execution_mode.set_execution_mode("paper", actor="pytest", reason=suffix)
    assert execution_mode.read_execution_mode()["mode"] == "paper"

    execution_health.write_execution_health({"state": "ok", "score": 1.0, "ts_ms": 1})
    assert execution_health.read_execution_health()["state"] == "ok"

    broker_order_state.set_broker_order_state(source_order_id=123456789, symbol="ZZTEST", state="PENDING")
    assert broker_order_state.read_broker_order_state(source_order_id=123456789, symbol="ZZTEST")["state"] == "PENDING"

    position_baseline.set_position_baseline("pytest", {"ZZTEST": 1.0})
    assert position_baseline.read_positions("pytest") == {"ZZTEST": 1.0}

    strategy_allocations.set_strategy_allocations({"pytest": 1.0}, reason={"reason": suffix}, ts_ms=1)
    assert strategy_allocations.read_strategy_allocations()["allocations"]["pytest"] == 1.0

    feature_snapshots.store_latest(
        {
            "symbol": "ZZTEST",
            "ts_ms": 1,
            "feature_set_tag": "pytest",
            "feature_ids": ["f"],
            "vector": [1.0],
            "features": {"f": 1.0},
            "source_timestamps": {},
            "availability": {},
            "created_ts_ms": 1,
        }
    )
    assert feature_snapshots.latest("ZZTEST", "pytest")["features"]["f"] == 1.0
