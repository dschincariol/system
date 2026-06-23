from __future__ import annotations

import base64
import importlib
import json
import os
import sys
import tempfile
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

VALID_DATA_SOURCE_MASTER_KEY = base64.b64encode(bytes(range(32))).decode("ascii")


def _reload(module_name: str):
    return importlib.reload(importlib.import_module(module_name))


def test_ingestion_shard_env_validation_and_stable_partitioning() -> None:
    shards = _reload("engine.runtime.ingestion_shards")

    default = shards.parse_ingestion_shard_env({})
    assert (default.index, default.count, default.enabled) == (0, 1, False)
    assert shards.ingestion_shard_job_name("ingestion_runtime", default) == "ingestion_runtime"

    configured = shards.parse_ingestion_shard_env(
        {"INGESTION_SHARD_INDEX": "1", "INGESTION_SHARD_COUNT": "3"}
    )
    assert configured.as_dict() == {
        "index": 1,
        "count": 3,
        "enabled": True,
        "label": "shard:1-of-3",
    }
    assert shards.ingestion_shard_job_name("ingestion_runtime", configured) == "ingestion_runtime:shard:1-of-3"

    symbols = ["SPY", "QQQ", "IWM", "AAPL", "MSFT", "BTC-USD"]
    parts = [shards.filter_symbols_for_shard(symbols, shards.IngestionShard(i, 3)) for i in range(3)]
    flattened = [symbol for part in parts for symbol in part]
    assert sorted(flattened) == sorted(symbols)
    assert len(flattened) == len(set(flattened))
    assert parts == [shards.filter_symbols_for_shard(symbols, shards.IngestionShard(i, 3)) for i in range(3)]

    with pytest.raises(ValueError, match="INGESTION_SHARD_COUNT_out_of_range"):
        shards.parse_ingestion_shard_env({"INGESTION_SHARD_COUNT": "0"})
    with pytest.raises(ValueError, match="INGESTION_SHARD_INDEX_out_of_range"):
        shards.parse_ingestion_shard_env({"INGESTION_SHARD_INDEX": "2", "INGESTION_SHARD_COUNT": "2"})
    with pytest.raises(ValueError, match="INGESTION_SHARD_INDEX_invalid_integer"):
        shards.parse_ingestion_shard_env({"INGESTION_SHARD_INDEX": "one", "INGESTION_SHARD_COUNT": "2"})


def test_ingestion_runtime_uses_sharded_lock_names_and_filters_singleton_children(monkeypatch) -> None:
    monkeypatch.setenv("INGESTION_SHARD_INDEX", "1")
    monkeypatch.setenv("INGESTION_SHARD_COUNT", "3")
    monkeypatch.setenv("ENGINE_MODE", "live")
    monkeypatch.setenv("EXECUTION_MODE", "paper")
    monkeypatch.setenv("INGESTION_CHILD_JOBS", "")

    ingestion_runtime = _reload("engine.runtime.ingestion_runtime")
    monkeypatch.setattr(
        ingestion_runtime,
        "desired_ingestion_jobs",
        lambda **_kwargs: ["stream_prices_polygon_ws", "poll_prices", "options_poll"],
    )
    monkeypatch.setattr(ingestion_runtime, "_resolve_child_script", lambda _name: REPO_ROOT / "tests" / "test_ingestion_sharding.py")

    assert ingestion_runtime.INGESTION_RUNTIME_JOB_LOCK_NAME == "ingestion_runtime:shard:1-of-3"
    assert ingestion_runtime.INGESTION_STATE_KEY == "ingestion_state::shard:1-of-3"
    assert ingestion_runtime._child_liveness_job_name("poll_prices") == "poll_prices:shard:1-of-3"
    assert ingestion_runtime._child_liveness_job_name("options_poll") == "options_poll:shard:1-of-3"
    assert ingestion_runtime._child_liveness_job_name("stream_prices_polygon_ws") == "stream_prices_polygon_ws"
    assert ingestion_runtime._child_candidates() == ["poll_prices", "options_poll"]


def test_runtime_supervisor_propagates_canonical_shard_env(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("INGESTION_SHARD_INDEX", "1")
    monkeypatch.setenv("INGESTION_SHARD_COUNT", "2")
    monkeypatch.setenv("TRADING_LOGS", str(tmp_path))

    supervisor = _reload("engine.runtime.supervisor")
    sup = supervisor.RuntimeSupervisor(jobs=None)
    sup.register_job("unit_sharded_child", "tests/test_ingestion_sharding.py", daemon=True)

    captured: dict[str, str] = {}

    class _FakeProc:
        pid = 45678

        def __init__(self, *_args, **kwargs) -> None:
            captured.update(dict(kwargs.get("env") or {}))
            self._rc = None

        def poll(self):
            return self._rc

        def terminate(self) -> None:
            self._rc = 0

        def wait(self, timeout=None):
            return self._rc

        def kill(self) -> None:
            self._rc = -9

    try:
        monkeypatch.setattr(supervisor.subprocess, "Popen", _FakeProc)
        result = sup.start("unit_sharded_child")
        assert result["ok"] is True
        assert captured["INGESTION_SHARD_INDEX"] == "1"
        assert captured["INGESTION_SHARD_COUNT"] == "2"
    finally:
        sup.stop_all()


def test_price_and_options_symbol_universes_are_shard_filtered(monkeypatch) -> None:
    monkeypatch.setenv("INGESTION_SHARD_INDEX", "0")
    monkeypatch.setenv("INGESTION_SHARD_COUNT", "2")
    monkeypatch.setenv("FORCE_FACTOR_PROXY_TICKERS", "0")

    poll_prices = _reload("engine.data.poll_prices")
    options_poll = _reload("engine.data.options_poll")
    shards = _reload("engine.runtime.ingestion_shards")

    rows = [
        ("SPY", json.dumps({"yf_ticker": "SPY", "polygon_ticker": "SPY"})),
        ("QQQ", json.dumps({"yf_ticker": "QQQ", "polygon_ticker": "QQQ"})),
        ("BTC", json.dumps({"price_provider": "ccxt", "ccxt_market": "BTC/USDT"})),
        ("AAPL", json.dumps({"yf_ticker": "AAPL", "polygon_ticker": "AAPL"})),
    ]

    class _Cursor:
        def __init__(self, result):
            self._result = result

        def fetchall(self):
            return list(self._result)

    class _Connection:
        def execute(self, *_args, **_kwargs):
            return _Cursor(rows)

        def close(self):
            return None

    monkeypatch.setattr(poll_prices, "connect", lambda *args, **kwargs: _Connection())
    monkeypatch.setattr(poll_prices, "load_default_symbols", lambda: [])

    yf_map, ccxt_map, polygon_map = poll_prices._load_symbol_providers()
    selected_symbols = set(yf_map) | set(ccxt_map) | set(polygon_map)
    assert selected_symbols
    assert all(shards.symbol_belongs_to_shard(symbol, poll_prices.INGESTION_SHARD) for symbol in selected_symbols)

    monkeypatch.setattr(options_poll, "get_active_symbols", lambda _con, limit=None: ["SPY", "QQQ", "BTC", "AAPL"])
    option_symbols = options_poll._load_active_symbols_for_shard(_Connection())
    assert option_symbols
    assert all(shards.symbol_belongs_to_shard(symbol, options_poll.INGESTION_SHARD) for symbol in option_symbols)


def test_job_locks_are_independent_and_stale_takeover_is_per_shard(monkeypatch) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        monkeypatch.setenv("DB_PATH", str(Path(tmp) / "shards.db"))
        monkeypatch.setenv("TS_STORAGE_BACKEND", "sqlite")
        monkeypatch.setenv("SQLITE_LIVENESS_QUEUE_ENABLED", "0")
        monkeypatch.setenv("DATA_SOURCE_MASTER_KEY", VALID_DATA_SOURCE_MASTER_KEY)

        storage = _reload("engine.runtime.storage")
        shards = _reload("engine.runtime.ingestion_shards")
        storage.init_db()

        pid = os.getpid()
        shard0 = shards.ingestion_shard_job_name("ingestion_runtime", shards.IngestionShard(0, 2))
        shard1 = shards.ingestion_shard_job_name("ingestion_runtime", shards.IngestionShard(1, 2))

        assert storage.acquire_job_lock(shard0, "owner-0", pid, ttl_s=60)
        assert storage.acquire_job_lock(shard1, "owner-1", pid, ttl_s=60)
        assert not storage.acquire_job_lock(shard0, "owner-0b", pid, stale_after_s=60)

        con = storage.connect()
        try:
            con.execute(
                "UPDATE job_locks SET heartbeat_ts_ms=? WHERE job_name=?",
                (1, shard0),
            )
            con.commit()
        finally:
            con.close()

        assert storage.acquire_job_lock(shard0, "owner-0b", pid, stale_after_s=1)

        con = storage.connect(readonly=True)
        try:
            rows = {
                str(row[0]): str(row[1])
                for row in con.execute(
                    "SELECT job_name, owner FROM job_locks WHERE job_name IN (?, ?)",
                    (shard0, shard1),
                ).fetchall()
            }
        finally:
            con.close()

        assert rows[shard0] == "owner-0b"
        assert rows[shard1] == "owner-1"
        storage.close_pooled_connections()


def test_start_system_uses_current_shard_liveness_for_active_check_and_cleanup(monkeypatch) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        monkeypatch.setenv("DB_PATH", str(tmp_path / "startup-shards.db"))
        monkeypatch.setenv("TRADING_LOGS", str(tmp_path / "logs"))
        monkeypatch.setenv("TRADING_DATA", str(tmp_path / "data"))
        monkeypatch.setenv("TS_STORAGE_BACKEND", "sqlite")
        monkeypatch.setenv("SQLITE_LIVENESS_QUEUE_ENABLED", "0")
        monkeypatch.setenv("DATA_SOURCE_MASTER_KEY", VALID_DATA_SOURCE_MASTER_KEY)
        monkeypatch.setenv("INGESTION_SHARD_INDEX", "1")
        monkeypatch.setenv("INGESTION_SHARD_COUNT", "2")

        storage = _reload("engine.runtime.storage")
        storage.init_db()
        start_system = _reload("start_system")

        assert Path(start_system._INGESTION_PID_PATH).name == "ingestion.shard-1-of-2.pid"
        assert Path(start_system._INGESTION_STDOUT_PATH).name == "ingestion.shard-1-of-2.stdout.log"
        assert start_system._ingestion_runtime_liveness_job_name() == "ingestion_runtime:shard:1-of-2"

        storage.put_job_heartbeat("ingestion_runtime:shard:0-of-2", "owner-0", os.getpid(), extra_json="{}")
        assert start_system._existing_ingestion_runtime_active() is False

        storage.put_job_heartbeat("ingestion_runtime:shard:1-of-2", "owner-1", os.getpid(), extra_json="{}")
        assert start_system._existing_ingestion_runtime_active() is True

        con = storage.connect()
        try:
            con.execute("DELETE FROM job_heartbeats")
            con.execute("DELETE FROM job_locks")
            con.commit()
        finally:
            con.close()

        storage.acquire_job_lock("ingestion_runtime:shard:0-of-2", "owner-0", os.getpid(), ttl_s=60)
        storage.put_job_heartbeat("ingestion_runtime:shard:0-of-2", "owner-0", os.getpid(), extra_json="{}")
        storage.acquire_job_lock("ingestion_runtime:shard:1-of-2", "owner-1", 987654321, ttl_s=60)
        storage.put_job_heartbeat("ingestion_runtime:shard:1-of-2", "owner-1", 987654321, extra_json="{}")
        con = storage.connect()
        try:
            con.execute(
                "INSERT OR REPLACE INTO price_feed_lock(id, owner, pid, ts_ms) VALUES(1, ?, ?, ?)",
                ("singleton-owner", os.getpid(), 1),
            )
            con.commit()
        finally:
            con.close()

        monkeypatch.setattr(start_system, "_discover_repo_ingestion_process_pids", lambda known_jobs=None: set())
        start_system._terminate_stale_ingestion_processes(time_budget_s=1.0)

        con = storage.connect(readonly=True)
        try:
            remaining = {
                str(row[0])
                for row in con.execute(
                    "SELECT job_name FROM job_locks ORDER BY job_name"
                ).fetchall()
            }
            price_feed_lock = con.execute("SELECT owner FROM price_feed_lock WHERE id=1").fetchone()
        finally:
            con.close()
            storage.close_pooled_connections()

        assert "ingestion_runtime:shard:0-of-2" in remaining
        assert "ingestion_runtime:shard:1-of-2" not in remaining
        assert str(price_feed_lock[0]) == "singleton-owner"


def test_health_snapshot_merges_sharded_ingestion_runtime_state(monkeypatch) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        monkeypatch.setenv("DB_PATH", str(Path(tmp) / "health-shards.db"))
        monkeypatch.setenv("TS_STORAGE_BACKEND", "sqlite")
        monkeypatch.setenv("SQLITE_LIVENESS_QUEUE_ENABLED", "0")
        monkeypatch.setenv("DATA_SOURCE_MASTER_KEY", VALID_DATA_SOURCE_MASTER_KEY)

        storage = _reload("engine.runtime.storage")
        runtime_meta = _reload("engine.runtime.runtime_meta")
        health = _reload("engine.runtime.health")
        storage.init_db()

        now_ms = 1_800_000_000_000
        base_state = {
            "running": True,
            "provider_status": "running",
            "market_state": {
                "last_price_ts_ms": now_ms - 500,
                "healthy_providers": 1,
                "providers": {"yfinance": {"ok": True, "last_ts_ms": now_ms - 500}},
            },
            "last_event_ts_ms": now_ms - 500,
            "ts_ms": now_ms - 250,
        }
        runtime_meta.meta_set(
            "ingestion_state::shard:0-of-2",
            json.dumps(
                {
                    **base_state,
                    "pid": 101,
                    "shard": {"index": 0, "count": 2, "enabled": True, "label": "shard:0-of-2"},
                    "children": {"poll_prices": {"running": True, "pid": 201}},
                },
                separators=(",", ":"),
                sort_keys=True,
            ),
        )
        runtime_meta.meta_set(
            "ingestion_state::shard:1-of-2",
            json.dumps(
                {
                    **base_state,
                    "pid": 102,
                    "shard": {"index": 1, "count": 2, "enabled": True, "label": "shard:1-of-2"},
                    "children": {"options_poll": {"running": True, "pid": 202}},
                },
                separators=(",", ":"),
                sort_keys=True,
            ),
        )
        storage.put_job_heartbeat("ingestion_runtime:shard:0-of-2", "owner-0", 101, extra_json="{}")
        storage.put_job_heartbeat("ingestion_runtime:shard:1-of-2", "owner-1", 102, extra_json="{}")

        monkeypatch.setattr(health, "market_data_status", lambda con=None: {})
        con = storage.connect(readonly=True)
        try:
            snapshot = health._shared_ingestion_runtime_snapshot(
                con,
                now_ms=now_ms,
                effective_prices_max_age_s=5.0,
            )
        finally:
            con.close()
            storage.close_pooled_connections()

        assert snapshot["running"] is True
        assert snapshot["stale"] is False
        assert snapshot["healthy_providers"] == 1
        assert {row["label"] for row in snapshot["shards"]} == {"shard:0-of-2", "shard:1-of-2"}
        assert set(snapshot["children"]) == {"shard:0-of-2:poll_prices", "shard:1-of-2:options_poll"}
