from __future__ import annotations

import inspect
import json
import sqlite3
import threading
import time

import pytest

from engine.data import poll_prices


def test_price_cycle_marks_live_when_first_price_marker_already_exists(monkeypatch):
    transitions = []

    monkeypatch.setattr(poll_prices, "meta_set_if_missing", lambda _key, _value: False)
    monkeypatch.setattr(poll_prices, "meta_set", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        poll_prices,
        "set_state",
        lambda state, detail: transitions.append((state, detail)),
    )

    poll_prices._finalize_post_commit_price_cycle(
        [],
        {"first_ts_ms": 1234567890, "provider": "yfinance"},
    )

    assert transitions == [(poll_prices.LIVE, "market_data_healthy")]


class _FakeSession:
    def __init__(self) -> None:
        self.errors = []

    def latency_ms(self) -> int:
        return 0

    def note_error(self, error) -> None:
        self.errors.append(str(error))


class _FakeManager:
    def __init__(self, name, snapshot_fn, *, ok=True, last_error="") -> None:
        self.name = str(name)
        self._snapshot_fn = snapshot_fn
        self._ok = bool(ok)
        self._last_error = str(last_error)

    def snapshot(self):
        return self._snapshot_fn(self.name)

    def provider_telemetry(self):
        return {"last_error": self._last_error}

    def ok(self):
        return self._ok


class _CountingConnection(sqlite3.Connection):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.price_history_queries = 0
        self.queries = []

    def execute(self, sql, params=()):  # type: ignore[override]
        text = " ".join(str(sql).split())
        self.queries.append((text, tuple(params or ())))
        if "FROM prices" in text:
            self.price_history_queries += 1
        return super().execute(sql, params or ())


class _FakeCursor:
    def __init__(self, rows) -> None:
        self._rows = list(rows)

    def fetchall(self):
        return list(self._rows)


class _FakePostgresHistoryConnection:
    def __init__(self, rows) -> None:
        self.rows = list(rows)
        self.queries = []
        self.closed = False

    def execute(self, sql, params=()):
        text = " ".join(str(sql).split())
        self.queries.append((text, tuple(params or ())))
        return _FakeCursor(self.rows)

    def close(self):
        self.closed = True


class _SymbolUniverseConnection:
    def __init__(self) -> None:
        self.active_rows = []
        self.fallback_rows = []
        self.active_symbol_selects = 0
        self.fallback_symbol_selects = 0
        self.updates = []
        self.closed = False

    def execute(self, sql, params=()):
        text = " ".join(str(sql).split())
        if "FROM symbols WHERE status IN ('ACTIVE','WATCH')" in text:
            self.active_symbol_selects += 1
            return _FakeCursor(self.active_rows)
        if "FROM symbols ORDER BY updated_ts_ms DESC" in text:
            self.fallback_symbol_selects += 1
            return _FakeCursor(self.fallback_rows)
        if text.startswith("UPDATE symbols SET meta_json="):
            self.updates.append((text, tuple(params or ())))
            return _FakeCursor([])
        raise AssertionError(f"unexpected SQL: {text}")

    def close(self):
        self.closed = True


def _price_history_connection() -> _CountingConnection:
    raw = sqlite3.connect(":memory:", factory=_CountingConnection)
    raw.execute("CREATE TABLE prices(ts_ms INTEGER, symbol TEXT, price REAL, px REAL)")
    raw.executemany(
        "INSERT INTO prices(ts_ms, symbol, price, px) VALUES (?,?,?,?)",
        [
            (1000, "ABC", 100.0, 100.0),
            (1100, "ABC", 101.0, 101.0),
            (1200, "ABC", 102.0, 102.0),
            (1000, "DEF", 200.0, 200.0),
            (1100, "DEF", 201.0, 201.0),
            (1200, "DEF", 202.0, 202.0),
            (1000, "GHI", 301.0, 301.0),
        ],
    )
    raw.commit()
    raw.queries.clear()
    raw.price_history_queries = 0
    return raw


def _active_universe(
    active_rows,
    *,
    yf_map=None,
    ccxt_map=None,
    polygon_map=None,
) -> poll_prices.ActiveSymbolUniverse:
    return poll_prices.ActiveSymbolUniverse(
        active_symbol_rows=tuple(active_rows),
        provider_symbol_rows=tuple(active_rows),
        yf_map=dict(yf_map or {}),
        ccxt_map=dict(ccxt_map or {}),
        polygon_map=dict(polygon_map or {}),
    )


def test_cycle_symbol_plan_loads_universe_once_and_reuses_provider_maps(monkeypatch):
    loads = []
    universe = _active_universe(
        [("AAPL", "{}"), ("BTC", "{}"), ("SPY", "{}")],
        yf_map={"AAPL": "AAPL"},
        ccxt_map={"BTC": "BTC/USDT"},
        polygon_map={"SPY": "SPY"},
    )

    def load_once():
        loads.append("load")
        return universe

    monkeypatch.setattr(poll_prices, "_load_active_symbol_universe", load_once)

    plan = poll_prices._build_cycle_symbol_plan(["yfinance", "ccxt", "polygon"])

    assert loads == ["load"]
    assert plan.universe is universe
    assert plan.assigned_symbol_count == 3
    assert plan.provider_symbol_maps == {
        "yfinance": {"AAPL": "AAPL"},
        "ccxt": {"BTC": "BTC/USDT"},
        "polygon": {"SPY": "SPY"},
    }


def test_cycle_symbol_plan_refreshes_dynamic_symbols_between_cycles(monkeypatch):
    loads = []
    universes = iter(
        [
            _active_universe([("AAPL", "{}")], yf_map={"AAPL": "AAPL"}),
            _active_universe([("TSLA", "{}")], yf_map={"TSLA": "TSLA"}),
        ]
    )

    def load_next():
        loads.append("load")
        return next(universes)

    monkeypatch.setattr(poll_prices, "_load_active_symbol_universe", load_next)

    first = poll_prices._build_cycle_symbol_plan(["yfinance"])
    second = poll_prices._build_cycle_symbol_plan(["yfinance"])

    assert loads == ["load", "load"]
    assert first.provider_symbol_maps == {"yfinance": {"AAPL": "AAPL"}}
    assert second.provider_symbol_maps == {"yfinance": {"TSLA": "TSLA"}}


def test_poll_cycle_reuses_active_universe_for_stale_marking(monkeypatch):
    now_ts_ms = 1_700_000_000_000
    stale_ts_ms = now_ts_ms - ((poll_prices.PRICE_STALE_AFTER_S + 10) * 1000)
    con = _SymbolUniverseConnection()
    con.active_rows = [
        ("AAPL", json.dumps({"price_status": {"last_seen_ts_ms": stale_ts_ms, "stale": True}})),
        ("MSFT", json.dumps({"price_status": {"last_seen_ts_ms": stale_ts_ms, "stale": False}})),
    ]
    alerts = []

    monkeypatch.setenv("FORCE_FACTOR_PROXY_TICKERS", "0")
    monkeypatch.setattr(poll_prices, "load_default_symbols", lambda: [])
    monkeypatch.setattr(poll_prices, "filter_symbol_mapping_for_shard", lambda mapping, _shard: dict(mapping))
    monkeypatch.setattr(poll_prices, "connect", lambda *args, **kwargs: con)
    monkeypatch.setattr(poll_prices, "_has_first_price_tick", lambda: True)
    monkeypatch.setattr(poll_prices, "emit_alert", lambda **kwargs: alerts.append(dict(kwargs)))

    def run_write(fn, **_kwargs):
        fn(con)
        return True, None

    monkeypatch.setattr(poll_prices, "_run_write_txn_allow_busy", run_write)

    universe = poll_prices._load_active_symbol_universe()
    assert con.active_symbol_selects == 1

    poll_prices._mark_stale(
        now_ts_ms,
        active_symbol_rows=universe.active_symbol_rows,
        fresh_symbols=["AAPL"],
        fresh_symbol_ts_ms={"AAPL": now_ts_ms - 1000},
    )

    assert con.active_symbol_selects == 1
    assert con.fallback_symbol_selects == 0
    assert [params[2] for _sql, params in con.updates] == ["AAPL", "MSFT"]
    updated_meta = {params[2]: json.loads(params[0]) for _sql, params in con.updates}
    assert updated_meta["AAPL"]["price_status"]["stale"] is False
    assert updated_meta["AAPL"]["price_status"]["last_seen_ts_ms"] == now_ts_ms - 1000
    assert updated_meta["MSFT"]["price_status"]["stale"] is True
    assert [alert["symbol"] for alert in alerts] == ["MSFT"]


def test_active_symbol_universe_refreshes_between_cycles(monkeypatch):
    con = _SymbolUniverseConnection()
    monkeypatch.setenv("FORCE_FACTOR_PROXY_TICKERS", "0")
    monkeypatch.setattr(poll_prices, "load_default_symbols", lambda: [])
    monkeypatch.setattr(poll_prices, "filter_symbol_mapping_for_shard", lambda mapping, _shard: dict(mapping))
    monkeypatch.setattr(poll_prices, "connect", lambda *args, **kwargs: con)

    con.active_rows = [("AAPL", json.dumps({}))]
    first = poll_prices._load_active_symbol_universe()

    con.active_rows = [("TSLA", json.dumps({}))]
    second = poll_prices._load_active_symbol_universe()

    assert con.active_symbol_selects == 2
    assert "AAPL" in first.yf_map
    assert "TSLA" not in first.yf_map
    assert "TSLA" in second.yf_map
    assert "AAPL" not in second.yf_map


def test_recent_prices_map_batches_many_symbols_in_one_query(monkeypatch):
    counting = _price_history_connection()
    monkeypatch.setattr(poll_prices, "connect", lambda readonly=False: counting)

    histories = poll_prices._recent_prices_map(["abc", "DEF", "GHI", "ABC"], 2)

    assert histories == {
        "ABC": [101.0, 102.0],
        "DEF": [201.0, 202.0],
        "GHI": [301.0],
    }
    assert counting.price_history_queries == 1
    assert len(counting.queries) == 1
    assert "ROW_NUMBER() OVER" in counting.queries[0][0]
    assert counting.queries[0][1] == ("ABC", "DEF", "GHI", 2)


def test_recent_prices_map_uses_one_postgres_lateral_query_for_many_symbols(monkeypatch):
    fake = _FakePostgresHistoryConnection(
        [
            ("ABC", 101.0),
            ("ABC", 102.0),
            ("DEF", 201.0),
            ("DEF", 202.0),
        ]
    )
    monkeypatch.setattr(poll_prices, "connect", lambda readonly=False: fake)
    monkeypatch.setattr(poll_prices.dbapi, "is_sqlite_connection", lambda _con: False)

    histories = poll_prices._recent_prices_map(["ABC", "DEF"], 2)

    assert histories == {"ABC": [101.0, 102.0], "DEF": [201.0, 202.0]}
    assert fake.closed is True
    assert len(fake.queries) == 1
    assert "JOIN LATERAL" in fake.queries[0][0]
    assert fake.queries[0][1] == ("ABC", "DEF", 2)


def test_split_detection_uses_batched_history_and_preserves_log_payload(monkeypatch):
    counting = _price_history_connection()
    monkeypatch.setattr(poll_prices, "connect", lambda readonly=False: counting)
    captured = {}

    def _capture_split_like_row(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(poll_prices, "log_split_like_price_row", _capture_split_like_row)

    histories = poll_prices._recent_prices_map(["ABC"], 10)
    rejected = poll_prices._reject_split_like_price_row(
        symbol="ABC",
        ts_ms=2000,
        current_price=40.0,
        price_payload={"provider": "polygon"},
        hist=histories["ABC"],
    )

    assert rejected is True
    assert counting.price_history_queries == 1
    assert captured == {
        "symbol": "ABC",
        "ts_ms": 2000,
        "previous_price": 102.0,
        "current_price": 40.0,
        "source": "polygon",
    }


def test_collect_rest_provider_snapshots_runs_bounded_parallel(monkeypatch):
    entered = []
    lock = threading.Lock()
    both_started = threading.Event()

    def snapshot_fn(name):
        with lock:
            entered.append(str(name))
            if len(entered) == 2:
                both_started.set()
        if not both_started.wait(timeout=1.0):
            raise AssertionError("provider snapshots did not overlap")
        return {str(name).upper(): {"price": 100.0, "ts_ms": 123, "source": str(name)}}

    monkeypatch.setattr(poll_prices, "POLL_PRICES_PROVIDER_MAX_WORKERS", 2)

    results = poll_prices._collect_rest_provider_snapshots(
        [
            ("yfinance", _FakeManager("yfinance", snapshot_fn), _FakeSession()),
            ("polygon", _FakeManager("polygon", snapshot_fn), _FakeSession()),
        ]
    )

    assert set(results) == {"yfinance", "polygon"}
    assert all(result["ok"] for result in results.values())
    assert all(result["latency_ms"] >= 0 for result in results.values())
    assert set(entered) == {"yfinance", "polygon"}


def test_collect_rest_provider_snapshots_isolates_provider_failure(monkeypatch):
    bad_session = _FakeSession()

    def good_snapshot(name):
        return {str(name).upper(): {"price": 101.0, "ts_ms": 123, "source": str(name)}}

    def bad_snapshot(_name):
        raise RuntimeError("upstream timeout")

    monkeypatch.setattr(poll_prices, "POLL_PRICES_PROVIDER_MAX_WORKERS", 2)

    results = poll_prices._collect_rest_provider_snapshots(
        [
            ("yfinance", _FakeManager("yfinance", good_snapshot), _FakeSession()),
            ("polygon", _FakeManager("polygon", bad_snapshot, ok=False), bad_session),
        ]
    )

    assert results["yfinance"]["ok"] is True
    assert results["yfinance"]["got"]
    assert results["polygon"]["ok"] is False
    assert "RuntimeError: upstream timeout" in str(results["polygon"]["error"])
    assert bad_session.errors == ["RuntimeError: upstream timeout"]


def test_collect_rest_provider_snapshots_times_out_one_provider_without_blocking(monkeypatch):
    slow_session = _FakeSession()
    release_slow = threading.Event()

    def fast_snapshot(name):
        return {str(name).upper(): {"price": 101.0, "ts_ms": 123, "source": str(name)}}

    def slow_snapshot(name):
        release_slow.wait(timeout=0.50)
        return {str(name).upper(): {"price": 102.0, "ts_ms": 123, "source": str(name)}}

    monkeypatch.setattr(poll_prices, "POLL_PRICES_PROVIDER_MAX_WORKERS", 2)
    monkeypatch.setattr(poll_prices, "_log_nonfatal", lambda *_args, **_kwargs: None)

    started = time.perf_counter()
    try:
        results = poll_prices._collect_rest_provider_snapshots(
            [
                ("slow", _FakeManager("slow", slow_snapshot), slow_session),
                ("fast", _FakeManager("fast", fast_snapshot), _FakeSession()),
            ],
            timeout_s=0.05,
        )
    finally:
        release_slow.set()
    elapsed_s = time.perf_counter() - started

    assert elapsed_s < 0.30
    assert results["fast"]["ok"] is True
    assert results["fast"]["got"]
    assert results["slow"]["ok"] is False
    assert "provider_snapshot_timeout_after_" in str(results["slow"]["error"])
    assert slow_session.errors == [results["slow"]["error"]]


def test_collect_rest_provider_snapshots_returns_provider_order_deterministically(monkeypatch):
    fast_done = threading.Event()

    def slow_snapshot(name):
        fast_done.wait(timeout=1.0)
        return {str(name).upper(): {"price": 101.0, "ts_ms": 123, "source": str(name)}}

    def fast_snapshot(name):
        fast_done.set()
        return {str(name).upper(): {"price": 102.0, "ts_ms": 123, "source": str(name)}}

    monkeypatch.setattr(poll_prices, "POLL_PRICES_PROVIDER_MAX_WORKERS", 2)

    results = poll_prices._collect_rest_provider_snapshots(
        [
            ("slow", _FakeManager("slow", slow_snapshot), _FakeSession()),
            ("fast", _FakeManager("fast", fast_snapshot), _FakeSession()),
        ],
        timeout_s=1.0,
    )

    assert list(results) == ["slow", "fast"]
    assert results["slow"]["got"]["SLOW"]["price"] == 101.0
    assert results["fast"]["got"]["FAST"]["price"] == 102.0


def test_provider_auxiliary_rows_consume_async_backpressure(monkeypatch):
    logged = []

    monkeypatch.setattr(
        poll_prices,
        "publish_price_events",
        lambda *_args, **_kwargs: {
            "async_persistence": {
                "attempted": True,
                "accepted": False,
                "backpressure": True,
                "reason": "enqueue_rejected",
            }
        },
    )
    monkeypatch.setattr(
        poll_prices,
        "_log_nonfatal",
        lambda event, exc, **context: logged.append((event, str(exc), context)),
    )

    ok = poll_prices._enqueue_provider_auxiliary_rows(
        [(1_700_000_000_000, "AAPL", "polygon", 101.0, 100.9, 101.1, 0.2, 1234.0)],
        [],
    )

    assert ok is False
    assert logged
    assert logged[0][0] == "poll_prices_provider_raw_async_backpressure"
    assert logged[0][2]["async_persistence"]["reason"] == "enqueue_rejected"


def test_successful_poll_cycle_keeps_backoff_when_producer_backpressured(monkeypatch):
    monkeypatch.setattr(poll_prices, "FAIL_BASE_S", 2.0)
    monkeypatch.setattr(poll_prices, "FAIL_MAX_S", 10.0)

    assert (
        poll_prices._successful_price_cycle_backoff_s(
            current_fail_s=0.0,
            producer_backpressure=True,
        )
        == 2.0
    )
    assert (
        poll_prices._successful_price_cycle_backoff_s(
            current_fail_s=4.0,
            producer_backpressure=True,
        )
        == 8.0
    )
    assert (
        poll_prices._successful_price_cycle_backoff_s(
            current_fail_s=8.0,
            producer_backpressure=True,
        )
        == 10.0
    )
    assert (
        poll_prices._successful_price_cycle_backoff_s(
            current_fail_s=8.0,
            producer_backpressure=False,
        )
        == 0.0
    )


def test_poll_prices_status_records_provider_snapshot_observability(monkeypatch):
    recorded = {}
    job_status = {}

    class SourceManager:
        def record_job_status(self, *args, **kwargs):
            job_status["args"] = args
            job_status["kwargs"] = kwargs

    def fake_record_pipeline_status(*args, **kwargs):
        recorded["args"] = args
        recorded["kwargs"] = kwargs
        return {"ok": kwargs["ok"], "meta": kwargs["meta"]}

    monkeypatch.setattr(poll_prices, "record_pipeline_status", fake_record_pipeline_status)

    status = poll_prices._record_poll_prices_status(
        SourceManager(),
        ok=True,
        providers=["yfinance", "polygon"],
        provider_errors={"polygon": "timeout"},
        provider_latencies_ms={"yfinance": 12, "polygon": 503},
        provider_result_counts={"yfinance": 4, "polygon": 0},
        message="unit test",
    )

    assert status["meta"]["provider_errors"] == {"polygon": "timeout"}
    assert status["meta"]["provider_latencies_ms"] == {"yfinance": 12, "polygon": 503}
    assert status["meta"]["provider_result_counts"] == {"yfinance": 4, "polygon": 0}
    assert recorded["kwargs"]["meta"]["provider_errors"] == {"polygon": "timeout"}
    assert job_status["kwargs"]["meta"]["provider_latencies_ms"]["polygon"] == 503


def test_normal_liveness_heartbeat_keeps_pooled_connections_warm(monkeypatch):
    calls = []

    def forbidden_pool_teardown():
        raise AssertionError("heartbeat must not tear down pooled connections")

    monkeypatch.setattr(
        poll_prices,
        "close_pooled_connections",
        forbidden_pool_teardown,
        raising=False,
    )
    monkeypatch.setattr(poll_prices, "_uses_child_job_lock", lambda: True)
    monkeypatch.setattr(poll_prices, "_uses_price_feed_lock", lambda: True)
    monkeypatch.setattr(
        poll_prices,
        "touch_job_lock",
        lambda *args, **kwargs: calls.append(("job_lock", args, kwargs)),
    )
    monkeypatch.setattr(
        poll_prices,
        "_touch_price_feed_lock",
        lambda now_ts_ms: calls.append(("price_feed_lock", now_ts_ms)),
    )

    def capture_heartbeat(job_name, owner, pid, *, extra_json, best_effort):
        calls.append(
            (
                "heartbeat",
                job_name,
                owner,
                pid,
                json.loads(extra_json),
                bool(best_effort),
            )
        )

    monkeypatch.setattr(poll_prices, "put_job_heartbeat", capture_heartbeat)

    poll_prices._write_liveness_heartbeat(
        now_ts_ms=1_700_000_000_000,
        fail_s=4.0,
        have_price_feed_lock=True,
        rest_managers={
            "yfinance": _FakeManager(
                "yfinance",
                lambda _name: {},
                last_error="",
            )
        },
    )

    assert [call[0] for call in calls] == ["job_lock", "price_feed_lock", "heartbeat"]
    heartbeat = calls[-1]
    assert heartbeat[1] == poll_prices.JOB_LIVENESS_NAME
    assert heartbeat[4]["job_name"] == poll_prices.JOB_NAME
    assert heartbeat[4]["liveness_job_name"] == poll_prices.JOB_LIVENESS_NAME
    assert heartbeat[4]["fail_backoff_s"] == 4.0
    assert heartbeat[4]["have_price_feed_lock"] is True
    assert heartbeat[4]["providers"] == {"yfinance": {"last_error": ""}}


def test_database_slowdown_write_backpressure_does_not_stampede_or_reset_pool(monkeypatch):
    run_calls = []
    logged = []

    def forbidden_pool_teardown():
        raise AssertionError("database slowdown must not reset pooled connections")

    def slow_run_write_txn(_fn, **kwargs):
        run_calls.append(dict(kwargs))
        raise TimeoutError("connection pool timeout after 0.050s")

    monkeypatch.setattr(
        poll_prices,
        "close_pooled_connections",
        forbidden_pool_teardown,
        raising=False,
    )
    monkeypatch.setattr(poll_prices, "run_write_txn", slow_run_write_txn)
    monkeypatch.setattr(
        poll_prices,
        "_log_nonfatal",
        lambda event, exc, **context: logged.append((event, str(exc), context)),
    )

    ok, result = poll_prices._run_write_txn_allow_busy(
        lambda _con: None,
        default=("kept", "warm"),
        table="prices",
        operation="ingest_merged_prices",
        context={"job": poll_prices.JOB_NAME},
        attempts=5,
        maintenance=False,
        busy_event="poll_prices_merged_prices_write_busy",
        warn_key="poll_prices_merged_prices_write_busy",
        extra={"merged_symbols": 3},
        timeout_s=0.25,
        busy_timeout_ms=250,
    )

    assert ok is False
    assert result == ("kept", "warm")
    assert len(run_calls) == 1
    assert run_calls[0]["attempts"] == 5
    assert logged == [
        (
            "poll_prices_merged_prices_write_busy",
            "connection pool timeout after 0.050s",
            {"warn_key": "poll_prices_merged_prices_write_busy", "merged_symbols": 3},
        )
    ]


def test_fatal_write_error_is_not_downgraded_to_slowdown(monkeypatch):
    def fatal_run_write_txn(*_args, **_kwargs):
        raise RuntimeError("schema missing")

    monkeypatch.setattr(
        poll_prices,
        "run_write_txn",
        fatal_run_write_txn,
    )

    with pytest.raises(RuntimeError, match="schema missing"):
        poll_prices._run_write_txn_allow_busy(
            lambda _con: None,
            default=None,
            table="prices",
            operation="ingest_merged_prices",
            busy_event="poll_prices_merged_prices_write_busy",
            warn_key="poll_prices_merged_prices_write_busy",
        )


def test_poll_prices_no_longer_resets_pooled_connections_in_hot_paths():
    assert "close_pooled_connections" not in inspect.getsource(poll_prices)
