from __future__ import annotations

import importlib
import json
import sqlite3
import sys
import threading
import time
from pathlib import Path
from unittest.mock import Mock

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload(*names: str):
    modules = []
    for name in names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


class _ManagedSqliteConnection(sqlite3.Connection):
    def begin_managed_write(self) -> None:
        self.execute("BEGIN IMMEDIATE")


_ManagedSqliteConnection.__module__ = "sqlite3"


class _SqliteRuntime:
    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)

    def connect(self, readonly: bool = False, **_kwargs):
        del readonly
        return sqlite3.connect(str(self.db_path), factory=_ManagedSqliteConnection)

    def run_write_txn(self, fn, *args, **kwargs):
        del args, kwargs
        con = self.connect()
        try:
            con.begin_managed_write()
            result = fn(con)
            con.commit()
            return result
        except Exception:
            con.rollback()
            raise
        finally:
            con.close()

    def table_exists(self, con, table: str) -> bool:
        row = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (str(table),),
        ).fetchone()
        return bool(row)


def _init_exec_db(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "execution_event_freshness.sqlite"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("RUNTIME_METRICS_BUFFER_ENABLED", "0")
    runtime = _SqliteRuntime(db_path)

    storage_facade = importlib.reload(importlib.import_module("engine.runtime.storage"))

    monkeypatch.setattr(storage_facade, "connect", runtime.connect, raising=False)
    monkeypatch.setattr(storage_facade, "connect_ro_direct", lambda **_kw: runtime.connect(readonly=True), raising=False)
    monkeypatch.setattr(storage_facade, "connect_rw_direct", lambda **_kw: runtime.connect(readonly=False), raising=False)
    monkeypatch.setattr(storage_facade, "run_write_txn", runtime.run_write_txn, raising=False)
    monkeypatch.setattr(storage_facade, "init_db", lambda schema=None: None, raising=False)
    monkeypatch.setattr(storage_facade, "_table_exists", runtime.table_exists, raising=False)

    metrics_store, _metrics, ledger, _reactivity = _reload(
        "engine.runtime.metrics_store",
        "engine.runtime.metrics",
        "engine.execution.execution_ledger",
        "engine.execution.kill_switch_reactivity",
    )
    monkeypatch.setattr(metrics_store, "_db_connect", runtime.connect, raising=False)
    monkeypatch.setattr(metrics_store, "_init_db", lambda: None, raising=False)
    monkeypatch.setattr(metrics_store, "run_write_txn", runtime.run_write_txn, raising=False)
    monkeypatch.setattr(ledger, "connect", runtime.connect, raising=False)
    monkeypatch.setattr(ledger, "connect_rw_direct", lambda **_kw: runtime.connect(readonly=False), raising=False)
    monkeypatch.setattr(ledger, "run_write_txn", runtime.run_write_txn, raising=False)
    monkeypatch.setattr(ledger, "_execution_ledger_schema_marker_ready", lambda: False, raising=False)
    monkeypatch.setattr(ledger, "_mark_execution_ledger_schema_ready", lambda con: None, raising=False)
    ledger.init_execution_ledger()
    con = runtime.connect()
    try:
        con.executescript(
            """
            CREATE TABLE IF NOT EXISTS alerts (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts_ms INTEGER,
              event_title TEXT,
              symbol TEXT,
              horizon_s INTEGER,
              expected_z REAL,
              confidence REAL,
              severity TEXT,
              rule_id TEXT,
              explain_json TEXT,
              dedupe_key TEXT
            );

            CREATE TABLE IF NOT EXISTS event_log (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts_ms INTEGER NOT NULL,
              event_type TEXT NOT NULL,
              event_source TEXT,
              event_version INTEGER NOT NULL DEFAULT 1,
              entity_type TEXT,
              entity_id TEXT,
              correlation_id TEXT,
              payload_json TEXT
            );

            CREATE TABLE IF NOT EXISTS model_marketplace_scores (
              model_id TEXT NOT NULL,
              model_name TEXT NOT NULL,
              symbol TEXT NOT NULL,
              horizon_s INTEGER NOT NULL,
              regime TEXT NOT NULL,
              stage TEXT,
              score REAL,
              trades INTEGER,
              wins INTEGER,
              losses INTEGER,
              gross_pnl REAL,
              net_pnl REAL,
              avg_confidence REAL,
              last_signal_ts_ms INTEGER,
              updated_ts_ms INTEGER,
              meta_json TEXT,
              PRIMARY KEY(model_id, model_name, symbol, horizon_s, regime)
            );

            CREATE TABLE IF NOT EXISTS champion_assignments (
              scope TEXT,
              symbol TEXT,
              horizon_s INTEGER,
              regime TEXT,
              model_name TEXT,
              state TEXT
            );
            """
        )
        con.commit()
    finally:
        con.close()
    return runtime, ledger


def _insert_alpaca_order(storage, *, client_order_id: str = "cid-1", broker_order_id: str = "bo-1") -> None:
    con = storage.connect()
    try:
        con.execute(
            """
            INSERT INTO execution_orders(
              client_order_id, broker, model_id, symbol, qty, submit_ts_ms,
              ref_px, broker_order_id, status, extra_json
            )
            VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            (
                str(client_order_id),
                "alpaca",
                "baseline",
                "AAPL",
                10.0,
                1_000,
                100.0,
                str(broker_order_id),
                "submitted",
                "{}",
            ),
        )
        con.commit()
    finally:
        con.close()


def test_alpaca_ws_and_poll_fills_are_idempotent_delta_recovered(tmp_path, monkeypatch):
    storage, _ledger = _init_exec_db(tmp_path, monkeypatch)
    (alpaca,) = _reload("engine.execution.broker_alpaca_rest")
    _insert_alpaca_order(storage)

    ws_partial = {
        "stream": "trade_updates",
        "data": {
            "event": "partial_fill",
            "execution_id": "exec-1",
            "timestamp": "1970-01-01T00:00:02Z",
            "qty": "4",
            "price": "100",
            "order": {
                "id": "bo-1",
                "client_order_id": "cid-1",
                "symbol": "AAPL",
                "side": "buy",
                "filled_qty": "4",
                "status": "partially_filled",
            },
        },
    }
    assert alpaca.apply_alpaca_trade_update(ws_partial, received_ts_ms=2_100)["status"] == "fill_logged"

    poll_order = {
        "id": "bo-1",
        "client_order_id": "cid-1",
        "symbol": "AAPL",
        "side": "buy",
        "status": "filled",
        "filled_qty": "10",
        "filled_avg_price": "101",
        "updated_at": "1970-01-01T00:00:03Z",
    }
    monkeypatch.setattr(alpaca, "list_orders_after", lambda after_ts_ms: [dict(poll_order)])
    assert alpaca.poll_and_log_fills(after_ts_ms=0)["fills_logged"] == 1
    assert alpaca.poll_and_log_fills(after_ts_ms=0)["fills_logged"] == 0
    assert alpaca.apply_alpaca_trade_update(ws_partial, received_ts_ms=2_200)["status"] in {
        "duplicate_or_no_delta",
        "fill_logged",
    }

    con = storage.connect(readonly=True)
    try:
        row = con.execute(
            "SELECT COUNT(*), COALESCE(SUM(fill_qty), 0.0) FROM execution_fills WHERE client_order_id='cid-1'"
        ).fetchone()
        assert int(row[0] or 0) == 2
        assert float(row[1] or 0.0) == 10.0
        extras = [
            json.loads(raw or "{}")
            for (raw,) in con.execute(
                "SELECT extra_json FROM execution_fills WHERE client_order_id='cid-1' ORDER BY fill_ts_ms, id"
            ).fetchall()
        ]
        assert all("fill_detection_latency_ms" in extra for extra in extras)
    finally:
        con.close()


def test_alpaca_stream_disconnect_runs_gap_poll(tmp_path, monkeypatch):
    storage, _ledger = _init_exec_db(tmp_path, monkeypatch)
    (alpaca,) = _reload("engine.execution.broker_alpaca_rest")
    _insert_alpaca_order(storage)

    stop = threading.Event()
    sent_messages: list[dict] = []

    class _FakeWebSocketApp:
        def __init__(self, _url, on_open=None, on_message=None, on_error=None, on_close=None):
            self.on_open = on_open
            self.on_message = on_message
            self.on_close = on_close

        def send(self, payload):
            sent_messages.append(json.loads(payload))

        def run_forever(self, **_kwargs):
            self.on_open(self)
            self.on_message(
                self,
                json.dumps(
                    {
                        "stream": "trade_updates",
                        "data": {
                            "event": "fill",
                            "execution_id": "exec-gap",
                            "timestamp": "1970-01-01T00:00:04Z",
                            "qty": "1",
                            "price": "100",
                            "order": {
                                "id": "bo-1",
                                "client_order_id": "cid-1",
                                "symbol": "AAPL",
                                "side": "buy",
                                "filled_qty": "1",
                                "status": "filled",
                            },
                        },
                    }
                ),
            )
            stop.set()
            self.on_close(self, 1000, "done")

    poll_mock = Mock(return_value={"ok": True, "fills_logged": 0})
    monkeypatch.setattr(alpaca, "KEY_ID", "key")
    monkeypatch.setattr(alpaca, "SECRET", "secret")
    monkeypatch.setattr(alpaca, "websocket", type("_WS", (), {"WebSocketApp": _FakeWebSocketApp}))
    monkeypatch.setattr(alpaca, "poll_and_log_fills", poll_mock)

    alpaca.run_trade_updates_stream_daemon(stop_event=stop)

    assert sent_messages[0]["action"] == "auth"
    assert sent_messages[1]["data"]["streams"] == ["trade_updates"]
    assert poll_mock.called
    con = storage.connect(readonly=True)
    try:
        assert int(con.execute("SELECT COUNT(*) FROM execution_fills").fetchone()[0] or 0) == 1
    finally:
        con.close()


def test_kill_switch_interrupts_router_slice_sleep_within_bound(tmp_path, monkeypatch):
    storage, _ledger = _init_exec_db(tmp_path, monkeypatch)
    broker_router, kill_switch = _reload("engine.execution.broker_router", "engine.execution.kill_switch")
    reactivity = importlib.import_module("engine.execution.kill_switch_reactivity")
    monkeypatch.setenv("EXEC_KILL_REACTION_BOUND_S", "0.2")
    monkeypatch.setenv("EXEC_ADAPTIVE_SLICING", "1")
    monkeypatch.setattr(kill_switch, "_get_lifecycle_state", lambda: {"state": "LIVE"})
    monkeypatch.setattr(kill_switch, "REQUIRE_FRESH_DATA", False)
    monkeypatch.setattr(kill_switch, "REQUIRE_FRESH_JOBS", False)
    monkeypatch.setattr(kill_switch, "_capital_risk_trigger", lambda _con: None)
    monkeypatch.setattr(kill_switch, "_model_risk_trigger", lambda _con, _mid: None)
    monkeypatch.setattr("engine.strategy.capital_guard.trading_allowed", lambda con=None: True)
    monkeypatch.setattr(broker_router, "_load_recent_slippage_bps", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        broker_router,
        "build_order_slices",
        lambda order, broker_name="": [
            {**dict(order), "qty": 1.0, "slice_interval_ms": 2_000, "slice_style": "twap", "slice_index": 0},
            {**dict(order), "qty": 1.0, "slice_interval_ms": 2_000, "slice_style": "twap", "slice_index": 1},
        ],
    )

    def _activate_direct() -> None:
        now_ms = int(time.time() * 1000)
        con = storage.connect()
        try:
            kill_switch._ensure_schema(con)
            con.execute(
                """
                INSERT INTO kill_switch_state(
                  scope, key, enabled, reason, actor, meta_json, created_ts_ms, updated_ts_ms
                )
                VALUES ('global', 'global', 1, 'unit_test', 'test', '{}', ?, ?)
                ON CONFLICT(scope, key) DO UPDATE SET
                  enabled=excluded.enabled,
                  reason=excluded.reason,
                  actor=excluded.actor,
                  meta_json=excluded.meta_json,
                  updated_ts_ms=excluded.updated_ts_ms
                """,
                (now_ms, now_ms),
            )
            con.commit()
        finally:
            con.close()
        reactivity.notify_kill_switch_state_changed(enabled=True, ts_ms=now_ms)

    calls = []

    def _adapter(**_kwargs):
        calls.append(time.monotonic())
        if len(calls) == 1:
            threading.Timer(0.05, _activate_direct).start()
        return {"ok": True, "status": "submitted"}

    started = time.monotonic()
    result = broker_router._adaptive_execute_orders(
        broker_name="sim",
        fn=_adapter,
        dry_run=False,
        override_orders=[{"symbol": "AAPL", "qty": 2.0, "model_id": "baseline"}],
        override_order_id=1,
        override_ts_ms=1_000,
    )
    elapsed = time.monotonic() - started

    assert result["ok"] is False
    assert result["status"] == "blocked_kill_switch_mid_slice"
    assert len(calls) == 1
    assert elapsed < 1.0


def test_crypto_funding_next_poll_aligns_to_settlement_mark(monkeypatch):
    monkeypatch.setenv("CRYPTO_FUNDING_ALIGN_TO_SETTLEMENT_MARKS", "1")
    monkeypatch.setenv("CRYPTO_FUNDING_SETTLEMENT_HOURS_UTC", "0,8,16")
    monkeypatch.setenv("CRYPTO_FUNDING_SETTLEMENT_LAG_SECONDS", "60")
    (funding_job,) = _reload("engine.data.jobs.ingest_crypto_funding")

    now_s = 8 * 3600 - 30
    assert funding_job.seconds_until_next_funding_mark(now_s=now_s) == 90.0

    now_s = 8 * 3600 + 90
    assert funding_job.seconds_until_next_funding_mark(now_s=now_s) == (16 * 3600 + 60) - now_s


def test_execution_metrics_expose_event_freshness_latencies(tmp_path, monkeypatch):
    storage, _ledger = _init_exec_db(tmp_path, monkeypatch)
    api_read, metrics_store = _reload("engine.api.api_read", "engine.runtime.metrics_store")
    monkeypatch.setattr(api_read, "_db_connect", storage.connect, raising=False)
    monkeypatch.setattr(api_read, "_table_exists", storage.table_exists, raising=False)
    monkeypatch.setattr(metrics_store, "_db_connect", storage.connect, raising=False)
    monkeypatch.setattr(metrics_store, "_init_db", lambda: None, raising=False)
    monkeypatch.setattr(metrics_store, "run_write_txn", storage.run_write_txn, raising=False)
    _insert_alpaca_order(storage)
    con = storage.connect()
    try:
        con.execute(
            """
            INSERT INTO execution_fills(
              client_order_id, fill_id, broker, model_id, symbol, ts_ms, fill_ts_ms,
              fill_qty, fill_px, extra_json
            )
            VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "cid-1",
                "fill-1",
                "alpaca",
                "baseline",
                "AAPL",
                2_000,
                2_000,
                1.0,
                100.0,
                json.dumps({"fill_detection_latency_ms": 123}),
            ),
        )
        con.commit()
    finally:
        con.close()
    metrics_store.write_runtime_metric("kill_reaction_latency_ms", 42, tags={"broker": "sim"}, ts_ms=3_000)

    payload = api_read.get_execution_metrics()
    assert payload["ok"] is True
    assert payload["avg_fill_detection_latency_ms"] == 123.0
    assert payload["latest_kill_reaction_latency_ms"] == 42.0
