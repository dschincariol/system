from __future__ import annotations

import json
import sqlite3
import threading
import time
from http.server import ThreadingHTTPServer
from pathlib import Path
from unittest.mock import Mock
from urllib.error import HTTPError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import pytest

import engine.api.http_transport as http_transport
from engine.strategy import portfolio_execution_intents as execution_intents
from engine.strategy.portfolio_execution_intents import _terminal_signed_qty
from engine.terminal.api import api_terminal as terminal_api
from engine.terminal.api import api_terminal_orders as terminal_orders


pytestmark = pytest.mark.safety_critical


class _TestHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True


class _FakeReadConnection:
    def __init__(self, *, price: float = 100.0, ts_ms: int | None = None, duplicate: bool = False) -> None:
        self.price = float(price)
        self.ts_ms = int(ts_ms or int(time.time() * 1000))
        self.duplicate = bool(duplicate)

    def execute(self, sql, params=()):
        text = str(sql).lower()
        if "pragma table_info(prices)" in text:
            return _FakeRows([(0, "symbol"), (1, "price"), (2, "ts_ms")])
        if "from prices" in text:
            return _FakeRows([(self.price, self.ts_ms)])
        if "from portfolio_orders" in text:
            explain = json.dumps({"terminal_order": {"qty": 1.0}})
            return _FakeRows([(self.ts_ms, "BUY", explain)] if self.duplicate else [])
        return _FakeRows([])

    def close(self) -> None:
        pass


class _FakeClosableReadConnection:
    def __init__(self) -> None:
        self.closed = False

    def execute(self, sql, params=()):
        if "sqlite_master" in str(sql):
            return _FakeRows([])
        return _FakeRows([])

    def close(self) -> None:
        self.closed = True


class _NoCloseSQLiteConnection:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con
        self.closed = False

    def execute(self, *args, **kwargs):
        return self.con.execute(*args, **kwargs)

    def close(self) -> None:
        self.closed = True


class _FakeRows(list):
    def fetchall(self):
        return list(self)

    def fetchone(self):
        return self[0] if self else None


class _FakeWriteConnection:
    def __init__(self) -> None:
        self.statements = []

    def execute(self, sql, params=()):
        self.statements.append((sql, params))
        return self


class _FakePositionReadConnection:
    def __init__(self, qty: float) -> None:
        self.qty = float(qty)

    def execute(self, sql, params=()):
        text = str(sql).lower()
        if "pragma table_info(prices)" in text:
            return _FakeRows([(0, "symbol"), (1, "price"), (2, "ts_ms")])
        if "from prices" in text:
            return _FakeRows([(100.0, int(time.time() * 1000))])
        if "broker_positions" in text:
            return _FakeRows([(self.qty,)])
        if "from portfolio_orders" in text:
            return _FakeRows([])
        return _FakeRows([])

    def close(self) -> None:
        pass


def _patch_terminal_order_storage(monkeypatch):
    writes = _FakeWriteConnection()

    monkeypatch.setattr(
        terminal_orders,
        "execution_gate_snapshot",
        lambda **_kwargs: {"real_trading_allowed": True},
    )
    monkeypatch.setattr(terminal_orders, "connect", lambda **_kwargs: _FakeReadConnection())
    monkeypatch.setattr(terminal_orders, "_table_exists", lambda _con, table: table in {"portfolio_orders", "prices"})
    monkeypatch.setattr(terminal_orders, "cache_invalidate_namespace", lambda _namespace: None)
    monkeypatch.setattr(terminal_orders, "run_write_txn", lambda fn, **_kwargs: fn(writes))
    return writes


def _patch_dispatcher_runtime_guards(monkeypatch):
    monkeypatch.setattr(http_transport, "deny_if_shutdown", lambda: None)
    monkeypatch.setattr(http_transport, "emit_counter", lambda *args, **kwargs: None)
    monkeypatch.setattr(http_transport, "emit_timing", lambda *args, **kwargs: None)


def _assert_terminal_sell_contract(writes):
    assert len(writes.statements) == 1
    _sql, params = writes.statements[0]
    assert params[3] == "SELL"
    assert params[4] == "FLAT"
    assert params[5] == "SHORT"
    assert params[8] == 0.0
    assert int(params[9]) > 0

    explain = json.loads(params[10])
    assert explain["terminal_order"]["sizing"] == "quantity"
    assert explain["terminal_order"]["symbol"] == "SPY"
    assert explain["terminal_order"]["side"] == "SELL"
    assert explain["terminal_order"]["qty"] == 3.0
    assert explain["terminal_order"]["signed_qty"] == -3.0
    assert explain["terminal_order"]["source_alert_id"] == int(params[9])


def test_terminal_order_accepts_dispatcher_body_and_records_quantity_contract(monkeypatch):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("ENGINE_MODE", "safe")
    writes = _patch_terminal_order_storage(monkeypatch)

    result = terminal_orders.api_post_terminal_order(
        urlparse("/api/terminal/order"),
        {"symbol": "spy", "side": "sell", "qty": 3},
        {},
    )

    assert result["ok"] is True
    assert result["symbol"] == "SPY"
    assert result["side"] == "SELL"
    assert result["qty"] == 3.0
    _assert_terminal_sell_contract(writes)


def test_terminal_order_accepts_paper_pipeline_gate(monkeypatch):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("ENGINE_MODE", "paper")
    writes = _patch_terminal_order_storage(monkeypatch)
    monkeypatch.setattr(
        terminal_orders,
        "execution_gate_snapshot",
        lambda **_kwargs: {
            "real_trading_allowed": False,
            "allow_execution_pipeline": True,
            "allow_simulation": True,
            "mode": "paper",
            "reason": "mode_paper",
        },
    )

    result = terminal_orders.api_post_terminal_order(
        urlparse("/api/terminal/order"),
        {"symbol": "spy", "side": "sell", "qty": 3},
        {},
    )

    assert result["ok"] is True
    assert result["symbol"] == "SPY"
    assert result["side"] == "SELL"
    assert result["qty"] == 3.0
    _assert_terminal_sell_contract(writes)


def test_terminal_order_accepts_paper_pipeline_gate_with_live_disabled(monkeypatch):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "1")
    monkeypatch.setenv("ENGINE_MODE", "paper")
    monkeypatch.setenv("EXECUTION_MODE", "paper")
    writes = _patch_terminal_order_storage(monkeypatch)
    monkeypatch.setattr(
        terminal_orders,
        "execution_gate_snapshot",
        lambda **_kwargs: {
            "real_trading_allowed": False,
            "allow_execution_pipeline": True,
            "allow_simulation": True,
            "allowed": True,
            "mode": "paper",
            "reason": "mode_paper",
        },
    )
    monkeypatch.setattr(
        terminal_orders,
        "_get_execution_mode",
        lambda: {"mode": "paper", "armed": 0, "source": "test"},
    )

    result = terminal_orders.api_post_terminal_order(
        urlparse("/api/terminal/order"),
        {"symbol": "spy", "side": "sell", "qty": 3},
        {},
    )

    assert result["ok"] is True
    assert result["symbol"] == "SPY"
    assert result["side"] == "SELL"
    assert result["qty"] == 3.0
    _assert_terminal_sell_contract(writes)


def test_terminal_flatten_accepts_paper_pipeline_gate(monkeypatch):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("ENGINE_MODE", "paper")
    writes = _FakeWriteConnection()
    monkeypatch.setattr(
        terminal_orders,
        "execution_gate_snapshot",
        lambda **_kwargs: {
            "real_trading_allowed": False,
            "allow_execution_pipeline": True,
            "allow_simulation": True,
            "mode": "paper",
            "reason": "mode_paper",
        },
    )
    monkeypatch.setattr(terminal_orders, "connect", lambda **_kwargs: _FakePositionReadConnection(2.0))
    monkeypatch.setattr(
        terminal_orders,
        "_table_exists",
        lambda _con, table: table in {"portfolio_orders", "broker_positions", "prices"},
    )
    monkeypatch.setattr(terminal_orders, "cache_invalidate_namespace", lambda _namespace: None)
    monkeypatch.setattr(terminal_orders, "run_write_txn", lambda fn, **_kwargs: fn(writes))

    result = terminal_orders.api_post_terminal_flatten(
        urlparse("/api/terminal/flatten"),
        {"symbol": "spy"},
        {},
    )

    assert result["ok"] is True
    assert result["symbol"] == "SPY"
    assert result["flatten_qty"] == 2.0
    assert len(writes.statements) == 1
    params = writes.statements[0][1]
    assert params[3] == "FLATTEN"
    assert params[4] == "LONG"
    assert params[5] == "FLAT"
    assert int(params[9]) > 0
    explain = json.loads(params[10])
    assert explain["terminal_order"]["flatten"] is True
    assert explain["terminal_order"]["side"] == "SELL"
    assert explain["terminal_order"]["qty"] == 2.0
    assert explain["terminal_order"]["source_alert_id"] == int(params[9])


def test_terminal_order_blocks_when_disable_live_execution_truthy(monkeypatch):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "true")
    monkeypatch.setenv("ENGINE_MODE", "live")
    gate = Mock(return_value={"real_trading_allowed": True})
    writes = _FakeWriteConnection()

    monkeypatch.setattr(terminal_orders, "execution_gate_snapshot", gate)
    monkeypatch.setattr(terminal_orders, "connect", lambda **_kwargs: _FakeReadConnection())
    monkeypatch.setattr(terminal_orders, "_table_exists", lambda _con, table: table == "portfolio_orders")
    monkeypatch.setattr(terminal_orders, "run_write_txn", lambda fn, **_kwargs: fn(writes))

    result = terminal_orders.api_post_terminal_order(
        urlparse("/api/terminal/order"),
        {"symbol": "spy", "side": "buy", "qty": 1},
        {},
    )

    assert result["ok"] is False
    assert result["error"] == "execution_blocked"
    assert result["gate"]["reason"] == "disable_live_execution_env"
    assert result["gate"]["real_trading_allowed"] is False
    assert writes.statements == []
    gate.assert_not_called()


def test_terminal_order_blocks_live_when_prelive_reconcile_disabled(monkeypatch):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("ENGINE_MODE", "live")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE", "0")
    gate = Mock(return_value={"real_trading_allowed": True})
    writes = _FakeWriteConnection()

    monkeypatch.setattr(terminal_orders, "execution_gate_snapshot", gate)
    monkeypatch.setattr(terminal_orders, "connect", lambda **_kwargs: _FakeReadConnection())
    monkeypatch.setattr(terminal_orders, "_table_exists", lambda _con, table: table == "portfolio_orders")
    monkeypatch.setattr(terminal_orders, "run_write_txn", lambda fn, **_kwargs: fn(writes))

    result = terminal_orders.api_post_terminal_order(
        urlparse("/api/terminal/order"),
        {"symbol": "spy", "side": "buy", "qty": 1},
        {},
    )

    assert result["ok"] is False
    assert result["error"] == "execution_blocked"
    assert result["gate"]["status"] == "prelive_reconcile_disabled_for_live"
    assert result["gate"]["prelive_reconcile_policy"]["enabled"] is False
    assert writes.statements == []
    gate.assert_not_called()


def _assert_rejection_recorded(writes, reason_code: str) -> None:
    joined = "\n".join(str(sql) for sql, _params in writes.statements)
    assert "terminal_intent_rejections" in joined
    assert any(reason_code in tuple(map(str, params)) for _sql, params in writes.statements)


def _portfolio_order_insert_count(writes) -> int:
    return sum(1 for sql, _params in writes.statements if "insert into portfolio_orders" in str(sql).lower())


def test_terminal_keyboard_shortcuts_use_visible_quantity_field():
    source = Path("ui/terminal/terminal.js").read_text(encoding="utf-8")

    assert 'submitTerminalOrder("BUY", currentOrderQty(), "Buy")' in source
    assert 'submitTerminalOrder("SELL", currentOrderQty(), "Sell")' in source
    assert 'submitTerminalOrder("BUY", 100' not in source
    assert 'submitTerminalOrder("SELL", 100' not in source


def test_terminal_order_rejects_stale_price_and_records_reason(monkeypatch):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("TERMINAL_PRICE_MAX_AGE_MS", "1000")
    writes = _patch_terminal_order_storage(monkeypatch)
    stale_ts = int(time.time() * 1000) - 10_000
    monkeypatch.setattr(terminal_orders, "connect", lambda **_kwargs: _FakeReadConnection(price=100.0, ts_ms=stale_ts))

    result = terminal_orders.api_post_terminal_order(
        urlparse("/api/terminal/order"),
        {"symbol": "spy", "side": "buy", "qty": 1},
        {},
    )

    assert result["ok"] is False
    assert result["error"] == "pre_trade_rejected"
    assert result["reason_code"] == "stale_price"
    _assert_rejection_recorded(writes, "stale_price")


def test_terminal_order_rejects_notional_cap_and_records_reason(monkeypatch):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("TERMINAL_MAX_NOTIONAL", "200")
    writes = _patch_terminal_order_storage(monkeypatch)

    result = terminal_orders.api_post_terminal_order(
        urlparse("/api/terminal/order"),
        {"symbol": "spy", "side": "buy", "qty": 3},
        {},
    )

    assert result["ok"] is False
    assert result["reason_code"] == "max_notional_exceeded"
    _assert_rejection_recorded(writes, "max_notional_exceeded")


def test_terminal_order_rejects_duplicate_recent_intent(monkeypatch):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("ENGINE_MODE", "safe")
    writes = _patch_terminal_order_storage(monkeypatch)
    monkeypatch.setattr(terminal_orders, "connect", lambda **_kwargs: _FakeReadConnection(duplicate=True))

    result = terminal_orders.api_post_terminal_order(
        urlparse("/api/terminal/order"),
        {"symbol": "spy", "side": "buy", "qty": 1},
        {},
    )

    assert result["ok"] is False
    assert result["reason_code"] == "duplicate_recent_order"
    _assert_rejection_recorded(writes, "duplicate_recent_order")


def test_terminal_order_rejects_per_symbol_qty_cap(monkeypatch):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("TERMINAL_SYMBOL_CAPS_JSON", json.dumps({"SPY": {"max_qty": 2}}))
    writes = _patch_terminal_order_storage(monkeypatch)

    result = terminal_orders.api_post_terminal_order(
        urlparse("/api/terminal/order"),
        {"symbol": "spy", "side": "buy", "qty": 3},
        {},
    )

    assert result["ok"] is False
    assert result["reason_code"] == "max_qty_exceeded"
    assert _portfolio_order_insert_count(writes) == 0
    _assert_rejection_recorded(writes, "max_qty_exceeded")


def test_terminal_order_requires_high_notional_threshold_confirmation(monkeypatch):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("TERMINAL_MAX_NOTIONAL", "1000")
    monkeypatch.setenv("TERMINAL_CONFIRM_NOTIONAL", "200")
    writes = _patch_terminal_order_storage(monkeypatch)

    result = terminal_orders.api_post_terminal_order(
        urlparse("/api/terminal/order"),
        {"symbol": "spy", "side": "buy", "qty": 3},
        {},
    )

    assert result["ok"] is False
    assert result["error"] == "confirmation_required"
    assert result["http_status"] == 422
    assert result["reason_code"] == "threshold_notional_confirmation_required"
    assert result["required_confirm"] == "HIGH_NOTIONAL"
    assert _portfolio_order_insert_count(writes) == 0
    _assert_rejection_recorded(writes, "threshold_notional_confirmation_required")


def test_terminal_order_allows_high_notional_after_threshold_confirmation(monkeypatch):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("TERMINAL_MAX_NOTIONAL", "1000")
    monkeypatch.setenv("TERMINAL_CONFIRM_NOTIONAL", "200")
    writes = _patch_terminal_order_storage(monkeypatch)

    result = terminal_orders.api_post_terminal_order(
        urlparse("/api/terminal/order"),
        {
            "symbol": "spy",
            "side": "buy",
            "qty": 3,
            "threshold_confirmation": "HIGH_NOTIONAL",
            "threshold_consequence_ack": True,
        },
        {},
    )

    assert result["ok"] is True
    assert result["estimated_notional"] == 300.0
    assert _portfolio_order_insert_count(writes) == 1


def test_terminal_flatten_requires_position_threshold_confirmation(monkeypatch):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("ENGINE_MODE", "paper")
    monkeypatch.setenv("TERMINAL_FLATTEN_CONFIRM_POSITION_QTY", "1")
    writes = _FakeWriteConnection()
    monkeypatch.setattr(
        terminal_orders,
        "execution_gate_snapshot",
        lambda **_kwargs: {
            "real_trading_allowed": False,
            "allow_execution_pipeline": True,
            "allow_simulation": True,
            "mode": "paper",
            "reason": "mode_paper",
        },
    )
    monkeypatch.setattr(terminal_orders, "connect", lambda **_kwargs: _FakePositionReadConnection(2.0))
    monkeypatch.setattr(
        terminal_orders,
        "_table_exists",
        lambda _con, table: table in {"portfolio_orders", "broker_positions", "prices"},
    )
    monkeypatch.setattr(terminal_orders, "cache_invalidate_namespace", lambda _namespace: None)
    monkeypatch.setattr(terminal_orders, "run_write_txn", lambda fn, **_kwargs: fn(writes))

    result = terminal_orders.api_post_terminal_flatten(
        urlparse("/api/terminal/flatten"),
        {"symbol": "spy"},
        {},
    )

    assert result["ok"] is False
    assert result["error"] == "confirmation_required"
    assert result["reason_code"] == "threshold_position_confirmation_required"
    assert result["required_confirm"] == "POSITION_LIMIT"
    assert _portfolio_order_insert_count(writes) == 0
    _assert_rejection_recorded(writes, "threshold_position_confirmation_required")


def test_terminal_flatten_allows_position_threshold_after_confirmation(monkeypatch):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("ENGINE_MODE", "paper")
    monkeypatch.setenv("TERMINAL_FLATTEN_CONFIRM_POSITION_QTY", "1")
    writes = _FakeWriteConnection()
    monkeypatch.setattr(
        terminal_orders,
        "execution_gate_snapshot",
        lambda **_kwargs: {
            "real_trading_allowed": False,
            "allow_execution_pipeline": True,
            "allow_simulation": True,
            "mode": "paper",
            "reason": "mode_paper",
        },
    )
    monkeypatch.setattr(terminal_orders, "connect", lambda **_kwargs: _FakePositionReadConnection(2.0))
    monkeypatch.setattr(
        terminal_orders,
        "_table_exists",
        lambda _con, table: table in {"portfolio_orders", "broker_positions", "prices"},
    )
    monkeypatch.setattr(terminal_orders, "cache_invalidate_namespace", lambda _namespace: None)
    monkeypatch.setattr(terminal_orders, "run_write_txn", lambda fn, **_kwargs: fn(writes))

    result = terminal_orders.api_post_terminal_flatten(
        urlparse("/api/terminal/flatten"),
        {
            "symbol": "spy",
            "threshold_confirmation": "POSITION_LIMIT",
            "threshold_consequence_ack": True,
        },
        {},
    )

    assert result["ok"] is True
    assert result["flatten_qty"] == 2.0
    assert _portfolio_order_insert_count(writes) == 1


def test_terminal_order_post_through_http_dispatcher_uses_json_body(monkeypatch, tmp_path):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE", "1")
    monkeypatch.setenv("ENV", "dev")
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("EXECUTION_MODE", "safe")
    monkeypatch.setenv("TS_API_ALLOW_LOCALHOST_MUTATIONS_WITHOUT_TOKEN", "1")
    writes = _patch_terminal_order_storage(monkeypatch)
    _patch_dispatcher_runtime_guards(monkeypatch)
    handler_cls = http_transport.build_handler(
        ROUTE_SPECS=[("POST", "/api/terminal/order", "api_post_terminal_order")],
        API_HANDLERS={"api_post_terminal_order": terminal_orders.api_post_terminal_order},
        dashboard_api_token="",
        ctx={},
        static_dir=str(tmp_path),
    )
    server = _TestHTTPServer(("127.0.0.1", 0), handler_cls)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        url = f"http://127.0.0.1:{server.server_port}/api/terminal/order"
        req = Request(
            url,
            data=json.dumps({
                "symbol": "spy",
                "side": "sell",
                "qty": 3,
                "confirm": "TRADE",
                "confirmation": "TRADE",
                "consequence_ack": True,
                "actor": "test",
                "source": "terminal_test",
            }).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urlopen(req, timeout=5) as response:
            result = json.loads(response.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    assert result["ok"] is True
    assert result["symbol"] == "SPY"
    assert result["side"] == "SELL"
    assert result["qty"] == 3.0
    _assert_terminal_sell_contract(writes)


def test_terminal_order_blocked_through_http_dispatcher_returns_structured_403(monkeypatch, tmp_path):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "true")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE", "1")
    monkeypatch.setenv("ENV", "dev")
    monkeypatch.setenv("ENGINE_MODE", "live")
    monkeypatch.setenv("EXECUTION_MODE", "live")
    monkeypatch.setenv("TS_API_ALLOW_LOCALHOST_MUTATIONS_WITHOUT_TOKEN", "1")
    dashboard_token = "dashboard-terminal-test-token-1234567890"
    writes = _FakeWriteConnection()
    gate = Mock(return_value={"real_trading_allowed": True})

    monkeypatch.setattr(terminal_orders, "execution_gate_snapshot", gate)
    monkeypatch.setattr(terminal_orders, "connect", lambda **_kwargs: _FakeReadConnection())
    monkeypatch.setattr(terminal_orders, "_table_exists", lambda _con, table: table == "portfolio_orders")
    monkeypatch.setattr(terminal_orders, "cache_invalidate_namespace", lambda _namespace: None)
    monkeypatch.setattr(terminal_orders, "run_write_txn", lambda fn, **_kwargs: fn(writes))
    _patch_dispatcher_runtime_guards(monkeypatch)
    handler_cls = http_transport.build_handler(
        ROUTE_SPECS=[("POST", "/api/terminal/order", "api_post_terminal_order")],
        API_HANDLERS={"api_post_terminal_order": terminal_orders.api_post_terminal_order},
        dashboard_api_token=dashboard_token,
        ctx={},
        static_dir=str(tmp_path),
    )
    server = _TestHTTPServer(("127.0.0.1", 0), handler_cls)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        url = f"http://127.0.0.1:{server.server_port}/api/terminal/order"
        req = Request(
            url,
            data=json.dumps({
                "symbol": "spy",
                "side": "buy",
                "qty": 1,
                "confirm": "TRADE",
                "confirmation": "TRADE",
                "consequence_ack": True,
                "actor": "test",
                "source": "terminal_test",
            }).encode("utf-8"),
            headers={"Content-Type": "application/json", "X-API-Token": dashboard_token},
            method="POST",
        )
        try:
            urlopen(req, timeout=5)
            raise AssertionError("request unexpectedly succeeded")
        except HTTPError as exc:
            code = exc.code
            result = json.loads(exc.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    assert code == 403
    assert result["ok"] is False
    assert result["error"] == "execution_blocked"
    assert result["reason_code"] == "disable_live_execution_env"
    assert result["meta"]["status"] == 403
    assert result["message"]
    assert result["gate"]["real_trading_allowed"] is False
    assert writes.statements == []
    gate.assert_not_called()


def test_terminal_snapshot_includes_execution_barrier(monkeypatch):
    monkeypatch.setattr(terminal_api, "api_get_terminal_watchlist", lambda *_args, **_kwargs: {"ok": True, "symbols": []})
    monkeypatch.setattr(terminal_api, "api_get_terminal_positions", lambda *_args, **_kwargs: {"ok": True, "rows": []})
    monkeypatch.setattr(terminal_api, "api_get_terminal_orders", lambda *_args, **_kwargs: {"ok": True, "data": {"broker": [], "portfolio": []}})
    monkeypatch.setattr(terminal_api, "api_get_terminal_fills", lambda *_args, **_kwargs: {"ok": True, "rows": []})
    monkeypatch.setattr(terminal_api, "api_get_terminal_equity", lambda *_args, **_kwargs: {"ok": True, "account": None, "series": []})
    monkeypatch.setattr(
        terminal_api,
        "execution_gate_snapshot",
        lambda **_kwargs: {
            "ok": True,
            "real_trading_allowed": False,
            "allowed": True,
            "allow_execution": False,
            "allow_execution_pipeline": True,
            "allow_simulation": True,
            "mode": "paper",
            "armed": 0,
            "reason": "mode_paper",
            "severity": "WARNING",
            "severity_reasons": ["mode_paper"],
            "ts_ms": 123456,
        },
    )

    result = terminal_api.api_get_terminal_snapshot(urlparse("/api/terminal/snapshot"), {})

    barrier = result["execution_barrier"]
    assert barrier["real_trading_allowed"] is False
    assert barrier["real_trading_blocked"] is True
    assert barrier["blocked"] is True
    assert barrier["execution_mode"] == "paper"
    assert barrier["gate_status"] == "mode_paper"
    assert barrier["allow_simulation"] is True
    assert barrier["updated_ts_ms"] == 123456
    assert "mode_paper" in barrier["blocking_reasons"]


def test_terminal_snapshot_price_reference_uses_prices_table_not_position_marks(monkeypatch):
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    wrapped = _NoCloseSQLiteConnection(con)
    now_ms = int(time.time() * 1000)
    con.execute("CREATE TABLE prices (symbol TEXT, price REAL, ts_ms INTEGER)")
    con.execute("INSERT INTO prices(symbol, price, ts_ms) VALUES ('SPY', 123.45, ?)", (now_ms - 250,))
    con.execute("CREATE TABLE broker_positions (symbol TEXT, market_px REAL, avg_px REAL, updated_ts_ms INTEGER)")
    con.execute(
        "INSERT INTO broker_positions(symbol, market_px, avg_px, updated_ts_ms) VALUES ('SPY', 999.0, 998.0, ?)",
        (now_ms,),
    )
    monkeypatch.setattr(terminal_api, "connect_ro", lambda: wrapped)
    monkeypatch.setattr(terminal_api, "api_get_terminal_watchlist", lambda *_args, **_kwargs: {"ok": True, "symbols": ["SPY"]})
    monkeypatch.setattr(terminal_api, "api_get_terminal_positions", lambda *_args, **_kwargs: {"ok": True, "rows": []})
    monkeypatch.setattr(terminal_api, "api_get_terminal_orders", lambda *_args, **_kwargs: {"ok": True, "data": {"broker": [], "portfolio": []}})
    monkeypatch.setattr(terminal_api, "api_get_terminal_fills", lambda *_args, **_kwargs: {"ok": True, "rows": []})
    monkeypatch.setattr(terminal_api, "api_get_terminal_equity", lambda *_args, **_kwargs: {"ok": True, "account": None, "series": []})
    monkeypatch.setattr(
        terminal_api,
        "execution_gate_snapshot",
        lambda **_kwargs: {"ok": True, "real_trading_allowed": True, "mode": "paper", "ts_ms": now_ms},
    )

    result = terminal_api.api_get_terminal_snapshot(urlparse("/api/terminal/snapshot?symbol=SPY"), {})

    price_ref = result["price_reference"]
    assert price_ref["ok"] is True
    assert price_ref["source"] == "prices"
    assert price_ref["symbol"] == "SPY"
    assert price_ref["price"] == 123.45
    assert price_ref["price"] != 999.0


def test_terminal_read_handlers_close_read_connections(monkeypatch):
    connections = []

    def fake_connect_ro():
        conn = _FakeClosableReadConnection()
        connections.append(conn)
        return conn

    monkeypatch.setattr(terminal_api, "connect_ro", fake_connect_ro)

    calls = [
        (terminal_api.api_get_terminal_watchlist, "/api/terminal/watchlist"),
        (terminal_api.api_get_terminal_positions, "/api/terminal/positions"),
        (terminal_api.api_get_terminal_orders, "/api/terminal/orders"),
        (terminal_api.api_get_terminal_fills, "/api/terminal/fills"),
        (terminal_api.api_get_terminal_equity, "/api/terminal/equity"),
        (terminal_api.api_get_terminal_markers, "/api/terminal/markers"),
        (terminal_api.api_get_terminal_markers, "/api/terminal/markers?symbol=SPY"),
        (terminal_api.api_get_terminal_decision_overlays, "/api/terminal/decision_overlays?symbol=SPY"),
    ]

    for handler, path in calls:
        handler(urlparse(path), {})

    assert len(connections) == len(calls)
    assert all(conn.closed for conn in connections)


def test_terminal_orders_include_rejected_intents_with_reason_codes(monkeypatch):
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    wrapped = _NoCloseSQLiteConnection(con)
    con.execute(
        """
        CREATE TABLE terminal_intent_rejections (
          id INTEGER PRIMARY KEY,
          ts_ms INTEGER NOT NULL,
          symbol TEXT NOT NULL,
          side TEXT,
          qty REAL,
          reason_code TEXT NOT NULL,
          reason TEXT NOT NULL,
          source TEXT NOT NULL,
          detail_json TEXT NOT NULL DEFAULT '{}'
        )
        """
    )
    con.execute(
        """
        INSERT INTO terminal_intent_rejections
        (id, ts_ms, symbol, side, qty, reason_code, reason, source, detail_json)
        VALUES (7, 123456, 'SPY', 'BUY', 10, 'max_notional_exceeded', 'Order exceeds max notional.', 'terminal', '{}')
        """
    )
    monkeypatch.setattr(terminal_api, "connect_ro", lambda: wrapped)

    payload = terminal_api.api_get_terminal_orders(urlparse("/api/terminal/orders"), {})

    rejected = payload["data"]["rejected"]
    assert len(rejected) == 1
    assert rejected[0]["id"] == 7
    assert rejected[0]["symbol"] == "SPY"
    assert rejected[0]["state"] == "REJECTED"
    assert rejected[0]["reason_code"] == "max_notional_exceeded"
    assert rejected[0]["reason"] == "Order exceeds max notional."
    assert rejected[0]["status_bucket"] == "rejected"
    assert rejected[0]["status_label"] == "Rejected"
    assert rejected[0]["rejection_reason_code"] == "max_notional_exceeded"
    assert payload["data"]["summary"]["rejected"] == 1
    assert payload["data"]["all"][0]["status_bucket"] == "rejected"


def test_terminal_orders_include_suppressed_intents_with_machine_reason(monkeypatch):
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    wrapped = _NoCloseSQLiteConnection(con)
    con.execute(
        """
        CREATE TABLE trade_attribution_ledger (
          id INTEGER PRIMARY KEY,
          ts_ms INTEGER NOT NULL,
          symbol TEXT NOT NULL,
          source_alert_id INTEGER,
          model_id TEXT,
          signal_json TEXT,
          execution_policy_json TEXT,
          decision_json TEXT,
          suppression_reason TEXT,
          expected_price REAL,
          fill_price REAL
        )
        """
    )
    con.execute(
        """
        INSERT INTO trade_attribution_ledger
        (id, ts_ms, symbol, source_alert_id, model_id, signal_json, execution_policy_json, decision_json, suppression_reason, expected_price, fill_price)
        VALUES (11, 223344, 'AAPL', 99, 'model-a', ?, '{}', ?, 'max_position', 101.25, NULL)
        """,
        (
            json.dumps({"side": "BUY", "qty": 15}),
            json.dumps({"blocked_by": "max_position"}),
        ),
    )
    monkeypatch.setattr(terminal_api, "connect_ro", lambda: wrapped)

    payload = terminal_api.api_get_terminal_orders(urlparse("/api/terminal/orders"), {})

    suppressed = payload["data"]["suppressed"]
    assert len(suppressed) == 1
    row = suppressed[0]
    assert row["symbol"] == "AAPL"
    assert row["status_bucket"] == "suppressed"
    assert row["reason_code"] == "max_position"
    assert row["suppression_reason"] == "max_position"
    assert row["expected_price"] == 101.25
    assert row["lineage_ids"]["source_alert_id"] == 99
    assert payload["data"]["summary"]["suppressed"] == 1


def test_terminal_fills_aggregate_partial_fills_with_tca_and_child_detail(monkeypatch):
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    wrapped = _NoCloseSQLiteConnection(con)
    con.execute(
        """
        CREATE TABLE execution_fills (
          id INTEGER PRIMARY KEY,
          client_order_id TEXT NOT NULL,
          fill_id TEXT,
          broker TEXT,
          symbol TEXT,
          portfolio_orders_id INTEGER,
          source_alert_id INTEGER,
          prediction_id INTEGER,
          ts_ms INTEGER,
          submit_ts_ms INTEGER,
          fill_ts_ms INTEGER,
          fill_qty REAL,
          fill_px REAL,
          expected_px REAL,
          mid_px REAL,
          spread_bps REAL,
          slippage_bps REAL,
          fees REAL,
          extra_json TEXT
        )
        """
    )
    con.execute(
        """
        INSERT INTO execution_fills
        (id, client_order_id, fill_id, broker, symbol, portfolio_orders_id, source_alert_id, prediction_id, ts_ms, submit_ts_ms, fill_ts_ms, fill_qty, fill_px, expected_px, mid_px, spread_bps, slippage_bps, fees, extra_json)
        VALUES (1, 'cid-1', 'fill-a', 'sim', 'AAPL', 21, 31, 41, 1000, 900, 1000, 4, 100.0, 99.8, 99.9, 2.0, 10.0, 0.10, ?)
        """,
        (json.dumps({"implementation_shortfall_bps": 12.0}),),
    )
    con.execute(
        """
        INSERT INTO execution_fills
        (id, client_order_id, fill_id, broker, symbol, portfolio_orders_id, source_alert_id, prediction_id, ts_ms, submit_ts_ms, fill_ts_ms, fill_qty, fill_px, expected_px, mid_px, spread_bps, slippage_bps, fees, extra_json)
        VALUES (2, 'cid-1', 'fill-b', 'sim', 'AAPL', 21, 31, 41, 1100, 900, 1100, 6, 101.0, 99.8, 99.9, 2.0, 20.0, 0.20, ?)
        """,
        (json.dumps({"implementation_shortfall_bps": 18.0}),),
    )
    monkeypatch.setattr(terminal_api, "connect_ro", lambda: wrapped)

    payload = terminal_api.api_get_terminal_fills(urlparse("/api/terminal/fills"), {})

    assert payload["ok"] is True
    assert payload["meta"]["source_table"] == "execution_fills"
    assert len(payload["raw_rows"]) == 2
    assert len(payload["rows"]) == 1
    row = payload["rows"][0]
    assert row["client_order_id"] == "cid-1"
    assert row["status_bucket"] == "partial"
    assert row["child_fill_count"] == 2
    assert len(row["child_fills"]) == 2
    assert row["qty"] == 10.0
    assert row["fill_vwap"] == pytest.approx(100.6)
    assert row["slippage_bps"] == pytest.approx(16.0)
    assert row["implementation_shortfall_bps"] == pytest.approx(15.6)
    assert payload["summary"]["partial_orders"] == 1
    assert payload["summary"]["avg_slippage_bps"] == pytest.approx(16.0)


def test_terminal_decision_overlays_explain_traded_and_not_traded_decisions(monkeypatch):
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    wrapped = _NoCloseSQLiteConnection(con)
    now = 1_789_500_000_000

    con.executescript(
        """
        CREATE TABLE broker_fills (
          ts_ms INTEGER,
          symbol TEXT,
          qty REAL,
          px REAL,
          source_order_id INTEGER,
          explain_json TEXT
        );
        CREATE TABLE portfolio_orders (
          id INTEGER,
          ts_ms INTEGER,
          symbol TEXT,
          action TEXT,
          from_side TEXT,
          to_side TEXT,
          delta_weight REAL,
          to_weight REAL,
          source_alert_id INTEGER,
          explain_json TEXT
        );
        CREATE TABLE trade_attribution_ledger (
          id INTEGER,
          ts_ms INTEGER,
          source_alert_id INTEGER,
          model_id TEXT,
          symbol TEXT,
          signal_json TEXT,
          execution_policy_json TEXT,
          suppression_reason TEXT,
          decision_json TEXT,
          expected_price REAL,
          fill_price REAL
        );
        CREATE TABLE execution_orders (
          client_order_id TEXT,
          submit_ts_ms INTEGER,
          symbol TEXT,
          qty REAL,
          ref_px REAL,
          expected_px REAL,
          mid_px REAL,
          source_alert_id INTEGER,
          extra_json TEXT
        );
        CREATE TABLE broker_positions (
          symbol TEXT,
          qty REAL,
          avg_px REAL,
          updated_ts_ms INTEGER
        );
        CREATE TABLE kill_switch_state (
          scope TEXT,
          key TEXT,
          enabled INTEGER,
          reason TEXT,
          actor TEXT,
          meta_json TEXT,
          created_ts_ms INTEGER,
          updated_ts_ms INTEGER
        );
        CREATE TABLE trade_suppression_audit (
          ts_ms INTEGER,
          state TEXT,
          action TEXT,
          reason TEXT,
          hard_block INTEGER
        );
        CREATE TABLE portfolio_risk_snapshots (
          ts_ms INTEGER,
          blocked INTEGER,
          drawdown REAL,
          info_json TEXT
        );
        CREATE TABLE risk_events (
          ts_ms INTEGER,
          trigger_type TEXT,
          reason TEXT
        );
        """
    )
    con.execute(
        "INSERT INTO broker_fills VALUES (?, ?, ?, ?, ?, ?)",
        (now, "SPY", 10.0, 101.0, 7, json.dumps({"entry_price": 101.0})),
    )
    con.execute(
        "INSERT INTO portfolio_orders VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            11,
            now + 1_000,
            "SPY",
            "BUY",
            None,
            "BUY",
            0.25,
            0.50,
            101,
            json.dumps({"entry_price": 100.5, "stop_loss_px": 98.0, "take_profit_px": 105.0, "max_risk_px": 97.5}),
        ),
    )
    con.execute(
        "INSERT INTO trade_attribution_ledger VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            21,
            now + 2_000,
            102,
            "baseline",
            "SPY",
            json.dumps({"side": "BUY", "qty": 2, "entry_price": 100.25}),
            json.dumps({}),
            "ttl_expired",
            json.dumps({}),
            100.25,
            None,
        ),
    )
    con.execute(
        "INSERT INTO trade_attribution_ledger VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            22,
            now + 3_000,
            103,
            "baseline",
            "SPY",
            json.dumps({"side": "SELL", "qty": -1, "entry_price": 99.75}),
            json.dumps({"blocked_by": "kill_switch"}),
            "kill_switch_db_global",
            json.dumps({"blocked_by": "kill_switch"}),
            99.75,
            None,
        ),
    )
    con.execute(
        "INSERT INTO execution_orders VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "risk-cap-1",
            now + 4_000,
            "SPY",
            5.0,
            102.0,
            102.25,
            102.1,
            104,
            json.dumps({"portfolio_risk_caps": {"scaled": True, "scale": 0.5, "caps": {"symbol_concentration_cap": 1000.0}}}),
        ),
    )
    con.execute("INSERT INTO broker_positions VALUES (?, ?, ?, ?)", ("SPY", 15.0, 99.5, now + 5_000))
    con.execute(
        "INSERT INTO kill_switch_state VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        ("global", "all", 1, "manual_test", "operator", json.dumps({}), now + 6_000, now + 6_000),
    )
    con.execute(
        "INSERT INTO trade_suppression_audit VALUES (?, ?, ?, ?, ?)",
        (now + 7_000, "SOFT_THROTTLE", "ENTER", "drawdown throttle", 0),
    )
    con.execute(
        "INSERT INTO portfolio_risk_snapshots VALUES (?, ?, ?, ?)",
        (now + 8_000, 1, 0.12, json.dumps({"block_reason": {"type": "drawdown_throttle"}})),
    )
    con.execute(
        "INSERT INTO risk_events VALUES (?, ?, ?)",
        (now + 9_000, "circuit_breaker", "daily drawdown circuit"),
    )
    con.commit()

    monkeypatch.setattr(terminal_api, "connect_ro", lambda: wrapped)

    payload = terminal_api.api_get_terminal_decision_overlays(
        urlparse("/api/terminal/decision_overlays?symbol=SPY"),
        {},
    )

    assert payload["ok"] is True
    assert payload["symbol"] == "SPY"
    marker_kinds = {m["kind"] for m in payload["markers"]}
    assert {"filled", "intended", "suppressed", "blocked", "risk_capped"}.issubset(marker_kinds)

    reason_codes = {m["reason_code"] for m in payload["markers"]}
    assert {"fill_executed", "portfolio_intent", "ttl_expired", "kill_switch_db_global", "portfolio_risk_cap_scaled"}.issubset(reason_codes)

    price_line_kinds = {line["kind"] for line in payload["price_lines"]}
    assert {"average_cost", "entry", "stop", "take_profit", "max_risk", "cap"}.issubset(price_line_kinds)

    window_kinds = {w["kind"] for w in payload["windows"]}
    assert {"kill_switch_window", "suppression_window", "drawdown_throttle_window", "circuit_breaker_window"}.issubset(window_kinds)
    assert payload["meta"]["markers_count"] == len(payload["markers"])
    assert "ttl_expired" in payload["meta"]["reason_codes"]

    legacy = terminal_api.api_get_terminal_markers(urlparse("/api/terminal/markers?symbol=SPY"), {})
    assert legacy["ok"] is True
    assert "price_lines" in legacy
    assert "windows" in legacy


def test_terminal_ui_gates_order_controls_from_execution_barrier():
    root = Path(__file__).resolve().parents[1]
    html = (root / "ui" / "terminal" / "terminal.html").read_text(encoding="utf-8")
    js = (root / "ui" / "terminal" / "terminal.js").read_text(encoding="utf-8")

    assert 'id="tradingSafetyStatus"' in html
    assert 'id="terminalArmChk"' in html
    assert "execution_barrier" in js
    assert "realTradingAllowed" in js
    assert "setOrderEntryEnabled(false, title)" in js
    assert "setFlattenEnabled(false, title)" in js
    assert "Flatten cannot be sent by keypress" in js
    assert "pointerdown" in js and "startFlattenHold" in js
    assert "confirmation_token" in js
    assert "confirmation_method" in js
    assert "source_surface: \"terminal\"" in js
    assert "actionId = normalizedToken === \"FLATTEN\" ? \"terminal.flatten\" : \"terminal.order\"" in js
    assert "request_id: requestId(actionId.replace" in js


def test_terminal_quantity_contract_survives_intent_weight_normalization():
    assert _terminal_signed_qty(
        {"terminal_order": {"sizing": "quantity", "side": "BUY", "qty": 2}}
    ) == 2.0
    assert _terminal_signed_qty(
        {"terminal_order": {"sizing": "quantity", "side": "SELL", "qty": 2}}
    ) == -2.0
    assert _terminal_signed_qty(
        {"terminal_order": {"sizing": "quantity", "side": "BUY", "qty": 2, "signed_qty": -5}}
    ) == -5.0


def test_terminal_quantity_contract_loads_as_explicit_qty_with_neutral_weights(monkeypatch):
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("EXECUTION_MODE", "safe")
    monkeypatch.setenv("BROKER", "sim")
    monkeypatch.setenv("BROKER_NAME", "sim")
    monkeypatch.setenv("LIVE_BROKER", "sim")
    monkeypatch.setattr(execution_intents, "DEFAULT_DECISION_ENGINE", None)
    monkeypatch.setattr(execution_intents, "get_competition_policy_for_intent", lambda **_kwargs: {})

    con = sqlite3.connect(":memory:")
    try:
        con.execute(
            """
            CREATE TABLE portfolio_orders (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts_ms INTEGER NOT NULL,
              model_id TEXT NOT NULL,
              symbol TEXT NOT NULL,
              action TEXT NOT NULL,
              from_side TEXT NOT NULL,
              to_side TEXT NOT NULL,
              from_weight REAL NOT NULL,
              to_weight REAL NOT NULL,
              delta_weight REAL NOT NULL,
              source_alert_id INTEGER,
              explain_json TEXT
            )
            """
        )
        con.execute(
            """
            INSERT INTO portfolio_orders (
              ts_ms, model_id, symbol, action, from_side, to_side,
              from_weight, to_weight, delta_weight, source_alert_id, explain_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(time.time() * 1000),
                "baseline",
                "SPY",
                "SELL",
                "FLAT",
                "SHORT",
                0.0,
                0.0,
                0.0,
                None,
                terminal_orders._terminal_explain("SPY", "SELL", 3),
            ),
        )
        con.commit()

        result = execution_intents.load_latest_execution_intents(con)
    finally:
        con.close()

    assert result["ok"] is True
    assert len(result["intents"]) == 1
    intent = result["intents"][0]
    assert intent["terminal_order"] is True
    assert intent["order_sizing"] == "quantity"
    assert intent["qty"] == -3.0
    assert intent["from_weight"] == 0.0
    assert intent["to_weight"] == 0.0
    assert intent["delta_weight"] == 0.0
