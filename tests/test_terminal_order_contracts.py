from __future__ import annotations

import json
import threading
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import engine.api.http_transport as http_transport
from engine.strategy.portfolio_execution_intents import _terminal_signed_qty
from engine.terminal.api import api_terminal as terminal_api
from engine.terminal.api import api_terminal_orders as terminal_orders


class _TestHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True


class _FakeReadConnection:
    def close(self) -> None:
        pass


class _FakeWriteConnection:
    def __init__(self) -> None:
        self.statements = []

    def execute(self, sql, params=()):
        self.statements.append((sql, params))
        return self


def _patch_terminal_order_storage(monkeypatch):
    writes = _FakeWriteConnection()

    monkeypatch.setattr(
        terminal_orders,
        "execution_gate_snapshot",
        lambda: {"real_trading_allowed": True},
    )
    monkeypatch.setattr(terminal_orders, "connect", lambda **_kwargs: _FakeReadConnection())
    monkeypatch.setattr(terminal_orders, "_table_exists", lambda _con, table: table == "portfolio_orders")
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
    assert params[8] == 0.0

    explain = json.loads(params[10])
    assert explain["terminal_order"]["sizing"] == "quantity"
    assert explain["terminal_order"]["symbol"] == "SPY"
    assert explain["terminal_order"]["side"] == "SELL"
    assert explain["terminal_order"]["qty"] == 3.0
    assert explain["terminal_order"]["signed_qty"] == -3.0


def test_terminal_order_accepts_dispatcher_body_and_records_quantity_contract(monkeypatch):
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


def test_terminal_order_post_through_http_dispatcher_uses_json_body(monkeypatch, tmp_path):
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
            data=json.dumps({"symbol": "spy", "side": "sell", "qty": 3}).encode("utf-8"),
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


def test_terminal_snapshot_includes_execution_barrier(monkeypatch):
    monkeypatch.setattr(terminal_api, "api_get_terminal_watchlist", lambda *_args, **_kwargs: {"ok": True, "symbols": []})
    monkeypatch.setattr(terminal_api, "api_get_terminal_positions", lambda *_args, **_kwargs: {"ok": True, "rows": []})
    monkeypatch.setattr(terminal_api, "api_get_terminal_orders", lambda *_args, **_kwargs: {"ok": True, "data": {"broker": [], "portfolio": []}})
    monkeypatch.setattr(terminal_api, "api_get_terminal_fills", lambda *_args, **_kwargs: {"ok": True, "rows": []})
    monkeypatch.setattr(terminal_api, "api_get_terminal_equity", lambda *_args, **_kwargs: {"ok": True, "account": None, "series": []})
    monkeypatch.setattr(
        terminal_api,
        "execution_gate_snapshot",
        lambda: {
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
