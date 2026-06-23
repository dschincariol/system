"""Characterization tests for the execution-ledger decomposition facade."""

from __future__ import annotations

import importlib
import inspect
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


@pytest.fixture()
def ledger_stack(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "execution_ledger_contract.db"))
    monkeypatch.setenv("TS_STORAGE_BACKEND", "sqlite")
    monkeypatch.setenv("TS_TESTING", "1")
    monkeypatch.setenv("TIMESCALE_ENABLED", "0")
    monkeypatch.setenv("TS_SECRETS_PROVIDER", "plaintext")
    monkeypatch.setenv("TRADING_LOGS", str(tmp_path / "logs"))
    monkeypatch.setenv("TRADING_DATA", str(tmp_path / "data"))

    storage, ledger = _reload_modules(
        "engine.runtime.db_guard",
        "engine.runtime.storage",
        "engine.execution.execution_ledger",
    )[1:]
    storage.init_db()
    ledger.init_execution_ledger()
    try:
        yield storage, ledger
    finally:
        storage.close_pooled_connections()


def test_execution_ledger_facade_signatures_and_schema_contract(ledger_stack):
    storage, ledger = ledger_stack

    expected_signatures = {
        "init_execution_ledger": "() -> None",
        "log_submit": "(client_order_id: str, broker: str, symbol: str, qty: float, submit_ts_ms: int, ref_px: Optional[float] = None, broker_order_id: Optional[str] = None, portfolio_orders_id: Optional[int] = None, source_alert_id: Optional[int] = None, extra: Optional[Dict[str, Any]] = None, expected_px: Optional[float] = None, mid_px: Optional[float] = None, bid_px: Optional[float] = None, ask_px: Optional[float] = None, spread_bps: Optional[float] = None, order_uid: Optional[str] = None, idempotency_status: Optional[str] = None, con=None) -> None",
        "log_fill": "(*args, **kwargs) -> None",
        "_log_fill_v1": "(client_order_id: str, fill_ts_ms: int, fill_qty: float, fill_px: float, fees: Optional[float] = None, liquidity: Optional[str] = None, raw: Optional[Dict[str, Any]] = None, con=None) -> None",
        "_log_fill_v2": "(client_order_id: str, fill_id: str, broker: str, symbol: str, qty: float, fill_px: float, fill_ts_ms: int, fees: Optional[float] = None, extra: Optional[Dict[str, Any]] = None, con=None) -> None",
        "audit_execution_integrity": "(*, model_id: Optional[str] = None, con=None) -> Dict[str, Any]",
        "compute_metrics_snapshot": "(limit_orders: int = 500) -> Dict[str, Any]",
        "compute_pnl_attribution_snapshot": "(lookback_orders: int = 500) -> Dict[str, Any]",
        "repair_execution_order_model_identity": "(limit: int = 5000) -> Dict[str, Any]",
        "_safe_json_dict": "(v: Any) -> Dict[str, Any]",
        "_safe_json_obj": "(v: Any) -> Dict[str, Any]",
        "_safe_float": "(value: Any, default: float = 0.0) -> float",
        "_safe_int": "(value: Any, default: int = 0) -> int",
        "_pick_float": "(*vals: Any) -> Optional[float]",
        "_extract_strategy_name": "(extra_payload: Any) -> Optional[str]",
        "_normalize_model_id": "(model_id: Any) -> str",
        "_extract_model_identity": "(extra_payload: Any) -> Dict[str, Any]",
        "_trade_outcome_label": "(pnl_value: float) -> str",
    }
    for name, expected in expected_signatures.items():
        assert hasattr(ledger, name), name
        assert str(inspect.signature(getattr(ledger, name))) == expected

    con = storage.connect(readonly=True)
    try:
        tables = {
            str(row[0])
            for row in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        indexes = {
            str(row[0])
            for row in con.execute(
                "SELECT name FROM sqlite_master WHERE type='index'"
            ).fetchall()
        }
    finally:
        con.close()

    assert {
        "execution_orders",
        "execution_fills",
        "execution_metrics",
        "pnl_attribution",
        "model_position_state",
    }.issubset(tables)
    assert {
        "idx_execution_orders_model_submit_ts",
        "idx_execution_fills_model_ts",
        "idx_execution_metrics_ts",
        "idx_pnl_attribution_ts",
        "uq_execution_fills_client_fillid",
    }.issubset(indexes)


def test_serialization_identity_and_label_helpers_lock_current_behavior(ledger_stack):
    _, ledger = ledger_stack

    payload = {
        "meta": {
            "model": {
                "id": " m42 ",
                "name": "Model 42",
                "kind": "gbm",
                "trained_ts_ms": "1700",
                "version": "v3",
            }
        },
        "regime_label": "risk_on",
        "horizon": "60",
        "explain": {"reason": {"strategy_alloc": {"mean_reversion": 1.0}}},
    }

    assert ledger._safe_json_dict({"a": 1}) == {"a": 1}
    assert ledger._safe_json_dict('{"a":1}') == {"a": 1}
    assert ledger._safe_json_obj("[1,2]") == {}
    assert ledger._safe_float("bad", 2.5) == 2.5
    assert ledger._safe_int("bad", 7) == 7
    assert ledger._normalize_model_id("  ") == "baseline"
    assert ledger._normalize_model_id(" m1 ") == "m1"
    assert ledger._trade_outcome_label(1.0) == "win"
    assert ledger._trade_outcome_label(-0.01) == "loss"
    assert ledger._trade_outcome_label(0.0) == "flat"

    with patch.object(ledger, "_warn_nonfatal") as warn_nonfatal:
        assert ledger._safe_json_dict("{bad json") == {}
        assert ledger._pick_float(None, "", "bad", "4.5") == 4.5
    assert warn_nonfatal.call_count == 2

    assert ledger._extract_strategy_name(payload) == "mean_reversion"
    assert (
        ledger._extract_strategy_name(
            {"execution": {"strategy_alloc": {"momentum": 0.8}}}
        )
        == "momentum"
    )
    assert ledger._extract_model_identity(payload) == {
        "regime": "risk_on",
        "horizon_s": 60,
        "model_id": "m42",
        "model_name": "Model 42",
        "model_kind": "gbm",
        "model_ts_ms": 1700,
        "model_version": "v3",
    }


def test_facade_serialization_helpers_delegate_to_extracted_module(
    ledger_stack, monkeypatch
):
    _, ledger = ledger_stack
    serialization = ledger._ledger_serialization

    calls = []

    def _record(name, value):
        calls.append((name, value))
        return value

    monkeypatch.setattr(
        serialization,
        "trade_outcome_label",
        lambda pnl_value: _record("trade_outcome_label", f"label:{pnl_value}"),
    )
    monkeypatch.setattr(
        serialization,
        "safe_json_dict",
        lambda v, *, warn_nonfatal=None: _record(
            "safe_json_dict",
            {"value": v, "warn_bound": warn_nonfatal is ledger._warn_nonfatal},
        ),
    )
    monkeypatch.setattr(
        serialization,
        "safe_json_obj",
        lambda v, *, warn_nonfatal=None: _record(
            "safe_json_obj",
            {"value": v, "warn_bound": warn_nonfatal is ledger._warn_nonfatal},
        ),
    )
    monkeypatch.setattr(
        serialization,
        "safe_float",
        lambda value, default=0.0: _record("safe_float", float(default) + 1.0),
    )
    monkeypatch.setattr(
        serialization,
        "safe_int",
        lambda value, default=0: _record("safe_int", int(default) + 1),
    )
    monkeypatch.setattr(
        serialization,
        "pick_float",
        lambda *vals, warn_nonfatal=None: _record(
            "pick_float",
            (vals, warn_nonfatal is ledger._warn_nonfatal),
        ),
    )
    monkeypatch.setattr(
        serialization,
        "extract_strategy_name",
        lambda extra_payload, *, warn_nonfatal=None: _record(
            "extract_strategy_name",
            f"strategy:{warn_nonfatal is ledger._warn_nonfatal}",
        ),
    )
    monkeypatch.setattr(
        serialization,
        "normalize_model_id",
        lambda model_id: _record("normalize_model_id", f"model:{model_id}"),
    )
    monkeypatch.setattr(
        serialization,
        "extract_model_identity",
        lambda extra_payload, *, warn_nonfatal=None: _record(
            "extract_model_identity",
            {"warn_bound": warn_nonfatal is ledger._warn_nonfatal},
        ),
    )

    assert ledger._trade_outcome_label(2.0) == "label:2.0"
    assert ledger._safe_json_dict("x") == {"value": "x", "warn_bound": True}
    assert ledger._safe_json_obj("y") == {"value": "y", "warn_bound": True}
    assert ledger._safe_float("bad", 2.0) == 3.0
    assert ledger._safe_int("bad", 2) == 3
    assert ledger._pick_float("a", "b") == (("a", "b"), True)
    assert ledger._extract_strategy_name({}) == "strategy:True"
    assert ledger._normalize_model_id("m") == "model:m"
    assert ledger._extract_model_identity({}) == {"warn_bound": True}
    assert [name for name, _ in calls] == [
        "trade_outcome_label",
        "safe_json_dict",
        "safe_json_obj",
        "safe_float",
        "safe_int",
        "pick_float",
        "extract_strategy_name",
        "normalize_model_id",
        "extract_model_identity",
    ]


def test_log_submit_idempotent_upsert_preserves_durable_order_state(ledger_stack):
    storage, ledger = ledger_stack

    ledger.log_submit(
        client_order_id="cid-submit",
        broker="sim",
        symbol="AAPL",
        qty=10.0,
        submit_ts_ms=1_700_000_000_000,
        ref_px=100.0,
        broker_order_id="broker-1",
        extra={
            "strategy_name": "mean_reversion",
            "model_id": "model-a",
            "model_version": "v1",
            "bid_px": 99.9,
            "ask_px": 100.1,
        },
        order_uid="order-uid-1",
        idempotency_status="claimed",
    )
    ledger.log_submit(
        client_order_id="cid-submit",
        broker="sim2",
        symbol="AAPL",
        qty=12.0,
        submit_ts_ms=1_700_000_000_500,
        ref_px=None,
        broker_order_id=None,
        extra={"model_id": "model-b"},
    )

    con = storage.connect(readonly=True)
    try:
        rows = con.execute(
            """
            SELECT client_order_id, broker, qty, submit_ts_ms, ref_px, broker_order_id,
                   order_uid, idempotency_status, model_id, model_version, status,
                   extra_json
            FROM execution_orders
            WHERE client_order_id='cid-submit'
            """
        ).fetchall()
    finally:
        con.close()

    assert len(rows) == 1
    row = rows[0]
    assert row[:11] == (
        "cid-submit",
        "sim2",
        12.0,
        1_700_000_000_500,
        100.0,
        "broker-1",
        "order-uid-1",
        "claimed",
        "model-b",
        "v1",
        "submitted",
    )
    extra = json.loads(row[11])
    assert extra["model_id"] == "model-b"
    assert extra["symbol"] == "AAPL"
    assert extra["submit_ts_ms"] == 1_700_000_000_500
    assert extra["ref_px"] is None


def test_log_submit_trace_reuses_supplied_transaction_connection(ledger_stack, monkeypatch):
    storage, ledger = ledger_stack
    traced_connections = []

    def fake_trace_event(*args, **kwargs):
        traced_connections.append(kwargs.get("con"))
        return {"trace_id": "trace", "span_id": "span"}

    monkeypatch.setattr(ledger, "trace_event", fake_trace_event)
    con = storage.connect()
    try:
        ledger.log_submit(
            client_order_id="cid-trace-con",
            broker="sim",
            symbol="AAPL",
            qty=1.0,
            submit_ts_ms=1_700_000_000_000,
            con=con,
        )
    finally:
        con.close()

    assert traced_connections == [con]


def test_fill_before_submit_recovery_and_idempotent_replay(ledger_stack):
    storage, ledger = ledger_stack

    with patch.object(
        ledger, "record_live_fill_attribution", return_value={"ok": True}
    ):
        ledger.log_fill(
            client_order_id="cid-fill-first",
            fill_id="fill-1",
            broker="sim",
            symbol="MSFT",
            qty=3.0,
            fill_px=50.25,
            fill_ts_ms=1_700_000_001_000,
            fees=0.15,
            extra={
                "model_id": "late-model",
                "model_version": "late-v1",
                "expected_px": 50.0,
                "mid_px": 50.1,
                "bid_px": 50.0,
                "ask_px": 50.2,
            },
        )
        ledger.log_fill(
            client_order_id="cid-fill-first",
            fill_id="fill-1",
            broker="sim",
            symbol="MSFT",
            qty=3.0,
            fill_px=50.25,
            fill_ts_ms=1_700_000_001_000,
            fees=0.15,
            extra={"model_id": "late-model"},
        )

    con = storage.connect(readonly=True)
    try:
        order = con.execute(
            """
            SELECT idempotency_status, status, model_id, model_version, symbol, qty,
                   submit_ts_ms, expected_px, mid_px, spread_bps, extra_json
            FROM execution_orders
            WHERE client_order_id='cid-fill-first'
            """
        ).fetchone()
        fills = con.execute(
            """
            SELECT fill_id, broker, model_id, model_version, symbol, fill_qty, fill_px,
                   expected_px, mid_px, slippage_bps, fill_latency_ms
            FROM execution_fills
            WHERE client_order_id='cid-fill-first'
            """
        ).fetchall()
    finally:
        con.close()

    assert order is not None
    assert order[:7] == (
        "fill_before_submit",
        "fill_pending_submit",
        "late-model",
        "late-v1",
        "MSFT",
        3.0,
        1_700_000_001_000,
    )
    assert float(order[7]) == 50.0
    assert float(order[8]) == 50.1
    assert round(float(order[9]), 6) == round(((50.2 - 50.0) / 50.1) * 10000.0, 6)
    placeholder_extra = json.loads(order[10])
    assert placeholder_extra["missing_local_order_reference"] is True
    assert placeholder_extra["fill_arrived_before_submit"] is True

    assert len(fills) == 1
    fill = fills[0]
    assert fill[:7] == (
        "fill-1",
        "sim",
        "late-model",
        "late-v1",
        "MSFT",
        3.0,
        50.25,
    )
    assert float(fill[7]) == 50.0
    assert float(fill[8]) == 50.1
    assert round(float(fill[9]), 6) == round(((50.25 - 50.0) / 50.0) * 10000.0, 6)
    assert int(fill[10]) == 0
