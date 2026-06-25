from __future__ import annotations

import json
import uuid
from types import SimpleNamespace
from typing import Any, Dict, List

import pytest


pytestmark = pytest.mark.safety_critical


class _FakeCon:
    def close(self) -> None:
        return None


class _FakeApp:
    def __init__(self) -> None:
        self.place_order_called = False

    def placeOrder(self, *_args: Any, **_kwargs: Any) -> None:
        self.place_order_called = True
        raise AssertionError("real IBKR placeOrder must not be called")

    def disconnect(self) -> None:
        return None


def _patch_ibkr_common(monkeypatch, mod: Any, *, asset_class: str) -> Dict[str, Any]:
    captured: Dict[str, Any] = {"orders": [], "audit_payloads": []}

    monkeypatch.setenv("EXEC_USE_SHARE_ROUNDING", "1")
    monkeypatch.setenv("EXEC_IBKR_SHARE_INCREMENT", "1")
    monkeypatch.setenv("EXEC_EQUITY_MIN_NOTIONAL_USD", "1")
    monkeypatch.setattr(mod, "connect", lambda: _FakeCon())
    monkeypatch.setattr(mod, "_real_trading_gate", lambda: {"ok": True, "real_trading_allowed": True})
    monkeypatch.setattr(mod, "_ibkr_credentials_block", lambda require_explicit=True: None)
    monkeypatch.setattr(mod, "_prelive_reconcile_or_block", lambda _broker: None)
    monkeypatch.setattr(mod, "apply_alpha_lifecycle", lambda **kwargs: (list(kwargs.get("orders") or []), {"ok": True}))
    monkeypatch.setattr(mod, "live_options_order_block", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(mod, "get_state", lambda *_args, **_kwargs: "0")
    monkeypatch.setattr(mod, "set_state", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(mod, "execution_allowed", lambda **_kwargs: (True, None, None))
    monkeypatch.setattr(mod, "compute_deployable_equity_from_env", lambda *_args, **_kwargs: 100000.0)
    monkeypatch.setattr(mod, "get_positions_live", lambda: [])
    monkeypatch.setattr(mod, "_load_latest_prices", lambda _con: {"AAPL": 100.0, "EURUSD": 100.0})
    monkeypatch.setattr(mod, "_connect_ib", lambda: _FakeApp())
    monkeypatch.setattr(mod, "_price_at_or_before", lambda _con, _symbol, _ts_ms: 100.0)
    monkeypatch.setattr(
        mod,
        "_apply_execution_risk_caps",
        lambda **kwargs: (float(kwargs["delta_qty"]), {"applied": False}),
    )
    monkeypatch.setattr(mod, "futures_order_block", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(mod, "_is_futures_symbol", lambda _symbol: False)
    monkeypatch.setattr(
        mod,
        "_adaptive_aggressiveness",
        lambda **kwargs: ("MARKET", "AGGRESSIVE", 0.0, abs(float(kwargs["qty"]) * float(kwargs["px"]))),
    )
    monkeypatch.setattr(mod, "_share_rounding_asset_class", lambda _symbol: asset_class)
    monkeypatch.setattr(mod, "_client_order_identity_for_ibkr_order", lambda **_kwargs: ("uid-1", "client-1"))
    monkeypatch.setattr(mod, "validate_ibkr_order_ref", lambda client_order_id: str(client_order_id))
    monkeypatch.setattr(mod, "_mk_contract_for_symbol", lambda _symbol: SimpleNamespace(symbol=_symbol))

    def fake_audit(**kwargs: Any) -> Dict[str, Any]:
        captured["audit_payloads"].append(dict(kwargs.get("payload") or {}))
        return {"ok": True, "event_id": len(captured["audit_payloads"])}

    def fake_market_order(qty: float) -> SimpleNamespace:
        order = SimpleNamespace(totalQuantity=abs(float(qty)), action=("BUY" if qty > 0 else "SELL"))
        captured["orders"].append({"qty": float(qty), "order": order})
        return order

    def fake_place(_app: Any, _oid: int, _contract: Any, order: Any, *, client_order_id: str) -> str:
        captured["placed_order"] = order
        captured["client_order_id"] = client_order_id
        return str(client_order_id)

    monkeypatch.setattr(mod, "record_broker_action_audit", fake_audit)
    monkeypatch.setattr(mod, "_mk_market_order", fake_market_order)
    monkeypatch.setattr(mod, "claim_order_submission_durable", lambda **_kwargs: {"ok": True, "order_uid": "uid-1", "client_order_id": "client-1"})
    monkeypatch.setattr(mod, "_consume_next_order_id", lambda _app: 101)
    monkeypatch.setattr(mod, "_place_order_with_order_ref", fake_place)
    monkeypatch.setattr(mod, "log_submit", lambda **_kwargs: None)
    monkeypatch.setattr(mod, "mark_order_submission_submitted_durable", lambda **_kwargs: None)
    monkeypatch.setattr(mod, "wait_with_kill_interrupt", lambda **_kwargs: (True, "", {}))
    return captured


def _order(symbol: str, weight: float) -> List[Dict[str, Any]]:
    return [{"source_order_id": 701, "symbol": symbol, "to_side": "LONG", "to_weight": float(weight)}]


def test_ibkr_equity_delta_builds_integer_order(monkeypatch) -> None:
    import engine.execution.broker_ibkr_gateway as mod

    captured = _patch_ibkr_common(monkeypatch, mod, asset_class="EQUITY")
    result = mod.apply_latest_portfolio_orders_live(
        dry_run=False,
        override_orders=_order("AAPL", 0.0124),
        override_order_id=9101,
        override_ts_ms=1_700_000_000_000,
    )

    assert result["ok"] is True
    assert result["submitted_n"] == 1
    assert captured["orders"][0]["qty"] == 12.0
    assert captured["orders"][0]["order"].totalQuantity == 12.0
    assert captured["audit_payloads"][0]["share_rounding"]["raw_qty"] == 12.4
    assert captured["audit_payloads"][0]["share_rounding"]["rounded_qty"] == 12.0

    canary = f"EQ07_CANARY_{uuid.uuid4().hex}"
    assert canary not in json.dumps({"result": result, "captured": captured}, default=str)


def test_ibkr_sub_min_equity_order_is_dropped_before_order_build(monkeypatch) -> None:
    import engine.execution.broker_ibkr_gateway as mod

    captured = _patch_ibkr_common(monkeypatch, mod, asset_class="EQUITY")
    result = mod.apply_latest_portfolio_orders_live(
        dry_run=False,
        override_orders=_order("AAPL", 0.0000005),
        override_order_id=9102,
        override_ts_ms=1_700_000_000_000,
    )

    assert result["ok"] is True
    assert result["submitted_n"] == 0
    assert captured["orders"] == []
    assert result["share_rounding_skipped"][0]["share_rounding"]["rounded_qty"] == 0.0


def test_ibkr_fx_quantity_is_not_share_rounded(monkeypatch) -> None:
    import engine.execution.broker_ibkr_gateway as mod

    captured = _patch_ibkr_common(monkeypatch, mod, asset_class="FX")
    result = mod.apply_latest_portfolio_orders_live(
        dry_run=False,
        override_orders=_order("EURUSD", 0.0124),
        override_order_id=9103,
        override_ts_ms=1_700_000_000_000,
    )

    assert result["ok"] is True
    assert result["submitted_n"] == 1
    assert captured["orders"][0]["qty"] == 12.4
    assert "share_rounding" not in captured["audit_payloads"][0]
