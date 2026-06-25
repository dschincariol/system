from __future__ import annotations

import json
import uuid
from typing import Any, Dict, List

import pytest


pytestmark = pytest.mark.safety_critical


class _FakeCon:
    def close(self) -> None:
        return None


def _patch_alpaca_common(monkeypatch, mod: Any, *, asset_class: str) -> Dict[str, Any]:
    captured: Dict[str, Any] = {"payloads": [], "audit_payloads": []}

    monkeypatch.setenv("EXEC_USE_SHARE_ROUNDING", "1")
    monkeypatch.setenv("EXEC_EQUITY_MIN_NOTIONAL_USD", "1")
    monkeypatch.delenv("EXEC_ALPACA_SHARE_INCREMENT", raising=False)
    monkeypatch.setattr(mod, "connect", lambda: _FakeCon())
    monkeypatch.setattr(mod, "_real_trading_gate", lambda: {"ok": True, "real_trading_allowed": True})
    monkeypatch.setattr(mod, "_alpaca_credentials_block", lambda: None)
    monkeypatch.setattr(mod, "_prelive_reconcile_or_block", lambda _broker: None)
    monkeypatch.setattr(mod, "apply_alpha_lifecycle", lambda **kwargs: (list(kwargs.get("orders") or []), {"ok": True}))
    monkeypatch.setattr(mod, "live_options_order_block", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(mod, "get_state", lambda *_args, **_kwargs: "0")
    monkeypatch.setattr(mod, "set_state", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(mod, "execution_allowed", lambda **_kwargs: (True, None, None))
    monkeypatch.setattr(mod, "get_account", lambda: {"equity": 100000.0, "buying_power": 100000.0, "cash": 100000.0})
    monkeypatch.setattr(mod, "compute_deployable_equity", lambda *_args, **_kwargs: 100000.0)
    monkeypatch.setattr(mod, "get_positions", lambda: [])
    monkeypatch.setattr(mod, "_load_latest_prices", lambda _con: {"AAPL": 100.0})
    monkeypatch.setattr(mod, "_price_at_or_before", lambda _con, _symbol, _ts_ms: 100.0)
    monkeypatch.setattr(
        mod,
        "_apply_execution_risk_caps",
        lambda **kwargs: (float(kwargs["delta_qty"]), {"applied": False}),
    )
    monkeypatch.setattr(mod, "_share_rounding_asset_class", lambda _symbol: asset_class)

    def fake_req(method: str, path: str, payload: Dict[str, Any], timeout_s: float | None = None) -> Dict[str, Any]:
        captured["payloads"].append(
            {"method": method, "path": path, "payload": dict(payload), "timeout_s": timeout_s}
        )
        return {"id": f"alpaca-order-{len(captured['payloads'])}"}

    def fake_audit(**kwargs: Any) -> Dict[str, Any]:
        captured["audit_payloads"].append(dict(kwargs.get("payload") or {}))
        return {"ok": True, "event_id": len(captured["audit_payloads"])}

    monkeypatch.setattr(mod, "_req", fake_req)
    monkeypatch.setattr(mod, "record_broker_action_audit", fake_audit)
    monkeypatch.setattr(mod, "claim_order_submission_durable", lambda **_kwargs: {"ok": True, "order_uid": "uid-1", "client_order_id": "client-1"})
    monkeypatch.setattr(mod, "log_submit", lambda **_kwargs: None)
    monkeypatch.setattr(mod, "mark_order_submission_submitted_durable", lambda **_kwargs: None)
    monkeypatch.setattr(mod, "wait_with_kill_interrupt", lambda **_kwargs: (True, "", {}))
    return captured


def _order(symbol: str, weight: float) -> List[Dict[str, Any]]:
    return [{"source_order_id": 801, "symbol": symbol, "to_side": "LONG", "to_weight": float(weight)}]


def test_alpaca_fractional_equity_qty_is_preserved_in_payload(monkeypatch) -> None:
    import engine.execution.broker_alpaca_rest as mod

    captured = _patch_alpaca_common(monkeypatch, mod, asset_class="EQUITY")
    result = mod.apply_latest_portfolio_orders_live(
        dry_run=False,
        override_orders=_order("AAPL", 0.0124),
        override_order_id=9201,
        override_ts_ms=1_700_000_000_000,
    )

    assert result["ok"] is True
    assert result["submitted_n"] == 1
    assert captured["payloads"][0]["payload"]["qty"] == "12.4"
    assert captured["audit_payloads"][0]["share_rounding"]["raw_qty"] == 12.4
    assert captured["audit_payloads"][0]["share_rounding"]["rounded_qty"] == 12.4

    canary = f"EQ07_CANARY_{uuid.uuid4().hex}"
    assert canary not in json.dumps({"result": result, "captured": captured}, default=str)


def test_alpaca_sub_min_equity_qty_is_dropped_before_submit(monkeypatch) -> None:
    import engine.execution.broker_alpaca_rest as mod

    captured = _patch_alpaca_common(monkeypatch, mod, asset_class="EQUITY")
    result = mod.apply_latest_portfolio_orders_live(
        dry_run=False,
        override_orders=_order("AAPL", 0.00000001),
        override_order_id=9202,
        override_ts_ms=1_700_000_000_000,
    )

    assert result["ok"] is True
    assert result["submitted_n"] == 0
    assert captured["payloads"] == []
    assert result["share_rounding_skipped"][0]["share_rounding"]["dropped"] is True
