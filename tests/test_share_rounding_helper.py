from __future__ import annotations

import importlib


def _helper():
    import engine.execution.share_rounding as share_rounding

    return importlib.reload(share_rounding)


def test_ibkr_rounds_toward_zero_and_preserves_sign(monkeypatch) -> None:
    monkeypatch.setenv("EXEC_USE_SHARE_ROUNDING", "1")
    monkeypatch.setenv("EXEC_EQUITY_MIN_NOTIONAL_USD", "1")
    share_rounding = _helper()

    qty, audit = share_rounding.round_equity_qty(10.7, 100.0, broker="ibkr", asset_class="EQUITY")
    neg_qty, neg_audit = share_rounding.round_equity_qty(-10.7, 100.0, broker="ibkr", asset_class="EQUITY")

    assert qty == 10.0
    assert neg_qty == -10.0
    assert audit["applied"] is True
    assert audit["dropped"] is False
    assert audit["raw_qty"] == 10.7
    assert audit["rounded_qty"] == 10.0
    assert audit["increment"] == 1.0
    assert neg_audit["rounded_qty"] == -10.0


def test_alpaca_fractional_default_and_min_notional_drop(monkeypatch) -> None:
    monkeypatch.setenv("EXEC_USE_SHARE_ROUNDING", "1")
    monkeypatch.setenv("EXEC_EQUITY_MIN_NOTIONAL_USD", "1")
    share_rounding = _helper()

    qty, audit = share_rounding.round_equity_qty(10.7, 100.0, broker="alpaca", asset_class="EQUITY")
    dust_qty, dust_audit = share_rounding.round_equity_qty(
        0.001,
        1.0,
        broker="alpaca",
        asset_class="EQUITY",
    )

    assert qty == 10.7
    assert audit["allow_fractional"] is True
    assert audit["changed"] is False
    assert dust_qty == 0.0
    assert dust_audit["dropped"] is True
    assert dust_audit["reason"] == "dropped_min_notional"
    assert dust_audit["min_notional"] == 1.0


def test_fx_passthrough_regardless_of_broker_and_gate(monkeypatch) -> None:
    monkeypatch.setenv("EXEC_USE_SHARE_ROUNDING", "1")
    share_rounding = _helper()

    qty, audit = share_rounding.round_equity_qty(10.7, 100.0, broker="ibkr", asset_class="FX")
    alpaca_qty, alpaca_audit = share_rounding.round_equity_qty(10.7, 100.0, broker="alpaca", asset_class="FX")

    assert qty == 10.7
    assert alpaca_qty == 10.7
    assert audit["reason"] == "fx_passthrough"
    assert alpaca_audit["reason"] == "fx_passthrough"
    assert audit["applied"] is False


def test_gate_off_is_unchanged(monkeypatch) -> None:
    monkeypatch.setenv("EXEC_USE_SHARE_ROUNDING", "0")
    share_rounding = _helper()

    qty, audit = share_rounding.round_equity_qty(10.7, 100.0, broker="ibkr", asset_class="EQUITY")

    assert qty == 10.7
    assert audit["reason"] == "disabled"
    assert audit["applied"] is False


def test_env_overrides_are_call_time_policy(monkeypatch) -> None:
    monkeypatch.setenv("EXEC_USE_SHARE_ROUNDING", "1")
    monkeypatch.setenv("EXEC_IBKR_SHARE_INCREMENT", "0.5")
    monkeypatch.setenv("EXEC_EQUITY_MIN_NOTIONAL_USD", "50")
    share_rounding = _helper()

    policy = share_rounding.equity_share_policy("ibkr")
    qty, audit = share_rounding.round_equity_qty(10.7, 100.0, broker="ibkr", asset_class="EQUITY")

    assert policy["increment"] == 0.5
    assert policy["min_notional"] == 50.0
    assert qty == 10.5
    assert audit["increment"] == 0.5
    assert audit["min_notional"] == 50.0

    monkeypatch.setenv("EXEC_IBKR_SHARE_INCREMENT", "0.25")
    next_qty, next_audit = share_rounding.round_equity_qty(10.7, 100.0, broker="ibkr", asset_class="EQUITY")
    assert next_qty == 10.5
    assert next_audit["increment"] == 0.25


def test_pure_same_input_same_output(monkeypatch) -> None:
    monkeypatch.setenv("EXEC_USE_SHARE_ROUNDING", "1")
    share_rounding = _helper()

    first = share_rounding.round_equity_qty(12.4, 100.0, broker="sim", asset_class="UNKNOWN")
    second = share_rounding.round_equity_qty(12.4, 100.0, broker="sim", asset_class="UNKNOWN")

    assert first == second
    assert first[0] == 12.0
    assert first[1]["eligibility_reason"] == "unknown_as_equity"
