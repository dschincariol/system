from __future__ import annotations

import importlib

import pytest


def _sizing(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DEPLOYABLE_EQUITY_MODE", "min_equity_bp")
    monkeypatch.setenv("DEPLOYABLE_BP_FACTOR", "0.50")
    monkeypatch.setenv("DEPLOYABLE_CASH_FACTOR", "1.00")
    monkeypatch.setenv("DEPLOYABLE_EQUITY_FACTOR", "1.00")

    import engine.execution.deployable_capital as deployable_capital
    import engine.strategy.equity_sizing as equity_sizing

    importlib.reload(deployable_capital)
    return importlib.reload(equity_sizing)


def test_equity_deployable_base_cash_mode_uses_account_equity_without_buying_power(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sizing = _sizing(monkeypatch)

    base, reason = sizing.equity_deployable_base(
        {"equity": 100_000.0},
        account_equity=100_000.0,
        mode="cash",
        max_leverage=1.0,
    )

    assert base == pytest.approx(100_000.0)
    assert reason["allowed_gross_weight"] == pytest.approx(1.0)
    assert reason["buying_power_missing"] is True


def test_equity_deployable_base_reg_t_uses_buying_power_ceiling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sizing = _sizing(monkeypatch)

    base, reason = sizing.equity_deployable_base(
        {"equity": 100_000.0, "buying_power": 120_000.0},
        account_equity=100_000.0,
        mode="reg_t",
        max_leverage=2.0,
    )

    assert base == pytest.approx(120_000.0)
    assert reason["deployable_equity"] == pytest.approx(60_000.0)
    assert reason["allowed_gross_weight"] == pytest.approx(1.2)
    assert reason["buying_power"] == pytest.approx(120_000.0)


def test_equity_deployable_base_reg_t_without_buying_power_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sizing = _sizing(monkeypatch)

    base, reason = sizing.equity_deployable_base(
        {"equity": 100_000.0},
        account_equity=100_000.0,
        mode="reg_t",
        max_leverage=2.0,
    )

    assert base == pytest.approx(0.0)
    assert reason["available"] is False
    assert reason["unavailable_reason"] == "equity_buying_power_unavailable"
    assert reason["buying_power_missing"] is True
    assert reason["deployable_equity"] == pytest.approx(0.0)
    assert reason["allowed_gross_weight"] == pytest.approx(0.0)


def test_clamp_equity_gross_to_leverage_scales_aggregate_gross() -> None:
    from engine.strategy.equity_sizing import clamp_equity_gross_to_leverage

    rows = {
        "AAPL": {"symbol": "AAPL", "weight": 1.0, "side": "LONG"},
        "MSFT": {"symbol": "MSFT", "weight": 0.6, "side": "SHORT"},
    }

    clamped, reason = clamp_equity_gross_to_leverage(
        rows,
        account_equity=100_000.0,
        allowed_gross_weight=1.0,
        mode="cash",
    )

    assert reason["type"] == "equity_leverage_cap"
    assert reason["scale"] == pytest.approx(0.625)
    assert reason["gross_pre"] == pytest.approx(1.6)
    assert reason["gross_post"] == pytest.approx(1.0)
    assert clamped["AAPL"]["weight"] == pytest.approx(0.625)
    assert clamped["AAPL"]["side"] == "LONG"
    assert clamped["MSFT"]["weight"] == pytest.approx(0.375)
    assert clamped["MSFT"]["side"] == "SHORT"


def test_clamp_equity_gross_to_leverage_is_noop_within_cap() -> None:
    from engine.strategy.equity_sizing import clamp_equity_gross_to_leverage

    rows = {"AAPL": {"symbol": "AAPL", "weight": 0.40, "side": "LONG"}}

    clamped, reason = clamp_equity_gross_to_leverage(
        rows,
        account_equity=100_000.0,
        allowed_gross_weight=1.0,
        mode="cash",
    )

    assert clamped == rows
    assert reason["clamped"] is False
    assert reason["gross_post"] == pytest.approx(0.40)
