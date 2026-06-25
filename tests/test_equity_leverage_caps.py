from __future__ import annotations

import importlib

import pytest


def _caps():
    return importlib.reload(importlib.import_module("engine.risk.equity_leverage_caps"))


def test_equity_leverage_mode_defaults_and_unknown_fail_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("EQUITY_LEVERAGE_MODE", raising=False)
    caps = _caps()
    assert caps.equity_leverage_mode() == "cash"

    monkeypatch.setenv("EQUITY_LEVERAGE_MODE", "unknown")
    assert caps.equity_leverage_mode() == "cash"

    monkeypatch.setenv("EQUITY_LEVERAGE_MODE", "reg-t")
    assert caps.equity_leverage_mode() == "reg_t"


def test_max_equity_leverage_defaults_and_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("EQUITY_LEVERAGE_CAPS_JSON", raising=False)
    caps = _caps()

    assert caps.max_equity_leverage(mode="cash") == pytest.approx(1.0)
    # Reg-T initial margin is 50%, so the default initial leverage cap is 2:1.
    assert caps.max_equity_leverage(mode="reg_t") == pytest.approx(2.0)
    assert caps.max_equity_leverage(mode="unknown") == pytest.approx(1.0)

    monkeypatch.setenv("EQUITY_LEVERAGE_CAPS_JSON", '{"cash":0.75,"reg_t":1.5}')
    assert caps.max_equity_leverage(mode="cash") == pytest.approx(0.75)
    assert caps.max_equity_leverage(mode="reg_t") == pytest.approx(1.5)

    monkeypatch.setenv("EQUITY_LEVERAGE_CAPS_JSON", '{"cash":0,"reg_t":"NaN"}')
    assert caps.max_equity_leverage(mode="cash") == pytest.approx(1.0)
    assert caps.max_equity_leverage(mode="reg_t") == pytest.approx(2.0)


def test_equity_leverage_caps_import_is_side_effect_free() -> None:
    module = importlib.import_module("engine.risk.equity_leverage_caps")
    assert module.max_equity_leverage(mode="cash") > 0.0
