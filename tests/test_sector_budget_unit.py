from __future__ import annotations

import importlib

import pytest


def _reload_engine(monkeypatch, **env):
    defaults = {
        "PORTFOLIO_RISK_USE_SECTOR_BUDGETS": "1",
        "PORTFOLIO_RISK_SECTOR_MAX_GROSS": "0.30",
        "PORTFOLIO_RISK_SECTOR_BUDGETS_JSON": "",
    }
    defaults.update({key: str(value) for key, value in env.items()})
    for key, value in defaults.items():
        monkeypatch.setenv(key, value)

    import engine.risk.portfolio_risk_engine as portfolio_risk_engine

    return importlib.reload(portfolio_risk_engine)


def _gross(engine, rows):
    return sum(engine._abs_weight(row) for row in rows.values())


def test_sector_budget_scales_over_budget_sector_and_preserves_signs(monkeypatch):
    engine = _reload_engine(monkeypatch)
    monkeypatch.setattr(engine, "_sector_for", lambda _con, symbol: "FINANCIALS")
    desired = {
        "XLF": {"symbol": "XLF", "weight": 0.30, "side": "LONG", "reason": {}},
        "KRE": {"symbol": "KRE", "weight": 0.30, "side": "SHORT", "reason": {}},
        "JPM": {"symbol": "JPM", "weight": 0.30, "side": "LONG", "reason": {}},
    }
    info = {}

    out = engine._apply_sector_budgets(None, desired, info)

    scale = 0.30 / 0.90
    assert _gross(engine, out) == pytest.approx(0.30)
    assert out["XLF"]["weight"] == pytest.approx(0.30 * scale)
    assert out["KRE"]["weight"] == pytest.approx(-0.30 * scale)
    assert out["JPM"]["weight"] == pytest.approx(0.30 * scale)
    assert engine._signed_weight(out["KRE"]) < 0.0
    for row in out.values():
        reason = row["reason"]["sector_budget"]
        assert reason["sector"] == "FINANCIALS"
        assert reason["gross_pre"] == pytest.approx(0.90)
        assert reason["cap"] == pytest.approx(0.30)
        assert reason["scale"] == pytest.approx(scale)
    assert info["sector_gross_pre"]["FINANCIALS"] == pytest.approx(0.90)
    assert info["sector_gross_post"]["FINANCIALS"] == pytest.approx(0.30)
    assert info["sector_budgets_hit"]["FINANCIALS"]["scale"] == pytest.approx(scale)


def test_diversified_book_under_sector_caps_is_untouched(monkeypatch):
    engine = _reload_engine(monkeypatch)
    sectors = {
        "XLF": "FINANCIALS",
        "XLE": "ENERGY",
        "XLK": "TECHNOLOGY",
        "XLV": "HEALTHCARE",
    }
    monkeypatch.setattr(engine, "_sector_for", lambda _con, symbol: sectors[str(symbol)])
    desired = {
        sym: {"symbol": sym, "weight": 0.20, "side": "LONG", "reason": {}}
        for sym in sorted(sectors)
    }
    info = {}

    out = engine._apply_sector_budgets(None, desired, info)

    assert out == desired
    assert all("sector_budget" not in row.get("reason", {}) for row in out.values())
    assert "sector_budgets_hit" not in info
    assert info["sector_gross_pre"]["ENERGY"] == pytest.approx(0.20)
    assert info["sector_gross_pre"]["FINANCIALS"] == pytest.approx(0.20)
    assert info["sector_gross_pre"]["HEALTHCARE"] == pytest.approx(0.20)
    assert info["sector_gross_pre"]["TECHNOLOGY"] == pytest.approx(0.20)


def test_no_sector_data_is_never_bucketed_or_scaled(monkeypatch):
    engine = _reload_engine(monkeypatch)
    monkeypatch.setattr(engine, "_sector_for", lambda _con, _symbol: "")
    desired = {
        "AAA": {"symbol": "AAA", "weight": 0.40, "side": "LONG", "reason": {}},
        "BBB": {"symbol": "BBB", "weight": 0.40, "side": "SHORT", "reason": {}},
    }
    info = {}

    out = engine._apply_sector_budgets(None, desired, info)

    assert out == desired
    assert info["sector_gross_pre"] == {}
    assert info["sector_gross_post"] == {}
    assert "sector_budgets_hit" not in info


def test_sector_budget_json_override_wins_over_default(monkeypatch):
    engine = _reload_engine(
        monkeypatch,
        PORTFOLIO_RISK_SECTOR_BUDGETS_JSON='{"ENERGY":0.50}',
    )
    monkeypatch.setattr(engine, "_sector_for", lambda _con, _symbol: "ENERGY")
    desired = {
        "XLE": {"symbol": "XLE", "weight": 0.20, "side": "LONG", "reason": {}},
        "OIH": {"symbol": "OIH", "weight": 0.20, "side": "LONG", "reason": {}},
        "XOP": {"symbol": "XOP", "weight": 0.20, "side": "LONG", "reason": {}},
    }
    info = {}

    out = engine._apply_sector_budgets(None, desired, info)

    scale = 0.50 / 0.60
    assert _gross(engine, out) == pytest.approx(0.50)
    assert info["sector_gross_post"]["ENERGY"] == pytest.approx(0.50)
    assert info["sector_budgets_hit"]["ENERGY"]["scale"] == pytest.approx(scale)
