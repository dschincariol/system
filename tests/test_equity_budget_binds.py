from __future__ import annotations

import importlib
import json
import uuid

import pytest


def _write_sec_registry(path):
    payload = {
        "fields": ["cik", "name", "ticker", "exchange"],
        "data": [
            [1, "NVIDIA CORP", "NVDA", "Nasdaq"],
            [2, "MICROSOFT CORP", "MSFT", "Nasdaq"],
        ],
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def _reload_engine(monkeypatch, registry_path, *, bind_equity_budget: str):
    monkeypatch.setenv("SEC_TICKER_MAP_CACHE", str(registry_path))
    monkeypatch.delenv("ASSET_CLASS_MAP_JSON", raising=False)
    monkeypatch.delenv("ASSET_MAP_USE_EQUITY_REGISTRY", raising=False)
    monkeypatch.setenv("PORTFOLIO_RISK_BIND_EQUITY_BUDGET", bind_equity_budget)
    monkeypatch.setenv("PORTFOLIO_RISK_USE_ASSET_CLASS_BUDGETS", "1")
    monkeypatch.delenv("PORTFOLIO_RISK_ASSET_CLASS_BUDGETS_JSON", raising=False)

    import engine.data.asset_map as asset_map
    import engine.risk.portfolio_risk_engine as portfolio_risk_engine

    importlib.reload(asset_map)
    return importlib.reload(portfolio_risk_engine)


def _desired():
    return {
        "NVDA": {"symbol": "NVDA", "weight": 0.45, "side": "LONG", "reason": {}},
        "MSFT": {"symbol": "MSFT", "weight": 0.45, "side": "LONG", "reason": {}},
    }


@pytest.mark.safety_critical
def test_equity_asset_class_budget_scales_combined_equity_gross(monkeypatch, tmp_path) -> None:
    canary = f"canary-{uuid.uuid4()}"
    registry_path = tmp_path / "sec_company_tickers_exchange.json"
    _write_sec_registry(registry_path)
    engine = _reload_engine(monkeypatch, registry_path, bind_equity_budget="1")
    desired = _desired()

    assert engine.ASSET_CLASS_BUDGETS["EQUITY"] == pytest.approx(0.80)
    assert engine._asset_class_for(None, "NVDA") == "EQUITY"
    assert engine._asset_class_for(None, "MSFT") == "EQUITY"

    out = engine._apply_asset_class_budgets(desired, info := {})

    expected_scale = 0.80 / 0.90
    assert info["asset_class_gross_pre"]["EQUITY"] == pytest.approx(0.90)
    assert info["asset_class_gross_post"]["EQUITY"] <= 0.80 + 1e-9
    assert info["asset_class_budgets_hit"]["EQUITY"]["scale"] == pytest.approx(expected_scale)
    assert out["NVDA"]["weight"] == pytest.approx(0.45 * expected_scale)
    assert out["MSFT"]["weight"] == pytest.approx(0.45 * expected_scale)
    for row in out.values():
        reason = row["reason"]["asset_class_budget"]
        assert reason["asset_class"] == "EQUITY"
        assert reason["gross_pre"] == pytest.approx(0.90)
        assert reason["cap"] == pytest.approx(0.80)
        assert reason["scale"] == pytest.approx(expected_scale)
    serialized = json.dumps({"info": info, "out": out}, sort_keys=True)
    assert canary not in serialized


@pytest.mark.safety_critical
def test_equity_asset_class_budget_flag_off_uses_legacy_one_hundred_percent(monkeypatch, tmp_path) -> None:
    canary = f"canary-{uuid.uuid4()}"
    registry_path = tmp_path / "sec_company_tickers_exchange.json"
    _write_sec_registry(registry_path)
    engine = _reload_engine(monkeypatch, registry_path, bind_equity_budget="0")
    desired = _desired()

    assert engine.ASSET_CLASS_BUDGETS["EQUITY"] == pytest.approx(1.00)
    out = engine._apply_asset_class_budgets(desired, info := {})

    assert info["asset_class_gross_pre"]["EQUITY"] == pytest.approx(0.90)
    assert info["asset_class_gross_post"]["EQUITY"] == pytest.approx(0.90)
    assert "asset_class_budgets_hit" not in info
    assert out["NVDA"]["weight"] == pytest.approx(0.45)
    assert out["MSFT"]["weight"] == pytest.approx(0.45)
    assert "asset_class_budget" not in out["NVDA"]["reason"]
    assert "asset_class_budget" not in out["MSFT"]["reason"]
    serialized = json.dumps({"info": info, "out": out}, sort_keys=True)
    assert canary not in serialized
