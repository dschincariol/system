from __future__ import annotations

import importlib
import json
import sqlite3
import uuid

import pytest

pytestmark = pytest.mark.safety_critical


class _DrawdownOK:
    ok = True
    drawdown = 0.0
    reason_code = "ok"

    def to_dict(self):
        return {"ok": True, "drawdown": 0.0, "reason_code": "ok"}


def _reload_engine(monkeypatch, **env):
    defaults = {
        "PORTFOLIO_RISK_USE_SECTOR_BUDGETS": "1",
        "PORTFOLIO_RISK_SECTOR_MAX_GROSS": "0.30",
        "PORTFOLIO_RISK_SECTOR_BUDGETS_JSON": "",
        "PORTFOLIO_RISK_USE_MONTE_CARLO": "0",
        "PORTFOLIO_RISK_USE_ASSET_CLASS_BUDGETS": "0",
        "PORTFOLIO_RISK_USE_FUTURES_MARGIN_CAPS": "0",
        "PORTFOLIO_RISK_USE_FX_LEVERAGE_CAPS": "0",
        "PORTFOLIO_RISK_USE_EQUITY_LEVERAGE_CAPS": "0",
        "PORTFOLIO_RISK_USE_CRYPTO_LEVERAGE_CAPS": "0",
        "PORTFOLIO_RISK_USE_STRATEGY_BUDGETS": "0",
        "PORTFOLIO_RISK_USE_ALPHA_DECAY_THROTTLE": "0",
        "PORTFOLIO_RISK_USE_VOL_CAPS": "0",
        "PORTFOLIO_RISK_USE_CORR_CLUSTERS": "0",
        "PORTFOLIO_RISK_USE_OPTIONS_GREEK_LIMITS": "0",
        "PORTFOLIO_RISK_MAX_GROSS": "100",
        "PORTFOLIO_RISK_MAX_NET": "100",
        "PORTFOLIO_RISK_MAX_SYMBOL_GROSS": "1",
    }
    defaults.update({key: str(value) for key, value in env.items()})
    for key, value in defaults.items():
        monkeypatch.setenv(key, value)

    import engine.risk.portfolio_risk_engine as portfolio_risk_engine

    return importlib.reload(portfolio_risk_engine)


def _patch_fast_apply_path(monkeypatch, engine):
    captured_state = {}
    monkeypatch.setattr(engine, "evaluate_current_drawdown", lambda _con: _DrawdownOK())
    monkeypatch.setattr(
        engine,
        "_load_live_positions",
        lambda _con: ({}, {"source": "unit_test", "equity_ref": 100_000.0, "equity_ref_source": "unit_test"}),
    )
    monkeypatch.setattr(engine, "_apply_drawdown_throttle", lambda desired, _dd, _info: dict(desired or {}))
    monkeypatch.setattr(engine, "_apply_futures_margin_caps", lambda _con, desired, _info: dict(desired or {}))
    monkeypatch.setattr(engine, "_apply_fx_leverage_caps", lambda _con, desired, _info: dict(desired or {}))
    monkeypatch.setattr(engine, "_apply_equity_leverage_caps", lambda _con, desired, _info: dict(desired or {}))
    monkeypatch.setattr(engine, "_apply_crypto_leverage_caps", lambda _con, desired, _info: dict(desired or {}))
    monkeypatch.setattr(engine, "_apply_strategy_budgets", lambda desired, _info: dict(desired or {}))
    monkeypatch.setattr(engine, "_apply_alpha_decay_throttle", lambda _con, desired, _info, _now_ms: dict(desired or {}))
    monkeypatch.setattr(engine, "_apply_symbol_vol_caps", lambda _con, desired, _info: dict(desired or {}))
    monkeypatch.setattr(engine, "_apply_corr_cluster_caps", lambda _con, desired, _info: dict(desired or {}))
    monkeypatch.setattr(engine, "_apply_portfolio_vol_target", lambda _con, desired, _info: dict(desired or {}))
    monkeypatch.setattr(engine, "_apply_portfolio_caps", lambda desired, _info: dict(desired or {}))
    monkeypatch.setattr(engine, "_persist_snapshot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "record_risk_block", lambda **_kwargs: None)
    monkeypatch.setattr(engine, "set_state", lambda key, value: captured_state.__setitem__(key, value))
    return captured_state


def _desired_financials():
    return {
        "XLF": {"symbol": "XLF", "weight": 0.20, "side": "LONG", "reason": {}},
        "KRE": {"symbol": "KRE", "weight": 0.20, "side": "LONG", "reason": {}},
        "JPM": {"symbol": "JPM", "weight": 0.20, "side": "LONG", "reason": {}},
    }


def _gov_sector_con(symbols: list[str]):
    from engine.data import quiver_gov

    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    quiver_gov.ensure_gov_tables(con)
    quiver_gov.seed_equity_sector_reference(con, symbols=symbols)
    con.commit()
    return con


def test_sector_budget_pipeline_compresses_and_post_check_passes(monkeypatch, caplog):
    canary = f"CANARY_{uuid.uuid4().hex}"
    monkeypatch.setenv("SECTOR_TEST_CANARY_DO_NOT_LOG", canary)
    engine = _reload_engine(monkeypatch)
    captured_state = _patch_fast_apply_path(monkeypatch, engine)
    monkeypatch.setattr(engine, "_sector_for", lambda _con, _symbol: "FINANCIALS")

    out, info = engine.apply_portfolio_risk_engine(None, _desired_financials(), {}, now_ms=1_700_000_000_000)

    assert sum(engine._abs_weight(row) for row in out.values()) == pytest.approx(0.30)
    assert info["sector_gross_post"]["FINANCIALS"] == pytest.approx(0.30)
    assert info["post_checks"]["sector_within_cap"] is True
    assert captured_state["portfolio_risk_block"] == "0"
    state_blob = captured_state["portfolio_risk_info"]
    assert "sector_gross_post" in state_blob
    assert "sector_budgets_hit" in state_blob
    assert "sector_budget" in json.dumps(out, sort_keys=True)
    assert canary not in state_blob
    assert canary not in json.dumps(info, sort_keys=True)
    assert canary not in caplog.text


def test_real_stock_sector_reference_seeds_production_lookup() -> None:
    from engine.data import quiver_gov

    con = _gov_sector_con(["AAPL", "MSFT", "UNKNOWN_TEST_SYMBOL"])
    try:
        assert quiver_gov.sector_for_symbol(con, "AAPL") == "TECHNOLOGY"
        assert quiver_gov.sector_for_symbol(con, "UNKNOWN_TEST_SYMBOL") == ""
        coverage = quiver_gov.sector_coverage_report(
            con,
            symbols=["AAPL", "MSFT", "UNKNOWN_TEST_SYMBOL"],
            equity_only=False,
        )
        assert coverage["total"] == 3
        assert coverage["resolved"] == 2
        assert coverage["unresolved"] == 1
        assert coverage["resolved_by_symbol"]["AAPL"] == "TECHNOLOGY"
        assert coverage["unresolved_symbols"] == ["UNKNOWN_TEST_SYMBOL"]
    finally:
        con.close()


def test_sector_budget_pipeline_clamps_real_stocks_from_production_lookup(monkeypatch):
    engine = _reload_engine(monkeypatch)
    captured_state = _patch_fast_apply_path(monkeypatch, engine)
    con = _gov_sector_con(["AAPL", "MSFT", "NVDA"])
    desired = {
        "AAPL": {"symbol": "AAPL", "weight": 0.20, "side": "LONG", "reason": {}},
        "MSFT": {"symbol": "MSFT", "weight": 0.20, "side": "LONG", "reason": {}},
        "NVDA": {"symbol": "NVDA", "weight": 0.20, "side": "LONG", "reason": {}},
    }
    try:
        out, info = engine.apply_portfolio_risk_engine(con, desired, {}, now_ms=1_700_000_000_000)
    finally:
        con.close()

    assert sum(engine._abs_weight(row) for row in out.values()) == pytest.approx(0.30)
    assert info["sector_gross_pre"]["TECHNOLOGY"] == pytest.approx(0.60)
    assert info["sector_gross_post"]["TECHNOLOGY"] == pytest.approx(0.30)
    assert info["post_checks"]["sector_within_cap"] is True
    assert captured_state["portfolio_risk_block"] == "0"
    assert all(row["reason"]["sector_budget"]["sector"] == "TECHNOLOGY" for row in out.values())


def test_sector_post_check_blocks_when_sector_budget_stage_is_bypassed(monkeypatch):
    engine = _reload_engine(monkeypatch)
    captured_state = _patch_fast_apply_path(monkeypatch, engine)
    monkeypatch.setattr(engine, "_sector_for", lambda _con, _symbol: "FINANCIALS")
    monkeypatch.setattr(engine, "_apply_sector_budgets", lambda _con, desired, _info: dict(desired or {}))

    out, info = engine.apply_portfolio_risk_engine(None, _desired_financials(), {}, now_ms=1_700_000_000_000)

    assert sum(engine._abs_weight(row) for row in out.values()) == pytest.approx(0.60)
    assert info["post_checks"]["sector_within_cap"] is False
    assert info["post_checks"]["sector_violations"]["FINANCIALS"]["gross"] == pytest.approx(0.60)
    assert info["post_checks"]["sector_violations"]["FINANCIALS"]["cap"] == pytest.approx(0.30)
    assert info["blocked"] is True
    assert info["block_reason"]["type"] == "post_cap_validation_failed"
    assert captured_state["portfolio_risk_block"] == "1"


def test_sector_budget_flag_off_matches_absent_stage(monkeypatch, caplog):
    canary = f"CANARY_{uuid.uuid4().hex}"
    monkeypatch.setenv("SECTOR_TEST_CANARY_DO_NOT_LOG", canary)
    engine = _reload_engine(monkeypatch, PORTFOLIO_RISK_USE_SECTOR_BUDGETS="0")
    captured_state = _patch_fast_apply_path(monkeypatch, engine)
    monkeypatch.setattr(engine, "_sector_for", lambda _con, _symbol: "FINANCIALS")
    desired = _desired_financials()

    out_enabled_off, info_enabled_off = engine.apply_portfolio_risk_engine(None, desired, {}, now_ms=1_700_000_000_000)
    blob_enabled_off = captured_state["portfolio_risk_info"]

    monkeypatch.setattr(engine, "_apply_sector_budgets", lambda _con, rows, _info: dict(rows or {}))
    captured_state.clear()
    out_absent, info_absent = engine.apply_portfolio_risk_engine(None, desired, {}, now_ms=1_700_000_000_000)
    blob_absent = captured_state["portfolio_risk_info"]

    assert out_enabled_off == desired
    assert out_enabled_off == out_absent
    assert info_enabled_off == info_absent
    assert blob_enabled_off == blob_absent
    assert info_enabled_off["post_checks"]["sector_within_cap"] is True
    assert "sector_gross_pre" not in info_enabled_off
    assert "sector_gross_post" not in info_enabled_off
    assert canary not in blob_enabled_off
    assert canary not in caplog.text
