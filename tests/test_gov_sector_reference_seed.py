from __future__ import annotations

import importlib
import json
import sqlite3

import pytest

pytestmark = pytest.mark.safety_critical


class _DrawdownOK:
    ok = True
    drawdown = 0.0
    reason_code = "ok"

    def to_dict(self):
        return {"ok": True, "drawdown": 0.0, "reason_code": "ok"}


def _reload_engine(monkeypatch):
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


def _make_sector_con(quiver_gov) -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    quiver_gov.ensure_gov_tables(con)
    con.execute(
        """
        CREATE TABLE symbols(
          symbol TEXT PRIMARY KEY,
          asset_class TEXT,
          status TEXT,
          score REAL,
          meta_json TEXT,
          updated_ts_ms INTEGER
        )
        """
    )
    con.executemany(
        """
        INSERT INTO symbols(symbol, asset_class, status, score, meta_json, updated_ts_ms)
        VALUES (?, 'EQUITY', 'ACTIVE', 1.0, '{}', 1700000000000)
        """,
        [("AAPL",), ("MSFT",), ("NVDA",), ("ZZZZ",)],
    )
    quiver_gov.seed_equity_sector_reference(con, symbols=["AAPL", "MSFT", "NVDA", "ZZZZ"])
    con.commit()
    return con


def test_checked_in_reference_seeds_production_lookup_and_reports_coverage():
    import engine.data.quiver_gov as quiver_gov

    quiver_gov = importlib.reload(quiver_gov)
    con = _make_sector_con(quiver_gov)
    try:
        assert quiver_gov.sector_for_symbol(con, "AAPL") == "TECHNOLOGY"
        assert quiver_gov.sector_for_symbol(con, "ZZZZ") == ""

        coverage = quiver_gov.sector_coverage_report(con)
        assert coverage["total"] == 4
        assert coverage["resolved"] == 3
        assert coverage["unresolved"] == 1
        assert coverage["resolved_by_symbol"]["AAPL"] == "TECHNOLOGY"
        assert coverage["unresolved_symbols"] == ["ZZZZ"]
    finally:
        con.close()


def test_real_stock_sector_clamps_end_to_end_through_production_lookup(monkeypatch):
    import engine.data.quiver_gov as quiver_gov

    quiver_gov = importlib.reload(quiver_gov)
    con = _make_sector_con(quiver_gov)
    engine = _reload_engine(monkeypatch)
    captured_state = _patch_fast_apply_path(monkeypatch, engine)
    desired = {
        "AAPL": {"symbol": "AAPL", "weight": 0.20, "side": "LONG", "reason": {}},
        "MSFT": {"symbol": "MSFT", "weight": 0.20, "side": "LONG", "reason": {}},
        "NVDA": {"symbol": "NVDA", "weight": 0.20, "side": "LONG", "reason": {}},
    }

    try:
        assert quiver_gov.sector_for_symbol(con, "AAPL") == "TECHNOLOGY"

        out, info = engine.apply_portfolio_risk_engine(con, desired, {}, now_ms=1_700_000_000_000)

        scale = 0.30 / 0.60
        assert sum(engine._abs_weight(row) for row in out.values()) == pytest.approx(0.30)
        assert out["AAPL"]["weight"] == pytest.approx(0.20 * scale)
        assert out["MSFT"]["weight"] == pytest.approx(0.20 * scale)
        assert out["NVDA"]["weight"] == pytest.approx(0.20 * scale)
        assert info["sector_gross_pre"]["TECHNOLOGY"] == pytest.approx(0.60)
        assert info["sector_gross_post"]["TECHNOLOGY"] == pytest.approx(0.30)
        assert info["sector_budgets_hit"]["TECHNOLOGY"]["scale"] == pytest.approx(scale)
        assert info["post_checks"]["sector_within_cap"] is True
        assert info["blocked"] is False
        assert captured_state["portfolio_risk_block"] == "0"
        assert "sector_budget" in json.dumps(out, sort_keys=True)
    finally:
        con.close()
