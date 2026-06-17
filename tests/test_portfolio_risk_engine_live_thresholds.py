from __future__ import annotations

import importlib
import json


def test_monte_carlo_cvar_thresholds_block_when_tail_losses_breach(monkeypatch):
    monkeypatch.setenv("PORTFOLIO_RISK_MC_CVAR_95_BLOCK", "0.05")
    monkeypatch.setenv("PORTFOLIO_RISK_MC_CVAR_99_BLOCK", "0.08")

    import engine.risk.portfolio_risk_engine as portfolio_risk_engine

    portfolio_risk_engine = importlib.reload(portfolio_risk_engine)
    now_ms = 1_700_000_000_000
    payload = {
        "ready": True,
        "status": "ok",
        "var_95": -0.01,
        "var_99": -0.02,
        "cvar_95": -0.055,
        "cvar_99": -0.09,
        "worst_simulated_drawdown": 0.01,
        "drawdown_percentiles": {"p95": 0.01, "p99": 0.02},
    }
    monkeypatch.setattr(
        portfolio_risk_engine,
        "get_state_row",
        lambda key, default: (json.dumps(payload, separators=(",", ":")), now_ms),
    )

    summary = portfolio_risk_engine._load_monte_carlo_risk_summary(now_ms)

    assert summary["blocked"] is True
    reason_types = {str(row.get("type") or "") for row in summary["reasons"]}
    assert "monte_carlo_cvar_95_block" in reason_types
    assert "monte_carlo_cvar_99_block" in reason_types
