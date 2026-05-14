"""
FILE: patch_dev_core_imports.py

Operational helper script for `patch_dev_core_imports`.
"""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

# Replace exact module prefixes anywhere in .py files.
# Longest-first to avoid partial overlaps.
MAP = {
    "engine.storage": "engine.runtime.storage",
    "engine.alerts": "engine.runtime.alerts",
    "engine.health": "engine.runtime.health",
    "engine.dashboard_weather_widgets": "engine.runtime.dashboard_weather_widgets",

    "engine.asset_map": "engine.data.asset_map",
    "engine.equity_snapshot": "engine.data.equity_snapshot",
    "engine.factor_universe": "engine.data.factor_universe",
    "engine.gdelt_macro": "engine.data.gdelt_macro",
    "engine.provider_router": "engine.data.provider_router",
    "engine.universe": "engine.data.universe",
    "engine.universe_discovery": "engine.data.universe_discovery",
    "engine.weather_features": "engine.data.weather_features",
    "engine.weather_api": "engine.data.weather_api",
    "engine.symbol_blacklist": "engine.data.symbol_blacklist",

    "engine.calendar.fmp_earnings": "engine.data.calendar.fmp_earnings",
    "engine.ingest.gdelt_ingest": "engine.data.ingest.gdelt_ingest",
    "engine.ingest.options_polygon": "engine.data.options.options_polygon",

    "engine.data.live_prices.ccxt_live": "engine.data.live_prices.ccxt_live",
    "engine.data.live_prices.ibkr_live": "engine.data.live_prices.ibkr_live",
    "engine.data.live_prices.polygon_live": "engine.data.live_prices.polygon_live",
    "engine.data.live_prices.provider": "engine.data.live_prices.provider",
    "engine.data.live_prices.yfinance_live": "engine.data.live_prices.yfinance_live",

    "engine.options.options_polygon": "engine.data.options.options_polygon",
    "engine.options.tradier_live": "engine.data.options.tradier_live",

    "engine.prices.csv_feed": "engine.data.prices.csv_feed",
    "engine.prices.returns": "engine.data.prices.returns",
    "engine.prices.volatility": "engine.data.prices.volatility",

    "engine.sec.edgar_live": "engine.data.sec.edgar_live",

    "engine.broker_alpaca_rest": "engine.execution.broker_alpaca_rest",
    "engine.broker_fill_utils": "engine.execution.broker_fill_utils",
    "engine.broker_ibkr_gateway": "engine.execution.broker_ibkr_gateway",
    "engine.broker_router": "engine.execution.broker_router",
    "engine.broker_sim": "engine.execution.broker_sim",
    "engine.dual_execution": "engine.execution.dual_execution",
    "engine.exec_conf_calibration": "engine.execution.exec_conf_calibration",
    "engine.exec_stats": "engine.execution.exec_stats",
    "engine.execution_analytics_engine": "engine.execution.execution_analytics_engine",
    "engine.execution_costs": "engine.execution.execution_costs",
    "engine.execution_ledger": "engine.execution.execution_ledger",
    "engine.execution_microstructure": "engine.execution.execution_microstructure",
    "engine.execution_mode": "engine.execution.execution_mode",
    "engine.execution_policy_engine": "engine.execution.execution_policy_engine",
    "engine.kill_switch": "engine.execution.kill_switch",
    "engine.kill_switch.snapshot": "engine.execution.kill_switch",
    "engine.position_reconcile": "engine.execution.position_reconcile",
    "engine.trade_attribution_ledger": "engine.execution.trade_attribution_ledger",

    "engine.adaptive_order_slicer": "engine.strategy.adaptive_order_slicer",
    "engine.alpha_lifecycle_engine": "engine.strategy.alpha_lifecycle_engine",
    "engine.capital_guard": "engine.strategy.capital_guard",
    "engine.clustering": "engine.strategy.clustering",
    "engine.confidence_adjust": "engine.strategy.confidence_adjust",
    "engine.corr_opt": "engine.strategy.corr_opt",
    "engine.decision_log": "engine.strategy.decision_log",
    "engine.drawdown_state": "engine.strategy.drawdown_state",
    "engine.drift": "engine.strategy.drift",
    "engine.drift_utils": "engine.strategy.drift_utils",
    "engine.edge_filter": "engine.strategy.edge_filter",
    "engine.embed_regressor": "engine.strategy.embed_regressor",
    "engine.feature_expansion": "engine.strategy.feature_expansion",
    "engine.labeling": "engine.strategy.labeling",
    "engine.learning": "engine.strategy.learning",
    "engine.market_stress": "engine.strategy.market_stress",
    "engine.model_registry": "engine.strategy.model_registry",
    "engine.model_v2": "engine.strategy.model_v2",
    "engine.news_domain": "engine.strategy.news_domain",
    "engine.opportunity_allocation": "engine.strategy.opportunity_allocation",
    "engine.pnl_decomposition_engine": "engine.strategy.pnl_decomposition_engine",
    "engine.portfolio": "engine.strategy.portfolio",
    "engine.portfolio_execution_intents": "engine.strategy.portfolio_execution_intents",
    "engine.portfolio_risk_gate": "engine.strategy.portfolio_risk_gate",
    "engine.predictor": "engine.strategy.predictor",
    "engine.promotion_audit": "engine.strategy.promotion_audit",
    "engine.promotion_guard": "engine.strategy.promotion_guard",
    "engine.promotion_hardening": "engine.strategy.promotion_hardening",
    "engine.regime_compat": "engine.strategy.regime_compat",
    "engine.regime_size": "engine.strategy.regime_size",
    "engine.regime_stack": "engine.strategy.regime_stack",
    "engine.risk": "engine.strategy.risk",
    "engine.risk_state": "engine.strategy.risk_state",
    "engine.rl_strategy_policy": "engine.strategy.rl_strategy_policy",
    "engine.rules_engine": "engine.strategy.rules_engine",
    "engine.shadow_trainer": "engine.strategy.shadow_trainer",
    "engine.size_policy": "engine.strategy.size_policy",
    "engine.social_context": "engine.strategy.social_context",
    "engine.social_regime": "engine.strategy.social_regime",
    "engine.social_risk": "engine.strategy.social_risk",
    "engine.strategy_selector": "engine.strategy.strategy_selector",
    "engine.tech_indicators": "engine.strategy.tech_indicators",
    "engine.temporal_encoder": "engine.strategy.temporal_encoder",
    "engine.temporal_predictor": "engine.strategy.temporal_predictor",
    "engine.training_guard": "engine.strategy.training_guard",
    "engine.validation": "engine.strategy.validation",

    # dev_core.regime was referenced but no module exists; direct to model_v2 where get_current_regime lives
    "engine.regime": "engine.strategy.model_v2",
}

ORDER = sorted(MAP.keys(), key=len, reverse=True)

def patch_file(path: Path) -> bool:
    src = path.read_text(encoding="utf-8", errors="ignore")
    out = src
    for k in ORDER:
        # Longest-first replacement prevents short prefixes from partially
        # rewriting names that should map to a more specific destination.
        out = out.replace(k, MAP[k])
    if out != src:
        path.write_text(out, encoding="utf-8")
        return True
    return False

def main() -> int:
    changed = []
    for p in REPO_ROOT.rglob("*.py"):
        # This is a repo migration helper, not something the runtime should ever
        # invoke. It deliberately rewrites source files in place.
        # do not rewrite inside venvs or caches if present
        if any(part in (".venv", "venv", "__pycache__", ".git") for part in p.parts):
            continue
        if patch_file(p):
            changed.append(str(p.relative_to(REPO_ROOT)))
    if changed:
        print("CHANGED:")
        for x in changed:
            print("  " + x)
    else:
        print("NO CHANGES")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
