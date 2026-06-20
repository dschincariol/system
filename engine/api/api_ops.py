"""Route metadata for ops, diagnostics, governance, and analytics APIs.

This module keeps route registration separate from handler imports so the
dashboard server can assemble read-side operator and diagnostics surfaces
without eagerly importing the full handler graph.
"""

ROUTE_SPECS = [
    ("GET", "/api/alerts", "api_get_alerts"),
    ("GET", "/api/notifications/status", "api_get_notifications_status"),
    ("POST", "/api/notifications/test", "api_post_notifications_test"),
    ("GET", "/api/validation", "api_get_validation"),
    ("GET", "/api/model/diagnostics", "api_get_model_diagnostics"),
    ("GET", "/api/model/registry", "api_get_model_registry"),
    ("GET", "/api/model/performance_divergence", "api_get_model_performance_divergence"),
    ("GET", "/api/model/lifecycle", "api_get_model_lifecycle"),
    ("GET", "/api/embed_model_eval", "api_get_embed_model_eval"),
    ("GET", "/api/embed_conf_calib", "api_get_embed_conf_calib"),
    ("GET", "/api/temporal/eval", "api_get_temporal_eval"),
    ("GET", "/api/temporal/models", "api_get_temporal_models"),
    ("GET", "/api/portfolio/backtest/latest", "api_get_latest_portfolio_backtest"),
    ("GET", "/api/execution/metrics", "api_get_execution_metrics"),
    ("GET", "/api/execution/stats", "api_get_execution_stats"),
    ("GET", "/api/execution/metrics/rolling", "api_get_execution_metrics_rolling"),
    ("GET", "/api/execution/metrics/by_symbol", "api_get_execution_metrics_by_symbol"),
    ("GET", "/api/execution/metrics/by_confidence", "api_get_execution_cost_by_confidence"),
    ("GET", "/api/execution/diagnostics", "api_get_execution_diagnostics"),
    ("GET", "/api/execution/advisories", "api_get_execution_advisories"),
    ("POST", "/api/execution/advisories/action", "api_post_execution_advisory_action"),
    ("GET", "/api/social/features", "api_get_social_features"),
    ("GET", "/api/social/regimes", "api_get_social_regimes"),
    ("GET", "/api/social/blocks", "api_get_social_blocks"),
    ("GET", "/api/news/latest", "api_get_news_latest"),
    ("GET", "/api/news/sentiment", "api_get_news_sentiment"),
    ("GET", "/api/operator/human_alignment", "api_get_human_alignment_summary"),
    ("GET", "/api/weather/snapshot", "api_get_weather_snapshot"),
    ("GET", "/api/weather/alerts", "api_get_weather_alerts"),
    ("GET", "/api/weather/effect", "api_get_weather_effect"),
    ("GET", "/api/data/feature_visibility", "api_get_feature_visibility"),
    ("GET", "/api/confidence_mass", "api_get_confidence_mass"),
    ("GET", "/api/governance/evidence", "api_get_governance_evidence"),
    ("GET", "/api/governance/evidence/promotion_blockers", "api_get_governance_evidence_promotion_blockers"),
    ("GET", "/api/governance/evidence/generated_candidates", "api_get_governance_evidence_generated_candidates"),
    ("GET", "/api/governance/evidence/shadow_capital", "api_get_governance_evidence_shadow_capital"),
    ("GET", "/api/governance/shadow_capital/scores", "api_get_shadow_capital_scores"),
    ("POST", "/api/promotion/rollback", "api_post_rollback"),
]

ROUTE_SPECS_OPS = ROUTE_SPECS
