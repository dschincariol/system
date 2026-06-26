"""Route specification assembly for the dashboard compatibility surface."""

from __future__ import annotations

import json
from typing import Any


FALLBACK_ROUTE_SPECS = [
    {"method": "GET",  "path": "/api/db/health",        "handler": "api_get_db_health"},

    # OPERATOR (required by ui/dashboard.js snapshot bundle)
    {"method": "GET", "path": "/api/operator_summary", "handler": "api_get_operator_summary"},
    {"method": "GET",  "path": "/api/operator/sidecar_status",  "handler": "api_get_operator_sidecar_status"},
    {"method": "GET",  "path": "/api/operator/status",            "handler": "api_get_operator_status"},
    {"method": "GET",  "path": "/api/operator/bootstrap",         "handler": "api_get_operator_bootstrap_status"},
    {"method": "GET",  "path": "/api/operator/bootstrap_status",  "handler": "api_get_operator_bootstrap_status"},
    {"method": "GET",  "path": "/api/operator/bootstrapStatus",   "handler": "api_get_operator_bootstrap_status"},
    {"method": "GET",  "path": "/api/operator/readiness",         "handler": "api_get_readiness"},
    {"method": "GET",  "path": "/api/operator/health",            "handler": "api_get_health"},
    {"method": "GET",  "path": "/api/operator/logs",              "handler": "api_get_operator_logs"},
    {"method": "GET",  "path": "/api/operator/stderr_tail",       "handler": "api_get_operator_stderr_tail"},
    {"method": "GET",  "path": "/api/operator/db_schema",         "handler": "api_get_schema_audit"},
    {"method": "GET",  "path": "/api/operator/snapshot",          "handler": "api_get_support_snapshot"},
    {"method": "GET",  "path": "/api/operator/market_data",       "handler": "api_get_operator_market_data"},
    {"method": "GET",  "path": "/api/operator/strategy_decisions","handler": "api_get_operator_strategy_decisions"},
    {"method": "GET",  "path": "/api/operator/preflight",         "handler": "api_get_operator_preflight"},

    {"method": "POST", "path": "/api/operator/start",             "handler": "api_post_operator_start"},
    {"method": "POST", "path": "/api/operator/bootstrap",         "handler": "api_post_operator_bootstrap"},
    {"method": "POST", "path": "/api/operator/stop",              "handler": "api_post_operator_stop"},
    {"method": "POST", "path": "/api/operator/restart",           "handler": "api_post_operator_restart"},
    {"method": "POST", "path": "/api/operator/restart_engine",    "handler": "api_post_operator_restart"},
    {"method": "POST", "path": "/api/operator/restart_feeds",     "handler": "api_post_operator_restart_feeds"},
    {"method": "POST", "path": "/api/operator/emergency_stop",    "handler": "api_post_operator_emergency_stop"},
    {"method": "POST", "path": "/api/operator/broker_risk",       "handler": "api_post_operator_broker_risk"},
    {"method": "POST", "path": "/api/operator/execution_arm",     "handler": "api_post_operator_execution_arm"},
    {"method": "POST", "path": "/api/operator/clear_manual_halt", "handler": "api_post_operator_clear_manual_halt"},
    {"method": "POST", "path": "/api/operator/autofix",           "handler": "api_post_operator_autofix"},
    {"method": "POST", "path": "/api/operator/clear_last_error",  "handler": "api_post_operator_clear_last_error"},
    {"method": "POST", "path": "/api/operator/clearLastError",    "handler": "api_post_operator_clear_last_error"},
    {"method": "GET",  "path": "/api/operator/institutional_check","handler": "api_get_operator_institutional_check"},
    {"method": "GET",  "path": "/api/operator/institutionalCheck","handler": "api_get_operator_institutional_check"},


    {"method": "GET",  "path": "/api/training_status",       "handler": "api_get_training_status"},
    {"method": "GET",  "path": "/api/pnl",                   "handler": "api_get_pnl"},
    {"method": "GET",  "path": "/api/status",                "handler": "api_get_status"},
    {"method": "GET",  "path": "/api/liveness",             "handler": "api_get_liveness"},
    {"method": "GET",  "path": "/api/system/config",         "handler": "api_get_runtime_config"},
    {"method": "GET",  "path": "/api/supervisor/status",     "handler": "api_get_supervisor_status"},
    {"method": "GET",  "path": "/api/ingestion/status",      "handler": "api_get_ingestion_status"},
    {"method": "GET",  "path": "/api/risk/portfolio",        "handler": "api_get_portfolio_risk"},
    {"method": "GET",  "path": "/api/market/session",        "handler": "api_get_market_session"},
    {"method": "GET",  "path": "/api/pnl/summary",           "handler": "api_get_pnl_summary"},
    {"method": "GET",  "path": "/api/risk/summary",          "handler": "api_get_risk_summary"},
    {"method": "GET",  "path": "/api/models/status",         "handler": "api_get_models_status"},
    {"method": "POST", "path": "/api/models/promote",        "handler": "api_post_models_promote"},

    # JOBS
    {"method": "GET",  "path": "/api/jobs",               "handler": "api_get_jobs"},
    {"method": "GET",  "path": "/api/jobs/catalog",       "handler": "api_get_jobs_catalog"},
    {"method": "POST", "path": "/api/jobs/start",         "handler": "api_post_job_start"},
    {"method": "POST", "path": "/api/jobs/stop",          "handler": "api_post_job_stop"},
    {"method": "GET",  "path": "/api/jobs/log",           "handler": "api_get_job_log"},
    {"method": "GET",  "path": "/api/jobs/history",       "handler": "api_get_job_history"},
    {"method": "POST", "path": "/api/pipeline/run",       "handler": "api_post_pipeline_run"},

    # OPS
    {"method": "GET", "path": "/api/alerts",                          "handler": "api_get_alerts"},
    {"method": "GET", "path": "/api/notifications/status",            "handler": "api_get_notifications_status"},
    {"method": "POST", "path": "/api/notifications/test",             "handler": "api_post_notifications_test"},
    {"method": "GET", "path": "/api/alerts/timeline",                 "handler": "api_get_alerts"},
    {"method": "GET", "path": "/api/feeds",                           "handler": "api_get_feeds"},
    {"method": "GET", "path": "/api/validation",                      "handler": "api_get_validation"},
    {"method": "GET", "path": "/api/model/diagnostics",               "handler": "api_get_model_diagnostics"},
    {"method": "GET", "path": "/api/model_registry",                  "handler": "api_get_model_registry"},
    {"method": "GET", "path": "/api/model/registry",                  "handler": "api_get_model_registry"},
    {"method": "GET", "path": "/api/embed_model_eval",                "handler": "api_get_embed_model_eval"},
    {"method": "GET", "path": "/api/embed_conf_calib",                "handler": "api_get_embed_conf_calib"},
    {"method": "GET", "path": "/api/temporal_eval",                   "handler": "api_get_temporal_eval"},
    {"method": "GET", "path": "/api/temporal/eval",                   "handler": "api_get_temporal_eval"},
    {"method": "GET", "path": "/api/temporal_models",                 "handler": "api_get_temporal_models"},
    {"method": "GET", "path": "/api/temporal/models",                 "handler": "api_get_temporal_models"},
    {"method": "GET", "path": "/api/backtest/portfolio/latest",       "handler": "api_get_latest_portfolio_backtest"},
    {"method": "GET", "path": "/api/portfolio/backtest/latest",       "handler": "api_get_latest_portfolio_backtest"},
    {"method": "GET", "path": "/api/execution_metrics",               "handler": "api_get_execution_metrics"},
    {"method": "GET", "path": "/api/execution/metrics",               "handler": "api_get_execution_metrics"},
    {"method": "GET", "path": "/api/execution/stats",                 "handler": "api_get_execution_stats"},
    {"method": "GET", "path": "/api/execution_metrics/rolling",       "handler": "api_get_execution_metrics_rolling"},
    {"method": "GET", "path": "/api/execution/metrics/rolling",       "handler": "api_get_execution_metrics_rolling"},
    {"method": "GET", "path": "/api/execution_metrics/by_symbol",     "handler": "api_get_execution_metrics_by_symbol"},
    {"method": "GET", "path": "/api/execution/metrics/by_symbol",     "handler": "api_get_execution_metrics_by_symbol"},
    {"method": "GET", "path": "/api/execution_metrics/by_confidence", "handler": "api_get_execution_cost_by_confidence"},
    {"method": "GET", "path": "/api/execution/metrics/by_confidence", "handler": "api_get_execution_cost_by_confidence"},
    {"method": "GET", "path": "/api/confidence_mass",                 "handler": "api_get_confidence_mass"},
    {"method": "GET", "path": "/api/social/features",                 "handler": "api_get_social_features"},
    {"method": "GET", "path": "/api/social/regimes",                  "handler": "api_get_social_regimes"},
    {"method": "GET", "path": "/api/social/blocks",                   "handler": "api_get_social_blocks"},
    {"method": "GET", "path": "/api/relevance_stats",                 "handler": "api_get_relevance_stats"},
    {"method": "GET", "path": "/api/relevance/stats",                 "handler": "api_get_relevance_stats"},
    {"method": "POST","path": "/api/champion/rollback",               "handler": "api_post_rollback"},

    # EXECUTION / PORTFOLIO / PROMOTION (UI hard-deps)
    {"method": "GET", "path": "/api/market_stress",                   "handler": "api_get_market_stress"},
    {"method": "GET", "path": "/api/market_stress_history",           "handler": "api_get_market_stress_history"},
    {"method": "GET", "path": "/api/portfolio",                       "handler": "api_get_portfolio"},
    {"method": "GET", "path": "/api/portfolio/backtest",              "handler": "api_get_portfolio_backtest"},
    {"method": "GET",  "path": "/api/prices",                          "handler": "api_get_prices"},
    {"method": "GET",  "path": "/api/trades",                          "handler": "api_get_trades"},
    {"method": "GET", "path": "/api/broker",                          "handler": "api_get_broker"},
    {"method": "GET", "path": "/api/strategy/status",                 "handler": "api_get_strategy_status"},
    {"method": "GET", "path": "/api/strategy_metrics",                "handler": "api_get_strategy_metrics"},
    {"method": "GET", "path": "/api/strategy/metrics",                "handler": "api_get_strategy_metrics"},
    {"method": "GET", "path": "/api/reconcile/broker_backtest",       "handler": "api_get_reconcile_broker_backtest"},
    {"method": "GET", "path": "/api/equity_drift",                    "handler": "api_get_equity_drift"},
    {"method": "GET", "path": "/api/temporal_shadow_eval",            "handler": "api_get_temporal_shadow_eval"},
    {"method": "GET", "path": "/api/temporal/shadow_eval",            "handler": "api_get_temporal_shadow_eval"},
    {"method": "GET", "path": "/api/promotion_audit",                 "handler": "api_get_promotion_audit"},
    {"method": "GET", "path": "/api/promotion/audit",                 "handler": "api_get_promotion_audit"},
    {"method": "GET", "path": "/api/causal/scores",                   "handler": "api_get_causal_scores"},
    {"method": "GET", "path": "/api/promotion/status",                "handler": "api_get_promotion_status"},
    {"method": "GET", "path": "/api/governance/summary",              "handler": "api_get_governance_summary"},

    # UI hard-deps present in ui/dashboard.js but missing from ROUTE_SPECS_* in this repo
    {"method": "GET",  "path": "/api/system/kill_switches",           "handler": "api_get_kill_switches"},  # alias
    {"method": "GET",  "path": "/api/alerts/by_id",                   "handler": "api_get_alert_by_id"},
    {"method": "POST", "path": "/api/alerts/{id}/ack",                "handler": "api_post_alert_ack"},
    {"method": "POST", "path": "/api/alerts/{id}/shelve",             "handler": "api_post_alert_shelve"},
    {"method": "POST", "path": "/api/alerts/{id}/resolve",            "handler": "api_post_alert_resolve"},
    {"method": "GET",  "path": "/api/ui/decisions",                   "handler": "api_get_recent_decisions"},
    {"method": "GET",  "path": "/api/ui/decision",                    "handler": "api_get_decision_detail"},
    {"method": "GET",  "path": "/api/data/feature_visibility",        "handler": "api_get_feature_visibility"},
    {"method": "GET",  "path": "/api/audit/records",                  "handler": "api_get_audit_records"},
    {"method": "POST", "path": "/api/ui/interaction",                 "handler": "api_post_ui_interaction"},
    {"method": "POST", "path": "/api/copilot/ask",                    "handler": "api_post_copilot_ask"},
    {"method": "GET",  "path": "/api/promotion/explain",              "handler": "api_get_promotion_explain"},
    {"method": "POST", "path": "/api/promotion/enable",               "handler": "api_post_promotion_enable"},
    {"method": "POST", "path": "/api/system/fix",                     "handler": "api_post_system_fix"},
    {"method": "GET",  "path": "/api/size_policy",                    "handler": "api_get_size_policy"},
    {"method": "GET",  "path": "/api/strategy/size_policy",           "handler": "api_get_size_policy"},
    {"method": "POST", "path": "/api/size_policy/train",              "handler": "api_post_size_policy_train"},
    {"method": "POST", "path": "/api/strategy/size_policy/train",     "handler": "api_post_size_policy_train"},
    {"method": "GET",  "path": "/api/model_metrics",                  "handler": "api_get_model_metrics"},
    {"method": "GET",  "path": "/api/model/metrics",                  "handler": "api_get_model_metrics"},
    {"method": "GET",  "path": "/api/execution_overlays",             "handler": "api_get_execution_overlays"},
    {"method": "GET",  "path": "/api/execution/overlays",             "handler": "api_get_execution_overlays"},
    {"method": "GET",  "path": "/api/crash_analytics",                "handler": "api_get_crash_analytics"},
    {"method": "GET",  "path": "/api/news/latest",                    "handler": "api_get_news_latest"},
    {"method": "GET",  "path": "/api/news/sentiment",                 "handler": "api_get_news_sentiment"},


    # TERMINAL
    {"method": "GET",  "path": "/api/terminal/watchlist",             "handler": "api_get_terminal_watchlist"},
    {"method": "GET",  "path": "/api/terminal/snapshot",              "handler": "api_get_terminal_snapshot"},
    {"method": "GET",  "path": "/api/terminal/positions",             "handler": "api_get_terminal_positions"},
    {"method": "GET",  "path": "/api/terminal/orders",                "handler": "api_get_terminal_orders"},
    {"method": "GET",  "path": "/api/terminal/fills",                 "handler": "api_get_terminal_fills"},
    {"method": "GET",  "path": "/api/terminal/equity",                "handler": "api_get_terminal_equity"},
    {"method": "GET",  "path": "/api/terminal/markers",               "handler": "api_get_terminal_markers"},
    {"method": "GET",  "path": "/api/terminal/decision_overlays",     "handler": "api_get_terminal_decision_overlays"},

    {"method": "POST", "path": "/api/terminal/order",                 "handler": "api_post_terminal_order"},
    {"method": "POST", "path": "/api/terminal/flatten",               "handler": "api_post_terminal_flatten"},
]


def normalize_route_specs(route_specs: list[Any] | tuple[Any, ...]) -> list[dict[str, str]]:
    seen = set()
    out = []

    for route in route_specs:
        if isinstance(route, dict):
            method = str(route.get("method", "")).upper().strip()
            path = str(route.get("path", "")).strip()
            handler = str(route.get("handler", "")).strip()
        elif isinstance(route, tuple) and len(route) >= 3:
            method = str(route[0]).upper().strip()
            path = str(route[1]).strip()
            handler = str(route[2]).strip()
        else:
            continue

        if not method or not path or not handler:
            continue

        key = (method, path)
        if key in seen:
            continue

        seen.add(key)
        out.append({
            "method": method,
            "path": path,
            "handler": handler,
        })

    return out


def build_raw_route_specs(
    *route_groups: list[Any] | tuple[Any, ...],
    fallback_route_specs: list[dict[str, str]] | None = None,
) -> list[Any]:
    raw: list[Any] = []
    for group in route_groups:
        raw.extend(list(group or []))
    raw.extend(
        list(FALLBACK_ROUTE_SPECS if fallback_route_specs is None else fallback_route_specs)
    )
    return raw


def filter_route_specs_for_handlers(
    route_specs: list[dict[str, str]],
    api_handlers: dict[str, Any],
) -> list[dict[str, str]]:
    missing_handlers = find_missing_route_handlers(route_specs, api_handlers)
    if missing_handlers:
        raise RuntimeError(
            "route_handler_registration_failed: "
            + json.dumps(missing_handlers[:50], sort_keys=True)
        )
    return list(route_specs)


def find_missing_route_handlers(
    route_specs: list[dict[str, str]],
    api_handlers: dict[str, Any],
) -> list[dict[str, str]]:
    missing_handlers: list[dict[str, str]] = []
    for route in route_specs:
        handler_name = str(route.get("handler") or "").strip()
        if not handler_name:
            missing_handlers.append(
                {
                    "method": str(route.get("method") or ""),
                    "path": str(route.get("path") or ""),
                    "handler": handler_name,
                    "reason": "blank_handler",
                }
            )
            continue
        handler = api_handlers.get(handler_name)
        if handler_name not in api_handlers or not callable(handler):
            missing_handlers.append(
                {
                    "method": str(route.get("method") or ""),
                    "path": str(route.get("path") or ""),
                    "handler": handler_name,
                    "reason": "handler_not_registered",
                }
            )
    return missing_handlers


def validate_canonical_route_owners(
    *,
    route_specs: list[dict[str, str]],
    api_handlers: dict[str, Any],
    canonical_route_owners: dict[tuple[str, str], dict[str, str]],
) -> None:
    route_index = {
        (str(route.get("method") or "").upper(), str(route.get("path") or "")): str(
            route.get("handler") or ""
        )
        for route in route_specs
    }
    for route_key, owner in canonical_route_owners.items():
        handler_name = str(owner.get("handler") or "")
        registered_handler = route_index.get(route_key)
        if registered_handler != handler_name:
            raise RuntimeError(
                f"canonical route owner mismatch for {route_key}: "
                f"expected handler {handler_name!r}, got {registered_handler!r}"
            )
        handler = api_handlers.get(handler_name)
        if not callable(handler):
            raise RuntimeError(f"canonical route handler {handler_name!r} is not callable")
        if getattr(handler, "__module__", "") != str(owner.get("module") or ""):
            raise RuntimeError(
                f"canonical route handler {handler_name!r} came from "
                f"{getattr(handler, '__module__', '')!r}, expected {owner.get('module')!r}"
            )
        if getattr(handler, "__name__", "") != str(owner.get("name") or ""):
            raise RuntimeError(
                f"canonical route handler {handler_name!r} resolved to "
                f"{getattr(handler, '__name__', '')!r}, expected {owner.get('name')!r}"
            )
