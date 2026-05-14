"""Static API SQL identifier allowlist and quoting helpers."""

from __future__ import annotations

try:  # pragma: no cover - exercised when psycopg is available.
    from psycopg import sql as _psycopg_sql
except Exception:  # pragma: no cover - fallback for minimal test envs.
    _psycopg_sql = None


_ALLOWED_TABLES: frozenset[str] = frozenset(
    {
        "alert_acks",
        "alert_resolutions",
        "alerts",
        "broker_fills",
        "broker_fills_v2",
        "broker_positions",
        "decision_log",
        "decisions",
        "events",
        "execution_fills",
        "execution_metrics",
        "execution_mode_audit",
        "execution_policy_audit",
        "execution_orders",
        "job_history",
        "job_locks",
        "kill_switch_audit",
        "labels",
        "model_promotion_audit",
        "pnl_attribution",
        "portfolio_state",
        "position_reconcile_audit",
        "prices",
        "promotion_statistical_evidence",
        "social_features",
        "social_regimes",
        "trade_attribution_ledger",
        "trade_decisions",
        "trades",
    }
)


def require_allowed_table_name(table_name: str) -> str:
    table_name = str(table_name or "").strip()
    assert table_name in _ALLOWED_TABLES, f"unauthorized_table:{table_name}"
    return table_name


def sql_identifier(table_name: str) -> str:
    table_name = require_allowed_table_name(table_name)
    if _psycopg_sql is not None:
        return _psycopg_sql.Identifier(table_name).as_string()
    return '"' + table_name.replace('"', '""') + '"'


__all__ = ["_ALLOWED_TABLES", "require_allowed_table_name", "sql_identifier"]
