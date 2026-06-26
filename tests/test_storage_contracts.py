from __future__ import annotations

import ast
import importlib
import inspect
import re
import sqlite3
import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

_TABLE_RE = re.compile(
    r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+([A-Za-z_][A-Za-z0-9_]*)",
    re.IGNORECASE,
)

# These helpers are the additive schema fragments that the standard init path
# re-applies after the long bootstrap transaction. They define the durable
# canonical table set for a fresh DB alongside init_db's main executescript.
_POST_BOOTSTRAP_STORAGE_HELPERS = (
    "_ensure_runtime_baseline_schema",
    "_ensure_runtime_aux_schema",
    "_ensure_strategy_metrics_schema",
    "_ensure_universe_audit_schema",
    "_ensure_universe_pit_schema",
    "_ensure_prices_schema",
    "_ensure_price_quotes_schema",
    "_ensure_price_quotes_raw_schema",
    "_ensure_price_provider_health_schema",
    "_ensure_ingestion_pipeline_health_schema",
    "_ensure_price_feed_lock_schema",
    "_ensure_labels_price_schema",
    "_ensure_execution_analytics_schema",
    "_ensure_kill_switch_schema",
    "_ensure_trade_attribution_ledger_schema",
    "_ensure_options_chain_schema",
    "_ensure_options_chain_v2_schema",
    "_ensure_options_symbol_ingestion_state_schema",
    "_ensure_insider_transactions_schema",
    "_ensure_congressional_trades_schema",
    "_ensure_weather_schema",
)

_EXTERNAL_SCHEMA_MODULES = (
    ("engine.runtime.storage_live_ingestion_schema", "SCHEMA"),
    ("engine.execution.execution_ledger", "SCHEMA"),
    ("engine.execution.order_idempotency", "SCHEMA"),
    ("engine.execution.order_command_boundary", "SCHEMA"),
    ("engine.strategy.portfolio", "SCHEMA"),
    ("engine.execution.broker_sim", "SCHEMA"),
)
_EXTERNAL_SCHEMA_FUNCTIONS = (
    ("engine.strategy.net_after_cost_labels", "ensure_net_after_cost_labels_schema"),
    ("engine.data.structured_document_events", "ensure_structured_document_event_schema"),
)
_EXTERNAL_SCHEMA_TABLE_ATTRS = (
    ("engine.strategy.net_after_cost_labels", "TABLE_NAME"),
)
_OWNED_LIVE_TABLE_OWNER_MODULES = {
    "prices": {
        "engine/runtime/storage_live_ingestion_schema.py",
        # Legacy bootstrap still seeds `prices`; repo validation blocks any new
        # owned-table DDL outside this existing boundary.
        "engine/runtime/jobs/repair_schema.py",
    },
    "price_quotes": {"engine/runtime/storage_live_ingestion_schema.py"},
    "price_quotes_raw": {
        "engine/runtime/storage_live_ingestion_schema.py",
        "engine/runtime/schema/migrations/0067_price_quotes_raw_event_key_conflict.py",
    },
    "price_provider_health": {"engine/runtime/storage_live_ingestion_schema.py"},
    "ingestion_pipeline_health": {"engine/runtime/storage_live_ingestion_schema.py"},
    "price_feed_lock": {"engine/runtime/storage_live_ingestion_schema.py"},
    "options_symbol_ingestion_state": {"engine/runtime/storage_live_ingestion_schema.py"},
}
_OWNED_LIVE_TABLE_DDL_PATTERNS = {
    table_name: re.compile(
        (
            rf"\bCREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+{re.escape(table_name)}\b"
            rf"|\bALTER\s+TABLE\s+{re.escape(table_name)}\b"
            rf"|\bDROP\s+TABLE(?:\s+IF\s+EXISTS)?\s+{re.escape(table_name)}\b"
            rf"|\bCREATE\s+(?:UNIQUE\s+)?INDEX(?:\s+IF\s+NOT\s+EXISTS)?\s+\S+\s+ON\s+{re.escape(table_name)}\b"
        ),
        re.IGNORECASE | re.MULTILINE,
    )
    for table_name in _OWNED_LIVE_TABLE_OWNER_MODULES
}
_OWNED_LIVE_TABLE_REPO_SCAN_SKIP_PARTS = {
    ".claude",
    ".venv",
    ".git",
    "node_modules",
    "tests",
    "docs",
    "__pycache__",
}

_DOCUMENTED_INDEXES = {
    "idx_prices_symbol_ts",
    "idx_price_quotes_symbol_ts",
    "idx_price_quotes_ts",
    "idx_price_quotes_raw_symbol_ts",
    "idx_price_quotes_raw_provider_ts",
    "idx_price_quotes_raw_ts",
    "idx_price_quotes_raw_provider_event_ts",
    "idx_price_provider_health_ts",
    "idx_price_provider_health_provider",
    "idx_ingestion_pipeline_health_ts",
    "idx_ingestion_pipeline_health_pipeline",
    "idx_options_symbol_ingestion_disabled",
    "idx_event_log_ts",
    "idx_event_log_type_ts",
    "idx_event_log_entity",
    "idx_event_log_corr",
    "idx_job_checkpoints_updated",
    "idx_decision_log_ts",
    "idx_decision_log_symbol_ts",
    "idx_decision_log_model_ts",
    "idx_predictions_ts",
    "idx_predictions_symbol_ts",
    "idx_predictions_model_ts",
    "idx_alerts_ts",
    "idx_alerts_event_id",
    "idx_alerts_prediction_id",
    "idx_alerts_severity_ts",
    "idx_alerts_symbol_ts",
    "uq_alerts_id_prediction_lineage",
    "idx_temporal_model_eval_ts",
    "idx_portfolio_state_updated_ts",
    "idx_portfolio_orders_ts",
    "idx_portfolio_orders_symbol_ts",
    "idx_portfolio_orders_model_ts",
    "idx_portfolio_orders_source_alert_ts",
    "idx_portfolio_orders_prediction_ts",
    "idx_portfolio_orders_source_alert_prediction_ts",
    "uq_portfolio_orders_id_source_prediction_lineage",
    "idx_execution_orders_submit_ts",
    "idx_execution_orders_source_alert",
    "idx_execution_orders_portfolio_order_submit_ts",
    "idx_execution_orders_prediction_submit_ts",
    "idx_execution_orders_source_alert_prediction_submit_ts",
    "idx_execution_orders_model_submit_ts",
    "idx_execution_orders_symbol_submit_ts",
    "idx_execution_orders_order_uid",
    "uq_execution_order_idempotency_client",
    "idx_execution_order_idempotency_status",
    "idx_execution_order_idempotency_symbol_ts",
    "idx_order_commands_ts",
    "idx_order_commands_batch_mode",
    "idx_order_events_ts",
    "idx_order_events_command_ts",
    "idx_order_events_type_ts",
    "idx_execution_fills_ts",
    "idx_execution_fills_client",
    "idx_execution_fills_model_ts",
    "idx_execution_fills_model_symbol_ts",
    "idx_execution_fills_portfolio_order_ts",
    "idx_execution_fills_source_alert_ts",
    "idx_execution_fills_prediction_ts",
    "idx_execution_fills_source_alert_prediction_ts",
    "idx_execution_fills_symbol_ts",
    "idx_execution_fills_fill_id",
    "uq_execution_fills_client_fillid",
    "idx_pnl_attribution_prediction_ts",
    "idx_pnl_attribution_ts",
    "idx_pnl_attribution_model_ts",
}

_CRITICAL_TABLE_SPECS = {
    "runtime_meta": {
        "key": {"type": "TEXT", "pk": 1},
        "value": {"type": "TEXT", "pk": 0},
        "updated_ts_ms": {"type": "INTEGER", "pk": 0},
    },
    "schema_version": {
        "version": {"type": "INTEGER", "pk": 1},
        "applied_ts_ms": {"type": "INTEGER", "pk": 0, "notnull": True},
        "status": {"type": "TEXT", "pk": 0, "notnull": True},
        "notes": {"type": "TEXT", "pk": 0},
    },
    "prices": {
        "ts_ms": {"type": "INTEGER", "pk": 2, "notnull": True},
        "symbol": {"type": "TEXT", "pk": 1, "notnull": True},
        "price": {"type": "REAL", "pk": 0},
        "px": {"type": "REAL", "pk": 0},
        "source": {"type": "TEXT", "pk": 0},
    },
    "price_quotes": {
        "ts_ms": {"type": "INTEGER", "pk": 2, "notnull": True},
        "symbol": {"type": "TEXT", "pk": 1, "notnull": True},
        "last": {"type": "REAL", "pk": 0},
        "bid": {"type": "REAL", "pk": 0},
        "ask": {"type": "REAL", "pk": 0},
        "spread": {"type": "REAL", "pk": 0},
        "volume": {"type": "REAL", "pk": 0},
        "source": {"type": "TEXT", "pk": 0},
        "last_trade_ts_ms": {"type": "INTEGER", "pk": 0},
        "last_quote_ts_ms": {"type": "INTEGER", "pk": 0},
        "last_update_ts_ms": {"type": "INTEGER", "pk": 0},
    },
    "price_quotes_raw": {
        "ts_ms": {"type": "INTEGER", "pk": 4, "notnull": True},
        "symbol": {"type": "TEXT", "pk": 1, "notnull": True},
        "provider": {"type": "TEXT", "pk": 2, "notnull": True},
        "event_key": {"type": "TEXT", "pk": 3, "notnull": True},
        "event_type": {"type": "TEXT", "pk": 0},
        "event_ts_ms": {"type": "INTEGER", "pk": 0},
        "last": {"type": "REAL", "pk": 0},
        "bid": {"type": "REAL", "pk": 0},
        "ask": {"type": "REAL", "pk": 0},
        "spread": {"type": "REAL", "pk": 0},
        "volume": {"type": "REAL", "pk": 0},
        "trade_ts_ms": {"type": "INTEGER", "pk": 0},
        "quote_ts_ms": {"type": "INTEGER", "pk": 0},
        "ingest_ts_ms": {"type": "INTEGER", "pk": 0},
        "source": {"type": "TEXT", "pk": 0},
    },
    "price_provider_health": {
        "ts_ms": {"type": "INTEGER", "pk": 2, "notnull": True},
        "provider": {"type": "TEXT", "pk": 1, "notnull": True},
        "ok": {"type": "INTEGER", "pk": 0, "notnull": True},
        "latency_ms": {"type": "INTEGER", "pk": 0},
        "n_symbols": {"type": "INTEGER", "pk": 0},
        "error": {"type": "TEXT", "pk": 0},
        "last_success_ts_ms": {"type": "INTEGER", "pk": 0},
        "error_count": {"type": "INTEGER", "pk": 0, "notnull": True, "default": "0"},
    },
    "ingestion_pipeline_health": {
        "ts_ms": {"type": "INTEGER", "pk": 2, "notnull": True},
        "pipeline": {"type": "TEXT", "pk": 1, "notnull": True},
        "ok": {"type": "INTEGER", "pk": 0, "notnull": True},
        "latency_ms": {"type": "INTEGER", "pk": 0},
        "raw_rows": {"type": "INTEGER", "pk": 0, "notnull": True, "default": "0"},
        "event_rows": {"type": "INTEGER", "pk": 0, "notnull": True, "default": "0"},
        "last_ingested_ts_ms": {"type": "INTEGER", "pk": 0},
        "error": {"type": "TEXT", "pk": 0},
        "meta_json": {"type": "TEXT", "pk": 0},
    },
    "price_feed_lock": {
        "id": {"type": "INTEGER", "pk": 1},
        "owner": {"type": "TEXT", "pk": 0, "notnull": True},
        "pid": {"type": "INTEGER", "pk": 0, "notnull": True},
        "ts_ms": {"type": "INTEGER", "pk": 0, "notnull": True},
    },
    "options_symbol_ingestion_state": {
        "symbol": {"type": "TEXT", "pk": 1, "notnull": True},
        "provider": {"type": "TEXT", "pk": 0, "notnull": True, "default": "''"},
        "consecutive_failures": {"type": "INTEGER", "pk": 0, "notnull": True, "default": "0"},
        "total_failures": {"type": "INTEGER", "pk": 0, "notnull": True, "default": "0"},
        "last_failure_ts_ms": {"type": "INTEGER", "pk": 0},
        "last_failure_error": {"type": "TEXT", "pk": 0},
        "last_success_ts_ms": {"type": "INTEGER", "pk": 0},
        "last_fresh_snapshot_ts_ms": {"type": "INTEGER", "pk": 0},
        "last_cached_snapshot_ts_ms": {"type": "INTEGER", "pk": 0},
        "last_fallback_ts_ms": {"type": "INTEGER", "pk": 0},
        "last_row_count": {"type": "INTEGER", "pk": 0, "notnull": True, "default": "0"},
        "disabled_until_ts_ms": {"type": "INTEGER", "pk": 0, "notnull": True, "default": "0"},
        "updated_ts_ms": {"type": "INTEGER", "pk": 0, "notnull": True},
    },
    "event_log": {
        "id": {"type": "INTEGER", "pk": 1},
        "ts_ms": {"type": "INTEGER", "pk": 0, "notnull": True},
        "event_type": {"type": "TEXT", "pk": 0, "notnull": True},
        "event_source": {"type": "TEXT", "pk": 0, "notnull": True},
        "event_version": {"type": "INTEGER", "pk": 0, "notnull": True, "default": "1"},
        "entity_type": {"type": "TEXT", "pk": 0},
        "entity_id": {"type": "TEXT", "pk": 0},
        "correlation_id": {"type": "TEXT", "pk": 0},
        "payload_json": {"type": "TEXT", "pk": 0, "notnull": True},
    },
    "job_locks": {
        "job_name": {"type": "TEXT", "pk": 1},
        "owner": {"type": "TEXT", "pk": 0, "notnull": True},
        "pid": {"type": "INTEGER", "pk": 0, "notnull": True},
        "acquired_ts_ms": {"type": "INTEGER", "pk": 0, "notnull": True},
        "heartbeat_ts_ms": {"type": "INTEGER", "pk": 0, "notnull": True},
        "expires_ms": {"type": "INTEGER", "pk": 0},
    },
    "job_heartbeats": {
        "job_name": {"type": "TEXT", "pk": 1},
        "owner": {"type": "TEXT", "pk": 0, "notnull": True},
        "pid": {"type": "INTEGER", "pk": 0, "notnull": True},
        "ts_ms": {"type": "INTEGER", "pk": 0, "notnull": True},
        "extra_json": {"type": "TEXT", "pk": 0},
    },
    "job_checkpoints": {
        "job_name": {"type": "TEXT", "pk": 1},
        "last_event_id": {"type": "INTEGER", "pk": 0},
        "last_event_ts_ms": {"type": "INTEGER", "pk": 0},
        "updated_ts_ms": {"type": "INTEGER", "pk": 0, "notnull": True},
    },
    "decision_log": {
        "id": {"type": "INTEGER", "pk": 1},
        "ts_ms": {"type": "INTEGER", "pk": 0, "notnull": True},
        "event_id": {"type": "INTEGER", "pk": 0, "notnull": True},
        "symbol": {"type": "TEXT", "pk": 0, "notnull": True},
        "horizon_s": {"type": "INTEGER", "pk": 0, "notnull": True},
        "predicted_z": {"type": "REAL", "pk": 0, "notnull": True},
        "confidence": {"type": "REAL", "pk": 0, "notnull": True},
        "model_name": {"type": "TEXT", "pk": 0, "notnull": True},
        "model_kind": {"type": "TEXT", "pk": 0},
        "model_ts_ms": {"type": "INTEGER", "pk": 0},
        "model_version": {"type": "TEXT", "pk": 0},
        "features_hash": {"type": "TEXT", "pk": 0},
        "feature_set_tag": {"type": "TEXT", "pk": 0},
        "features_json": {"type": "TEXT", "pk": 0},
        "explain_json": {"type": "TEXT", "pk": 0},
        "extra_json": {"type": "TEXT", "pk": 0},
        "components_json": {"type": "TEXT", "pk": 0},
        "component_vector": {"type": "TEXT", "pk": 0},
        "prev_hash": {"type": "BLOB", "pk": 0},
        "row_hash": {"type": "BLOB", "pk": 0},
    },
    "predictions": {
        "id": {"type": "INTEGER", "pk": 1},
        "ts_ms": {"type": "INTEGER", "pk": 0, "notnull": True},
        "event_id": {"type": "INTEGER", "pk": 0, "notnull": True},
        "symbol": {"type": "TEXT", "pk": 0, "notnull": True},
        "horizon_s": {"type": "INTEGER", "pk": 0, "notnull": True},
        "predicted_z": {"type": "REAL", "pk": 0, "notnull": True},
        "confidence": {"type": "REAL", "pk": 0, "notnull": True},
        "confidence_raw": {"type": "REAL", "pk": 0},
        "prediction_strength": {"type": "REAL", "pk": 0},
        "model_name": {"type": "TEXT", "pk": 0},
        "model_id": {"type": "TEXT", "pk": 0},
        "model_version": {"type": "TEXT", "pk": 0},
        "regime_time_ms": {"type": "INTEGER", "pk": 0},
        "volatility_regime": {"type": "TEXT", "pk": 0, "notnull": True, "default": "'unknown'"},
        "trend_regime": {"type": "TEXT", "pk": 0, "notnull": True, "default": "'unknown'"},
        "liquidity_regime": {"type": "TEXT", "pk": 0, "notnull": True, "default": "'unknown'"},
    },
    "alerts": {
        "id": {"type": "INTEGER", "pk": 1},
        "ts_ms": {"type": "INTEGER", "pk": 0, "notnull": True},
        "event_id": {"type": "INTEGER", "pk": 0},
        "prediction_id": {"type": "INTEGER", "pk": 0},
        "event_title": {"type": "TEXT", "pk": 0, "notnull": True},
        "symbol": {"type": "TEXT", "pk": 0, "notnull": True},
        "horizon_s": {"type": "INTEGER", "pk": 0, "notnull": True},
        "expected_z": {"type": "REAL", "pk": 0, "notnull": True},
        "confidence": {"type": "REAL", "pk": 0, "notnull": True},
        "severity": {"type": "TEXT", "pk": 0, "notnull": True},
        "rule_id": {"type": "TEXT", "pk": 0, "notnull": True},
        "explain_json": {"type": "TEXT", "pk": 0},
        "dedupe_key": {"type": "TEXT", "pk": 0, "notnull": True},
        "title": {"type": "TEXT", "pk": 0},
        "message": {"type": "TEXT", "pk": 0},
        "source": {"type": "TEXT", "pk": 0},
        "status": {"type": "TEXT", "pk": 0, "notnull": True, "default": "'open'"},
        "detail_json": {"type": "TEXT", "pk": 0},
        "updated_ts_ms": {"type": "INTEGER", "pk": 0, "default": "0"},
        "model_name": {"type": "TEXT", "pk": 0},
        "model_id": {"type": "TEXT", "pk": 0},
        "model_version": {"type": "TEXT", "pk": 0},
        "portfolio_first_seen_ts_ms": {"type": "INTEGER", "pk": 0, "notnull": True, "default": "0"},
        "portfolio_last_seen_ts_ms": {"type": "INTEGER", "pk": 0, "notnull": True, "default": "0"},
        "portfolio_consumed_ts_ms": {"type": "INTEGER", "pk": 0, "notnull": True, "default": "0"},
        "portfolio_expired_ts_ms": {"type": "INTEGER", "pk": 0, "notnull": True, "default": "0"},
        "portfolio_status": {"type": "TEXT", "pk": 0, "notnull": True, "default": "'new'"},
    },
    "equity_drift": {
        "ts_ms": {"type": "INTEGER", "pk": 1},
        "broker_equity": {"type": "REAL", "pk": 0, "notnull": True},
        "backtest_equity": {"type": "REAL", "pk": 0, "notnull": True},
        "diff_equity": {"type": "REAL", "pk": 0, "notnull": True},
        "diff_equity_pct": {"type": "REAL", "pk": 0, "notnull": True},
        "level": {"type": "TEXT", "pk": 0, "notnull": True},
        "reason": {"type": "TEXT", "pk": 0},
        "backtest_run_id": {"type": "INTEGER", "pk": 0},
        "backtest_ts_ms": {"type": "INTEGER", "pk": 0},
        "detail_json": {"type": "TEXT", "pk": 0},
    },
    "portfolio_state": {
        "model_id": {"type": "TEXT", "pk": 1, "notnull": True, "default": "'baseline'"},
        "symbol": {"type": "TEXT", "pk": 2, "notnull": True},
        "side": {"type": "TEXT", "pk": 0, "notnull": True},
        "weight": {"type": "REAL", "pk": 0, "notnull": True},
        "opened_ts_ms": {"type": "INTEGER", "pk": 0, "notnull": True},
        "updated_ts_ms": {"type": "INTEGER", "pk": 0, "notnull": True},
        "source_alert_id": {"type": "INTEGER", "pk": 0},
        "explain_json": {"type": "TEXT", "pk": 0},
    },
    "portfolio_orders": {
        "id": {"type": "INTEGER", "pk": 1},
        "ts_ms": {"type": "INTEGER", "pk": 0, "notnull": True},
        "model_id": {"type": "TEXT", "pk": 0, "notnull": True, "default": "'baseline'"},
        "symbol": {"type": "TEXT", "pk": 0, "notnull": True},
        "action": {"type": "TEXT", "pk": 0, "notnull": True},
        "from_side": {"type": "TEXT", "pk": 0, "notnull": True},
        "to_side": {"type": "TEXT", "pk": 0, "notnull": True},
        "from_weight": {"type": "REAL", "pk": 0, "notnull": True},
        "to_weight": {"type": "REAL", "pk": 0, "notnull": True},
        "delta_weight": {"type": "REAL", "pk": 0, "notnull": True},
        "source_alert_id": {"type": "INTEGER", "pk": 0},
        "prediction_id": {"type": "INTEGER", "pk": 0},
        "explain_json": {"type": "TEXT", "pk": 0},
    },
    "execution_orders": {
        "client_order_id": {"type": "TEXT", "pk": 1},
        "order_uid": {"type": "TEXT", "pk": 0},
        "idempotency_status": {"type": "TEXT", "pk": 0},
        "broker": {"type": "TEXT", "pk": 0, "notnull": True},
        "portfolio_orders_id": {"type": "INTEGER", "pk": 0},
        "source_alert_id": {"type": "INTEGER", "pk": 0},
        "prediction_id": {"type": "INTEGER", "pk": 0},
        "model_id": {"type": "TEXT", "pk": 0, "notnull": True, "default": "'baseline'"},
        "model_version": {"type": "TEXT", "pk": 0},
        "symbol": {"type": "TEXT", "pk": 0, "notnull": True},
        "qty": {"type": "REAL", "pk": 0, "notnull": True},
        "submit_ts_ms": {"type": "INTEGER", "pk": 0, "notnull": True},
        "ref_px": {"type": "REAL", "pk": 0},
        "expected_px": {"type": "REAL", "pk": 0},
        "mid_px": {"type": "REAL", "pk": 0},
        "bid_px": {"type": "REAL", "pk": 0},
        "ask_px": {"type": "REAL", "pk": 0},
        "spread_bps": {"type": "REAL", "pk": 0},
        "broker_order_id": {"type": "TEXT", "pk": 0},
        "status": {"type": "TEXT", "pk": 0, "notnull": True, "default": "'submitted'"},
        "extra_json": {"type": "TEXT", "pk": 0},
    },
    "execution_order_idempotency": {
        "order_uid": {"type": "TEXT", "pk": 1},
        "broker": {"type": "TEXT", "pk": 0, "notnull": True},
        "portfolio_orders_id": {"type": "INTEGER", "pk": 0},
        "portfolio_ts_ms": {"type": "INTEGER", "pk": 0},
        "source_order_id": {"type": "INTEGER", "pk": 0},
        "source_alert_id": {"type": "INTEGER", "pk": 0},
        "symbol": {"type": "TEXT", "pk": 0, "notnull": True},
        "client_order_id": {"type": "TEXT", "pk": 0, "notnull": True},
        "broker_order_id": {"type": "TEXT", "pk": 0},
        "status": {"type": "TEXT", "pk": 0, "notnull": True},
        "first_seen_ts_ms": {"type": "INTEGER", "pk": 0, "notnull": True},
        "claimed_ts_ms": {"type": "INTEGER", "pk": 0, "notnull": True},
        "updated_ts_ms": {"type": "INTEGER", "pk": 0, "notnull": True},
        "submit_ts_ms": {"type": "INTEGER", "pk": 0},
        "last_error": {"type": "TEXT", "pk": 0},
        "payload_json": {"type": "TEXT", "pk": 0, "notnull": True},
    },
    "order_commands": {
        "command_id": {"type": "TEXT", "pk": 1},
        "ts_ms": {"type": "INTEGER", "pk": 0, "notnull": True},
        "updated_ts_ms": {"type": "INTEGER", "pk": 0, "notnull": True},
        "batch_id": {"type": "INTEGER", "pk": 0},
        "payload_ts_ms": {"type": "INTEGER", "pk": 0},
        "correlation_id": {"type": "TEXT", "pk": 0},
        "mode": {"type": "TEXT", "pk": 0, "notnull": True},
        "broker": {"type": "TEXT", "pk": 0, "notnull": True},
        "payload_source": {"type": "TEXT", "pk": 0, "notnull": True},
        "status": {"type": "TEXT", "pk": 0, "notnull": True, "default": "'ready'"},
        "real_order_count": {"type": "INTEGER", "pk": 0, "notnull": True, "default": "0"},
        "shadow_order_count": {"type": "INTEGER", "pk": 0, "notnull": True, "default": "0"},
        "blocked_order_count": {"type": "INTEGER", "pk": 0, "notnull": True, "default": "0"},
        "command_json": {"type": "TEXT", "pk": 0, "notnull": True},
        "result_json": {"type": "TEXT", "pk": 0},
    },
    "order_events": {
        "id": {"type": "INTEGER", "pk": 1},
        "ts_ms": {"type": "INTEGER", "pk": 0, "notnull": True},
        "command_id": {"type": "TEXT", "pk": 0},
        "batch_id": {"type": "INTEGER", "pk": 0},
        "correlation_id": {"type": "TEXT", "pk": 0},
        "event_type": {"type": "TEXT", "pk": 0, "notnull": True},
        "mode": {"type": "TEXT", "pk": 0, "notnull": True},
        "broker": {"type": "TEXT", "pk": 0, "notnull": True},
        "status": {"type": "TEXT", "pk": 0, "notnull": True},
        "payload_json": {"type": "TEXT", "pk": 0, "notnull": True},
    },
    "execution_fills": {
        "id": {"type": "INTEGER", "pk": 1},
        "client_order_id": {"type": "TEXT", "pk": 0, "notnull": True},
        "fill_id": {"type": "TEXT", "pk": 0},
        "broker": {"type": "TEXT", "pk": 0},
        "model_id": {"type": "TEXT", "pk": 0, "notnull": True, "default": "'baseline'"},
        "model_version": {"type": "TEXT", "pk": 0},
        "symbol": {"type": "TEXT", "pk": 0},
        "portfolio_orders_id": {"type": "INTEGER", "pk": 0},
        "source_alert_id": {"type": "INTEGER", "pk": 0},
        "prediction_id": {"type": "INTEGER", "pk": 0},
        "ts_ms": {"type": "INTEGER", "pk": 0},
        "submit_ts_ms": {"type": "INTEGER", "pk": 0},
        "fill_ts_ms": {"type": "INTEGER", "pk": 0, "notnull": True},
        "fill_qty": {"type": "REAL", "pk": 0, "notnull": True},
        "fill_px": {"type": "REAL", "pk": 0, "notnull": True},
        "expected_px": {"type": "REAL", "pk": 0},
        "mid_px": {"type": "REAL", "pk": 0},
        "bid_px": {"type": "REAL", "pk": 0},
        "ask_px": {"type": "REAL", "pk": 0},
        "spread_bps": {"type": "REAL", "pk": 0},
        "slippage_bps": {"type": "REAL", "pk": 0},
        "fill_latency_ms": {"type": "INTEGER", "pk": 0},
        "fees": {"type": "REAL", "pk": 0},
        "commission": {"type": "REAL", "pk": 0},
        "liquidity": {"type": "TEXT", "pk": 0},
        "raw_json": {"type": "TEXT", "pk": 0},
        "extra_json": {"type": "TEXT", "pk": 0},
    },
    "pnl_attribution": {
        "ts_ms": {"type": "INTEGER", "pk": 1, "notnull": True},
        "source_alert_id": {"type": "INTEGER", "pk": 2, "notnull": True},
        "prediction_id": {"type": "INTEGER", "pk": 0},
        "model_id": {"type": "TEXT", "pk": 3, "notnull": True, "default": "'baseline'"},
        "model_version": {"type": "TEXT", "pk": 0},
        "symbol": {"type": "TEXT", "pk": 4, "notnull": True},
        "pnl": {"type": "REAL", "pk": 0, "notnull": True},
        "fees": {"type": "REAL", "pk": 0, "notnull": True},
        "slippage_bps": {"type": "REAL", "pk": 0},
        "position_size": {"type": "REAL", "pk": 0},
        "avg_price": {"type": "REAL", "pk": 0},
        "realized_pnl": {"type": "REAL", "pk": 0},
        "unrealized_pnl": {"type": "REAL", "pk": 0},
        "extra_json": {"type": "TEXT", "pk": 0},
    },
}


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


def _parse_names(pattern: re.Pattern[str], texts: list[str]) -> set[str]:
    names = set()
    for text in texts:
        for match in pattern.finditer(text):
            name = str(match.group(1) or "").strip()
            if name and "legacy" not in name.lower():
                names.add(name)
    return names


def _canonical_schema_sources(storage_module) -> list[str]:
    sources = [inspect.getsource(storage_module.init_db)]
    for helper_name in _POST_BOOTSTRAP_STORAGE_HELPERS:
        sources.append(inspect.getsource(getattr(storage_module, helper_name)))
    for module_name, attr_name in _EXTERNAL_SCHEMA_MODULES:
        module = importlib.import_module(module_name)
        sources.append(str(getattr(module, attr_name)))
    for module_name, attr_name in _EXTERNAL_SCHEMA_FUNCTIONS:
        module = importlib.import_module(module_name)
        sources.append(inspect.getsource(getattr(module, attr_name)))
    for module_name, attr_name in _EXTERNAL_SCHEMA_TABLE_ATTRS:
        module = importlib.import_module(module_name)
        table_name = str(getattr(module, attr_name))
        sources.append(f"CREATE TABLE IF NOT EXISTS {table_name}")
    return sources


def _expected_tables_from_sources(storage_module) -> set[str]:
    return _parse_names(_TABLE_RE, _canonical_schema_sources(storage_module))


def _repo_owned_live_table_ddl_locations() -> dict[str, list[str]]:
    hits: dict[str, set[str]] = {
        table_name: set()
        for table_name in _OWNED_LIVE_TABLE_OWNER_MODULES
    }
    for path in (REPO_ROOT / "engine").rglob("*.py"):
        if any(part in _OWNED_LIVE_TABLE_REPO_SCAN_SKIP_PARTS for part in path.parts):
            continue
        rel_path = path.relative_to(REPO_ROOT).as_posix()
        text = path.read_text(encoding="utf-8", errors="ignore")
        for table_name, pattern in _OWNED_LIVE_TABLE_DDL_PATTERNS.items():
            if pattern.search(text):
                hits[table_name].add(rel_path)
    return {
        table_name: sorted(paths)
        for table_name, paths in hits.items()
    }


def _actual_tables(db_path: Path) -> set[str]:
    with sqlite3.connect(str(db_path)) as con:
        rows = con.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
            """
        ).fetchall()
    return {str(row[0]) for row in rows}


def _actual_indexes(db_path: Path) -> set[str]:
    with sqlite3.connect(str(db_path)) as con:
        rows = con.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type='index' AND name NOT LIKE 'sqlite_%'
            """
        ).fetchall()
    return {str(row[0]) for row in rows}


def _table_columns(db_path: Path, table_name: str) -> dict[str, dict[str, object]]:
    with sqlite3.connect(str(db_path)) as con:
        rows = con.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {
        str(row[1]): {
            "type": str(row[2] or "").upper(),
            "notnull": bool(row[3]),
            "default": None if row[4] is None else str(row[4]),
            "pk": int(row[5] or 0),
        }
        for row in rows
    }


def _table_foreign_keys(db_path: Path, table_name: str) -> list[dict[str, object]]:
    with sqlite3.connect(str(db_path)) as con:
        rows = con.execute(f"PRAGMA foreign_key_list({table_name})").fetchall()
    return [
        {
            "id": int(row[0]),
            "seq": int(row[1]),
            "table": str(row[2]),
            "from": str(row[3]),
            "to": str(row[4]),
            "on_update": str(row[5]),
            "on_delete": str(row[6]),
        }
        for row in rows
    ]


def _foreign_key_groups(db_path: Path, table_name: str) -> list[dict[str, object]]:
    groups: dict[int, dict[str, object]] = {}
    for row in _table_foreign_keys(db_path, table_name):
        fk_id = int(row["id"])
        group = groups.setdefault(
            fk_id,
            {
                "table": str(row["table"]),
                "from": [],
                "to": [],
                "on_update": str(row["on_update"]),
                "on_delete": str(row["on_delete"]),
            },
        )
        group["from"].append((int(row["seq"]), str(row["from"])))
        group["to"].append((int(row["seq"]), str(row["to"])))
    return [
        {
            "table": str(group["table"]),
            "from": tuple(name for _, name in sorted(group["from"])),
            "to": tuple(name for _, name in sorted(group["to"])),
            "on_update": str(group["on_update"]),
            "on_delete": str(group["on_delete"]),
        }
        for _, group in sorted(groups.items())
    ]


def _assert_column_contract(db_path: Path, table_name: str, expected_columns: dict[str, dict[str, object]]) -> None:
    actual_columns = _table_columns(db_path, table_name)
    assert set(actual_columns) == set(expected_columns), (
        f"{table_name} columns mismatch: "
        f"expected={sorted(expected_columns)} actual={sorted(actual_columns)}"
    )
    for column_name, expected_spec in expected_columns.items():
        actual_spec = actual_columns[column_name]
        assert actual_spec["type"] == expected_spec["type"], (
            f"{table_name}.{column_name} type mismatch: "
            f"expected={expected_spec['type']} actual={actual_spec['type']}"
        )
        if "pk" in expected_spec:
            assert actual_spec["pk"] == expected_spec["pk"], (
                f"{table_name}.{column_name} pk mismatch: "
                f"expected={expected_spec['pk']} actual={actual_spec['pk']}"
            )
        if "notnull" in expected_spec:
            assert actual_spec["notnull"] == expected_spec["notnull"], (
                f"{table_name}.{column_name} notnull mismatch: "
                f"expected={expected_spec['notnull']} actual={actual_spec['notnull']}"
            )
        if "default" in expected_spec:
            assert actual_spec["default"] == expected_spec["default"], (
                f"{table_name}.{column_name} default mismatch: "
                f"expected={expected_spec['default']} actual={actual_spec['default']}"
            )


@pytest.fixture()
def storage_runtime(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db_path = tmp_path / "storage_contracts.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("TS_STORAGE_BACKEND", "sqlite")
    monkeypatch.setenv("TIMESCALE_ENABLED", "0")
    monkeypatch.setenv("SQLITE_LIVENESS_DB_ENABLED", "0")
    monkeypatch.setenv("SQLITE_LIVENESS_QUEUE_ENABLED", "0")
    monkeypatch.delenv("TIMESCALE_DSN", raising=False)
    monkeypatch.delenv("TIMESCALE_URL", raising=False)
    monkeypatch.delenv("TIMESCALE_DATABASE_URL", raising=False)
    monkeypatch.delenv("SQLITE_LIVENESS_DB_PATH", raising=False)
    monkeypatch.setenv("FEATURE_STORE_ENABLED", "0")
    monkeypatch.setenv("FEATURE_STORE_INIT_ON_STARTUP", "0")

    _, storage = _reload_modules(
        "engine.runtime.db_guard",
        "engine.runtime.storage",
    )

    try:
        yield {"db_path": db_path, "storage": storage}
    finally:
        try:
            storage.shutdown_timeseries_storage(timeout_s=0.1)
        except Exception:
            pass
        try:
            storage.close_pooled_connections()
        except Exception:
            pass


@pytest.fixture()
def initialized_storage(storage_runtime):
    storage_runtime["storage"].init_db()
    return storage_runtime


def test_schema_initializes_without_error(storage_runtime) -> None:
    storage = storage_runtime["storage"]
    db_path = storage_runtime["db_path"]

    storage.init_db()

    assert db_path.exists(), f"expected initialized DB at {db_path}"
    validation = storage.get_db_validation_snapshot()
    assert validation["ok"] is True, validation
    assert validation["missing_tables"] == [], validation
    assert validation["missing_columns"] == {}, validation
    assert validation["missing_indexes"] == [], validation
    assert validation["schema_version"] == storage.SCHEMA_VERSION, validation
    assert validation["expected_schema_version"] == storage.SCHEMA_VERSION, validation
    assert validation["schema_version_ok"] is True, validation
    assert validation["schema_status"] == "applied", validation


def test_all_expected_tables_exist(initialized_storage) -> None:
    storage = initialized_storage["storage"]
    db_path = initialized_storage["db_path"]

    expected_tables = _expected_tables_from_sources(storage)
    actual_tables = _actual_tables(db_path)
    missing_tables = sorted(expected_tables - actual_tables)

    assert expected_tables, "source-derived canonical table catalog was empty"
    assert missing_tables == [], f"missing tables: {missing_tables}"


def test_critical_tables_have_expected_columns(initialized_storage) -> None:
    db_path = initialized_storage["db_path"]

    for table_name, expected_columns in _CRITICAL_TABLE_SPECS.items():
        _assert_column_contract(db_path, table_name, expected_columns)


def test_documented_indexes_exist(initialized_storage) -> None:
    db_path = initialized_storage["db_path"]
    actual_indexes = _actual_indexes(db_path)
    missing_indexes = sorted(_DOCUMENTED_INDEXES - actual_indexes)

    assert missing_indexes == [], f"missing indexes: {missing_indexes}"


def test_trade_lineage_prediction_foreign_keys_exist(initialized_storage) -> None:
    db_path = initialized_storage["db_path"]

    expected_groups = {
        "alerts": {
            ("prediction_id",): ("predictions", ("id",)),
        },
        "portfolio_orders": {
            ("prediction_id",): ("predictions", ("id",)),
            ("source_alert_id", "prediction_id"): ("alerts", ("id", "prediction_id")),
        },
        "execution_orders": {
            ("portfolio_orders_id",): ("portfolio_orders", ("id",)),
            ("prediction_id",): ("predictions", ("id",)),
            ("source_alert_id", "prediction_id"): ("alerts", ("id", "prediction_id")),
            ("portfolio_orders_id", "source_alert_id", "prediction_id"): ("portfolio_orders", ("id", "source_alert_id", "prediction_id")),
        },
        "execution_fills": {
            ("portfolio_orders_id",): ("portfolio_orders", ("id",)),
            ("prediction_id",): ("predictions", ("id",)),
            ("source_alert_id", "prediction_id"): ("alerts", ("id", "prediction_id")),
            ("portfolio_orders_id", "source_alert_id", "prediction_id"): ("portfolio_orders", ("id", "source_alert_id", "prediction_id")),
        },
    }
    for table_name, expected in expected_groups.items():
        foreign_keys = _foreign_key_groups(db_path, table_name)
        for from_cols, expected_target in expected.items():
            matches = [
                fk
                for fk in foreign_keys
                if (
                    tuple(fk["from"]),
                    str(fk["table"]),
                    tuple(fk["to"]),
                ) == (tuple(from_cols), str(expected_target[0]), tuple(expected_target[1]))
            ]
            assert matches, f"missing foreign key on {table_name}: {foreign_keys}"
            assert all(str(fk["on_delete"]).upper() == "SET NULL" for fk in matches), foreign_keys


def test_trade_lineage_constraints_reject_mismatched_alert_prediction(initialized_storage) -> None:
    storage = initialized_storage["storage"]
    con = storage.connect()
    try:
        prediction_one = int(
            con.execute(
                """
                INSERT INTO predictions(
                  ts_ms, event_id, symbol, horizon_s, predicted_z, confidence,
                  confidence_raw, prediction_strength, model_name, model_id, model_version
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                (1_710_000_000_000, 11, "AAPL", 300, 1.0, 0.9, 0.9, 0.8, "m", "m1", "v1"),
            ).lastrowid
            or 0
        )
        prediction_two = int(
            con.execute(
                """
                INSERT INTO predictions(
                  ts_ms, event_id, symbol, horizon_s, predicted_z, confidence,
                  confidence_raw, prediction_strength, model_name, model_id, model_version
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                (1_710_000_001_000, 12, "AAPL", 300, 0.8, 0.7, 0.7, 0.6, "m", "m1", "v1"),
            ).lastrowid
            or 0
        )
        con.execute(
            """
            INSERT INTO alerts(
              id, ts_ms, event_id, prediction_id, event_title, symbol, horizon_s, expected_z, confidence,
              severity, rule_id, explain_json, dedupe_key, model_name, model_id, model_version
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                501,
                1_710_000_002_000,
                11,
                int(prediction_one),
                "typed lineage",
                "AAPL",
                300,
                1.0,
                0.9,
                "HIGH",
                "rule.lineage",
                "{}",
                "AAPL:300:rule.lineage:501",
                "m",
                "m1",
                "v1",
            ),
        )
        with pytest.raises(sqlite3.IntegrityError):
            con.execute(
                """
                INSERT INTO execution_orders(
                  client_order_id, broker, source_alert_id, prediction_id, model_id, symbol, qty, submit_ts_ms, status, extra_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                ("cid-mismatch", "paper", 501, int(prediction_two), "m1", "AAPL", 1.0, 1_710_000_003_000, "submitted", "{}"),
            )
    finally:
        con.close()


def test_init_db_backfills_job_heartbeats_owner_column(storage_runtime) -> None:
    storage = storage_runtime["storage"]
    db_path = storage_runtime["db_path"]

    with sqlite3.connect(str(db_path)) as con:
        con.execute(
            """
            CREATE TABLE job_heartbeats (
                job_name TEXT PRIMARY KEY,
                pid INTEGER,
                ts_ms INTEGER NOT NULL,
                status TEXT,
                extra_json TEXT
            )
            """
        )

    storage.init_db()

    actual_columns = _table_columns(db_path, "job_heartbeats")

    assert "owner" in actual_columns
    assert actual_columns["owner"]["type"] == "TEXT"
    assert actual_columns["owner"]["notnull"] is True


def test_init_db_backfills_legacy_live_ingestion_columns(storage_runtime) -> None:
    storage = storage_runtime["storage"]
    db_path = storage_runtime["db_path"]

    with sqlite3.connect(str(db_path)) as con:
        con.execute(
            """
            CREATE TABLE prices (
                ts_ms INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                price REAL,
                PRIMARY KEY(symbol, ts_ms)
            )
            """
        )
        con.execute(
            """
            CREATE TABLE price_quotes (
                ts_ms INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                last REAL,
                bid REAL,
                ask REAL,
                spread REAL,
                volume REAL,
                PRIMARY KEY(symbol, ts_ms)
            )
            """
        )
        con.execute(
            """
            CREATE TABLE price_provider_health (
                ts_ms INTEGER NOT NULL,
                provider TEXT NOT NULL,
                ok INTEGER NOT NULL,
                latency_ms INTEGER,
                n_symbols INTEGER,
                error TEXT,
                PRIMARY KEY(provider, ts_ms)
            )
            """
        )
        con.execute(
            """
            CREATE TABLE ingestion_pipeline_health (
                ts_ms INTEGER NOT NULL,
                pipeline TEXT NOT NULL,
                ok INTEGER NOT NULL,
                PRIMARY KEY(pipeline, ts_ms)
            )
            """
        )
        con.execute(
            """
            CREATE TABLE price_feed_lock (
                id INTEGER PRIMARY KEY
            )
            """
        )
        con.execute(
            """
            CREATE TABLE options_symbol_ingestion_state (
                symbol TEXT NOT NULL PRIMARY KEY
            )
            """
        )
        con.execute("INSERT INTO prices(ts_ms, symbol, price) VALUES (1001, 'AAPL', 101.25)")
        con.execute(
            """
            INSERT INTO price_quotes(ts_ms, symbol, last, bid, ask, spread, volume)
            VALUES (1001, 'AAPL', 101.25, 101.0, 101.5, 0.5, 10.0)
            """
        )
        con.execute(
            """
            INSERT INTO price_provider_health(ts_ms, provider, ok, latency_ms, n_symbols, error)
            VALUES (1002, 'polygon', 1, 25, 5, NULL)
            """
        )
        con.execute(
            """
            INSERT INTO ingestion_pipeline_health(ts_ms, pipeline, ok)
            VALUES (1003, 'poll_prices', 1)
            """
        )
        con.execute("INSERT INTO price_feed_lock(id) VALUES (1)")
        con.execute("INSERT INTO options_symbol_ingestion_state(symbol) VALUES ('SPY')")

    storage.init_db()

    prices_columns = _table_columns(db_path, "prices")
    quotes_columns = _table_columns(db_path, "price_quotes")
    provider_columns = _table_columns(db_path, "price_provider_health")
    pipeline_columns = _table_columns(db_path, "ingestion_pipeline_health")
    lock_columns = _table_columns(db_path, "price_feed_lock")
    options_state_columns = _table_columns(db_path, "options_symbol_ingestion_state")

    assert "px" in prices_columns
    assert "source" in prices_columns
    assert "source" in quotes_columns
    assert "last_trade_ts_ms" in quotes_columns
    assert "last_quote_ts_ms" in quotes_columns
    assert "last_update_ts_ms" in quotes_columns
    assert "last_success_ts_ms" in provider_columns
    assert "error_count" in provider_columns
    assert "latency_ms" in pipeline_columns
    assert "raw_rows" in pipeline_columns
    assert "event_rows" in pipeline_columns
    assert "last_ingested_ts_ms" in pipeline_columns
    assert "error" in pipeline_columns
    assert "meta_json" in pipeline_columns
    assert "owner" in lock_columns
    assert "pid" in lock_columns
    assert "ts_ms" in lock_columns
    assert "provider" in options_state_columns
    assert "consecutive_failures" in options_state_columns
    assert "total_failures" in options_state_columns
    assert "last_failure_ts_ms" in options_state_columns
    assert "last_failure_error" in options_state_columns
    assert "last_success_ts_ms" in options_state_columns
    assert "last_fresh_snapshot_ts_ms" in options_state_columns
    assert "last_cached_snapshot_ts_ms" in options_state_columns
    assert "last_fallback_ts_ms" in options_state_columns
    assert "last_row_count" in options_state_columns
    assert "disabled_until_ts_ms" in options_state_columns
    assert "updated_ts_ms" in options_state_columns

    with sqlite3.connect(str(db_path)) as con:
        price_row = con.execute("SELECT ts_ms, symbol, price FROM prices").fetchone()
        quote_row = con.execute(
            "SELECT symbol, last, source, last_trade_ts_ms, last_quote_ts_ms, last_update_ts_ms FROM price_quotes"
        ).fetchone()
        provider_row = con.execute(
            "SELECT provider, ok, error_count FROM price_provider_health"
        ).fetchone()
        pipeline_row = con.execute(
            "SELECT pipeline, ok, raw_rows, event_rows FROM ingestion_pipeline_health"
        ).fetchone()
        lock_row = con.execute("SELECT id, owner, pid, ts_ms FROM price_feed_lock").fetchone()
        options_state_row = con.execute(
            """
            SELECT
                symbol,
                provider,
                consecutive_failures,
                total_failures,
                last_row_count,
                disabled_until_ts_ms,
                updated_ts_ms
            FROM options_symbol_ingestion_state
            """
        ).fetchone()

    assert price_row == (1001, "AAPL", 101.25)
    assert quote_row == ("AAPL", 101.25, None, None, None, None)
    assert provider_row == ("polygon", 1, 0)
    assert pipeline_row == ("poll_prices", 1, 0, 0)
    assert lock_row == (1, "", 0, 0)
    assert options_state_row == ("SPY", "", 0, 0, 0, 0, 0)


def test_init_db_rebuilds_legacy_prices_schema_to_owned_contract(storage_runtime) -> None:
    storage = storage_runtime["storage"]
    db_path = storage_runtime["db_path"]

    with sqlite3.connect(str(db_path)) as con:
        con.execute(
            """
            CREATE TABLE prices (
                ts_ms INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                price REAL NOT NULL,
                source TEXT,
                provider TEXT,
                ingest_ts_ms INTEGER,
                PRIMARY KEY (ts_ms, symbol)
            )
            """
        )
        con.execute(
            """
            INSERT INTO prices(ts_ms, symbol, price, source, provider, ingest_ts_ms)
            VALUES (4001, 'IWM', 201.5, 'legacy_feed', 'legacy_provider', 4001)
            """
        )

    storage.init_db()

    actual_columns = _table_columns(db_path, "prices")
    assert set(actual_columns) == {"ts_ms", "symbol", "price", "px", "source"}
    assert actual_columns["symbol"]["pk"] == 1
    assert actual_columns["ts_ms"]["pk"] == 2
    assert "provider" not in actual_columns
    assert "ingest_ts_ms" not in actual_columns

    with sqlite3.connect(str(db_path)) as con:
        row = con.execute(
            "SELECT ts_ms, symbol, price, px, source FROM prices"
        ).fetchone()

    assert row == (4001, "IWM", 201.5, 201.5, "legacy_feed")


def test_init_db_rebuilds_prices_extra_json_drift_to_owned_contract(storage_runtime) -> None:
    storage = storage_runtime["storage"]
    db_path = storage_runtime["db_path"]

    with sqlite3.connect(str(db_path)) as con:
        con.execute(
            """
            CREATE TABLE prices (
                ts_ms INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                price REAL,
                px REAL,
                source TEXT,
                extra_json TEXT,
                PRIMARY KEY(symbol, ts_ms)
            )
            """
        )
        con.execute(
            """
            INSERT INTO prices(ts_ms, symbol, price, px, source, extra_json)
            VALUES (4002, 'IWM', 202.5, 202.5, 'legacy_feed', '{"bid":202.4}')
            """
        )

    storage.init_db()

    actual_columns = _table_columns(db_path, "prices")
    assert set(actual_columns) == {"ts_ms", "symbol", "price", "px", "source"}
    assert actual_columns["symbol"]["pk"] == 1
    assert actual_columns["ts_ms"]["pk"] == 2

    with sqlite3.connect(str(db_path)) as con:
        row = con.execute(
            "SELECT ts_ms, symbol, price, px, source FROM prices"
        ).fetchone()

    assert row == (4002, "IWM", 202.5, 202.5, "legacy_feed")


def test_init_db_repairs_legacy_events_for_normalized_options_dq(storage_runtime) -> None:
    storage = storage_runtime["storage"]
    db_path = storage_runtime["db_path"]

    with sqlite3.connect(str(db_path)) as con:
        con.execute(
            """
            CREATE TABLE events (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts_ms INTEGER NOT NULL,
              timestamp INTEGER,
              event_type TEXT,
              symbol TEXT,
              source TEXT,
              title TEXT,
              body TEXT,
              url TEXT,
              event_key TEXT,
              importance_score REAL,
              meta_json TEXT
            )
            """
        )
        con.execute(
            """
            INSERT INTO events(ts_ms, timestamp, event_type, symbol, source, title, event_key, meta_json)
            VALUES (5001, 5001, 'legacy', 'SPY', 'legacy', 'legacy event', 'legacy:5001', '{}')
            """
        )

    storage.init_db()

    actual_columns = _table_columns(db_path, "events")
    for column_name in (
        "raw_payload",
        "derived_features",
        "source_id",
        "dedupe_hash",
        "event_key",
        "ts_ms",
    ):
        assert column_name in actual_columns

    assert "ux_events_event_key_ts_ms" in _actual_indexes(db_path)
    with sqlite3.connect(str(db_path)) as con:
        index_columns = [
            str(row[2])
            for row in con.execute("PRAGMA index_info(ux_events_event_key_ts_ms)").fetchall()
        ]
        row = con.execute(
            "SELECT raw_payload, derived_features, source_id, dedupe_hash FROM events WHERE event_key=?",
            ("legacy:5001",),
        ).fetchone()

    assert index_columns == ["event_key", "ts_ms"]
    assert row == (None, None, None, None)


def test_init_db_rebuilds_legacy_price_quotes_raw_schema(storage_runtime) -> None:
    storage = storage_runtime["storage"]
    db_path = storage_runtime["db_path"]

    with sqlite3.connect(str(db_path)) as con:
        con.execute(
            """
            CREATE TABLE price_quotes_raw (
                ts_ms INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                provider TEXT NOT NULL,
                last REAL,
                bid REAL,
                ask REAL,
                spread REAL,
                volume REAL,
                PRIMARY KEY(symbol, provider, ts_ms)
            )
            """
        )
        con.execute(
            """
            INSERT INTO price_quotes_raw(ts_ms, symbol, provider, last, bid, ask, spread, volume)
            VALUES (2001, 'MSFT', 'polygon', 301.0, 300.5, 301.5, 1.0, 10.0)
            """
        )

    storage.init_db()

    actual_columns = _table_columns(db_path, "price_quotes_raw")
    assert "event_key" in actual_columns
    assert "event_type" in actual_columns
    assert "event_ts_ms" in actual_columns
    assert "trade_ts_ms" in actual_columns
    assert "quote_ts_ms" in actual_columns
    assert "ingest_ts_ms" in actual_columns
    assert "source" in actual_columns

    with sqlite3.connect(str(db_path)) as con:
        row = con.execute(
            """
            SELECT symbol, provider, event_key, event_type, event_ts_ms, trade_ts_ms, quote_ts_ms, ingest_ts_ms, source
            FROM price_quotes_raw
            """
        ).fetchone()

    assert row == (
        "MSFT",
        "polygon",
        "legacy:MSFT:polygon:legacy:2001:2001:2001:2001",
        "legacy",
        2001,
        2001,
        2001,
        2001,
        "polygon",
    )


def test_init_db_rebuilds_price_quotes_raw_pk_drift(storage_runtime) -> None:
    storage = storage_runtime["storage"]
    db_path = storage_runtime["db_path"]

    with sqlite3.connect(str(db_path)) as con:
        con.execute(
            """
            CREATE TABLE price_quotes_raw (
                ts_ms INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                provider TEXT NOT NULL,
                event_key TEXT NOT NULL,
                event_type TEXT,
                event_ts_ms INTEGER,
                last REAL,
                bid REAL,
                ask REAL,
                spread REAL,
                volume REAL,
                trade_ts_ms INTEGER,
                quote_ts_ms INTEGER,
                ingest_ts_ms INTEGER,
                source TEXT,
                PRIMARY KEY(symbol, provider, event_key)
            )
            """
        )
        con.execute(
            """
            INSERT INTO price_quotes_raw(
                ts_ms, symbol, provider, event_key, event_type, event_ts_ms,
                last, bid, ask, spread, volume, trade_ts_ms, quote_ts_ms, ingest_ts_ms, source
            )
            VALUES (2002, 'MSFT', 'polygon', 'evt-1', 'trade', 2002, 302.0, 301.5, 302.5, 1.0, 11.0, 2002, 2002, 2002, 'polygon')
            """
        )

    storage.init_db()

    actual_columns = _table_columns(db_path, "price_quotes_raw")
    assert actual_columns["symbol"]["pk"] == 1
    assert actual_columns["provider"]["pk"] == 2
    assert actual_columns["event_key"]["pk"] == 3
    assert actual_columns["ts_ms"]["pk"] == 4

    with sqlite3.connect(str(db_path)) as con:
        row = con.execute(
            """
            SELECT symbol, provider, event_key, event_type, event_ts_ms, trade_ts_ms, quote_ts_ms, ingest_ts_ms, source
            FROM price_quotes_raw
            """
        ).fetchone()

    assert row == ("MSFT", "polygon", "evt-1", "trade", 2002, 2002, 2002, 2002, "polygon")


def test_init_db_reapplies_live_ingestion_schema_when_version_markers_are_current(storage_runtime) -> None:
    storage = storage_runtime["storage"]
    db_path = storage_runtime["db_path"]

    storage.init_db()
    storage._INIT_DB_READY_PATH = ""

    with sqlite3.connect(str(db_path)) as con:
        con.execute("DROP TABLE IF EXISTS price_feed_lock")
        con.execute("DROP TABLE IF EXISTS price_quotes")
        con.execute(
            """
            CREATE TABLE price_quotes (
                ts_ms INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                last REAL,
                bid REAL,
                ask REAL,
                spread REAL,
                volume REAL,
                PRIMARY KEY(symbol, ts_ms)
            )
            """
        )
        con.execute(
            "CREATE INDEX idx_price_quotes_symbol_ts ON price_quotes(symbol, ts_ms)"
        )
        con.execute("CREATE INDEX idx_price_quotes_ts ON price_quotes(ts_ms)")
        con.execute(
            """
            INSERT INTO price_quotes(ts_ms, symbol, last, bid, ask, spread, volume)
            VALUES (3001, 'QQQ', 500.0, 499.5, 500.5, 1.0, 12.0)
            """
        )
        con.execute("DROP TABLE IF EXISTS options_symbol_ingestion_state")
        con.execute(
            """
            CREATE TABLE options_symbol_ingestion_state (
                symbol TEXT NOT NULL PRIMARY KEY,
                disabled_until_ts_ms INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        con.execute(
            """
            CREATE INDEX idx_options_symbol_ingestion_disabled
            ON options_symbol_ingestion_state(disabled_until_ts_ms)
            """
        )
        con.execute(
            "INSERT INTO options_symbol_ingestion_state(symbol, disabled_until_ts_ms) VALUES ('QQQ', 0)"
        )

    storage.init_db()

    validation = storage.get_db_validation_snapshot(include_quick_check=False)
    assert validation["ok"] is True, validation

    quotes_columns = _table_columns(db_path, "price_quotes")
    lock_columns = _table_columns(db_path, "price_feed_lock")
    options_state_columns = _table_columns(db_path, "options_symbol_ingestion_state")

    assert "source" in quotes_columns
    assert "last_trade_ts_ms" in quotes_columns
    assert "owner" in lock_columns
    assert "pid" in lock_columns
    assert "provider" in options_state_columns
    assert "updated_ts_ms" in options_state_columns


def test_live_ingestion_schema_ddl_is_storage_owned() -> None:
    ddl_locations = _repo_owned_live_table_ddl_locations()
    unexpected_locations = {
        table_name: [
            path
            for path in paths
            if path not in _OWNED_LIVE_TABLE_OWNER_MODULES[table_name]
        ]
        for table_name, paths in ddl_locations.items()
    }
    unexpected_locations = {
        table_name: paths
        for table_name, paths in unexpected_locations.items()
        if paths
    }

    assert unexpected_locations == {}, unexpected_locations


def test_no_unexpected_orphan_tables_if_repo_has_canonical_catalog(initialized_storage) -> None:
    storage = initialized_storage["storage"]
    db_path = initialized_storage["db_path"]

    expected_tables = _expected_tables_from_sources(storage)
    actual_tables = _actual_tables(db_path)
    extra_tables = sorted(actual_tables - expected_tables)

    assert expected_tables, "source-derived canonical table catalog was empty"
    assert extra_tables == [], f"unexpected tables: {extra_tables}"


def test_storage_helpers_can_connect_and_query_basic_state(initialized_storage) -> None:
    storage = initialized_storage["storage"]
    db_path = initialized_storage["db_path"]

    assert Path(storage.DB_PATH) == db_path

    ro_con = storage.connect_ro()
    try:
        schema_row = ro_con.execute(
            "SELECT value FROM runtime_meta WHERE key='schema_version'"
        ).fetchone()
    finally:
        ro_con.close()

    rw_con = storage.connect(readonly=False)
    try:
        lock_count_row = rw_con.execute("SELECT COUNT(*) FROM job_locks").fetchone()
    finally:
        rw_con.close()

    validation = storage.get_db_validation_snapshot()
    checkpoint = storage.get_job_checkpoint("storage_contracts_probe")

    assert schema_row is not None
    assert str(schema_row[0]) == str(storage.SCHEMA_VERSION)
    assert lock_count_row is not None
    assert int(lock_count_row[0]) == 0
    assert validation["ok"] is True, validation
    assert validation["missing_tables"] == [], validation
    assert validation["missing_columns"] == {}, validation
    assert validation["missing_indexes"] == [], validation
    assert validation["schema_version"] == storage.SCHEMA_VERSION, validation
    assert validation["schema_version_ok"] is True, validation
    assert checkpoint == {"last_event_id": 0, "last_event_ts_ms": 0}


def test_db_validation_reports_contract_fields(initialized_storage) -> None:
    storage = initialized_storage["storage"]

    validation = storage.get_db_validation_snapshot()

    assert validation["required_tables"], validation
    assert validation["required_columns"], validation
    assert validation["required_indexes"], validation
    assert isinstance(validation["missing_cols"], dict), validation
    assert validation["missing_cols"] == validation["missing_columns"], validation
    assert "price_feed_lock" in validation["required_tables"], validation
    assert "options_symbol_ingestion_state" in validation["required_tables"], validation


def test_db_validation_snapshot_public_shape_golden(initialized_storage) -> None:
    storage = initialized_storage["storage"]

    validation = storage.get_db_validation_snapshot(include_quick_check=False)
    public_shape = {
        "keys": sorted(validation.keys()),
        "ok": validation["ok"],
        "storage": validation["storage"],
        "backend": validation["backend"],
        "db_exists": validation["db_exists"],
        "schema_version": validation["schema_version"],
        "expected_schema_version": validation["expected_schema_version"],
        "schema_version_ok": validation["schema_version_ok"],
        "schema_status": validation["schema_status"],
        "quick_check": validation["quick_check"],
        "required_table_count": len(validation["required_tables"]),
        "required_index_count": len(validation["required_indexes"]),
        "have_table_count": len(validation["have_tables"]),
        "owned_tables": validation["owned_tables"],
        "execution_orders_columns": validation["required_columns"]["execution_orders"],
        "pnl_attribution_columns": validation["required_columns"]["pnl_attribution"],
        "empty_drift": {
            "missing_tables": validation["missing_tables"],
            "missing_columns": validation["missing_columns"],
            "missing_cols": validation["missing_cols"],
            "missing_indexes": validation["missing_indexes"],
            "owned_missing_tables": validation["owned_missing_tables"],
            "owned_missing_columns": validation["owned_missing_columns"],
            "owned_unexpected_columns": validation["owned_unexpected_columns"],
            "owned_type_mismatches": validation["owned_type_mismatches"],
            "owned_pk_mismatches": validation["owned_pk_mismatches"],
            "owned_missing_indexes": validation["owned_missing_indexes"],
            "owned_drift_tables": validation["owned_drift_tables"],
        },
    }

    assert public_shape == {
        "keys": [
            "backend",
            "db_exists",
            "db_path",
            "expected_schema_version",
            "have_tables",
            "missing_cols",
            "missing_columns",
            "missing_indexes",
            "missing_tables",
            "ok",
            "owned_drift_tables",
            "owned_missing_columns",
            "owned_missing_indexes",
            "owned_missing_tables",
            "owned_pk_mismatches",
            "owned_schema_ok",
            "owned_tables",
            "owned_type_mismatches",
            "owned_unexpected_columns",
            "quick_check",
            "required_columns",
            "required_indexes",
            "required_tables",
            "schema_status",
            "schema_version",
            "schema_version_ok",
            "storage",
            "ts_ms",
        ],
        "ok": True,
        "storage": "sqlite",
        "backend": "sqlite",
        "db_exists": True,
        "schema_version": storage.SCHEMA_VERSION,
        "expected_schema_version": storage.SCHEMA_VERSION,
        "schema_version_ok": True,
        "schema_status": "applied",
        "quick_check": "skipped",
        "required_table_count": 41,
        "required_index_count": 82,
        "have_table_count": 135,
        "owned_tables": [
            "prices",
            "price_quotes",
            "price_quotes_raw",
            "price_provider_health",
            "ingestion_pipeline_health",
            "price_feed_lock",
            "options_symbol_ingestion_state",
        ],
        "execution_orders_columns": [
            "client_order_id",
            "order_uid",
            "idempotency_status",
            "broker",
            "portfolio_orders_id",
            "source_alert_id",
            "prediction_id",
            "model_id",
            "model_version",
            "symbol",
            "qty",
            "submit_ts_ms",
            "ref_px",
            "expected_px",
            "mid_px",
            "bid_px",
            "ask_px",
            "spread_bps",
            "broker_order_id",
            "status",
            "extra_json",
        ],
        "pnl_attribution_columns": [
            "ts_ms",
            "source_alert_id",
            "prediction_id",
            "model_id",
            "model_version",
            "symbol",
            "pnl",
            "fees",
            "slippage_bps",
            "position_size",
            "avg_price",
            "realized_pnl",
            "unrealized_pnl",
            "extra_json",
        ],
        "empty_drift": {
            "missing_tables": [],
            "missing_columns": {},
            "missing_cols": {},
            "missing_indexes": [],
            "owned_missing_tables": [],
            "owned_missing_columns": {},
            "owned_unexpected_columns": {},
            "owned_type_mismatches": {},
            "owned_pk_mismatches": {},
            "owned_missing_indexes": {},
            "owned_drift_tables": [],
        },
    }


def test_storage_facade_exposes_validated_backend_contract(initialized_storage) -> None:
    storage = initialized_storage["storage"]

    backend = storage.get_active_backend()

    assert storage.get_active_backend_name() == "sqlite"
    assert backend.STORAGE_BACKEND_NAME == "sqlite"
    for symbol in storage._REQUIRED_BACKEND_SYMBOLS:
        assert hasattr(backend, symbol), symbol


def test_sqlite_base_schema_has_no_dead_statements_after_terminal_return() -> None:
    storage_sqlite = importlib.import_module("engine.runtime.storage_sqlite")
    tree = ast.parse(inspect.getsource(storage_sqlite._base_schema))
    body = tree.body[0].body
    return_indexes = [idx for idx, node in enumerate(body) if isinstance(node, ast.Return)]

    assert return_indexes, "_base_schema should end with an explicit return after commit"
    assert return_indexes[-1] == len(body) - 1


def test_sqlite_pg_compat_helpers_do_not_clone_runtime_function_code() -> None:
    storage_sqlite = importlib.import_module("engine.runtime.storage_sqlite")
    source = inspect.getsource(storage_sqlite)

    assert "FunctionType" not in source
    assert ".__code__" not in source
    assert "_clone_pg_helpers" not in source
    assert "put_event" in storage_sqlite._PG_COMPAT_HELPER_NAMES
    assert callable(storage_sqlite.put_event)


def test_db_validation_reports_owned_table_contract_fields(initialized_storage) -> None:
    storage = initialized_storage["storage"]

    validation = storage.get_db_validation_snapshot(include_quick_check=False)

    assert validation["owned_schema_ok"] is True, validation
    assert "prices" in validation["owned_tables"], validation
    assert validation["owned_missing_tables"] == [], validation
    assert validation["owned_missing_columns"] == {}, validation
    assert validation["owned_unexpected_columns"] == {}, validation
    assert validation["owned_pk_mismatches"] == {}, validation
    assert validation["owned_missing_indexes"] == {}, validation
    assert validation["owned_drift_tables"] == [], validation


def test_db_validation_flags_owned_table_drift(initialized_storage) -> None:
    storage = initialized_storage["storage"]

    con = storage.connect_rw_direct()
    try:
        con.execute("ALTER TABLE prices ADD COLUMN provider TEXT")
        con.commit()
    finally:
        con.close()

    validation = storage.get_db_validation_snapshot(include_quick_check=False)

    assert validation["ok"] is False, validation
    assert validation["owned_schema_ok"] is False, validation
    assert validation["owned_unexpected_columns"] == {"prices": ["provider"]}, validation
    assert "prices" in validation["owned_drift_tables"], validation
