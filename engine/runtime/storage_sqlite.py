"""SQLite-backed runtime storage used only for isolated Python tests.

The production facade is Postgres-only.  This module exists so unit tests that
create a temporary ``DB_PATH`` can exercise storage-facing code without probing
ambient PgBouncer/Postgres, system credentials, or metrics bootstraps.
"""

from __future__ import annotations

import logging
import json
import os
import re
import tempfile
import threading
import time
import importlib
import inspect
from contextlib import contextmanager
from pathlib import Path
from types import FunctionType
from typing import Any, Callable, Iterable, Sequence

from engine.runtime.platform import default_data_root

LOGGER = logging.getLogger(__name__)
sqlite3 = importlib.import_module("sqlite3")

SCHEMA_VERSION = 1
DB_PATH = Path(
    os.environ.get("DB_PATH")
    or (Path(os.environ.get("TS_DATA_ROOT") or tempfile.gettempdir()) / "runtime-test.sqlite")
)
PG_LIVENESS_DB_ENABLED = False
PG_LIVENESS_DB_PATH = DB_PATH.with_suffix(".liveness.sqlite")
_SQLITE_LIVENESS_DB_ENABLED = _env_truthy(os.environ.get("SQLITE_LIVENESS_DB_ENABLED", "1")) if "_env_truthy" in globals() else str(os.environ.get("SQLITE_LIVENESS_DB_ENABLED", "1")).strip().lower() in {"1", "true", "yes", "on"}
_SQLITE_LIVENESS_DB_PATH = PG_LIVENESS_DB_PATH

_WRITE_LOCK = globals().get("_WRITE_LOCK") or threading.RLock()
_INIT_LOCK = threading.RLock()
_DDL_LOCK = threading.RLock()
_THREAD_LOCAL = globals().get("_THREAD_LOCAL") or threading.local()
_INITIALIZED_PATHS: set[str] = globals().get("_INITIALIZED_PATHS", set())
_SQLITE_BUSY_TIMEOUT_MS = int(os.environ.get("SQLITE_BUSY_TIMEOUT_MS", "5000") or 5000)

_PREVIOUS_SQLITE_LIVENESS_STOP = globals().get("_SQLITE_LIVENESS_STOP")
_PREVIOUS_SQLITE_LIVENESS_THREAD = globals().get("_SQLITE_LIVENESS_THREAD")
if _PREVIOUS_SQLITE_LIVENESS_STOP is not None:
    try:
        _PREVIOUS_SQLITE_LIVENESS_STOP.set()
    except Exception:
        LOGGER.debug("sqlite_liveness_reload_stop_failed", exc_info=True)
if (
    _PREVIOUS_SQLITE_LIVENESS_THREAD is not None
    and getattr(_PREVIOUS_SQLITE_LIVENESS_THREAD, "is_alive", lambda: False)()
    and _PREVIOUS_SQLITE_LIVENESS_THREAD is not threading.current_thread()
):
    try:
        _PREVIOUS_SQLITE_LIVENESS_THREAD.join(timeout=1.0)
    except Exception:
        LOGGER.debug("sqlite_liveness_reload_join_failed", exc_info=True)

_SQLITE_TRACE_LOCK = threading.Lock()
_SQLITE_TRACE_HISTORY: list[dict[str, Any]] = []
_SQLITE_TRACE_LONGEST_LOCKS: list[dict[str, Any]] = []
_SQLITE_TRACE_BY_TABLE: dict[str, dict[str, Any]] = {}
_SQLITE_TRACE_BY_PATH: dict[str, dict[str, Any]] = {}
_SQLITE_TRACE_TOTALS: dict[str, Any] = {
    "write_count": 0,
    "read_count": 0,
    "write_ms": 0.0,
    "read_ms": 0.0,
    "busy_count": 0,
    "retries": 0,
    "busy_retry_count": 0,
    "slow_write_count": 0,
    "cannot_commit_count": 0,
}

_INSIDER_TRANSACTION_COLUMNS = (
    "ts_ms",
    "symbol",
    "event_id",
    "source_transaction_id",
    "created_ts_ms",
    "ingested_ts_ms",
    "source",
    "filing_accession",
    "filing_identifier",
    "filing_url",
    "filing_ts_ms",
    "availability_ts_ms",
    "filing_date",
    "filing_accepted_at",
    "transaction_ts_ms",
    "transaction_date",
    "issuer_name",
    "issuer_cik",
    "insider_name",
    "insider_cik",
    "insider_role",
    "insider_title",
    "transaction_code",
    "transaction_type",
    "direction",
    "security_type",
    "shares",
    "price",
    "value",
    "ownership_nature",
    "is_10b5_1_plan",
    "entity_id",
    "resolution_status",
    "resolution_method",
    "payload_json",
    "diagnostics_json",
)
_CONGRESSIONAL_TRADE_COLUMNS = (
    "ts_ms",
    "symbol",
    "event_id",
    "source_trade_id",
    "source_record_id",
    "source_url",
    "created_ts_ms",
    "ingested_ts_ms",
    "source",
    "chamber",
    "office",
    "politician_name",
    "owner_name",
    "issuer_name",
    "transaction_type_raw",
    "transaction_type",
    "direction",
    "amount_range",
    "amount_low",
    "amount_high",
    "amount_mid",
    "transaction_date",
    "transaction_ts_ms",
    "disclosure_date",
    "disclosure_ts_ms",
    "entity_id",
    "resolution_status",
    "resolution_method",
    "payload_json",
    "diagnostics_json",
)
_FINRA_SHORT_SALE_VOLUME_COLUMNS = (
    "ts_ms",
    "symbol",
    "trade_date",
    "trade_ts_ms",
    "availability_ts_ms",
    "source_record_id",
    "source_url",
    "ingested_ts_ms",
    "short_volume",
    "short_exempt_volume",
    "total_volume",
    "market",
    "payload_json",
    "diagnostics_json",
)
_FINRA_SHORT_INTEREST_COLUMNS = (
    "ts_ms",
    "symbol",
    "settlement_date",
    "settlement_ts_ms",
    "dissemination_date",
    "dissemination_ts_ms",
    "availability_ts_ms",
    "source_record_id",
    "ingested_ts_ms",
    "short_interest_shares",
    "days_to_cover",
    "payload_json",
    "diagnostics_json",
)
_CRYPTO_FUNDING_RATE_COLUMNS = (
    "ts_ms",
    "symbol",
    "exchange",
    "perp_market",
    "spot_market",
    "funding_ts_ms",
    "availability_ts_ms",
    "funding_rate",
    "mark_price",
    "index_price",
    "spot_price",
    "spot_ts_ms",
    "perp_ts_ms",
    "perp_basis_pct",
    "source_record_id",
    "ingested_ts_ms",
    "is_live",
    "payload_json",
    "diagnostics_json",
)


def _adapt_json(value: Any) -> str:
    import json

    return json.dumps(value, separators=(",", ":"), sort_keys=True, default=str)


sqlite3.register_adapter(dict, _adapt_json)
sqlite3.register_adapter(list, _adapt_json)
sqlite3.register_adapter(tuple, _adapt_json)


def _env_truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _liveness_db_enabled() -> bool:
    return _env_truthy(os.environ.get("SQLITE_LIVENESS_DB_ENABLED", "1"))


def _current_db_path() -> Path:
    global DB_PATH, PG_LIVENESS_DB_PATH, _SQLITE_LIVENESS_DB_ENABLED, _SQLITE_LIVENESS_DB_PATH
    configured = str(os.environ.get("DB_PATH") or "").strip()
    if configured:
        path = Path(configured).expanduser()
    else:
        if _env_truthy(os.environ.get("TS_TESTING")):
            root = Path(tempfile.gettempdir()) / f"trading-system-tests-{os.getpid()}"
        else:
            root = Path(os.environ.get("TS_DATA_ROOT") or default_data_root()).expanduser()
        path = root / "runtime-test.sqlite"
        os.environ.setdefault("DB_PATH", str(path))
    path.parent.mkdir(parents=True, exist_ok=True)
    DB_PATH = path
    PG_LIVENESS_DB_PATH = DB_PATH.with_suffix(".liveness.sqlite")
    _SQLITE_LIVENESS_DB_ENABLED = _liveness_db_enabled()
    _SQLITE_LIVENESS_DB_PATH = PG_LIVENESS_DB_PATH
    return path


def _current_liveness_db_path() -> Path:
    _current_db_path()
    path = Path(os.environ.get("SQLITE_LIVENESS_DB_PATH") or str(_SQLITE_LIVENESS_DB_PATH)).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _column_type(name: str, *, for_alter: bool = False) -> str:
    text = str(name or "").lower()
    if text == "id":
        return "INTEGER" if for_alter else "INTEGER PRIMARY KEY AUTOINCREMENT"
    if text.startswith("is_") or text.startswith("use_") or text.endswith("_enabled") or text in {
        "ok",
        "active",
        "approved",
        "completed",
        "passed",
        "cooldown_applied",
    }:
        return "INTEGER"
    if (
        text.endswith("_ts")
        or text.endswith("_ts_ms")
        or text.endswith("_s")
        or text.endswith("_ms")
        or text in {
            "ts",
            "ts_ms",
            "timestamp",
            "pid",
            "n",
            "n_paths",
            "n_trials",
            "path_index",
            "horizon_s",
            "backtest_run_id",
            "time",
            "prediction_time",
            "tracked_prediction_id",
            "prediction_id",
            "outcome_id",
            "directional_accuracy",
            "trial_count",
            "best_trial_number",
        }
    ):
        return "INTEGER"
    if (
        "price" in text
        or "value" in text
        or "score" in text
        or "rate" in text
        or "ratio" in text
        or "sharpe" in text
        or "drawdown" in text
        or "return" in text
        or "confidence" in text
        or "equity" in text
        or "cash" in text
        or "pnl" in text
        or "margin" in text
        or "ret" in text
        or "cost" in text
        or "fee" in text
        or "slippage" in text
        or text in {"shares", "amount_low", "amount_high", "amount_mid", "volatility"}
    ):
        return "REAL"
    if text.endswith("_blob") or text in {"blob", "payload_bytes"}:
        return "BLOB"
    return "TEXT"


def _ident(name: str) -> str:
    text = str(name or "").strip()
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", text):
        raise ValueError(f"invalid_identifier:{text}")
    return text


def _quote(name: str) -> str:
    return '"' + _ident(name).replace('"', '""') + '"'


def _split_columns(raw: str) -> list[str]:
    return [
        part.strip().strip('"')
        for part in str(raw or "").split(",")
        if part.strip()
    ]


def _connection_execute_raw(con: sqlite3.Connection, sql: str, params: Any = None):
    if params is None:
        return sqlite3.Connection.execute(con, sql)
    return sqlite3.Connection.execute(con, sql, params)


def _active_write_connection() -> "StorageConnection | None":
    con = getattr(_THREAD_LOCAL, "active_write_connection", None)
    if con is not None and bool(getattr(con, "in_transaction", False)):
        return con
    return None


def _mark_active_write_connection(con: "StorageConnection") -> None:
    _THREAD_LOCAL.active_write_connection = con


def _clear_active_write_connection(con: "StorageConnection") -> None:
    if getattr(_THREAD_LOCAL, "active_write_connection", None) is con:
        _THREAD_LOCAL.active_write_connection = None


def _is_read_statement(sql: str) -> bool:
    return bool(re.match(r"^\s*(?:SELECT|WITH|PRAGMA)\b", str(sql or ""), flags=re.IGNORECASE))


def _is_auto_write_statement(sql: str) -> bool:
    return bool(re.match(r"^\s*(?:INSERT|UPDATE|DELETE|REPLACE)\b", str(sql or ""), flags=re.IGNORECASE))


def _table_columns(con: sqlite3.Connection, table: str) -> set[str]:
    rows = _connection_execute_raw(con, f"PRAGMA table_info({_quote(table)})").fetchall()
    return {str(row[1]) for row in rows or []}


def _table_info(con: sqlite3.Connection, table: str) -> list[sqlite3.Row]:
    return list(_connection_execute_raw(con, f"PRAGMA table_info({_quote(table)})").fetchall() or [])


def _table_sql(con: sqlite3.Connection, table: str) -> str:
    row = _connection_execute_raw(
        con,
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (str(table),),
    ).fetchone()
    return str(row[0] or "") if row else ""


def _normalized_sql_signature(sql: str) -> str:
    return re.sub(r"\s+", "", str(sql or "")).upper()


def _next_legacy_table_name(con: sqlite3.Connection, table: str) -> str:
    base = f"{_ident(table)}_legacy_exact_once"
    candidate = base
    idx = 2
    while _connection_execute_raw(
        con,
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (candidate,),
    ).fetchone():
        candidate = f"{base}_{idx}"
        idx += 1
    return candidate


def _legacy_expr(legacy_columns: set[str], column_name: str, fallback_sql: str) -> str:
    clean = _ident(column_name)
    return _quote(clean) if clean in legacy_columns else str(fallback_sql)


def _copy_legacy_rows(
    con: sqlite3.Connection,
    *,
    legacy_table: str,
    target_table: str,
    columns: Sequence[str],
    expressions: dict[str, str] | None = None,
) -> None:
    legacy_columns = _table_columns(con, legacy_table)
    exprs = dict(expressions or {})
    select_exprs = [
        str(exprs.get(col) or _legacy_expr(legacy_columns, col, "NULL"))
        for col in columns
    ]
    cols_sql = ", ".join(_quote(col) for col in columns)
    select_sql = ", ".join(select_exprs)
    _connection_execute_raw(
        con,
        f"INSERT OR IGNORE INTO {_quote(target_table)}({cols_sql}) SELECT {select_sql} FROM {_quote(legacy_table)}",
    )


def _needs_exact_rebuild(
    con: sqlite3.Connection,
    table: str,
    columns: Sequence[str],
    *,
    pk: dict[str, int] | None = None,
    required_sql_tokens: Sequence[str] = (),
) -> bool:
    if not _table_exists(con, table):
        return False
    rows = _table_info(con, table)
    actual = [str(row[1]) for row in rows]
    if actual != [str(col) for col in columns]:
        return True
    if pk:
        actual_pk = {str(row[1]): int(row[5] or 0) for row in rows}
        for column_name, ordinal in pk.items():
            if int(actual_pk.get(str(column_name)) or 0) != int(ordinal):
                return True
    signature = _normalized_sql_signature(_table_sql(con, table))
    return any(_normalized_sql_signature(token) not in signature for token in required_sql_tokens)


def _alter_add_column_if_missing(con: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    clean_table = _ident(table)
    clean_column = _ident(column)
    if clean_column in _table_columns(con, clean_table):
        return
    try:
        _connection_execute_raw(
            con,
            f"ALTER TABLE {_quote(clean_table)} ADD COLUMN {_quote(clean_column)} {ddl}",
        )
    except sqlite3.OperationalError as exc:
        if "duplicate column name" in str(exc).lower() and clean_column in _table_columns(con, clean_table):
            return
        raise


def _ensure_table(con: sqlite3.Connection, table: str, columns: Sequence[str] = ()) -> None:
    table_name = _ident(table)
    requested = []
    seen_requested: set[str] = set()
    for col in columns:
        clean = str(col).strip().strip('"')
        if not clean:
            continue
        clean = _ident(clean)
        key = clean.lower()
        if key in seen_requested:
            continue
        seen_requested.add(key)
        requested.append(clean)
    table_exists = bool(
        _connection_execute_raw(
            con,
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (table_name,),
        ).fetchone()
    )
    if not table_exists and "id" not in {col.lower() for col in requested}:
        requested = ["id", *requested]
    if requested:
        defs = ", ".join(f"{_quote(col)} {_column_type(col)}" for col in requested)
        _connection_execute_raw(con, f"CREATE TABLE IF NOT EXISTS {_quote(table_name)} ({defs})")
    existing = _table_columns(con, table_name)
    for col in requested:
        if col not in existing:
            try:
                _connection_execute_raw(
                    con,
                    f"ALTER TABLE {_quote(table_name)} ADD COLUMN {_quote(col)} {_column_type(col, for_alter=True)}",
                )
            except sqlite3.OperationalError as exc:
                if "duplicate column name" in str(exc).lower() and col in _table_columns(con, table_name):
                    existing.add(col)
                    continue
                raise
            existing.add(col)


def _ensure_columns(con: sqlite3.Connection, table: str, columns: Sequence[str]) -> None:
    _ensure_table(con, table, columns)
    existing = _table_columns(con, table)
    seen: set[str] = set()
    for col in columns:
        clean = str(col).strip().strip('"')
        if not clean:
            continue
        clean = _ident(clean)
        key = clean.lower()
        if key in seen:
            continue
        seen.add(key)
        if clean not in existing:
            try:
                _connection_execute_raw(
                    con,
                    f"ALTER TABLE {_quote(table)} ADD COLUMN {_quote(clean)} {_column_type(clean, for_alter=True)}",
                )
            except sqlite3.OperationalError as exc:
                if "duplicate column name" in str(exc).lower() and clean in _table_columns(con, table):
                    existing.add(clean)
                    continue
                raise
            existing.add(clean)


_INIT_SENTINEL_TABLES = (
    "runtime_meta",
    "schema_version",
    "schema_migrations",
    "predictions",
    "alerts",
    "prediction_history",
    "regime_state",
    "model_feature_snapshots",
    "gdelt_macro_features",
    "event_log_state",
    "ipc_channels",
    "broker_connection_health",
    "execution_health_state",
    "model_stats_regime",
    "realized_outcomes",
    "model_performance",
    "model_hyperparameter_registry",
    "model_best_params",
    "data_sources",
    "strategy_metrics",
    "size_policy",
    "walk_forward_runs",
    "walk_forward_scores",
    "tracked_predictions",
    "prediction_explanations",
    "job_locks",
    "job_heartbeats",
    "price_feed_lock",
    "price_provider_health",
)

_INIT_SENTINEL_INDEXES = (
    "idx_predictions_ts",
    "idx_predictions_symbol_ts",
    "idx_predictions_model_ts",
    "idx_alerts_prediction_id",
    "idx_regime_state_symbol_time_desc",
    "idx_tracked_predictions_prediction_id",
    "idx_execution_orders_prediction_submit_ts",
    "idx_execution_fills_model_ts",
    "idx_pnl_attribution_prediction_ts",
    "idx_pnl_attribution_ts",
    "idx_pnl_attribution_model_ts",
    "idx_price_provider_health_ts",
    "idx_model_hparam_registry_family_ts",
    "ux_model_best_params_model_family_symbol",
    "idx_realized_outcomes_symbol_ts",
    "ux_model_performance_tracked_prediction_id",
    "idx_model_performance_identity_time",
    "idx_model_performance_model_id_time",
    "idx_model_performance_regime_time",
)

_INIT_SENTINEL_COLUMNS = {
    "alerts": ("prediction_id", "model_name", "model_id", "model_version", "event_id"),
    "decision_log": ("prev_hash", "row_hash", "component_vector"),
    "prediction_history": ("confidence_raw", "prediction_strength", "model_id", "model_version"),
    "regime_state": ("time", "symbol", "volatility_regime", "trend_regime", "liquidity_regime"),
    "model_feature_snapshots": ("symbol", "ts_ms", "feature_set_tag", "features_json", "created_ts_ms"),
    "gdelt_macro_features": ("bucket_ts_ms", "bucket_sec", "doc_count", "tone_mean"),
    "event_log_state": ("namespace", "state_key", "state_value", "updated_ts_ms", "payload_json"),
    "ipc_channels": ("channel", "owner", "state_json", "last_seq", "updated_ts_ms"),
    "broker_connection_health": ("ts_ms", "broker", "ok", "state", "details_json"),
    "execution_health_state": ("ts_ms", "state", "score", "extra_json"),
    "model_stats_regime": ("symbol", "horizon_s", "regime", "mean_impact_z"),
    "realized_outcomes": ("symbol", "ts_ms", "realized_return", "metadata_json"),
    "model_performance": (
        "tracked_prediction_id",
        "prediction_id",
        "outcome_id",
        "time",
        "prediction_time",
        "symbol",
        "model_id",
        "model_name",
        "model_version",
        "horizon_s",
        "prediction",
        "realized_return",
        "error",
        "directional_accuracy",
        "pnl_impact",
        "rolling_score",
        "regime_time_ms",
        "volatility_regime",
        "trend_regime",
        "liquidity_regime",
        "metadata_json",
    ),
    "model_hyperparameter_registry": (
        "ts",
        "model_family",
        "model_name",
        "tuner",
        "objective",
        "params",
        "params_json",
        "trial_count",
        "best_trial_number",
        "cpcv_mean_sharpe",
        "cpcv_median_sharpe",
        "cpcv_pbo",
        "diagnostics",
    ),
    "model_best_params": ("model_family", "symbol", "params_json", "value", "trial_number", "seed"),
    "data_sources": ("source_key", "display_name", "source_type", "job_name", "key_version"),
    "strategy_metrics": ("strategy_name", "window_days", "ts_ms", "metrics_json", "is_active"),
    "size_policy": ("ts_ms", "lookback_days", "buckets", "method", "metrics_json"),
    "walk_forward_runs": ("run_id", "params_json", "metrics_json", "ts_ms", "model_selection_json"),
    "walk_forward_scores": ("run_id", "symbol", "horizon_s", "model_name", "model_version", "model_kind"),
    "tracked_predictions": ("prediction_id", "source_alert_id", "model_id", "tracking_source", "metadata_json"),
    "job_heartbeats": ("owner", "pid", "extra_json"),
    "price_quotes": ("source", "spread"),
    "execution_orders": ("prediction_id", "model_id", "model_version", "spread_bps"),
    "execution_fills": ("prediction_id", "model_id", "model_version", "source_alert_id"),
    "pnl_attribution": ("prediction_id", "model_id", "model_version", "pnl", "realized_pnl", "unrealized_pnl"),
    "model_marketplace_scores": ("regime", "stage", "trades", "net_pnl", "updated_ts_ms", "meta_json"),
    "model_metrics": ("model_name", "symbol", "horizon_s", "metrics_json"),
    "labels_exec": ("event_id", "symbol", "horizon_s", "net_z", "total_cost_bps"),
}


def _sqlite_schema_sentinels_ready(path: Path) -> bool:
    if not Path(path).exists():
        return False
    con = sqlite3.connect(str(path), timeout=0.05, isolation_level=None)
    try:
        rows = sqlite3.Connection.execute(
            con,
            "SELECT type, name FROM sqlite_master WHERE type IN ('table', 'index')",
        ).fetchall()
        tables = {str(row[1]) for row in rows if str(row[0]) == "table"}
        indexes = {str(row[1]) for row in rows if str(row[0]) == "index"}
        if not set(_INIT_SENTINEL_TABLES).issubset(tables):
            return False
        if not set(_INIT_SENTINEL_INDEXES).issubset(indexes):
            return False
        for table, required_columns in _INIT_SENTINEL_COLUMNS.items():
            if table not in tables:
                return False
            info = sqlite3.Connection.execute(con, f"PRAGMA table_info({_quote(table)})").fetchall()
            columns = {str(row[1]) for row in info or []}
            if not set(required_columns).issubset(columns):
                return False
        try:
            from engine.runtime.storage_live_ingestion_schema import OWNED_LIVE_TABLE_COLUMN_SPECS

            for table, expected_specs in OWNED_LIVE_TABLE_COLUMN_SPECS.items():
                if table not in tables:
                    return False
                info = sqlite3.Connection.execute(con, f"PRAGMA table_info({_quote(table)})").fetchall() or []
                actual = {str(row[1]): int(row[5] or 0) for row in info}
                if set(actual) != set(expected_specs):
                    return False
                for column_name, expected_spec in expected_specs.items():
                    if int(actual.get(str(column_name)) or 0) != int((expected_spec or {}).get("pk") or 0):
                        return False
        except Exception:
            return False
        return True
    except sqlite3.OperationalError as exc:
        if "locked" in str(exc).lower():
            return True
        raise
    finally:
        con.close()


def _unique_index_name(table: str, columns: Sequence[str]) -> str:
    suffix = "_".join(str(col).strip().strip('"') for col in columns if str(col).strip())
    return _ident(f"ux_{table}_{suffix}"[:120])


def _ensure_unique(con: sqlite3.Connection, table: str, columns: Sequence[str]) -> None:
    cols = [str(col).strip().strip('"') for col in columns if str(col).strip()]
    if not cols:
        return
    _ensure_columns(con, table, cols)
    index_name = _unique_index_name(table, cols)
    cols_sql = ", ".join(_quote(col) for col in cols)
    _connection_execute_raw(
        con,
        f"CREATE UNIQUE INDEX IF NOT EXISTS {_quote(index_name)} ON {_quote(table)} ({cols_sql})",
    )


def _insert_shape(sql: str) -> tuple[str, list[str], list[str]]:
    text = str(sql or "")
    insert = re.search(
        r"\bINSERT\s+(?:OR\s+\w+\s+)?INTO\s+(?:\"(?P<qtable>[^\"]+)\"|(?P<table>[A-Za-z_][A-Za-z0-9_]*))\s*\((?P<cols>.*?)\)",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not insert:
        return "", [], []
    table = str(insert.group("qtable") or insert.group("table") or "")
    columns = _split_columns(insert.group("cols") or "")
    conflict_match = re.search(r"\bON\s+CONFLICT\s*\((?P<cols>.*?)\)", text, flags=re.IGNORECASE | re.DOTALL)
    conflict_columns = _split_columns(conflict_match.group("cols") if conflict_match else "")
    return table, columns, conflict_columns


def _inject_insert_column_expr(sql: str, table: str, column: str, expr: str) -> str:
    table_name = _ident(table)
    column_name = _ident(column)
    pattern = re.compile(
        rf"(?P<prefix>\bINSERT\s+(?:OR\s+\w+\s+)?INTO\s+(?:{re.escape(table_name)}|\"{re.escape(table_name)}\")\s*)"
        r"\((?P<cols>[^)]*)\)(?P<mid>\s*VALUES\s*)\((?P<values>[^)]*)\)",
        flags=re.IGNORECASE | re.DOTALL,
    )
    match = pattern.search(str(sql or ""))
    if not match:
        return sql
    columns = _split_columns(match.group("cols") or "")
    if column_name in columns:
        return sql
    replacement = (
        f"{match.group('prefix')}({match.group('cols')}, {column_name})"
        f"{match.group('mid')}({str(match.group('values') or '').strip()}, {expr})"
    )
    return f"{sql[:match.start()]}{replacement}{sql[match.end():]}"


def _table_from_sql(sql: str) -> str:
    text = str(sql or "")
    for pattern in (
        r"\bFROM\s+(?:\"([^\"]+)\"|([A-Za-z_][A-Za-z0-9_]*))",
        r"\bINTO\s+(?:\"([^\"]+)\"|([A-Za-z_][A-Za-z0-9_]*))",
        r"\bUPDATE\s+(?:\"([^\"]+)\"|([A-Za-z_][A-Za-z0-9_]*))",
        r"\bJOIN\s+(?:\"([^\"]+)\"|([A-Za-z_][A-Za-z0-9_]*))",
    ):
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return str(match.group(1) or match.group(2) or "")
    return ""


def _selected_columns(sql: str) -> list[str]:
    match = re.search(r"\bSELECT\s+(?P<cols>.*?)\s+\bFROM\b", str(sql or ""), flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return []
    out: list[str] = []
    for part in str(match.group("cols") or "").split(","):
        token = part.strip()
        if not token or token == "*" or "(" in token:
            continue
        token = re.split(r"\s+AS\s+|\s+", token, flags=re.IGNORECASE)[-1]
        token = token.strip().strip('"')
        if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", token):
            out.append(token)
    return out


def _prepare_sql(con: sqlite3.Connection, sql: str) -> str:
    text = str(sql or "").strip()
    if not text:
        return text
    if re.match(r"SET\s+search_path\b", text, flags=re.IGNORECASE):
        return "SELECT 1 WHERE 0"
    if re.search(r"\bFROM\s+information_schema\.tables\b", text, flags=re.IGNORECASE):
        table_match = re.search(r"\btable_name\s*=\s*['\"]([^'\"]+)['\"]", text, flags=re.IGNORECASE)
        if table_match:
            table_name = _ident(str(table_match.group(1) or ""))
            return (
                "SELECT 1 FROM sqlite_master "
                f"WHERE type='table' AND name='{table_name}' LIMIT 1"
            )
        if re.search(r"\btable_name\s*=\s*\?", text, flags=re.IGNORECASE):
            return "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1"
    text = re.sub(r"%s", "?", text)
    text = re.sub(r"::(?:jsonb|json|text|bigint|integer|double precision|real|bytea|regclass)", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bTRUE\b", "1", text, flags=re.IGNORECASE)
    text = re.sub(r"\bFALSE\b", "0", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+FOR\s+UPDATE\b", "", text, flags=re.IGNORECASE)
    table, columns, conflict_columns = _insert_shape(text)
    if table == "alerts" and "dedupe_key" not in columns:
        text = _inject_insert_column_expr(
            text,
            "alerts",
            "dedupe_key",
            "'legacy:' || lower(hex(randomblob(8)))",
        )
        table, columns, conflict_columns = _insert_shape(text)
    if table:
        _ensure_columns(con, table, columns)
        if conflict_columns:
            _ensure_unique(con, table, conflict_columns)
    return text


def _normalize_param(value: Any) -> Any:
    if isinstance(value, (dict, list, tuple)):
        return _adapt_json(value)
    if isinstance(value, memoryview):
        return bytes(value)
    return value


def _normalize_params(params: Any) -> Any:
    if params is None:
        return None
    if isinstance(params, dict):
        return {str(key): _normalize_param(value) for key, value in params.items()}
    if isinstance(params, (tuple, list)):
        return tuple(_normalize_param(value) for value in params)
    return _normalize_param(params)


def _execute_with_repair(con: sqlite3.Connection, sql: str, params: Any = None):
    with _DDL_LOCK:
        prepared = _prepare_sql(con, sql)
    normalized = _normalize_params(params)
    attempts = 0
    while True:
        try:
            return _connection_execute_raw(con, prepared, normalized)
        except sqlite3.OperationalError as exc:
            attempts += 1
            if attempts > 4:
                raise
            message = str(exc)
            missing_table = re.search(r"no such table:\s*([A-Za-z_][A-Za-z0-9_]*)", message, flags=re.IGNORECASE)
            if missing_table:
                if _is_read_statement(prepared):
                    raise
                table = str(missing_table.group(1))
                _ensure_table(con, table, _selected_columns(prepared))
                continue
            missing_insert_col = re.search(
                r"table\s+([A-Za-z_][A-Za-z0-9_]*)\s+has\s+no\s+column\s+named\s+([A-Za-z_][A-Za-z0-9_]*)",
                message,
                flags=re.IGNORECASE,
            )
            if missing_insert_col:
                _ensure_columns(con, str(missing_insert_col.group(1)), [str(missing_insert_col.group(2))])
                continue
            missing_col = re.search(
                r"no such column:\s*(?:[A-Za-z_][A-Za-z0-9_]*\.)?([A-Za-z_][A-Za-z0-9_]*)",
                message,
                flags=re.IGNORECASE,
            )
            table = _table_from_sql(prepared)
            if missing_col and table:
                if _is_read_statement(prepared):
                    raise
                _ensure_columns(con, table, [str(missing_col.group(1))])
                continue
            if "ON CONFLICT clause does not match" in message:
                table_name, _, conflict_columns = _insert_shape(prepared)
                if table_name and conflict_columns:
                    _ensure_unique(con, table_name, conflict_columns)
                    continue
            raise


def _trace_call_path() -> str:
    override = str(getattr(_THREAD_LOCAL, "trace_call_path", "") or "")
    if override:
        return override
    preferred = {
        ("price_router.py", "publish_price_events"),
        ("data_source_manager.py", "record_source_status"),
        ("runtime_meta.py", "_run_meta_write"),
        ("telemetry_append_buffer.py", "_flush_rows"),
    }
    fallback = ""
    public_fallback = ""
    try:
        for frame in inspect.stack(context=0)[2:]:
            filename = str(frame.filename or "")
            basename = os.path.basename(filename)
            func = str(frame.function or "")
            if basename == "storage_sqlite.py":
                continue
            path = f"{basename}:{func}"
            if (basename, func) in preferred:
                return path
            if not fallback:
                fallback = path
            if not public_fallback and not func.startswith("_"):
                public_fallback = path
    except Exception:
        return "unknown"
    return public_fallback or fallback or "unknown"


def _record_sqlite_trace(sql: str, *, row_count: int = 1, elapsed_ms: float = 0.0) -> None:
    text = str(sql or "")
    if not _is_auto_write_statement(text):
        return
    table = _table_from_sql(text) or "unknown"
    path = _trace_call_path()
    writes = max(1, int(row_count or 1))
    elapsed = max(0.0, float(elapsed_ms or 0.0))
    with _SQLITE_TRACE_LOCK:
        _SQLITE_TRACE_TOTALS["write_count"] = int(_SQLITE_TRACE_TOTALS.get("write_count") or 0) + writes
        _SQLITE_TRACE_TOTALS["write_ms"] = float(_SQLITE_TRACE_TOTALS.get("write_ms") or 0.0) + elapsed
        table_stats = _SQLITE_TRACE_BY_TABLE.setdefault(
            table,
            {"table": table, "writes": 0, "write_ms": 0.0, "busy": 0},
        )
        table_stats["writes"] = int(table_stats.get("writes") or 0) + writes
        table_stats["write_ms"] = float(table_stats.get("write_ms") or 0.0) + elapsed
        path_stats = _SQLITE_TRACE_BY_PATH.setdefault(
            path,
            {"path": path, "writes": 0, "write_ms": 0.0, "busy": 0},
        )
        path_stats["writes"] = int(path_stats.get("writes") or 0) + writes
        path_stats["write_ms"] = float(path_stats.get("write_ms") or 0.0) + elapsed
        _SQLITE_TRACE_HISTORY.append(
            {
                "ts_ms": int(time.time() * 1000),
                "table": table,
                "path": path,
                "writes": writes,
                "write_ms": elapsed,
            }
        )
        del _SQLITE_TRACE_HISTORY[:-200]


def _record_sqlite_busy(sql: str, exc: BaseException | None = None) -> None:
    table = _table_from_sql(str(sql or "")) or "unknown"
    path = _trace_call_path()
    last_error = str(exc or "database is locked")
    with _SQLITE_TRACE_LOCK:
        _SQLITE_TRACE_TOTALS["busy_count"] = int(_SQLITE_TRACE_TOTALS.get("busy_count") or 0) + 1
        table_stats = _SQLITE_TRACE_BY_TABLE.setdefault(
            table,
            {"table": table, "writes": 0, "write_ms": 0.0, "busy": 0, "lock_errors": 0, "last_error": ""},
        )
        table_stats["busy"] = int(table_stats.get("busy") or 0) + 1
        table_stats["lock_errors"] = int(table_stats.get("lock_errors") or 0) + 1
        table_stats["last_error"] = last_error
        path_stats = _SQLITE_TRACE_BY_PATH.setdefault(
            path,
            {"path": path, "writes": 0, "write_ms": 0.0, "busy": 0, "lock_errors": 0, "last_error": ""},
        )
        path_stats["busy"] = int(path_stats.get("busy") or 0) + 1
        path_stats["lock_errors"] = int(path_stats.get("lock_errors") or 0) + 1
        path_stats["last_error"] = last_error


class _BufferedCursor:
    def __init__(self, rows: Sequence[Any], *, rowcount: int = -1, lastrowid: int | None = None) -> None:
        self._rows = list(rows or [])
        self._index = 0
        self.rowcount = int(rowcount)
        self.lastrowid = lastrowid

    def fetchone(self):
        if self._index >= len(self._rows):
            return None
        row = self._rows[self._index]
        self._index += 1
        return row

    def fetchall(self):
        rows = list(self._rows[self._index :])
        self._index = len(self._rows)
        return rows


def _is_job_heartbeat_read(sql: str) -> bool:
    return bool(
        _liveness_db_enabled()
        and _is_read_statement(sql)
        and re.search(
            r"\bFROM\s+(?:\"job_heartbeats\"|job_heartbeats|\"job_locks\"|job_locks)\b",
            str(sql or ""),
            flags=re.IGNORECASE,
        )
    )


def _is_job_heartbeat_write(sql: str) -> bool:
    return bool(
        _liveness_db_enabled()
        and not _is_read_statement(sql)
        and (
            re.search(
                r"\bINTO\s+(?:\"job_heartbeats\"|job_heartbeats|\"job_locks\"|job_locks)\b",
                str(sql or ""),
                flags=re.IGNORECASE,
            )
            or re.search(
                r"\bUPDATE\s+(?:\"job_heartbeats\"|job_heartbeats|\"job_locks\"|job_locks)\b",
                str(sql or ""),
                flags=re.IGNORECASE,
            )
            or re.search(
                r"\bDELETE\s+FROM\s+(?:\"job_heartbeats\"|job_heartbeats|\"job_locks\"|job_locks)\b",
                str(sql or ""),
                flags=re.IGNORECASE,
            )
            or re.search(
                r"\bREPLACE\s+INTO\s+(?:\"job_heartbeats\"|job_heartbeats|\"job_locks\"|job_locks)\b",
                str(sql or ""),
                flags=re.IGNORECASE,
            )
        )
    )


def _read_liveness_rows(sql: str, params: Any = None) -> _BufferedCursor:
    _ensure_liveness_db_schema()
    con = _connect_liveness_storage_ro_direct()
    try:
        cur = _execute_with_repair(con, sql, params)
        rows = cur.fetchall()
        return _BufferedCursor(rows, rowcount=len(rows))
    finally:
        con.close()


def _write_liveness_rows(sql: str, params: Any = None) -> _BufferedCursor:
    _ensure_liveness_db_schema()
    con = _connect_liveness_storage_rw_direct()
    try:
        try:
            cur = con.execute(sql, params)
        except sqlite3.IntegrityError:
            if not re.match(
                r"^\s*INSERT\s+INTO\s+(?:\"job_heartbeats\"|job_heartbeats|\"job_locks\"|job_locks)\b",
                str(sql or ""),
                flags=re.IGNORECASE,
            ):
                raise
            cur = con.execute(
                re.sub(
                    r"^\s*INSERT\s+INTO",
                    "INSERT OR REPLACE INTO",
                    str(sql or ""),
                    count=1,
                    flags=re.IGNORECASE,
                ),
                params,
            )
        con.commit()
        return _BufferedCursor([], rowcount=int(getattr(cur, "rowcount", -1)), lastrowid=getattr(cur, "lastrowid", None))
    finally:
        con.close()


class StorageConnection(sqlite3.Connection):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.row_factory = sqlite3.Row
        self.readonly = False
        self._after_commit: list[Callable[[], None]] = []
        self._managed_write_active = False
        self._allow_managed_commit = False
        self._allow_managed_rollback = False
        self._suppress_manual_transaction_control = False
        self._write_lock_owned = False

    def _acquire_write_lock(self) -> None:
        if not bool(self._write_lock_owned):
            timeout_s = max(
                0.1,
                float(os.environ.get("SQLITE_WRITE_LOCK_TIMEOUT_S", "30") or 30.0),
            )
            acquired = _WRITE_LOCK.acquire(timeout=timeout_s)
            if not acquired:
                threads = [
                    f"{thread.name}:{thread.ident}:{'alive' if thread.is_alive() else 'stopped'}"
                    for thread in threading.enumerate()
                ]
                raise sqlite3.OperationalError(
                    "sqlite_write_lock_timeout:"
                    f"timeout_s={timeout_s}:lock={_WRITE_LOCK!r}:threads={threads}"
                )
            self._write_lock_owned = True

    def _release_write_lock(self) -> None:
        if bool(self._write_lock_owned):
            self._write_lock_owned = False
            _WRITE_LOCK.release()

    def _empty_cursor(self):
        return sqlite3.Connection.execute(self, "SELECT 1 WHERE 0")

    def _transaction_control_noop(self, sql: str):
        if not (bool(self._managed_write_active) and bool(self._suppress_manual_transaction_control)):
            return None
        if re.match(r"^\s*(?:BEGIN|COMMIT|END|ROLLBACK)\b", str(sql or ""), flags=re.IGNORECASE):
            return self._empty_cursor()
        return None

    def execute(self, sql: str, parameters: Any = None):  # type: ignore[override]
        if not bool(getattr(self, "_liveness_storage_connection", False)) and _is_job_heartbeat_read(sql):
            return _read_liveness_rows(sql, parameters)
        if not bool(getattr(self, "_liveness_storage_connection", False)) and _is_job_heartbeat_write(sql):
            return _write_liveness_rows(sql, parameters)
        noop = self._transaction_control_noop(sql)
        if noop is not None:
            return noop
        start_s = time.monotonic()
        if (
            not bool(getattr(self, "readonly", False))
            and not bool(getattr(self, "in_transaction", False))
            and _is_auto_write_statement(sql)
        ):
            self._acquire_write_lock()
            try:
                sqlite3.Connection.execute(self, "BEGIN IMMEDIATE")
            except Exception as exc:
                if _is_transient_sqlite_error(exc):
                    _record_sqlite_busy(sql, exc)
                self._release_write_lock()
                raise
            _mark_active_write_connection(self)
        try:
            cur = _execute_with_repair(self, sql, parameters)
            _record_sqlite_trace(sql, elapsed_ms=(time.monotonic() - start_s) * 1000.0)
            return cur
        except Exception as exc:
            if _is_transient_sqlite_error(exc):
                _record_sqlite_busy(sql, exc)
            raise

    def executemany(self, sql: str, seq_of_parameters: Iterable[Any]):  # type: ignore[override]
        start_s = time.monotonic()
        with _DDL_LOCK:
            prepared = _prepare_sql(self, sql)
        normalized = [_normalize_params(params) for params in seq_of_parameters]
        if (
            not bool(getattr(self, "readonly", False))
            and not bool(getattr(self, "in_transaction", False))
            and _is_auto_write_statement(prepared)
        ):
            self._acquire_write_lock()
            try:
                sqlite3.Connection.execute(self, "BEGIN IMMEDIATE")
            except Exception as exc:
                if _is_transient_sqlite_error(exc):
                    _record_sqlite_busy(prepared, exc)
                self._release_write_lock()
                raise
            _mark_active_write_connection(self)
        try:
            cur = sqlite3.Connection.executemany(self, prepared, normalized)
            _record_sqlite_trace(prepared, row_count=len(normalized), elapsed_ms=(time.monotonic() - start_s) * 1000.0)
            return cur
        except Exception as exc:
            if _is_transient_sqlite_error(exc):
                _record_sqlite_busy(prepared, exc)
            raise

    def executescript(self, sql_script: str):  # type: ignore[override]
        cursor = None
        with _DDL_LOCK:
            for statement in re.split(r";\s*(?:\r?\n|$)", str(sql_script or "")):
                text = statement.strip()
                if text:
                    cursor = self.execute(text)
        return cursor or sqlite3.Connection.execute(self, "SELECT 1 WHERE 0")

    def begin_managed_write(self) -> None:
        if bool(getattr(self, "readonly", False)):
            raise sqlite3.OperationalError("write_transaction_not_allowed_on_readonly_connection")
        self._acquire_write_lock()
        if not bool(getattr(self, "in_transaction", False)):
            try:
                sqlite3.Connection.execute(self, "BEGIN IMMEDIATE")
            except Exception:
                self._release_write_lock()
                raise
        self._managed_write_active = True
        _mark_active_write_connection(self)

    def register_after_commit(self, callback: Callable[[], None]) -> None:
        self._after_commit.append(callback)

    def commit(self) -> None:  # type: ignore[override]
        if (
            bool(self._managed_write_active)
            and bool(self._suppress_manual_transaction_control)
            and not bool(self._allow_managed_commit)
        ):
            return
        callbacks: list[Callable[[], None]] = []
        try:
            sqlite3.Connection.commit(self)
            self._managed_write_active = False
            _clear_active_write_connection(self)
            callbacks = list(self._after_commit)
            self._after_commit.clear()
        finally:
            self._release_write_lock()
        for callback in callbacks:
            callback()

    def commit_managed_write(self) -> None:
        self._allow_managed_commit = True
        try:
            self.commit()
        finally:
            self._allow_managed_commit = False

    def rollback(self) -> None:  # type: ignore[override]
        if (
            bool(self._managed_write_active)
            and bool(self._suppress_manual_transaction_control)
            and not bool(self._allow_managed_rollback)
        ):
            return
        self._after_commit.clear()
        try:
            sqlite3.Connection.rollback(self)
            self._managed_write_active = False
            _clear_active_write_connection(self)
        finally:
            self._release_write_lock()

    def rollback_managed_write(self) -> None:
        self._allow_managed_rollback = True
        try:
            self.rollback()
        finally:
            self._allow_managed_rollback = False

    def close(self) -> None:  # type: ignore[override]
        if (
            bool(getattr(self, "in_transaction", False))
            and _active_write_connection() is self
            and bool(self._managed_write_active)
        ):
            return
        if bool(getattr(self, "in_transaction", False)):
            try:
                sqlite3.Connection.rollback(self)
            except Exception:
                LOGGER.debug("sqlite_close_rollback_failed", exc_info=True)
        _clear_active_write_connection(self)
        try:
            sqlite3.Connection.close(self)
        finally:
            self._release_write_lock()

    @contextmanager
    def transaction(self):
        self.execute("BEGIN")
        try:
            yield self
        except Exception:
            self.rollback()
            raise
        else:
            self.commit()


def _apply_pragmas(con: sqlite3.Connection, *, readonly: bool = False, busy_timeout_ms: int | None = None) -> None:
    con.row_factory = sqlite3.Row
    timeout_ms = int(busy_timeout_ms if busy_timeout_ms is not None else _SQLITE_BUSY_TIMEOUT_MS)
    con.execute(f"PRAGMA busy_timeout={timeout_ms};")
    con.execute("PRAGMA journal_mode;")
    con.execute("PRAGMA busy_timeout;")
    if bool(readonly):
        con.execute("PRAGMA query_only=ON;")
        return
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA foreign_keys=ON;")
    con.execute("PRAGMA defer_foreign_keys=ON;")
    con.execute("PRAGMA trusted_schema=OFF;")
    con.execute("PRAGMA recursive_triggers=ON;")
    con.execute("PRAGMA wal_checkpoint(PASSIVE);")


def _maybe_quick_check(*args: Any, **kwargs: Any) -> None:
    del args, kwargs


def _maybe_wal_checkpoint(*args: Any, **kwargs: Any) -> None:
    del args, kwargs


def _connect_raw(
    *,
    readonly: bool = False,
    timeout_s: float | None = None,
    busy_timeout_ms: int | None = None,
    path_override: Path | str | None = None,
) -> StorageConnection:
    path = Path(path_override).expanduser() if path_override is not None else _current_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    timeout = max(0.05, float(timeout_s if timeout_s is not None else os.environ.get("SQLITE_TIMEOUT_S", "30") or 30.0))
    con = sqlite3.connect(
        str(path),
        timeout=timeout,
        isolation_level=None,
        check_same_thread=False,
        factory=StorageConnection,
    )
    con.readonly = bool(readonly)
    _apply_pragmas(con, readonly=readonly, busy_timeout_ms=busy_timeout_ms)
    return con


_LIVENESS_JOB_LOCKS_SQL = """
CREATE TABLE IF NOT EXISTS job_locks (
  job_name TEXT PRIMARY KEY,
  owner TEXT NOT NULL,
  pid INTEGER NOT NULL,
  acquired_ts_ms INTEGER NOT NULL,
  heartbeat_ts_ms INTEGER NOT NULL,
  expires_ms INTEGER
)
"""
_LIVENESS_JOB_HEARTBEATS_SQL = """
CREATE TABLE IF NOT EXISTS job_heartbeats (
  job_name TEXT PRIMARY KEY,
  owner TEXT NOT NULL,
  pid INTEGER NOT NULL,
  ts_ms INTEGER NOT NULL,
  extra_json TEXT
)
"""
_LIVENESS_TABLE_CONTRACTS: dict[str, dict[str, Any]] = {
    "job_locks": {
        "sql": _LIVENESS_JOB_LOCKS_SQL,
        "columns": ("job_name", "owner", "pid", "acquired_ts_ms", "heartbeat_ts_ms", "expires_ms"),
        "pk": {"job_name": 1},
        "order_column": "heartbeat_ts_ms",
        "defaults": {
            "owner": "''",
            "pid": "0",
            "acquired_ts_ms": "0",
            "heartbeat_ts_ms": "0",
            "expires_ms": "NULL",
        },
    },
    "job_heartbeats": {
        "sql": _LIVENESS_JOB_HEARTBEATS_SQL,
        "columns": ("job_name", "owner", "pid", "ts_ms", "extra_json"),
        "pk": {"job_name": 1},
        "order_column": "ts_ms",
        "defaults": {
            "owner": "''",
            "pid": "0",
            "ts_ms": "0",
            "extra_json": "NULL",
        },
    },
}


def _copy_latest_liveness_rows(
    con: sqlite3.Connection,
    *,
    legacy_table: str,
    target_table: str,
    columns: Sequence[str],
    defaults: dict[str, str],
    order_column: str,
) -> None:
    legacy_columns = _table_columns(con, legacy_table)
    if "job_name" not in legacy_columns:
        return
    expressions: list[str] = []
    for column in columns:
        clean = _ident(column)
        if clean == "job_name":
            expressions.append(f"CAST({_quote(clean)} AS TEXT)")
            continue
        fallback = str(defaults.get(clean) or "NULL")
        if clean in legacy_columns and fallback.upper() != "NULL":
            expressions.append(f"COALESCE({_quote(clean)}, {fallback})")
        elif clean in legacy_columns:
            expressions.append(_quote(clean))
        else:
            expressions.append(fallback)
    cols_sql = ", ".join(_quote(column) for column in columns)
    select_sql = ", ".join(expressions)
    order_expr = f"COALESCE({_quote(order_column)}, 0)" if order_column in legacy_columns else "0"
    _connection_execute_raw(
        con,
        (
            f"INSERT OR REPLACE INTO {_quote(target_table)}({cols_sql}) "
            f"SELECT {select_sql} FROM {_quote(legacy_table)} "
            "WHERE job_name IS NOT NULL AND TRIM(CAST(job_name AS TEXT)) <> '' "
            f"ORDER BY {order_expr}, rowid"
        ),
    )


def _ensure_liveness_table_contract(con: sqlite3.Connection, table: str) -> None:
    spec = dict(_LIVENESS_TABLE_CONTRACTS[str(table)])
    columns = tuple(str(column) for column in (spec.get("columns") or ()))
    pk = dict(spec.get("pk") or {})
    create_sql = str(spec.get("sql") or "")
    if _needs_exact_rebuild(con, str(table), columns, pk=pk):
        legacy_table = _next_legacy_table_name(con, str(table))
        _connection_execute_raw(con, f"ALTER TABLE {_quote(table)} RENAME TO {_quote(legacy_table)}")
        _connection_execute_raw(con, create_sql)
        _copy_latest_liveness_rows(
            con,
            legacy_table=legacy_table,
            target_table=str(table),
            columns=columns,
            defaults=dict(spec.get("defaults") or {}),
            order_column=str(spec.get("order_column") or "rowid"),
        )
        return
    _connection_execute_raw(con, create_sql)


def _ensure_liveness_db_schema() -> None:
    if not _liveness_db_enabled():
        return
    path = _current_liveness_db_path()
    key = f"liveness:{path.resolve()}"
    if key in _INITIALIZED_PATHS and path.exists():
        return
    with _INIT_LOCK:
        if key in _INITIALIZED_PATHS and path.exists():
            return
        con = _connect_raw(readonly=False, path_override=path)
        try:
            _ensure_liveness_table_contract(con, "job_locks")
            _ensure_liveness_table_contract(con, "job_heartbeats")
            sqlite3.Connection.commit(con)
            _INITIALIZED_PATHS.add(key)
        finally:
            con.close()


def connect(readonly: bool = False, **kwargs: Any) -> StorageConnection:
    init_db()
    if not bool(readonly):
        active = _active_write_connection()
        if active is not None:
            return active
    return _connect_raw(readonly=readonly, **kwargs)


def connect_ro() -> StorageConnection:
    return connect(readonly=True)


def connect_ro_direct(**kwargs: Any) -> StorageConnection:
    init_db()
    return _connect_raw(readonly=True, **kwargs)


def connect_rw_direct(**kwargs: Any) -> StorageConnection:
    init_db()
    return _connect_raw(readonly=False, **kwargs)


def connect_liveness_ro_direct(**kwargs: Any) -> StorageConnection:
    return _connect_liveness_storage_ro_direct(**kwargs)


def connect_liveness_rw_direct(**kwargs: Any) -> StorageConnection:
    return _connect_liveness_storage_rw_direct(**kwargs)


def _connect_liveness_storage_ro_direct(**kwargs: Any) -> StorageConnection:
    if not _liveness_db_enabled():
        return connect_ro_direct(**kwargs)
    _ensure_liveness_db_schema()
    con = _connect_raw(readonly=True, path_override=_current_liveness_db_path(), **kwargs)
    con._liveness_storage_connection = True
    return con


def _connect_liveness_storage_rw_direct(**kwargs: Any) -> StorageConnection:
    if not _liveness_db_enabled():
        return connect_rw_direct(**kwargs)
    _ensure_liveness_db_schema()
    con = _connect_raw(readonly=False, path_override=_current_liveness_db_path(), **kwargs)
    con._liveness_storage_connection = True
    return con


@contextmanager
def connection(readonly: bool = False):
    con = connect(readonly=readonly)
    try:
        yield con
    finally:
        con.close()


@contextmanager
def transaction(readonly: bool = False):
    con = connect(readonly=readonly)
    try:
        if not readonly:
            con.execute("BEGIN IMMEDIATE")
        yield con
        if not readonly:
            con.commit()
    except Exception:
        if not readonly:
            con.rollback()
        raise
    finally:
        con.close()


def execute(sql: str, params: Any = None):
    with connect(readonly=False) as con:
        cur = con.execute(sql, params)
        con.commit()
        return cur


def executemany(sql: str, seq_of_params: Iterable[Any]):
    with connect(readonly=False) as con:
        cur = con.executemany(sql, seq_of_params)
        con.commit()
        return cur


def fetch_one(sql: str, params: Any = None):
    with connect(readonly=True) as con:
        return con.execute(sql, params).fetchone()


def fetch_all(sql: str, params: Any = None):
    with connect(readonly=True) as con:
        return con.execute(sql, params).fetchall()


def _is_transient_sqlite_error(exc: BaseException) -> bool:
    text = str(exc or "").lower()
    return isinstance(exc, sqlite3.OperationalError) and (
        "locked" in text or "busy" in text or "interrupted" in text
    )


def run_write_txn(
    fn: Callable[[StorageConnection], Any],
    *,
    attempts: int = 3,
    table: str | None = None,
    operation: str | None = None,
    direct: bool = False,
    maintenance: bool = True,
    timeout_s: float | None = None,
    busy_timeout_ms: int | None = None,
    **kwargs: Any,
) -> Any:
    del table, operation, maintenance, kwargs
    last_error: BaseException | None = None
    for attempt in range(max(1, int(attempts or 1))):
        with _WRITE_LOCK:
            if _active_write_connection() is not None:
                raise sqlite3.OperationalError("write_transaction_already_active")
            connector = connect_rw_direct if bool(direct) else connect
            con = connector(timeout_s=timeout_s, busy_timeout_ms=busy_timeout_ms)
            try:
                con.begin_managed_write()
                if hasattr(con, "_suppress_manual_transaction_control"):
                    con._suppress_manual_transaction_control = True
                result = fn(con)
                if hasattr(con, "commit_managed_write"):
                    con.commit_managed_write()
                else:
                    con.commit()
                return result
            except Exception as exc:
                if hasattr(con, "rollback_managed_write"):
                    con.rollback_managed_write()
                else:
                    con.rollback()
                last_error = exc
                if not _is_transient_sqlite_error(exc) or attempt >= max(1, int(attempts or 1)) - 1:
                    raise
                with _SQLITE_TRACE_LOCK:
                    _SQLITE_TRACE_TOTALS["busy_retry_count"] = int(_SQLITE_TRACE_TOTALS.get("busy_retry_count") or 0) + 1
                    _SQLITE_TRACE_TOTALS["retries"] = int(_SQLITE_TRACE_TOTALS.get("retries") or 0) + 1
                time.sleep(min(0.25, 0.02 * (attempt + 1)))
            finally:
                if hasattr(con, "_suppress_manual_transaction_control"):
                    con._suppress_manual_transaction_control = False
                con.close()
    if last_error is not None:
        raise last_error
    return None


def register_after_commit(con: StorageConnection | None, callback: Callable[[], None]) -> None:
    if con is not None and hasattr(con, "register_after_commit"):
        con.register_after_commit(callback)
    else:
        callback()


def _safe_commit(con: StorageConnection, *, maintenance: bool = True) -> None:
    del maintenance
    con.commit()


def set_write_maintenance(con: StorageConnection, enabled: bool = True) -> None:
    del con, enabled


def note_write(con: StorageConnection, *, maintenance: bool = True) -> None:
    del con, maintenance


def checkpoint_if_due(*args: Any, **kwargs: Any) -> None:
    del args, kwargs


def _new_connection(**kwargs: Any) -> StorageConnection:
    return connect(**kwargs)


def _pid_is_running(pid: int) -> bool:
    if int(pid or 0) <= 0:
        return False
    try:
        os.kill(int(pid), 0)
        return True
    except OSError:
        return False


def _table_exists(con: StorageConnection, table: str) -> bool:
    row = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (str(table),),
    ).fetchone()
    return bool(row)


def _has_column(con: StorageConnection, table: str, col: str) -> bool:
    rows = con.execute(f"PRAGMA table_info({_ident(table)})").fetchall() or []
    return str(col) in {str(row[1]) for row in rows}


def _create_table(con: sqlite3.Connection, table: str, columns: Sequence[str], unique: Sequence[Sequence[str]] = ()) -> None:
    _ensure_table(con, table, columns)
    for cols in unique:
        _ensure_unique(con, table, cols)


def _ensure_regime_state_schema(con: sqlite3.Connection) -> None:
    create_sql = """
        CREATE TABLE IF NOT EXISTS regime_state (
          time INTEGER NOT NULL,
          symbol TEXT NOT NULL,
          volatility_regime TEXT NOT NULL,
          trend_regime TEXT NOT NULL,
          liquidity_regime TEXT NOT NULL,
          created_ts_ms INTEGER NOT NULL DEFAULT 0,
          PRIMARY KEY(symbol, time)
        )
    """
    desired_columns = (
        "time",
        "symbol",
        "volatility_regime",
        "trend_regime",
        "liquidity_regime",
        "created_ts_ms",
    )
    if not _table_exists(con, "regime_state"):
        _connection_execute_raw(con, create_sql)
    else:
        rows = _table_info(con, "regime_state")
        actual_columns = {str(row[1]) for row in rows}
        actual_pk = {str(row[1]): int(row[5] or 0) for row in rows}
        needs_rebuild = (
            not set(desired_columns).issubset(actual_columns)
            or int(actual_pk.get("symbol") or 0) != 1
            or int(actual_pk.get("time") or 0) != 2
        )
        if needs_rebuild:
            legacy_table = _next_legacy_table_name(con, "regime_state")
            _connection_execute_raw(con, "DROP INDEX IF EXISTS idx_regime_state_symbol_time_desc")
            _connection_execute_raw(con, f"ALTER TABLE {_quote('regime_state')} RENAME TO {_quote(legacy_table)}")
            _connection_execute_raw(con, create_sql)
            legacy_columns = _table_columns(con, legacy_table)
            def _coalesce_expr(candidates: Sequence[str], default_sql: str) -> str:
                available = [str(candidate) for candidate in candidates if str(candidate) in legacy_columns]
                if not available:
                    return str(default_sql)
                return f"COALESCE({', '.join(available)}, {default_sql})"

            expressions = {
                "time": _coalesce_expr(("time", "ts_ms", "created_ts_ms"), "0"),
                "symbol": "COALESCE(symbol, '')" if "symbol" in legacy_columns else "''",
                "volatility_regime": (
                    "COALESCE(volatility_regime, 'unknown')"
                    if "volatility_regime" in legacy_columns
                    else "'unknown'"
                ),
                "trend_regime": (
                    "COALESCE(trend_regime, 'unknown')"
                    if "trend_regime" in legacy_columns
                    else "'unknown'"
                ),
                "liquidity_regime": (
                    "COALESCE(liquidity_regime, 'unknown')"
                    if "liquidity_regime" in legacy_columns
                    else "'unknown'"
                ),
                "created_ts_ms": _coalesce_expr(("created_ts_ms", "time", "ts_ms"), "0"),
            }
            _copy_legacy_rows(
                con,
                legacy_table=legacy_table,
                target_table="regime_state",
                columns=desired_columns,
                expressions=expressions,
            )
    _connection_execute_raw(
        con,
        "CREATE INDEX IF NOT EXISTS idx_regime_state_symbol_time_desc ON regime_state(symbol, time DESC)",
    )


def _ensure_runtime_aux_schema(con: sqlite3.Connection) -> None:
    """
    CREATE TABLE IF NOT EXISTS runtime_meta (...);
    CREATE TABLE IF NOT EXISTS schema_version (...);
    CREATE TABLE IF NOT EXISTS schema_migrations (...);
    CREATE TABLE IF NOT EXISTS runtime_metrics (...);
    CREATE TABLE IF NOT EXISTS events (...);
    CREATE TABLE IF NOT EXISTS labels (...);
    CREATE TABLE IF NOT EXISTS predictions (...);
    CREATE TABLE IF NOT EXISTS alerts (...);
    CREATE TABLE IF NOT EXISTS decision_log (...);
    CREATE TABLE IF NOT EXISTS shadow_predictions (...);
    CREATE TABLE IF NOT EXISTS prediction_history (...);
    CREATE TABLE IF NOT EXISTS regime_state (...);
    CREATE TABLE IF NOT EXISTS tracked_model_registry (...);
    CREATE TABLE IF NOT EXISTS tracked_predictions (...);
    CREATE TABLE IF NOT EXISTS prediction_explanations (...);
    CREATE TABLE IF NOT EXISTS alert_interactions (...);
    CREATE TABLE IF NOT EXISTS decision_views (...);
    CREATE TABLE IF NOT EXISTS equity_history (...);
    CREATE TABLE IF NOT EXISTS equity_drift (...);
    CREATE TABLE IF NOT EXISTS broker_account (...);
    CREATE TABLE IF NOT EXISTS risk_state (...);
    CREATE TABLE IF NOT EXISTS job_locks (...);
    CREATE TABLE IF NOT EXISTS job_heartbeats (...);
    CREATE TABLE IF NOT EXISTS job_checkpoints (...);
    CREATE TABLE IF NOT EXISTS model_registry (...);
    CREATE TABLE IF NOT EXISTS models (...);
    CREATE TABLE IF NOT EXISTS model_versions (...);
    CREATE TABLE IF NOT EXISTS model_version_performance (...);
    CREATE TABLE IF NOT EXISTS model_lifecycle_runs (...);
    CREATE TABLE IF NOT EXISTS model_marketplace_scores (...);
    CREATE TABLE IF NOT EXISTS champion_assignments (...);
    CREATE TABLE IF NOT EXISTS model_competition_rankings (...);
    CREATE TABLE IF NOT EXISTS model_hyperparameter_registry (...);
    CREATE TABLE IF NOT EXISTS model_best_params (...);
    CREATE TABLE IF NOT EXISTS alpha_candidates (...);
    CREATE TABLE IF NOT EXISTS alpha_lifecycle (...);
    CREATE TABLE IF NOT EXISTS hypothesis_registry (...);
    CREATE TABLE IF NOT EXISTS backtest_cpcv_runs (...);
    CREATE TABLE IF NOT EXISTS backtest_cpcv_path_results (...);
    CREATE TABLE IF NOT EXISTS drift_retrain_events (...);
    CREATE TABLE IF NOT EXISTS promotion_statistical_evidence (...);
    CREATE TABLE IF NOT EXISTS temporal_model_eval (...);
    CREATE TABLE IF NOT EXISTS news_event_features (...);
    CREATE TABLE IF NOT EXISTS news_symbol_features (...);
    CREATE TABLE IF NOT EXISTS options_event_features (...);
    CREATE TABLE IF NOT EXISTS finbert_sentiment_enrichments (...);
    CREATE TABLE IF NOT EXISTS finra_short_sale_volume (...);
    CREATE TABLE IF NOT EXISTS finra_short_interest (...);
    CREATE TABLE IF NOT EXISTS crypto_funding_rates (...);
    """
    # storage-route-audit: allow - centralized init_db schema creation under _INIT_LOCK.
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS runtime_meta (
          key TEXT PRIMARY KEY,
          value TEXT,
          updated_ts_ms INTEGER
        );

        CREATE TABLE IF NOT EXISTS schema_version (
          version INTEGER PRIMARY KEY,
          applied_ts_ms INTEGER NOT NULL,
          status TEXT NOT NULL,
          notes TEXT
        );

        CREATE TABLE IF NOT EXISTS schema_migrations (
          version INTEGER PRIMARY KEY,
          name TEXT,
          applied_ts_ms INTEGER
        );

        CREATE TABLE IF NOT EXISTS runtime_metrics (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER,
          metric TEXT,
          value_num REAL,
          value_text TEXT,
          tags_json TEXT
        );

        CREATE TABLE IF NOT EXISTS market_features (
          ts_ms INTEGER NOT NULL,
          symbol TEXT NOT NULL,
          v INTEGER NOT NULL,
          features_json TEXT NOT NULL,
          PRIMARY KEY(symbol, ts_ms, v)
        );
        CREATE INDEX IF NOT EXISTS idx_market_features_symbol_ts
          ON market_features(symbol, ts_ms);

        CREATE TABLE IF NOT EXISTS events (
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
        );

        CREATE TABLE IF NOT EXISTS labels (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          event_id INTEGER,
          symbol TEXT,
          horizon_s INTEGER,
          label TEXT,
          impact_z REAL,
          created_at_ms INTEGER,
          ts_ms INTEGER
        );

        CREATE TABLE IF NOT EXISTS labels_exec (
          event_id INTEGER NOT NULL,
          symbol TEXT NOT NULL,
          horizon_s INTEGER NOT NULL,
          ts_ms INTEGER NOT NULL,
          source TEXT NOT NULL DEFAULT 'heuristic',
          realized INTEGER NOT NULL DEFAULT 0,
          side INTEGER NOT NULL,
          gross_ret REAL NOT NULL,
          net_ret REAL NOT NULL,
          gross_z REAL,
          net_z REAL,
          mid_in REAL,
          mid_out REAL,
          spread_in REAL,
          fees_bps REAL NOT NULL,
          slippage_bps REAL NOT NULL,
          spread_bps REAL NOT NULL,
          total_cost_bps REAL NOT NULL,
          extra_json TEXT,
          PRIMARY KEY(event_id, symbol, horizon_s)
        );

        CREATE TABLE IF NOT EXISTS event_log (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER NOT NULL,
          event_type TEXT NOT NULL,
          event_source TEXT NOT NULL,
          event_version INTEGER NOT NULL DEFAULT 1,
          entity_type TEXT,
          entity_id TEXT,
          correlation_id TEXT,
          payload_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS shadow_predictions (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          event_id INTEGER,
          symbol TEXT,
          horizon_s INTEGER,
          model_name TEXT,
          prediction REAL,
          confidence REAL,
          ts_ms INTEGER,
          payload_json TEXT
        );

        CREATE TABLE IF NOT EXISTS prediction_history (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER,
          event_id INTEGER,
          symbol TEXT,
          horizon_s INTEGER,
          predicted_z REAL,
          confidence REAL,
          confidence_raw REAL,
          prediction_strength REAL,
          model_name TEXT,
          model_id TEXT,
          model_version TEXT,
          regime_time_ms INTEGER,
          volatility_regime TEXT,
          trend_regime TEXT,
          liquidity_regime TEXT
        );

        CREATE TABLE IF NOT EXISTS regime_state (
          time INTEGER NOT NULL,
          symbol TEXT NOT NULL,
          volatility_regime TEXT NOT NULL,
          trend_regime TEXT NOT NULL,
          liquidity_regime TEXT NOT NULL,
          created_ts_ms INTEGER NOT NULL DEFAULT 0,
          PRIMARY KEY(symbol, time)
        );

        CREATE TABLE IF NOT EXISTS tracked_model_registry (
          model_name TEXT NOT NULL,
          version TEXT NOT NULL,
          created_ts_ms INTEGER NOT NULL,
          updated_ts_ms INTEGER NOT NULL,
          metadata_json TEXT NOT NULL DEFAULT '{}',
          PRIMARY KEY(model_name, version)
        );

        CREATE TABLE IF NOT EXISTS tracked_predictions (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER NOT NULL,
          symbol TEXT NOT NULL,
          model_name TEXT NOT NULL,
          model_version TEXT,
          prediction REAL NOT NULL,
          confidence REAL,
          features_version TEXT,
          event_id INTEGER,
          horizon_s INTEGER,
          prediction_id INTEGER,
          source_alert_id INTEGER,
          model_id TEXT,
          tracking_source TEXT,
          metadata_json TEXT NOT NULL DEFAULT '{}'
        );

        CREATE TABLE IF NOT EXISTS prediction_explanations (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          symbol TEXT NOT NULL,
          ts INTEGER NOT NULL,
          model_family TEXT NOT NULL,
          model_name TEXT,
          version TEXT,
          explanation_type TEXT NOT NULL,
          top_features TEXT,
          base_value REAL,
          diagnostics TEXT,
          created_ts INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS alert_interactions (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER,
          alert_id INTEGER,
          decision_id INTEGER,
          interaction_type TEXT,
          actor TEXT,
          session_id TEXT,
          source TEXT,
          detail_json TEXT
        );

        CREATE TABLE IF NOT EXISTS decision_views (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER,
          decision_id INTEGER,
          actor TEXT,
          session_id TEXT,
          source TEXT,
          detail_json TEXT
        );

        CREATE TABLE IF NOT EXISTS equity_history (
          ts_ms INTEGER PRIMARY KEY,
          equity REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS equity_drift (
          ts_ms INTEGER PRIMARY KEY,
          broker_equity REAL NOT NULL,
          backtest_equity REAL NOT NULL,
          diff_equity REAL NOT NULL,
          diff_equity_pct REAL NOT NULL,
          level TEXT NOT NULL,
          reason TEXT,
          backtest_run_id INTEGER,
          backtest_ts_ms INTEGER,
          detail_json TEXT
        );

        CREATE TABLE IF NOT EXISTS broker_account (
          ts_ms INTEGER PRIMARY KEY,
          updated_ts_ms INTEGER,
          broker TEXT,
          account_id TEXT,
          equity REAL,
          cash REAL,
          buying_power REAL,
          maintenance_margin REAL,
          day_pnl REAL,
          unrealized_pnl REAL,
          realized_pnl REAL,
          currency TEXT,
          extra_json TEXT
        );

        CREATE TABLE IF NOT EXISTS risk_state (
          key TEXT PRIMARY KEY,
          value TEXT,
          updated_ts_ms INTEGER
        );

        CREATE TABLE IF NOT EXISTS job_locks (
          job_name TEXT PRIMARY KEY,
          owner TEXT NOT NULL,
          pid INTEGER NOT NULL,
          acquired_ts_ms INTEGER NOT NULL,
          heartbeat_ts_ms INTEGER NOT NULL,
          expires_ms INTEGER
        );

        CREATE TABLE IF NOT EXISTS job_heartbeats (
          job_name TEXT PRIMARY KEY,
          owner TEXT NOT NULL,
          pid INTEGER NOT NULL,
          ts_ms INTEGER NOT NULL,
          extra_json TEXT
        );

        CREATE TABLE IF NOT EXISTS job_checkpoints (
          job_name TEXT PRIMARY KEY,
          last_event_id INTEGER,
          last_event_ts_ms INTEGER,
          updated_ts_ms INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS model_registry (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          model_name TEXT,
          version TEXT,
          model_version TEXT,
          family TEXT,
          model_family TEXT,
          status TEXT,
          promotion_status TEXT,
          created_ts INTEGER,
          updated_ts INTEGER,
          metadata_json TEXT,
          metrics_json TEXT,
          feature_schema_json TEXT,
          blob BLOB
        );

        CREATE TABLE IF NOT EXISTS models (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          symbol TEXT,
          model_name TEXT,
          version TEXT,
          status TEXT,
          is_active INTEGER,
          training_data_window_json TEXT,
          performance_metrics_json TEXT,
          metadata_json TEXT,
          created_ts INTEGER,
          updated_ts INTEGER
        );

        CREATE TABLE IF NOT EXISTS model_versions (
          model_name TEXT NOT NULL,
          model_version TEXT NOT NULL,
          model_kind TEXT,
          parent_version TEXT,
          mutation_kind TEXT,
          stage TEXT,
          status TEXT,
          live_ready INTEGER,
          training_job_name TEXT,
          train_scope_json TEXT,
          meta_json TEXT,
          created_ts_ms INTEGER,
          updated_ts_ms INTEGER,
          PRIMARY KEY(model_name, model_version)
        );

        CREATE TABLE IF NOT EXISTS model_version_performance (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          model_name TEXT,
          model_version TEXT,
          metric_scope TEXT,
          metric_name TEXT,
          metric_value REAL,
          sample_n INTEGER,
          recorded_ts_ms INTEGER,
          meta_json TEXT
        );

        CREATE TABLE IF NOT EXISTS model_lifecycle_runs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          model_name TEXT,
          model_version TEXT,
          parent_version TEXT,
          action TEXT,
          status TEXT,
          triggered_by TEXT,
          mutation_kind TEXT,
          details_json TEXT,
          created_ts_ms INTEGER,
          updated_ts_ms INTEGER
        );

        CREATE TABLE IF NOT EXISTS model_marketplace_scores (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          model_id TEXT NOT NULL DEFAULT 'baseline',
          model_name TEXT NOT NULL,
          symbol TEXT NOT NULL,
          horizon_s INTEGER NOT NULL DEFAULT 0,
          regime TEXT NOT NULL DEFAULT 'global',
          stage TEXT NOT NULL DEFAULT 'challenger',
          score REAL NOT NULL DEFAULT 0,
          trades INTEGER NOT NULL DEFAULT 0,
          wins INTEGER NOT NULL DEFAULT 0,
          losses INTEGER NOT NULL DEFAULT 0,
          gross_pnl REAL NOT NULL DEFAULT 0,
          net_pnl REAL NOT NULL DEFAULT 0,
          avg_confidence REAL NOT NULL DEFAULT 0,
          last_signal_ts_ms INTEGER,
          updated_ts_ms INTEGER NOT NULL DEFAULT 0,
          meta_json TEXT,
          status TEXT,
          created_ts INTEGER,
          updated_ts INTEGER,
          metadata_json TEXT
        );
        CREATE UNIQUE INDEX IF NOT EXISTS uq_model_marketplace_scores_key
          ON model_marketplace_scores(model_id, model_name, symbol, horizon_s, regime);
        CREATE INDEX IF NOT EXISTS idx_model_marketplace_stage_score
          ON model_marketplace_scores(stage, score DESC, updated_ts_ms DESC);
        CREATE INDEX IF NOT EXISTS idx_model_marketplace_symbol_horizon
          ON model_marketplace_scores(symbol, horizon_s, score DESC);

        CREATE TABLE IF NOT EXISTS model_metrics (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER NOT NULL,
          model_name TEXT NOT NULL,
          symbol TEXT NOT NULL,
          horizon_s INTEGER NOT NULL,
          n INTEGER NOT NULL,
          metrics_json TEXT NOT NULL,
          UNIQUE(model_name, symbol, horizon_s)
        );

        CREATE TABLE IF NOT EXISTS champion_assignments (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          scope TEXT,
          symbol TEXT,
          horizon_s INTEGER,
          model_id TEXT,
          model_name TEXT,
          assigned_ts_ms INTEGER,
          metadata_json TEXT
        );

        CREATE TABLE IF NOT EXISTS model_competition_rankings (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ranking_scope TEXT,
          model_id TEXT,
          model_name TEXT,
          rank INTEGER,
          score REAL,
          created_ts_ms INTEGER,
          metadata_json TEXT
        );

        CREATE TABLE IF NOT EXISTS realized_outcomes (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          symbol TEXT NOT NULL,
          ts_ms INTEGER NOT NULL,
          realized_return REAL NOT NULL,
          metadata_json TEXT NOT NULL DEFAULT '{}',
          created_ts_ms INTEGER,
          updated_ts_ms INTEGER,
          UNIQUE(symbol, ts_ms)
        );

        CREATE TABLE IF NOT EXISTS model_performance (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          tracked_prediction_id INTEGER,
          prediction_id INTEGER,
          outcome_id INTEGER,
          "time" INTEGER NOT NULL,
          prediction_time INTEGER,
          symbol TEXT,
          model_id TEXT,
          model_name TEXT,
          model_version TEXT,
          horizon_s INTEGER,
          prediction REAL,
          realized_return REAL,
          error REAL,
          directional_accuracy INTEGER,
          pnl_impact REAL,
          rolling_score REAL,
          regime_time_ms INTEGER,
          volatility_regime TEXT NOT NULL DEFAULT 'unknown',
          trend_regime TEXT NOT NULL DEFAULT 'unknown',
          liquidity_regime TEXT NOT NULL DEFAULT 'unknown',
          metadata_json TEXT NOT NULL DEFAULT '{}',
          created_ts_ms INTEGER,
          updated_ts_ms INTEGER
        );

        CREATE TABLE IF NOT EXISTS model_hyperparameter_registry (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts INTEGER,
          model_family TEXT,
          model_name TEXT,
          symbol TEXT,
          tuner TEXT,
          objective TEXT,
          study_name TEXT,
          params TEXT,
          params_json TEXT,
          metric_value REAL,
          trial_count INTEGER,
          best_trial_number INTEGER,
          seed INTEGER,
          cpcv_mean_sharpe REAL,
          cpcv_median_sharpe REAL,
          cpcv_pbo REAL,
          diagnostics TEXT
        );

        CREATE TABLE IF NOT EXISTS model_best_params (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          model_family TEXT,
          symbol TEXT,
          ts INTEGER,
          study_name TEXT,
          params_json TEXT,
          value REAL,
          trial_number INTEGER,
          seed INTEGER
        );

        CREATE TABLE IF NOT EXISTS alpha_candidates (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          candidate_name TEXT,
          candidate_version TEXT,
          model_family TEXT,
          feature_ids TEXT,
          generation_method TEXT,
          hyperparams TEXT,
          status TEXT,
          diagnostics TEXT,
          created_ts INTEGER
        );

        CREATE TABLE IF NOT EXISTS alpha_lifecycle (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          candidate_id INTEGER,
          stage TEXT,
          outcome TEXT,
          metrics TEXT,
          notes TEXT,
          created_ts INTEGER,
          alert_id INTEGER,
          created_ts_ms INTEGER,
          expires_ts_ms INTEGER,
          half_life_ms INTEGER,
          volatility REAL,
          status TEXT,
          last_touch_ts_ms INTEGER,
          meta_json TEXT
        );

        CREATE TABLE IF NOT EXISTS hypothesis_registry (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          created_ts INTEGER,
          model_name TEXT,
          candidate_version TEXT,
          n_observations INTEGER,
          t_statistic REAL,
          deflated_sharpe REAL,
          threshold_t REAL,
          n_competing_trials INTEGER,
          passed INTEGER,
          diagnostics TEXT
        );

        CREATE TABLE IF NOT EXISTS backtest_cpcv_runs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          created_ts INTEGER,
          ts INTEGER,
          model_name TEXT,
          candidate_version TEXT,
          model_id TEXT,
          n_splits INTEGER,
          n_test_splits INTEGER,
          embargo_pct REAL,
          n_paths INTEGER,
          path_index INTEGER,
          path_returns TEXT,
          path_sharpes TEXT,
          mean_sharpe REAL,
          median_sharpe REAL,
          pbo REAL,
          sharpe REAL,
          deflated_sharpe REAL,
          n_trials INTEGER,
          total_return REAL,
          max_drawdown REAL,
          cfg TEXT,
          payload TEXT,
          diagnostics TEXT
        );

        CREATE TABLE IF NOT EXISTS backtest_cpcv_path_results (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          created_ts INTEGER,
          ts INTEGER,
          model_name TEXT,
          candidate_version TEXT,
          path_index INTEGER,
          path_returns TEXT,
          path_sharpes TEXT,
          sharpe REAL,
          deflated_sharpe REAL,
          payload TEXT
        );

        CREATE TABLE IF NOT EXISTS drift_retrain_events (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          created_ts INTEGER,
          model_name TEXT,
          family TEXT,
          trigger_type TEXT,
          trigger_metrics TEXT,
          action_taken TEXT,
          cooldown_applied INTEGER,
          candidate_version TEXT,
          outcome_status TEXT,
          diagnostics TEXT
        );

        CREATE TABLE IF NOT EXISTS promotion_statistical_evidence (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts INTEGER,
          model_id TEXT,
          feature_id TEXT,
          evidence_kind TEXT,
          test_name TEXT,
          t_stat REAL,
          p_value REAL,
          q_value REAL,
          bootstrap_samples INTEGER,
          decision TEXT,
          payload_json TEXT,
          prev_hash TEXT,
          row_hash TEXT
        );

        CREATE TABLE IF NOT EXISTS temporal_model_eval (
          key_type TEXT,
          key TEXT,
          horizon_s INTEGER,
          model_kind TEXT,
          ts_ms INTEGER,
          n_train INTEGER,
          n_eval INTEGER,
          rmse REAL,
          spearman REAL,
          directional_acc REAL
        );

        CREATE INDEX IF NOT EXISTS idx_event_log_ts ON event_log(ts_ms);
        """
    )
    con.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_events_event_key_ts_ms ON events(event_key, ts_ms)")
    con.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_model_best_params_model_family_symbol ON model_best_params(model_family, symbol)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_model_hparam_registry_family_ts ON model_hyperparameter_registry(model_family, ts DESC)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_realized_outcomes_symbol_ts ON realized_outcomes(symbol, ts_ms)")
    con.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_model_performance_tracked_prediction_id ON model_performance(tracked_prediction_id)")
    con.execute('CREATE INDEX IF NOT EXISTS idx_model_performance_identity_time ON model_performance(model_name, model_version, symbol, "time" DESC, id DESC)')
    con.execute('CREATE INDEX IF NOT EXISTS idx_model_performance_model_id_time ON model_performance(model_id, symbol, "time" DESC, id DESC)')
    con.execute('CREATE INDEX IF NOT EXISTS idx_model_performance_regime_time ON model_performance(model_name, model_version, symbol, volatility_regime, trend_regime, liquidity_regime, "time" DESC, id DESC)')
    con.execute("CREATE INDEX IF NOT EXISTS idx_job_checkpoints_updated ON job_checkpoints(updated_ts_ms)")
    _ensure_regime_state_schema(con)
    con.execute("CREATE INDEX IF NOT EXISTS idx_tracked_model_registry_updated ON tracked_model_registry(updated_ts_ms DESC)")
    for column_name, ddl in (
        ("prediction_id", "INTEGER"),
        ("source_alert_id", "INTEGER"),
        ("model_id", "TEXT"),
        ("tracking_source", "TEXT"),
        ("metadata_json", "TEXT NOT NULL DEFAULT '{}'"),
    ):
        _alter_add_column_if_missing(con, "tracked_predictions", column_name, ddl)
    con.execute("CREATE INDEX IF NOT EXISTS idx_tracked_predictions_ts ON tracked_predictions(ts_ms DESC)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_tracked_predictions_symbol_ts ON tracked_predictions(symbol, ts_ms DESC)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_tracked_predictions_prediction_id ON tracked_predictions(prediction_id)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_prediction_explanations_symbol_ts ON prediction_explanations(symbol, ts DESC)")
    for column_name, ddl in (
        ("confidence_raw", "REAL"),
        ("prediction_strength", "REAL"),
        ("model_id", "TEXT"),
        ("model_version", "TEXT"),
        ("regime_time_ms", "INTEGER"),
        ("volatility_regime", "TEXT"),
        ("trend_regime", "TEXT"),
        ("liquidity_regime", "TEXT"),
    ):
        _alter_add_column_if_missing(con, "prediction_history", column_name, ddl)


def _ensure_strategy_metrics_schema(con: sqlite3.Connection) -> None:
    """
    CREATE TABLE IF NOT EXISTS portfolio_bt_runs (...);
    CREATE TABLE IF NOT EXISTS portfolio_bt_points (...);
    """
    _create_table(con, "portfolio_bt_runs", ("id", "ts_ms", "start_ts_ms", "end_ts_ms", "metrics_json"))
    _create_table(
        con,
        "portfolio_bt_points",
        ("run_id", "ts_ms", "ret", "equity", "drawdown", "exec_cost", "slippage", "fees", "detail_json"),
        (("run_id", "ts_ms"),),
    )
    con.execute("CREATE INDEX IF NOT EXISTS idx_portfolio_bt_points_run ON portfolio_bt_points(run_id, ts_ms)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_portfolio_bt_points_ts ON portfolio_bt_points(ts_ms)")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS model_metrics (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER NOT NULL,
          model_name TEXT NOT NULL,
          symbol TEXT NOT NULL,
          horizon_s INTEGER NOT NULL,
          n INTEGER NOT NULL,
          metrics_json TEXT NOT NULL,
          UNIQUE(model_name, symbol, horizon_s)
        )
        """
    )
    con.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_model_metrics_key ON model_metrics(model_name, symbol, horizon_s)"
    )


def _ensure_universe_audit_schema(con: sqlite3.Connection) -> None:
    """
    CREATE TABLE IF NOT EXISTS symbols (...);
    CREATE TABLE IF NOT EXISTS symbol_universe (...);
    """
    _create_table(con, "symbols", ("symbol", "score", "status", "asset_class", "meta_json", "updated_ts_ms"), (("symbol",),))
    _create_table(con, "symbol_universe", ("symbol", "status", "first_seen_ms", "last_seen_ms", "seen_n", "meta_json"), (("symbol",),))


def _ensure_universe_pit_schema(con: sqlite3.Connection) -> None:
    """SQLite test backend keeps universe PIT state in symbol_universe."""
    del con


def _ensure_labels_price_schema(con: sqlite3.Connection) -> None:
    """Labels and prices are created by runtime and live-ingestion schema helpers."""
    _alter_add_column_if_missing(con, "labels", "impact_z", "REAL")
    _alter_add_column_if_missing(con, "labels", "created_at_ms", "INTEGER")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS labels_exec (
          event_id INTEGER NOT NULL,
          symbol TEXT NOT NULL,
          horizon_s INTEGER NOT NULL,
          ts_ms INTEGER NOT NULL,
          source TEXT NOT NULL DEFAULT 'heuristic',
          realized INTEGER NOT NULL DEFAULT 0,
          side INTEGER NOT NULL,
          gross_ret REAL NOT NULL,
          net_ret REAL NOT NULL,
          gross_z REAL,
          net_z REAL,
          mid_in REAL,
          mid_out REAL,
          spread_in REAL,
          fees_bps REAL NOT NULL,
          slippage_bps REAL NOT NULL,
          spread_bps REAL NOT NULL,
          total_cost_bps REAL NOT NULL,
          extra_json TEXT,
          PRIMARY KEY(event_id, symbol, horizon_s)
        )
        """
    )


def _ensure_model_marketplace_scores_schema(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS model_marketplace_scores (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          model_id TEXT NOT NULL DEFAULT 'baseline',
          model_name TEXT NOT NULL,
          symbol TEXT NOT NULL,
          horizon_s INTEGER NOT NULL DEFAULT 0,
          regime TEXT NOT NULL DEFAULT 'global',
          stage TEXT NOT NULL DEFAULT 'challenger',
          score REAL NOT NULL DEFAULT 0,
          trades INTEGER NOT NULL DEFAULT 0,
          wins INTEGER NOT NULL DEFAULT 0,
          losses INTEGER NOT NULL DEFAULT 0,
          gross_pnl REAL NOT NULL DEFAULT 0,
          net_pnl REAL NOT NULL DEFAULT 0,
          avg_confidence REAL NOT NULL DEFAULT 0,
          last_signal_ts_ms INTEGER,
          updated_ts_ms INTEGER NOT NULL DEFAULT 0,
          meta_json TEXT,
          status TEXT,
          created_ts INTEGER,
          updated_ts INTEGER,
          metadata_json TEXT
        )
        """
    )
    for column_name, ddl in (
        ("model_id", "TEXT NOT NULL DEFAULT 'baseline'"),
        ("model_name", "TEXT NOT NULL DEFAULT ''"),
        ("symbol", "TEXT NOT NULL DEFAULT ''"),
        ("horizon_s", "INTEGER NOT NULL DEFAULT 0"),
        ("regime", "TEXT NOT NULL DEFAULT 'global'"),
        ("stage", "TEXT NOT NULL DEFAULT 'challenger'"),
        ("score", "REAL NOT NULL DEFAULT 0"),
        ("trades", "INTEGER NOT NULL DEFAULT 0"),
        ("wins", "INTEGER NOT NULL DEFAULT 0"),
        ("losses", "INTEGER NOT NULL DEFAULT 0"),
        ("gross_pnl", "REAL NOT NULL DEFAULT 0"),
        ("net_pnl", "REAL NOT NULL DEFAULT 0"),
        ("avg_confidence", "REAL NOT NULL DEFAULT 0"),
        ("last_signal_ts_ms", "INTEGER"),
        ("updated_ts_ms", "INTEGER NOT NULL DEFAULT 0"),
        ("meta_json", "TEXT"),
        ("status", "TEXT"),
        ("created_ts", "INTEGER"),
        ("updated_ts", "INTEGER"),
        ("metadata_json", "TEXT"),
    ):
        _alter_add_column_if_missing(con, "model_marketplace_scores", column_name, ddl)
    con.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_model_marketplace_scores_key
          ON model_marketplace_scores(model_id, model_name, symbol, horizon_s, regime)
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_model_marketplace_stage_score
          ON model_marketplace_scores(stage, score DESC, updated_ts_ms DESC)
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_model_marketplace_symbol_horizon
          ON model_marketplace_scores(symbol, horizon_s, score DESC)
        """
    )


def _ensure_execution_analytics_schema(con: sqlite3.Connection) -> None:
    """Execution analytics tables are created from engine.execution.execution_ledger.SCHEMA."""
    del con


def _ensure_kill_switch_schema(con: sqlite3.Connection) -> None:
    """
    CREATE TABLE IF NOT EXISTS kill_switch_state (...);
    CREATE TABLE IF NOT EXISTS kill_switch_audit (...);
    """
    _create_table(
        con,
        "kill_switch_state",
        ("scope", "key", "enabled", "reason", "actor", "meta_json", "created_ts_ms", "updated_ts_ms"),
        (("scope", "key"),),
    )
    _create_table(
        con,
        "kill_switch_audit",
        ("id", "ts_ms", "action", "scope", "key", "enabled", "actor", "reason", "meta_json", "prev_hash", "row_hash"),
    )
    con.execute("CREATE INDEX IF NOT EXISTS idx_kill_switch_scope_enabled ON kill_switch_state(scope, enabled)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_kill_switch_audit_ts ON kill_switch_audit(ts_ms)")


def _ensure_trade_attribution_ledger_schema(con: sqlite3.Connection) -> None:
    """
    CREATE TABLE IF NOT EXISTS trade_attribution_ledger (...);
    """
    _create_table(
        con,
        "trade_attribution_ledger",
        (
            "id",
            "ts_ms",
            "source_alert_id",
            "model_id",
            "symbol",
            "signal_json",
            "model_json",
            "regime_vector_json",
            "execution_policy_json",
            "suppression_reason",
            "pnl",
            "fees",
            "slippage_bps",
            "decision_json",
            "created_ts_ms",
            "prev_hash",
            "row_hash",
        ),
    )
    con.execute("CREATE INDEX IF NOT EXISTS idx_trade_attr_alert ON trade_attribution_ledger(source_alert_id)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_trade_attr_model_ts ON trade_attribution_ledger(model_id, ts_ms)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_trade_attr_symbol_ts ON trade_attribution_ledger(symbol, ts_ms)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_trade_attr_ts ON trade_attribution_ledger(ts_ms)")


def _ensure_options_chain_schema(con: sqlite3.Connection) -> None:
    """
    CREATE TABLE IF NOT EXISTS options_chain (...);
    """
    _create_table(
        con,
        "options_chain",
        (
            "id",
            "ts_ms",
            "symbol",
            "expiry",
            "strike",
            "call_put",
            "iv",
            "open_interest",
            "volume",
            "source",
            "payload_json",
        ),
        (("symbol", "expiry", "strike", "call_put", "ts_ms"),),
    )


def _ensure_options_chain_v2_schema(con: sqlite3.Connection) -> None:
    """
    CREATE TABLE IF NOT EXISTS options_chain_v2 (...);
    """
    _create_table(
        con,
        "options_chain_v2",
        (
            "id",
            "ts_ms",
            "underlying",
            "contract",
            "expiration",
            "contract_type",
            "strike",
            "iv",
            "open_interest",
            "volume",
            "bid",
            "ask",
            "delta",
            "gamma",
            "theta",
            "vega",
            "source",
            "payload_json",
        ),
        (("contract", "ts_ms"),),
    )


def _ensure_insider_transactions_schema(con: sqlite3.Connection) -> None:
    """CREATE TABLE IF NOT EXISTS insider_transactions (...);"""
    _create_table(con, "insider_transactions", ("id", *_INSIDER_TRANSACTION_COLUMNS), (("source_transaction_id",),))


def _ensure_congressional_trades_schema(con: sqlite3.Connection) -> None:
    """CREATE TABLE IF NOT EXISTS congressional_trades (...);"""
    _create_table(con, "congressional_trades", ("id", *_CONGRESSIONAL_TRADE_COLUMNS), (("source_trade_id",),))


def _ensure_prices_schema(con: sqlite3.Connection) -> None:
    from engine.runtime.storage_live_ingestion_schema import ensure_prices_schema

    ensure_prices_schema(con, warn_nonfatal=_warn_nonfatal)


def _ensure_price_quotes_schema(con: sqlite3.Connection) -> None:
    from engine.runtime.storage_live_ingestion_schema import ensure_price_quotes_schema

    ensure_price_quotes_schema(con, warn_nonfatal=_warn_nonfatal)


def _ensure_price_quotes_raw_schema(con: sqlite3.Connection) -> None:
    from engine.runtime.storage_live_ingestion_schema import ensure_price_quotes_raw_schema

    ensure_price_quotes_raw_schema(con, warn_nonfatal=_warn_nonfatal)


def _ensure_price_provider_health_schema(con: sqlite3.Connection) -> None:
    from engine.runtime.storage_live_ingestion_schema import ensure_price_provider_health_schema

    ensure_price_provider_health_schema(con, warn_nonfatal=_warn_nonfatal)


def _ensure_ingestion_pipeline_health_schema(con: sqlite3.Connection) -> None:
    from engine.runtime.storage_live_ingestion_schema import ensure_ingestion_pipeline_health_schema

    ensure_ingestion_pipeline_health_schema(con, warn_nonfatal=_warn_nonfatal)


def _ensure_price_feed_lock_schema(con: sqlite3.Connection) -> None:
    from engine.runtime.storage_live_ingestion_schema import ensure_price_feed_lock_schema

    ensure_price_feed_lock_schema(con, warn_nonfatal=_warn_nonfatal)


def _ensure_options_symbol_ingestion_state_schema(con: sqlite3.Connection) -> None:
    from engine.runtime.storage_live_ingestion_schema import ensure_options_symbol_ingestion_state_schema

    ensure_options_symbol_ingestion_state_schema(con, warn_nonfatal=_warn_nonfatal)


_EXACT_TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    "job_locks": ("job_name", "owner", "pid", "acquired_ts_ms", "heartbeat_ts_ms", "expires_ms"),
    "job_heartbeats": ("job_name", "owner", "pid", "ts_ms", "extra_json"),
    "job_checkpoints": ("job_name", "last_event_id", "last_event_ts_ms", "updated_ts_ms"),
    "prices": ("ts_ms", "symbol", "price", "px", "source"),
    "price_quotes": (
        "ts_ms",
        "symbol",
        "last",
        "bid",
        "ask",
        "spread",
        "volume",
        "source",
        "last_trade_ts_ms",
        "last_quote_ts_ms",
        "last_update_ts_ms",
    ),
    "price_quotes_raw": (
        "ts_ms",
        "symbol",
        "provider",
        "event_key",
        "event_type",
        "event_ts_ms",
        "last",
        "bid",
        "ask",
        "spread",
        "volume",
        "trade_ts_ms",
        "quote_ts_ms",
        "ingest_ts_ms",
        "source",
    ),
    "price_provider_health": (
        "ts_ms",
        "provider",
        "ok",
        "latency_ms",
        "n_symbols",
        "error",
        "last_success_ts_ms",
        "error_count",
    ),
    "ingestion_pipeline_health": (
        "ts_ms",
        "pipeline",
        "ok",
        "latency_ms",
        "raw_rows",
        "event_rows",
        "last_ingested_ts_ms",
        "error",
        "meta_json",
    ),
    "price_feed_lock": ("id", "owner", "pid", "ts_ms"),
    "options_symbol_ingestion_state": (
        "symbol",
        "provider",
        "consecutive_failures",
        "total_failures",
        "last_failure_ts_ms",
        "last_failure_error",
        "last_success_ts_ms",
        "last_fresh_snapshot_ts_ms",
        "last_cached_snapshot_ts_ms",
        "last_fallback_ts_ms",
        "last_row_count",
        "disabled_until_ts_ms",
        "updated_ts_ms",
    ),
    "predictions": (
        "id",
        "ts_ms",
        "event_id",
        "symbol",
        "horizon_s",
        "predicted_z",
        "confidence",
        "confidence_raw",
        "prediction_strength",
        "model_name",
        "model_id",
        "model_version",
        "regime_time_ms",
        "volatility_regime",
        "trend_regime",
        "liquidity_regime",
    ),
    "alerts": (
        "id",
        "ts_ms",
        "event_id",
        "prediction_id",
        "event_title",
        "symbol",
        "horizon_s",
        "expected_z",
        "confidence",
        "severity",
        "rule_id",
        "explain_json",
        "dedupe_key",
        "title",
        "message",
        "source",
        "status",
        "detail_json",
        "updated_ts_ms",
        "model_name",
        "model_id",
        "model_version",
        "portfolio_first_seen_ts_ms",
        "portfolio_last_seen_ts_ms",
        "portfolio_consumed_ts_ms",
        "portfolio_expired_ts_ms",
        "portfolio_status",
    ),
    "decision_log": (
        "id",
        "ts_ms",
        "event_id",
        "symbol",
        "horizon_s",
        "predicted_z",
        "confidence",
        "model_name",
        "model_kind",
        "model_ts_ms",
        "model_version",
        "features_hash",
        "feature_set_tag",
        "features_json",
        "explain_json",
        "extra_json",
        "components_json",
        "component_vector",
        "prev_hash",
        "row_hash",
    ),
    "portfolio_state": (
        "model_id",
        "symbol",
        "side",
        "weight",
        "opened_ts_ms",
        "updated_ts_ms",
        "source_alert_id",
        "explain_json",
    ),
    "portfolio_orders": (
        "id",
        "ts_ms",
        "model_id",
        "symbol",
        "action",
        "from_side",
        "to_side",
        "from_weight",
        "to_weight",
        "delta_weight",
        "source_alert_id",
        "prediction_id",
        "explain_json",
    ),
    "execution_orders": (
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
    ),
    "execution_fills": (
        "id",
        "client_order_id",
        "fill_id",
        "broker",
        "model_id",
        "model_version",
        "symbol",
        "portfolio_orders_id",
        "source_alert_id",
        "prediction_id",
        "ts_ms",
        "submit_ts_ms",
        "fill_ts_ms",
        "fill_qty",
        "fill_px",
        "expected_px",
        "mid_px",
        "bid_px",
        "ask_px",
        "spread_bps",
        "slippage_bps",
        "fill_latency_ms",
        "fees",
        "commission",
        "liquidity",
        "raw_json",
        "extra_json",
    ),
    "pnl_attribution": (
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
    ),
}

_EXACT_TABLE_PK: dict[str, dict[str, int]] = {
    "job_locks": {"job_name": 1},
    "job_heartbeats": {"job_name": 1},
    "job_checkpoints": {"job_name": 1},
    "prices": {"symbol": 1, "ts_ms": 2},
    "price_quotes": {"symbol": 1, "ts_ms": 2},
    "price_quotes_raw": {"symbol": 1, "provider": 2, "event_key": 3},
    "price_provider_health": {"provider": 1, "ts_ms": 2},
    "ingestion_pipeline_health": {"pipeline": 1, "ts_ms": 2},
    "price_feed_lock": {"id": 1},
    "options_symbol_ingestion_state": {"symbol": 1},
    "predictions": {"id": 1},
    "alerts": {"id": 1},
    "decision_log": {"id": 1},
    "portfolio_state": {"model_id": 1, "symbol": 2},
    "portfolio_orders": {"id": 1},
    "execution_orders": {"client_order_id": 1},
    "execution_fills": {"id": 1},
    "pnl_attribution": {"ts_ms": 1, "source_alert_id": 2, "model_id": 3, "symbol": 4},
}

_FK_TOKENS: dict[str, tuple[str, ...]] = {
    "alerts": ("FOREIGN KEY(prediction_id) REFERENCES predictions(id) ON DELETE SET NULL",),
    "portfolio_orders": (
        "FOREIGN KEY(prediction_id) REFERENCES predictions(id) ON DELETE SET NULL",
        "FOREIGN KEY(source_alert_id, prediction_id) REFERENCES alerts(id, prediction_id) ON DELETE SET NULL",
    ),
    "execution_orders": (
        "FOREIGN KEY(portfolio_orders_id) REFERENCES portfolio_orders(id) ON DELETE SET NULL",
        "FOREIGN KEY(prediction_id) REFERENCES predictions(id) ON DELETE SET NULL",
        "FOREIGN KEY(source_alert_id, prediction_id) REFERENCES alerts(id, prediction_id) ON DELETE SET NULL",
        "FOREIGN KEY(portfolio_orders_id, source_alert_id, prediction_id) REFERENCES portfolio_orders(id, source_alert_id, prediction_id) ON DELETE SET NULL",
    ),
    "execution_fills": (
        "FOREIGN KEY(portfolio_orders_id) REFERENCES portfolio_orders(id) ON DELETE SET NULL",
        "FOREIGN KEY(prediction_id) REFERENCES predictions(id) ON DELETE SET NULL",
        "FOREIGN KEY(source_alert_id, prediction_id) REFERENCES alerts(id, prediction_id) ON DELETE SET NULL",
        "FOREIGN KEY(portfolio_orders_id, source_alert_id, prediction_id) REFERENCES portfolio_orders(id, source_alert_id, prediction_id) ON DELETE SET NULL",
    ),
}


def _contract_create_sql(table: str) -> str:
    ddl: dict[str, str] = {
        "job_locks": """
        CREATE TABLE IF NOT EXISTS job_locks (
          job_name TEXT PRIMARY KEY,
          owner TEXT NOT NULL,
          pid INTEGER NOT NULL,
          acquired_ts_ms INTEGER NOT NULL,
          heartbeat_ts_ms INTEGER NOT NULL,
          expires_ms INTEGER
        )
        """,
        "job_heartbeats": """
        CREATE TABLE IF NOT EXISTS job_heartbeats (
          job_name TEXT PRIMARY KEY,
          owner TEXT NOT NULL,
          pid INTEGER NOT NULL,
          ts_ms INTEGER NOT NULL,
          extra_json TEXT
        )
        """,
        "job_checkpoints": """
        CREATE TABLE IF NOT EXISTS job_checkpoints (
          job_name TEXT PRIMARY KEY,
          last_event_id INTEGER,
          last_event_ts_ms INTEGER,
          updated_ts_ms INTEGER NOT NULL
        )
        """,
        "predictions": """
        CREATE TABLE IF NOT EXISTS predictions (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER NOT NULL,
          event_id INTEGER NOT NULL,
          symbol TEXT NOT NULL,
          horizon_s INTEGER NOT NULL,
          predicted_z REAL NOT NULL,
          confidence REAL NOT NULL,
          confidence_raw REAL,
          prediction_strength REAL,
          model_name TEXT,
          model_id TEXT,
          model_version TEXT,
          regime_time_ms INTEGER,
          volatility_regime TEXT NOT NULL DEFAULT 'unknown',
          trend_regime TEXT NOT NULL DEFAULT 'unknown',
          liquidity_regime TEXT NOT NULL DEFAULT 'unknown'
        )
        """,
        "alerts": """
        CREATE TABLE IF NOT EXISTS alerts (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER NOT NULL,
          event_id INTEGER,
          prediction_id INTEGER,
          event_title TEXT NOT NULL,
          symbol TEXT NOT NULL,
          horizon_s INTEGER NOT NULL,
          expected_z REAL NOT NULL,
          confidence REAL NOT NULL,
          severity TEXT NOT NULL,
          rule_id TEXT NOT NULL,
          explain_json TEXT,
          dedupe_key TEXT NOT NULL,
          title TEXT,
          message TEXT,
          source TEXT,
          status TEXT NOT NULL DEFAULT 'open',
          detail_json TEXT,
          updated_ts_ms INTEGER DEFAULT 0,
          model_name TEXT,
          model_id TEXT,
          model_version TEXT,
          portfolio_first_seen_ts_ms INTEGER NOT NULL DEFAULT 0,
          portfolio_last_seen_ts_ms INTEGER NOT NULL DEFAULT 0,
          portfolio_consumed_ts_ms INTEGER NOT NULL DEFAULT 0,
          portfolio_expired_ts_ms INTEGER NOT NULL DEFAULT 0,
          portfolio_status TEXT NOT NULL DEFAULT 'new',
          FOREIGN KEY(prediction_id) REFERENCES predictions(id) ON DELETE SET NULL
        )
        """,
        "decision_log": """
        CREATE TABLE IF NOT EXISTS decision_log (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER NOT NULL,
          event_id INTEGER NOT NULL,
          symbol TEXT NOT NULL,
          horizon_s INTEGER NOT NULL,
          predicted_z REAL NOT NULL,
          confidence REAL NOT NULL,
          model_name TEXT NOT NULL,
          model_kind TEXT,
          model_ts_ms INTEGER,
          model_version TEXT,
          features_hash TEXT,
          feature_set_tag TEXT,
          features_json TEXT,
          explain_json TEXT,
          extra_json TEXT,
          components_json TEXT,
          component_vector TEXT,
          prev_hash BLOB,
          row_hash BLOB
        )
        """,
        "portfolio_state": """
        CREATE TABLE IF NOT EXISTS portfolio_state (
          model_id TEXT NOT NULL DEFAULT 'baseline',
          symbol TEXT NOT NULL,
          side TEXT NOT NULL,
          weight REAL NOT NULL,
          opened_ts_ms INTEGER NOT NULL,
          updated_ts_ms INTEGER NOT NULL,
          source_alert_id INTEGER,
          explain_json TEXT,
          PRIMARY KEY(model_id, symbol)
        )
        """,
        "portfolio_orders": """
        CREATE TABLE IF NOT EXISTS portfolio_orders (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER NOT NULL,
          model_id TEXT NOT NULL DEFAULT 'baseline',
          symbol TEXT NOT NULL,
          action TEXT NOT NULL,
          from_side TEXT NOT NULL,
          to_side TEXT NOT NULL,
          from_weight REAL NOT NULL,
          to_weight REAL NOT NULL,
          delta_weight REAL NOT NULL,
          source_alert_id INTEGER,
          prediction_id INTEGER,
          explain_json TEXT,
          FOREIGN KEY(prediction_id) REFERENCES predictions(id) ON DELETE SET NULL,
          FOREIGN KEY(source_alert_id, prediction_id) REFERENCES alerts(id, prediction_id) ON DELETE SET NULL
        )
        """,
        "execution_orders": """
        CREATE TABLE IF NOT EXISTS execution_orders (
          client_order_id TEXT PRIMARY KEY,
          order_uid TEXT,
          idempotency_status TEXT,
          broker TEXT NOT NULL,
          portfolio_orders_id INTEGER,
          source_alert_id INTEGER,
          prediction_id INTEGER,
          model_id TEXT NOT NULL DEFAULT 'baseline',
          model_version TEXT,
          symbol TEXT NOT NULL,
          qty REAL NOT NULL,
          submit_ts_ms INTEGER NOT NULL,
          ref_px REAL,
          expected_px REAL,
          mid_px REAL,
          bid_px REAL,
          ask_px REAL,
          spread_bps REAL,
          broker_order_id TEXT,
          status TEXT NOT NULL DEFAULT 'submitted',
          extra_json TEXT,
          FOREIGN KEY(portfolio_orders_id) REFERENCES portfolio_orders(id) ON DELETE SET NULL,
          FOREIGN KEY(prediction_id) REFERENCES predictions(id) ON DELETE SET NULL,
          FOREIGN KEY(source_alert_id, prediction_id) REFERENCES alerts(id, prediction_id) ON DELETE SET NULL,
          FOREIGN KEY(portfolio_orders_id, source_alert_id, prediction_id) REFERENCES portfolio_orders(id, source_alert_id, prediction_id) ON DELETE SET NULL
        )
        """,
        "execution_fills": """
        CREATE TABLE IF NOT EXISTS execution_fills (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          client_order_id TEXT NOT NULL,
          fill_id TEXT,
          broker TEXT,
          model_id TEXT NOT NULL DEFAULT 'baseline',
          model_version TEXT,
          symbol TEXT,
          portfolio_orders_id INTEGER,
          source_alert_id INTEGER,
          prediction_id INTEGER,
          ts_ms INTEGER,
          submit_ts_ms INTEGER,
          fill_ts_ms INTEGER NOT NULL,
          fill_qty REAL NOT NULL,
          fill_px REAL NOT NULL,
          expected_px REAL,
          mid_px REAL,
          bid_px REAL,
          ask_px REAL,
          spread_bps REAL,
          slippage_bps REAL,
          fill_latency_ms INTEGER,
          fees REAL,
          commission REAL,
          liquidity TEXT,
          raw_json TEXT,
          extra_json TEXT,
          FOREIGN KEY(portfolio_orders_id) REFERENCES portfolio_orders(id) ON DELETE SET NULL,
          FOREIGN KEY(prediction_id) REFERENCES predictions(id) ON DELETE SET NULL,
          FOREIGN KEY(source_alert_id, prediction_id) REFERENCES alerts(id, prediction_id) ON DELETE SET NULL,
          FOREIGN KEY(portfolio_orders_id, source_alert_id, prediction_id) REFERENCES portfolio_orders(id, source_alert_id, prediction_id) ON DELETE SET NULL
        )
        """,
        "pnl_attribution": """
        CREATE TABLE IF NOT EXISTS pnl_attribution (
          ts_ms INTEGER NOT NULL,
          source_alert_id INTEGER NOT NULL,
          prediction_id INTEGER,
          model_id TEXT NOT NULL DEFAULT 'baseline',
          model_version TEXT,
          symbol TEXT NOT NULL,
          pnl REAL NOT NULL,
          fees REAL NOT NULL,
          slippage_bps REAL,
          position_size REAL,
          avg_price REAL,
          realized_pnl REAL,
          unrealized_pnl REAL,
          extra_json TEXT,
          PRIMARY KEY(ts_ms, source_alert_id, model_id, symbol)
        )
        """,
    }
    return ddl[table]


def _legacy_copy_expressions(table: str, legacy_table: str) -> dict[str, str]:
    # The actual legacy column set is looked up by _contract_rebuild_table; this
    # function only centralizes table-specific defaults.
    defaults: dict[str, dict[str, str]] = {
        "prices": {
            "px": "COALESCE(px, price)",
            "source": "source",
        },
        "price_quotes_raw": {
            "event_key": "'legacy:' || symbol || ':' || provider || ':' || ts_ms",
            "event_type": "'legacy'",
            "event_ts_ms": "ts_ms",
            "trade_ts_ms": "ts_ms",
            "quote_ts_ms": "ts_ms",
            "ingest_ts_ms": "ts_ms",
            "source": "provider",
        },
        "price_provider_health": {"error_count": "0"},
        "ingestion_pipeline_health": {"latency_ms": "NULL", "raw_rows": "0", "event_rows": "0"},
        "price_feed_lock": {"owner": "''", "pid": "0", "ts_ms": "0"},
        "options_symbol_ingestion_state": {
            "provider": "''",
            "consecutive_failures": "0",
            "total_failures": "0",
            "last_row_count": "0",
            "disabled_until_ts_ms": "0",
            "updated_ts_ms": "0",
        },
        "job_locks": {
            "owner": "''",
            "pid": "0",
            "acquired_ts_ms": "0",
            "heartbeat_ts_ms": "0",
        },
        "job_heartbeats": {
            "owner": "''",
            "pid": "0",
            "ts_ms": "0",
        },
        "job_checkpoints": {
            "last_event_id": "0",
            "last_event_ts_ms": "0",
            "updated_ts_ms": "0",
        },
        "predictions": {
            "predicted_z": "COALESCE(predicted_z, prediction, 0.0)",
            "confidence": "COALESCE(confidence, 0.0)",
            "confidence_raw": "confidence",
            "prediction_strength": "prediction",
            "volatility_regime": "COALESCE(volatility_regime, 'unknown')",
            "trend_regime": "COALESCE(trend_regime, 'unknown')",
            "liquidity_regime": "COALESCE(liquidity_regime, 'unknown')",
        },
        "alerts": {
            "prediction_id": "NULL",
            "event_title": "COALESCE(event_title, title, message, '')",
            "symbol": "COALESCE(symbol, '')",
            "horizon_s": "COALESCE(horizon_s, 0)",
            "expected_z": "COALESCE(expected_z, 0.0)",
            "confidence": "COALESCE(confidence, 0.0)",
            "severity": "COALESCE(severity, '')",
            "rule_id": "COALESCE(rule_id, '')",
            "dedupe_key": "COALESCE(NULLIF(dedupe_key, ''), 'legacy:' || id)",
            "status": "COALESCE(NULLIF(status, ''), 'open')",
            "updated_ts_ms": "COALESCE(updated_ts_ms, 0)",
            "portfolio_first_seen_ts_ms": "0",
            "portfolio_last_seen_ts_ms": "0",
            "portfolio_consumed_ts_ms": "0",
            "portfolio_expired_ts_ms": "0",
            "portfolio_status": "'new'",
        },
        "decision_log": {
            "event_id": "COALESCE(event_id, 0)",
            "symbol": "COALESCE(symbol, '')",
            "horizon_s": "COALESCE(horizon_s, 0)",
            "predicted_z": "COALESCE(predicted_z, 0.0)",
            "confidence": "COALESCE(confidence, 0.0)",
            "model_name": "COALESCE(model_name, '')",
        },
        "portfolio_state": {
            "model_id": "COALESCE(NULLIF(model_id, ''), 'baseline')",
            "side": "COALESCE(side, 'FLAT')",
            "weight": "COALESCE(weight, 0.0)",
            "opened_ts_ms": "COALESCE(opened_ts_ms, updated_ts_ms, ts_ms, 0)",
            "updated_ts_ms": "COALESCE(updated_ts_ms, ts_ms, 0)",
        },
        "portfolio_orders": {
            "model_id": "COALESCE(NULLIF(model_id, ''), 'baseline')",
            "action": "COALESCE(action, '')",
            "from_side": "COALESCE(from_side, '')",
            "to_side": "COALESCE(to_side, '')",
            "from_weight": "COALESCE(from_weight, 0.0)",
            "to_weight": "COALESCE(to_weight, 0.0)",
            "delta_weight": "COALESCE(delta_weight, 0.0)",
        },
        "execution_orders": {
            "order_uid": "NULL",
            "idempotency_status": "NULL",
            "portfolio_orders_id": "NULL",
            "prediction_id": "NULL",
            "model_id": "COALESCE(NULLIF(model_id, ''), 'baseline')",
            "model_version": "NULL",
            "expected_px": "NULL",
            "mid_px": "NULL",
            "bid_px": "NULL",
            "ask_px": "NULL",
            "spread_bps": "NULL",
            "status": "COALESCE(NULLIF(status, ''), 'submitted')",
        },
        "execution_fills": {
            "model_id": "COALESCE(NULLIF(model_id, ''), 'baseline')",
            "model_version": "NULL",
            "portfolio_orders_id": "NULL",
            "source_alert_id": "NULL",
            "prediction_id": "NULL",
            "ts_ms": "COALESCE(ts_ms, fill_ts_ms, 0)",
            "submit_ts_ms": "NULL",
            "expected_px": "NULL",
            "mid_px": "NULL",
            "bid_px": "NULL",
            "ask_px": "NULL",
            "spread_bps": "NULL",
            "slippage_bps": "NULL",
            "fill_latency_ms": "NULL",
            "fees": "COALESCE(fees, 0.0)",
            "commission": "COALESCE(commission, 0.0)",
        },
        "pnl_attribution": {
            "source_alert_id": "COALESCE(source_alert_id, 0)",
            "prediction_id": "NULL",
            "model_id": "COALESCE(NULLIF(model_id, ''), 'baseline')",
            "model_version": "NULL",
            "pnl": "COALESCE(pnl, net_pnl, realized_pnl, unrealized_pnl, 0.0)",
            "fees": "COALESCE(fees, 0.0)",
            "slippage_bps": "NULL",
            "position_size": "NULL",
            "avg_price": "NULL",
            "realized_pnl": "realized_pnl",
            "unrealized_pnl": "unrealized_pnl",
            "extra_json": "NULL",
        },
    }
    del legacy_table
    return defaults.get(str(table), {})


def _contract_rebuild_table(con: sqlite3.Connection, table: str) -> None:
    legacy_table = _next_legacy_table_name(con, table)
    _connection_execute_raw(con, f"ALTER TABLE {_quote(table)} RENAME TO {_quote(legacy_table)}")
    _connection_execute_raw(con, _contract_create_sql(table))
    legacy_columns = _table_columns(con, legacy_table)
    defaults = _legacy_copy_expressions(table, legacy_table)
    if table == "prices":
        if "price" in legacy_columns and "px" in legacy_columns:
            defaults["px"] = "COALESCE(px, price)"
        elif "price" in legacy_columns:
            defaults["px"] = "price"
        elif "px" in legacy_columns:
            defaults["px"] = "px"
        else:
            defaults["px"] = "NULL"
        if "price" not in legacy_columns and "px" in legacy_columns:
            defaults["price"] = "px"
        if "source" not in legacy_columns:
            defaults["source"] = "NULL"
    if table == "predictions":
        defaults["predicted_z"] = (
            "predicted_z"
            if "predicted_z" in legacy_columns
            else ("prediction" if "prediction" in legacy_columns else "0.0")
        )
        defaults["confidence"] = "confidence" if "confidence" in legacy_columns else "0.0"
        defaults["confidence_raw"] = "confidence" if "confidence" in legacy_columns else "NULL"
        defaults["prediction_strength"] = "prediction" if "prediction" in legacy_columns else "NULL"
        for regime_col in ("volatility_regime", "trend_regime", "liquidity_regime"):
            defaults[regime_col] = (
                f"COALESCE({regime_col}, 'unknown')" if regime_col in legacy_columns else "'unknown'"
            )
    if table in {"alerts", "portfolio_orders", "execution_orders", "execution_fills", "pnl_attribution"}:
        if "model_id" not in legacy_columns and "model_id" in _EXACT_TABLE_COLUMNS[table]:
            defaults["model_id"] = "'baseline'"
        if "model_version" not in legacy_columns and "model_version" in _EXACT_TABLE_COLUMNS[table]:
            defaults["model_version"] = "NULL"
        if "prediction_id" not in legacy_columns and "prediction_id" in _EXACT_TABLE_COLUMNS[table]:
            defaults["prediction_id"] = "NULL"
    if table == "alerts":
        title_sources = [_quote(col) for col in ("event_title", "title", "message") if col in legacy_columns]
        title_terms = [*title_sources, "''"]
        defaults["event_title"] = f"COALESCE({', '.join(title_terms)})"
        if "dedupe_key" in legacy_columns:
            defaults["dedupe_key"] = "COALESCE(NULLIF(dedupe_key, ''), 'legacy:' || CAST(rowid AS TEXT))"
        else:
            defaults["dedupe_key"] = "'legacy:' || CAST(rowid AS TEXT)"
    if table == "execution_orders":
        for nullable_col in ("order_uid", "idempotency_status", "portfolio_orders_id", "expected_px", "mid_px", "bid_px", "ask_px", "spread_bps"):
            if nullable_col not in legacy_columns:
                defaults[nullable_col] = "NULL"
        if "status" not in legacy_columns:
            defaults["status"] = "'submitted'"
    if table == "execution_fills":
        for nullable_col in (
            "portfolio_orders_id",
            "source_alert_id",
            "ts_ms",
            "submit_ts_ms",
            "expected_px",
            "mid_px",
            "bid_px",
            "ask_px",
            "spread_bps",
            "slippage_bps",
            "fill_latency_ms",
            "liquidity",
            "raw_json",
            "extra_json",
        ):
            if nullable_col not in legacy_columns:
                defaults[nullable_col] = "NULL"
        if "fees" not in legacy_columns:
            defaults["fees"] = "0.0"
        if "commission" not in legacy_columns:
            defaults["commission"] = "NULL"
    if table == "pnl_attribution":
        if "pnl" in legacy_columns:
            defaults["pnl"] = "pnl"
        elif "net_pnl" in legacy_columns:
            defaults["pnl"] = "net_pnl"
        elif "realized_pnl" in legacy_columns and "unrealized_pnl" in legacy_columns:
            defaults["pnl"] = "COALESCE(realized_pnl, 0.0) + COALESCE(unrealized_pnl, 0.0)"
        else:
            defaults["pnl"] = "0.0"
        if "fees" not in legacy_columns:
            defaults["fees"] = "0.0"
        if "source_alert_id" not in legacy_columns:
            defaults["source_alert_id"] = "0"
    expressions: dict[str, str] = {}
    for column in _EXACT_TABLE_COLUMNS[table]:
        if column in legacy_columns:
            expressions[column] = _quote(column)
        elif column in defaults:
            expressions[column] = defaults[column]
        else:
            expressions[column] = "NULL"
    for column, expr in defaults.items():
        if column in _EXACT_TABLE_COLUMNS[table]:
            tokens = {token.strip('"') for token in re.findall(r'"([A-Za-z_][A-Za-z0-9_]*)"', expr)}
            bare_tokens = set(re.findall(r"\b[A-Za-z_][A-Za-z0-9_]*\b", expr))
            if not tokens and any(token in legacy_columns for token in bare_tokens):
                expressions[column] = expr
            elif not tokens:
                expressions[column] = expr
    for column, expr in list(expressions.items()):
        bare = set(re.findall(r"\b[A-Za-z_][A-Za-z0-9_]*\b", expr))
        sql_words = {
            "COALESCE",
            "NULLIF",
            "NULL",
            "CASE",
            "WHEN",
            "THEN",
            "ELSE",
            "END",
            "CAST",
            "AS",
            "TEXT",
        }
        unknown = {
            token
            for token in bare
            if token.upper() not in sql_words
            and not token.isdigit()
            and token not in legacy_columns
            and token not in {"legacy", "rowid", "unknown", "baseline", "open", "new", "submitted", "FLAT"}
        }
        if unknown:
            expressions[column] = _quote(column) if column in legacy_columns else "NULL"
    _copy_legacy_rows(
        con,
        legacy_table=legacy_table,
        target_table=table,
        columns=_EXACT_TABLE_COLUMNS[table],
        expressions=expressions,
    )


def _ensure_contract_tables(con: sqlite3.Connection) -> None:
    from engine.runtime.storage_live_ingestion_schema import ensure_live_ingestion_schema

    ensure_live_ingestion_schema(con, warn_nonfatal=_warn_nonfatal)
    for table in (
        "job_locks",
        "job_heartbeats",
        "job_checkpoints",
        "predictions",
        "decision_log",
        "portfolio_state",
        "pnl_attribution",
    ):
        if _needs_exact_rebuild(
            con,
            table,
            _EXACT_TABLE_COLUMNS[table],
            pk=_EXACT_TABLE_PK.get(table),
        ):
            _contract_rebuild_table(con, table)

    for table in (
        "job_locks",
        "job_heartbeats",
        "job_checkpoints",
        "predictions",
    ):
        _connection_execute_raw(con, _contract_create_sql(table))

    for table in ("alerts", "portfolio_orders", "execution_orders", "execution_fills"):
        if _needs_exact_rebuild(
            con,
            table,
            _EXACT_TABLE_COLUMNS[table],
            pk=_EXACT_TABLE_PK.get(table),
            required_sql_tokens=_FK_TOKENS.get(table, ()),
        ):
            _contract_rebuild_table(con, table)
        _connection_execute_raw(con, _contract_create_sql(table))
        if table == "alerts":
            _connection_execute_raw(
                con,
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_alerts_id_prediction_lineage ON alerts(id, prediction_id)",
            )
        elif table == "portfolio_orders":
            _connection_execute_raw(
                con,
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_portfolio_orders_id_source_prediction_lineage ON portfolio_orders(id, source_alert_id, prediction_id)",
            )

    for table in (
        "job_locks",
        "job_heartbeats",
        "job_checkpoints",
        "predictions",
        "decision_log",
        "portfolio_state",
        "pnl_attribution",
    ):
        _connection_execute_raw(con, _contract_create_sql(table))


def _ensure_contract_indexes(con: sqlite3.Connection) -> None:
    con.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_event_log_ts ON event_log(ts_ms);
        CREATE INDEX IF NOT EXISTS idx_event_log_type_ts ON event_log(event_type, ts_ms);
        CREATE INDEX IF NOT EXISTS idx_event_log_entity ON event_log(entity_type, entity_id, ts_ms);
        CREATE INDEX IF NOT EXISTS idx_event_log_corr ON event_log(correlation_id, ts_ms);

        CREATE INDEX IF NOT EXISTS idx_decision_log_ts ON decision_log(ts_ms);
        CREATE INDEX IF NOT EXISTS idx_decision_log_symbol_ts ON decision_log(symbol, ts_ms);
        CREATE INDEX IF NOT EXISTS idx_decision_log_model_ts ON decision_log(model_name, model_ts_ms, ts_ms);

        CREATE INDEX IF NOT EXISTS idx_predictions_ts ON predictions(ts_ms);
        CREATE INDEX IF NOT EXISTS idx_predictions_symbol_ts ON predictions(symbol, ts_ms);
        CREATE INDEX IF NOT EXISTS idx_predictions_model_ts ON predictions(model_id, ts_ms);

        CREATE INDEX IF NOT EXISTS idx_alerts_ts ON alerts(ts_ms);
        CREATE INDEX IF NOT EXISTS idx_alerts_event_id ON alerts(event_id);
        CREATE INDEX IF NOT EXISTS idx_alerts_prediction_id ON alerts(prediction_id, ts_ms DESC);
        CREATE INDEX IF NOT EXISTS idx_alerts_severity_ts ON alerts(severity, ts_ms);
        CREATE INDEX IF NOT EXISTS idx_alerts_symbol_ts ON alerts(symbol, ts_ms);
        CREATE UNIQUE INDEX IF NOT EXISTS uq_alerts_id_prediction_lineage ON alerts(id, prediction_id);

        CREATE INDEX IF NOT EXISTS idx_portfolio_state_updated_ts ON portfolio_state(updated_ts_ms);
        CREATE INDEX IF NOT EXISTS idx_portfolio_orders_ts ON portfolio_orders(ts_ms);
        CREATE INDEX IF NOT EXISTS idx_portfolio_orders_symbol_ts ON portfolio_orders(symbol, ts_ms);
        CREATE INDEX IF NOT EXISTS idx_portfolio_orders_model_ts ON portfolio_orders(model_id, ts_ms);
        CREATE INDEX IF NOT EXISTS idx_portfolio_orders_source_alert_ts ON portfolio_orders(source_alert_id, ts_ms);
        CREATE INDEX IF NOT EXISTS idx_portfolio_orders_prediction_ts ON portfolio_orders(prediction_id, ts_ms);
        CREATE INDEX IF NOT EXISTS idx_portfolio_orders_source_alert_prediction_ts ON portfolio_orders(source_alert_id, prediction_id, ts_ms);
        CREATE UNIQUE INDEX IF NOT EXISTS uq_portfolio_orders_id_source_prediction_lineage ON portfolio_orders(id, source_alert_id, prediction_id);

        CREATE INDEX IF NOT EXISTS idx_execution_orders_submit_ts ON execution_orders(submit_ts_ms);
        CREATE INDEX IF NOT EXISTS idx_execution_orders_source_alert ON execution_orders(source_alert_id);
        CREATE INDEX IF NOT EXISTS idx_execution_orders_portfolio_order_submit_ts ON execution_orders(portfolio_orders_id, submit_ts_ms);
        CREATE INDEX IF NOT EXISTS idx_execution_orders_prediction_submit_ts ON execution_orders(prediction_id, submit_ts_ms);
        CREATE INDEX IF NOT EXISTS idx_execution_orders_source_alert_prediction_submit_ts ON execution_orders(source_alert_id, prediction_id, submit_ts_ms);
        CREATE INDEX IF NOT EXISTS idx_execution_orders_broker_order_id ON execution_orders(broker_order_id);
        CREATE INDEX IF NOT EXISTS idx_execution_orders_model_submit_ts ON execution_orders(model_id, submit_ts_ms);
        CREATE INDEX IF NOT EXISTS idx_execution_orders_symbol_submit_ts ON execution_orders(symbol, submit_ts_ms);
        CREATE INDEX IF NOT EXISTS idx_execution_orders_order_uid ON execution_orders(order_uid);

        CREATE INDEX IF NOT EXISTS idx_execution_fills_ts ON execution_fills(fill_ts_ms);
        CREATE INDEX IF NOT EXISTS idx_execution_fills_client ON execution_fills(client_order_id);
        CREATE INDEX IF NOT EXISTS idx_execution_fills_model_ts ON execution_fills(model_id, fill_ts_ms);
        CREATE INDEX IF NOT EXISTS idx_execution_fills_model_symbol_ts ON execution_fills(model_id, symbol, fill_ts_ms, id);
        CREATE INDEX IF NOT EXISTS idx_execution_fills_portfolio_order_ts ON execution_fills(portfolio_orders_id, fill_ts_ms);
        CREATE INDEX IF NOT EXISTS idx_execution_fills_source_alert_ts ON execution_fills(source_alert_id, fill_ts_ms);
        CREATE INDEX IF NOT EXISTS idx_execution_fills_prediction_ts ON execution_fills(prediction_id, fill_ts_ms);
        CREATE INDEX IF NOT EXISTS idx_execution_fills_source_alert_prediction_ts ON execution_fills(source_alert_id, prediction_id, fill_ts_ms);
        CREATE INDEX IF NOT EXISTS idx_execution_fills_symbol_ts ON execution_fills(symbol, fill_ts_ms);
        CREATE INDEX IF NOT EXISTS idx_execution_fills_fill_id ON execution_fills(fill_id);
        CREATE UNIQUE INDEX IF NOT EXISTS uq_execution_fills_client_fillid ON execution_fills(client_order_id, fill_id) WHERE fill_id IS NOT NULL;

        CREATE INDEX IF NOT EXISTS idx_pnl_attribution_prediction_ts ON pnl_attribution(prediction_id, ts_ms DESC);
        CREATE INDEX IF NOT EXISTS idx_pnl_attribution_ts ON pnl_attribution(ts_ms DESC);
        CREATE INDEX IF NOT EXISTS idx_pnl_attribution_model_ts ON pnl_attribution(model_id, ts_ms DESC);

        CREATE INDEX IF NOT EXISTS idx_temporal_model_eval_ts ON temporal_model_eval(ts_ms);
        """
    )


def _ensure_external_schema(con: sqlite3.Connection) -> None:
    for module_name in (
        "engine.execution.order_command_boundary",
        "engine.execution.broker_sim",
    ):
        module = importlib.import_module(module_name)
        con.executescript(str(getattr(module, "SCHEMA")))
    # These modules own tables we already create with stronger SQLite FK
    # constraints; CREATE TABLE IF NOT EXISTS preserves the contract tables and
    # still creates auxiliary tables/indexes from the module schema.
    for module_name in (
        "engine.execution.execution_ledger",
        "engine.strategy.portfolio",
    ):
        module = importlib.import_module(module_name)
        con.executescript(str(getattr(module, "SCHEMA")))


def _mark_schema_applied(con: sqlite3.Connection) -> None:
    now = int(time.time() * 1000)
    con.execute(
        """
        INSERT INTO schema_version(version, applied_ts_ms, status, notes)
        VALUES (?, ?, 'applied', 'sqlite_test_schema')
        ON CONFLICT(version) DO UPDATE SET
          applied_ts_ms=excluded.applied_ts_ms,
          status=excluded.status,
          notes=excluded.notes
        """,
        (SCHEMA_VERSION, now),
    )
    con.execute(
        """
        INSERT INTO runtime_meta(key, value, updated_ts_ms)
        VALUES('schema_version', ?, ?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_ts_ms=excluded.updated_ts_ms
        """,
        (str(SCHEMA_VERSION), now),
    )
    con.execute(
        """
        INSERT OR IGNORE INTO schema_migrations(version, name, applied_ts_ms)
        VALUES (?, ?, ?)
        """,
        (SCHEMA_VERSION, "test_sqlite_schema", now),
    )


def _ensure_runtime_baseline_schema(con: sqlite3.Connection) -> None:
    # storage-route-audit: allow - centralized init_db schema creation under _INIT_LOCK.
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS model_feature_snapshots (
          symbol TEXT NOT NULL,
          ts_ms INTEGER NOT NULL,
          feature_set_tag TEXT NOT NULL,
          snapshot_version INTEGER NOT NULL DEFAULT 1,
          feature_ids_json TEXT NOT NULL,
          vector_json TEXT NOT NULL,
          features_json TEXT NOT NULL,
          source_timestamps_json TEXT NOT NULL,
          availability_json TEXT NOT NULL,
          created_ts_ms INTEGER NOT NULL,
          PRIMARY KEY(symbol, ts_ms, feature_set_tag)
        );
        CREATE INDEX IF NOT EXISTS idx_model_feature_snapshots_symbol_ts
          ON model_feature_snapshots(symbol, ts_ms);
        CREATE INDEX IF NOT EXISTS idx_model_feature_snapshots_ts
          ON model_feature_snapshots(ts_ms);
        CREATE INDEX IF NOT EXISTS idx_model_feature_snapshots_symbol_feature_set_ts_desc
          ON model_feature_snapshots(symbol, feature_set_tag, ts_ms DESC);

        CREATE TABLE IF NOT EXISTS gdelt_macro_features (
          bucket_ts_ms INTEGER NOT NULL,
          bucket_sec INTEGER NOT NULL,
          doc_count INTEGER NOT NULL DEFAULT 0,
          tone_mean REAL NOT NULL DEFAULT 0.0,
          tone_std REAL NOT NULL DEFAULT 0.0,
          conflict_share REAL NOT NULL DEFAULT 0.0,
          econ_share REAL NOT NULL DEFAULT 0.0,
          PRIMARY KEY(bucket_ts_ms, bucket_sec)
        );
        CREATE INDEX IF NOT EXISTS idx_gdelt_macro_bucket
          ON gdelt_macro_features(bucket_ts_ms);

        CREATE TABLE IF NOT EXISTS event_log_state (
          namespace TEXT NOT NULL,
          state_key TEXT NOT NULL,
          state_value TEXT,
          updated_ts_ms INTEGER NOT NULL,
          payload_json TEXT,
          PRIMARY KEY(namespace, state_key)
        );

        CREATE TABLE IF NOT EXISTS ipc_channels (
          channel TEXT PRIMARY KEY,
          owner TEXT,
          state_json TEXT NOT NULL DEFAULT '{}',
          last_seq INTEGER NOT NULL DEFAULT 0,
          updated_ts_ms INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS ipc_messages (
          seq INTEGER PRIMARY KEY AUTOINCREMENT,
          channel TEXT,
          msg_type TEXT,
          payload_json TEXT,
          sender TEXT,
          created_ts_ms INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_ipc_messages_channel_created
          ON ipc_messages(channel, created_ts_ms);
        CREATE INDEX IF NOT EXISTS idx_ipc_messages_channel_seq
          ON ipc_messages(channel, seq);

        CREATE TABLE IF NOT EXISTS broker_connection_health (
          ts_ms INTEGER NOT NULL,
          broker TEXT NOT NULL,
          ok INTEGER NOT NULL,
          state TEXT NOT NULL,
          latency_ms REAL,
          error TEXT,
          details_json TEXT,
          PRIMARY KEY(ts_ms, broker)
        );
        CREATE INDEX IF NOT EXISTS idx_broker_connection_health_broker_ts
          ON broker_connection_health(broker, ts_ms);

        CREATE TABLE IF NOT EXISTS execution_health_state (
          ts_ms INTEGER NOT NULL PRIMARY KEY,
          state TEXT NOT NULL,
          score REAL,
          n INTEGER,
          mean_slippage_bps REAL,
          p95_slippage_bps REAL,
          mean_latency_ms REAL,
          p95_latency_ms REAL,
          routing_error_rate REAL,
          open_due INTEGER,
          broker_failures INTEGER,
          extra_json TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_execution_health_state_ts
          ON execution_health_state(ts_ms);

        CREATE TABLE IF NOT EXISTS execution_alerts (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER NOT NULL,
          severity TEXT NOT NULL,
          alert_type TEXT NOT NULL,
          state TEXT NOT NULL,
          details_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_execution_alerts_ts
          ON execution_alerts(ts_ms);
        CREATE INDEX IF NOT EXISTS idx_execution_alerts_type_ts
          ON execution_alerts(alert_type, ts_ms);

        CREATE TABLE IF NOT EXISTS model_stats_regime (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER NOT NULL,
          symbol TEXT NOT NULL,
          horizon_s INTEGER NOT NULL,
          regime TEXT NOT NULL,
          n INTEGER NOT NULL,
          mean_impact_z REAL NOT NULL,
          UNIQUE(symbol, horizon_s, regime)
        );
        CREATE TABLE IF NOT EXISTS model_stats (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER NOT NULL,
          symbol TEXT NOT NULL,
          horizon_s INTEGER NOT NULL,
          n INTEGER NOT NULL,
          mean_impact_z REAL NOT NULL,
          UNIQUE(symbol, horizon_s)
        );
        CREATE INDEX IF NOT EXISTS idx_msreg_sym
          ON model_stats_regime(symbol, horizon_s);
        CREATE INDEX IF NOT EXISTS idx_ms_sym
          ON model_stats(symbol, horizon_s);

        CREATE TABLE IF NOT EXISTS data_sources (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          source_key TEXT NOT NULL UNIQUE,
          display_name TEXT NOT NULL,
          source_type TEXT NOT NULL,
          provider_name TEXT,
          job_name TEXT NOT NULL,
          enabled INTEGER NOT NULL DEFAULT 1,
          credentials_enc TEXT,
          key_version TEXT DEFAULT 'master_key',
          settings_json TEXT,
          status TEXT,
          last_error TEXT,
          last_success_ts_ms INTEGER,
          last_test_ts_ms INTEGER,
          error_count INTEGER NOT NULL DEFAULT 0,
          config_hash TEXT,
          created_ts_ms INTEGER NOT NULL,
          updated_ts_ms INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_data_sources_job_name
          ON data_sources(job_name);
        CREATE INDEX IF NOT EXISTS idx_data_sources_enabled
          ON data_sources(enabled);
        CREATE INDEX IF NOT EXISTS idx_data_sources_type
          ON data_sources(source_type);

        CREATE TABLE IF NOT EXISTS data_source_audit (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER NOT NULL,
          actor TEXT NOT NULL,
          action TEXT NOT NULL,
          source_key TEXT NOT NULL,
          source_type TEXT,
          provider_name TEXT,
          job_name TEXT,
          success INTEGER NOT NULL DEFAULT 1,
          message TEXT,
          detail_json TEXT,
          client_ip TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_data_source_audit_source_ts
          ON data_source_audit(source_key, ts_ms DESC);
        CREATE INDEX IF NOT EXISTS idx_data_source_audit_actor_ts
          ON data_source_audit(actor, ts_ms DESC);

        CREATE TABLE IF NOT EXISTS data_source_logs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER NOT NULL,
          source_key TEXT NOT NULL,
          level TEXT NOT NULL,
          event_type TEXT NOT NULL,
          message TEXT,
          detail_json TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_data_source_logs_source_ts
          ON data_source_logs(source_key, ts_ms DESC);

        CREATE TABLE IF NOT EXISTS alert_shelves (
          alert_id INTEGER PRIMARY KEY,
          shelved_ts_ms INTEGER NOT NULL,
          expires_ts_ms INTEGER NOT NULL,
          shelved_by TEXT,
          reason TEXT NOT NULL,
          source TEXT,
          severity TEXT,
          detail_json TEXT NOT NULL DEFAULT '{}'
        );

        CREATE TABLE IF NOT EXISTS alert_lifecycle_events (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          alert_id INTEGER NOT NULL,
          ts_ms INTEGER NOT NULL,
          lifecycle_state TEXT NOT NULL,
          actor TEXT,
          reason TEXT,
          source TEXT,
          detail_json TEXT NOT NULL DEFAULT '{}'
        );
        CREATE INDEX IF NOT EXISTS idx_alert_lifecycle_events_alert_ts
          ON alert_lifecycle_events(alert_id, ts_ms DESC);

        CREATE TABLE IF NOT EXISTS broker_config_audit (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER NOT NULL,
          action TEXT NOT NULL,
          actor TEXT NOT NULL,
          active_broker TEXT,
          success INTEGER NOT NULL,
          message TEXT,
          detail_json TEXT NOT NULL DEFAULT '{}'
        );
        CREATE INDEX IF NOT EXISTS idx_broker_config_audit_ts
          ON broker_config_audit(ts_ms DESC);

        CREATE TABLE IF NOT EXISTS terminal_intent_rejections (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER NOT NULL,
          symbol TEXT NOT NULL,
          side TEXT,
          qty REAL,
          reason_code TEXT NOT NULL,
          reason TEXT NOT NULL,
          source TEXT NOT NULL,
          detail_json TEXT NOT NULL DEFAULT '{}'
        );
        CREATE INDEX IF NOT EXISTS idx_terminal_intent_rejections_symbol_ts
          ON terminal_intent_rejections(symbol, ts_ms DESC);

        CREATE TABLE IF NOT EXISTS strategy_metrics (
          strategy_name TEXT,
          window_days INTEGER,
          ts_ms INTEGER NOT NULL,
          start_ts_ms INTEGER NOT NULL DEFAULT 0,
          end_ts_ms INTEGER NOT NULL DEFAULT 0,
          metrics_json TEXT NOT NULL DEFAULT '{}',
          is_active INTEGER NOT NULL DEFAULT 1,
          strategy TEXT,
          UNIQUE(strategy_name, window_days)
        );
        CREATE INDEX IF NOT EXISTS idx_strategy_metrics_ts
          ON strategy_metrics(ts_ms);

        CREATE TABLE IF NOT EXISTS size_policy (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER NOT NULL,
          lookback_days INTEGER NOT NULL DEFAULT 0,
          buckets INTEGER NOT NULL DEFAULT 0,
          method TEXT NOT NULL,
          params_json TEXT,
          metrics_json TEXT
        );
        CREATE TABLE IF NOT EXISTS size_policy_points (
          policy_id INTEGER NOT NULL,
          bucket_idx INTEGER NOT NULL,
          conf_lo REAL NOT NULL,
          conf_hi REAL NOT NULL,
          n INTEGER NOT NULL DEFAULT 0,
          mean_net_ret REAL NOT NULL DEFAULT 0.0,
          std_net_ret REAL NOT NULL DEFAULT 0.0,
          factor REAL NOT NULL DEFAULT 1.0,
          PRIMARY KEY(policy_id, bucket_idx)
        );
        CREATE INDEX IF NOT EXISTS idx_size_policy_ts
          ON size_policy(ts_ms);
        CREATE INDEX IF NOT EXISTS idx_size_policy_points_policy
          ON size_policy_points(policy_id, bucket_idx);

        CREATE TABLE IF NOT EXISTS walk_forward_runs (
          run_id TEXT PRIMARY KEY,
          params_json TEXT NOT NULL,
          metrics_json TEXT NOT NULL,
          ts_ms INTEGER NOT NULL,
          model_selection_json TEXT
        );
        CREATE TABLE IF NOT EXISTS walk_forward_scores (
          run_id TEXT NOT NULL,
          symbol TEXT NOT NULL,
          horizon_s INTEGER NOT NULL,
          ts_ms INTEGER NOT NULL,
          n INTEGER NOT NULL,
          mae REAL NOT NULL,
          dir_acc REAL NOT NULL,
          model_name TEXT,
          model_version TEXT,
          model_kind TEXT,
          PRIMARY KEY(run_id, symbol, horizon_s)
        );
        """
    )
    for table_name, columns in {
        "model_feature_snapshots": (
            "snapshot_version",
            "feature_ids_json",
            "vector_json",
            "features_json",
            "source_timestamps_json",
            "availability_json",
            "created_ts_ms",
        ),
        "ipc_channels": ("owner", "state_json", "last_seq", "updated_ts_ms"),
        "data_sources": ("key_version",),
        "strategy_metrics": (
            "strategy_name",
            "window_days",
            "ts_ms",
            "metrics_json",
            "start_ts_ms",
            "end_ts_ms",
            "is_active",
            "strategy",
        ),
        "size_policy": ("ts_ms", "lookback_days", "buckets", "method", "params_json", "metrics_json"),
        "walk_forward_runs": ("model_selection_json",),
        "walk_forward_scores": ("model_name", "model_version", "model_kind"),
    }.items():
        for column_name in columns:
            _alter_add_column_if_missing(con, table_name, column_name, _column_type(column_name, for_alter=True))


def _base_schema(con: sqlite3.Connection) -> None:
    _ensure_runtime_aux_schema(con)
    _ensure_contract_tables(con)
    _ensure_contract_indexes(con)
    _ensure_runtime_baseline_schema(con)
    _ensure_strategy_metrics_schema(con)
    _ensure_universe_audit_schema(con)
    _ensure_universe_pit_schema(con)
    _ensure_labels_price_schema(con)
    _ensure_model_marketplace_scores_schema(con)
    _ensure_external_schema(con)
    _ensure_execution_analytics_schema(con)
    _ensure_kill_switch_schema(con)
    _ensure_trade_attribution_ledger_schema(con)
    _ensure_options_chain_schema(con)
    _ensure_options_chain_v2_schema(con)
    _ensure_insider_transactions_schema(con)
    _ensure_congressional_trades_schema(con)
    _create_table(con, "news_event_features", ("id", "event_id", "ts_ms", "symbol", "payload_json", "sentiment_score", "embedding_novelty_score", "novelty_score", "stale_flag", "is_duplicate", "finbert_label", "finbert_score", "finbert_confidence", "finbert_pos", "finbert_neg", "finbert_neu"), (("event_id",),))
    _create_table(con, "news_symbol_features", ("id", "event_id", "ts_ms", "symbol", "payload_json"))
    _create_table(con, "options_event_features", ("id", "event_id", "ts_ms", "symbol", "payload_json"))
    _create_table(con, "finbert_sentiment_enrichments", ("id", "ts_ms", "symbol", "event_id", "source_identifier", "model_name", "payload_json"))
    _create_table(con, "finra_short_sale_volume", ("id", *_FINRA_SHORT_SALE_VOLUME_COLUMNS), (("source_record_id",),))
    _create_table(con, "finra_short_interest", ("id", *_FINRA_SHORT_INTEREST_COLUMNS), (("source_record_id",),))
    _create_table(con, "crypto_funding_rates", ("id", *_CRYPTO_FUNDING_RATE_COLUMNS), (("source_record_id",),))
    _mark_schema_applied(con)
    con.commit()
    return
    core_tables: dict[str, tuple[Sequence[str], Sequence[Sequence[str]]]] = {
        "runtime_metrics": (("id", "ts_ms", "metric", "value_num", "value_text", "tags_json"), ()),
        "schema_migrations": (("version", "name", "applied_ts_ms"), (("version",),)),
        "broker_account": (
            (
                "ts_ms",
                "updated_ts_ms",
                "broker",
                "account_id",
                "equity",
                "cash",
                "buying_power",
                "maintenance_margin",
                "day_pnl",
                "unrealized_pnl",
                "realized_pnl",
                "currency",
                "extra_json",
            ),
            (("ts_ms",),),
        ),
        "equity_history": (("ts_ms", "equity"), (("ts_ms",),)),
        "equity_drift": (
            (
                "ts_ms",
                "broker_equity",
                "backtest_equity",
                "diff_equity",
                "diff_equity_pct",
                "level",
                "reason",
                "backtest_run_id",
                "backtest_ts_ms",
                "detail_json",
            ),
            (("ts_ms",),),
        ),
        "events": (("id", "ts_ms", "timestamp", "event_type", "symbol", "source", "title", "body", "url", "event_key", "importance_score", "meta_json"), (("event_key", "ts_ms"),)),
        "prices": (("id", "ts_ms", "symbol", "price", "source"), (("symbol", "ts_ms", "source"),)),
        "price_quotes": (("id", "ts_ms", "symbol", "bid", "ask", "last", "price", "provider", "source"), (("symbol", "provider", "ts_ms"),)),
        "price_quotes_raw": (("id", "ts_ms", "symbol", "provider", "payload_json"), (("symbol", "provider", "ts_ms"),)),
        "labels": (("id", "event_id", "symbol", "horizon_s", "label", "ts_ms"), ()),
        "predictions": (("id", "event_id", "symbol", "horizon_s", "prediction", "model_name", "ts_ms"), ()),
        "shadow_predictions": (("id", "event_id", "symbol", "horizon_s", "model_name", "prediction", "confidence", "ts_ms", "payload_json"), ()),
        "insider_transactions": (("id", *_INSIDER_TRANSACTION_COLUMNS), (("source_transaction_id",),)),
        "congressional_trades": (("id", *_CONGRESSIONAL_TRADE_COLUMNS), (("source_trade_id",),)),
        "news_event_features": (("id", "event_id", "ts_ms", "symbol", "payload_json", "sentiment_score", "embedding_novelty_score", "novelty_score", "stale_flag", "is_duplicate", "finbert_label", "finbert_score", "finbert_confidence", "finbert_pos", "finbert_neg", "finbert_neu"), (("event_id",),)),
        "news_symbol_features": (("id", "event_id", "ts_ms", "symbol", "payload_json"), ()),
        "options_event_features": (("id", "event_id", "ts_ms", "symbol", "payload_json"), ()),
        "finbert_sentiment_enrichments": (("id", "ts_ms", "symbol", "event_id", "source_identifier", "model_name", "payload_json"), ()),
        "finra_short_sale_volume": (("id", *_FINRA_SHORT_SALE_VOLUME_COLUMNS), (("source_record_id",),)),
        "finra_short_interest": (("id", *_FINRA_SHORT_INTEREST_COLUMNS), (("source_record_id",),)),
        "crypto_funding_rates": (("id", *_CRYPTO_FUNDING_RATE_COLUMNS), (("source_record_id",),)),
        "alpha_candidates": (("id", "candidate_name", "candidate_version", "model_family", "feature_ids", "generation_method", "hyperparams", "status", "diagnostics", "created_ts"), ()),
        "alpha_lifecycle": (("id", "candidate_id", "stage", "outcome", "metrics", "notes", "created_ts", "alert_id", "created_ts_ms", "expires_ts_ms", "half_life_ms", "volatility", "status", "last_touch_ts_ms", "meta_json"), ()),
        "hypothesis_registry": (("id", "created_ts", "model_name", "candidate_version", "n_observations", "t_statistic", "deflated_sharpe", "threshold_t", "n_competing_trials", "passed", "diagnostics"), ()),
        "backtest_cpcv_runs": (("id", "created_ts", "ts", "model_name", "candidate_version", "model_id", "n_splits", "n_test_splits", "embargo_pct", "n_paths", "path_index", "path_returns", "path_sharpes", "mean_sharpe", "median_sharpe", "pbo", "sharpe", "deflated_sharpe", "n_trials", "total_return", "max_drawdown", "cfg", "payload", "diagnostics"), ()),
        "backtest_cpcv_path_results": (("id", "created_ts", "ts", "model_name", "candidate_version", "path_index", "path_returns", "path_sharpes", "sharpe", "deflated_sharpe", "payload"), ()),
        "drift_retrain_events": (("id", "created_ts", "model_name", "family", "trigger_type", "trigger_metrics", "action_taken", "cooldown_applied", "candidate_version", "outcome_status", "diagnostics"), ()),
        "model_hyperparameter_registry": (
            (
                "id",
                "ts",
                "model_family",
                "model_name",
                "symbol",
                "tuner",
                "objective",
                "study_name",
                "params",
                "params_json",
                "metric_value",
                "trial_count",
                "best_trial_number",
                "seed",
                "cpcv_mean_sharpe",
                "cpcv_median_sharpe",
                "cpcv_pbo",
                "diagnostics",
            ),
            (),
        ),
        "model_best_params": (("id", "model_family", "symbol", "ts", "study_name", "params_json", "value", "trial_number", "seed"), (("model_family", "symbol"),)),
        "model_registry": (
            (
                "id",
                "model_name",
                "version",
                "model_version",
                "family",
                "model_family",
                "model_kind",
                "model_ts_ms",
                "stage",
                "regime",
                "status",
                "promotion_status",
                "created_ts",
                "created_ts_ms",
                "updated_ts",
                "updated_ts_ms",
                "last_promotion_ts_ms",
                "metadata_json",
                "metrics_json",
                "performance_metrics_json",
                "feature_schema_json",
                "note",
                "blob",
            ),
            (("model_name", "version"),),
        ),
        "models": (
            (
                "id",
                "symbol",
                "model_name",
                "version",
                "model_kind",
                "status",
                "is_active",
                "artifact_uri",
                "training_start_ts_ms",
                "training_end_ts_ms",
                "training_data_window_json",
                "performance_metrics_json",
                "metadata_json",
                "selection_metric_name",
                "selection_metric_value",
                "selection_metric_higher_is_better",
                "created_ts",
                "created_ts_ms",
                "updated_ts",
                "updated_ts_ms",
            ),
            (("symbol", "model_name", "version"),),
        ),
        "model_versions": (
            (
                "model_name",
                "model_version",
                "model_kind",
                "parent_version",
                "mutation_kind",
                "stage",
                "status",
                "live_ready",
                "training_job_name",
                "train_scope_json",
                "meta_json",
                "created_ts_ms",
                "updated_ts_ms",
            ),
            (("model_name", "model_version"),),
        ),
        "model_version_performance": (
            (
                "id",
                "model_name",
                "model_version",
                "metric_scope",
                "metric_name",
                "metric_value",
                "sample_n",
                "recorded_ts_ms",
                "meta_json",
            ),
            (),
        ),
        "model_lifecycle_runs": (
            (
                "id",
                "model_name",
                "model_version",
                "parent_version",
                "action",
                "status",
                "triggered_by",
                "mutation_kind",
                "details_json",
                "created_ts_ms",
                "updated_ts_ms",
            ),
            (),
        ),
        "model_marketplace_scores": (("id", "model_id", "model_name", "symbol", "horizon_s", "score", "status", "created_ts", "updated_ts", "metadata_json"), ()),
        "champion_assignments": (("id", "scope", "symbol", "horizon_s", "model_id", "model_name", "assigned_ts_ms", "metadata_json"), (("scope", "symbol", "horizon_s"),)),
        "model_competition_rankings": (("id", "ranking_scope", "model_id", "model_name", "rank", "score", "created_ts_ms", "metadata_json"), ()),
        "realized_outcomes": (("id", "symbol", "ts_ms", "realized_return", "metadata_json", "created_ts_ms", "updated_ts_ms"), (("symbol", "ts_ms"),)),
        "model_performance": (
            (
                "id",
                "tracked_prediction_id",
                "prediction_id",
                "outcome_id",
                "time",
                "prediction_time",
                "symbol",
                "model_id",
                "model_name",
                "model_version",
                "horizon_s",
                "prediction",
                "realized_return",
                "error",
                "directional_accuracy",
                "pnl_impact",
                "rolling_score",
                "regime_time_ms",
                "volatility_regime",
                "trend_regime",
                "liquidity_regime",
                "metadata_json",
                "created_ts_ms",
                "updated_ts_ms",
            ),
            (("tracked_prediction_id",),),
        ),
        "job_locks": (("id", "job_name", "owner", "pid", "acquired_ts_ms", "heartbeat_ts_ms", "expires_ms"), (("job_name",),)),
        "job_heartbeats": (("id", "job_name", "owner", "pid", "ts_ms", "extra_json"), (("job_name",),)),
        "job_checkpoints": (("id", "job_name", "last_event_id", "last_event_ts_ms", "updated_ts_ms"), (("job_name",),)),
        "portfolio_bt_runs": (("id", "ts_ms", "start_ts_ms", "end_ts_ms", "metrics_json"), ()),
        "portfolio_bt_points": (
            ("run_id", "ts_ms", "ret", "equity", "drawdown", "exec_cost", "slippage", "fees", "detail_json"),
            (("run_id", "ts_ms"),),
        ),
        "risk_state": (("key", "value", "updated_ts_ms"), (("key",),)),
        "runtime_meta": (("key", "value", "updated_ts_ms"), (("key",),)),
        "symbols": (("symbol", "score", "status", "asset_class", "meta_json", "updated_ts_ms"), (("symbol",),)),
        "symbol_universe": (("symbol", "status", "first_seen_ms", "last_seen_ms", "seen_n", "meta_json"), (("symbol",),)),
        "alerts": (
            (
                "id",
                "ts_ms",
                "event_id",
                "event_title",
                "symbol",
                "horizon_s",
                "expected_z",
                "confidence",
                "severity",
                "rule_id",
                "explain_json",
                "dedupe_key",
                "title",
                "message",
                "source",
                "status",
                "detail_json",
                "updated_ts_ms",
                "model_name",
                "model_id",
                "model_version",
            ),
            (("dedupe_key",),),
        ),
        "tracked_predictions": (("id", "ts_ms", "symbol", "model_name", "prediction", "metadata_json"), ()),
        "execution_orders": (("id", "ts_ms", "symbol", "side", "quantity", "status", "metadata_json"), ()),
        "execution_fills": (("id", "ts_ms", "order_id", "symbol", "quantity", "price", "metadata_json"), ()),
        "pnl_attribution": (("id", "ts_ms", "symbol", "pnl", "metadata_json"), ()),
        "decision_log": (("id", "ts_ms", "symbol", "decision", "reason_json", "metadata_json"), ()),
        "promotion_statistical_evidence": (
            (
                "id",
                "ts",
                "model_id",
                "feature_id",
                "evidence_kind",
                "test_name",
                "t_stat",
                "p_value",
                "q_value",
                "bootstrap_samples",
                "decision",
                "payload_json",
                "prev_hash",
                "row_hash",
            ),
            (("model_id", "evidence_kind", "ts"),),
        ),
    }
    for table, (columns, uniques) in core_tables.items():
        _create_table(con, table, columns, uniques)
    con.execute("INSERT OR IGNORE INTO schema_migrations(version, name, applied_ts_ms) VALUES (?, ?, ?)", (SCHEMA_VERSION, "test_sqlite_schema", int(time.time() * 1000)))
    con.commit()


def apply_migrations() -> list[int]:
    init_db()
    return [SCHEMA_VERSION]


def init_db(schema: str | None = None):
    del schema
    path = _current_db_path()
    key = str(path.resolve())
    with _INIT_LOCK:
        if key in _INITIALIZED_PATHS and _sqlite_schema_sentinels_ready(path):
            return [SCHEMA_VERSION]
        con = _connect_raw(readonly=False)
        try:
            _base_schema(con)
            _INITIALIZED_PATHS.add(key)
        finally:
            con.close()
    _ensure_liveness_db_schema()
    return [SCHEMA_VERSION]


def init_rl_portfolio_tables(con=None) -> None:
    del con
    init_db()


def close_pooled_connections() -> None:
    active = _active_write_connection()
    if active is None:
        return None
    try:
        if bool(getattr(active, "in_transaction", False)):
            try:
                sqlite3.Connection.rollback(active)
            except Exception:
                LOGGER.debug("sqlite_close_pooled_rollback_failed", exc_info=True)
        active._managed_write_active = False
        _clear_active_write_connection(active)
        try:
            sqlite3.Connection.close(active)
        except Exception:
            LOGGER.debug("sqlite_close_pooled_close_failed", exc_info=True)
    finally:
        active._release_write_lock()
    return None


_SQLITE_LIVENESS_FLUSH_INTERVAL_S = max(
    0.05,
    float(os.environ.get("SQLITE_LIVENESS_FLUSH_INTERVAL_S", "1.0") or 1.0),
)
_SQLITE_LIVENESS_FLUSH_JITTER_RATIO = min(
    1.0,
    max(0.0, float(os.environ.get("SQLITE_LIVENESS_FLUSH_JITTER_RATIO", "0.5") or 0.5)),
)
_SQLITE_LIVENESS_MIN_PERSIST_INTERVAL_MS = max(
    0,
    int(float(os.environ.get("SQLITE_LIVENESS_MIN_PERSIST_INTERVAL_S", "0") or 0) * 1000.0),
)
_SQLITE_LIVENESS_MAX_BATCH = max(1, int(os.environ.get("SQLITE_LIVENESS_MAX_BATCH", "64") or 64))
_SQLITE_LIVENESS_LOCK = threading.Condition()
_SQLITE_LIVENESS_PENDING: dict[str, dict[str, Any]] = {}
_SQLITE_LIVENESS_LAST_PERSIST_MS: dict[str, int] = {}
_SQLITE_LIVENESS_STOP = threading.Event()
_SQLITE_LIVENESS_THREAD: threading.Thread | None = None
_SQLITE_LIVENESS_STATE: dict[str, Any] = {
    "pending_count": 0,
    "flush_batches": 0,
    "flushed": 0,
    "dropped": 0,
    "last_enqueue_ts_ms": 0,
    "last_flush_ts_ms": 0,
    "last_error": "",
    "last_error_ts_ms": 0,
}


def _staggered_liveness_flush_interval_s() -> float:
    base = max(0.05, float(_SQLITE_LIVENESS_FLUSH_INTERVAL_S))
    jitter = min(1.0, max(0.0, float(_SQLITE_LIVENESS_FLUSH_JITTER_RATIO)))
    if jitter <= 0.0:
        return float(base)
    bucket = max(0, int(os.getpid()) % 17)
    return float(base * (1.0 + ((float(bucket) / 16.0) * jitter)))


_SQLITE_LIVENESS_EFFECTIVE_FLUSH_INTERVAL_S = _staggered_liveness_flush_interval_s()


def _liveness_queue_enabled() -> bool:
    return _env_truthy(os.environ.get("SQLITE_LIVENESS_QUEUE_ENABLED"))


def _liveness_flush_backoff_s(consecutive_failures: int) -> float:
    failures = max(1, min(int(consecutive_failures), 5))
    base = max(1.0, float(_SQLITE_LIVENESS_EFFECTIVE_FLUSH_INTERVAL_S))
    return min(10.0, float(base * (2 ** failures)))


def _merge_json_text(left: str | None, right: str | None) -> str | None:
    if not left:
        return right
    if not right:
        return left
    try:
        base = json.loads(str(left) or "{}")
        update = json.loads(str(right) or "{}")
    except Exception:
        return right
    if not isinstance(base, dict) or not isinstance(update, dict):
        return right
    for key, value in update.items():
        if isinstance(base.get(key), dict) and isinstance(value, dict):
            merged = dict(base.get(key) or {})
            merged.update(value)
            base[key] = merged
        else:
            base[key] = value
    return json.dumps(base, separators=(",", ":"), sort_keys=True)


def _enqueue_job_liveness(job_name: str, owner: str, pid: int, extra_json: str | None) -> None:
    now_ms = int(time.time() * 1000)
    key = str(job_name)
    with _SQLITE_LIVENESS_LOCK:
        previous = _SQLITE_LIVENESS_PENDING.get(key)
        merged = _merge_json_text(previous.get("extra_json") if previous else None, extra_json)
        _SQLITE_LIVENESS_PENDING[key] = {
            "job_name": key,
            "owner": str(owner),
            "pid": int(pid),
            "extra_json": merged,
            "queued_ts_ms": int(now_ms),
        }
        _SQLITE_LIVENESS_STATE["pending_count"] = int(len(_SQLITE_LIVENESS_PENDING))
        _SQLITE_LIVENESS_STATE["last_enqueue_ts_ms"] = int(now_ms)
        _SQLITE_LIVENESS_LOCK.notify_all()


def _drain_job_liveness_batch(*, max_rows: int | None = None, force: bool = False) -> list[dict[str, Any]]:
    del force
    with _SQLITE_LIVENESS_LOCK:
        limit = max(1, int(max_rows or _SQLITE_LIVENESS_MAX_BATCH))
        keys = list(_SQLITE_LIVENESS_PENDING.keys())[:limit]
        rows = [dict(_SQLITE_LIVENESS_PENDING.pop(key) or {}) for key in keys]
        _SQLITE_LIVENESS_STATE["pending_count"] = int(len(_SQLITE_LIVENESS_PENDING))
        return rows


def _requeue_job_liveness_batch(rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    with _SQLITE_LIVENESS_LOCK:
        for row in reversed(list(rows)):
            key = str((row or {}).get("job_name") or "")
            if not key:
                continue
            existing = _SQLITE_LIVENESS_PENDING.get(key)
            if existing:
                row = {
                    **dict(row),
                    "extra_json": _merge_json_text(
                        str(row.get("extra_json") or "") or None,
                        str(existing.get("extra_json") or "") or None,
                    ),
                }
            _SQLITE_LIVENESS_PENDING[key] = dict(row)
        _SQLITE_LIVENESS_STATE["pending_count"] = int(len(_SQLITE_LIVENESS_PENDING))
        _SQLITE_LIVENESS_LOCK.notify_all()


def _put_job_heartbeat_now(job_name: str, owner: str, pid: int, extra_json: str | None = None) -> None:
    now_ms = int(time.time() * 1000)
    con = _connect_liveness_storage_rw_direct()
    try:
        con.execute(
            """
            INSERT INTO job_heartbeats(job_name, owner, pid, ts_ms, extra_json)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(job_name) DO UPDATE SET
              owner=excluded.owner,
              pid=excluded.pid,
              ts_ms=excluded.ts_ms,
              extra_json=excluded.extra_json
            """,
            (str(job_name), str(owner), int(pid), now_ms, extra_json),
        )
        con.execute(
            "UPDATE job_locks SET heartbeat_ts_ms=? WHERE job_name=? AND owner=? AND pid=?",
            (now_ms, str(job_name), str(owner), int(pid)),
        )
        con.commit()
        _SQLITE_LIVENESS_LAST_PERSIST_MS[str(job_name)] = int(now_ms)
    except Exception:
        try:
            if hasattr(con, "rollback_managed_write"):
                con.rollback_managed_write()
            else:
                con.rollback()
        except Exception:
            LOGGER.debug("sqlite_liveness_heartbeat_rollback_failed", exc_info=True)
        raise
    finally:
        con.close()


def _flush_job_liveness_batch(rows: list[dict[str, Any]]) -> int:
    flushed = 0
    for row in list(rows or []):
        job_name = str((row or {}).get("job_name") or "")
        if not job_name:
            continue
        _put_job_heartbeat_now(
            job_name,
            str((row or {}).get("owner") or ""),
            int((row or {}).get("pid") or 0),
            (str((row or {}).get("extra_json")) if (row or {}).get("extra_json") is not None else None),
        )
        flushed += 1
    return int(flushed)


def put_job_heartbeat(
    job_name: str,
    owner: str,
    pid: int,
    extra_json: str | None = None,
    *,
    best_effort: bool = False,
) -> None:
    if _liveness_queue_enabled():
        _enqueue_job_liveness(job_name, owner, pid, extra_json)
        _ensure_job_liveness_writer_started()
        return None
    try:
        _put_job_heartbeat_now(job_name, owner, pid, extra_json)
    except Exception as exc:
        if bool(best_effort) and _is_transient_sqlite_error(exc):
            return None
        raise


def _ensure_job_liveness_writer_started() -> None:
    global _SQLITE_LIVENESS_THREAD
    if not _liveness_queue_enabled():
        return None
    thread = _SQLITE_LIVENESS_THREAD
    if thread is not None and thread.is_alive():
        return None
    with _SQLITE_LIVENESS_LOCK:
        thread = _SQLITE_LIVENESS_THREAD
        if thread is not None and thread.is_alive():
            return None
        _SQLITE_LIVENESS_STOP.clear()
        _SQLITE_LIVENESS_THREAD = threading.Thread(
            target=_job_liveness_writer_loop,
            name="sqlite-liveness-writer",
            daemon=True,
        )
        _SQLITE_LIVENESS_THREAD.start()
    return None


def touch_job_lock(job_name: str, owner: str, pid: int, *, best_effort: bool = False) -> None:
    now_ms = int(time.time() * 1000)
    con = connect_rw_direct()
    try:
        con.begin_managed_write()
        con.execute(
            "UPDATE job_locks SET heartbeat_ts_ms=? WHERE job_name=? AND owner=? AND pid=?",
            (now_ms, str(job_name), str(owner), int(pid)),
        )
        if hasattr(con, "commit_managed_write"):
            con.commit_managed_write()
        else:
            con.commit()
    except Exception as exc:
        try:
            if hasattr(con, "rollback_managed_write"):
                con.rollback_managed_write()
            else:
                con.rollback()
        except Exception:
            LOGGER.debug("sqlite_liveness_touch_lock_rollback_failed", exc_info=True)
        if bool(best_effort) and _is_transient_sqlite_error(exc):
            return None
        raise
    finally:
        con.close()


def flush_job_liveness_queue(*, max_batches: int = 8, force: bool = True) -> dict[str, Any]:
    total_flushed = 0
    for _ in range(max(1, int(max_batches or 1))):
        batch = _drain_job_liveness_batch()
        if not batch:
            break
        now_ms = int(time.time() * 1000)
        writable: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        for row in batch:
            job_name = str((row or {}).get("job_name") or "")
            last_ms = int(_SQLITE_LIVENESS_LAST_PERSIST_MS.get(job_name) or 0)
            if (
                not bool(force)
                and _SQLITE_LIVENESS_MIN_PERSIST_INTERVAL_MS > 0
                and last_ms > 0
                and (now_ms - last_ms) < _SQLITE_LIVENESS_MIN_PERSIST_INTERVAL_MS
            ):
                skipped.append(row)
            else:
                writable.append(row)
        try:
            flushed = _flush_job_liveness_batch(writable)
        except Exception:
            _requeue_job_liveness_batch(batch)
            raise
        if skipped:
            _requeue_job_liveness_batch(skipped)
        total_flushed += int(flushed)
        with _SQLITE_LIVENESS_LOCK:
            _SQLITE_LIVENESS_STATE["flush_batches"] = int(_SQLITE_LIVENESS_STATE.get("flush_batches") or 0) + 1
            _SQLITE_LIVENESS_STATE["flushed"] = int(_SQLITE_LIVENESS_STATE.get("flushed") or 0) + int(flushed)
            if flushed:
                _SQLITE_LIVENESS_STATE["last_flush_ts_ms"] = int(time.time() * 1000)
            _SQLITE_LIVENESS_STATE["last_error"] = ""
        if skipped:
            break
    snapshot = _job_liveness_queue_snapshot()
    return {
        "ok": True,
        "enabled": bool(_liveness_queue_enabled()),
        "flushed": int(total_flushed),
        "pending": int(snapshot.get("pending_count") or 0),
        "pending_count": int(snapshot.get("pending_count") or 0),
    }


def _job_liveness_writer_loop() -> None:
    consecutive_failures = 0
    while True:
        wait_s = (
            _liveness_flush_backoff_s(consecutive_failures)
            if consecutive_failures > 0
            else float(_SQLITE_LIVENESS_EFFECTIVE_FLUSH_INTERVAL_S)
        )
        if _SQLITE_LIVENESS_STOP.wait(timeout=float(wait_s)):
            return
        batch = _drain_job_liveness_batch()
        if not batch:
            continue
        try:
            flushed = _flush_job_liveness_batch(batch)
            consecutive_failures = 0
            with _SQLITE_LIVENESS_LOCK:
                _SQLITE_LIVENESS_STATE["flush_batches"] = int(_SQLITE_LIVENESS_STATE.get("flush_batches") or 0) + 1
                _SQLITE_LIVENESS_STATE["flushed"] = int(_SQLITE_LIVENESS_STATE.get("flushed") or 0) + int(flushed)
                _SQLITE_LIVENESS_STATE["last_flush_ts_ms"] = int(time.time() * 1000)
                _SQLITE_LIVENESS_STATE["last_error"] = ""
        except Exception as exc:
            consecutive_failures = min(consecutive_failures + 1, 5)
            _requeue_job_liveness_batch(batch)
            with _SQLITE_LIVENESS_LOCK:
                _SQLITE_LIVENESS_STATE["last_error"] = f"{type(exc).__name__}:{exc}"
                _SQLITE_LIVENESS_STATE["last_error_ts_ms"] = int(time.time() * 1000)


def shutdown_job_liveness_queue(*, timeout_s: float = 2.0) -> dict[str, Any]:
    _SQLITE_LIVENESS_STOP.set()
    thread = _SQLITE_LIVENESS_THREAD
    if thread is not None and thread.is_alive():
        thread.join(timeout=max(0.0, float(timeout_s)))
    return flush_job_liveness_queue(force=True)


def _job_liveness_queue_snapshot() -> dict[str, Any]:
    with _SQLITE_LIVENESS_LOCK:
        state = dict(_SQLITE_LIVENESS_STATE)
        pending_count = int(len(_SQLITE_LIVENESS_PENDING))
    state["enabled"] = bool(_liveness_queue_enabled())
    state["pending_count"] = int(pending_count)
    state["pending"] = int(pending_count)
    state["flush_interval_base_s"] = float(_SQLITE_LIVENESS_FLUSH_INTERVAL_S)
    state["flush_jitter_ratio"] = float(_SQLITE_LIVENESS_FLUSH_JITTER_RATIO)
    state["flush_interval_s"] = float(_SQLITE_LIVENESS_EFFECTIVE_FLUSH_INTERVAL_S)
    state["min_persist_interval_ms"] = int(_SQLITE_LIVENESS_MIN_PERSIST_INTERVAL_MS)
    state["db_enabled"] = bool(_liveness_db_enabled())
    state["db_path"] = str(_current_liveness_db_path())
    return state


def get_connection_debug_snapshot() -> dict[str, Any]:
    with _SQLITE_TRACE_LOCK:
        top_tables = sorted(
            (dict(value or {}) for value in _SQLITE_TRACE_BY_TABLE.values()),
            key=lambda row: int(row.get("writes") or 0),
            reverse=True,
        )
        top_paths = sorted(
            (dict(value or {}) for value in _SQLITE_TRACE_BY_PATH.values()),
            key=lambda row: int(row.get("writes") or 0),
            reverse=True,
        )
        trace = {
            "history": list(_SQLITE_TRACE_HISTORY),
            "longest_locks": list(_SQLITE_TRACE_LONGEST_LOCKS),
            "by_table": dict(_SQLITE_TRACE_BY_TABLE),
            "by_path": dict(_SQLITE_TRACE_BY_PATH),
            "totals": dict(_SQLITE_TRACE_TOTALS),
            "top_write_tables": top_tables[:20],
            "top_contention_paths": top_paths[:20],
        }
    return {
        "storage": "sqlite",
        "backend": "sqlite",
        "db_path": str(_current_db_path()),
        "sqlite_trace": trace,
        "txn_stats": {
            "busy_retry_count": int(_SQLITE_TRACE_TOTALS.get("busy_retry_count") or 0),
            "slow_write_count": int(_SQLITE_TRACE_TOTALS.get("slow_write_count") or 0),
            "cannot_commit_count": int(_SQLITE_TRACE_TOTALS.get("cannot_commit_count") or 0),
        },
        "liveness_queue": _job_liveness_queue_snapshot(),
    }


_REQUIRED_TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    "runtime_meta": ("key", "value", "updated_ts_ms"),
    "schema_version": ("version", "applied_ts_ms", "status", "notes"),
    "market_features": ("ts_ms", "symbol", "v", "features_json"),
    "regime_state": ("time", "symbol", "volatility_regime", "trend_regime", "liquidity_regime", "created_ts_ms"),
    "event_log": (
        "id",
        "ts_ms",
        "event_type",
        "event_source",
        "event_version",
        "entity_type",
        "entity_id",
        "correlation_id",
        "payload_json",
    ),
    "event_log_state": ("namespace", "state_key", "state_value", "updated_ts_ms", "payload_json"),
    "prediction_history": (
        "id",
        "ts_ms",
        "event_id",
        "symbol",
        "horizon_s",
        "predicted_z",
        "confidence",
        "confidence_raw",
        "prediction_strength",
        "model_name",
        "model_id",
        "model_version",
    ),
    "model_feature_snapshots": (
        "symbol",
        "ts_ms",
        "feature_set_tag",
        "snapshot_version",
        "feature_ids_json",
        "vector_json",
        "features_json",
        "source_timestamps_json",
        "availability_json",
        "created_ts_ms",
    ),
    "gdelt_macro_features": (
        "bucket_ts_ms",
        "bucket_sec",
        "doc_count",
        "tone_mean",
        "tone_std",
        "conflict_share",
        "econ_share",
    ),
    "ipc_channels": ("channel", "owner", "state_json", "last_seq", "updated_ts_ms"),
    "broker_connection_health": ("ts_ms", "broker", "ok", "state", "latency_ms", "error", "details_json"),
    "execution_health_state": (
        "ts_ms",
        "state",
        "score",
        "n",
        "mean_slippage_bps",
        "p95_slippage_bps",
        "mean_latency_ms",
        "p95_latency_ms",
        "routing_error_rate",
        "open_due",
        "broker_failures",
        "extra_json",
    ),
    "model_stats_regime": ("id", "ts_ms", "symbol", "horizon_s", "regime", "n", "mean_impact_z"),
    "data_sources": (
        "id",
        "source_key",
        "display_name",
        "source_type",
        "provider_name",
        "job_name",
        "enabled",
        "credentials_enc",
        "key_version",
        "settings_json",
        "status",
        "last_error",
        "last_success_ts_ms",
        "last_test_ts_ms",
        "error_count",
        "config_hash",
        "created_ts_ms",
        "updated_ts_ms",
    ),
    "strategy_metrics": ("strategy_name", "window_days", "ts_ms", "metrics_json", "is_active"),
    "size_policy": ("id", "ts_ms", "lookback_days", "buckets", "method", "params_json", "metrics_json"),
    "walk_forward_runs": ("run_id", "params_json", "metrics_json", "ts_ms", "model_selection_json"),
    "walk_forward_scores": (
        "run_id",
        "symbol",
        "horizon_s",
        "ts_ms",
        "n",
        "mae",
        "dir_acc",
        "model_name",
        "model_version",
        "model_kind",
    ),
    "job_locks": ("job_name", "owner", "pid", "acquired_ts_ms", "heartbeat_ts_ms", "expires_ms"),
    "job_heartbeats": ("job_name", "owner", "pid", "ts_ms", "extra_json"),
    "job_checkpoints": ("job_name", "last_event_id", "last_event_ts_ms", "updated_ts_ms"),
    **_EXACT_TABLE_COLUMNS,
}

_REQUIRED_INDEXES: tuple[str, ...] = (
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
    "idx_market_features_symbol_ts",
    "idx_event_log_type_ts",
    "idx_event_log_entity",
    "idx_event_log_corr",
    "idx_model_feature_snapshots_symbol_ts",
    "idx_model_feature_snapshots_ts",
    "idx_model_feature_snapshots_symbol_feature_set_ts_desc",
    "idx_gdelt_macro_bucket",
    "idx_broker_connection_health_broker_ts",
    "idx_execution_health_state_ts",
    "idx_msreg_sym",
    "idx_data_sources_enabled",
    "idx_data_sources_job_name",
    "idx_data_sources_type",
    "idx_strategy_metrics_ts",
    "idx_size_policy_ts",
    "idx_size_policy_points_policy",
    "idx_job_checkpoints_updated",
    "idx_decision_log_ts",
    "idx_decision_log_symbol_ts",
    "idx_decision_log_model_ts",
    "idx_predictions_ts",
    "idx_predictions_symbol_ts",
    "idx_predictions_model_ts",
    "idx_regime_state_symbol_time_desc",
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
)


def _validation_table_columns(con: sqlite3.Connection, table: str) -> dict[str, dict[str, Any]]:
    rows = sqlite3.Connection.execute(con, f"PRAGMA table_info({_quote(table)})").fetchall() or []
    return {
        str(row[1]): {
            "type": str(row[2] or "").upper(),
            "notnull": bool(row[3]),
            "default": None if row[4] is None else str(row[4]),
            "pk": int(row[5] or 0),
        }
        for row in rows
    }


def _validation_index_names(con: sqlite3.Connection) -> set[str]:
    rows = sqlite3.Connection.execute(
        con,
        "SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'",
    ).fetchall() or []
    return {str(row[0]) for row in rows}


def get_db_validation_snapshot(*, include_quick_check: bool = True, strict: bool = False) -> dict[str, Any]:
    path = _current_db_path()
    have_tables: list[str] = []
    missing_tables: list[str] = []
    missing_columns: dict[str, list[str]] = {}
    missing_indexes: list[str] = []
    owned_missing_tables: list[str] = []
    owned_missing_columns: dict[str, list[str]] = {}
    owned_unexpected_columns: dict[str, list[str]] = {}
    owned_pk_mismatches: dict[str, dict[str, dict[str, int]]] = {}
    owned_missing_indexes: dict[str, list[str]] = {}
    schema_version: int | None = None
    schema_status = "missing"
    quick_check = "skipped"
    try:
        con = _connect_raw(readonly=True)
        try:
            have_tables = [
                str(row[0])
                for row in sqlite3.Connection.execute(
                    con,
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
                ).fetchall()
            ]
            have_set = set(have_tables)
            for table, columns in _REQUIRED_TABLE_COLUMNS.items():
                if table not in have_set:
                    missing_tables.append(table)
                    continue
                actual = _validation_table_columns(con, table)
                missing = [col for col in columns if col not in actual]
                if missing:
                    missing_columns[table] = missing

            index_names = _validation_index_names(con)
            missing_indexes = sorted(name for name in _REQUIRED_INDEXES if name not in index_names)

            if "schema_version" in have_set:
                row = sqlite3.Connection.execute(
                    con,
                    """
                    SELECT version, status
                    FROM schema_version
                    ORDER BY version DESC
                    LIMIT 1
                    """,
                ).fetchone()
                if row:
                    schema_version = int(row[0] or 0)
                    schema_status = str(row[1] or "unknown")
            if schema_version is None and "runtime_meta" in have_set:
                row = sqlite3.Connection.execute(
                    con,
                    "SELECT value FROM runtime_meta WHERE key='schema_version' LIMIT 1",
                ).fetchone()
                if row:
                    schema_version = int(float(str(row[0] or "0")))
                    schema_status = "legacy"

            from engine.runtime.storage_live_ingestion_schema import (
                OWNED_LIVE_TABLE_COLUMN_SPECS,
                OWNED_LIVE_TABLE_REQUIRED_INDEXES,
            )

            for table, expected_specs in OWNED_LIVE_TABLE_COLUMN_SPECS.items():
                if table not in have_set:
                    owned_missing_tables.append(table)
                    continue
                actual = _validation_table_columns(con, table)
                expected_cols = set(expected_specs)
                actual_cols = set(actual)
                missing = sorted(expected_cols - actual_cols)
                unexpected = sorted(actual_cols - expected_cols)
                if missing:
                    owned_missing_columns[table] = missing
                if unexpected:
                    owned_unexpected_columns[table] = unexpected
                pk_diff: dict[str, dict[str, int]] = {}
                for column_name, expected_spec in expected_specs.items():
                    if column_name not in actual:
                        continue
                    actual_pk = int(actual[column_name].get("pk") or 0)
                    expected_pk = int((expected_spec or {}).get("pk") or 0)
                    if actual_pk != expected_pk:
                        pk_diff[column_name] = {"expected": expected_pk, "actual": actual_pk}
                if pk_diff:
                    owned_pk_mismatches[table] = pk_diff
                missing_owned_indexes = [
                    name
                    for name in OWNED_LIVE_TABLE_REQUIRED_INDEXES.get(table, ())
                    if name not in index_names
                ]
                if missing_owned_indexes:
                    owned_missing_indexes[table] = sorted(missing_owned_indexes)

            if include_quick_check:
                row = sqlite3.Connection.execute(con, "PRAGMA quick_check").fetchone()
                quick_check = str(row[0] if row else "ok")
            else:
                quick_check = "skipped"
        finally:
            con.close()
    except Exception as exc:
        if strict:
            raise
        return {
            "ok": False,
            "storage": "sqlite",
            "backend": "sqlite",
            "db_path": str(path),
            "db_exists": bool(path.exists()),
            "error": f"{type(exc).__name__}: {exc}",
            "schema_version": schema_version,
            "expected_schema_version": SCHEMA_VERSION,
            "schema_version_ok": False,
            "schema_status": schema_status,
            "missing_tables": missing_tables,
            "missing_columns": missing_columns,
            "missing_cols": missing_columns,
            "missing_indexes": missing_indexes,
            "owned_schema_ok": False,
        }

    schema_version_ok = (
        schema_version is not None
        and int(schema_version) == int(SCHEMA_VERSION)
        and schema_status in {
            "applied",
            "verified",
            "legacy",
        }
    )
    owned_drift_tables = sorted(
        set(owned_missing_tables)
        | set(owned_missing_columns)
        | set(owned_unexpected_columns)
        | set(owned_pk_mismatches)
        | set(owned_missing_indexes)
    )
    owned_schema_ok = not owned_drift_tables
    quick_check_ok = str(quick_check).lower() in {"ok", "skipped", "not_applicable"}
    ok = (
        not missing_tables
        and not missing_columns
        and not missing_indexes
        and bool(schema_version_ok)
        and bool(owned_schema_ok)
        and bool(quick_check_ok)
    )
    out: dict[str, Any] = {
        "ok": bool(ok),
        "storage": "sqlite",
        "backend": "sqlite",
        "db_path": str(path),
        "db_exists": bool(path.exists()),
        "have_tables": list(have_tables),
        "required_tables": list(_REQUIRED_TABLE_COLUMNS.keys()),
        "required_columns": {table: list(cols) for table, cols in _REQUIRED_TABLE_COLUMNS.items()},
        "required_indexes": list(_REQUIRED_INDEXES),
        "missing_tables": list(missing_tables),
        "missing_columns": dict(missing_columns),
        "missing_cols": dict(missing_columns),
        "missing_indexes": list(missing_indexes),
        "schema_version": (int(schema_version) if schema_version is not None else None),
        "expected_schema_version": SCHEMA_VERSION,
        "schema_version_ok": bool(schema_version_ok),
        "schema_status": str(schema_status),
        "quick_check": str(quick_check),
        "owned_tables": list(__import__("engine.runtime.storage_live_ingestion_schema", fromlist=["OWNED_LIVE_TABLE_COLUMN_SPECS"]).OWNED_LIVE_TABLE_COLUMN_SPECS.keys()),
        "owned_schema_ok": bool(owned_schema_ok),
        "owned_missing_tables": list(owned_missing_tables),
        "owned_missing_columns": dict(owned_missing_columns),
        "owned_unexpected_columns": dict(owned_unexpected_columns),
        "owned_type_mismatches": {},
        "owned_pk_mismatches": dict(owned_pk_mismatches),
        "owned_missing_indexes": dict(owned_missing_indexes),
        "owned_drift_tables": list(owned_drift_tables),
        "ts_ms": int(time.time() * 1000),
    }
    return out


def _backfill_alert_prediction_ids(con: Any | None = None) -> int:
    """Compatibility helper retained for legacy repair callers."""

    owns_connection = con is None
    db = con or connect()
    try:
        if not _table_exists(db, "alerts") or not _table_exists(db, "predictions"):
            return 0
        cursor = db.execute(
            """
            UPDATE alerts
            SET prediction_id = (
                SELECT p.id
                FROM predictions p
                WHERE p.event_id = alerts.event_id
                  AND (p.symbol = alerts.symbol OR p.symbol IS NULL OR alerts.symbol IS NULL)
                  AND (p.horizon_s = alerts.horizon_s OR p.horizon_s IS NULL OR alerts.horizon_s IS NULL)
                ORDER BY p.id DESC
                LIMIT 1
            )
            WHERE prediction_id IS NULL
              AND event_id IS NOT NULL
            """
        )
        if hasattr(db, "commit"):
            db.commit()
        return max(0, int(getattr(cursor, "rowcount", 0) or 0))
    finally:
        if owns_connection:
            db.close()


def get_db_debug_snapshot(*, include_quick_check: bool = True) -> dict[str, Any]:
    return get_db_validation_snapshot(include_quick_check=include_quick_check)


def get_timescale_client():
    from engine.runtime.timescale_client import get_timescale_client as _get_timescale_client

    return _get_timescale_client()


def init_timeseries_storage() -> dict[str, Any]:
    return get_timeseries_storage_snapshot()


def shutdown_timeseries_storage(timeout_s: float | None = None) -> dict[str, Any]:
    del timeout_s
    return get_timeseries_storage_snapshot()


def get_timeseries_storage_snapshot() -> dict[str, Any]:
    return {"enabled": False, "ok": True, "detail": "sqlite_test_storage"}


def get_job_checkpoint(job_name: str) -> dict[str, int]:
    row = fetch_one(
        "SELECT last_event_id, last_event_ts_ms FROM job_checkpoints WHERE job_name=? LIMIT 1",
        (str(job_name),),
    )
    if not row:
        return {"last_event_id": 0, "last_event_ts_ms": 0}
    return {
        "last_event_id": int(row[0] or 0),
        "last_event_ts_ms": int(row[1] or 0),
    }


def put_job_checkpoint(
    job_name: str,
    last_event_id: int,
    last_event_ts_ms: int,
    *,
    con: StorageConnection | None = None,
) -> None:
    now_ms = int(time.time() * 1000)

    def _write(db: StorageConnection) -> None:
        db.execute(
            """
            INSERT INTO job_checkpoints(job_name, last_event_id, last_event_ts_ms, updated_ts_ms)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(job_name) DO UPDATE SET
              last_event_id=excluded.last_event_id,
              last_event_ts_ms=excluded.last_event_ts_ms,
              updated_ts_ms=excluded.updated_ts_ms
            """,
            (str(job_name), int(last_event_id), int(last_event_ts_ms), int(now_ms)),
        )

    if con is not None:
        _write(con)
        return
    run_write_txn(_write, table="job_checkpoints", operation="put_job_checkpoint")


def _json_payload(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except Exception:
        return value


def _insert_dict(
    table: str,
    row: dict[str, Any],
    *,
    returning_id: bool = False,
    con: StorageConnection | None = None,
):
    table_name = _ident(table)
    clean = {str(key): value for key, value in dict(row or {}).items() if value is not None}
    if not clean:
        return 0
    columns = [_ident(column) for column in clean]
    sql = (
        f"INSERT INTO {_quote(table_name)} ({', '.join(_quote(column) for column in columns)}) "
        f"VALUES ({', '.join(['?'] * len(columns))})"
    )
    values = tuple(clean[column] for column in columns)

    def _write(db: StorageConnection):
        cur = db.execute(sql, values)
        if returning_id:
            return int(getattr(cur, "lastrowid", 0) or 0)
        return int(cur.rowcount or 0)

    if con is not None:
        return _write(con)
    return run_write_txn(_write)


def _bounded_limit(value: Any, *, default: int = 100, maximum: int = 10000) -> int:
    try:
        limit = int(value if value is not None else default)
    except Exception:
        limit = int(default)
    return max(1, min(int(maximum), int(limit)))


def _format_plain_row(
    row: Any,
    columns: Sequence[str],
    *,
    json_columns: Sequence[str] = (),
) -> dict[str, Any]:
    json_column_set = {str(column) for column in json_columns}
    out: dict[str, Any] = {}
    for idx, column in enumerate(columns):
        try:
            value = row[column]
        except Exception:
            value = row[idx]
        if str(column) in json_column_set:
            value = _json_payload(value)
        out[str(column)] = value
    return out


def _normalise_model_hparam_row(row: dict[str, Any]) -> dict[str, Any]:
    clean = dict(row or {})
    clean.setdefault("ts", int(time.time() * 1000))
    clean.setdefault("symbol", "GLOBAL")
    clean.setdefault("study_name", "")
    clean.setdefault("tuner", "")
    clean.setdefault("objective", "")
    clean.setdefault("trial_count", 0)
    clean.setdefault("best_trial_number", 0)
    params = clean.get("params")
    params_json = clean.get("params_json")
    if params_json in (None, "") and params not in (None, ""):
        clean["params_json"] = params
    if params in (None, "") and params_json not in (None, ""):
        clean["params"] = params_json
    return clean


def record_model_hyperparameter_registry(con: StorageConnection | None = None, **kwargs: Any) -> int:
    row = _normalise_model_hparam_row(dict(kwargs or {}))

    def _write(db: StorageConnection) -> int:
        registry_id = int(_insert_dict("model_hyperparameter_registry", row, returning_id=True, con=db) or 0)
        params = _json_payload(row.get("params")) or _json_payload(row.get("params_json")) or {}
        if isinstance(params, dict) and row.get("model_family"):
            upsert_model_best_params(
                model_family=str(row.get("model_family") or ""),
                symbol=str(row.get("symbol") or "GLOBAL"),
                study_name=str(row.get("study_name") or ""),
                params_json=dict(params),
                value=float(row.get("metric_value") or 0.0),
                ts=int(row.get("ts") or time.time() * 1000),
                trial_number=(None if row.get("best_trial_number") is None else int(row.get("best_trial_number") or 0)),
                seed=(None if row.get("seed") is None else int(row.get("seed") or 0)),
                con=db,
            )
        return registry_id

    if con is not None:
        return _write(con)
    return int(run_write_txn(_write) or 0)


def upsert_model_best_params(
    *,
    model_family: str,
    symbol: str,
    study_name: str,
    params_json: Any,
    value: float,
    ts: int | None = None,
    trial_number: int | None = None,
    seed: int | None = None,
    con: StorageConnection | None = None,
) -> int:
    row = {
        "model_family": str(model_family or "").strip(),
        "symbol": str(symbol or "global").strip().upper() or "GLOBAL",
        "ts": int(ts if ts is not None else time.time() * 1000),
        "study_name": str(study_name or "").strip(),
        "params_json": params_json or {},
        "value": float(value),
        "trial_number": None if trial_number is None else int(trial_number),
        "seed": None if seed is None else int(seed),
    }

    def _write(db: StorageConnection) -> int:
        db.execute(
            """
            INSERT INTO model_best_params(
              model_family, symbol, ts, study_name, params_json, value, trial_number, seed
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(model_family, symbol) DO UPDATE SET
              ts=excluded.ts,
              study_name=excluded.study_name,
              params_json=excluded.params_json,
              value=excluded.value,
              trial_number=excluded.trial_number,
              seed=excluded.seed
            """,
            (
                row["model_family"],
                row["symbol"],
                row["ts"],
                row["study_name"],
                row["params_json"],
                row["value"],
                row["trial_number"],
                row["seed"],
            ),
        )
        return 1

    if con is not None:
        return int(_write(con) or 0)
    return int(run_write_txn(_write) or 0)


def fetch_model_best_params(
    *,
    model_family: str,
    symbol: str = "GLOBAL",
    con: StorageConnection | None = None,
) -> dict[str, Any] | None:
    owns = con is None
    db = con or connect(readonly=True)
    try:
        family = str(model_family or "").strip()
        sym = str(symbol or "GLOBAL").strip().upper() or "GLOBAL"
        columns = ("model_family", "symbol", "ts", "study_name", "params_json", "value", "trial_number", "seed")
        row = db.execute(
            f"""
            SELECT {', '.join(columns)}
            FROM model_best_params
            WHERE model_family=? AND symbol=?
            LIMIT 1
            """,
            (family, sym),
        ).fetchone()
        if row is None and sym != "GLOBAL":
            row = db.execute(
                f"""
                SELECT {', '.join(columns)}
                FROM model_best_params
                WHERE model_family=? AND symbol='GLOBAL'
                LIMIT 1
                """,
                (family,),
            ).fetchone()
        if not row:
            return None
        out = _format_plain_row(row, columns, json_columns=("params_json",))
        out["params"] = dict(out.get("params_json") or {})
        return out
    finally:
        if owns:
            db.close()


def fetch_latest_model_hyperparameters(*args: Any, **kwargs: Any):
    if args:
        kwargs.setdefault("model_family", args[0])
    model_family = str(kwargs.get("model_family") or "").strip()
    model_name = str(kwargs.get("model_name") or "").strip()
    if not model_family and model_name:
        model_family = model_name.split(":", 1)[0]
    if not model_family:
        return None

    owns = kwargs.get("con") is None
    db = kwargs.get("con") or connect(readonly=True)
    try:
        if _table_exists(db, "model_hyperparameter_registry"):
            available = _table_columns(db, "model_hyperparameter_registry")
            desired = (
                "id",
                "ts",
                "model_family",
                "model_name",
                "symbol",
                "tuner",
                "objective",
                "study_name",
                "params",
                "params_json",
                "metric_value",
                "trial_count",
                "best_trial_number",
                "seed",
                "cpcv_mean_sharpe",
                "cpcv_median_sharpe",
                "cpcv_pbo",
                "diagnostics",
            )
            columns = tuple(column for column in desired if column in available)
            if columns:
                filters = ["model_family=?"]
                params: list[Any] = [model_family]
                if model_name and "model_name" in available:
                    filters.append("model_name=?")
                    params.append(model_name)
                tuner = kwargs.get("tuner")
                if tuner not in (None, "") and "tuner" in available:
                    filters.append("tuner=?")
                    params.append(str(tuner))
                symbol = kwargs.get("symbol")
                if symbol not in (None, "") and "symbol" in available:
                    filters.append("symbol=?")
                    params.append(str(symbol).strip().upper() or "GLOBAL")
                row = db.execute(
                    f"""
                    SELECT {', '.join(_quote(column) for column in columns)}
                    FROM model_hyperparameter_registry
                    WHERE {' AND '.join(filters)}
                    ORDER BY ts DESC, id DESC
                    LIMIT 1
                    """,
                    tuple(params),
                ).fetchone()
                if row:
                    out = _format_plain_row(row, columns, json_columns=("params", "params_json", "diagnostics"))
                    params_value = out.get("params")
                    if not isinstance(params_value, dict):
                        params_value = out.get("params_json")
                    out["params"] = dict(params_value or {})
                    if "params_json" in out and not isinstance(out.get("params_json"), dict):
                        out["params_json"] = dict(out.get("params") or {})
                    return out
        symbol = str(kwargs.get("symbol") or "GLOBAL")
        return fetch_model_best_params(model_family=model_family, symbol=symbol, con=db)
    finally:
        if owns:
            db.close()


_CLONE_NAMES = [
    "_upsert_dict",
    "put_event",
    "put_normalized_event",
    "put_price",
    "_payload_writer",
    "_alt_data_row",
    "_alt_data_upsert",
    "put_news_event_feature",
    "_payload_dict",
    "put_finbert_sentiment_enrichment",
    "put_news_symbol_feature",
    "put_options_event_feature",
    "put_insider_transaction",
    "put_congressional_trade",
    "put_finra_short_sale_volume",
    "put_finra_short_interest",
    "put_crypto_funding_rate",
    "load_finbert_sentiment_enrichment_for_event",
    "load_latest_finbert_sentiment_enrichment",
    "record_prediction_explanation",
    "fetch_prediction_explanations",
    "fetch_latest_prediction_explanation",
    "log_alert_interaction",
    "log_decision_view",
    "record_hypothesis_result",
    "record_backtest_cpcv_run",
    "record_backtest_cpcv_path_result",
    "record_alpha_candidate",
    "update_alpha_candidate",
    "record_alpha_lifecycle",
    "record_drift_retrain_event",
    "_empty_recent",
    "fetch_recent_hypothesis_registry",
    "fetch_recent_alpha_candidates",
    "fetch_alpha_lifecycle",
    "fetch_recent_drift_retrain_events",
    "fetch_recent_backtest_cpcv_runs",
    "fetch_recent_audit_records",
    "fetch_audit_record",
    "fetch_recent_promotion_statistical_evidence",
    "fetch_recent_decisions",
    "fetch_latest_backtest_cpcv_run",
    "fetch_latest_drift_retrain_event",
    "fetch_decision_detail",
    "_fetch_audit_records",
    "_with_read_connection",
    "_backfill_alert_prediction_ids",
    "_audit_table_name",
    "_relation_exists_compat",
    "_table_column_metadata",
    "_format_audit_record",
    "_row_to_dict",
    "_hash_hex",
    "_json_read_value",
    "_json_safe_value",
    "fetch_human_alignment_report",
    "acquire_job_lock",
    "release_job_lock",
    "touch_job_lock",
    "put_job_heartbeat",
    "flush_job_liveness_queue",
    "shutdown_job_liveness_queue",
    "_job_liveness_queue_snapshot",
    "_warn_nonfatal",
    "_warn_nonfatal_once",
    "_ensure_price_quotes_schema",
    "_ensure_price_quotes_raw_schema",
]


_PG_HELPERS_LOCK = threading.RLock()
_PG_HELPERS_CLONED = False
_PG_HELPERS_ERROR: str | None = None


def _clone_pg_helpers() -> bool:
    global _PG_HELPERS_CLONED, _PG_HELPERS_ERROR
    if _PG_HELPERS_CLONED:
        return True
    with _PG_HELPERS_LOCK:
        if _PG_HELPERS_CLONED:
            return True
        try:
            from engine.runtime import storage_pg as _pg
        except Exception as exc:
            _PG_HELPERS_ERROR = f"{type(exc).__name__}: {exc}"
            return False

        for name in (
            "_INSIDER_TRANSACTION_COLUMNS",
            "_CONGRESSIONAL_TRADE_COLUMNS",
            "_FINRA_SHORT_SALE_VOLUME_COLUMNS",
            "_FINRA_SHORT_INTEREST_COLUMNS",
            "_CRYPTO_FUNDING_RATE_COLUMNS",
        ):
            globals()[name] = getattr(_pg, name)

        for name in _CLONE_NAMES:
            source = getattr(_pg, name)
            if not isinstance(source, FunctionType):
                globals()[name] = source
                continue
            cloned = FunctionType(
                source.__code__,
                globals(),
                name,
                source.__defaults__,
                source.__closure__,
            )
            cloned.__kwdefaults__ = source.__kwdefaults__
            cloned.__doc__ = source.__doc__
            globals()[name] = cloned
        _PG_HELPERS_ERROR = None
        _PG_HELPERS_CLONED = True
        return True


def _make_lazy_pg_helper(name: str):
    def _lazy_pg_helper(*args: Any, **kwargs: Any):
        if not _clone_pg_helpers():
            detail = _PG_HELPERS_ERROR or "storage_pg_helpers_unavailable"
            raise RuntimeError(f"sqlite_storage_helper_requires_storage_pg:{name}:{detail}")
        target = globals().get(name)
        if target is _lazy_pg_helper:
            raise RuntimeError(f"sqlite_storage_helper_unresolved:{name}")
        return target(*args, **kwargs)

    _lazy_pg_helper.__name__ = name
    _lazy_pg_helper.__qualname__ = name
    _lazy_pg_helper.__doc__ = "Lazy SQLite helper cloned from storage_pg when Postgres dependencies are installed."
    return _lazy_pg_helper


for _helper_name in _CLONE_NAMES:
    globals().setdefault(_helper_name, _make_lazy_pg_helper(_helper_name))
del _helper_name


def __getattr__(name: str):
    if name.startswith("_ensure_") and name.endswith("_schema"):
        def _ensure(*args: Any, **kwargs: Any) -> None:
            del args, kwargs
            init_db()

        return _ensure
    raise AttributeError(name)


__all__ = [name for name in globals() if not name.startswith("__")]
