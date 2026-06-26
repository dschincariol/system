"""Thread-safe Timescale/Postgres storage for price-related tables only."""

from __future__ import annotations

import logging
import math
import os
import random
import threading
import time
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, cast

try:
    import psycopg
    from psycopg_pool import ConnectionPool
except Exception:  # pragma: no cover - optional dependency at runtime
    psycopg = None  # type: ignore[assignment]
    ConnectionPool = None  # type: ignore[assignment]

from engine.data.price_event_keys import compute_price_raw_event_key
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.ingestion_tuning import env_bool, tuned_float, tuned_int
from engine.runtime.logging import get_logger
from engine.runtime.metrics import emit_counter, emit_gauge, emit_timing
from engine.runtime.observability import record_component_health
from engine.runtime.pg_connection_hygiene import rollback_if_in_transaction
from engine.runtime.platform import connection_info_with_pg_password
from engine.runtime.price_timescale_schema import (
    PRICE_TIMESCALE_COPY_TYPES,
    PRICE_TIMESCALE_SCHEMA_INDEXES,
    PRICE_TIMESCALE_STAGING_TABLE_COLUMNS,
    PRICE_TIMESCALE_STAGING_TABLE_COLUMN_SPECS,
    PRICE_TIMESCALE_STAGING_TABLE_NAMES,
    PRICE_TIMESCALE_TABLES,
    PRICE_TIMESCALE_TABLE_COLUMNS,
    price_timescale_create_table_sql,
    price_timescale_staging_table_ddl,
    price_timescale_time_desc_index_sql,
)
from engine.runtime.pg_durability import (
    maybe_apply_sync_refetchable_pg_durability,
    refetchable_pg_durability_snapshot,
)
from engine.runtime.schema.table_classification import hypertable_chunk_interval, hypertable_chunk_interval_ms

LOG = get_logger("runtime.storage_pg_prices")
_STORE_LOCK = threading.Lock()
_STORE: "PostgresPriceStorage | None" = None
_PG_PRICE_SCHEMA_TABLE_COLUMNS: dict[str, tuple[str, ...]] = dict(PRICE_TIMESCALE_TABLE_COLUMNS)
_PG_PRICE_HYPERTABLE_TABLES: tuple[str, ...] = PRICE_TIMESCALE_TABLES
_PG_PRICE_HYPERTABLE_TIME_COLUMNS: dict[str, str] = {
    table_name: "time" for table_name in _PG_PRICE_HYPERTABLE_TABLES
}
_PG_PRICE_STAGING_TABLE_NAMES: dict[str, str] = dict(PRICE_TIMESCALE_STAGING_TABLE_NAMES)
_PG_PRICE_STAGING_TABLE_COLUMN_SPECS: dict[str, tuple[tuple[str, str], ...]] = dict(
    PRICE_TIMESCALE_STAGING_TABLE_COLUMN_SPECS
)
_PG_PRICE_STAGING_TABLE_COLUMNS: dict[str, tuple[str, ...]] = dict(PRICE_TIMESCALE_STAGING_TABLE_COLUMNS)
_PG_PRICE_SCHEMA_INDEXES: tuple[str, ...] = PRICE_TIMESCALE_SCHEMA_INDEXES
_PG_MAX_BIND_PARAMS = 65_535
_PG_VALUES_UPSERT_PAGE_SIZE = 1_000
_PRICE_TICKS_CONFLICT_KEY_INDEXES = (1, 0)
_PRICE_QUOTES_CONFLICT_KEY_INDEXES = (1, 0)
_PRICE_QUOTES_RAW_CONFLICT_KEY_INDEXES = (1, 2, 3, 0)
_PG_PRICE_COPY_TYPES: dict[str, tuple[str, ...]] = dict(PRICE_TIMESCALE_COPY_TYPES)


def _env_bool(name: str, default: bool = False) -> bool:
    return env_bool(name, default=default)


def _env_float(name: str, default: float) -> float:
    return tuned_float(name, default, 0.0, float("inf"))


def _env_int(name: str, default: int) -> int:
    return tuned_int(name, default, 0, 2**31 - 1)


def _quote_ident(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _price_chunk_policy_status() -> dict[str, dict[str, Any]]:
    return {
        table_name: {
            "desired_interval": hypertable_chunk_interval(table_name),
            "desired_interval_ms": int(hypertable_chunk_interval_ms(table_name)),
            "actual_interval": "",
            "actual_interval_ms": None,
        }
        for table_name in _PG_PRICE_HYPERTABLE_TABLES
    }


def _column_list_sql(columns: Iterable[str]) -> str:
    return ", ".join(_quote_ident(str(column)) for column in columns)


def _session_timeout_ms(timeout_s: Any) -> int:
    try:
        seconds = float(timeout_s)
    except (TypeError, ValueError):
        seconds = 1.0
    if not math.isfinite(seconds) or seconds < 1.0:
        seconds = 1.0
    return int(seconds * 1000)


def _staging_table_name(table_name: str) -> str:
    try:
        return _PG_PRICE_STAGING_TABLE_NAMES[str(table_name)]
    except KeyError as exc:
        raise ValueError(f"unknown_price_staging_table:{table_name}") from exc


def _staging_relation_ref(schema_ref: str, table_name: str) -> str:
    return f"{schema_ref}.{_quote_ident(_staging_table_name(table_name))}"


def _staging_index_name(table_name: str) -> str:
    return f"idx_{_staging_table_name(table_name)}_session"


def _staging_table_ddl(schema_ref: str, table_name: str) -> str:
    return price_timescale_staging_table_ddl(schema_ref, table_name)


def _staging_session_token() -> str:
    return f"{os.getpid()}:{threading.get_ident()}:{time.time_ns()}:{random.getrandbits(64)}"


class _CopyUnavailableError(RuntimeError):
    """Raised when the active DB adapter/proxy cannot provide psycopg binary COPY."""


class _PriceWriteCircuitOpen(RuntimeError):
    """Raised when price writes are intentionally backpressured."""


@dataclass(frozen=True)
class _FailureClassification:
    retryable: bool
    reset_pool: bool
    failure_class: str
    reason: str


@dataclass(frozen=True)
class _NormalizedPriceWriteRows:
    price_rows: list[tuple[Any, ...]]
    quote_rows: list[tuple[Any, ...]]
    raw_rows: list[tuple[Any, ...]]
    input_prices: int
    input_quotes: int
    input_raw: int
    row_copy_avoided_rows: int
    row_copy_fallback_rows: int
    safe_float_calls: int
    safe_int_calls: int
    datetime_conversions: int
    symbol_parses: int
    event_key_normalizations: int

    @property
    def dropped_rows(self) -> dict[str, int]:
        return {
            "prices": max(0, int(self.input_prices) - len(self.price_rows)),
            "quotes": max(0, int(self.input_quotes) - len(self.quote_rows)),
            "raw": max(0, int(self.input_raw) - len(self.raw_rows)),
        }

    @property
    def input_rows(self) -> int:
        return int(self.input_prices) + int(self.input_quotes) + int(self.input_raw)


@dataclass
class _PriceWriteNormalizationStats:
    safe_float_calls: int = 0
    safe_int_calls: int = 0
    datetime_conversions: int = 0
    symbol_parses: int = 0
    event_key_normalizations: int = 0


_RETRYABLE_ERROR_NAMES = {
    "AdminShutdown",
    "CannotConnectNow",
    "ConnectionException",
    "DeadlockDetected",
    "LockNotAvailable",
    "OperationalError",
    "PoolTimeout",
    "QueryCanceled",
    "SerializationFailure",
    "TooManyConnections",
}
_POOL_RESET_ERROR_NAMES = {
    "AdminShutdown",
    "CannotConnectNow",
    "ConnectionException",
    "ConnectionFailure",
    "ConnectionDoesNotExist",
    "InterfaceError",
    "OperationalError",
    "PoolTimeout",
}
_FATAL_ERROR_NAMES = {
    "AttributeError",
    "CheckViolation",
    "DataError",
    "DatatypeMismatch",
    "FeatureNotSupported",
    "ForeignKeyViolation",
    "IntegrityError",
    "InvalidTextRepresentation",
    "NotNullViolation",
    "ProgrammingError",
    "SyntaxError",
    "TypeError",
    "UndefinedColumn",
    "UndefinedTable",
    "UniqueViolation",
    "ValueError",
}
_RETRYABLE_SQLSTATES = {"40001", "40P01", "55P03", "57014", "57P01", "57P02", "57P03"}
_RETRYABLE_SQLSTATE_PREFIXES = ("08", "53")
_FATAL_SQLSTATE_PREFIXES = ("0A", "22", "23", "42")


def _is_copy_unavailable_exception(error: BaseException) -> bool:
    error_name = type(error).__name__
    if error_name in {"AttributeError", "NotImplementedError", "NotSupportedError"}:
        return True
    message = str(error).lower()
    return "copy" in message and ("not support" in message or "unavailable" in message)


def _exception_chain(error: BaseException) -> tuple[BaseException, ...]:
    chain: list[BaseException] = []
    current: BaseException | None = error
    seen_ids: set[int] = set()
    while current is not None and id(current) not in seen_ids:
        seen_ids.add(id(current))
        chain.append(current)
        next_error = current.__cause__ or current.__context__
        current = next_error if isinstance(next_error, BaseException) else None
    return tuple(chain)


def _exception_names(error: BaseException) -> set[str]:
    return {type(item).__name__ for item in _exception_chain(error)}


def _exception_sqlstate(error: BaseException) -> str:
    for item in _exception_chain(error):
        for attr_name in ("sqlstate", "pgcode"):
            value = getattr(item, attr_name, None)
            if value:
                return str(value).strip().upper()
    return ""


def _classify_pg_price_failure(error: BaseException) -> _FailureClassification:
    names = _exception_names(error)
    sqlstate = _exception_sqlstate(error)
    text = " ".join(str(item) for item in _exception_chain(error)).lower()
    if isinstance(error, _PriceWriteCircuitOpen):
        return _FailureClassification(
            retryable=True,
            reset_pool=False,
            failure_class="circuit_open",
            reason="write_circuit_open",
        )
    if any(name in _FATAL_ERROR_NAMES for name in names) or "storage_pg_prices_copy_unavailable" in text:
        return _FailureClassification(
            retryable=False,
            reset_pool=False,
            failure_class="fatal",
            reason=next((name for name in sorted(names) if name in _FATAL_ERROR_NAMES), type(error).__name__),
        )
    if sqlstate and (
        sqlstate in _RETRYABLE_SQLSTATES
        or any(sqlstate.startswith(prefix) for prefix in _RETRYABLE_SQLSTATE_PREFIXES)
    ):
        return _FailureClassification(
            retryable=True,
            reset_pool=sqlstate.startswith("08") or sqlstate in {"57P01", "57P02", "57P03"},
            failure_class="retryable",
            reason=f"sqlstate:{sqlstate}",
        )
    if sqlstate and any(sqlstate.startswith(prefix) for prefix in _FATAL_SQLSTATE_PREFIXES):
        return _FailureClassification(
            retryable=False,
            reset_pool=False,
            failure_class="fatal",
            reason=f"sqlstate:{sqlstate}",
        )
    retryable_text = (
        "could not connect",
        "couldn't get a connection",
        "connection pool unavailable",
        "connection refused",
        "deadlock detected",
        "lock timeout",
        "query canceled",
        "server closed the connection",
        "statement timeout",
        "temporarily unavailable",
        "timeout",
        "too many connections",
    )
    reset_text = (
        "broken pipe",
        "could not connect",
        "couldn't get a connection",
        "connection already closed",
        "connection pool unavailable",
        "connection refused",
        "connection reset",
        "server closed the connection",
    )
    retryable = any(name in _RETRYABLE_ERROR_NAMES for name in names) or any(
        fragment in text for fragment in retryable_text
    )
    if retryable:
        return _FailureClassification(
            retryable=True,
            reset_pool=any(name in _POOL_RESET_ERROR_NAMES for name in names)
            or any(fragment in text for fragment in reset_text),
            failure_class="retryable",
            reason=next((name for name in sorted(names) if name in _RETRYABLE_ERROR_NAMES), type(error).__name__),
        )
    return _FailureClassification(
        retryable=False,
        reset_pool=False,
        failure_class="fatal",
        reason=type(error).__name__,
    )


def _dedupe_rows_by_conflict_key(
    rows: list[tuple[Any, ...]],
    conflict_key_indexes: tuple[int, ...],
) -> list[tuple[Any, ...]]:
    keyed_rows: dict[tuple[Any, ...], tuple[Any, ...]] = {}
    key_order: list[tuple[Any, ...]] = []
    for row in rows:
        key = tuple(row[index] for index in conflict_key_indexes)
        if key not in keyed_rows:
            key_order.append(key)
        keyed_rows[key] = row
    if len(key_order) == len(rows):
        return rows
    return [keyed_rows[key] for key in key_order]


def _execute_many_values(
    cur: Any,
    sql: str,
    rows: Iterable[tuple[Any, ...]],
    *,
    conflict_key_indexes: tuple[int, ...],
    page_size: int = _PG_VALUES_UPSERT_PAGE_SIZE,
    max_bind_params: int = _PG_MAX_BIND_PARAMS,
) -> None:
    batch = [tuple(row) for row in rows]
    if not batch:
        return
    row_arity = len(batch[0])
    if row_arity <= 0:
        raise ValueError("batch_values_row_arity_empty")
    if any(len(row) != row_arity for row in batch):
        raise ValueError("batch_values_row_arity_mismatch")
    if not conflict_key_indexes:
        raise ValueError("batch_values_conflict_key_missing")
    normalized_key_indexes = tuple(int(index) for index in conflict_key_indexes)
    if any(index < 0 or index >= row_arity for index in normalized_key_indexes):
        raise ValueError("batch_values_conflict_key_out_of_range")
    placeholders = "(" + ", ".join("%s" for _ in range(len(batch[0]))) + ")"
    base_sql = str(sql)
    if "VALUES %s" not in base_sql:
        raise ValueError("batch_values_placeholder_missing")
    max_rows_by_params = int(max_bind_params) // int(row_arity)
    if max_rows_by_params <= 0:
        raise ValueError("batch_values_parameter_limit_too_low")
    rows_per_page = max(1, min(int(page_size), int(max_rows_by_params)))
    for offset in range(0, len(batch), rows_per_page):
        page = _dedupe_rows_by_conflict_key(
            batch[offset : offset + rows_per_page],
            normalized_key_indexes,
        )
        values_sql = ", ".join(placeholders for _ in page)
        rendered_sql = base_sql.replace("VALUES %s", f"VALUES {values_sql}", 1)
        params = tuple(value for row in page for value in row)
        if len(params) > int(max_bind_params):
            raise ValueError("batch_values_parameter_limit_exceeded")
        cur.execute(rendered_sql, params)


def _pg_price_compress_orderby(table_name: str) -> str:
    time_column = _PG_PRICE_HYPERTABLE_TIME_COLUMNS.get(str(table_name), "time")
    return f'{_quote_ident(time_column)} DESC'


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        out = float(value)
    except Exception:
        return None
    if not math.isfinite(out):
        return None
    return float(out)


def _safe_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except Exception:
        return None


def _normalize_symbol(value: Any) -> str:
    return str(value or "").strip().upper()


def _dt_from_ms(value: Any) -> datetime | None:
    ts_ms = _safe_int(value)
    if ts_ms is None or ts_ms <= 0:
        return None
    return datetime.fromtimestamp(float(ts_ms) / 1000.0, tz=timezone.utc)


def _dt_from_normalized_ms(ts_ms: int | None) -> datetime | None:
    if ts_ms is None or ts_ms <= 0:
        return None
    return datetime.fromtimestamp(float(ts_ms) / 1000.0, tz=timezone.utc)


_MISSING: Any = object()


def _normalize_event_key(
    row: Mapping[str, Any],
    *,
    provider: Any = _MISSING,
    symbol: Any = _MISSING,
    event_type: Any = _MISSING,
    event_ts_ms: Any = _MISSING,
    ts_ms: Any = _MISSING,
) -> str:
    raw = str(row.get("event_key") or "").strip()
    if raw:
        return raw
    return compute_price_raw_event_key(
        row,
        provider=(row.get("provider") or row.get("source")) if provider is _MISSING else provider,
        symbol=row.get("symbol") if symbol is _MISSING else symbol,
        event_type=(row.get("event_type") or "U") if event_type is _MISSING else event_type,
        event_ts_ms=(
            row.get("event_ts_ms") or row.get("ts_ms") or row.get("timestamp")
            if event_ts_ms is _MISSING
            else event_ts_ms
        ),
        ts_ms=(row.get("ts_ms") or row.get("timestamp")) if ts_ms is _MISSING else ts_ms,
    )


def _row_mapping(row: Any) -> tuple[Mapping[str, Any], bool]:
    if isinstance(row, Mapping):
        return row, False
    return dict(row or {}), True


class _NormalizedPriceInputRow:
    """Cache row-level conversions shared by all target-table builders."""

    __slots__ = (
        "row",
        "stats",
        "symbol",
        "_event_key",
        "_float_cache",
        "_int_cache",
        "_time",
    )

    def __init__(self, row: Mapping[str, Any], stats: _PriceWriteNormalizationStats):
        self.row = row
        self.stats = stats
        stats.symbol_parses += 1
        self.symbol = _normalize_symbol(row.get("symbol"))
        self._event_key: Any = _MISSING
        self._float_cache: dict[str, float | None] = {}
        self._int_cache: dict[str, int | None] = {}
        self._time: Any = _MISSING

    def raw_or_default(self, *names: str, default: Any = "") -> Any:
        for name in names:
            value = self.row.get(name)
            if value:
                return value
        return default

    def raw_or_expression(self, *names: str) -> tuple[str | None, Any]:
        for name in names:
            value = self.row.get(name)
            if value:
                return name, value
        if not names:
            return None, None
        last_name = names[-1]
        return last_name, self.row.get(last_name)

    def float_field(self, name: str) -> float | None:
        key = str(name)
        if key not in self._float_cache:
            self.stats.safe_float_calls += 1
            self._float_cache[key] = _safe_float(self.row.get(key))
        return self._float_cache[key]

    def int_field(self, name: str) -> int | None:
        key = str(name)
        if key not in self._int_cache:
            self.stats.safe_int_calls += 1
            self._int_cache[key] = _safe_int(self.row.get(key))
        return self._int_cache[key]

    def int_or(self, *names: str) -> int | None:
        name, value = self.raw_or_expression(*names)
        if name is None or value is None:
            return None
        return self.int_field(name)

    @property
    def time(self) -> datetime | None:
        if self._time is _MISSING:
            self.stats.datetime_conversions += 1
            self._time = _dt_from_normalized_ms(self.int_or("ts_ms", "timestamp"))
        return self._time

    @property
    def price_last(self) -> float | None:
        price = self.row.get("price")
        if price not in (None, ""):
            return self.float_field("price")
        return self.float_field("last")

    @property
    def event_key(self) -> str:
        if self._event_key is _MISSING:
            self.stats.event_key_normalizations += 1
            _event_ts_name, event_ts_value = self.raw_or_expression("event_ts_ms", "ts_ms", "timestamp")
            _ts_name, ts_value = self.raw_or_expression("ts_ms", "timestamp")
            self._event_key = _normalize_event_key(
                self.row,
                provider=self.raw_or_default("provider", "source"),
                symbol=self.symbol,
                event_type=self.raw_or_default("event_type", default="U"),
                event_ts_ms=event_ts_value,
                ts_ms=ts_value,
            )
        return str(self._event_key)


def _build_price_tick_row(row: _NormalizedPriceInputRow) -> tuple[Any, ...] | None:
    if not row.symbol or row.time is None:
        return None
    return (
        row.time,
        row.symbol,
        row.price_last,
        str(row.raw_or_default("source", "provider")),
        str(row.raw_or_default("provider", "source")),
        row.float_field("bid"),
        row.float_field("ask"),
        row.float_field("spread"),
        row.float_field("volume"),
        row.int_field("latency_ms"),
        row.float_field("provider_score"),
        row.int_field("last_update_ts_ms"),
        row.int_field("ingest_ts_ms"),
    )


def _build_price_quote_row(row: _NormalizedPriceInputRow) -> tuple[Any, ...] | None:
    if not row.symbol or row.time is None:
        return None
    return (
        row.time,
        row.symbol,
        row.float_field("last"),
        row.float_field("bid"),
        row.float_field("ask"),
        row.float_field("spread"),
        row.float_field("volume"),
        str(row.raw_or_default("source", "provider")),
        row.int_or("last_trade_ts_ms", "trade_ts_ms"),
        row.int_or("last_quote_ts_ms", "quote_ts_ms"),
        row.int_field("last_update_ts_ms"),
    )


def _build_price_raw_row(row: _NormalizedPriceInputRow) -> tuple[Any, ...] | None:
    provider = str(row.raw_or_default("provider", "source")).strip()
    if not row.symbol or not provider or row.time is None:
        return None
    return (
        row.time,
        row.symbol,
        str(row.raw_or_default("provider", "source")),
        row.event_key,
        str(row.raw_or_default("event_type")),
        row.int_or("event_ts_ms", "timestamp"),
        row.float_field("last"),
        row.float_field("bid"),
        row.float_field("ask"),
        row.float_field("spread"),
        row.float_field("volume"),
        row.int_field("trade_ts_ms"),
        row.int_field("quote_ts_ms"),
        row.int_field("ingest_ts_ms"),
        str(row.raw_or_default("source", "provider")),
    )


def _normalize_price_write_rows(
    *,
    prices: Iterable[Mapping[str, Any]] = (),
    quotes: Iterable[Mapping[str, Any]] = (),
    raw: Iterable[Mapping[str, Any]] = (),
) -> _NormalizedPriceWriteRows:
    price_rows: list[tuple[Any, ...]] = []
    quote_rows: list[tuple[Any, ...]] = []
    raw_rows: list[tuple[Any, ...]] = []
    input_prices = 0
    input_quotes = 0
    input_raw = 0
    row_copy_avoided_rows = 0
    row_copy_fallback_rows = 0
    stats = _PriceWriteNormalizationStats()
    normalized_by_identity: dict[int, _NormalizedPriceInputRow] = {}

    def _normalized_row(raw_row: Mapping[str, Any]) -> tuple[_NormalizedPriceInputRow, bool]:
        row, copied = _row_mapping(raw_row)
        cached = normalized_by_identity.get(id(row))
        if cached is None:
            cached = _NormalizedPriceInputRow(row, stats)
            normalized_by_identity[id(row)] = cached
        return cached, copied

    for raw_row in prices or ():
        input_prices += 1
        row, copied = _normalized_row(raw_row)
        if copied:
            row_copy_fallback_rows += 1
        else:
            row_copy_avoided_rows += 1
        price_row = _build_price_tick_row(row)
        if price_row is not None:
            price_rows.append(price_row)

    for raw_row in quotes or ():
        input_quotes += 1
        row, copied = _normalized_row(raw_row)
        if copied:
            row_copy_fallback_rows += 1
        else:
            row_copy_avoided_rows += 1
        quote_row = _build_price_quote_row(row)
        if quote_row is not None:
            quote_rows.append(quote_row)

    for raw_row in raw or ():
        input_raw += 1
        row, copied = _normalized_row(raw_row)
        if copied:
            row_copy_fallback_rows += 1
        else:
            row_copy_avoided_rows += 1
        raw_row_tuple = _build_price_raw_row(row)
        if raw_row_tuple is not None:
            raw_rows.append(raw_row_tuple)

    return _NormalizedPriceWriteRows(
        price_rows=price_rows,
        quote_rows=quote_rows,
        raw_rows=raw_rows,
        input_prices=int(input_prices),
        input_quotes=int(input_quotes),
        input_raw=int(input_raw),
        row_copy_avoided_rows=int(row_copy_avoided_rows),
        row_copy_fallback_rows=int(row_copy_fallback_rows),
        safe_float_calls=int(stats.safe_float_calls),
        safe_int_calls=int(stats.safe_int_calls),
        datetime_conversions=int(stats.datetime_conversions),
        symbol_parses=int(stats.symbol_parses),
        event_key_normalizations=int(stats.event_key_normalizations),
    )


def _warn_nonfatal(code: str, error: BaseException, **extra: Any) -> None:
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.runtime.storage_pg_prices",
        extra=dict(extra or {}) or None,
        persist=False,
    )


def _check_price_storage_connection(con: Any) -> None:
    rollback_if_in_transaction(
        con,
        logger=LOG,
        context="storage_pg_prices_pool_check",
    )
    check_connection = getattr(ConnectionPool, "check_connection", None)
    if callable(check_connection):
        check_connection(con)


def _reset_price_storage_connection(con: Any) -> None:
    rollback_if_in_transaction(
        con,
        logger=LOG,
        context="storage_pg_prices_pool_reset",
    )


@dataclass(frozen=True)
class PostgresPriceStorageConfig:
    """Configure the optional Postgres or Timescale price-storage sidecar."""

    enabled: bool
    dsn: str
    schema_name: str
    pool_min_size: int
    pool_max_size: int
    connect_timeout_s: float
    lock_timeout_s: float
    command_timeout_s: float
    idle_in_txn_timeout_s: float
    retry_attempts: int
    retry_base_s: float
    retry_max_s: float
    application_name: str
    retention_days: int = 0
    compression_after_days: int = 0
    copy_enabled: bool = True
    copy_fallback_enabled: bool = True
    circuit_failure_threshold: int = 3
    circuit_open_s: float = 5.0

    @classmethod
    def from_env(cls) -> "PostgresPriceStorageConfig":
        """Build the price-storage configuration from environment variables."""
        dsn = str(
            os.environ.get("TIMESCALE_PRICES_DSN")
            or os.environ.get("TIMESCALE_DSN")
            or os.environ.get("TIMESCALE_URL")
            or os.environ.get("TIMESCALE_DATABASE_URL")
            or ""
        ).strip()
        if dsn:
            dsn = connection_info_with_pg_password(dsn)
        enabled = _env_bool("TIMESCALE_PRICES_ENABLED", default=bool(dsn))
        pool_min_size = tuned_int("TIMESCALE_PRICES_POOL_MIN_SIZE", 1, 1, 16)
        pool_max_size = max(pool_min_size, tuned_int("TIMESCALE_PRICES_POOL_MAX_SIZE", 4, 1, 16))
        async_enabled = env_bool("ASYNC_PRICE_WRITER_ENABLED", default=bool(enabled))
        async_workers = tuned_int("ASYNC_PRICE_WRITER_WORKERS", 4, 1, 16)
        if bool(enabled) and bool(async_enabled) and int(pool_max_size) < int(async_workers):
            raise RuntimeError(
                "timescale_prices_pool_too_small_for_async_writer:"
                f"TIMESCALE_PRICES_POOL_MAX_SIZE={int(pool_max_size)};"
                f"ASYNC_PRICE_WRITER_WORKERS={int(async_workers)}"
            )
        return cls(
            enabled=bool(enabled),
            dsn=dsn,
            schema_name=str(os.environ.get("TIMESCALE_PRICES_SCHEMA") or os.environ.get("TIMESCALE_SCHEMA") or "public").strip() or "public",
            pool_min_size=int(pool_min_size),
            pool_max_size=int(pool_max_size),
            connect_timeout_s=tuned_float("TIMESCALE_PRICES_CONNECT_TIMEOUT_S", 5.0, 0.1, 30.0),
            lock_timeout_s=tuned_float("TIMESCALE_PRICES_LOCK_TIMEOUT_S", 5.0, 0.05, 30.0),
            command_timeout_s=tuned_float("TIMESCALE_PRICES_COMMAND_TIMEOUT_S", 30.0, 1.0, 120.0),
            idle_in_txn_timeout_s=tuned_float("TIMESCALE_PRICES_IDLE_IN_TXN_TIMEOUT_S", 60.0, 1.0, 300.0),
            retry_attempts=tuned_int("TIMESCALE_PRICES_RETRY_ATTEMPTS", 3, 1, 10),
            retry_base_s=tuned_float("TIMESCALE_PRICES_RETRY_BASE_S", 0.25, 0.01, 5.0),
            retry_max_s=tuned_float("TIMESCALE_PRICES_RETRY_MAX_S", 5.0, 0.1, 30.0),
            application_name=str(os.environ.get("TIMESCALE_PRICES_APPLICATION_NAME") or "trading-system-price-storage").strip() or "trading-system-price-storage",
            retention_days=max(0, _env_int("TIMESCALE_PRICES_RETENTION_DAYS", _env_int("TIMESCALE_RETENTION_DAYS", 0))),
            compression_after_days=max(
                0,
                _env_int("TIMESCALE_PRICES_COMPRESSION_AFTER_DAYS", _env_int("TIMESCALE_COMPRESSION_AFTER_DAYS", 0)),
            ),
            copy_enabled=_env_bool("TIMESCALE_PRICES_COPY_ENABLED", default=True),
            copy_fallback_enabled=_env_bool("TIMESCALE_PRICES_COPY_FALLBACK_ENABLED", default=True),
            circuit_failure_threshold=tuned_int("TIMESCALE_PRICES_CIRCUIT_FAILURE_THRESHOLD", 3, 1, 100),
            circuit_open_s=tuned_float("TIMESCALE_PRICES_CIRCUIT_OPEN_S", 5.0, 0.1, 300.0),
        )


class PostgresPriceStorage:
    """Thread-safe writer for price, quote, and raw rows in Postgres-compatible stores."""

    def __init__(self, config: PostgresPriceStorageConfig | None = None):
        self._config = config or PostgresPriceStorageConfig.from_env()
        self._pool: Any = None
        self._state_lock = threading.RLock()
        self._schema_ready = False
        self._schema_error: str | None = None
        self._schema_validation: dict[str, Any] = {
            "required_tables": sorted(
                list(_PG_PRICE_SCHEMA_TABLE_COLUMNS) + list(_PG_PRICE_STAGING_TABLE_COLUMNS)
            ),
            "required_indexes": list(_PG_PRICE_SCHEMA_INDEXES),
            "missing_tables": [],
            "missing_columns": {},
            "missing_indexes": [],
        }
        self._policy_status: dict[str, Any] = {
            "retention_days": int(self._config.retention_days),
            "compression_after_days": int(self._config.compression_after_days),
            "chunk_intervals": _price_chunk_policy_status(),
            "applied": False,
            "last_error": "",
        }
        self._last_error: str | None = None
        self._last_error_ts_ms = 0
        self._last_connect_ts_ms = 0
        self._metrics: dict[str, Any] = {
            "retry_count": 0,
            "write_batches": 0,
            "written_prices": 0,
            "written_quotes": 0,
            "written_raw": 0,
            "dropped_rows": 0,
            "copy_batches": 0,
            "copy_rows": 0,
            "values_batches": 0,
            "values_rows": 0,
            "copy_fallbacks": 0,
            "write_failures": 0,
            "retryable_failures": 0,
            "fatal_failures": 0,
            "pool_resets": 0,
            "last_failure_class": "",
            "last_failure_reason": "",
            "last_failure_retryable": False,
            "last_failure_reset_pool": False,
            "write_circuit_open": False,
            "write_circuit_open_until_ts_ms": 0,
            "write_circuit_last_opened_ts_ms": 0,
            "write_circuit_opened_count": 0,
            "write_circuit_rejected_batches": 0,
            "write_circuit_consecutive_failures": 0,
            "write_circuit_last_failure": "",
            "write_circuit_last_failure_class": "",
            "last_write_duration_ms": 0,
            "last_write_path": "",
            "last_copy_unavailable": "",
            "total_write_duration_ms": 0,
            "last_write_ts_ms": 0,
            "normalization_input_rows": 0,
            "normalization_safe_float_calls": 0,
            "normalization_safe_int_calls": 0,
            "normalization_datetime_conversions": 0,
            "normalization_symbol_parses": 0,
            "normalization_event_key_normalizations": 0,
            "row_copy_avoided_rows": 0,
            "row_copy_fallback_rows": 0,
        }

    @property
    def enabled(self) -> bool:
        return bool(self._config.enabled)

    def start(self) -> dict[str, Any]:
        if not self.enabled:
            return self.get_snapshot()
        if ConnectionPool is None or psycopg is None:
            raise RuntimeError("timescale_prices_enabled_but_psycopg_not_installed")
        with self._state_lock:
            if self._pool is None:
                pool = ConnectionPool(
                    conninfo=str(self._config.dsn),
                    min_size=int(self._config.pool_min_size),
                    max_size=int(self._config.pool_max_size),
                    timeout=float(self._config.connect_timeout_s),
                    kwargs={
                        "connect_timeout": int(max(1, round(self._config.connect_timeout_s))),
                        "application_name": str(self._config.application_name),
                    },
                    check=_check_price_storage_connection,
                    reset=_reset_price_storage_connection,
                    open=False,
                )
                try:
                    pool.open(wait=True, timeout=float(self._config.connect_timeout_s))
                except Exception:
                    try:
                        pool.close(timeout=float(self._config.connect_timeout_s))
                    except Exception:
                        pass  # no-op-guard: allow - best-effort cleanup after failed pool open
                    raise
                self._pool = pool
        self.ensure_schema()
        return self.get_snapshot()

    def close(self) -> dict[str, Any]:
        with self._state_lock:
            pool = self._pool
            self._pool = None
        if pool is not None:
            try:
                pool.close(timeout=float(self._config.connect_timeout_s))
            except Exception as exc:
                self._record_error(exc)
        return self.get_snapshot()

    def _record_error(self, error: BaseException) -> None:
        self._last_error = f"{type(error).__name__}:{error}"
        self._last_error_ts_ms = int(time.time() * 1000)
        _warn_nonfatal("STORAGE_PG_PRICES_WRITE_FAILED", error, enabled=bool(self.enabled))
        record_component_health(
            "storage_pg_prices",
            ok=False,
            status="error",
            detail=str(self._last_error),
            observed_ts_ms=int(self._last_error_ts_ms),
            extra={"enabled": bool(self.enabled)},
        )

    def _record_failure_classification(self, classification: _FailureClassification) -> None:
        with self._state_lock:
            if bool(classification.retryable):
                self._metrics["retryable_failures"] = int(self._metrics.get("retryable_failures") or 0) + 1
            else:
                self._metrics["fatal_failures"] = int(self._metrics.get("fatal_failures") or 0) + 1
            self._metrics["last_failure_class"] = str(classification.failure_class)
            self._metrics["last_failure_reason"] = str(classification.reason)
            self._metrics["last_failure_retryable"] = bool(classification.retryable)
            self._metrics["last_failure_reset_pool"] = bool(classification.reset_pool)

    def _note_retry(self) -> None:
        with self._state_lock:
            self._metrics["retry_count"] = int(self._metrics.get("retry_count") or 0) + 1

    def _retry_delay_s(self, attempt: int) -> float:
        delay_s = min(
            float(self._config.retry_max_s),
            float(self._config.retry_base_s) * (2 ** max(0, int(attempt) - 1)),
        )
        delay_s += random.uniform(0.0, min(0.25, float(self._config.retry_base_s)))
        return float(delay_s)

    def _reset_pool(self) -> None:
        with self._state_lock:
            pool = self._pool
            self._pool = None
            self._metrics["pool_resets"] = int(self._metrics.get("pool_resets") or 0) + 1
        if pool is not None:
            try:
                pool.close(timeout=float(self._config.connect_timeout_s))
            except Exception as exc:
                self._record_error(exc)

    def _write_circuit_is_open_locked(self, now_ts_ms: int) -> bool:
        if not bool(self._metrics.get("write_circuit_open")):
            return False
        open_until_ts_ms = int(self._metrics.get("write_circuit_open_until_ts_ms") or 0)
        if open_until_ts_ms > int(now_ts_ms):
            return True
        self._metrics["write_circuit_open"] = False
        self._metrics["write_circuit_open_until_ts_ms"] = 0
        self._metrics["write_circuit_consecutive_failures"] = 0
        return False

    def _raise_if_write_circuit_open(self) -> None:
        now_ts_ms = int(time.time() * 1000)
        rejected = False
        with self._state_lock:
            if self._write_circuit_is_open_locked(now_ts_ms):
                rejected = True
                self._metrics["write_circuit_rejected_batches"] = int(
                    self._metrics.get("write_circuit_rejected_batches") or 0
                ) + 1
                open_until_ts_ms = int(self._metrics.get("write_circuit_open_until_ts_ms") or 0)
                reason = str(self._metrics.get("write_circuit_last_failure") or "retryable_write_failures")
            else:
                open_until_ts_ms = 0
                reason = ""
        if not rejected:
            emit_gauge("storage_pg_prices_write_circuit_open", 0, component="engine.runtime.storage_pg_prices")
            return
        emit_counter(
            "storage_pg_prices_write_circuit_rejected_batches",
            1,
            component="engine.runtime.storage_pg_prices",
            extra_tags={"reason": str(reason)},
        )
        emit_gauge("storage_pg_prices_write_circuit_open", 1, component="engine.runtime.storage_pg_prices")
        record_component_health(
            "storage_pg_prices",
            ok=False,
            status="backpressure",
            detail="write_circuit_open",
            observed_ts_ms=int(now_ts_ms),
            extra={
                "enabled": bool(self.enabled),
                "open_until_ts_ms": int(open_until_ts_ms),
                "reason": str(reason),
            },
        )
        raise _PriceWriteCircuitOpen(
            "storage_pg_prices_write_batch_circuit_open:"
            f"open_until_ts_ms={int(open_until_ts_ms)};reason={str(reason)}"
        )

    def _record_write_circuit_failure(
        self,
        error: BaseException,
        classification: _FailureClassification,
    ) -> None:
        if not bool(classification.retryable):
            return
        now_ts_ms = int(time.time() * 1000)
        opened = False
        with self._state_lock:
            consecutive = int(self._metrics.get("write_circuit_consecutive_failures") or 0) + 1
            self._metrics["write_circuit_consecutive_failures"] = int(consecutive)
            self._metrics["write_circuit_last_failure"] = f"{type(error).__name__}:{error}"
            self._metrics["write_circuit_last_failure_class"] = str(classification.reason)
            threshold = max(1, int(self._config.circuit_failure_threshold))
            if consecutive >= threshold:
                opened = not bool(self._metrics.get("write_circuit_open"))
                self._metrics["write_circuit_open"] = True
                self._metrics["write_circuit_open_until_ts_ms"] = int(
                    now_ts_ms + round(float(self._config.circuit_open_s) * 1000.0)
                )
                self._metrics["write_circuit_last_opened_ts_ms"] = int(now_ts_ms)
                if opened:
                    self._metrics["write_circuit_opened_count"] = int(
                        self._metrics.get("write_circuit_opened_count") or 0
                    ) + 1
                open_until_ts_ms = int(self._metrics.get("write_circuit_open_until_ts_ms") or 0)
            else:
                open_until_ts_ms = 0
        if opened:
            emit_counter(
                "storage_pg_prices_write_circuit_opened",
                1,
                component="engine.runtime.storage_pg_prices",
                extra_tags={"reason": str(classification.reason)},
            )
            emit_gauge("storage_pg_prices_write_circuit_open", 1, component="engine.runtime.storage_pg_prices")
            record_component_health(
                "storage_pg_prices",
                ok=False,
                status="backpressure",
                detail="write_circuit_opened",
                observed_ts_ms=int(now_ts_ms),
                extra={
                    "enabled": bool(self.enabled),
                    "open_until_ts_ms": int(open_until_ts_ms),
                    "failure": f"{type(error).__name__}:{error}",
                    "reason": str(classification.reason),
                },
            )

    def _record_write_circuit_success(self) -> None:
        with self._state_lock:
            was_open = bool(self._metrics.get("write_circuit_open"))
            self._metrics["write_circuit_open"] = False
            self._metrics["write_circuit_open_until_ts_ms"] = 0
            self._metrics["write_circuit_consecutive_failures"] = 0
            self._metrics["write_circuit_last_failure_class"] = ""
        if was_open:
            emit_gauge("storage_pg_prices_write_circuit_open", 0, component="engine.runtime.storage_pg_prices")

    def _record_schema_validation(self, validation: Mapping[str, Any]) -> None:
        with self._state_lock:
            self._schema_validation = {
                "required_tables": list(validation.get("required_tables") or []),
                "required_indexes": list(validation.get("required_indexes") or []),
                "missing_tables": list(validation.get("missing_tables") or []),
                "missing_columns": dict(validation.get("missing_columns") or {}),
                "missing_indexes": list(validation.get("missing_indexes") or []),
            }

    def _record_policy_status(self, *, applied: bool, last_error: str = "") -> None:
        with self._state_lock:
            previous = dict(self._policy_status)
            self._policy_status = {
                "retention_days": int(self._config.retention_days),
                "compression_after_days": int(self._config.compression_after_days),
                "chunk_intervals": dict(previous.get("chunk_intervals") or _price_chunk_policy_status()),
                "applied": bool(applied),
                "last_error": str(last_error or ""),
            }

    def _apply_timescale_policies(self, cur: Any, relation_name: str, table_name: str) -> None:
        if int(self._config.compression_after_days) > 0:
            compress_orderby = _pg_price_compress_orderby(table_name)
            cur.execute(
                f"ALTER TABLE {relation_name} SET ("
                f"timescaledb.compress, "
                f"timescaledb.compress_orderby = '{compress_orderby}', "
                f"timescaledb.compress_segmentby = 'symbol'"
                f")"
            )
            cur.execute(
                "SELECT add_compression_policy(%s::regclass, %s::interval, if_not_exists => TRUE)",
                (relation_name, f"{int(self._config.compression_after_days)} days"),
            )
        if int(self._config.retention_days) > 0:
            cur.execute(
                "SELECT add_retention_policy(%s::regclass, %s::interval, if_not_exists => TRUE)",
                (relation_name, f"{int(self._config.retention_days)} days"),
            )

    def _set_timescale_chunk_interval(self, cur: Any, relation_name: str, table_name: str) -> None:
        cur.execute(
            "SELECT set_chunk_time_interval(%s::regclass, %s::interval)",
            (relation_name, hypertable_chunk_interval(table_name)),
        )

    def _record_actual_chunk_intervals(self, cur: Any) -> None:
        chunk_intervals = _price_chunk_policy_status()
        for table_name in _PG_PRICE_HYPERTABLE_TABLES:
            cur.execute(
                """
                SELECT time_interval::text,
                       (EXTRACT(EPOCH FROM time_interval) * 1000)::bigint
                FROM timescaledb_information.dimensions
                WHERE hypertable_schema = %s
                  AND hypertable_name = %s
                  AND column_name = 'time'
                LIMIT 1
                """,
                (str(self._config.schema_name), str(table_name)),
            )
            row = cur.fetchone()
            if not row:
                continue
            actual_interval = str(row[0] or "")
            actual_interval_ms = int(row[1]) if row[1] is not None else None
            chunk_intervals[table_name]["actual_interval"] = actual_interval
            chunk_intervals[table_name]["actual_interval_ms"] = actual_interval_ms
            if actual_interval_ms is not None:
                emit_gauge(
                    "storage_pg_prices_hypertable_chunk_interval_ms",
                    int(actual_interval_ms),
                    component="engine.runtime.storage_pg_prices",
                    extra_tags={
                        "table": str(table_name),
                        "desired_interval": hypertable_chunk_interval(table_name),
                    },
                )
        with self._state_lock:
            previous = dict(self._policy_status)
            previous["chunk_intervals"] = chunk_intervals
            self._policy_status = previous

    def _validate_schema(self, cur: Any) -> dict[str, Any]:
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
            """,
            (str(self._config.schema_name),),
        )
        table_rows = cur.fetchall() or []
        present_tables = {
            str(row[0]).strip()
            for row in table_rows
            if row and row[0] is not None and str(row[0]).strip()
        }
        required_table_columns = {
            **_PG_PRICE_SCHEMA_TABLE_COLUMNS,
            **_PG_PRICE_STAGING_TABLE_COLUMNS,
        }
        required_tables = sorted(required_table_columns)
        missing_tables = [table_name for table_name in required_tables if table_name not in present_tables]

        cur.execute(
            """
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = %s
            """,
            (str(self._config.schema_name),),
        )
        column_rows = cur.fetchall() or []
        present_columns: dict[str, set[str]] = {}
        for row in column_rows:
            if not row:
                continue
            table_name = str(row[0] or "").strip()
            column_name = str(row[1] or "").strip().lower()
            if not table_name or not column_name:
                continue
            present_columns.setdefault(table_name, set()).add(column_name)
        missing_columns: dict[str, list[str]] = {}
        for table_name, columns in required_table_columns.items():
            if table_name in missing_tables:
                continue
            table_columns = present_columns.get(table_name, set())
            absent = [
                str(column)
                for column in columns
                if str(column).strip().lower() not in table_columns
            ]
            if absent:
                missing_columns[str(table_name)] = absent

        cur.execute(
            """
            SELECT indexname
            FROM pg_indexes
            WHERE schemaname = %s
            """,
            (str(self._config.schema_name),),
        )
        index_rows = cur.fetchall() or []
        present_indexes = {
            str(row[0]).strip()
            for row in index_rows
            if row and row[0] is not None and str(row[0]).strip()
        }
        required_indexes = list(_PG_PRICE_SCHEMA_INDEXES)
        missing_indexes = [
            index_name
            for index_name in required_indexes
            if index_name not in present_indexes
        ]
        validation = {
            "required_tables": required_tables,
            "required_indexes": required_indexes,
            "missing_tables": missing_tables,
            "missing_columns": missing_columns,
            "missing_indexes": missing_indexes,
        }
        self._record_schema_validation(validation)
        if missing_tables or missing_columns or missing_indexes:
            raise RuntimeError(
                "timescale_prices_schema_invalid:"
                f"missing_tables={missing_tables};"
                f"missing_columns={missing_columns};"
                f"missing_indexes={missing_indexes}"
            )
        return validation

    def _prepare_connection(self, con: Any) -> None:
        with con.cursor() as cur:
            cur.execute(
                f"SET SESSION statement_timeout = {_session_timeout_ms(self._config.command_timeout_s)}"
            )
            cur.execute(
                f"SET SESSION lock_timeout = {_session_timeout_ms(self._config.lock_timeout_s)}"
            )
            cur.execute(
                "SET SESSION idle_in_transaction_session_timeout = "
                f"{_session_timeout_ms(self._config.idle_in_txn_timeout_s)}"
            )
            cur.execute("SET SESSION TIME ZONE 'UTC'")
            cur.execute("SELECT 1")
        self._last_connect_ts_ms = int(time.time() * 1000)

    @contextmanager
    def _connection(self):
        if self._pool is None:
            self.start()
        with self._state_lock:
            pool = self._pool
        if pool is None:
            raise RuntimeError("timescale_prices_connection_pool_unavailable")
        con = pool.getconn(timeout=float(self._config.connect_timeout_s))
        discard = False
        try:
            rollback_if_in_transaction(
                con,
                logger=LOG,
                context="storage_pg_prices_acquire",
            )
            con.autocommit = False
            self._prepare_connection(con)
            yield con
        except Exception:
            discard = True
            try:
                con.rollback()
            except Exception:
                pass  # no-op-guard: allow - connection may already be broken
            raise
        finally:
            try:
                if not discard:
                    try:
                        rollback_if_in_transaction(
                            con,
                            logger=LOG,
                            context="storage_pg_prices_release",
                        )
                    except Exception:
                        discard = True
                if discard:
                    try:
                        con.close()
                    except Exception:
                        pass  # no-op-guard: allow - pool will discard closed connections
                pool.putconn(con)
            except Exception as exc:
                self._record_error(exc)

    def _run_with_retry(self, callback: Any, *, operation: str) -> Any:
        last_error: BaseException | None = None
        last_classification: _FailureClassification | None = None
        for attempt in range(1, int(self._config.retry_attempts) + 1):
            try:
                result = callback()
                if last_error is not None:
                    with self._state_lock:
                        self._last_error = None
                return result
            except Exception as exc:
                last_error = exc
                classification = _classify_pg_price_failure(exc)
                last_classification = classification
                self._record_error(exc)
                self._record_failure_classification(classification)
                if str(operation) == "write_batch":
                    with self._state_lock:
                        self._metrics["write_failures"] = int(self._metrics.get("write_failures") or 0) + 1
                emit_counter(
                    "storage_pg_prices_failures",
                    1,
                    component="engine.runtime.storage_pg_prices",
                    extra_tags={
                        "operation": str(operation),
                        "error": type(exc).__name__,
                        "failure_class": str(classification.failure_class),
                        "retryable": str(bool(classification.retryable)).lower(),
                        "reset_pool": str(bool(classification.reset_pool)).lower(),
                    },
                )
                if not bool(classification.retryable):
                    break
                if attempt >= int(self._config.retry_attempts):
                    break
                self._note_retry()
                if bool(classification.reset_pool):
                    self._reset_pool()
                time.sleep(self._retry_delay_s(attempt))
        if str(operation) == "write_batch" and last_error is not None and last_classification is not None:
            self._record_write_circuit_failure(last_error, last_classification)
        raise RuntimeError(f"storage_pg_prices_{operation}_failed:{last_error}") from last_error

    def _ensure_raw_event_conflict_contract(self, cur: Any, *, raw_ref: str) -> None:
        desired = ("symbol", "provider", "event_key", "time")
        relation_name = f"{self._config.schema_name}.price_quotes_raw"
        cur.execute(
            """
            SELECT c.conname, array_agg(a.attname ORDER BY keys.ord) AS columns
            FROM pg_constraint c
            JOIN unnest(c.conkey) WITH ORDINALITY AS keys(attnum, ord) ON TRUE
            JOIN pg_attribute a
              ON a.attrelid = c.conrelid
             AND a.attnum = keys.attnum
            WHERE c.conrelid = %s::regclass
              AND c.contype = 'p'
            GROUP BY c.conname
            LIMIT 1
            """,
            (relation_name,),
        )
        row = cur.fetchone()
        constraint_name = ""
        columns: tuple[str, ...] = ()
        if row:
            constraint_name = str(row[0] or "")
            columns = tuple(str(col) for col in (row[1] or ()))
        if columns == desired and constraint_name == "price_quotes_raw_pkey":
            return
        cur.execute(
            f"""
            DELETE FROM {raw_ref} older
            USING {raw_ref} newer
            WHERE older.ctid < newer.ctid
              AND older.symbol = newer.symbol
              AND older.provider = newer.provider
              AND older.event_key = newer.event_key
              AND older.{_quote_ident('time')} = newer.{_quote_ident('time')}
            """
        )
        if constraint_name:
            cur.execute(
                f"ALTER TABLE {raw_ref} DROP CONSTRAINT IF EXISTS {_quote_ident(constraint_name)}"
            )
        cur.execute(
            f"""
            ALTER TABLE {raw_ref}
            ADD CONSTRAINT price_quotes_raw_pkey
            PRIMARY KEY(symbol, provider, event_key, {_quote_ident('time')})
            """
        )

    def ensure_schema(self) -> dict[str, Any]:
        if not self.enabled:
            return self.get_snapshot()
        with self._state_lock:
            if self._schema_ready:
                return self.get_snapshot()

        schema_ref = _quote_ident(self._config.schema_name)
        price_ticks_ref = f"{schema_ref}.price_ticks"
        quotes_ref = f"{schema_ref}.price_quotes"
        raw_ref = f"{schema_ref}.price_quotes_raw"
        price_ticks_stage_ref = _staging_relation_ref(schema_ref, "price_ticks")
        quotes_stage_ref = _staging_relation_ref(schema_ref, "price_quotes")
        raw_stage_ref = _staging_relation_ref(schema_ref, "price_quotes_raw")

        def _ensure() -> None:
            with self._connection() as con:
                with con.cursor() as cur:
                    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_ref}")
                    cur.execute(price_timescale_create_table_sql(price_ticks_ref, "price_ticks"))
                    cur.execute(price_timescale_create_table_sql(quotes_ref, "price_quotes"))
                    cur.execute(price_timescale_create_table_sql(raw_ref, "price_quotes_raw"))
                    self._ensure_raw_event_conflict_contract(cur, raw_ref=raw_ref)
                    cur.execute(_staging_table_ddl(schema_ref, "price_ticks"))
                    cur.execute(_staging_table_ddl(schema_ref, "price_quotes"))
                    cur.execute(_staging_table_ddl(schema_ref, "price_quotes_raw"))
                    cur.execute(price_timescale_time_desc_index_sql(price_ticks_ref, "price_ticks"))
                    cur.execute(price_timescale_time_desc_index_sql(quotes_ref, "price_quotes"))
                    cur.execute(price_timescale_time_desc_index_sql(raw_ref, "price_quotes_raw"))
                    cur.execute(
                        f"CREATE INDEX IF NOT EXISTS {_quote_ident(_staging_index_name('price_ticks'))} "
                        f"ON {price_ticks_stage_ref} (staging_session)"
                    )
                    cur.execute(
                        f"CREATE INDEX IF NOT EXISTS {_quote_ident(_staging_index_name('price_quotes'))} "
                        f"ON {quotes_stage_ref} (staging_session)"
                    )
                    cur.execute(
                        f"CREATE INDEX IF NOT EXISTS {_quote_ident(_staging_index_name('price_quotes_raw'))} "
                        f"ON {raw_stage_ref} (staging_session)"
                    )
                con.commit()
                try:
                    with con.cursor() as cur:
                        cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE")
                        for relation in (
                            f"{self._config.schema_name}.price_ticks",
                            f"{self._config.schema_name}.price_quotes",
                            f"{self._config.schema_name}.price_quotes_raw",
                        ):
                            table_name = relation.rsplit(".", 1)[-1]
                            cur.execute(
                                """
                                SELECT create_hypertable(
                                  %s::regclass,
                                  'time',
                                  chunk_time_interval => %s::interval,
                                  if_not_exists => TRUE,
                                  migrate_data => TRUE
                                )
                                """,
                                (relation, hypertable_chunk_interval(table_name)),
                            )
                            self._set_timescale_chunk_interval(cur, relation, table_name)
                            self._apply_timescale_policies(cur, relation, table_name)
                        self._record_actual_chunk_intervals(cur)
                    con.commit()
                    self._record_policy_status(applied=True)
                except Exception as exc:
                    # Plain Postgres remains a supported degraded mode.
                    try:
                        con.rollback()
                    except Exception:
                        pass  # no-op-guard: allow best-effort rollback
                    self._record_policy_status(applied=False, last_error=f"{type(exc).__name__}:{exc}")
                with con.cursor() as cur:
                    self._validate_schema(cur)
                con.commit()

        try:
            self._run_with_retry(_ensure, operation="ensure_schema")
            with self._state_lock:
                self._schema_ready = True
                self._schema_error = None
            record_component_health(
                "storage_pg_prices",
                ok=True,
                status="ok",
                detail="schema_ready",
                extra={"enabled": bool(self.enabled), "schema_name": str(self._config.schema_name)},
            )
            return self.get_snapshot()
        except Exception as exc:
            with self._state_lock:
                self._schema_ready = False
                self._schema_error = f"{type(exc).__name__}:{exc}"
            raise

    def _target_relation_refs(self) -> tuple[str, str, str, str]:
        schema_ref = _quote_ident(self._config.schema_name)
        return (
            schema_ref,
            f"{schema_ref}.price_ticks",
            f"{schema_ref}.price_quotes",
            f"{schema_ref}.price_quotes_raw",
        )

    def _copy_rows_binary(
        self,
        cur: Any,
        *,
        relation_ref: str,
        columns: tuple[str, ...],
        type_names: tuple[str, ...],
        rows: Iterable[tuple[Any, ...]],
    ) -> None:
        copy_factory = getattr(cur, "copy", None)
        if not callable(copy_factory):
            raise _CopyUnavailableError("cursor_copy_api_missing")
        copy_sql = f"COPY {relation_ref} ({_column_list_sql(columns)}) FROM STDIN (FORMAT BINARY)"
        try:
            copy_context = cast(AbstractContextManager[Any], copy_factory(copy_sql))
        except Exception as exc:
            if _is_copy_unavailable_exception(exc):
                raise _CopyUnavailableError(f"cursor_copy_open_failed:{exc}") from exc
            raise
        with copy_context as copy:
            set_types = getattr(copy, "set_types", None)
            write_row = getattr(copy, "write_row", None)
            if not callable(set_types):
                raise _CopyUnavailableError("binary_copy_set_types_missing")
            if not callable(write_row):
                raise _CopyUnavailableError("binary_copy_write_row_missing")
            set_types(list(type_names))
            for row in rows:
                write_row(tuple(row))

    def _cleanup_staging(
        self,
        cur: Any,
        *,
        schema_ref: str,
        staging_session: str,
        table_names: Iterable[str],
    ) -> None:
        for table_name in sorted(set(str(name) for name in table_names)):
            cur.execute(
                f"DELETE FROM {_staging_relation_ref(schema_ref, table_name)} WHERE staging_session = %s",
                (str(staging_session),),
            )

    def _cleanup_staging_best_effort(
        self,
        cur: Any,
        *,
        schema_ref: str,
        staging_session: str,
        table_names: Iterable[str],
    ) -> None:
        try:
            self._cleanup_staging(
                cur,
                schema_ref=schema_ref,
                staging_session=staging_session,
                table_names=table_names,
            )
        except Exception:
            pass  # no-op-guard: allow - rollback also removes uncommitted staging rows.

    def _copy_stage_and_upsert(
        self,
        cur: Any,
        *,
        schema_ref: str,
        table_name: str,
        target_ref: str,
        rows: list[tuple[Any, ...]],
        staging_session: str,
    ) -> None:
        if not rows:
            return
        stage_ref = _staging_relation_ref(schema_ref, table_name)
        staging_table_name = _PG_PRICE_STAGING_TABLE_NAMES[table_name]
        cur.execute(
            f"DELETE FROM {stage_ref} WHERE staging_session = %s",
            (str(staging_session),),
        )
        self._copy_rows_binary(
            cur,
            relation_ref=stage_ref,
            columns=_PG_PRICE_STAGING_TABLE_COLUMNS[staging_table_name],
            type_names=_PG_PRICE_COPY_TYPES[table_name],
            rows=((str(staging_session), int(ordinal), *tuple(row)) for ordinal, row in enumerate(rows)),
        )
        cur.execute(
            self._upsert_from_staging_sql(
                table_name=table_name,
                target_ref=target_ref,
                stage_ref=stage_ref,
            ),
            (str(staging_session),),
        )

    def _upsert_from_staging_sql(self, *, table_name: str, target_ref: str, stage_ref: str) -> str:
        target_columns = _PG_PRICE_SCHEMA_TABLE_COLUMNS[str(table_name)]
        target_column_sql = _column_list_sql(target_columns)
        select_column_sql = ", ".join(f"staged.{_quote_ident(column)}" for column in target_columns)
        staging_select_sql = ", ".join(_quote_ident(column) for column in target_columns)
        if table_name in {"price_ticks", "price_quotes"}:
            distinct_key_sql = f"{_quote_ident('symbol')}, {_quote_ident('time')}"
            conflict_sql = f"symbol, {_quote_ident('time')}"
        elif table_name == "price_quotes_raw":
            distinct_key_sql = f"{_quote_ident('symbol')}, {_quote_ident('provider')}, {_quote_ident('event_key')}, {_quote_ident('time')}"
            conflict_sql = f"symbol, provider, event_key, {_quote_ident('time')}"
        else:
            raise ValueError(f"unknown_price_staging_upsert_table:{table_name}")
        if table_name == "price_ticks":
            update_sql = """
                              last=EXCLUDED.last,
                              source=EXCLUDED.source,
                              provider=EXCLUDED.provider,
                              bid=EXCLUDED.bid,
                              ask=EXCLUDED.ask,
                              spread=EXCLUDED.spread,
                              volume=EXCLUDED.volume,
                              latency_ms=EXCLUDED.latency_ms,
                              provider_score=EXCLUDED.provider_score,
                              last_update_ts_ms=EXCLUDED.last_update_ts_ms,
                              ingest_ts_ms=EXCLUDED.ingest_ts_ms
                            """
        elif table_name == "price_quotes":
            update_sql = """
                              last=EXCLUDED.last,
                              bid=EXCLUDED.bid,
                              ask=EXCLUDED.ask,
                              spread=EXCLUDED.spread,
                              volume=EXCLUDED.volume,
                              source=EXCLUDED.source,
                              last_trade_ts_ms=EXCLUDED.last_trade_ts_ms,
                              last_quote_ts_ms=EXCLUDED.last_quote_ts_ms,
                              last_update_ts_ms=EXCLUDED.last_update_ts_ms
                            """
        else:
            update_sql = f"""
                              {_quote_ident('time')}=EXCLUDED.{_quote_ident('time')},
                              event_type=EXCLUDED.event_type,
                              event_ts_ms=EXCLUDED.event_ts_ms,
                              last=EXCLUDED.last,
                              bid=EXCLUDED.bid,
                              ask=EXCLUDED.ask,
                              spread=EXCLUDED.spread,
                              volume=EXCLUDED.volume,
                              trade_ts_ms=EXCLUDED.trade_ts_ms,
                              quote_ts_ms=EXCLUDED.quote_ts_ms,
                              ingest_ts_ms=EXCLUDED.ingest_ts_ms,
                              source=EXCLUDED.source
                            """
        return f"""
                            INSERT INTO {target_ref}({target_column_sql})
                            SELECT {select_column_sql}
                            FROM (
                              SELECT DISTINCT ON ({distinct_key_sql})
                                {staging_select_sql}, staging_ordinal
                              FROM {stage_ref}
                              WHERE staging_session = %s
                              ORDER BY {distinct_key_sql}, staging_ordinal DESC
                            ) AS staged
                            ON CONFLICT({conflict_sql}) DO UPDATE SET
                              {update_sql}
                            """

    def _write_batch_copy(
        self,
        *,
        price_rows: list[tuple[Any, ...]],
        quote_rows: list[tuple[Any, ...]],
        raw_rows: list[tuple[Any, ...]],
    ) -> str:
        schema_ref, price_ticks_ref, quotes_ref, raw_ref = self._target_relation_refs()
        staging_session = _staging_session_token()
        target_tables = tuple(
            table_name
            for table_name, rows in (
                ("price_ticks", price_rows),
                ("price_quotes", quote_rows),
                ("price_quotes_raw", raw_rows),
            )
            if rows
        )
        staged_tables: list[str] = []
        with self._connection() as con:
            with con.cursor() as cur:
                maybe_apply_sync_refetchable_pg_durability(
                    cur,
                    scope="storage_pg_prices.write_batch",
                    target_tables=target_tables,
                )
                try:
                    if price_rows:
                        staged_tables.append("price_ticks")
                        self._copy_stage_and_upsert(
                            cur,
                            schema_ref=schema_ref,
                            table_name="price_ticks",
                            target_ref=price_ticks_ref,
                            rows=price_rows,
                            staging_session=staging_session,
                        )
                    if quote_rows:
                        staged_tables.append("price_quotes")
                        self._copy_stage_and_upsert(
                            cur,
                            schema_ref=schema_ref,
                            table_name="price_quotes",
                            target_ref=quotes_ref,
                            rows=quote_rows,
                            staging_session=staging_session,
                        )
                    if raw_rows:
                        staged_tables.append("price_quotes_raw")
                        self._copy_stage_and_upsert(
                            cur,
                            schema_ref=schema_ref,
                            table_name="price_quotes_raw",
                            target_ref=raw_ref,
                            rows=raw_rows,
                            staging_session=staging_session,
                        )
                except Exception:
                    self._cleanup_staging_best_effort(
                        cur,
                        schema_ref=schema_ref,
                        staging_session=staging_session,
                        table_names=staged_tables,
                    )
                    raise
                self._cleanup_staging(
                    cur,
                    schema_ref=schema_ref,
                    staging_session=staging_session,
                    table_names=staged_tables,
                )
            con.commit()
        return "copy_staging"

    def _write_batch_values(
        self,
        *,
        price_rows: list[tuple[Any, ...]],
        quote_rows: list[tuple[Any, ...]],
        raw_rows: list[tuple[Any, ...]],
        write_path: str = "values_upsert",
    ) -> str:
        _schema_ref, price_ticks_ref, quotes_ref, raw_ref = self._target_relation_refs()
        target_tables = tuple(
            table_name
            for table_name, rows in (
                ("price_ticks", price_rows),
                ("price_quotes", quote_rows),
                ("price_quotes_raw", raw_rows),
            )
            if rows
        )
        with self._connection() as con:
            with con.cursor() as cur:
                maybe_apply_sync_refetchable_pg_durability(
                    cur,
                    scope="storage_pg_prices.write_batch",
                    target_tables=target_tables,
                )
                if price_rows:
                    _execute_many_values(
                        cur,
                        f"""
                            INSERT INTO {price_ticks_ref}(
                              "time", symbol, last, source, provider, bid, ask, spread, volume,
                              latency_ms, provider_score, last_update_ts_ms, ingest_ts_ms
                            ) VALUES %s
                            ON CONFLICT(symbol, "time") DO UPDATE SET
                              last=EXCLUDED.last,
                              source=EXCLUDED.source,
                              provider=EXCLUDED.provider,
                              bid=EXCLUDED.bid,
                              ask=EXCLUDED.ask,
                              spread=EXCLUDED.spread,
                              volume=EXCLUDED.volume,
                              latency_ms=EXCLUDED.latency_ms,
                              provider_score=EXCLUDED.provider_score,
                              last_update_ts_ms=EXCLUDED.last_update_ts_ms,
                              ingest_ts_ms=EXCLUDED.ingest_ts_ms
                            """,
                        price_rows,
                        conflict_key_indexes=_PRICE_TICKS_CONFLICT_KEY_INDEXES,
                    )
                if quote_rows:
                    _execute_many_values(
                        cur,
                        f"""
                            INSERT INTO {quotes_ref}(
                              "time", symbol, last, bid, ask, spread, volume, source,
                              last_trade_ts_ms, last_quote_ts_ms, last_update_ts_ms
                            ) VALUES %s
                            ON CONFLICT(symbol, "time") DO UPDATE SET
                              last=EXCLUDED.last,
                              bid=EXCLUDED.bid,
                              ask=EXCLUDED.ask,
                              spread=EXCLUDED.spread,
                              volume=EXCLUDED.volume,
                              source=EXCLUDED.source,
                              last_trade_ts_ms=EXCLUDED.last_trade_ts_ms,
                              last_quote_ts_ms=EXCLUDED.last_quote_ts_ms,
                              last_update_ts_ms=EXCLUDED.last_update_ts_ms
                            """,
                        quote_rows,
                        conflict_key_indexes=_PRICE_QUOTES_CONFLICT_KEY_INDEXES,
                    )
                if raw_rows:
                    _execute_many_values(
                        cur,
                        f"""
                            INSERT INTO {raw_ref}(
                              "time", symbol, provider, event_key, event_type, event_ts_ms, last, bid, ask,
                              spread, volume, trade_ts_ms, quote_ts_ms, ingest_ts_ms, source
                            ) VALUES %s
                            ON CONFLICT(symbol, provider, event_key, "time") DO UPDATE SET
                              "time"=EXCLUDED."time",
                              event_type=EXCLUDED.event_type,
                              event_ts_ms=EXCLUDED.event_ts_ms,
                              last=EXCLUDED.last,
                              bid=EXCLUDED.bid,
                              ask=EXCLUDED.ask,
                              spread=EXCLUDED.spread,
                              volume=EXCLUDED.volume,
                              trade_ts_ms=EXCLUDED.trade_ts_ms,
                              quote_ts_ms=EXCLUDED.quote_ts_ms,
                              ingest_ts_ms=EXCLUDED.ingest_ts_ms,
                              source=EXCLUDED.source
                            """,
                        raw_rows,
                        conflict_key_indexes=_PRICE_QUOTES_RAW_CONFLICT_KEY_INDEXES,
                    )
            con.commit()
        return str(write_path)

    def _record_copy_fallback(self, error: BaseException) -> None:
        reason = f"{type(error).__name__}:{error}"
        with self._state_lock:
            self._metrics["copy_fallbacks"] = int(self._metrics.get("copy_fallbacks") or 0) + 1
            self._metrics["last_copy_unavailable"] = reason
        emit_counter(
            "storage_pg_prices_copy_fallbacks",
            1,
            component="engine.runtime.storage_pg_prices",
            extra_tags={"reason": type(error).__name__},
        )
        _warn_nonfatal(
            "STORAGE_PG_PRICES_COPY_UNAVAILABLE_FALLBACK",
            error,
            fallback="values_upsert",
        )

    def _record_successful_write_metrics(
        self,
        *,
        write_path: str,
        price_rows: int,
        quote_rows: int,
        raw_rows: int,
    ) -> None:
        row_counts = {
            "price_ticks": int(price_rows),
            "price_quotes": int(quote_rows),
            "price_quotes_raw": int(raw_rows),
        }
        total_rows = int(sum(row_counts.values()))
        if str(write_path) == "copy_staging":
            path_kind = "copy"
        elif str(write_path).startswith("values_upsert"):
            path_kind = "values"
        else:
            path_kind = "other"
        with self._state_lock:
            if path_kind == "copy":
                self._metrics["copy_batches"] = int(self._metrics.get("copy_batches") or 0) + 1
                self._metrics["copy_rows"] = int(self._metrics.get("copy_rows") or 0) + int(total_rows)
            elif path_kind == "values":
                self._metrics["values_batches"] = int(self._metrics.get("values_batches") or 0) + 1
                self._metrics["values_rows"] = int(self._metrics.get("values_rows") or 0) + int(total_rows)
        metric_tags = {"write_path": str(write_path), "path_kind": str(path_kind)}
        emit_counter(
            "storage_pg_prices_write_batches",
            1,
            component="engine.runtime.storage_pg_prices",
            extra_tags=metric_tags,
        )
        emit_counter(
            "storage_pg_prices_written_rows",
            int(total_rows),
            component="engine.runtime.storage_pg_prices",
            extra_tags={**metric_tags, "table": "all"},
        )
        if path_kind == "copy":
            emit_counter(
                "storage_pg_prices_copy_batches",
                1,
                component="engine.runtime.storage_pg_prices",
                extra_tags={"write_path": str(write_path)},
            )
            emit_counter(
                "storage_pg_prices_copy_rows",
                int(total_rows),
                component="engine.runtime.storage_pg_prices",
                extra_tags={"write_path": str(write_path)},
            )
        elif path_kind == "values":
            emit_counter(
                "storage_pg_prices_values_batches",
                1,
                component="engine.runtime.storage_pg_prices",
                extra_tags={"write_path": str(write_path)},
            )
            emit_counter(
                "storage_pg_prices_values_rows",
                int(total_rows),
                component="engine.runtime.storage_pg_prices",
                extra_tags={"write_path": str(write_path)},
            )
        for table_name, row_count in row_counts.items():
            if int(row_count) <= 0:
                continue
            emit_counter(
                "storage_pg_prices_written_rows",
                int(row_count),
                component="engine.runtime.storage_pg_prices",
                extra_tags={**metric_tags, "table": str(table_name)},
            )

    def write_batch(
        self,
        *,
        prices: Iterable[Mapping[str, Any]] = (),
        quotes: Iterable[Mapping[str, Any]] = (),
        raw: Iterable[Mapping[str, Any]] = (),
    ) -> dict[str, Any]:
        if not self.enabled:
            return {"ok": True, "prices": 0, "quotes": 0, "raw": 0, "enabled": False}
        normalized = _normalize_price_write_rows(prices=prices, quotes=quotes, raw=raw)
        price_rows = normalized.price_rows
        quote_rows = normalized.quote_rows
        raw_rows = normalized.raw_rows
        dropped_rows = normalized.dropped_rows
        with self._state_lock:
            self._metrics["normalization_input_rows"] = int(
                self._metrics.get("normalization_input_rows") or 0
            ) + int(normalized.input_rows)
            self._metrics["normalization_safe_float_calls"] = int(
                self._metrics.get("normalization_safe_float_calls") or 0
            ) + int(normalized.safe_float_calls)
            self._metrics["normalization_safe_int_calls"] = int(
                self._metrics.get("normalization_safe_int_calls") or 0
            ) + int(normalized.safe_int_calls)
            self._metrics["normalization_datetime_conversions"] = int(
                self._metrics.get("normalization_datetime_conversions") or 0
            ) + int(normalized.datetime_conversions)
            self._metrics["normalization_symbol_parses"] = int(
                self._metrics.get("normalization_symbol_parses") or 0
            ) + int(normalized.symbol_parses)
            self._metrics["normalization_event_key_normalizations"] = int(
                self._metrics.get("normalization_event_key_normalizations") or 0
            ) + int(normalized.event_key_normalizations)
            self._metrics["row_copy_avoided_rows"] = int(
                self._metrics.get("row_copy_avoided_rows") or 0
            ) + int(normalized.row_copy_avoided_rows)
            self._metrics["row_copy_fallback_rows"] = int(
                self._metrics.get("row_copy_fallback_rows") or 0
            ) + int(normalized.row_copy_fallback_rows)
        if normalized.row_copy_avoided_rows:
            emit_counter(
                "storage_pg_prices_row_copies_avoided",
                int(normalized.row_copy_avoided_rows),
                component="engine.runtime.storage_pg_prices",
            )
        if normalized.row_copy_fallback_rows:
            emit_counter(
                "storage_pg_prices_row_copy_fallback_rows",
                int(normalized.row_copy_fallback_rows),
                component="engine.runtime.storage_pg_prices",
            )
        if any(int(value) > 0 for value in dropped_rows.values()):
            dropped_total = int(sum(int(value) for value in dropped_rows.values()))
            with self._state_lock:
                self._metrics["dropped_rows"] = int(self._metrics.get("dropped_rows") or 0) + int(dropped_total)
            emit_counter(
                "storage_pg_prices_dropped_rows",
                int(dropped_total),
                component="engine.runtime.storage_pg_prices",
                extra_tags={"reason": "invalid_rows"},
            )
            _warn_nonfatal(
                "STORAGE_PG_PRICES_INVALID_ROWS_DROPPED",
                ValueError(f"invalid_rows_dropped:{dropped_rows}"),
                dropped_rows=dropped_rows,
            )
        if not price_rows and not quote_rows and not raw_rows:
            return {"ok": True, "prices": 0, "quotes": 0, "raw": 0, "enabled": True}
        self._raise_if_write_circuit_open()
        try:
            self.start()
        except Exception as exc:
            classification = _classify_pg_price_failure(exc)
            self._record_error(exc)
            self._record_failure_classification(classification)
            with self._state_lock:
                self._metrics["write_failures"] = int(self._metrics.get("write_failures") or 0) + 1
            emit_counter(
                "storage_pg_prices_failures",
                1,
                component="engine.runtime.storage_pg_prices",
                extra_tags={
                    "operation": "write_batch_start",
                    "error": type(exc).__name__,
                    "failure_class": str(classification.failure_class),
                    "retryable": str(bool(classification.retryable)).lower(),
                    "reset_pool": str(bool(classification.reset_pool)).lower(),
                },
            )
            self._record_write_circuit_failure(exc, classification)
            raise RuntimeError(f"storage_pg_prices_write_batch_failed:{exc}") from exc

        def _write() -> str:
            if bool(self._config.copy_enabled):
                try:
                    return self._write_batch_copy(
                        price_rows=price_rows,
                        quote_rows=quote_rows,
                        raw_rows=raw_rows,
                    )
                except _CopyUnavailableError as exc:
                    if not bool(self._config.copy_fallback_enabled):
                        raise RuntimeError(f"storage_pg_prices_copy_unavailable:{exc}") from exc
                    self._record_copy_fallback(exc)
                    return self._write_batch_values(
                        price_rows=price_rows,
                        quote_rows=quote_rows,
                        raw_rows=raw_rows,
                        write_path="values_upsert_copy_unavailable",
                    )
            return self._write_batch_values(
                price_rows=price_rows,
                quote_rows=quote_rows,
                raw_rows=raw_rows,
                write_path="values_upsert_copy_disabled",
            )

        write_started = time.perf_counter()
        write_path = str(self._run_with_retry(_write, operation="write_batch") or "")
        write_duration_ms = float((time.perf_counter() - write_started) * 1000.0)
        now_ts_ms = int(time.time() * 1000)
        self._record_write_circuit_success()
        self._record_successful_write_metrics(
            write_path=str(write_path),
            price_rows=int(len(price_rows)),
            quote_rows=int(len(quote_rows)),
            raw_rows=int(len(raw_rows)),
        )
        with self._state_lock:
            self._metrics["write_batches"] = int(self._metrics.get("write_batches") or 0) + 1
            self._metrics["written_prices"] = int(self._metrics.get("written_prices") or 0) + int(len(price_rows))
            self._metrics["written_quotes"] = int(self._metrics.get("written_quotes") or 0) + int(len(quote_rows))
            self._metrics["written_raw"] = int(self._metrics.get("written_raw") or 0) + int(len(raw_rows))
            self._metrics["last_write_duration_ms"] = int(round(write_duration_ms))
            self._metrics["total_write_duration_ms"] = int(self._metrics.get("total_write_duration_ms") or 0) + int(round(write_duration_ms))
            self._metrics["last_write_ts_ms"] = int(now_ts_ms)
            self._metrics["last_write_path"] = str(write_path)
            self._last_error = None
        emit_timing(
            "storage_pg_prices_db_write_duration_ms",
            float(write_duration_ms),
            component="engine.runtime.storage_pg_prices",
        )
        record_component_health(
            "storage_pg_prices",
            ok=True,
            status="ok",
            detail="write_batch_ok",
            observed_ts_ms=int(now_ts_ms),
            latency_ms=float(write_duration_ms),
            extra={
                "enabled": bool(self.enabled),
                "price_rows": int(len(price_rows)),
                "quote_rows": int(len(quote_rows)),
                "raw_rows": int(len(raw_rows)),
                "dropped_rows": dict(dropped_rows),
                "normalization_safe_float_calls": int(normalized.safe_float_calls),
                "normalization_safe_int_calls": int(normalized.safe_int_calls),
                "normalization_datetime_conversions": int(normalized.datetime_conversions),
                "normalization_symbol_parses": int(normalized.symbol_parses),
                "normalization_event_key_normalizations": int(
                    normalized.event_key_normalizations
                ),
                "row_copy_avoided_rows": int(normalized.row_copy_avoided_rows),
                "row_copy_fallback_rows": int(normalized.row_copy_fallback_rows),
                "write_duration_ms": int(round(write_duration_ms)),
                "write_path": str(write_path),
            },
        )
        return {
            "ok": True,
            "enabled": True,
            "prices": int(len(price_rows)),
            "quotes": int(len(quote_rows)),
            "raw": int(len(raw_rows)),
            "dropped_rows": dict(dropped_rows),
            "normalization_safe_float_calls": int(normalized.safe_float_calls),
            "normalization_safe_int_calls": int(normalized.safe_int_calls),
            "normalization_datetime_conversions": int(normalized.datetime_conversions),
            "normalization_symbol_parses": int(normalized.symbol_parses),
            "normalization_event_key_normalizations": int(normalized.event_key_normalizations),
            "row_copy_avoided_rows": int(normalized.row_copy_avoided_rows),
            "row_copy_fallback_rows": int(normalized.row_copy_fallback_rows),
            "write_duration_ms": float(write_duration_ms),
            "write_path": str(write_path),
        }

    def get_snapshot(self) -> dict[str, Any]:
        with self._state_lock:
            now_ts_ms = int(time.time() * 1000)
            write_circuit_open = self._write_circuit_is_open_locked(now_ts_ms)
            metrics = dict(self._metrics)
            pool_ready = self._pool is not None
            schema_ready = bool(self._schema_ready)
            schema_error = self._schema_error
            schema_validation = dict(self._schema_validation)
            policy_status = dict(self._policy_status)
            durability = refetchable_pg_durability_snapshot()
            last_error = self._last_error
            last_error_ts_ms = int(self._last_error_ts_ms or 0)
            last_connect_ts_ms = int(self._last_connect_ts_ms or 0)
        last_write_ts_ms = int(metrics.get("last_write_ts_ms") or 0)
        age_s = round((time.time() * 1000 - last_write_ts_ms) / 1000.0, 1) if last_write_ts_ms > 0 else None
        schema_ok = not (
            list(schema_validation.get("missing_tables") or [])
            or dict(schema_validation.get("missing_columns") or {})
            or list(schema_validation.get("missing_indexes") or [])
        )
        return {
            "ok": (not self.enabled)
            or (
                bool(pool_ready)
                and bool(schema_ready)
                and bool(schema_ok)
                and not schema_error
                and not last_error
                and not bool(write_circuit_open)
            ),
            "enabled": bool(self.enabled),
            "dsn_configured": bool(self._config.dsn),
            "pool_ready": bool(pool_ready),
            "pool_min_size": int(self._config.pool_min_size),
            "pool_max_size": int(self._config.pool_max_size),
            "schema_ready": bool(schema_ready),
            "schema_ok": bool(schema_ok),
            "schema_error": schema_error,
            "schema_name": str(self._config.schema_name),
            "schema_validation": schema_validation,
            "policy_status": policy_status,
            "durability": durability,
            "connect_timeout_s": float(self._config.connect_timeout_s),
            "command_timeout_s": float(self._config.command_timeout_s),
            "lock_timeout_s": float(self._config.lock_timeout_s),
            "copy_enabled": bool(self._config.copy_enabled),
            "copy_fallback_enabled": bool(self._config.copy_fallback_enabled),
            "copy_fallbacks": int(metrics.get("copy_fallbacks") or 0),
            "circuit_failure_threshold": int(self._config.circuit_failure_threshold),
            "circuit_open_s": float(self._config.circuit_open_s),
            "write_circuit_open": bool(write_circuit_open),
            "write_circuit_open_until_ts_ms": (
                int(metrics.get("write_circuit_open_until_ts_ms") or 0)
                if int(metrics.get("write_circuit_open_until_ts_ms") or 0) > 0
                else None
            ),
            "write_circuit_last_opened_ts_ms": (
                int(metrics.get("write_circuit_last_opened_ts_ms") or 0)
                if int(metrics.get("write_circuit_last_opened_ts_ms") or 0) > 0
                else None
            ),
            "write_circuit_opened_count": int(metrics.get("write_circuit_opened_count") or 0),
            "write_circuit_rejected_batches": int(metrics.get("write_circuit_rejected_batches") or 0),
            "write_circuit_consecutive_failures": int(
                metrics.get("write_circuit_consecutive_failures") or 0
            ),
            "write_circuit_last_failure": str(metrics.get("write_circuit_last_failure") or ""),
            "write_circuit_last_failure_class": str(metrics.get("write_circuit_last_failure_class") or ""),
            "backpressure_active": bool(write_circuit_open),
            "last_write_path": str(metrics.get("last_write_path") or ""),
            "last_copy_unavailable": str(metrics.get("last_copy_unavailable") or ""),
            "last_error": last_error,
            "last_error_ts_ms": (int(last_error_ts_ms) if last_error_ts_ms > 0 else None),
            "last_connect_ts_ms": (int(last_connect_ts_ms) if last_connect_ts_ms > 0 else None),
            "retry_count": int(metrics.get("retry_count") or 0),
            "write_batches": int(metrics.get("write_batches") or 0),
            "written_prices": int(metrics.get("written_prices") or 0),
            "written_quotes": int(metrics.get("written_quotes") or 0),
            "written_raw": int(metrics.get("written_raw") or 0),
            "dropped_rows": int(metrics.get("dropped_rows") or 0),
            "copy_batches": int(metrics.get("copy_batches") or 0),
            "copy_rows": int(metrics.get("copy_rows") or 0),
            "values_batches": int(metrics.get("values_batches") or 0),
            "values_rows": int(metrics.get("values_rows") or 0),
            "write_failures": int(metrics.get("write_failures") or 0),
            "retryable_failures": int(metrics.get("retryable_failures") or 0),
            "fatal_failures": int(metrics.get("fatal_failures") or 0),
            "pool_resets": int(metrics.get("pool_resets") or 0),
            "last_failure_class": str(metrics.get("last_failure_class") or ""),
            "last_failure_reason": str(metrics.get("last_failure_reason") or ""),
            "last_failure_retryable": bool(metrics.get("last_failure_retryable")),
            "last_failure_reset_pool": bool(metrics.get("last_failure_reset_pool")),
            "last_write_duration_ms": int(metrics.get("last_write_duration_ms") or 0),
            "total_write_duration_ms": int(metrics.get("total_write_duration_ms") or 0),
            "normalization_input_rows": int(metrics.get("normalization_input_rows") or 0),
            "normalization_safe_float_calls": int(
                metrics.get("normalization_safe_float_calls") or 0
            ),
            "normalization_safe_int_calls": int(
                metrics.get("normalization_safe_int_calls") or 0
            ),
            "normalization_datetime_conversions": int(
                metrics.get("normalization_datetime_conversions") or 0
            ),
            "normalization_symbol_parses": int(
                metrics.get("normalization_symbol_parses") or 0
            ),
            "normalization_event_key_normalizations": int(
                metrics.get("normalization_event_key_normalizations") or 0
            ),
            "row_copy_avoided_rows": int(metrics.get("row_copy_avoided_rows") or 0),
            "row_copy_fallback_rows": int(metrics.get("row_copy_fallback_rows") or 0),
            "last_write_ts_ms": (int(last_write_ts_ms) if last_write_ts_ms > 0 else None),
            "age_s": age_s,
            "ts_ms": int(time.time() * 1000),
        }


def get_price_storage() -> PostgresPriceStorage:
    """Return the process-wide Postgres price-storage singleton."""
    global _STORE
    if _STORE is None:
        with _STORE_LOCK:
            if _STORE is None:
                _STORE = PostgresPriceStorage()
    return _STORE


def init_pg_price_storage() -> dict[str, Any]:
    """Start the process-wide Postgres price-storage sidecar."""
    try:
        return get_price_storage().start()
    except Exception as exc:
        _warn_nonfatal("STORAGE_PG_PRICES_INIT_FAILED", exc)
        return {
            "ok": False,
            "enabled": bool(PostgresPriceStorageConfig.from_env().enabled),
            "dsn_configured": bool(PostgresPriceStorageConfig.from_env().dsn),
            "last_error": f"{type(exc).__name__}:{exc}",
            "ts_ms": int(time.time() * 1000),
        }


def shutdown_pg_price_storage() -> dict[str, Any]:
    """Stop the process-wide Postgres price-storage sidecar."""
    global _STORE
    with _STORE_LOCK:
        store = _STORE
        _STORE = None
    if store is None:
        return {
            "ok": True,
            "enabled": False,
            "pool_ready": False,
            "schema_ready": False,
            "detail": "pg_price_storage_not_started",
            "ts_ms": int(time.time() * 1000),
        }
    snapshot = dict(store.close() or {})
    snapshot["detail"] = "pg_price_storage_stopped"
    return snapshot


__all__ = [
    "PostgresPriceStorage",
    "PostgresPriceStorageConfig",
    "get_price_storage",
    "init_pg_price_storage",
    "shutdown_pg_price_storage",
]
