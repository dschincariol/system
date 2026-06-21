"""Thread-safe Timescale/Postgres storage for price-related tables only."""

from __future__ import annotations

import logging
import math
import os
import random
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

try:
    import psycopg
    from psycopg_pool import ConnectionPool
except Exception:  # pragma: no cover - optional dependency at runtime
    psycopg = None  # type: ignore[assignment]
    ConnectionPool = None  # type: ignore[assignment]

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.ingestion_tuning import env_bool, tuned_float, tuned_int
from engine.runtime.logging import get_logger
from engine.runtime.metrics import emit_counter, emit_timing
from engine.runtime.observability import record_component_health
from engine.runtime.platform import connection_info_with_pg_password

LOG = get_logger("runtime.storage_pg_prices")
_STORE_LOCK = threading.Lock()
_STORE: "PostgresPriceStorage | None" = None
_PG_PRICE_SCHEMA_TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    "price_ticks": (
        "time",
        "symbol",
        "last",
        "source",
        "provider",
        "bid",
        "ask",
        "spread",
        "volume",
        "latency_ms",
        "provider_score",
        "last_update_ts_ms",
        "ingest_ts_ms",
    ),
    "price_quotes": (
        "time",
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
        "time",
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
}
_PG_PRICE_SCHEMA_INDEXES: tuple[str, ...] = (
    "price_ticks_pkey",
    "price_quotes_pkey",
    "price_quotes_raw_pkey",
    "idx_price_ticks_time_desc",
    "idx_price_quotes_time_desc",
    "idx_price_quotes_raw_time_desc",
)


def _env_bool(name: str, default: bool = False) -> bool:
    return env_bool(name, default=default)


def _env_float(name: str, default: float) -> float:
    return tuned_float(name, default, 0.0, float("inf"))


def _env_int(name: str, default: int) -> int:
    return tuned_int(name, default, 0, 2**31 - 1)


def _quote_ident(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _execute_many_values(cur: Any, sql: str, rows: Iterable[tuple[Any, ...]]) -> None:
    batch = [tuple(row) for row in rows]
    if not batch:
        return
    placeholders = "(" + ", ".join("%s" for _ in range(len(batch[0]))) + ")"
    rendered_sql = str(sql).replace("VALUES %s", f"VALUES {placeholders}", 1)
    if rendered_sql == str(sql):
        raise ValueError("batch_values_placeholder_missing")
    cur.executemany(rendered_sql, batch)


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


def _normalize_event_key(row: Mapping[str, Any]) -> str:
    raw = str(row.get("event_key") or "").strip()
    if raw:
        return raw
    provider = str(row.get("provider") or row.get("source") or "").strip().lower()
    symbol = _normalize_symbol(row.get("symbol"))
    event_type = str(row.get("event_type") or "").strip().upper() or "U"
    event_ts_ms = int(_safe_int(row.get("event_ts_ms") or row.get("ts_ms") or row.get("timestamp")) or 0)
    trade_id = str(row.get("trade_id") or "").strip()
    sequence_number = str(row.get("sequence_number") or "").strip()
    last = _safe_float(row.get("last") if row.get("last") not in (None, "") else row.get("price"))
    bid = _safe_float(row.get("bid"))
    ask = _safe_float(row.get("ask"))
    volume = _safe_float(row.get("volume"))
    return f"{provider}|{symbol}|{event_type}|{event_ts_ms}|{trade_id}|{sequence_number}|{last}|{bid}|{ask}|{volume}"


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
            "required_tables": sorted(_PG_PRICE_SCHEMA_TABLE_COLUMNS),
            "required_indexes": list(_PG_PRICE_SCHEMA_INDEXES),
            "missing_tables": [],
            "missing_columns": {},
            "missing_indexes": [],
        }
        self._policy_status: dict[str, Any] = {
            "retention_days": int(self._config.retention_days),
            "compression_after_days": int(self._config.compression_after_days),
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
            "last_write_duration_ms": 0,
            "total_write_duration_ms": 0,
            "last_write_ts_ms": 0,
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
        if pool is not None:
            try:
                pool.close(timeout=float(self._config.connect_timeout_s))
            except Exception as exc:
                self._record_error(exc)

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
            self._policy_status = {
                "retention_days": int(self._config.retention_days),
                "compression_after_days": int(self._config.compression_after_days),
                "applied": bool(applied),
                "last_error": str(last_error or ""),
            }

    def _apply_timescale_policies(self, cur: Any, relation_name: str) -> None:
        if int(self._config.compression_after_days) > 0:
            cur.execute(
                f"ALTER TABLE {relation_name} SET (timescaledb.compress, timescaledb.compress_segmentby = 'symbol')"
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
        required_tables = sorted(_PG_PRICE_SCHEMA_TABLE_COLUMNS)
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
        for table_name, columns in _PG_PRICE_SCHEMA_TABLE_COLUMNS.items():
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
            cur.execute("SET SESSION statement_timeout = %s", (int(max(1.0, self._config.command_timeout_s) * 1000),))
            cur.execute("SET SESSION lock_timeout = %s", (int(max(1.0, self._config.lock_timeout_s) * 1000),))
            cur.execute(
                "SET SESSION idle_in_transaction_session_timeout = %s",
                (int(max(1.0, self._config.idle_in_txn_timeout_s) * 1000),),
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
        con.autocommit = False
        try:
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
        for attempt in range(1, int(self._config.retry_attempts) + 1):
            try:
                return callback()
            except Exception as exc:
                last_error = exc
                self._record_error(exc)
                if attempt >= int(self._config.retry_attempts):
                    break
                self._note_retry()
                self._reset_pool()
                time.sleep(self._retry_delay_s(attempt))
        raise RuntimeError(f"storage_pg_prices_{operation}_failed:{last_error}") from last_error

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

        def _ensure() -> None:
            with self._connection() as con:
                with con.cursor() as cur:
                    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_ref}")
                    cur.execute(
                        f"""
                        CREATE TABLE IF NOT EXISTS {price_ticks_ref} (
                          "time" TIMESTAMPTZ NOT NULL,
                          symbol TEXT NOT NULL,
                          last DOUBLE PRECISION,
                          source TEXT,
                          provider TEXT,
                          bid DOUBLE PRECISION,
                          ask DOUBLE PRECISION,
                          spread DOUBLE PRECISION,
                          volume DOUBLE PRECISION,
                          latency_ms INTEGER,
                          provider_score DOUBLE PRECISION,
                          last_update_ts_ms BIGINT,
                          ingest_ts_ms BIGINT,
                          PRIMARY KEY(symbol, "time")
                        )
                        """
                    )
                    cur.execute(
                        f"""
                        CREATE TABLE IF NOT EXISTS {quotes_ref} (
                          "time" TIMESTAMPTZ NOT NULL,
                          symbol TEXT NOT NULL,
                          last DOUBLE PRECISION,
                          bid DOUBLE PRECISION,
                          ask DOUBLE PRECISION,
                          spread DOUBLE PRECISION,
                          volume DOUBLE PRECISION,
                          source TEXT,
                          last_trade_ts_ms BIGINT,
                          last_quote_ts_ms BIGINT,
                          last_update_ts_ms BIGINT,
                          PRIMARY KEY(symbol, "time")
                        )
                        """
                    )
                    cur.execute(
                        f"""
                        CREATE TABLE IF NOT EXISTS {raw_ref} (
                          "time" TIMESTAMPTZ NOT NULL,
                          symbol TEXT NOT NULL,
                          provider TEXT NOT NULL,
                          event_key TEXT NOT NULL,
                          event_type TEXT,
                          event_ts_ms BIGINT,
                          last DOUBLE PRECISION,
                          bid DOUBLE PRECISION,
                          ask DOUBLE PRECISION,
                          spread DOUBLE PRECISION,
                          volume DOUBLE PRECISION,
                          trade_ts_ms BIGINT,
                          quote_ts_ms BIGINT,
                          ingest_ts_ms BIGINT,
                          source TEXT,
                          PRIMARY KEY(symbol, provider, event_key)
                        )
                        """
                    )
                    cur.execute(
                        f'CREATE INDEX IF NOT EXISTS idx_price_ticks_time_desc ON {price_ticks_ref} ("time" DESC)'
                    )
                    cur.execute(
                        f'CREATE INDEX IF NOT EXISTS idx_price_quotes_time_desc ON {quotes_ref} ("time" DESC)'
                    )
                    cur.execute(
                        f'CREATE INDEX IF NOT EXISTS idx_price_quotes_raw_time_desc ON {raw_ref} ("time" DESC)'
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
                            cur.execute(
                                "SELECT create_hypertable(%s::regclass, 'time', if_not_exists => TRUE, migrate_data => TRUE)",
                                (relation,),
                            )
                            self._apply_timescale_policies(cur, relation)
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

    def write_batch(
        self,
        *,
        prices: Iterable[Mapping[str, Any]] = (),
        quotes: Iterable[Mapping[str, Any]] = (),
        raw: Iterable[Mapping[str, Any]] = (),
    ) -> dict[str, Any]:
        if not self.enabled:
            return {"ok": True, "prices": 0, "quotes": 0, "raw": 0, "enabled": False}
        self.start()
        input_prices = [dict(row or {}) for row in (prices or [])]
        input_quotes = [dict(row or {}) for row in (quotes or [])]
        input_raw = [dict(row or {}) for row in (raw or [])]
        price_rows = [
            (
                _dt_from_ms(row.get("ts_ms") or row.get("timestamp")),
                _normalize_symbol(row.get("symbol")),
                _safe_float(row.get("price") if row.get("price") not in (None, "") else row.get("last")),
                str(row.get("source") or row.get("provider") or ""),
                str(row.get("provider") or row.get("source") or ""),
                _safe_float(row.get("bid")),
                _safe_float(row.get("ask")),
                _safe_float(row.get("spread")),
                _safe_float(row.get("volume")),
                _safe_int(row.get("latency_ms")),
                _safe_float(row.get("provider_score")),
                _safe_int(row.get("last_update_ts_ms")),
                _safe_int(row.get("ingest_ts_ms")),
            )
            for row in input_prices
            if _normalize_symbol(row.get("symbol")) and _dt_from_ms(row.get("ts_ms") or row.get("timestamp")) is not None
        ]
        quote_rows = [
            (
                _dt_from_ms(row.get("ts_ms") or row.get("timestamp")),
                _normalize_symbol(row.get("symbol")),
                _safe_float(row.get("last")),
                _safe_float(row.get("bid")),
                _safe_float(row.get("ask")),
                _safe_float(row.get("spread")),
                _safe_float(row.get("volume")),
                str(row.get("source") or row.get("provider") or ""),
                _safe_int(row.get("last_trade_ts_ms") or row.get("trade_ts_ms")),
                _safe_int(row.get("last_quote_ts_ms") or row.get("quote_ts_ms")),
                _safe_int(row.get("last_update_ts_ms")),
            )
            for row in input_quotes
            if _normalize_symbol(row.get("symbol")) and _dt_from_ms(row.get("ts_ms") or row.get("timestamp")) is not None
        ]
        raw_rows = [
            (
                _dt_from_ms(row.get("ts_ms") or row.get("timestamp")),
                _normalize_symbol(row.get("symbol")),
                str(row.get("provider") or row.get("source") or ""),
                _normalize_event_key(row),
                str(row.get("event_type") or ""),
                _safe_int(row.get("event_ts_ms") or row.get("timestamp")),
                _safe_float(row.get("last")),
                _safe_float(row.get("bid")),
                _safe_float(row.get("ask")),
                _safe_float(row.get("spread")),
                _safe_float(row.get("volume")),
                _safe_int(row.get("trade_ts_ms")),
                _safe_int(row.get("quote_ts_ms")),
                _safe_int(row.get("ingest_ts_ms")),
                str(row.get("source") or row.get("provider") or ""),
            )
            for row in input_raw
            if _normalize_symbol(row.get("symbol"))
            and str(row.get("provider") or row.get("source") or "").strip()
            and _dt_from_ms(row.get("ts_ms") or row.get("timestamp")) is not None
        ]
        dropped_rows = {
            "prices": max(0, len(input_prices) - len(price_rows)),
            "quotes": max(0, len(input_quotes) - len(quote_rows)),
            "raw": max(0, len(input_raw) - len(raw_rows)),
        }
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

        schema_ref = _quote_ident(self._config.schema_name)
        price_ticks_ref = f"{schema_ref}.price_ticks"
        quotes_ref = f"{schema_ref}.price_quotes"
        raw_ref = f"{schema_ref}.price_quotes_raw"
        def _write() -> None:
            with self._connection() as con:
                with con.cursor() as cur:
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
                        )
                    if raw_rows:
                        _execute_many_values(
                            cur,
                            f"""
                            INSERT INTO {raw_ref}(
                              "time", symbol, provider, event_key, event_type, event_ts_ms, last, bid, ask,
                              spread, volume, trade_ts_ms, quote_ts_ms, ingest_ts_ms, source
                            ) VALUES %s
                            ON CONFLICT(symbol, provider, event_key) DO UPDATE SET
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
                        )
                con.commit()

        write_started = time.perf_counter()
        self._run_with_retry(_write, operation="write_batch")
        write_duration_ms = float((time.perf_counter() - write_started) * 1000.0)
        now_ts_ms = int(time.time() * 1000)
        with self._state_lock:
            self._metrics["write_batches"] = int(self._metrics.get("write_batches") or 0) + 1
            self._metrics["written_prices"] = int(self._metrics.get("written_prices") or 0) + int(len(price_rows))
            self._metrics["written_quotes"] = int(self._metrics.get("written_quotes") or 0) + int(len(quote_rows))
            self._metrics["written_raw"] = int(self._metrics.get("written_raw") or 0) + int(len(raw_rows))
            self._metrics["last_write_duration_ms"] = int(round(write_duration_ms))
            self._metrics["total_write_duration_ms"] = int(self._metrics.get("total_write_duration_ms") or 0) + int(round(write_duration_ms))
            self._metrics["last_write_ts_ms"] = int(now_ts_ms)
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
                "write_duration_ms": int(round(write_duration_ms)),
            },
        )
        return {
            "ok": True,
            "enabled": True,
            "prices": int(len(price_rows)),
            "quotes": int(len(quote_rows)),
            "raw": int(len(raw_rows)),
            "dropped_rows": dict(dropped_rows),
            "write_duration_ms": float(write_duration_ms),
        }

    def get_snapshot(self) -> dict[str, Any]:
        with self._state_lock:
            metrics = dict(self._metrics)
            pool_ready = self._pool is not None
            schema_ready = bool(self._schema_ready)
            schema_error = self._schema_error
            schema_validation = dict(self._schema_validation)
            policy_status = dict(self._policy_status)
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
            "ok": (not self.enabled) or (bool(pool_ready) and bool(schema_ready) and bool(schema_ok) and not schema_error and not last_error),
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
            "connect_timeout_s": float(self._config.connect_timeout_s),
            "command_timeout_s": float(self._config.command_timeout_s),
            "lock_timeout_s": float(self._config.lock_timeout_s),
            "last_error": last_error,
            "last_error_ts_ms": (int(last_error_ts_ms) if last_error_ts_ms > 0 else None),
            "last_connect_ts_ms": (int(last_connect_ts_ms) if last_connect_ts_ms > 0 else None),
            "retry_count": int(metrics.get("retry_count") or 0),
            "write_batches": int(metrics.get("write_batches") or 0),
            "written_prices": int(metrics.get("written_prices") or 0),
            "written_quotes": int(metrics.get("written_quotes") or 0),
            "written_raw": int(metrics.get("written_raw") or 0),
            "dropped_rows": int(metrics.get("dropped_rows") or 0),
            "last_write_duration_ms": int(metrics.get("last_write_duration_ms") or 0),
            "total_write_duration_ms": int(metrics.get("total_write_duration_ms") or 0),
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
