"""Read-routing helpers for price queries during SQLite -> Timescale cutover."""

from __future__ import annotations

import atexit
import os
import threading
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Tuple

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.pg_connection_hygiene import (
    rollback_if_in_transaction,
    transaction_status_name,
)
from engine.runtime.price_migration_validation import get_price_migration_validation_snapshot
from engine.runtime.price_timescale_schema import (
    price_timescale_time_after_ms_predicate,
    price_timescale_time_ref,
    price_timescale_ts_ms_expr,
)
from engine.runtime.state_cache import cache_get_or_load
from engine.runtime.storage import connect_ro
from engine.runtime.storage_pg_prices import (
    ConnectionPool,
    PostgresPriceStorageConfig,
    _quote_ident,
    psycopg,
)

LOG = get_logger("runtime.price_read_router")
_READ_BACKEND = str(os.environ.get("PRICE_READ_BACKEND", "auto") or "auto").strip().lower()
_READ_FALLBACK_TO_SQLITE = str(os.environ.get("PRICE_READ_FALLBACK_TO_SQLITE", "1")).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
_READ_REQUIRE_VALIDATION = str(os.environ.get("PRICE_READ_REQUIRE_VALIDATION", "1")).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
_READ_BACKEND_MODES = {"auto", "sqlite", "timescale"}
_READ_CACHE_TTL_S = 0.75
_POOL_ROLE = "price_read"
_CONFIG_ENV_KEYS = (
    "INGESTION_TUNING_PROFILE",
    "TIMESCALE_PRICES_DSN",
    "TIMESCALE_DSN",
    "TIMESCALE_URL",
    "TIMESCALE_DATABASE_URL",
    "TIMESCALE_PRICES_ENABLED",
    "TIMESCALE_PRICES_SCHEMA",
    "TIMESCALE_SCHEMA",
    "TIMESCALE_PRICES_POOL_MIN_SIZE",
    "TIMESCALE_PRICES_POOL_MAX_SIZE",
    "TIMESCALE_PRICES_CONNECT_TIMEOUT_S",
    "TIMESCALE_PRICES_LOCK_TIMEOUT_S",
    "TIMESCALE_PRICES_COMMAND_TIMEOUT_S",
    "TIMESCALE_PRICES_IDLE_IN_TXN_TIMEOUT_S",
    "TIMESCALE_PRICES_APPLICATION_NAME",
    "TIMESCALE_PRICES_RETENTION_DAYS",
    "TIMESCALE_RETENTION_DAYS",
    "TIMESCALE_PRICES_COMPRESSION_AFTER_DAYS",
    "TIMESCALE_COMPRESSION_AFTER_DAYS",
    "TIMESCALE_PRICES_COPY_ENABLED",
    "TIMESCALE_PRICES_COPY_FALLBACK_ENABLED",
    "ASYNC_PRICE_WRITER_ENABLED",
    "ASYNC_PRICE_WRITER_WORKERS",
)
_PG_PASSWORD_ENV_KEYS = (
    "TS_PG_PASSWORD_FILE",
    "TIMESCALE_PASSWORD_FILE",
    "TS_PG_PASSWORD_APP_FILE",
    "TS_PG_APP_PASSWORD_FILE",
    "TS_PG_PASSWORD_INGEST_FILE",
    "TS_PG_INGEST_PASSWORD_FILE",
    "TS_PG_PASSWORD_READER_FILE",
    "TS_PG_READER_PASSWORD_FILE",
    "PGPASSWORD_FILE",
    "TS_PG_PASSWORD_SECRET",
    "TIMESCALE_PASSWORD_SECRET",
    "TS_PG_PASSWORD_APP_SECRET",
    "TS_PG_APP_PASSWORD_SECRET",
    "TS_PG_PASSWORD_INGEST_SECRET",
    "TS_PG_INGEST_PASSWORD_SECRET",
    "TS_PG_PASSWORD_READER_SECRET",
    "TS_PG_READER_PASSWORD_SECRET",
    "PGPASSWORD_SECRET",
    "TS_PG_PASSWORD",
    "TIMESCALE_PASSWORD",
    "TS_PG_PASSWORD_APP",
    "TS_PG_APP_PASSWORD",
    "TS_PG_PASSWORD_INGEST",
    "TS_PG_INGEST_PASSWORD",
    "TS_PG_PASSWORD_READER",
    "TS_PG_READER_PASSWORD",
    "PGPASSWORD",
    "TS_SECRETS_PROVIDER",
    "TS_DEV_SECRETS_DIR",
    "CREDENTIALS_DIRECTORY",
)
_POOL_LOCK = threading.RLock()
_CONFIG_KEY: tuple[Any, ...] | None = None
_CONFIG: PostgresPriceStorageConfig | None = None
_ACTIVE_POOL_KEY: tuple[Any, ...] | None = None
_POOLS: dict[tuple[Any, ...], Any] = {}
_TIMESCALE_TABLE_COLUMNS_CACHE: dict[tuple[Any, ...], dict[str, str]] = {}


def _file_fingerprint(path: str) -> tuple[Any, ...]:
    text = str(path or "").strip()
    if not text:
        return ("", None)
    try:
        stat = os.stat(os.path.expanduser(text))
    except OSError:
        return (text, "missing")
    return (text, int(stat.st_mtime_ns), int(stat.st_size))


def _env_fingerprint(keys: tuple[str, ...]) -> tuple[Any, ...]:
    items: list[Any] = []
    for key in keys:
        value = os.environ.get(key)
        items.append((key, value))
        if key.endswith("_FILE") and value:
            items.append((f"{key}:stat", _file_fingerprint(value)))
    return tuple(items)


def _get_price_config() -> PostgresPriceStorageConfig:
    global _CONFIG, _CONFIG_KEY
    key = _env_fingerprint(_CONFIG_ENV_KEYS + _PG_PASSWORD_ENV_KEYS)
    with _POOL_LOCK:
        if _CONFIG is not None and _CONFIG_KEY == key:
            return _CONFIG
        config = PostgresPriceStorageConfig.from_env()
        _CONFIG = config
        _CONFIG_KEY = key
        return config


def _session_timeout_ms(timeout_s: Any) -> int:
    try:
        seconds = float(timeout_s)
    except (TypeError, ValueError):
        seconds = 1.0
    if seconds < 1.0:
        seconds = 1.0
    return int(seconds * 1000)


def _warn_nonfatal(code: str, error: BaseException, **extra: Any) -> None:
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=30,
        component="engine.runtime.price_read_router",
        extra=dict(extra or {}) or None,
        persist=False,
    )


def _check_timescale_price_read_connection(con: Any) -> None:
    rollback_if_in_transaction(
        con,
        logger=LOG,
        context="price_read_pool_check",
    )
    check_connection = getattr(ConnectionPool, "check_connection", None)
    if callable(check_connection):
        check_connection(con)


def _reset_timescale_price_read_connection(con: Any) -> None:
    rollback_if_in_transaction(
        con,
        logger=LOG,
        context="price_read_pool_reset",
    )


def _sqlite_table_exists(con: Any, name: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (str(name),),
    ).fetchone()
    return bool(row)


def _sqlite_table_columns(con: Any, name: str) -> Dict[str, str]:
    try:
        rows = con.execute(f"PRAGMA table_info({_quote_ident(str(name))})").fetchall() or []
    except Exception:
        return {}
    return {
        str(row[1] or "").strip().lower(): str(row[1] or "").strip()
        for row in rows
        if len(row) > 1 and str(row[1] or "").strip()
    }


def _pick_column(columns: Dict[str, str], *names: str) -> str:
    for name in names:
        key = str(name or "").strip().lower()
        if key in columns:
            return columns[key]
    return ""


def _sqlite_ts_ms_expr(timestamp_column: str) -> str:
    column_ref = _quote_ident(timestamp_column)
    if str(timestamp_column).strip().lower() == "ts_ms":
        return f"CAST({column_ref} AS INTEGER)"
    return (
        "CASE "
        f"WHEN typeof({column_ref}) IN ('integer','real') THEN CAST({column_ref} AS INTEGER) "
        f"ELSE CAST(strftime('%s', {column_ref}) AS INTEGER) * 1000 "
        "END"
    )


def _sqlite_last_expr(columns: Dict[str, str]) -> str:
    last_col = _pick_column(columns, "last")
    if last_col:
        return f"CAST({_quote_ident(last_col)} AS REAL)"
    price_col = _pick_column(columns, "price")
    px_col = _pick_column(columns, "px")
    if price_col and px_col:
        return f"CAST(COALESCE({_quote_ident(price_col)}, {_quote_ident(px_col)}) AS REAL)"
    if price_col:
        return f"CAST({_quote_ident(price_col)} AS REAL)"
    if px_col:
        return f"CAST({_quote_ident(px_col)} AS REAL)"
    return ""


def _sqlite_volume_expr(columns: Dict[str, str]) -> str:
    volume_col = _pick_column(columns, "volume")
    return f"CAST({_quote_ident(volume_col)} AS REAL)" if volume_col else "NULL"


def _is_postgres_storage_connection(con: Any) -> bool:
    module_name = str(getattr(type(con), "__module__", "") or "")
    if module_name.endswith("storage_pg"):
        return True
    raw = getattr(con, "raw", None)
    return bool(raw is not None and hasattr(raw, "pgconn"))


def _timescale_table_columns(cur: Any, schema_name: str, table_name: str) -> Dict[str, str]:
    cache_key = (_CONFIG_KEY, str(schema_name), str(table_name))
    cached = _TIMESCALE_TABLE_COLUMNS_CACHE.get(cache_key)
    if cached is not None:
        return dict(cached)
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
        """,
        (str(schema_name), str(table_name)),
    )
    rows = cur.fetchall() or []
    columns = {
        str(row[0] or "").strip().lower(): str(row[0] or "").strip()
        for row in rows
        if row and str(row[0] or "").strip()
    }
    _TIMESCALE_TABLE_COLUMNS_CACHE[cache_key] = dict(columns)
    return columns


def _timescale_ts_ms_expr_for_column(timestamp_column: str) -> str:
    if str(timestamp_column).strip().lower() == "time":
        return price_timescale_ts_ms_expr()
    return f"{_quote_ident(timestamp_column)}::BIGINT"


def _timescale_since_predicate(timestamp_column: str, *, placeholder: str = "%s") -> str:
    if str(timestamp_column).strip().lower() == "time":
        return price_timescale_time_after_ms_predicate(placeholder=placeholder)
    return f"{_quote_ident(timestamp_column)} > {str(placeholder)}"


def _timescale_order_expr(timestamp_column: str) -> str:
    if str(timestamp_column).strip().lower() == "time":
        return price_timescale_time_ref()
    return _quote_ident(timestamp_column)


def _compat_ts_ms_expr(timestamp_column: str, *, postgres: bool) -> str:
    if not postgres:
        return _sqlite_ts_ms_expr(timestamp_column)
    return _timescale_ts_ms_expr_for_column(timestamp_column)


def _compat_since_predicate(timestamp_column: str, *, postgres: bool, placeholder: str = "?") -> str:
    if not postgres:
        return f"{_sqlite_ts_ms_expr(timestamp_column)} > {str(placeholder)}"
    return _timescale_since_predicate(timestamp_column, placeholder=placeholder)


def _compat_order_expr(timestamp_column: str, *, postgres: bool) -> str:
    if not postgres:
        return _sqlite_ts_ms_expr(timestamp_column)
    return _timescale_order_expr(timestamp_column)


def _timescale_last_expr(columns: Dict[str, str]) -> str:
    last_col = _pick_column(columns, "last")
    if last_col:
        return _quote_ident(last_col)
    price_col = _pick_column(columns, "price")
    px_col = _pick_column(columns, "px")
    if price_col and px_col:
        return f"COALESCE({_quote_ident(price_col)}, {_quote_ident(px_col)})"
    if price_col:
        return _quote_ident(price_col)
    if px_col:
        return _quote_ident(px_col)
    return ""


def _timescale_volume_expr(columns: Dict[str, str]) -> str:
    volume_col = _pick_column(columns, "volume")
    return _quote_ident(volume_col) if volume_col else "NULL::DOUBLE PRECISION"


def _timescale_enabled() -> bool:
    config = _get_price_config()
    return bool(config.enabled and config.dsn and psycopg is not None and ConnectionPool is not None)


def _read_backend_mode() -> str:
    return str(_READ_BACKEND if _READ_BACKEND in _READ_BACKEND_MODES else "auto")


def get_price_read_backend() -> str:
    if _read_backend_mode() == "sqlite":
        return "sqlite"
    if _timescale_enabled():
        if _READ_REQUIRE_VALIDATION:
            try:
                snapshot = dict(get_price_migration_validation_snapshot() or {})
            except Exception as exc:
                _warn_nonfatal("PRICE_READ_ROUTER_VALIDATION_FETCH_FAILED", exc)
                return "sqlite"
            if not bool(snapshot.get("enabled")):
                return "sqlite"
            if not bool(snapshot.get("ok")):
                return "sqlite"
        return "timescale"
    return "sqlite"


def _pool_application_name(config: PostgresPriceStorageConfig) -> str:
    base = str(config.application_name or "trading-system-price-storage").strip() or "trading-system-price-storage"
    suffix = "read-router"
    return base if suffix in base else f"{base}-{suffix}"


def _pool_key(config: PostgresPriceStorageConfig) -> tuple[Any, ...]:
    return (
        _POOL_ROLE,
        str(config.dsn),
        str(config.schema_name or "public"),
        int(config.pool_min_size),
        int(config.pool_max_size),
        float(config.connect_timeout_s),
        float(config.lock_timeout_s),
        float(config.command_timeout_s),
        float(config.idle_in_txn_timeout_s),
        _pool_application_name(config),
    )


def _close_pool(pool: Any, *, timeout_s: float) -> None:
    try:
        pool.close(timeout=float(timeout_s))
    except TypeError:
        try:
            pool.close()
        except Exception as exc:
            LOG.debug("price read pool close failed: %s", exc, exc_info=True)
    except Exception as exc:
        LOG.debug("price read pool close failed: %s", exc, exc_info=True)


def close_timescale_price_read_pool() -> None:
    global _ACTIVE_POOL_KEY
    with _POOL_LOCK:
        pools = list(_POOLS.items())
        _POOLS.clear()
        _TIMESCALE_TABLE_COLUMNS_CACHE.clear()
        _ACTIVE_POOL_KEY = None
    for key, pool in pools:
        timeout_s = float(key[5]) if len(key) > 5 else 1.0
        _close_pool(pool, timeout_s=timeout_s)


def _get_timescale_price_read_pool(config: PostgresPriceStorageConfig) -> Any:
    global _ACTIVE_POOL_KEY
    if ConnectionPool is None or psycopg is None:
        raise RuntimeError("timescale_price_reader_pool_unavailable")
    key = _pool_key(config)
    with _POOL_LOCK:
        pool = _POOLS.get(key)
        if pool is not None:
            _ACTIVE_POOL_KEY = key
            return pool
        old_keys = [pool_key for pool_key in _POOLS if pool_key[0] == _POOL_ROLE]
        for old_key in old_keys:
            old_pool = _POOLS.pop(old_key, None)
            if old_pool is not None:
                timeout_s = float(old_key[5]) if len(old_key) > 5 else float(config.connect_timeout_s)
                _close_pool(old_pool, timeout_s=timeout_s)
        pool = ConnectionPool(
            conninfo=str(config.dsn),
            min_size=int(config.pool_min_size),
            max_size=int(config.pool_max_size),
            timeout=float(config.connect_timeout_s),
            kwargs={
                "connect_timeout": int(max(1, round(float(config.connect_timeout_s)))),
                "application_name": _pool_application_name(config),
            },
            check=_check_timescale_price_read_connection,
            reset=_reset_timescale_price_read_connection,
            open=False,
        )
        try:
            pool.open(wait=True, timeout=float(config.connect_timeout_s))
        except Exception:
            _close_pool(pool, timeout_s=float(config.connect_timeout_s))
            raise
        _POOLS[key] = pool
        _ACTIVE_POOL_KEY = key
        return pool


def _prepare_timescale_connection(con: Any, config: PostgresPriceStorageConfig) -> None:
    try:
        con.autocommit = True
    except Exception as exc:
        _warn_nonfatal("PRICE_READ_ROUTER_AUTOCOMMIT_SET_FAILED", exc)
    with con.cursor() as cur:
        cur.execute(f"SET SESSION statement_timeout = {_session_timeout_ms(config.command_timeout_s)}")
        cur.execute(f"SET SESSION lock_timeout = {_session_timeout_ms(config.lock_timeout_s)}")
        cur.execute(
            "SET SESSION idle_in_transaction_session_timeout = "
            f"{_session_timeout_ms(config.idle_in_txn_timeout_s)}"
        )
        cur.execute("SET SESSION TIME ZONE 'UTC'")


@contextmanager
def _timescale_connection():
    config = _get_price_config()
    if psycopg is None or ConnectionPool is None or not str(config.dsn or "").strip():
        raise RuntimeError("timescale_price_reader_not_configured")
    pool = _get_timescale_price_read_pool(config)
    con = pool.getconn(timeout=float(config.connect_timeout_s))
    discard = False
    try:
        rollback_if_in_transaction(
            con,
            logger=LOG,
            context="price_read_acquire",
        )
        _prepare_timescale_connection(con, config)
        yield con, str(config.schema_name or "public")
    except Exception as exc:
        discard = True
        _warn_nonfatal(
            "PRICE_READ_ROUTER_TIMESCALE_CONNECTION_FAILED",
            exc,
            transaction_status=transaction_status_name(con),
        )
        try:
            con.rollback()
        except Exception as exc:
            _warn_nonfatal("PRICE_READ_ROUTER_ROLLBACK_FAILED", exc)
        raise
    finally:
        if not discard:
            try:
                rollback_if_in_transaction(
                    con,
                    logger=LOG,
                    context="price_read_release",
                )
            except Exception:
                discard = True
        if discard:
            try:
                con.close()
            except Exception as exc:
                _warn_nonfatal("PRICE_READ_ROUTER_CONNECTION_CLOSE_FAILED", exc)
        try:
            pool.putconn(con)
        except Exception as exc:
            _warn_nonfatal("PRICE_READ_ROUTER_POOL_RETURN_FAILED", exc)


atexit.register(close_timescale_price_read_pool)


def _cached_read(cache_key: str, loader: Any) -> Any:
    return cache_get_or_load("price_read_router", str(cache_key), loader, ttl_s=_READ_CACHE_TTL_S)


def _fetch_timescale_price_rows(*, symbol: str = "", limit: int = 200) -> List[Dict[str, Any]]:
    with _timescale_connection() as (con, schema_name):
        schema_ref = _quote_ident(schema_name)
        time_ref = price_timescale_time_ref()
        ts_ms_expr = price_timescale_ts_ms_expr()
        params: List[Any] = []
        where_sql = ""
        if symbol:
            where_sql = "WHERE symbol = %s"
            params.append(str(symbol))
        params.append(int(limit))
        sql = f"""
            SELECT
              {ts_ms_expr} AS ts_ms,
              symbol,
              last AS price,
              last AS px,
              COALESCE(source, provider, 'timescale') AS source
            FROM {schema_ref}.price_ticks
            {where_sql}
            ORDER BY {time_ref} DESC
            LIMIT %s
        """
        with con.cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []
        return [
            {
                "ts_ms": int(row[0] or 0),
                "symbol": str(row[1] or ""),
                "price": (float(row[2]) if row[2] is not None else None),
                "px": (float(row[3]) if row[3] is not None else None),
                "source": (str(row[4]) if row[4] is not None else None),
            }
            for row in rows
        ]


def _fetch_sqlite_price_rows(*, symbol: str = "", limit: int = 200) -> List[Dict[str, Any]]:
    con = connect_ro()
    try:
        rows = []
        if _sqlite_table_exists(con, "prices"):
            if symbol:
                rows = con.execute(
                    """
                    SELECT ts_ms, symbol, COALESCE(price, px) AS price, px, source
                    FROM prices
                    WHERE symbol = ?
                    ORDER BY ts_ms DESC
                    LIMIT ?
                    """,
                    (str(symbol), int(limit)),
                ).fetchall() or []
            else:
                rows = con.execute(
                    """
                    SELECT ts_ms, symbol, COALESCE(price, px) AS price, px, source
                    FROM prices
                    ORDER BY ts_ms DESC
                    LIMIT ?
                    """,
                    (int(limit),),
                ).fetchall() or []
        elif _sqlite_table_exists(con, "price_quotes"):
            if symbol:
                rows = con.execute(
                    """
                    SELECT ts_ms, symbol, last AS price, last AS px, 'price_quotes' AS source
                    FROM price_quotes
                    WHERE symbol = ?
                    ORDER BY ts_ms DESC
                    LIMIT ?
                    """,
                    (str(symbol), int(limit)),
                ).fetchall() or []
            else:
                rows = con.execute(
                    """
                    SELECT ts_ms, symbol, last AS price, last AS px, 'price_quotes' AS source
                    FROM price_quotes
                    ORDER BY ts_ms DESC
                    LIMIT ?
                    """,
                    (int(limit),),
                ).fetchall() or []
        elif _sqlite_table_exists(con, "price_quotes_raw"):
            if symbol:
                rows = con.execute(
                    """
                    SELECT ts_ms, symbol, last AS price, last AS px, 'price_quotes_raw' AS source
                    FROM price_quotes_raw
                    WHERE symbol = ?
                    ORDER BY ts_ms DESC
                    LIMIT ?
                    """,
                    (str(symbol), int(limit)),
                ).fetchall() or []
            else:
                rows = con.execute(
                    """
                    SELECT ts_ms, symbol, last AS price, last AS px, 'price_quotes_raw' AS source
                    FROM price_quotes_raw
                    ORDER BY ts_ms DESC
                    LIMIT ?
                    """,
                    (int(limit),),
                ).fetchall() or []
        return [
            {
                "ts_ms": int(row[0] or 0),
                "symbol": str(row[1] or ""),
                "price": (float(row[2]) if row[2] is not None else None),
                "px": (float(row[3]) if row[3] is not None else None),
                "source": (str(row[4]) if row[4] is not None else None),
            }
            for row in rows
        ]
    finally:
        con.close()


def fetch_price_rows(*, symbol: str = "", limit: int = 200) -> List[Dict[str, Any]]:
    symbol_key = str(symbol or "").strip().upper()
    bounded_limit = max(1, min(5000, int(limit or 200)))
    backend = get_price_read_backend()

    def _load() -> List[Dict[str, Any]]:
        if backend == "timescale":
            try:
                return _fetch_timescale_price_rows(symbol=symbol_key, limit=bounded_limit)
            except Exception as exc:
                _warn_nonfatal(
                    "PRICE_READ_ROUTER_TIMESCALE_FETCH_FAILED",
                    exc,
                    symbol=str(symbol_key),
                    limit=int(bounded_limit),
                )
                if not _READ_FALLBACK_TO_SQLITE:
                    raise
        return _fetch_sqlite_price_rows(symbol=symbol_key, limit=bounded_limit)

    return _cached_read(f"price_rows:{backend}:{symbol_key}:{bounded_limit}", _load)


def _quote_rows_as_tuples(rows: List[Any]) -> List[Tuple[int, Optional[float], Optional[float]]]:
    out = [
        (
            int(row[0] or 0),
            (float(row[1]) if row[1] is not None else None),
            (float(row[2]) if row[2] is not None else None),
        )
        for row in rows
    ]
    return sorted(out, key=lambda row: int(row[0] or 0))


def _fetch_timescale_quote_rows(*, symbol: str, since_ts_ms: int, limit: int) -> List[Tuple[int, Optional[float], Optional[float]]]:
    with _timescale_connection() as (con, schema_name):
        schema_ref = _quote_ident(schema_name)
        params = [str(symbol), int(since_ts_ms), int(limit)]
        with con.cursor() as cur:
            rows = []
            for table_name in ("price_quotes", "price_quotes_raw", "price_ticks", "prices"):
                columns = _timescale_table_columns(cur, schema_name, table_name)
                timestamp_col = _pick_column(columns, "time", "ts_ms")
                last_expr = _timescale_last_expr(columns)
                symbol_col = _pick_column(columns, "symbol")
                if not (timestamp_col and last_expr and symbol_col):
                    continue
                volume_expr = _timescale_volume_expr(columns)
                ts_ms_expr = _timescale_ts_ms_expr_for_column(timestamp_col)
                since_predicate = _timescale_since_predicate(timestamp_col, placeholder="%s")
                order_expr = _timescale_order_expr(timestamp_col)
                cur.execute(
                    f"""
                    SELECT ts_ms, last, volume
                    FROM (
                      SELECT
                        {ts_ms_expr} AS ts_ms,
                        {last_expr} AS last,
                        {volume_expr} AS volume
                      FROM {schema_ref}.{_quote_ident(table_name)}
                      WHERE {_quote_ident(symbol_col)} = %s
                        AND {since_predicate}
                      ORDER BY {order_expr} DESC
                      LIMIT %s
                    ) newest_rows
                    ORDER BY ts_ms ASC
                    """,
                    tuple(params),
                )
                rows = cur.fetchall() or []
                if rows:
                    break
        return _quote_rows_as_tuples(rows)


def _fetch_sqlite_quote_rows(*, symbol: str, since_ts_ms: int, limit: int) -> List[Tuple[int, Optional[float], Optional[float]]]:
    con = connect_ro()
    try:
        postgres = _is_postgres_storage_connection(con)
        rows = []
        for table_name in ("price_quotes", "price_quotes_raw", "price_ticks", "prices"):
            if not _sqlite_table_exists(con, table_name):
                continue
            columns = _sqlite_table_columns(con, table_name)
            timestamp_col = _pick_column(columns, "ts_ms", "time")
            last_expr = _sqlite_last_expr(columns)
            symbol_col = _pick_column(columns, "symbol")
            if not (timestamp_col and last_expr and symbol_col):
                continue
            ts_ms_expr = _compat_ts_ms_expr(timestamp_col, postgres=postgres)
            since_predicate = _compat_since_predicate(timestamp_col, postgres=postgres, placeholder="?")
            order_expr = _compat_order_expr(timestamp_col, postgres=postgres)
            volume_expr = _sqlite_volume_expr(columns)
            rows = con.execute(
                f"""
                SELECT ts_ms, last, volume
                FROM (
                  SELECT {ts_ms_expr} AS ts_ms, {last_expr} AS last, {volume_expr} AS volume
                  FROM {_quote_ident(table_name)}
                  WHERE {_quote_ident(symbol_col)}=?
                    AND {since_predicate}
                  ORDER BY {order_expr} DESC
                  LIMIT ?
                ) newest_rows
                ORDER BY ts_ms ASC
                """,
                (str(symbol), int(since_ts_ms), int(limit)),
            ).fetchall() or []
            if rows:
                break
        return _quote_rows_as_tuples(rows)
    finally:
        con.close()


def fetch_quote_rows(*, symbol: str, since_ts_ms: int, limit: int) -> List[Tuple[int, Optional[float], Optional[float]]]:
    backend = get_price_read_backend()
    if backend == "timescale":
        try:
            return _fetch_timescale_quote_rows(symbol=symbol, since_ts_ms=since_ts_ms, limit=limit)
        except Exception as exc:
            _warn_nonfatal(
                "PRICE_READ_ROUTER_TIMESCALE_QUOTE_FETCH_FAILED",
                exc,
                symbol=str(symbol),
                limit=int(limit),
            )
            if not _READ_FALLBACK_TO_SQLITE:
                raise
    return _fetch_sqlite_quote_rows(symbol=symbol, since_ts_ms=since_ts_ms, limit=limit)
