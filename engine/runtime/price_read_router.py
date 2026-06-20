"""Read-routing helpers for price queries during SQLite -> Timescale cutover."""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Tuple

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.price_migration_validation import get_price_migration_validation_snapshot
from engine.runtime.storage import connect_ro
from engine.runtime.storage_pg_prices import (
    PostgresPriceStorageConfig,
    _quote_ident,
    psycopg2,
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


def _sqlite_table_exists(con: Any, name: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (str(name),),
    ).fetchone()
    return bool(row)


def _timescale_enabled() -> bool:
    config = PostgresPriceStorageConfig.from_env()
    return bool(config.enabled and config.dsn and psycopg2 is not None)


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


@contextmanager
def _timescale_connection():
    config = PostgresPriceStorageConfig.from_env()
    if psycopg2 is None or not str(config.dsn or "").strip():
        raise RuntimeError("timescale_price_reader_not_configured")
    con = psycopg2.connect(
        dsn=str(config.dsn),
        connect_timeout=int(max(1, round(float(config.connect_timeout_s)))),
        application_name="trading-system-price-read-router",
    )
    try:
        yield con, str(config.schema_name or "public")
    finally:
        con.close()


def _fetch_timescale_price_rows(*, symbol: str = "", limit: int = 200) -> List[Dict[str, Any]]:
    with _timescale_connection() as (con, schema_name):
        schema_ref = _quote_ident(schema_name)
        params: List[Any] = []
        where_sql = ""
        if symbol:
            where_sql = "WHERE symbol = %s"
            params.append(str(symbol))
        params.append(int(limit))
        sql = f"""
            SELECT
              (EXTRACT(EPOCH FROM "time") * 1000)::BIGINT AS ts_ms,
              symbol,
              last AS price,
              last AS px,
              COALESCE(source, provider, 'timescale') AS source
            FROM {schema_ref}.price_ticks
            {where_sql}
            ORDER BY "time" DESC
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
    backend = get_price_read_backend()
    if backend == "timescale":
        try:
            return _fetch_timescale_price_rows(symbol=symbol, limit=limit)
        except Exception as exc:
            _warn_nonfatal(
                "PRICE_READ_ROUTER_TIMESCALE_FETCH_FAILED",
                exc,
                symbol=str(symbol),
                limit=int(limit),
            )
            if not _READ_FALLBACK_TO_SQLITE:
                raise
    return _fetch_sqlite_price_rows(symbol=symbol, limit=limit)


def _fetch_timescale_quote_rows(*, symbol: str, since_ts_ms: int, limit: int) -> List[Tuple[int, Optional[float], Optional[float]]]:
    with _timescale_connection() as (con, schema_name):
        schema_ref = _quote_ident(schema_name)
        params = [str(symbol), int(since_ts_ms), int(limit)]
        sql = f"""
            SELECT
              (EXTRACT(EPOCH FROM "time") * 1000)::BIGINT AS ts_ms,
              last,
              volume
            FROM {schema_ref}.price_quotes
            WHERE symbol = %s
              AND "time" > TO_TIMESTAMP(%s / 1000.0)
            ORDER BY "time" ASC
            LIMIT %s
        """
        with con.cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []
            if not rows:
                cur.execute(
                    f"""
                    SELECT
                      (EXTRACT(EPOCH FROM "time") * 1000)::BIGINT AS ts_ms,
                      last,
                      volume
                    FROM {schema_ref}.price_quotes_raw
                    WHERE symbol = %s
                      AND "time" > TO_TIMESTAMP(%s / 1000.0)
                    ORDER BY "time" ASC
                    LIMIT %s
                    """,
                    tuple(params),
                )
                rows = cur.fetchall() or []
        return [
            (
                int(row[0] or 0),
                (float(row[1]) if row[1] is not None else None),
                (float(row[2]) if row[2] is not None else None),
            )
            for row in rows
        ]


def _fetch_sqlite_quote_rows(*, symbol: str, since_ts_ms: int, limit: int) -> List[Tuple[int, Optional[float], Optional[float]]]:
    con = connect_ro()
    try:
        rows = []
        if _sqlite_table_exists(con, "price_quotes"):
            rows = con.execute(
                """
                SELECT ts_ms, last, volume
                FROM price_quotes
                WHERE symbol=?
                  AND ts_ms > ?
                ORDER BY ts_ms ASC
                LIMIT ?
                """,
                (str(symbol), int(since_ts_ms), int(limit)),
            ).fetchall() or []
        if not rows and _sqlite_table_exists(con, "price_quotes_raw"):
            rows = con.execute(
                """
                SELECT ts_ms, last, volume
                FROM price_quotes_raw
                WHERE symbol=?
                  AND ts_ms > ?
                ORDER BY ts_ms ASC
                LIMIT ?
                """,
                (str(symbol), int(since_ts_ms), int(limit)),
            ).fetchall() or []
        return [
            (
                int(row[0] or 0),
                (float(row[1]) if row[1] is not None else None),
                (float(row[2]) if row[2] is not None else None),
            )
            for row in rows
        ]
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
