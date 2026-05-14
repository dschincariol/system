"""Postgres and PgBouncer observability snapshots."""

from __future__ import annotations

import logging
import os
import time
from typing import Any, Callable, Iterable

from engine.runtime.logging import get_logger
from engine.runtime.platform import is_linux

LOG = get_logger("runtime.observability.pg_stats")

MetricWriter = Callable[..., None]

_WARNED_KEYS: set[str] = set()
_SEEN_QUERY_TEXT: set[str] = set()
_LAST_QUERY_STATS: dict[str, tuple[int, float, int]] = {}
_LAST_TABLE_STATS: dict[str, tuple[int, int, int]] = {}
_LAST_DATABASE_STATS: dict[str, tuple[int, int]] = {}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _warn_once(key: str, message: str, error: BaseException | None = None) -> None:
    if key in _WARNED_KEYS:
        return
    _WARNED_KEYS.add(key)
    if error is None:
        LOG.warning(message)
    else:
        LOG.warning("%s: %s: %s", message, type(error).__name__, error)


def _row_get(row: Any, key: str, index: int | None = None, default: Any = None) -> Any:
    if row is None:
        return default
    if isinstance(row, dict):
        return row.get(key, default)
    getter = getattr(row, "get", None)
    if callable(getter):
        try:
            return getter(key, default)
        except Exception:
            logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)
    try:
        return row[key]
    except Exception:
        logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)
    if index is not None:
        try:
            return row[index]
        except Exception:
            return default
    return default


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _as_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return int(default)
        return int(value)
    except Exception:
        return int(default)


def _default_metric_writer() -> MetricWriter:
    from engine.runtime.metrics_store import write_runtime_metric

    return write_runtime_metric


def _default_storage_connect():
    from engine.runtime.storage import connect_ro_direct

    return connect_ro_direct(timeout_s=float(os.environ.get("OBSERVABILITY_PG_TIMEOUT_S", "1.0") or 1.0))


def _write_metric(
    writer: MetricWriter,
    metric: str,
    *,
    value_num: Any = None,
    value_text: Any = None,
    tags: dict[str, Any] | None = None,
    ts_ms: int,
) -> int:
    writer(
        str(metric),
        value_num=value_num,
        value_text=value_text,
        tags=dict(tags or {}),
        ts_ms=int(ts_ms),
    )
    return 1


def _pg_stat_statements_available(con: Any) -> bool:
    try:
        row = con.execute("SELECT 1 FROM pg_extension WHERE extname='pg_stat_statements' LIMIT 1").fetchone()
        if not row:
            return False
        reg = con.execute("SELECT to_regclass('pg_stat_statements')").fetchone()
        return bool(reg and _row_get(reg, "to_regclass", 0) is not None)
    except Exception as exc:
        _warn_once(
            "pg_stat_statements_noop",
            "pg_stat_statements is not installed or not visible; Postgres observability snapshot is disabled",
            exc,
        )
        return False


def _pg_stat_statement_columns(con: Any) -> set[str]:
    rows = con.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'pg_stat_statements'
        """
    ).fetchall()
    return {str(_row_get(row, "column_name", 0) or "") for row in rows or []}


def _statement_query(con: Any, *, order_by: str, limit: int) -> list[Any]:
    columns = _pg_stat_statement_columns(con)
    query_id_col = "queryid" if "queryid" in columns else "query_id"
    total_col = "total_exec_time" if "total_exec_time" in columns else "total_time"
    mean_col = "mean_exec_time" if "mean_exec_time" in columns else "mean_time"
    order_col = total_col if order_by == "total_time" else "calls"
    return con.execute(
        f"""
        SELECT
          {query_id_col}::text AS query_id,
          query,
          calls::bigint AS calls,
          {total_col}::double precision AS total_time_ms,
          {mean_col}::double precision AS mean_time_ms,
          rows::bigint AS rows
        FROM pg_stat_statements
        WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
        ORDER BY {order_col} DESC
        LIMIT {int(limit)}
        """
    ).fetchall()


def _snapshot_pg_stat_statements(con: Any, writer: MetricWriter, *, ts_ms: int, limit: int) -> int:
    emitted = 0
    seen_in_snapshot: set[tuple[str, str]] = set()
    for order_by in ("total_time", "calls"):
        rows = _statement_query(con, order_by=order_by, limit=limit)
        for rank, row in enumerate(rows or [], start=1):
            query_id = str(_row_get(row, "query_id", 0) or "").strip()
            if not query_id:
                query_id = f"rank:{order_by}:{rank}"
            calls = _as_int(_row_get(row, "calls", 2))
            total_ms = _as_float(_row_get(row, "total_time_ms", 3))
            mean_ms = _as_float(_row_get(row, "mean_time_ms", 4))
            rows_returned = _as_int(_row_get(row, "rows", 5))
            query_text = str(_row_get(row, "query", 1) or "")
            text_first_seen = query_id not in _SEEN_QUERY_TEXT
            if text_first_seen:
                _SEEN_QUERY_TEXT.add(query_id)

            tags = {
                "query_id": query_id,
                "order_by": order_by,
                "rank": int(rank),
            }
            emitted += _write_metric(
                writer,
                "pg_stat_statements.total_time_ms",
                value_num=total_ms,
                value_text=(query_text if text_first_seen else None),
                tags=tags,
                ts_ms=ts_ms,
            )
            emitted += _write_metric(
                writer,
                "pg_stat_statements.calls",
                value_num=calls,
                tags=tags,
                ts_ms=ts_ms,
            )
            emitted += _write_metric(
                writer,
                "pg_stat_statements.mean_time_ms",
                value_num=mean_ms,
                tags=tags,
                ts_ms=ts_ms,
            )
            emitted += _write_metric(
                writer,
                "pg_stat_statements.rows",
                value_num=rows_returned,
                tags=tags,
                ts_ms=ts_ms,
            )

            if (query_id, order_by) in seen_in_snapshot:
                continue
            seen_in_snapshot.add((query_id, order_by))
            previous = _LAST_QUERY_STATS.get(query_id)
            if previous is not None:
                prev_calls, prev_total_ms, prev_rows = previous
                emitted += _write_metric(
                    writer,
                    "pg_stat_statements.delta_calls",
                    value_num=max(0, calls - int(prev_calls)),
                    tags=tags,
                    ts_ms=ts_ms,
                )
                emitted += _write_metric(
                    writer,
                    "pg_stat_statements.delta_total_time_ms",
                    value_num=max(0.0, total_ms - float(prev_total_ms)),
                    tags=tags,
                    ts_ms=ts_ms,
                )
                emitted += _write_metric(
                    writer,
                    "pg_stat_statements.delta_rows",
                    value_num=max(0, rows_returned - int(prev_rows)),
                    tags=tags,
                    ts_ms=ts_ms,
                )
            _LAST_QUERY_STATS[query_id] = (int(calls), float(total_ms), int(rows_returned))
    return int(emitted)


def _snapshot_user_tables(con: Any, writer: MetricWriter, *, ts_ms: int) -> int:
    rows = con.execute(
        """
        SELECT
          schemaname,
          relname,
          (seq_scan + idx_scan)::bigint AS reads,
          (n_tup_ins + n_tup_upd + n_tup_del)::bigint AS writes,
          n_tup_ins::bigint AS inserts,
          n_tup_upd::bigint AS updates,
          n_tup_del::bigint AS deletes,
          n_dead_tup::bigint AS dead_tuples
        FROM pg_stat_user_tables
        ORDER BY (n_tup_ins + n_tup_upd + n_tup_del) DESC
        LIMIT 100
        """
    ).fetchall()
    emitted = 0
    for row in rows or []:
        schema = str(_row_get(row, "schemaname", 0) or "")
        table = str(_row_get(row, "relname", 1) or "")
        table_key = f"{schema}.{table}" if schema else table
        reads = _as_int(_row_get(row, "reads", 2))
        writes = _as_int(_row_get(row, "writes", 3))
        dead_tuples = _as_int(_row_get(row, "dead_tuples", 7))
        tags = {"schema": schema, "table": table}
        emitted += _write_metric(writer, "postgres.table.reads_total", value_num=reads, tags=tags, ts_ms=ts_ms)
        emitted += _write_metric(writer, "postgres.table.writes_total", value_num=writes, tags=tags, ts_ms=ts_ms)
        emitted += _write_metric(writer, "postgres.table.dead_tuples", value_num=dead_tuples, tags=tags, ts_ms=ts_ms)

        previous = _LAST_TABLE_STATS.get(table_key)
        if previous is not None:
            prev_ts_ms, prev_reads, prev_writes = previous
            delta_s = max(0.001, (int(ts_ms) - int(prev_ts_ms)) / 1000.0)
            read_rate = max(0, reads - int(prev_reads)) / delta_s
            write_rate = max(0, writes - int(prev_writes)) / delta_s
        else:
            read_rate = 0.0
            write_rate = 0.0
        emitted += _write_metric(
            writer,
            "postgres.table.read_rate_per_s",
            value_num=read_rate,
            tags=tags,
            ts_ms=ts_ms,
        )
        emitted += _write_metric(
            writer,
            "postgres.table.write_rate_per_s",
            value_num=write_rate,
            tags=tags,
            ts_ms=ts_ms,
        )
        _LAST_TABLE_STATS[table_key] = (int(ts_ms), int(reads), int(writes))
    return int(emitted)


def _snapshot_database(con: Any, writer: MetricWriter, *, ts_ms: int) -> int:
    row = con.execute(
        """
        SELECT
          datname,
          blks_hit::bigint AS blks_hit,
          blks_read::bigint AS blks_read,
          deadlocks::bigint AS deadlocks,
          conflicts::bigint AS conflicts,
          xact_commit::bigint AS xact_commit,
          xact_rollback::bigint AS xact_rollback,
          tup_inserted::bigint AS tup_inserted,
          tup_updated::bigint AS tup_updated,
          tup_deleted::bigint AS tup_deleted
        FROM pg_stat_database
        WHERE datname = current_database()
        """
    ).fetchone()
    if not row:
        return 0
    datname = str(_row_get(row, "datname", 0) or "")
    hits = _as_int(_row_get(row, "blks_hit", 1))
    reads = _as_int(_row_get(row, "blks_read", 2))
    deadlocks = _as_int(_row_get(row, "deadlocks", 3))
    conflicts = _as_int(_row_get(row, "conflicts", 4))
    writes = (
        _as_int(_row_get(row, "tup_inserted", 7))
        + _as_int(_row_get(row, "tup_updated", 8))
        + _as_int(_row_get(row, "tup_deleted", 9))
    )
    cache_hit_ratio = float(hits / (hits + reads)) if (hits + reads) > 0 else 1.0
    tags = {"database": datname}
    emitted = 0
    emitted += _write_metric(
        writer,
        "postgres.database.cache_hit_ratio",
        value_num=cache_hit_ratio,
        tags=tags,
        ts_ms=ts_ms,
    )
    emitted += _write_metric(writer, "postgres.database.deadlocks", value_num=deadlocks, tags=tags, ts_ms=ts_ms)
    emitted += _write_metric(writer, "postgres.database.conflicts", value_num=conflicts, tags=tags, ts_ms=ts_ms)
    previous = _LAST_DATABASE_STATS.get(datname)
    if previous is not None:
        prev_ts_ms, prev_writes = previous
        delta_s = max(0.001, (int(ts_ms) - int(prev_ts_ms)) / 1000.0)
        write_rate = max(0, writes - int(prev_writes)) / delta_s
    else:
        write_rate = 0.0
    emitted += _write_metric(
        writer,
        "postgres.database.write_rate_per_s",
        value_num=write_rate,
        tags=tags,
        ts_ms=ts_ms,
    )
    _LAST_DATABASE_STATS[datname] = (int(ts_ms), int(writes))
    return int(emitted)


def _snapshot_connections(con: Any, writer: MetricWriter, *, ts_ms: int) -> int:
    rows = con.execute(
        """
        SELECT COALESCE(state, 'unknown') AS state, COUNT(*)::bigint AS count
        FROM pg_stat_activity
        WHERE datname = current_database()
        GROUP BY COALESCE(state, 'unknown')
        """
    ).fetchall()
    emitted = 0
    for row in rows or []:
        state = str(_row_get(row, "state", 0) or "unknown")
        count = _as_int(_row_get(row, "count", 1))
        safe_state = state.replace(" ", "_").lower()
        emitted += _write_metric(
            writer,
            f"postgres.connections.{safe_state}",
            value_num=count,
            tags={"state": state},
            ts_ms=ts_ms,
        )
    return int(emitted)


def _table_exists(con: Any, table_name: str) -> bool:
    try:
        row = con.execute("SELECT to_regclass(?)", (str(table_name),)).fetchone()
        return bool(row and _row_get(row, "to_regclass", 0) is not None)
    except Exception:
        return False


def _snapshot_ingestion_lag(con: Any, writer: MetricWriter, *, ts_ms: int) -> int:
    if not _table_exists(con, "price_provider_health"):
        return 0
    rows = con.execute(
        """
        SELECT provider, MAX(ts_ms)::bigint AS latest_ts_ms
        FROM price_provider_health
        GROUP BY provider
        """
    ).fetchall()
    emitted = 0
    for row in rows or []:
        provider = str(_row_get(row, "provider", 0) or "")
        latest_ts_ms = _as_int(_row_get(row, "latest_ts_ms", 1))
        lag_s = max(0.0, (int(ts_ms) - int(latest_ts_ms)) / 1000.0) if latest_ts_ms > 0 else -1.0
        emitted += _write_metric(
            writer,
            "ingestion.source_lag_s",
            value_num=lag_s,
            tags={"source": provider, "table": "price_provider_health"},
            ts_ms=ts_ms,
        )
    return int(emitted)


def _snapshot_replication(con: Any, writer: MetricWriter, *, ts_ms: int) -> int:
    try:
        rows = con.execute(
            """
            SELECT
              application_name,
              client_addr::text AS client_addr,
              state,
              COALESCE(EXTRACT(EPOCH FROM replay_lag) * 1000.0, 0.0) AS replay_lag_ms
            FROM pg_stat_replication
            """
        ).fetchall()
    except Exception as exc:
        _warn_once("pg_stat_replication_unavailable", "pg_stat_replication is not readable", exc)
        return 0
    emitted = 0
    for row in rows or []:
        emitted += _write_metric(
            writer,
            "postgres.replication.lag_ms",
            value_num=_as_float(_row_get(row, "replay_lag_ms", 3)),
            tags={
                "application_name": str(_row_get(row, "application_name", 0) or ""),
                "client_addr": str(_row_get(row, "client_addr", 1) or ""),
                "state": str(_row_get(row, "state", 2) or ""),
            },
            ts_ms=ts_ms,
        )
    return int(emitted)


def _pgbouncer_admin_dsn() -> str:
    configured = str(os.environ.get("TS_PGBOUNCER_ADMIN_DSN") or "").strip()
    if configured:
        return configured
    if not is_linux():
        return ""
    port = str(os.environ.get("PGBOUNCER_PORT") or os.environ.get("TRADING_PGBOUNCER_PORT") or "6432").strip()
    return f"host=/var/run/postgresql port={port} dbname=pgbouncer user=postgres"


def _numeric_items(row: Any, skip: Iterable[str]) -> Iterable[tuple[str, float]]:
    skip_set = {str(item) for item in skip}
    if isinstance(row, dict):
        items = row.items()
    else:
        keys = getattr(row, "keys", lambda: [])()
        items = ((str(key), _row_get(row, str(key))) for key in keys)
    for key, value in items:
        if str(key) in skip_set:
            continue
        try:
            yield str(key), float(value)
        except Exception:
            continue


def snapshot_pgbouncer(
    *,
    metric_writer: MetricWriter | None = None,
    ts_ms: int | None = None,
    admin_dsn: str | None = None,
) -> int:
    writer = metric_writer or _default_metric_writer()
    emitted_ts_ms = int(ts_ms or _now_ms())
    dsn = str(admin_dsn if admin_dsn is not None else _pgbouncer_admin_dsn()).strip()
    if not dsn:
        return 0
    try:
        import psycopg
        from psycopg.rows import dict_row

        with psycopg.connect(dsn, autocommit=True, row_factory=dict_row, connect_timeout=1) as con:
            with con.cursor() as cur:
                cur.execute("SHOW STATS")
                stats_rows = cur.fetchall()
                cur.execute("SHOW POOLS")
                pool_rows = cur.fetchall()
    except Exception as exc:
        _warn_once("pgbouncer_stats_unavailable", "PgBouncer admin stats are not readable", exc)
        return 0

    emitted = 0
    for row in stats_rows or []:
        database = str(_row_get(row, "database", default="") or "")
        for key, value in _numeric_items(row, skip=("database",)):
            emitted += _write_metric(
                writer,
                f"pgbouncer.stats.{key}",
                value_num=value,
                tags={"database": database},
                ts_ms=emitted_ts_ms,
            )
    for row in pool_rows or []:
        tags = {
            "database": str(_row_get(row, "database", default="") or ""),
            "user": str(_row_get(row, "user", default="") or ""),
            "pool_mode": str(_row_get(row, "pool_mode", default="") or ""),
        }
        for key, value in _numeric_items(row, skip=("database", "user", "pool_mode")):
            emitted += _write_metric(
                writer,
                f"pgbouncer.pool.{key}",
                value_num=value,
                tags=tags,
                ts_ms=emitted_ts_ms,
            )
    return int(emitted)


def snapshot_pg_observability(
    *,
    storage_connect: Callable[[], Any] | None = None,
    metric_writer: MetricWriter | None = None,
    include_pgbouncer: bool = True,
    ts_ms: int | None = None,
    statement_limit: int = 50,
) -> dict[str, Any]:
    writer = metric_writer or _default_metric_writer()
    connect = storage_connect or _default_storage_connect
    emitted_ts_ms = int(ts_ms or _now_ms())
    emitted = 0
    started = time.perf_counter()

    con = connect()
    try:
        if not _pg_stat_statements_available(con):
            _warn_once(
                "pg_stat_statements_noop",
                "pg_stat_statements is not installed; observability snapshotter is running as a no-op",
            )
            return {
                "ok": True,
                "skipped": True,
                "reason": "pg_stat_statements_unavailable",
                "emitted": 0,
                "ts_ms": emitted_ts_ms,
            }
        emitted += _snapshot_pg_stat_statements(con, writer, ts_ms=emitted_ts_ms, limit=int(statement_limit))
        emitted += _snapshot_user_tables(con, writer, ts_ms=emitted_ts_ms)
        emitted += _snapshot_database(con, writer, ts_ms=emitted_ts_ms)
        emitted += _snapshot_connections(con, writer, ts_ms=emitted_ts_ms)
        emitted += _snapshot_replication(con, writer, ts_ms=emitted_ts_ms)
        emitted += _snapshot_ingestion_lag(con, writer, ts_ms=emitted_ts_ms)
    finally:
        close = getattr(con, "close", None)
        if callable(close):
            close()

    if include_pgbouncer:
        emitted += snapshot_pgbouncer(metric_writer=writer, ts_ms=emitted_ts_ms)

    return {
        "ok": True,
        "skipped": False,
        "emitted": int(emitted),
        "elapsed_ms": round((time.perf_counter() - started) * 1000.0, 3),
        "ts_ms": emitted_ts_ms,
    }


__all__ = [
    "snapshot_pg_observability",
    "snapshot_pgbouncer",
]
