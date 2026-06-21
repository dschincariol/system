from __future__ import annotations

import importlib
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class _Cursor:
    def __init__(self, rows):
        self._rows = list(rows or [])

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


class _FakeConnection:
    def __init__(self, *, has_pg_stat_statements: bool = True):
        self.has_pg_stat_statements = bool(has_pg_stat_statements)
        self.closed = False

    def execute(self, sql, params=None):
        del params
        text = " ".join(str(sql).lower().split())
        if "from pg_extension" in text:
            return _Cursor([(1,)] if self.has_pg_stat_statements else [])
        if "to_regclass('pg_stat_statements')" in text:
            return _Cursor([("pg_stat_statements",)] if self.has_pg_stat_statements else [(None,)])
        if "from information_schema.columns" in text and "pg_stat_statements" in text:
            return _Cursor(
                [
                    {"column_name": "queryid"},
                    {"column_name": "query"},
                    {"column_name": "calls"},
                    {"column_name": "total_exec_time"},
                    {"column_name": "mean_exec_time"},
                    {"column_name": "rows"},
                ]
            )
        if "from pg_stat_statements" in text:
            return _Cursor(
                [
                    {
                        "query_id": "42",
                        "query": "SELECT * FROM prices WHERE symbol = $1",
                        "calls": 12,
                        "total_time_ms": 125.5,
                        "mean_time_ms": 10.4,
                        "rows": 120,
                    }
                ]
            )
        if "from pg_stat_user_tables" in text:
            return _Cursor(
                [
                    {
                        "schemaname": "trading",
                        "relname": "prices",
                        "reads": 100,
                        "writes": 25,
                        "dead_tuples": 2,
                    }
                ]
            )
        if "from pg_stat_database" in text:
            return _Cursor(
                [
                    {
                        "datname": "trading",
                        "blks_hit": 990,
                        "blks_read": 10,
                        "deadlocks": 0,
                        "conflicts": 0,
                        "tup_inserted": 20,
                        "tup_updated": 3,
                        "tup_deleted": 2,
                    }
                ]
            )
        if "from pg_stat_activity" in text:
            return _Cursor([{"state": "active", "count": 3}, {"state": "idle", "count": 7}])
        if "from pg_stat_replication" in text:
            return _Cursor([])
        if "from pg_stat_archiver" in text:
            return _Cursor(
                [
                    {
                        "archived_count": 20,
                        "failed_count": 1,
                        "last_archived_at_ts": 110.0,
                        "last_failed_at_ts": 100.0,
                        "last_archived_wal": "0000000100000000000000AB",
                        "last_failed_wal": "0000000100000000000000AA",
                    }
                ]
            )
        if "from pg_ls_waldir()" in text:
            return _Cursor([{"wal_bytes": 32 * 1024 * 1024, "wal_files": 2}])
        if "from pg_ls_dir('pg_wal/archive_status')" in text:
            return _Cursor([{"ready_count": 1}])
        if "select to_regclass(?)" in text:
            return _Cursor([("price_provider_health",)])
        if "from price_provider_health" in text:
            return _Cursor([{"provider": "polygon", "latest_ts_ms": 90000}])
        raise AssertionError(f"unexpected SQL: {sql}")

    def close(self):
        self.closed = True


def test_pg_observability_snapshot_emits_dashboard_metrics():
    pg_stats = importlib.reload(importlib.import_module("engine.runtime.observability.pg_stats"))
    captured = []

    def writer(metric, value_num=None, value_text=None, tags=None, ts_ms=None):
        captured.append(
            {
                "metric": metric,
                "value_num": value_num,
                "value_text": value_text,
                "tags": dict(tags or {}),
                "ts_ms": ts_ms,
            }
        )

    fake = _FakeConnection()
    result = pg_stats.snapshot_pg_observability(
        storage_connect=lambda: fake,
        metric_writer=writer,
        include_pgbouncer=False,
        ts_ms=120000,
        statement_limit=1,
    )

    metrics = {row["metric"] for row in captured}
    assert result["ok"] is True
    assert result["skipped"] is False
    assert fake.closed is True
    assert "pg_stat_statements.total_time_ms" in metrics
    assert "postgres.table.write_rate_per_s" in metrics
    assert "postgres.table.writes_total" in metrics
    assert "postgres.database.cache_hit_ratio" in metrics
    assert "postgres.connections.active" in metrics
    assert "postgres.wal_archiver.failed_count" in metrics
    assert "postgres.wal.directory_bytes" in metrics
    assert "postgres.wal.archive_ready_count" in metrics
    assert "ingestion.source_lag_s" in metrics
    assert any(row["value_text"] and "SELECT * FROM prices" in row["value_text"] for row in captured)


def test_pg_observability_snapshot_noops_without_pg_stat_statements():
    pg_stats = importlib.reload(importlib.import_module("engine.runtime.observability.pg_stats"))
    captured = []
    fake = _FakeConnection(has_pg_stat_statements=False)

    result = pg_stats.snapshot_pg_observability(
        storage_connect=lambda: fake,
        metric_writer=lambda *args, **kwargs: captured.append((args, kwargs)),
        include_pgbouncer=False,
        ts_ms=120000,
    )

    assert result["ok"] is True
    assert result["skipped"] is True
    assert result["reason"] == "pg_stat_statements_unavailable"
    assert result["emitted"] > 0
    metrics = {args[0] for args, _kwargs in captured}
    assert "postgres.wal_archiver.failed_count" in metrics
    assert "postgres.wal.directory_bytes" in metrics
