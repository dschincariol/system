from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from engine.api.http_transport import _derive_response_status
from engine.dashboard import db_health


class _Cursor:
    def __init__(self, rows: list[tuple[Any, ...]]):
        self._rows = rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


class _SQLiteHealthConnection:
    def __init__(self):
        self.closed = False

    def execute(self, sql: str):
        if sql.strip().lower().startswith("pragma quick_check"):
            return _Cursor([("ok",)])
        raise AssertionError(f"unexpected sqlite health sql: {sql}")

    def close(self):
        self.closed = True


class _PostgresHealthConnection:
    def __init__(self):
        self.closed = False

    def execute(self, sql: str):
        text = sql.strip().lower()
        if "pragma" in text or "sqlite_master" in text:
            raise AssertionError(f"sqlite sql used for postgres health: {sql}")
        if text == "select 1":
            return _Cursor([(1,)])
        raise AssertionError(f"unexpected postgres health sql: {sql}")

    def close(self):
        self.closed = True


class _PostgresDashboardConnection:
    def __init__(self):
        self.closed = False

    def execute(self, sql: str):
        text = " ".join(sql.strip().lower().split())
        if "pragma" in text or "sqlite_master" in text:
            raise AssertionError(f"sqlite sql used for postgres dashboard query: {sql}")
        if "from pg_catalog.pg_tables" in text:
            return _Cursor([("runtime_meta",), ("event_log",)])
        if text == 'select count(*) from "runtime_meta"':
            return _Cursor([(3,)])
        if text == 'select count(*) from "event_log"':
            return _Cursor([(5,)])
        raise AssertionError(f"unexpected postgres dashboard sql: {sql}")

    def close(self):
        self.closed = True


def test_sqlite_suffix_size_helper_never_raises_for_local_sidecars(tmp_path: Path):
    db_path = tmp_path / "runtime"
    db_path.write_bytes(b"main")
    (tmp_path / "runtime-wal").write_bytes(b"wal-data")
    (tmp_path / "runtime-shm").write_bytes(b"shm")

    assert db_health._sqlite_local_file_size(db_path, "") == (4, None)
    assert db_health._sqlite_local_file_size(db_path, "-wal") == (8, None)
    assert db_health._sqlite_local_file_size(db_path, "-shm") == (3, None)

    size, note = db_health._sqlite_local_file_size(db_path, "-journal")
    assert size is None
    assert note is not None
    assert note["code"] == "unexpected_sqlite_local_suffix"


def test_sqlite_backend_still_computes_local_wal_size(tmp_path: Path):
    db_path = tmp_path / "runtime"
    db_path.write_bytes(b"main-db")
    (tmp_path / "runtime-wal").write_bytes(b"sqlite-wal")

    con = _SQLiteHealthConnection()
    payload = db_health.db_health_snapshot(
        db_path=db_path,
        base_dir=str(tmp_path),
        connect_ro=lambda: con,
        backend_name="sqlite",
    )

    assert payload["ok"] is True
    assert payload["backend"] == "sqlite"
    assert payload["local_wal_applicable"] is True
    assert payload["size_bytes"] == 7
    assert payload["wal_bytes"] == 10
    assert payload["shm_bytes"] == 0
    assert payload["wal_source"] == "sqlite_local_files"
    assert payload["wal_notes"] == []
    assert con.closed is True


def test_postgres_db_health_handler_returns_200_without_local_wal_suffix(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    from engine.api import api_system
    from engine.runtime import health as runtime_health
    from engine.runtime import storage

    pg_root = tmp_path / "pgdata"
    pg_root.mkdir()

    def snapshot():
        return db_health.db_health_snapshot(
            db_path=pg_root,
            base_dir=str(tmp_path),
            connect_ro=_PostgresHealthConnection,
            backend_name="postgres",
        )

    dashboard_con = _PostgresDashboardConnection()
    monkeypatch.setattr(
        api_system,
        "api_get_system_state",
        lambda _parsed, ctx: {"ok": True, "handler_count": len(ctx.get("API_HANDLERS") or {})},
    )
    monkeypatch.setattr(runtime_health, "get_health_snapshot", lambda: {"ok": True, "source": "health"})
    monkeypatch.setattr(api_system, "_recent_runtime_errors", lambda limit=10: [{"limit": limit}])
    monkeypatch.setattr(storage, "get_db_debug_snapshot", lambda: {"storage": "postgres", "ok": True})

    payload = db_health.api_get_db_health(
        None,
        {},
        db_health_snapshot_fn=snapshot,
        dashboard_db_connect=lambda: dashboard_con,
        jobs={},
        supervisor=None,
        api_handlers={"api_get_db_health": object()},
    )

    assert _derive_response_status(payload) == 200
    assert payload["ok"] is True
    assert payload["backend"] == "postgres"
    assert payload["liveness"] == "ok"
    assert payload["integrity"] == "not_applicable"
    assert payload["wal_bytes"] is None
    assert payload["shm_bytes"] is None
    assert payload["local_wal_applicable"] is False
    assert payload["wal_source"] == "not_applicable_postgres"
    assert payload["tables"] == ["runtime_meta", "event_log"]
    assert payload["row_counts"] == {"runtime_meta": 3, "event_log": 5}
    assert "Invalid suffix" not in str(payload)
    assert payload["error"] is None
    assert dashboard_con.closed is True
