from __future__ import annotations

import base64
import importlib
import json
import sqlite3
import uuid
from pathlib import Path

from engine.runtime.data_source_log_store import (
    ensure_data_source_logs_schema,
    redact_existing_data_source_log_details,
)


VALID_DATA_SOURCE_MASTER_KEY = base64.b64encode(bytes(range(32))).decode("ascii")


def test_data_source_log_cleanup_uses_json_null_semantics_for_sqlite_legacy_rows(tmp_path: Path) -> None:
    canary = f"codex-startup-json-{uuid.uuid4().hex}"
    con = sqlite3.connect(tmp_path / "runtime.db")
    try:
        ensure_data_source_logs_schema(con)
        rows = [
            (1, "null_detail", None),
            (2, "empty_detail", ""),
            (3, "empty_object", "{}"),
            (4, "invalid_legacy_text", "{not-json"),
            (5, "valid_secret", json.dumps({"status": "configured", "credentials": {"api_key": canary}})),
        ]
        for row_id, event_type, detail_json in rows:
            con.execute(
                """
                INSERT INTO data_source_logs(id, ts_ms, source_key, level, event_type, message, detail_json)
                VALUES (?,?,?,?,?,?,?)
                """,
                (row_id, 1_700_000_000_000 + row_id, "polygon", "INFO", event_type, "unit", detail_json),
            )
        con.commit()

        first = redact_existing_data_source_log_details(con)
        second = redact_existing_data_source_log_details(con)

        assert first == {"scanned": 4, "updated": 3}
        assert second == {"scanned": 4, "updated": 0}
        persisted = {
            str(row[0]): row[1]
            for row in con.execute(
                "SELECT event_type, detail_json FROM data_source_logs ORDER BY id"
            ).fetchall()
        }
        assert persisted["null_detail"] is None
        assert persisted["empty_detail"] == "{}"
        assert persisted["empty_object"] == "{}"
        assert persisted["invalid_legacy_text"] == "{}"
        valid = json.loads(str(persisted["valid_secret"] or "{}"))
        assert valid["status"] == "configured"
        assert valid["credentials"] == "[REDACTED]"
        assert canary not in json.dumps(persisted, sort_keys=True, default=str)
    finally:
        con.close()


def test_data_source_log_cleanup_does_not_compare_jsonb_to_empty_string() -> None:
    canary = f"codex-jsonb-{uuid.uuid4().hex}"

    class _Rows:
        def __init__(self, rows):
            self._rows = list(rows)

        def fetchone(self):
            return self._rows[0] if self._rows else None

        def fetchall(self):
            return list(self._rows)

    class _JsonbConnection:
        def __init__(self) -> None:
            self.select_sql = ""
            self.updates: list[tuple[object, int]] = []

        def execute(self, sql: str, params=()):
            text = str(sql)
            if "sqlite_master" in text:
                return _Rows([(1,)])
            if "PRAGMA table_info" in text:
                return _Rows([(0, "detail_json", "JSONB", 0, None, 0)])
            if "SELECT id, detail_json" in text:
                self.select_sql = text
                return _Rows([(7, {"status": "configured", "api_token": canary})])
            if "UPDATE data_source_logs" in text:
                self.updates.append((params[0], int(params[1])))
                return _Rows([])
            raise AssertionError(f"unexpected SQL: {text}")

    con = _JsonbConnection()
    result = redact_existing_data_source_log_details(con)

    assert result == {"scanned": 1, "updated": 1}
    assert "<> ''" not in con.select_sql
    assert "detail_json IS NOT NULL" in con.select_sql
    assert con.updates == [({"api_token": "[REDACTED]", "status": "configured"}, 7)]
    assert canary not in json.dumps(con.updates, sort_keys=True, default=str)


def test_data_source_manager_initialization_repeats_without_rerunning_cleanup(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("DB_PATH", str(tmp_path / "runtime.db"))
    monkeypatch.setenv("TS_STORAGE_BACKEND", "sqlite")
    monkeypatch.setenv("DATA_SOURCE_MASTER_KEY", VALID_DATA_SOURCE_MASTER_KEY)
    monkeypatch.delenv("DATA_SOURCE_MASTER_KEY_FILE", raising=False)
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("ENGINE_SUPERVISED", "0")
    monkeypatch.setenv("TIMESCALE_ENABLED", "0")
    monkeypatch.setenv("TIMESCALE_TELEMETRY_MIRROR_ENABLED", "0")

    storage = importlib.reload(importlib.import_module("engine.runtime.storage"))
    data_source_manager = importlib.reload(importlib.import_module("services.data_source_manager"))
    storage.init_db()
    manager = data_source_manager.DataSourceManager()

    manager.initialize()
    manager._initialized = False
    manager.initialize()

    con = storage.connect_ro_direct()
    try:
        marker = con.execute(
            "SELECT value FROM runtime_meta WHERE key=?",
            ("data_source_logs_sqlite_redaction_v1",),
        ).fetchone()
        summary = con.execute(
            "SELECT value FROM runtime_meta WHERE key=?",
            ("data_source_logs_sqlite_redaction_v1_summary",),
        ).fetchone()
    finally:
        con.close()
        storage.close_pooled_connections()
    assert marker and str(marker[0]) == "1"
    assert summary and json.loads(str(summary[0] or "{}"))["updated"] >= 0


def test_postgres_write_txn_applies_bounded_timeouts_before_retry(monkeypatch) -> None:
    storage_pg = importlib.reload(importlib.import_module("engine.runtime.storage_pg"))

    class _FakeCursor:
        def __init__(self, raw) -> None:
            self.raw = raw

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql: str, params=()):
            self.raw.statements.append((str(sql), tuple(params or ())))

    class _FakeRaw:
        def __init__(self) -> None:
            self.statements: list[tuple[str, tuple[object, ...]]] = []

        def cursor(self):
            return _FakeCursor(self)

    class _FakeConnection:
        def __init__(self) -> None:
            self.raw = _FakeRaw()
            self.commits = 0
            self.rollbacks = 0
            self.closed = False

        def commit(self) -> None:
            self.commits += 1

        def rollback(self) -> None:
            self.rollbacks += 1

        def close(self) -> None:
            self.closed = True

    connections: list[_FakeConnection] = []

    def _connect(*, readonly=False, timeout_s=None):
        assert readonly is False
        assert timeout_s == 0.25
        con = _FakeConnection()
        connections.append(con)
        return con

    attempts = {"n": 0}

    def _write(_con):
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise storage_pg.errors.DeadlockDetected("deadlock detected")
        return "ok"

    monkeypatch.setattr(storage_pg, "connect", _connect)

    assert storage_pg.run_write_txn(
        _write,
        table="runtime_meta",
        operation="startup_unit",
        attempts=2,
        timeout_s=0.25,
    ) == "ok"
    assert len(connections) == 2
    for con in connections:
        assert ("SELECT set_config('lock_timeout', %s, true)", ("250ms",)) in con.raw.statements
        assert (
            "SELECT set_config('statement_timeout', %s, true)",
            ("250ms",),
        ) in con.raw.statements
        assert con.closed is True
    assert connections[0].rollbacks == 1
    assert connections[1].commits == 1


def test_startup_repair_retry_classifier_includes_postgres_deadlocks() -> None:
    start_system = importlib.reload(importlib.import_module("start_system"))
    assert start_system._db_repair_lock_contention("deadlock detected")
    assert start_system._db_repair_lock_contention({"error": "canceling statement due to lock timeout"})
    assert start_system._db_repair_lock_contention("LockNotAvailable")


def test_best_effort_startup_event_can_queue_without_schema_reinit(monkeypatch) -> None:
    event_log = importlib.reload(importlib.import_module("engine.runtime.event_log"))
    queued: list[object] = []

    def _boom_init():
        raise AssertionError("init_event_log should not run for buffered best-effort events")

    monkeypatch.setattr(event_log, "init_event_log", _boom_init)
    monkeypatch.setattr(event_log, "_EVENT_LOG_BUFFER_ENABLED", True)
    monkeypatch.setattr(event_log, "_enqueue_event_log_rows", lambda rows: queued.extend(rows) or True)

    assert event_log.append_event(
        event_type="ingestion_prebind_deferred",
        event_source="start_system",
        entity_type="process",
        entity_id="ingestion_runtime",
        payload={"entry": "start_ingestion.py"},
        best_effort=True,
    ) is None
    assert len(queued) == 1
