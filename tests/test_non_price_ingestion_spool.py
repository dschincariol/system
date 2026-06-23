from __future__ import annotations

import importlib
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _reload(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


def test_non_price_spool_replays_after_reopen_and_deletes_only_selected_rows(tmp_path: Path) -> None:
    from engine.runtime.non_price_ingestion_spool import SQLiteNonPriceIngestionSpool

    path = tmp_path / "non_price_spool.sqlite"
    spool = SQLiteNonPriceIngestionSpool(
        path=path,
        max_rows=8,
        max_bytes=1_000_000,
        busy_timeout_ms=50,
    )
    spool.enqueue(
        table="price_provider_health",
        rows=[(1, "polygon", 1, 10, 3, None, 1, 0)],
        created_ts_ms=10,
    )
    spool.close()

    reopened = SQLiteNonPriceIngestionSpool(
        path=path,
        max_rows=8,
        max_bytes=1_000_000,
        busy_timeout_ms=50,
    )
    records, corrupt = reopened.select_batch(limit_rows=4, tables=("price_provider_health",))

    assert corrupt == []
    assert len(records) == 1
    assert records[0].rows == ((1, "polygon", 1, 10, 3, None, 1, 0),)
    assert reopened.stats()["pending_rows"] == 1

    assert reopened.delete([records[0].id]) == 1
    assert reopened.stats()["pending_rows"] == 0


def test_non_price_spool_row_cap_backpressures_without_unbounded_memory(tmp_path: Path) -> None:
    from engine.runtime.non_price_ingestion_spool import (
        NonPriceIngestionSpoolFullError,
        SQLiteNonPriceIngestionSpool,
    )

    spool = SQLiteNonPriceIngestionSpool(
        path=tmp_path / "non_price_spool.sqlite",
        max_rows=1,
        max_bytes=1_000_000,
        busy_timeout_ms=50,
    )
    spool.enqueue(table="price_provider_health", rows=[(1, "polygon")], created_ts_ms=10)

    with pytest.raises(NonPriceIngestionSpoolFullError):
        spool.enqueue(table="price_provider_health", rows=[(2, "polygon")], created_ts_ms=11)

    stats = spool.stats()
    assert stats["pending_rows"] == 1
    assert stats["rows_fill_ratio"] == 1.0


def test_telemetry_append_buffer_uses_durable_spool_for_replay(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "runtime.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("ENGINE_SUPERVISED", "1")
    monkeypatch.setenv("TELEMETRY_APPEND_BUFFER_ENABLED", "1")
    monkeypatch.setenv("TELEMETRY_APPEND_BUFFER_FLUSH_INTERVAL_S", "60")
    monkeypatch.setenv("TELEMETRY_APPEND_BUFFER_MAX_BATCH", "8")
    monkeypatch.setenv("TELEMETRY_APPEND_BUFFER_MAX_ROWS", "8")

    storage, telemetry_append_buffer = _reload(
        "engine.runtime.storage",
        "engine.runtime.telemetry_append_buffer",
    )
    storage.init_db()
    row = (1_700_000_000_000, "poll_prices", 1, 12, 2, 1, 1_700_000_000_000, None, "{}")

    with patch.object(telemetry_append_buffer, "_ensure_buffer_thread_started", return_value=None):
        assert telemetry_append_buffer.enqueue_ingestion_pipeline_health(row) is True

    before = telemetry_append_buffer.get_telemetry_append_buffer_snapshot()
    assert before["write_path"] == "durable_sqlite_spool"
    assert before["spool_pending_rows"] == 1
    assert before["pending_by_table"]["ingestion_pipeline_health"] == 1
    assert sum(len(rows) for rows in telemetry_append_buffer._BUFFER_PENDING.values()) == 0

    storage, telemetry_append_buffer = _reload(
        "engine.runtime.storage",
        "engine.runtime.telemetry_append_buffer",
    )
    after_reload = telemetry_append_buffer.get_telemetry_append_buffer_snapshot()
    assert after_reload["spool_pending_rows"] == 1
    assert after_reload["pending_by_table"]["ingestion_pipeline_health"] == 1

    flushed = telemetry_append_buffer.flush_telemetry_append_buffers(
        max_batches=4,
        tables=("ingestion_pipeline_health",),
    )
    assert flushed["flushed"] == 1
    assert flushed["spool_pending_rows"] == 0
    assert flushed["deleted_rows"] == 1

    con = storage.connect_ro_direct()
    try:
        persisted = con.execute(
            "SELECT COUNT(*) FROM ingestion_pipeline_health WHERE pipeline=?",
            ("poll_prices",),
        ).fetchone()
    finally:
        con.close()
    assert int(persisted[0] or 0) == 1


def test_telemetry_append_buffer_retains_spool_rows_when_flush_fails(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("DB_PATH", str(tmp_path / "runtime.db"))
    monkeypatch.setenv("ENGINE_SUPERVISED", "1")
    monkeypatch.setenv("TELEMETRY_APPEND_BUFFER_ENABLED", "1")
    monkeypatch.setenv("TELEMETRY_APPEND_BUFFER_FLUSH_INTERVAL_S", "60")

    storage, telemetry_append_buffer = _reload(
        "engine.runtime.storage",
        "engine.runtime.telemetry_append_buffer",
    )
    storage.init_db()
    row = (1_700_000_000_001, "poll_prices", 1, 12, 2, 1, 1_700_000_000_001, None, "{}")

    with patch.object(telemetry_append_buffer, "_ensure_buffer_thread_started", return_value=None):
        assert telemetry_append_buffer.enqueue_ingestion_pipeline_health(row) is True

    with patch.object(telemetry_append_buffer, "_flush_rows", side_effect=RuntimeError("db down")):
        with pytest.raises(RuntimeError, match="db down"):
            telemetry_append_buffer.flush_telemetry_append_buffers(
                max_batches=1,
                tables=("ingestion_pipeline_health",),
            )

    snapshot = telemetry_append_buffer.get_telemetry_append_buffer_snapshot()
    assert snapshot["spool_pending_rows"] == 1
    assert snapshot["pending_by_table"]["ingestion_pipeline_health"] == 1


def test_telemetry_append_buffer_db_outage_is_bounded_and_durable(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("DB_PATH", str(tmp_path / "runtime.db"))
    monkeypatch.setenv("ENGINE_SUPERVISED", "1")
    monkeypatch.setenv("TELEMETRY_APPEND_BUFFER_ENABLED", "1")
    monkeypatch.setenv("TELEMETRY_APPEND_BUFFER_FLUSH_INTERVAL_S", "60")
    monkeypatch.setenv("TELEMETRY_APPEND_BUFFER_MAX_BATCH", "2")
    monkeypatch.setenv("TELEMETRY_APPEND_BUFFER_MAX_ROWS", "2")

    storage, telemetry_append_buffer = _reload(
        "engine.runtime.storage",
        "engine.runtime.telemetry_append_buffer",
    )
    storage.init_db()

    rows = [
        (1_700_000_000_010, "poll_prices", 1, 12, 2, 1, 1_700_000_000_010, None, "{}"),
        (1_700_000_000_011, "poll_prices", 1, 13, 3, 2, 1_700_000_000_011, None, "{}"),
    ]
    with patch.object(telemetry_append_buffer, "_ensure_buffer_thread_started", return_value=None):
        for row in rows:
            assert telemetry_append_buffer.enqueue_ingestion_pipeline_health(row) is True

    with patch.object(telemetry_append_buffer, "_flush_rows", side_effect=RuntimeError("db down")):
        with pytest.raises(RuntimeError, match="db down"):
            telemetry_append_buffer.flush_telemetry_append_buffers(
                max_batches=1,
                tables=("ingestion_pipeline_health",),
            )

    outage_snapshot = telemetry_append_buffer.get_telemetry_append_buffer_snapshot()
    assert outage_snapshot["spool_pending_rows"] == 2
    assert outage_snapshot["pending_by_table"]["ingestion_pipeline_health"] == 2
    assert outage_snapshot["flush_failures"] == 1
    assert outage_snapshot["retry_count"] == 1
    assert outage_snapshot["dropped_rows"] == 0
    assert outage_snapshot["queue_depth"] == 2
    assert outage_snapshot["oldest_age_ms"] >= 0
    assert outage_snapshot["backpressure_active"] is True
    assert outage_snapshot["backpressure_events"] >= 1
    assert sum(len(pending) for pending in telemetry_append_buffer._BUFFER_PENDING.values()) == 0

    extra_row = (1_700_000_000_012, "poll_prices", 1, 14, 4, 3, 1_700_000_000_012, None, "{}")
    with patch.object(telemetry_append_buffer, "_ensure_buffer_thread_started", return_value=None):
        assert telemetry_append_buffer.enqueue_ingestion_pipeline_health(extra_row) is False

    full_snapshot = telemetry_append_buffer.get_telemetry_append_buffer_snapshot()
    assert full_snapshot["spool_pending_rows"] == 2
    assert full_snapshot["dropped_rows"] == 1
    assert full_snapshot["last_rejected_reason"] == "buffer_overflow"
    assert full_snapshot["backpressure_active"] is True

    storage, telemetry_append_buffer = _reload(
        "engine.runtime.storage",
        "engine.runtime.telemetry_append_buffer",
    )
    replay_snapshot = telemetry_append_buffer.get_telemetry_append_buffer_snapshot()
    assert replay_snapshot["spool_pending_rows"] == 2
    assert replay_snapshot["pending_by_table"]["ingestion_pipeline_health"] == 2
