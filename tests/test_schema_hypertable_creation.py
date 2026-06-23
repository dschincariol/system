from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from engine.runtime.schema.table_classification import Hypertable, TABLE_CLASS, hypertable_chunk_interval_ms

_PREPARED_STORAGE = None
_UNAVAILABLE_REASON: str | None = None


def _ensure_test_pg_password() -> None:
    configured = str(os.environ.get("TS_PG_DSN") or "")
    has_password = "password=" in configured.lower() or any(
        os.environ.get(name)
        for name in (
            "TS_PG_PASSWORD",
            "TS_PG_PASSWORD_APP",
            "TS_PG_APP_PASSWORD",
            "PGPASSWORD",
        )
    )
    if not has_password:
        os.environ.setdefault("TS_PG_PASSWORD", "test-app-password")


def _prepare_db():
    global _PREPARED_STORAGE, _UNAVAILABLE_REASON
    if _PREPARED_STORAGE is not None:
        return _PREPARED_STORAGE
    if _UNAVAILABLE_REASON is not None:
        pytest.skip(_UNAVAILABLE_REASON)

    psycopg = pytest.importorskip("psycopg")
    from engine.runtime.platform import default_pg_dsn
    from engine.runtime import storage_pg

    _ensure_test_pg_password()
    dsn = str(os.environ.get("TS_PG_DSN") or default_pg_dsn()).strip()
    try:
        with psycopg.connect(dsn, connect_timeout=1, autocommit=True) as raw:
            with raw.cursor() as cur:
                cur.execute("SET search_path = trading, public")
                cur.execute("SELECT default_version FROM pg_available_extensions WHERE name = 'timescaledb'")
                row = cur.fetchone()
    except Exception as exc:
        _UNAVAILABLE_REASON = f"Postgres is not available for schema tests: {exc}"
        pytest.skip(_UNAVAILABLE_REASON)

    if not row or row[0] is None:
        _UNAVAILABLE_REASON = "TimescaleDB extension is not available"
        pytest.skip(_UNAVAILABLE_REASON)

    storage_pg.apply_migrations()
    _PREPARED_STORAGE = storage_pg
    return storage_pg


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute("SELECT to_regclass(?)", (str(table_name),)).fetchone()
    return bool(row and row[0] is not None)


def _column_exists(conn, table_name: str, column_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = ANY (current_schemas(false))
          AND table_name = ?
          AND column_name = ?
        """,
        (str(table_name), str(column_name)),
    ).fetchone()
    return bool(row)


def _existing_classified_hypertables(conn) -> dict[str, Hypertable]:
    out: dict[str, Hypertable] = {}
    for table_name, classification in TABLE_CLASS.items():
        if not isinstance(classification, Hypertable):
            continue
        if _table_exists(conn, table_name) and _column_exists(conn, table_name, classification.time_column):
            out[table_name] = classification
    return out


def _dimension_interval_ms(value) -> int | None:
    if value is None:
        return None
    total_seconds = getattr(value, "total_seconds", None)
    if callable(total_seconds):
        return int(round(float(total_seconds()) * 1000.0))
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        pass
    parts = text.split()
    if len(parts) >= 2 and parts[0].isdigit():
        unit = parts[1].rstrip(",").lower()
        multiplier = {
            "day": 86_400_000,
            "days": 86_400_000,
            "week": 604_800_000,
            "weeks": 604_800_000,
        }.get(unit)
        if multiplier is not None:
            return int(parts[0]) * int(multiplier)
    return None


def test_each_existing_classified_hypertable_is_created() -> None:
    storage_pg = _prepare_db()
    with storage_pg.connect_ro_direct(timeout_s=1) as conn:
        expected = _existing_classified_hypertables(conn)
        rows = conn.execute(
            """
            SELECT hypertable_name
            FROM timescaledb_information.hypertables
            WHERE hypertable_schema = ANY (current_schemas(false))
            """
        ).fetchall()
        actual = {str(row[0]) for row in rows or []}
        missing = sorted(set(expected) - actual)
        assert not missing, "Classified hypertables missing from Timescale: " + ", ".join(missing)
        assert len(actual) >= len(expected)


def test_each_hypertable_has_time_dimension() -> None:
    storage_pg = _prepare_db()
    with storage_pg.connect_ro_direct(timeout_s=1) as conn:
        for table_name, classification in _existing_classified_hypertables(conn).items():
            rows = conn.execute(
                """
                SELECT *
                FROM timescaledb_information.dimensions
                WHERE hypertable_schema = ANY (current_schemas(false))
                  AND hypertable_name = ?
                  AND column_name = ?
                """,
                (table_name, classification.time_column),
            ).fetchall()
            assert rows, f"{table_name} is missing a Timescale dimension on {classification.time_column}"
            row = rows[0]
            interval = (
                row.get("integer_interval")
                or row.get("time_interval")
                or row.get("interval_length")
                or row.get("time_interval")
            )
            assert interval is not None, f"{table_name} dimension has no chunk interval"
            actual_ms = _dimension_interval_ms(interval)
            assert actual_ms == hypertable_chunk_interval_ms(table_name), (
                f"{table_name} chunk interval mismatch: "
                f"actual_ms={actual_ms} expected_ms={hypertable_chunk_interval_ms(table_name)}"
            )
