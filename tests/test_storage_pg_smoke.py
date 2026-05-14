import os
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

psycopg = pytest.importorskip("psycopg")

from engine.runtime import storage
from engine.runtime.platform import default_pg_dsn


def _ensure_test_pg_password(monkeypatch) -> None:
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
        monkeypatch.setenv("TS_PG_PASSWORD", "test-app-password")


def _pg_or_skip(monkeypatch):
    monkeypatch.setenv("TS_PG_POOL_TIMEOUT", os.environ.get("TS_PG_POOL_TIMEOUT", "0.5"))
    monkeypatch.setenv("TS_PG_POOL_MIN_SIZE", os.environ.get("TS_PG_POOL_MIN_SIZE", "1"))
    monkeypatch.setenv("TS_PG_POOL_SIZE", os.environ.get("TS_PG_POOL_SIZE", "1"))
    _ensure_test_pg_password(monkeypatch)
    try:
        with psycopg.connect(os.environ.get("TS_PG_DSN") or default_pg_dsn(), connect_timeout=1) as con:
            con.execute("SELECT 1").fetchone()
    except Exception as exc:
        pytest.skip(f"Postgres is not reachable through TS_PG_DSN: {exc}")


def test_round_trip_insert_select_and_pool_checkout(monkeypatch):
    _pg_or_skip(monkeypatch)
    storage.apply_migrations()
    table = f"storage_smoke_{os.getpid()}"
    start = time.perf_counter()
    con = storage.connect_rw_direct(timeout_s=1.0)
    elapsed = time.perf_counter() - start
    try:
        assert elapsed < 1.0
        con.execute(f"DROP TABLE IF EXISTS {table}")
        con.execute(f"CREATE TABLE {table} (id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL)")
        con.execute(f"INSERT INTO {table}(name) VALUES (?)", ("alpha",))
        con.commit()
        row = con.execute(f"SELECT name FROM {table} WHERE name=?", ("alpha",)).fetchone()
        assert row[0] == "alpha"
    finally:
        try:
            con.execute(f"DROP TABLE IF EXISTS {table}")
            con.commit()
        finally:
            con.close()


def test_transaction_rollback_isolation(monkeypatch):
    _pg_or_skip(monkeypatch)
    table = f"storage_rollback_{os.getpid()}"
    con = storage.connect_rw_direct(timeout_s=1.0)
    try:
        con.execute(f"DROP TABLE IF EXISTS {table}")
        con.execute(f"CREATE TABLE {table} (name TEXT NOT NULL)")
        con.commit()
        con.execute(f"INSERT INTO {table}(name) VALUES (?)", ("rolled-back",))
        con.rollback()
        row = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        assert int(row[0]) == 0
    finally:
        try:
            con.execute(f"DROP TABLE IF EXISTS {table}")
            con.commit()
        finally:
            con.close()
