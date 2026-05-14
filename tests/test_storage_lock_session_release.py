from __future__ import annotations

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
from engine.runtime.locks_pg import _lock_key, advisory_lock
from engine.runtime.platform import default_pg_dsn


def _configure_pg_env(monkeypatch) -> None:
    monkeypatch.setenv("TS_PG_POOL_TIMEOUT", "0.5")
    monkeypatch.setenv("TS_PG_POOL_MIN_SIZE", "1")
    monkeypatch.setenv("TS_PG_POOL_SIZE", "1")
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


def _dsn() -> str:
    return os.environ.get("TS_PG_DSN") or default_pg_dsn()


def _pg_or_skip() -> None:
    try:
        with psycopg.connect(_dsn(), connect_timeout=1) as con:
            con.execute("SELECT 1").fetchone()
    except Exception as exc:
        pytest.skip(f"Postgres is not reachable through TS_PG_DSN: {exc}")


def test_advisory_lock_releases_session_lock_before_pool_return(monkeypatch):
    _configure_pg_env(monkeypatch)
    storage.close_pooled_connections()
    _pg_or_skip()

    name = f"test-key-{os.getpid()}-{int(time.time() * 1000)}"
    missing_table = f"storage_lock_missing_{os.getpid()}_{int(time.time() * 1000)}"

    with pytest.raises(Exception):
        with advisory_lock(name) as conn:
            conn.execute(f"SELECT * FROM {missing_table}")

    acquired_at = time.perf_counter()
    with advisory_lock(name):
        pass
    assert time.perf_counter() - acquired_at < 0.5

    with psycopg.connect(_dsn(), connect_timeout=1) as con:
        row = con.execute("SELECT pg_try_advisory_lock(%s)", (int(_lock_key(name)),)).fetchone()
        assert row is not None and bool(row[0])
        con.execute("SELECT pg_advisory_unlock(%s)", (int(_lock_key(name)),))

    storage.close_pooled_connections()
