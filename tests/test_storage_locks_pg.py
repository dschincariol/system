import os
import sys
import threading
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

psycopg = pytest.importorskip("psycopg")

from engine.runtime import storage
from engine.runtime.platform import default_pg_dsn
from engine.runtime.locks_pg import advisory_lock


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


def test_advisory_lock_contention_blocks_until_release(monkeypatch):
    _pg_or_skip(monkeypatch)
    name = f"lock-test-{os.getpid()}"
    first_acquired = threading.Event()
    release_first = threading.Event()
    second_started = threading.Event()
    second_acquired_at = []
    released_at = []

    def first():
        with advisory_lock(name):
            first_acquired.set()
            release_first.wait(timeout=5)
            released_at.append(time.perf_counter())

    def second():
        second_started.set()
        with advisory_lock(name):
            second_acquired_at.append(time.perf_counter())

    t1 = threading.Thread(target=first)
    t2 = threading.Thread(target=second)
    t1.start()
    assert first_acquired.wait(timeout=2)
    t2.start()
    assert second_started.wait(timeout=2)
    time.sleep(0.1)
    assert not second_acquired_at
    release_first.set()
    t1.join(timeout=2)
    t2.join(timeout=2)
    assert second_acquired_at
    assert second_acquired_at[0] >= released_at[0]
    assert second_acquired_at[0] - released_at[0] < 0.2
