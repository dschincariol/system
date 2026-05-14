from __future__ import annotations

import os
import sys
import textwrap
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

psycopg = pytest.importorskip("psycopg")

from engine.runtime import storage
from engine.runtime.platform import default_pg_dsn
from engine.runtime.schema.migrator import MIGRATION_LOCK_KEY, apply_migrations


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


def _package(tmp_path, name: str):
    pkg = tmp_path / name
    migrations = pkg / "migrations"
    migrations.mkdir(parents=True)
    (pkg / "__init__.py").write_text("", encoding="utf-8")
    (migrations / "__init__.py").write_text("", encoding="utf-8")
    sys.path.insert(0, str(tmp_path))
    return f"{name}.migrations", migrations


def test_migration_lock_released_after_rollback(monkeypatch, tmp_path):
    _configure_pg_env(monkeypatch)
    _pg_or_skip()
    storage.close_pooled_connections()

    package, migrations = _package(tmp_path, f"mig_lock_pkg_{os.getpid()}_{int(time.time() * 1000)}")
    migration_id = 400_000_000 + (int(time.time() * 1000) % 100_000_000)
    (migrations / "0001_fail.py").write_text(
        textwrap.dedent(
            f"""
            id = {migration_id}
            description = "failed lock release"
            def up(conn):
                raise RuntimeError("rollback releases migration lock")
            """
        ),
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError):
        apply_migrations(package=package)

    with psycopg.connect(_dsn(), connect_timeout=1) as con:
        row = con.execute(
            "SELECT pg_try_advisory_xact_lock(%s)",
            (int(MIGRATION_LOCK_KEY),),
        ).fetchone()
        assert row is not None and bool(row[0])

    storage.close_pooled_connections()
