import os
import sys
import textwrap
import threading
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

psycopg = pytest.importorskip("psycopg")

from engine.runtime import storage
from engine.runtime.platform import default_pg_dsn
from engine.runtime.schema.migrator import apply_migrations


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


def _package(tmp_path, name: str):
    pkg = tmp_path / name
    migrations = pkg / "migrations"
    migrations.mkdir(parents=True)
    (pkg / "__init__.py").write_text("", encoding="utf-8")
    (migrations / "__init__.py").write_text("", encoding="utf-8")
    sys.path.insert(0, str(tmp_path))
    return f"{name}.migrations", migrations


def test_migrations_apply_once_and_second_run_is_noop(monkeypatch, tmp_path):
    _pg_or_skip(monkeypatch)
    package, migrations = _package(tmp_path, f"mig_pkg_{os.getpid()}_a")
    table = f"migration_once_{os.getpid()}"
    migration_id = 100000 + os.getpid()
    (migrations / "0001_probe.py").write_text(
        textwrap.dedent(
            f"""
            id = {migration_id}
            description = "probe"
            def up(conn):
                conn.execute("CREATE TABLE IF NOT EXISTS {table} (id BIGSERIAL PRIMARY KEY, value TEXT)")
                conn.execute("INSERT INTO {table}(value) VALUES (?)", ("applied",))
            """
        ),
        encoding="utf-8",
    )
    try:
        assert apply_migrations(package=package) == [migration_id]
        assert apply_migrations(package=package) == []
        row = storage.fetch_one(f"SELECT COUNT(*) FROM {table}")
        assert int(row[0]) == 1
    finally:
        with storage.transaction() as con:
            con.execute(f"DROP TABLE IF EXISTS {table}")


def test_failed_migration_rolls_back(monkeypatch, tmp_path):
    _pg_or_skip(monkeypatch)
    package, migrations = _package(tmp_path, f"mig_pkg_{os.getpid()}_b")
    table = f"migration_failed_{os.getpid()}"
    migration_id = 200000 + os.getpid()
    (migrations / "0001_failed.py").write_text(
        textwrap.dedent(
            f"""
            id = {migration_id}
            description = "failed"
            def up(conn):
                conn.execute("CREATE TABLE {table} (id BIGSERIAL PRIMARY KEY)")
                raise RuntimeError("boom")
            """
        ),
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError):
        apply_migrations(package=package)
    row = storage.fetch_one(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = ANY (current_schemas(false))
          AND table_name = ?
        """,
        (table,),
    )
    assert row is None


def test_concurrent_appliers_only_apply_once(monkeypatch, tmp_path):
    _pg_or_skip(monkeypatch)
    package, migrations = _package(tmp_path, f"mig_pkg_{os.getpid()}_c")
    table = f"migration_concurrent_{os.getpid()}"
    migration_id = 300000 + os.getpid()
    (migrations / "0001_concurrent.py").write_text(
        textwrap.dedent(
            f"""
            id = {migration_id}
            description = "concurrent"
            def up(conn):
                conn.execute("CREATE TABLE IF NOT EXISTS {table} (id BIGSERIAL PRIMARY KEY, value TEXT)")
                conn.execute("SELECT pg_sleep(0.1)")
                conn.execute("INSERT INTO {table}(value) VALUES (?)", ("applied",))
            """
        ),
        encoding="utf-8",
    )
    results = []
    errors = []

    def run():
        try:
            results.append(apply_migrations(package=package))
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=run), threading.Thread(target=run)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    try:
        assert not errors
        assert sorted(results, key=len) == [[], [migration_id]]
        row = storage.fetch_one(f"SELECT COUNT(*) FROM {table}")
        assert int(row[0]) == 1
    finally:
        with storage.transaction() as con:
            con.execute(f"DROP TABLE IF EXISTS {table}")


def test_baseline_migration_covers_legacy_schema_surface():
    import importlib

    baseline = importlib.import_module("engine.runtime.schema.migrations.0001_baseline")
    table_names = {str(name) for name, _body in baseline.TABLE_DEFS}
    assert len(table_names) >= 164
    for required in (
        "alerts",
        "events",
        "prices",
        "price_quotes",
        "runtime_meta",
        "job_locks",
        "portfolio_state",
        "trade_attribution_ledger",
        "model_registry",
        "execution_order_idempotency",
    ):
        assert required in table_names
