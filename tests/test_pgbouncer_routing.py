from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_linux_default_dsn_routes_app_traffic_to_pgbouncer(monkeypatch, tmp_path):
    from services.secrets import loader
    from engine.runtime import platform as runtime_platform

    (tmp_path / "pg_password_app").write_text("secret", encoding="utf-8")
    monkeypatch.setenv("TS_SECRETS_PROVIDER", "plaintext")
    monkeypatch.setenv("TS_DEV_SECRETS_DIR", str(tmp_path))
    monkeypatch.delenv("TS_ENV", raising=False)
    monkeypatch.setattr(loader, "_record_access", lambda **kwargs: None)
    monkeypatch.setattr(runtime_platform.sys, "platform", "linux")
    assert "port=6432" in runtime_platform.default_pg_dsn()
    assert "port=5432" in runtime_platform.default_admin_pg_dsn()


def test_process_local_pool_sizes_shrink_under_pgbouncer(monkeypatch):
    from engine.runtime import storage_pool

    monkeypatch.delenv("TS_PG_POOL_SIZE", raising=False)

    monkeypatch.setenv("TS_PG_POOL_PROFILE", "application")
    importlib.reload(storage_pool)
    assert storage_pool.default_pool_size() == 4

    monkeypatch.setenv("TS_PG_POOL_PROFILE", "ingest")
    importlib.reload(storage_pool)
    assert storage_pool.default_pool_size() == 8

    monkeypatch.setenv("TS_PG_POOL_PROFILE", "jobs")
    importlib.reload(storage_pool)
    assert storage_pool.default_pool_size() == 2


def test_pgbouncer_config_uses_transaction_pooling_and_role_overrides():
    config = (ROOT / "ops/server/config/pgbouncer.ini").read_text(encoding="utf-8")

    assert "pool_mode = transaction" in config
    assert "default_pool_size = 25" in config
    assert "reserve_pool_size = 5" in config
    assert "max_client_conn = 200" in config
    assert "server_idle_timeout = 60" in config
    assert "query_wait_timeout = 30" in config
    assert "application_name_add_host = 1" in config
    assert "ts_ingest = pool_size=50" in config
    assert "ts_app = pool_size=40" in config
    assert "ts_reader = pool_size=15" in config
    assert "reserve_pool_size=5" in config


def test_prepared_statements_work_through_pgbouncer_when_available():
    dsn = str(os.environ.get("TS_PGBOUNCER_TEST_DSN") or "").strip()
    if not dsn:
        pytest.skip("TS_PGBOUNCER_TEST_DSN is not set")
    psycopg = pytest.importorskip("psycopg")

    with psycopg.connect(dsn, connect_timeout=2, prepare_threshold=1) as con:
        for value in range(8):
            row = con.execute("SELECT %s::int + 1", (value,)).fetchone()
            assert int(row[0]) == value + 1


def test_hundred_clients_multiplex_under_pool_size_when_available():
    pool_dsn = str(os.environ.get("TS_PGBOUNCER_TEST_DSN") or "").strip()
    direct_dsn = str(os.environ.get("TS_PG_DIRECT_TEST_DSN") or "").strip()
    if not pool_dsn or not direct_dsn:
        pytest.skip("TS_PGBOUNCER_TEST_DSN and TS_PG_DIRECT_TEST_DSN are required")
    psycopg = pytest.importorskip("psycopg")

    clients = []
    try:
        for _ in range(100):
            clients.append(psycopg.connect(pool_dsn, connect_timeout=2))
        with psycopg.connect(direct_dsn, connect_timeout=2) as admin:
            row = admin.execute(
                """
                SELECT COUNT(*)
                FROM pg_stat_activity
                WHERE datname = current_database()
                  AND usename IN ('ts_app', 'ts_ingest', 'ts_reader')
                """
            ).fetchone()
            assert int(row[0] or 0) <= int(os.environ.get("TS_PGBOUNCER_ASSERT_POOL_SIZE", "50"))
    finally:
        for con in clients:
            con.close()
