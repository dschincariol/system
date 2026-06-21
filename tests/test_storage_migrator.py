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
from engine.runtime.schema.migrator import apply_migrations, expected_migration_ids, expected_schema_version


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


def test_realized_outcomes_migration_matches_model_scoring_contract():
    import importlib

    migration = importlib.import_module("engine.runtime.schema.migrations.0050_realized_outcomes")
    statements = []

    class FakeConnection:
        def execute(self, sql, params=None):
            assert params is None
            statements.append(str(sql))

    migration.up(FakeConnection())

    sql = "\n".join(statements)
    assert "CREATE TABLE IF NOT EXISTS realized_outcomes" in sql
    for column in ("symbol", "ts_ms", "realized_return", "metadata_json", "created_ts_ms", "updated_ts_ms"):
        assert column in sql
    assert "UNIQUE(symbol, ts_ms)" in sql
    assert "idx_realized_outcomes_symbol_ts" in sql


def test_expected_schema_version_tracks_latest_migration_module():
    ids = expected_migration_ids()

    assert ids
    assert ids[-1] == expected_schema_version()
    assert expected_schema_version() == 64


def test_model_scoring_indexes_migration_covers_unresolved_query_contract():
    import importlib

    migration = importlib.import_module("engine.runtime.schema.migrations.0063_model_scoring_indexes")
    statements = []
    available_columns = {
        "tracked_predictions": {"id", "prediction_id", "ts_ms"},
        "model_performance": {
            "id",
            "tracked_prediction_id",
            "prediction_id",
            "time",
            "created_ts_ms",
            "updated_ts_ms",
        },
    }

    class FakeConnection:
        def execute(self, sql, params=None):
            text = str(sql)
            if "FROM pg_attribute" in text:
                table = str((params or ("", ""))[0])
                column = str((params or ("", ""))[1])
                return type(
                    "Cursor",
                    (),
                    {"fetchone": lambda _self: (1,) if column in available_columns.get(table, set()) else None},
                )()
            statements.append(text)
            return type("Cursor", (), {"fetchone": lambda _self: None, "fetchall": lambda _self: []})()

    migration.up(FakeConnection())
    sql = "\n".join(statements)

    assert migration.id == 63
    assert "idx_tracked_predictions_prediction_id_ts_id" in sql
    assert "ON tracked_predictions(prediction_id, ts_ms DESC, id DESC)" in sql
    assert sql.count("WHERE prediction_id IS NOT NULL") == 2
    assert "idx_model_performance_prediction_id" in sql
    assert "ON model_performance(prediction_id)" in sql
    assert "DELETE FROM model_performance" in sql
    assert "PARTITION BY tracked_prediction_id" in sql
    assert "ux_model_performance_tracked_prediction_id" in sql
    assert "CREATE UNIQUE INDEX IF NOT EXISTS ux_model_performance_tracked_prediction_id" in sql


def test_graph_relational_migration_declares_shadow_snapshot_contract():
    import importlib

    migration = importlib.import_module("engine.runtime.schema.migrations.0062_graph_relational_learning")
    statements = []

    class FakeConnection:
        def execute(self, sql, params=None):
            assert params is None
            statements.append(str(sql))

    migration.up(FakeConnection())
    sql = "\n".join(statements)

    assert migration.id == 62
    for table in ("graph_relationship_edges", "graph_relational_snapshots"):
        assert f"CREATE TABLE IF NOT EXISTS {table}" in sql
    for column in (
        "availability_ts_ms BIGINT NOT NULL",
        "snapshot_version BIGINT NOT NULL",
        "feature_ids_json JSONB NOT NULL",
        "metadata_json JSONB NOT NULL",
    ):
        assert column in sql
    for index_name in (
        "idx_graph_relationship_edges_source_avail",
        "idx_graph_relationship_edges_target_avail",
        "idx_graph_relational_snapshots_symbol_ts",
        "idx_graph_relational_snapshots_graph_ts",
    ):
        assert index_name in sql


def test_policy_ope_migration_covers_contract():
    import importlib

    migration = importlib.import_module("engine.runtime.schema.migrations.0061_policy_ope")
    statements = []

    class FakeConnection:
        def execute(self, sql, params=None):
            assert params is None
            statements.append(str(sql))

    migration.up(FakeConnection())
    sql = "\n".join(statements)

    assert migration.id == 61
    for table in ("policy_ope_observations", "policy_ope_evidence"):
        assert f"CREATE TABLE IF NOT EXISTS {table}" in sql
        assert "prev_hash BYTEA" in sql
        assert "row_hash BYTEA" in sql
    for column in (
        "behavior_propensity",
        "target_propensity",
        "logged_model_estimate",
        "target_model_estimate",
        "effective_n",
        "support",
        "ci_lower",
        "ci_upper",
        "decision TEXT NOT NULL",
    ):
        assert column in sql
    for index_name in (
        "idx_policy_ope_obs_candidate_ts",
        "idx_policy_ope_obs_model_ts",
        "idx_policy_ope_obs_scope_ts",
        "idx_policy_ope_evidence_candidate_ts",
        "idx_policy_ope_evidence_model_ts",
        "idx_policy_ope_evidence_decision_ts",
    ):
        assert index_name in sql


def test_live_owned_schema_canonicalization_migration_declares_contract(monkeypatch):
    import importlib

    migration = importlib.import_module("engine.runtime.schema.migrations.0056_live_owned_schema_canonicalization")
    statements = []
    calls = []

    class FakeConnection:
        def execute(self, sql, params=None):
            del params
            statements.append(str(sql))

    monkeypatch.setattr(
        "engine.runtime.storage_live_ingestion_schema.ensure_prices_schema",
        lambda conn, *, warn_nonfatal: calls.append(("prices", conn, warn_nonfatal)),
    )
    monkeypatch.setattr(
        "engine.runtime.storage_live_ingestion_schema.ensure_price_quotes_raw_schema",
        lambda conn, *, warn_nonfatal: calls.append(("price_quotes_raw", conn, warn_nonfatal)),
    )

    conn = FakeConnection()
    migration.up(conn)
    sql = "\n".join(statements)

    assert migration.id == 56
    assert "ALTER TABLE strategy_metrics ADD COLUMN IF NOT EXISTS is_active" in sql
    assert [name for name, _conn, _warn in calls] == ["prices", "price_quotes_raw"]


def test_live_ingestion_required_indexes_migration_covers_contract():
    import importlib

    migration = importlib.import_module("engine.runtime.schema.migrations.0055_live_ingestion_required_indexes")
    statements = []
    available_columns = {
        "prices": {"symbol", "ts_ms"},
        "price_quotes": {"symbol", "ts_ms"},
        "price_quotes_raw": {"symbol", "provider", "ts_ms", "event_ts_ms"},
        "price_provider_health": {"provider", "ts_ms"},
        "ingestion_pipeline_health": {"pipeline", "ts_ms"},
        "options_symbol_ingestion_state": {"disabled_until_ts_ms"},
        "alerts": {"id", "prediction_id"},
        "temporal_model_eval": {"ts_ms"},
        "execution_orders": {
            "portfolio_orders_id",
            "source_alert_id",
            "prediction_id",
            "model_id",
            "symbol",
            "submit_ts_ms",
            "order_uid",
        },
        "execution_fills": {
            "id",
            "client_order_id",
            "fill_id",
            "model_id",
            "symbol",
            "portfolio_orders_id",
            "source_alert_id",
            "prediction_id",
            "fill_ts_ms",
        },
        "pnl_attribution": {"prediction_id", "model_id", "ts_ms"},
    }

    class FakeConnection:
        def execute(self, sql, params=None):
            text = str(sql)
            if "FROM pg_attribute" in text:
                table = str((params or ("", ""))[0])
                column = str((params or ("", ""))[1])
                assert "ANY" not in text
                return type(
                    "Cursor",
                    (),
                    {"fetchone": lambda _self: (1,) if column in available_columns.get(table, set()) else None},
                )()
            statements.append(text)
            return type("Cursor", (), {"fetchone": lambda _self: None, "fetchall": lambda _self: []})()

    migration.up(FakeConnection())
    sql = "\n".join(statements)

    for index_name in (
        "idx_prices_symbol_ts",
        "idx_price_quotes_symbol_ts",
        "idx_price_quotes_raw_provider_event_ts",
        "idx_price_provider_health_ts",
        "idx_ingestion_pipeline_health_pipeline",
        "idx_options_symbol_ingestion_disabled",
        "uq_alerts_id_prediction_lineage",
        "idx_execution_orders_prediction_submit_ts",
        "idx_execution_fills_source_alert_prediction_ts",
        "uq_execution_fills_client_fillid",
        "idx_pnl_attribution_model_ts",
    ):
        assert index_name in sql
