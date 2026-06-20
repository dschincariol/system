from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_production_backend_test_mode_preserves_explicit_service_targets(monkeypatch) -> None:
    from engine.runtime import test_isolation

    monkeypatch.setenv("TS_PRODUCTION_BACKEND_TESTS", "1")
    monkeypatch.setenv("TS_STORAGE_BACKEND", "postgres")
    monkeypatch.setenv("TS_PG_DSN", "host=127.0.0.1 port=5432 user=ts_app dbname=trading_ci password=test")
    monkeypatch.setenv("TS_PG_PASSWORD", "test")
    monkeypatch.setenv("TS_REDIS_URL", "redis://127.0.0.1:6379/0")
    monkeypatch.setenv("LIVE_CACHE_BACKEND", "redis")
    monkeypatch.setenv("LIVE_CACHE_REDIS_URL", "redis://127.0.0.1:6379/0")
    monkeypatch.setenv("TS_PG_POOL_TIMEOUT", "15")

    env = test_isolation._build_base_test_env()

    assert env["TS_TESTING"] == "1"
    assert env["TS_STORAGE_BACKEND"] == "postgres"
    assert "trading_ci" in env["TS_PG_DSN"]
    assert env["TS_REDIS_URL"] == "redis://127.0.0.1:6379/0"
    assert env["LIVE_CACHE_BACKEND"] == "redis"
    assert env["LIVE_CACHE_REDIS_URL"] == "redis://127.0.0.1:6379/0"
    assert env["TS_PG_POOL_TIMEOUT"] == "15"


def test_required_backend_junit_inspection_detects_skipped_tests(tmp_path: Path) -> None:
    from tools.run_required_backend_tests import inspect_junit

    junit = tmp_path / "pytest.xml"
    junit.write_text(
        """
        <testsuite tests="1" skipped="1">
          <testcase classname="tests.test_pg" name="test_needs_postgres">
            <skipped message="postgres not reachable at TS_PG_DSN" />
          </testcase>
        </testsuite>
        """,
        encoding="utf-8",
    )

    test_count, skipped = inspect_junit(junit)

    assert test_count == 1
    assert skipped == ["tests.test_pg::test_needs_postgres: postgres not reachable at TS_PG_DSN"]


def test_validate_workflow_has_hard_gated_production_backend_job() -> None:
    workflow = (ROOT / ".github/workflows/validate.yml").read_text(encoding="utf-8")

    assert "production-backend:" in workflow
    assert "Production backend gate (Postgres + Redis)" in workflow
    assert "image: timescale/timescaledb:latest-pg16" in workflow
    assert "image: redis:7" in workflow
    assert 'TS_PRODUCTION_BACKEND_TESTS: "1"' in workflow
    assert "TS_STORAGE_BACKEND: postgres" in workflow
    assert "TS_PG_DSN:" in workflow
    assert "TS_REDIS_URL:" in workflow
    assert "tools/run_required_backend_tests.py" in workflow
    assert 'requires_postgres or requires_redis' in workflow
    assert "tests/test_storage_migrator.py" in workflow
    assert "tests/test_layer5_audit_chain_bypass_detected.py" in workflow
    assert "tests/test_live_cache.py::LiveCacheTests::test_explicit_redis_live_cache_round_trip" in workflow
    assert "engine.runtime.staging_prod_preflight" in workflow
    assert "DATA_SOURCE_MASTER_KEY=" in workflow
    assert "actions/upload-artifact@v4" in workflow


def test_sqlite_contract_job_declares_it_is_not_the_production_gate() -> None:
    workflow = (ROOT / ".github/workflows/validate.yml").read_text(encoding="utf-8")

    assert "SQLite contract validation" in workflow
    assert "This job is intentionally not the production-backend gate." in workflow


def test_local_reproduction_docs_cover_backend_gate_and_evidence() -> None:
    docs = (ROOT / "docs/PRODUCTION_BACKEND_CI.md").read_text(encoding="utf-8")

    assert "TS_PRODUCTION_BACKEND_TESTS=1" in docs
    assert "TS_STORAGE_BACKEND=postgres" in docs
    assert "TS_PG_DSN=" in docs
    assert "TS_REDIS_URL=" in docs
    assert "tools/run_required_backend_tests.py" in docs
    assert "requires_postgres or requires_redis" in docs
    assert "engine.runtime.staging_prod_preflight" in docs
    assert "var/artifacts/preflight/staging" in docs
