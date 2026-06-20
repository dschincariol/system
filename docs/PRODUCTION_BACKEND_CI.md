# Production Backend CI

The GitHub `Production backend gate (Postgres + Redis)` job is the go-live backend gate. It is separate from the Linux SQLite contract job, which proves the local test backend contract on `ubuntu-latest`.

## What The Gate Runs

- provisions a Postgres 16 service with TimescaleDB available and a Redis 7 service
- sets `TS_PRODUCTION_BACKEND_TESTS=1`, `TS_STORAGE_BACKEND=postgres`, `TS_PG_DSN`, and `TS_REDIS_URL`
- runs all tests marked `requires_postgres` or `requires_redis`
- fails if the marker-selected tests collect zero tests or produce any pytest skip
- runs targeted production-path tests for migrations, Postgres locks, idempotency uniqueness, audit-chain detection, execution arming persistence, promotion evidence, CPCV/model competition, and Redis-backed cache wrappers
- runs `python -m engine.runtime.staging_prod_preflight`, which invokes `engine/runtime/prod_preflight.py --json`, against the CI Postgres target and uploads the redacted evidence artifact

The skip failure is enforced by [tools/run_required_backend_tests.py](../tools/run_required_backend_tests.py), which inspects pytest JUnit XML instead of grepping terminal output.

## Local Reproduction

Start local services on the same ports used by CI. The Postgres container uses the TimescaleDB image because the production migration path creates Timescale extension objects.

```bash
docker run --rm --name trading-ci-postgres \
  -e POSTGRES_DB=trading_ci \
  -e POSTGRES_USER=ts_app \
  -e POSTGRES_PASSWORD=test-app-password \
  -p 5432:5432 timescale/timescaledb:latest-pg16
```

```bash
docker run --rm --name trading-ci-redis \
  -p 6379:6379 redis:7
```

In a second shell from the repo root:

```bash
python -m pip install -r requirements-dev.txt

export PYTHONPATH=.
export TS_PRODUCTION_BACKEND_TESTS=1
export TS_STORAGE_BACKEND=postgres
export TS_PG_DSN="host=127.0.0.1 port=5432 user=ts_app dbname=trading_ci password=test-app-password"
export TS_PG_PASSWORD=test-app-password
export TS_PG_SCHEMA_PER_DB_PATH=1
export TS_PG_POOL_SIZE=16
export TS_PG_POOL_MIN_SIZE=1
export TS_PG_POOL_TIMEOUT=15
export TS_PG_CONNECT_TIMEOUT=5
export TRADING_UNIT_TEST_SCHEMA_FAST=1
export TRADING_FAILURE_DIAGNOSTICS_PERSIST=0
export TRADING_PG_AUTOINIT_ON_CONNECT=1
export TS_REDIS_URL=redis://127.0.0.1:6379/0
export TS_CACHE_REAL_INTEGRATION=1
export LIVE_CACHE_BACKEND=redis
export LIVE_CACHE_REDIS_URL=redis://127.0.0.1:6379/0
export APP_ENV=test
export ENGINE_MODE=safe
export EXECUTION_MODE=safe
export OPERATOR_MODE=safe
export PROD_LOCK=0
export KILL_SWITCH_GLOBAL=0
```

Run the marker gate:

```bash
python tools/run_required_backend_tests.py \
  --label marked-production-backend-tests \
  -- -q -m "requires_postgres or requires_redis" -rs
```

Run the targeted production-path suite:

```bash
python tools/run_required_backend_tests.py \
  --label targeted-production-path-tests \
  -- -q -rs \
    tests/test_storage_migrator.py \
    tests/test_migrator_lock_release_on_rollback.py \
    tests/test_storage_pg_runtime_regressions.py \
    tests/test_storage_locks_pg.py \
    tests/test_layer5_audit_chain_bypass_detected.py \
    tests/test_live_trading_preflight.py::test_execution_mode_refuses_live_arming_before_initial_kill_switch_hold \
    tests/test_live_trading_preflight.py::test_operator_execution_arm_writes_hash_chain_audit \
    tests/test_runtime_reliability_regressions.py::RuntimeReliabilityRegressionTests::test_execution_mode_respects_caller_transaction_boundaries \
    tests/test_runtime_reliability_regressions.py::RuntimeReliabilityRegressionTests::test_concurrent_order_claims_use_single_idempotency_row \
    tests/test_promotion_guard_fdr.py \
    tests/test_cpcv.py \
    tests/test_model_competition_real_pnl.py \
    tests/test_cache_wrappers_integration.py::test_real_redis_postgres_wrappers_integration \
    tests/test_live_cache.py::LiveCacheTests::test_explicit_redis_live_cache_round_trip
```

## Staging Preflight Evidence

For local staging-style evidence, use [docs/STAGING_PROD_PREFLIGHT_EVIDENCE.md](STAGING_PROD_PREFLIGHT_EVIDENCE.md). The CI job creates an env file under `$RUNNER_TEMP`, includes only test credentials such as a synthetic `DATA_SOURCE_MASTER_KEY`, runs the staging harness with ambient-env isolation, and uploads `var/artifacts/preflight/staging/*.json`. The artifact includes a redacted Postgres target summary, guardrail result, `prod_preflight.py --json` output, and subprocess exit code.
