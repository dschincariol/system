# Production Backend CI

The GitHub `Production backend gate (Postgres + Redis)` job is the go-live backend gate. It is separate from the Linux SQLite contract job, which proves the local test backend contract on `ubuntu-latest`, and from the `Safety-critical money path (SQLite/mocks)` job, which runs the runtime-owned safety-control suites on every merge.

## What The Gate Runs

- provisions a Postgres 16 service with TimescaleDB available and a Redis 7 service; the Postgres service sets `--shm-size 1g` so the full-suite schema/migration workload does not exhaust Docker's small default shared-memory mount
- sets `TS_PRODUCTION_BACKEND_TESTS=1`, `TS_STORAGE_BACKEND=postgres`, `TS_PG_DSN`, and `TS_REDIS_URL`
- runs all tests marked `requires_postgres` or `requires_redis`
- fails if the marker-selected tests collect zero tests or produce any pytest skip
- runs the full `pytest tests/` tree once under `TS_STORAGE_BACKEND=postgres`
  with `LIVE_CACHE_BACKEND=redis`; this is intentionally slower than the old
  targeted-only backend gate, but it exercises the production storage/cache
  implementation where locking, isolation, upsert, and idempotency behavior can
  differ from SQLite
- installs Python test dependencies through `requirements-dev.txt`, which applies
  `requirements-dev.lock.txt` before resolving packages
- runs targeted production-path tests for migrations, Postgres locks, idempotency uniqueness, audit-chain detection, execution arming persistence, promotion evidence, CPCV/model competition, and Redis-backed cache wrappers
- inherits the repo pytest timeout policy from `pyproject.toml`: a 120 second per-test default through `pytest-timeout` with `timeout_method=thread`
- inherits the repo pytest socket isolation policy: DNS and non-local sockets are blocked by default, while the provisioned Postgres and Redis services remain reachable through `127.0.0.1`
- runs `python -m engine.runtime.staging_prod_preflight`, which invokes `engine/runtime/prod_preflight.py --json`, against the CI Postgres target and uploads the redacted evidence artifact
- uploads JUnit XML for the backend marker, targeted backend, and full Postgres
  suite runs

The skip and shrinkage failures are enforced by [tools/run_required_backend_tests.py](../tools/run_required_backend_tests.py), which inspects pytest JUnit XML instead of grepping terminal output. The marked and targeted backend runs fail on any skip. The full-suite Postgres run allows only explicitly configured optional-capability skips, such as ROCm hardware, Julia/PySR, pgbouncer DSNs, Stable-Baselines3-vs-fallback RL coverage, or Node helper availability; Postgres/Redis reachability skips remain failures because those services are provisioned in the job.

`@pytest.mark.live_network` tests are not part of this PR gate. They require an explicit run with `TRADING_TEST_ALLOW_LIVE_NETWORK=1` and should be reserved for reviewed live-service smoke checks outside normal CI.

## Safety-Critical Money Path Gate

The `Safety-critical money path (SQLite/mocks)` job runs the eight runtime-owned
money-path control suites under safe-mode SQLite/mocks:

- `tests/test_kill_switch_regressions.py`
- `tests/test_broker_router_dry_run_gates.py`
- `tests/test_broker_order_idempotency_regressions.py`
- `tests/test_broker_apply_orders_modes.py`
- `tests/test_drawdown_fail_closed.py`
- `tests/test_real_capital_safety_e2e.py`
- `tests/test_position_reconcile_safety.py`
- `tests/test_live_prelive_reconcile_policy.py`

Each file uses module-level `pytestmark = pytest.mark.safety_critical`. Module
scope is deliberate because these files mix `unittest.TestCase` methods and
module-level `test_` functions; a single module-level mark is selected reliably
by `pytest -m safety_critical` for both styles.

The CI command passes every file as an expected source and requires at least
`130` selected tests. If a file is removed, renamed, loses the marker, or the
selection goes empty, the runner prints `selected_tests=N` and fails the job
before the gate can silently shrink. A repository meta-test also checks that the
workflow names exactly those eight files and that each carries the module-level
marker.

## Local Reproduction

Start local services on the same ports used by CI. The Postgres container uses the TimescaleDB image because the production migration path creates Timescale extension objects.

```bash
docker run --rm --name trading-ci-postgres \
  --shm-size=1g \
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

Run the full Postgres/Redis suite:

```bash
TRADING_UNIT_TEST_SCHEMA_FAST=0 python tools/run_required_backend_tests.py \
  --label full-postgres-redis-suite \
  --min-selected 2400 \
  --allow-skip-message-regex "PySR and Julia are required" \
  --allow-skip-message-regex "TS_PGBOUNCER_TEST_DSN" \
  --allow-skip-message-regex "requires Python 3\\.12 ROCm runtime image" \
  --allow-skip-message-regex "ROCm torch GPU is unavailable" \
  --allow-skip-message-regex "node executable is not available" \
  --allow-skip-message-regex "node is required" \
  --allow-skip-message-regex "dependency-free fallback" \
  -- -q tests/ -rs
```

The full-suite job is not sharded and does not disable pytest plugins. That
keeps the merge gate as close as practical to the real pytest invocation while
accepting the runtime cost of a slower Postgres lane. It also disables the
fast schema shortcut so Timescale continuous aggregates, compression, and
retention policies are validated on the same path production uses.

Run the safety-critical SQLite/mocks gate:

```bash
python tools/run_required_backend_tests.py \
  --label safety-critical-money-path \
  --min-selected 130 \
  --expected-source tests/test_kill_switch_regressions.py \
  --expected-source tests/test_broker_router_dry_run_gates.py \
  --expected-source tests/test_broker_order_idempotency_regressions.py \
  --expected-source tests/test_broker_apply_orders_modes.py \
  --expected-source tests/test_drawdown_fail_closed.py \
  --expected-source tests/test_real_capital_safety_e2e.py \
  --expected-source tests/test_position_reconcile_safety.py \
  --expected-source tests/test_live_prelive_reconcile_policy.py \
  -- -q -m "safety_critical" -rs \
    tests/test_kill_switch_regressions.py \
    tests/test_broker_router_dry_run_gates.py \
    tests/test_broker_order_idempotency_regressions.py \
    tests/test_broker_apply_orders_modes.py \
    tests/test_drawdown_fail_closed.py \
    tests/test_real_capital_safety_e2e.py \
    tests/test_position_reconcile_safety.py \
    tests/test_live_prelive_reconcile_policy.py
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

Canonical harness mechanics (setup, run commands, guardrails, evidence contents): see [docs/STAGING_PROD_PREFLIGHT_EVIDENCE.md](STAGING_PROD_PREFLIGHT_EVIDENCE.md).

CI-specific behavior only: the production-backend gate creates an env file under `$RUNNER_TEMP`, includes only test credentials such as a synthetic `DATA_SOURCE_MASTER_KEY`, runs the staging harness with ambient-env isolation, and uploads `var/artifacts/preflight/staging/*.json`. The artifact includes a redacted Postgres target summary, guardrail result, `prod_preflight.py --json` output, and subprocess exit code.
