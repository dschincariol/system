# Production Checklist

This checklist is grounded in the deployment artifacts under `deploy/`, the runtime entrypoints `start_system.py` and `start_ingestion.py`, `.env.example`, `services/credential_encryption.py`, `tools/validate_repo.py`, `tools/validate_docs.py`, and `engine/runtime/prod_preflight.py`.

## 1. Host And Runtime Preparation

- Copy `.env.example` to `.env` and set the required runtime values for the target host.
- Keep `ENGINE_MODE=safe` and `EXECUTION_MODE=safe` for the initial bring-up.
- If `DASHBOARD_HOST` is not loopback, set `DASHBOARD_API_TOKEN`.
- For the compose operator sidecar, set `OPERATOR_API_TOKEN`; live smoke must send it as `OPERATOR_API_TOKEN` or `PIPELINE_SMOKE_OPERATOR_TOKEN`.
- Provide a credential-encryption root with `DATA_SOURCE_MASTER_KEY` or `DATA_SOURCE_MASTER_KEY_FILE`.
- For the compose deployment path, copy `deploy/compose/.env.example` to `deploy/compose/.env` and set provider bootstrap credentials there only on the target host. Use `POLYGON_API_KEY`, `TRADIER_API_TOKEN`, `ALPACA_KEY_ID`, and `ALPACA_SECRET_KEY`; leave `PROD_LOCK=1`, `ALLOW_TRAINING=0`, `TRADING_IMPORT_SMOKE_IMPORT_JOBS=0`, `POLYGON_REST_ENABLED=0`, `POLYGON_WS_ENABLED=0`, `TRADIER_ENABLED=0`, `BROKER_NAME=sim`, and `BROKER=sim` until the dependency-only stack is healthy.
- Install Python dependencies with `python -m pip install -r requirements.txt`.
- Install Node dependencies with `npm ci`.
- If Timescale/Postgres, Redis, or object storage are part of the target stack, bring them up from `deploy/compose/docker-compose.external-services.yml` or the equivalent approved deployment layer before runtime bring-up.

## 2. Deployment Artifacts Present In Repo

The repository already includes these deployment assets:

- `deploy/install_trading_system.sh`
- `deploy/bin/install_python_env.sh`
- `deploy/bin/service_ctl.sh`
- `deploy/bin/backup_trading_db.sh`
- `deploy/bin/upgrade_trading_system.sh`
- `deploy/systemd/trading-engine.service`
- `deploy/systemd/trading-operator.service`
- `deploy/systemd/trading-upgrade.service`
- `deploy/systemd/trading-backup.service`
- `deploy/systemd/trading-backup.timer`
- `deploy/compose/docker-compose.external-services.yml`
- `deploy/compose/docker-compose.stack.yml`
- `deploy/compose/.env.example`
- `deploy/compose/README.md`

## 3. Static Validation Before A Change Ships

- Run `python tools/validate_docs.py` for doc-only changes.
- Run `python tools/validate_repo.py` before merge for the full deterministic validation set.
- Run `python engine/runtime/prod_preflight.py --json` when you want the explicit production preflight and smoke-cycle result.
- When external dependencies are enabled, set `PREFLIGHT_REQUIRE_TIMESCALE=1`, `PREFLIGHT_REQUIRE_REDIS=1`, and/or `PREFLIGHT_REQUIRE_OBJECT_STORAGE=1` so production preflight fails closed on missing or unreachable dependency endpoints.

## 4. Bring-Up Checks

- Start the Python runtime through `start_system.py` or the deployment wrapper that invokes it.
- Start the operator sidecar through `boot/operator_server.js` or the deployment wrapper that invokes it.
- If you are using the compose deployment path, bring the stack up with both compose files and treat the operator container as a proxy sidecar, not the lifecycle owner of the runtime.
- Confirm the dependency endpoints referenced by `TIMESCALE_DSN`, `TIMESCALE_PRICES_DSN`, `LIVE_CACHE_REDIS_URL`, and `OBJECT_STORE_ENDPOINT` are reachable from the runtime host before allowing the runtime to leave safe mode.
- Confirm `GET /api/readiness` returns a coherent readiness payload.
- Confirm `GET /api/execution/barrier` reflects the expected safe-mode block before any live enablement.
- Confirm `GET /api/operator/provider_telemetry` shows fresh provider activity for the sources that should be running.
- Confirm `GET /api/operator/service_status` and `GET /api/operator/support_snapshot` do not show unresolved startup failures.

## 5. Data-Source And Secret Checks

- Use `ui/data_sources.html` as the source-of-truth setup surface for provider credentials and source-specific settings.
- For first container bring-up, use compose `.env` only as a bootstrap contract for runtime provider variables: `POLYGON_API_KEY`, `POLYGON_REST_ENABLED`, `POLYGON_WS_ENABLED`, `TRADIER_API_TOKEN`, `TRADIER_ENABLED`, `OPTIONS_PROVIDER_CHAIN`, `OPTIONS_CRITICAL_SYMBOLS`, `ALPACA_BASE_URL`, `ALPACA_KEY_ID`, and `ALPACA_SECRET_KEY`.
- Do not enable live provider flags until `python engine/runtime/prod_preflight.py --json` passes dependency readiness and the operator readiness endpoints are reachable.
- Use `POST /api/data_sources/test` through the UI or API before enabling a newly configured source.
- Do not treat `.env` as the long-lived source of truth for provider credentials once the data-source manager is initialized.

## 6. Before Enabling Real Trading

- Confirm the runtime is not in `BOOTING`, `WARMING_UP`, `SHUTDOWN`, `KILL_SWITCH`, or unknown lifecycle state.
- Confirm `/api/execution/barrier` shows the expected mode, arming state, and reason.
- Confirm there are no active global or model kill switches that should still block execution.
- Confirm portfolio-risk APIs and broker-facing status APIs are healthy enough for the intended mode.
- Confirm the initial transition out of safe or shadow mode is an intentional operator action, not a bootstrap default.
- If object storage is required for artifacts or dataset bundles, confirm `ARTIFACT_STORE_MIRROR_ROOT` exists and is writable by the runtime user.

## 7. Ongoing Operational Checks

- Keep database backups configured through the provided backup script and systemd timer.
- Keep `logs/` available for runtime and operator log tails.
- Use `/api/operator/runtime_watchdogs`, `/api/operator/provider_telemetry`, and `/api/operator/support_snapshot` as the first-line operational checks.
- Run `python tools/validate_repo.py --live` only against an intentionally running stack when a live smoke test is required.
- Re-run `python engine/runtime/prod_preflight.py --json` after dependency changes so external-service readiness and runtime smoke are captured together.
- For the compose deployment path, run `docker compose --env-file deploy/compose/.env -f deploy/compose/docker-compose.external-services.yml -f deploy/compose/docker-compose.stack.yml exec runtime python engine/runtime/prod_preflight.py --json` before calling the stack staging-ready.
