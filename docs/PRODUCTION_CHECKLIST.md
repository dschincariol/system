# Production Checklist

This checklist is grounded in the deployment artifacts under `deploy/`, the runtime entrypoints `start_system.py` and `start_ingestion.py`, `.env.example`, `services/credential_encryption.py`, `tools/validate_repo.py`, `tools/validate_docs.py`, and `engine/runtime/prod_preflight.py`.

## 1. Host And Runtime Preparation

- Copy `.env.example` to `.env` and set the required runtime values for the target host.
- Keep `ENGINE_MODE=safe` and `EXECUTION_MODE=safe` for the initial bring-up.
- Set `DASHBOARD_API_TOKEN` to a generated high-entropy value for every production or live deployment, even when the dashboard binds to loopback. Do not use placeholders such as `change-me`, `secret`, or short test tokens.
- Leave `TS_API_ALLOW_LOCALHOST_MUTATIONS_WITHOUT_TOKEN=0` or unset outside explicit local dev/test. The no-token localhost mutation fallback is accepted only when the environment is dev/test, both engine and execution modes are safe/dev, and that flag is enabled.
- For the compose operator sidecar, set `OPERATOR_API_TOKEN`; live smoke must send it as `OPERATOR_API_TOKEN` or `PIPELINE_SMOKE_OPERATOR_TOKEN`.
- Provide a credential-encryption root with `DATA_SOURCE_MASTER_KEY` or `DATA_SOURCE_MASTER_KEY_FILE`.
- For the compose deployment path, copy `deploy/compose/.env.example` to `deploy/compose/.env` and set provider bootstrap credentials there only on the target host. Use `POLYGON_API_KEY`, `TRADIER_API_TOKEN`, `ALPACA_KEY_ID`, and `ALPACA_SECRET_KEY`; leave `PROD_LOCK=1`, `ALLOW_TRAINING=0`, `TRADING_IMPORT_SMOKE_IMPORT_JOBS=0`, `POLYGON_REST_ENABLED=0`, `POLYGON_WS_ENABLED=0`, `TRADIER_ENABLED=0`, `BROKER_NAME=sim`, and `BROKER=sim` until the dependency-only stack is healthy.
- For local Linux/macOS validation workstations, run `bash tools/bootstrap_local_toolchain.sh` from the repository root. It prepares `.venv`, installs Python dependencies from `requirements.txt`, installs Node.js 20.19.4 with npm 10.8.2 inside `.venv` when needed, runs `npm ci`, and creates user-local shims for the `python`, `python3`, `node`, `npm`, and `npx` command names.
- Install Python dependencies with `python -m pip install -r requirements.txt`.
- Use Node.js 20 LTS (`>=20.17.0 <21`) with npm 10.x for the operator UI. The repository `.npmrc` sets `engine-strict=true`, so `npm ci` fails early on unsupported Node/npm versions.
- Install Node dependencies reproducibly with `npm ci`.
- Bring up the Postgres/PgBouncer endpoint required by `TS_PG_DSN` before runtime bring-up. Postgres runtime storage is mandatory for production-like operation; SQLite is not a production fallback.
- If Timescale sidecars, Redis, or object storage are part of the target stack, bring them up from `deploy/compose/docker-compose.external-services.yml` or the equivalent approved deployment layer before runtime bring-up.
- Set `DB_PATH` to an absolute local data directory such as `/var/lib/trading`. It remains a data-root/legacy compatibility hint, not the Postgres database target.
- On a single-server systemd host, confirm `/etc/credstore.encrypted/` contains encrypted credentials named `master_key`, `pg_password_app`, `pg_password_ingest`, and `pg_password_reader`, and that service units expose only the needed entries with `LoadCredentialEncrypted=`.
- Confirm the runtime process can read/search/write the `DB_PATH` data root before schema initialization. `prod_preflight.py` validates this before touching Postgres so missing `CREDENTIALS_DIRECTORY` or bad `/var/lib/trading` permissions fail with provisioning errors instead of late schema errors.

## 2. Deployment Artifacts Present In Repo

The repository already includes these deployment assets:

- `deploy/install_trading_system.sh`
- `deploy/bin/install_python_env.sh`
- `deploy/bin/service_ctl.sh`
- `deploy/bin/backup_trading_db.sh` for legacy/local SQLite-file backups only
- `deploy/bin/upgrade_trading_system.sh`
- `deploy/systemd/trading-engine.service`
- `deploy/systemd/trading-operator.service`
- `deploy/systemd/trading-upgrade.service`
- `deploy/systemd/trading-backup.service`
- `deploy/systemd/trading-backup.timer`
- `deploy/compose/docker-compose.external-services.yml`
- `deploy/compose/docker-compose.stack.yml`
- `deploy/compose/.env.example`
- `ops/backup/base_backup.sh`
- `ops/backup/wal_archive.sh`
- `ops/backup/restore.sh`
- `ops/backup/restore_drill.sh`
- `ops/backup/backup_restore_evidence.sh`
- `ops/server/install_backup_evidence_gate.sh`
- `ops/server/systemd/trading-base-backup.service`
- `ops/server/systemd/trading-base-backup.timer`
- `ops/server/systemd/trading-backup-evidence.service`
- `ops/server/systemd/trading-backup-evidence.timer`
- `ops/server/systemd/trading-restore-drill.service`
- `ops/server/systemd/trading-restore-drill.timer`
- `deploy/compose/README.md`

## 3. Static Validation Before A Change Ships

- Run `python tools/validate_docs.py` for doc-only changes.
- Run `npm run check:ui` after `npm ci` for UI changes and before production handoff. It checks tracked local asset references, dashboard JS syntax with the production Node runtime, and the browser-helper test suite.
- Run `python tools/validate_dependency_lock.py` after dependency manifest changes.
- Run `python tools/validate_repo.py` before merge for the full deterministic validation set.
- Run `python engine/runtime/prod_preflight.py --json` when you want the explicit production preflight and smoke-cycle result.
- Treat `prod_preflight.py` provisioning errors as hard blockers. Missing systemd credentials, missing required Postgres role password credentials, or an unreadable/unwritable runtime data root must be fixed in the host/unit layer before retrying.
- Treat an API-auth preflight failure as a hard production blocker. Production/live mode must have a non-placeholder `DASHBOARD_API_TOKEN`; localhost-only fallback is dev-only.
- When external dependencies are enabled, set `PREFLIGHT_REQUIRE_TIMESCALE=1`, `PREFLIGHT_REQUIRE_REDIS=1`, and/or `PREFLIGHT_REQUIRE_OBJECT_STORAGE=1` so production preflight fails closed on missing or unreachable dependency endpoints.

## 4. Bring-Up Checks

- Start the Python runtime through `start_system.py` or the deployment wrapper that invokes it.
- Start the operator sidecar through `boot/operator_server.js` or the deployment wrapper that invokes it.
- If you are using the compose deployment path, bring the stack up with both compose files and treat the operator container as a proxy sidecar, not the lifecycle owner of the runtime.
- Confirm the dependency endpoints referenced by `TIMESCALE_DSN`, `TIMESCALE_PRICES_DSN`, `LIVE_CACHE_REDIS_URL`, and `OBJECT_STORE_ENDPOINT` are reachable from the runtime host before allowing the runtime to leave safe mode.
- Confirm the Postgres endpoint referenced by `TS_PG_DSN` or platform defaults is reachable, and that `python engine/runtime/prod_preflight.py --json` reports the Postgres contract as healthy.
- Confirm `GET /api/readiness` returns a coherent readiness payload.
- Confirm `GET /api/execution/barrier` reflects the expected safe-mode block before any live enablement.
- Confirm `GET /api/operator/provider_telemetry` shows fresh provider activity for the sources that should be running.
- Confirm `GET /api/operator/service_status` and `GET /api/operator/support_snapshot` do not show unresolved startup failures.

## 5. Data-Source And Secret Checks

- Use `ui/data_sources.html` as the source-of-truth setup surface for provider credentials and source-specific settings.
- Data-source CRUD, terminal order-entry, operator control, job-control, and repair/governance mutation routes are POST-only and pass through dashboard mutation auth, rate limiting, and append-only `api_mutation` event logging before handler execution.
- Use `/api/broker/config`, `/api/broker/test_connection`, and `/api/broker/audit` as the broker configuration control plane. Do not activate a non-`sim` broker until the connection test passes for the same broker and the audit rows show the expected operator action.
- For first container bring-up, use compose `.env` only as a bootstrap contract for runtime provider variables: `POLYGON_API_KEY`, `POLYGON_REST_ENABLED`, `POLYGON_WS_ENABLED`, `TRADIER_API_TOKEN`, `TRADIER_ENABLED`, `OPTIONS_PROVIDER_CHAIN`, `OPTIONS_CRITICAL_SYMBOLS`, `ALPACA_BASE_URL`, `ALPACA_KEY_ID`, and `ALPACA_SECRET_KEY`.
- Do not enable live provider flags until `python engine/runtime/prod_preflight.py --json` passes dependency readiness and the operator readiness endpoints are reachable.
- Use `POST /api/data_sources/test` through the UI or API before enabling a newly configured source.
- Do not treat `.env` as the long-lived source of truth for provider credentials once the data-source manager is initialized.

## 6. Before Enabling Real Trading

- Confirm the runtime is not in `BOOTING`, `WARMING_UP`, `SHUTDOWN`, `KILL_SWITCH`, or unknown lifecycle state.
- Confirm `DISABLE_LIVE_EXECUTION` is unset or explicitly false (`0`, `false`, `no`, or `off`). A truthy or unknown non-empty value is a hard live-capital block in the runtime gate, kill-switch cascade, broker router, broker adapters, and terminal order-entry APIs.
- Confirm `ENGINE_MODE=live` and `EXECUTION_MODE=live`; environment mode alone does not arm execution.
- Confirm `LIVE_BROKER`, `BROKER`, `BROKER_NAME`, and `BROKER_FAILOVER` identify the intended live broker and do not include `sim`, `paper`, or `sandbox`.
- Confirm `GET /api/broker/config` shows the intended active broker, masked credentials, and the expected last passing test result; confirm `GET /api/broker/audit` contains the recent configuration and test actions.
- Confirm the initial deployment hold has `KILL_SWITCH_GLOBAL=1` until operator signoff. Clearing that hold is not sufficient to trade; live execution still requires the audited DB `execution_mode` row to be `mode=live, armed=1`.
- Confirm provider credentials are real for the selected broker. Alpaca live mode must not use `https://paper-api.alpaca.markets`; IBKR live mode must set `IBKR_HOST`, `IBKR_PORT`, and `IBKR_CLIENT_ID` explicitly.
- Confirm pre-live position reconciliation is enabled with `EXECUTION_PRELIVE_RECONCILE` unset or true. Any break-glass override must include non-placeholder actor and reason values and must be visible in runtime event evidence.
- Confirm `/api/execution/barrier` shows the expected mode, arming state, and reason.
- Confirm there are no active global or model kill switches that should still block execution.
- Confirm portfolio-risk APIs and broker-facing status APIs are healthy enough for the intended mode.
- Confirm the initial transition out of safe or shadow mode is an intentional operator action, not a bootstrap default.
- If object storage is required for artifacts or dataset bundles, confirm `ARTIFACT_STORE_MIRROR_ROOT` exists and is writable by the runtime user.

## 7. Ongoing Operational Checks

- Keep Postgres base backups, WAL archive, backup pruning, and restore-drill timers configured through `ops/backup/` and `ops/server/systemd/`. The older `deploy/bin/backup_trading_db.sh` copies a SQLite file and is not sufficient for the current Postgres-backed runtime.
- Treat restores as part of operations: run the restore drill into a clean target on the agreed cadence and keep the latest drill report with the backup evidence.
- Before live promotion, run `ops/backup/backup_restore_evidence.sh` on the target server. Keep the timestamped report and `latest_backup_restore_evidence.json`; live preflight and model promotion guard fail closed when verified backup, WAL archive evidence, restore drill freshness, or restore duration violates the configured `BACKUP_EVIDENCE_*` RPO/RTO policy.
- On the Compose production server, install this gate with `sudo bash ops/server/install_backup_evidence_gate.sh --compose --restart-postgres --run-evidence`. This applies the TimescaleDB WAL archive bind mount/settings, installs the 60-second evidence refresh timer, and runs version-matched backup/restore tools from the Timescale image.
- Keep `logs/` available for runtime and operator log tails.
- Use `/api/operator/runtime_watchdogs`, `/api/operator/provider_telemetry`, and `/api/operator/support_snapshot` as the first-line operational checks.
- Run `python tools/validate_repo.py --live` only against an intentionally running stack when a live smoke test is required.
- Re-run `python engine/runtime/prod_preflight.py --json` after dependency changes so external-service readiness and runtime smoke are captured together.
- For the compose deployment path, run `docker compose --env-file deploy/compose/.env -f deploy/compose/docker-compose.external-services.yml -f deploy/compose/docker-compose.stack.yml exec runtime python engine/runtime/prod_preflight.py --json` before calling the stack staging-ready.
