# Production Checklist

This checklist is grounded in the deployment artifacts under `deploy/`, the runtime entrypoints `start_system.py` and `start_ingestion.py`, `.env.example`, `services/credential_encryption.py`, `tools/validate_repo.py`, `tools/validate_docs.py`, and `engine/runtime/prod_preflight.py`.

## 1. Host And Runtime Preparation

- Copy `.env.example` to `.env` and set the required runtime values for the target host.
- Keep `ENGINE_MODE=safe` and `EXECUTION_MODE=safe` for the initial bring-up.
- Set `DASHBOARD_API_TOKEN` to a generated high-entropy value for every production or live deployment, even when the dashboard binds to loopback. Do not use placeholders such as `change-me`, `secret`, or short test tokens. The same token protects dashboard mutations reached through the `/operator/api/*` same-origin bridge before any request is proxied to the operator sidecar.
- Keep `LIVE_TRADING_CONFIRM` unset or `0` during production bring-up. Set it to the exact built-in phrase `I_UNDERSTAND_LIVE_TRADING` only in the target host's operator-controlled deployment configuration when live execution is intentionally being enabled.
- Leave `TS_API_ALLOW_LOCALHOST_MUTATIONS_WITHOUT_TOKEN=0` or unset outside explicit local dev/test. The no-token localhost mutation fallback is accepted only when the environment is dev/test, both engine and execution modes are safe/dev, and that flag is enabled.
- For the compose operator sidecar, set `OPERATOR_API_TOKEN`; live smoke must send it as `OPERATOR_API_TOKEN` or `PIPELINE_SMOKE_OPERATOR_TOKEN`. Loopback alone does not authorize sidecar reads, mutations, or WebSocket upgrades. Only `/api/operator/ping` is unauthenticated. The dashboard bridge forwards `X-Operator-Token` only from server-side config after dashboard auth succeeds.
- Provide a credential-encryption root with `DATA_SOURCE_MASTER_KEY` or `DATA_SOURCE_MASTER_KEY_FILE`. Production/live preflight accepts only canonical base64 text for exactly 32 random bytes; raw text, placeholders, short strings, low-entropy values, empty files, and ordinary files with group/other permissions fail closed. Prefer the file form:
  ```bash
  sudo install -o trading -g trading -m 0600 /dev/null /var/lib/trading/.data_source_master_key
  sudo -u trading sh -c 'openssl rand -base64 32 > /var/lib/trading/.data_source_master_key'
  ```
- For the compose deployment path, copy `deploy/compose/.env.example` to `deploy/compose/.env` and set provider bootstrap credentials there only on the target host. Use `POLYGON_API_KEY`, `TRADIER_API_TOKEN`, `ALPACA_KEY_ID`, and `ALPACA_SECRET_KEY`; leave `PROD_LOCK=1`, `ALLOW_TRAINING=0`, `TRADING_IMPORT_SMOKE_IMPORT_JOBS=0`, `POLYGON_REST_ENABLED=0`, `POLYGON_WS_ENABLED=0`, `TRADIER_ENABLED=0`, `ENGINE_MODE=safe`, `EXECUTION_MODE=safe`, `DISABLE_LIVE_EXECUTION=1`, and the initial kill-switch hold enabled until the dependency-only stack is healthy. Keep `LIVE_BROKER`, `BROKER`, `BROKER_NAME`, and `BROKER_FAILOVER` pinned to the same intended live broker (`ibkr` in the template).
- Keep `TRADING_DEPENDENCY_PROFILE=cpu`, `RUNTIME_HARDWARE_PROFILE=cpu`, all `*_DEVICE=cpu`, `NVIDIA_TELEMETRY_ENABLED=0`, `GPU_THROTTLE_ENABLE=0`, `PINNED_ENABLE=0`, `PINNED_PREFETCH=0`, `TORCH_ALLOW_TF32=0`, `CUDNN_ALLOW_TF32=0`, and `CUDNN_BENCHMARK=0` for the default AMD/CPU deployment. NVIDIA CUDA requires both `TRADING_DEPENDENCY_PROFILE=nvidia-cuda` and `RUNTIME_HARDWARE_PROFILE=nvidia`; AMD/ROCm remains blocked until a reviewed host-specific requirements file, ROCm runtime, device permissions, and PyTorch HIP support are validated. See [DEPENDENCY_PROFILES.md](DEPENDENCY_PROFILES.md) for verification and CPU rollback commands.
- Keep Docker resource isolation enabled for compose production. The default profile in `deploy/compose/.env.example` targets a 16-core / 32-thread / 123 GiB host: runtime `12 CPU / 48g RAM / 8g shm`, Timescale `8 CPU / 32g RAM / 2g shm`, Redis `2 CPU / 8g RAM / 6gb maxmemory`, MinIO `2 CPU / 6g RAM`, and operator `1 CPU / 2g RAM`. This caps containers at 25 logical CPUs and 96 GiB RAM, leaving about 7 logical CPUs and 27 GiB RAM for the OS, Docker, diagnostics, tests, IDEs, and emergency shell work. Do not lower `TRADING_RESOURCE_MIN_HEADROOM_CPUS=6` or `TRADING_RESOURCE_MIN_HEADROOM_MEMORY=24g` without a host-specific capacity review.
- Keep Timescale/Postgres tuning aligned with the container limits: `PREFLIGHT_REQUIRE_DOCKER_POSTGRES_TUNING=1`, `TIMESCALE_SHARED_BUFFERS=8GB`, `TIMESCALE_EFFECTIVE_CACHE_SIZE=22GB`, `TIMESCALE_WORK_MEM=48MB`, `TIMESCALE_MAINTENANCE_WORK_MEM=2GB`, `TIMESCALE_AUTOVACUUM_WORK_MEM=512MB`, `TIMESCALE_MAX_CONNECTIONS=100`, `TIMESCALE_MAX_WAL_SIZE=16GB`, `TIMESCALE_MAX_SLOT_WAL_KEEP_SIZE=8GB`, and `TIMESCALE_WAL_DISK_BUDGET=40g`. `prod_preflight.py` reports unbounded services, excessive Postgres estimates, WAL budget drift, Redis maxmemory drift, undersized `/dev/shm`, and oversized runtime worker/thread defaults before the stack is production-ready.
- For the same host, set `INGESTION_TUNING_PROFILE=host_32t_123g` and keep `INGESTION_TUNING_MAX_TOTAL_DB_CONNECTIONS=32`, `INGESTION_TUNING_MAX_BUFFERED_ROWS=1200000`, `INGESTION_CHILD_TS_PG_POOL_SIZE=3`, `INGESTION_CHILD_TIMESCALE_POOL_MAX_SIZE=4`, and `INGESTION_CHILD_TIMESCALE_PRICES_POOL_MAX_SIZE=4`. The profile raises ingestion writer batches and selected parent pools while reducing queue depth so buffered-row risk does not grow, and child feed jobs get smaller explicit pools to avoid multiplying connections across processes. `start_ingestion.py` and `prod_preflight.py` fail closed when explicit queue or pool overrides exceed hard bounds or the combined DB pool/queue budget. During soak, confirm `/api/health` and `runtime_meta.ingestion_state.writer_diagnostics` show stable queue depth, low flush latency, no dead letters, no dropped rows, and no Timescale backpressure before live enablement.
- For local Linux/macOS validation workstations, run `bash tools/bootstrap_local_toolchain.sh` from the repository root. It prepares `.venv`, installs the selected Python dependency profile (`TRADING_DEPENDENCY_PROFILE=cpu` by default), installs Node.js 20.19.4 with npm 10.8.2 inside `.venv` when needed, runs `npm ci`, and creates user-local shims for the `python`, `python3`, `node`, `npm`, and `npx` command names.
- Install CPU/default Python dependencies with `TRADING_DEPENDENCY_PROFILE=cpu python -m pip install -r requirements.txt`.
- Use Node.js 20 LTS (`>=20.17.0 <21`) with npm 10.x for the operator UI. The repository `.npmrc` sets `engine-strict=true`, so `npm ci` fails early on unsupported Node/npm versions.
- Install Node dependencies reproducibly with `npm ci`.
- Bring up the Postgres/PgBouncer endpoint required by `TS_PG_DSN` before runtime bring-up. Postgres runtime storage is mandatory for production-like operation; SQLite is not a production fallback.
- If Timescale sidecars, Redis, or object storage are part of the target stack, bring them up from `deploy/compose/docker-compose.external-services.yml` or the equivalent approved deployment layer before runtime bring-up.
- Set `DB_PATH` to an absolute local data directory such as `/var/lib/trading` on systemd hosts or `/app/data` inside compose containers. It remains a data-root/legacy compatibility hint, not the Postgres database target. Relative values fail runtime config and production preflight.
- On a single-server systemd host, confirm `/etc/credstore.encrypted/` contains encrypted credentials named `master_key`, `pg_password_app`, `pg_password_ingest`, `pg_password_reader`, `redis_password`, `object_store_secret_key`, and `dashboard_api_token`, and that service units expose only the needed entries with `LoadCredentialEncrypted=`.
- Confirm the runtime process can read/search/write the `DB_PATH` data root before schema initialization. `prod_preflight.py` validates this before touching Postgres so missing `CREDENTIALS_DIRECTORY` or bad `/var/lib/trading` permissions fail with provisioning errors instead of late schema errors.
- Confirm disk retention defaults before first bring-up. Compose caps Docker stdout/stderr with `DOCKER_LOG_DRIVER=local`, `DOCKER_LOG_MAX_SIZE=50m`, and `DOCKER_LOG_MAX_FILE=5`; file logs rotate daily or at `maxsize 50M`, keep 10 compressed rotations with `maxage 21`, and cover `/app/logs`, `/opt/trading-system/logs`, `/opt/trading/app/logs`, ingestion stdout/stderr logs, boot stderr logs, the diagnostics-only operator-AI JSONL log, and the compose `trading-logs` volume.
- Keep disk pressure thresholds explicit on production hosts: warning defaults are `DISK_PRESSURE_WARN_FREE_PCT=15` or `DISK_PRESSURE_WARN_FREE_BYTES=21474836480`; critical defaults are `DISK_PRESSURE_CRITICAL_FREE_PCT=5` or `DISK_PRESSURE_CRITICAL_FREE_BYTES=5368709120`. Critical disk pressure fails production preflight and startup validation.

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
- Run `python tools/validate_repo.py` before merge for the full deterministic validation set. Its default startup graph check isolates itself from local Postgres/Timescale and Redis settings; use live validation when those services must be exercised.
- Run `python engine/runtime/prod_preflight.py --json` when you want the explicit production preflight and smoke-cycle result.
- Treat `disk pressure critical` preflight errors as hard blockers. Warning-level disk pressure should trigger cleanup before ingestion, Postgres writes, backup evidence, or operator diagnostics lose write headroom.
- Treat `prod_preflight.py` provisioning errors as hard blockers. Missing systemd credentials, missing required Postgres role password credentials, or an unreadable/unwritable runtime data root must be fixed in the host/unit layer before retrying.
- Treat an API-auth preflight failure as a hard production blocker. Production/live mode must have a non-placeholder `DASHBOARD_API_TOKEN`; localhost-only fallback is dev-only.
- Treat a data-source master-key preflight failure as a hard production blocker. `engine/runtime/config_schema.py` validates the master key before go-live, and `services/credential_encryption.py` repeats the check before encrypting provider or broker credentials.
- Treat an operator-sidecar preflight failure as a hard production blocker. Production/live mode must have a non-placeholder `OPERATOR_API_TOKEN`, must not publish the sidecar port by default, and must reject unauthenticated sensitive sidecar GETs such as `/api/operator/config`.
- Treat resource-isolation preflight warnings as production blockers. A warning means one of the compose services is effectively unbounded, host headroom cannot be verified, `/dev/shm` is too small for runtime model/data-loader work, or Postgres/Redis/runtime worker/thread settings no longer fit the configured service limits.
- When external dependencies are enabled, set `PREFLIGHT_REQUIRE_TIMESCALE=1`, `PREFLIGHT_REQUIRE_REDIS=1`, and/or `PREFLIGHT_REQUIRE_OBJECT_STORAGE=1` so production preflight fails closed on missing or unreachable dependency endpoints.
- For systemd hosts, keep dependency URLs passwordless and use credential-name env vars such as `LIVE_CACHE_REDIS_PASSWORD_SECRET=redis_password`, `OBJECT_STORE_SECRET_KEY_SECRET=object_store_secret_key`, and `DASHBOARD_API_TOKEN_SECRET=dashboard_api_token`; the unit must load those names with `LoadCredentialEncrypted=`.

## 4. Bring-Up Checks

- Start the Python runtime through `start_system.py` or the deployment wrapper that invokes it.
- Start the operator sidecar through `boot/operator_server.js` or the deployment wrapper that invokes it.
- If you are using the compose deployment path, bring the stack up with both compose files and treat the operator container as an internal proxy sidecar, not the lifecycle owner of the runtime. Access operator APIs through the dashboard bridge (`/operator/api/...`) unless you are inside the compose network with `X-Operator-Token`.
- Confirm the dependency endpoints referenced by `TIMESCALE_DSN`, `TIMESCALE_PRICES_DSN`, `LIVE_CACHE_REDIS_URL`, and `OBJECT_STORE_ENDPOINT` are reachable from the runtime host before allowing the runtime to leave safe mode.
- Confirm the Postgres endpoint referenced by `TS_PG_DSN` or platform defaults is reachable, and that `python engine/runtime/prod_preflight.py --json` reports the Postgres contract as healthy.
- Treat Postgres contract failures as hard blockers. Preflight validates `schema_migrations`, required tables/columns/indexes, primary keys, owned live-ingestion table shape, and catalog type drift before smoke jobs run.
- Confirm `GET /api/readiness` returns a coherent readiness payload.
- Confirm `GET /api/operator/readiness_evidence` returns normalized evidence items and that any live/paper blocker includes source ownership, freshness, detail, and remediation. Treat `status=blocked` or critical `unavailable` evidence as a hard blocker; stale or missing critical evidence must not be accepted as passing.
- Confirm `GET /api/execution/barrier` reflects the expected safe-mode block before any live enablement.
- Confirm `GET /api/operator/provider_telemetry` shows fresh provider activity for the sources that should be running.
- Confirm `GET /api/operator/service_status` and `GET /api/operator/support_snapshot` through the dashboard return redacted diagnostics and do not show unresolved startup failures. Sensitive dashboard GETs such as `/api/system/config`, `/api/operator/logs`, `/api/operator/support_snapshot`, and `/api/terminal/positions` require `X-API-Token` in production/live or remote-bind deployments; query-string `token` auth is rejected in production/live.

## 5. Data-Source And Secret Checks

- Use `ui/data_sources.html` as the source-of-truth setup surface for provider credentials and source-specific settings.
- Confirm `DATA_SOURCE_MASTER_KEY_FILE` points to a `0600` file containing `openssl rand -base64 32` output before storing provider or broker credentials through the operator UI.
- Data-source CRUD, terminal order-entry, operator control, job-control, repair/governance mutation routes, sensitive dashboard GET routes, and bridged `/operator/api/*` sidecar mutations are protected in the transport before handler execution or proxying. Protected routes pass through dashboard auth, rate limiting, and append-only audit event logging. Protected bridged operator sidecar reads require dashboard auth before the bridge forwards the server-side sidecar token.
- Use `/api/broker/config`, `/api/broker/test_connection`, and `/api/broker/audit` as the broker configuration control plane. Do not activate a non-`sim` broker until the connection test passes freshly for the same broker and the audit rows show the expected operator action. `BROKER_CONNECTION_TEST_MAX_AGE_S` controls the backend activation freshness window and defaults to 24 hours.
- For first container bring-up, use compose `.env` only as a bootstrap contract for runtime provider variables: `POLYGON_API_KEY`, `POLYGON_REST_ENABLED`, `POLYGON_WS_ENABLED`, `TRADIER_API_TOKEN`, `TRADIER_ENABLED`, `OPTIONS_PROVIDER_CHAIN`, `OPTIONS_CRITICAL_SYMBOLS`, `ALPACA_BASE_URL`, `ALPACA_KEY_ID`, and `ALPACA_SECRET_KEY`.
- Do not enable live provider flags until `python engine/runtime/prod_preflight.py --json` passes dependency readiness and the operator readiness endpoints are reachable.
- Use `POST /api/data_sources/test` through the UI or API before enabling a newly configured source.
- Do not treat `.env` as the long-lived source of truth for provider credentials once the data-source manager is initialized.

## 6. Before Enabling Real Trading

- Confirm the runtime is not in `BOOTING`, `WARMING_UP`, `SHUTDOWN`, `KILL_SWITCH`, or unknown lifecycle state.
- Confirm `DISABLE_LIVE_EXECUTION` is explicitly false (`0`, `false`, `no`, or `off`) only as part of the approved live-enablement process. Unset, truthy, and unknown non-empty values are hard live-capital blocks in the runtime gate, kill-switch cascade, broker router, broker adapters, and terminal order-entry APIs.
- Confirm `ENGINE_MODE=live` and `EXECUTION_MODE=live`; environment mode alone does not arm execution.
- Confirm `LIVE_TRADING_CONFIRM=I_UNDERSTAND_LIVE_TRADING`. The expected phrase is fixed in code; `LIVE_TRADING_CONFIRM_PHRASE` overrides and `LIVE_TRADING_REQUIRE_CONFIRMATION=0` are rejected in live mode.
- Confirm `LIVE_BROKER`, `BROKER`, `BROKER_NAME`, and every `BROKER_FAILOVER` entry identify the same intended live broker and do not include `sim`, `paper`, `sandbox`, or mixed live brokers.
- Confirm live AI safety is clean before live capital is armed. `DECISION_ENGINE_ENABLED=1`, `DECISION_MIN_CONFIDENCE`, `DECISION_MIN_ABS_PREDICTION`, `UNCERTAINTY_SIZING_PRODUCTION_POLICY`, `UNCERTAINTY_HIGH_THRESHOLD`, `UNCERTAINTY_HARD_THRESHOLD`, `UNCERTAINTY_MAX_AGE_MS`, `OOD_SUPPRESS_THRESHOLD`, and `OOD_HARD_THRESHOLD` must be explicit. Missing or disabled values fail `live_trading_preflight()` and suppress live risk-increasing orders through the execution policy.
- Confirm the live model resolves without fallback for `LIVE_AI_PREFLIGHT_SYMBOLS` and `LIVE_AI_PREFLIGHT_HORIZONS_S`, and every resolved live model has a readable artifact alias, SHA, or path. Silent fallback from the requested model to another model, missing artifacts, online-model dummy zero predictions, `RL_ALLOW_FALLBACK_AGENT=1`, or RL/LLM/advisory source metadata on live orders are hard blockers.
- Keep `OPTIONS_INSTRUMENTS_MODE=shadow` for production. Options chain ingestion and options-derived features are not live options execution support. Live options orders are blocked in production code by runtime config validation, `live_trading_preflight().options_instruments`, the broker router, and direct Alpaca/IBKR adapter checks until a reviewed live options adapter exists and the Greeks, liquidity, bid/ask, assignment/exercise, expiration-risk, margin-impact, broker-support, position-limit, and kill-switch controls are implemented and enabled.
- Confirm uncertainty-driven sizing is explicit before live capital is armed. `UNCERTAINTY_SIZING_PRODUCTION_POLICY` must be set to `log_only`, `shrink`, or `strict`; when it is missing, live AI safety blocks risk-increasing live orders before broker routing. Use `shrink` or `strict` for production enforcement, and reserve `log_only` for an explicitly accepted research/observation period.
- Confirm hierarchical alpha shrinkage is active for live portfolio construction. Live/prod mode forces shrinkage on even if `ALPHA_SHRINKAGE_ENABLED=0`; production templates still set it to `1` for clarity. `alpha_shrinkage` diagnostics in portfolio order reasons and `runtime_meta.last_alpha_shrinkage` must show thin-history symbols being size-reduced or conservatively pooled to neutral when no prior exists.
- Confirm `CONFORMAL_MODE`, `OOD_MODE`, and `UNCERTAINTY_SIZING_MODE` match the intended rollout. `CONFORMAL_MODE=gate_and_size` applies interval-crosses-zero suppression and wide-interval size reduction; `OOD_MODE=suppress` applies OOD compression/blocking; `UNCERTAINTY_SIZING_MODE=enforce` applies model-intent and epistemic uncertainty sizing outside live mode.
- Confirm `GET /api/broker/config` shows the intended active broker, masked credentials, and the expected last passing test result; confirm `GET /api/broker/audit` contains the recent configuration and test actions.
- Confirm the Readiness Evidence dashboard card has no `BLOCKED` or critical `UNAVAILABLE` rows before live/paper operation. The card groups blockers by owning subsystem and links to the owning dashboard screen; broker activation reads this same evidence path before posting the activation mutation.
- Confirm the initial deployment hold has `KILL_SWITCH_GLOBAL=1` until operator signoff. Clearing that hold is not sufficient to trade; live execution still requires the audited DB `execution_mode` row to be `mode=live, armed=1`. `live_trading_preflight()` recomputes the canonical `execution_mode_audit` hash chain and rejects missing latest arming rows, missing previous hashes, row-hash mismatches, actor/reason/mode/armed tampering, and timestamp order breaks.
- Confirm kill-switch cache freshness is visible before live enablement. `KILL_SWITCH_CACHE_TTL_S` defaults to a bounded 30 seconds and is clamped to at most 300 seconds; stale Redis snapshots fail closed as `provider_unavailable` if storage cannot be rechecked. Keep the `kill_switch_cache_refresh` daemon running or run it manually after deployment so `/api/system/kill_switches`, `/api/health`, and `/api/execution/barrier` show `loaded_ts_ms`, `source`, `max_age_ms`, `cache_age_ms`, and `cache_status`.
- Confirm capital equity freshness before live enablement. `snapshot_equity` is expected to keep `equity_history` current; live capital gating fails closed when the table is missing, the query fails, the latest point is older than `KILL_SWITCH_MAX_EQUITY_AGE_S`/`DRAWDOWN_MAX_EQUITY_AGE_S`, or required daily, rolling, or VaR windows have fewer than `KILL_SWITCH_DAILY_EQUITY_MIN_POINTS`, `KILL_SWITCH_ROLLING_EQUITY_MIN_POINTS`, or `KILL_SWITCH_VAR_EQUITY_MIN_POINTS`. `/api/operator/preflight_report`, `run_preflight()`, and `prod_preflight.py --json` surface `capital_equity_freshness` with per-window reason codes.
- Confirm `RULES_AUTO_RESUME` is unset or `0` unless explicitly accepted for this deployment. When enabled, rules auto-resume only clears rules-owned rows (`actor=rules_engine` plus matching `meta_json.trigger` such as `drawdown`, `drift`, `exec_winrate`, or `cost_spike`). Manual/operator/emergency/startup/preflight/break-glass holds must be cleared through `POST /api/operator/clear_manual_halt` with `CLEAR_MANUAL_HALT` confirmation and an audit reason.
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
- Before live promotion, run `ops/backup/backup_restore_evidence.sh` on the target server. Keep the timestamped report and `latest_backup_restore_evidence.json`; live preflight and model promotion guard fail closed when the JSON artifact is missing/invalid, when verified backup, WAL archive evidence, restore drill freshness, restore duration, signature freshness, or signature verification violates the configured `BACKUP_EVIDENCE_*` policy. Live and production-required evidence paths require a valid HMAC-SHA256 signature even if an old env file still sets `BACKUP_EVIDENCE_REQUIRE_SIGNATURE=0`.
- Inspect backup accounting before deleting anything under Docker or `/var/backups/trading`:

```bash
sudo /opt/trading/app/ops/backup/accounting.sh
docker system df
docker builder du
```

The accounting script reports host path, container path, Docker mount source when available, apparent and allocated backup bytes, subdirectory sizes, base/WAL inventory, and the active `TS_BACKUP_KEEP_DAILY_DAYS`, `TS_BACKUP_KEEP_WEEKLY_DAYS`, and `TS_BACKUP_WAL_CUSHION_DAYS` policy.
- Store the backup evidence signing key outside the backup evidence directory. The installer creates `/etc/trading/backup_evidence.hmac.key` when missing; expected permissions are `root:trading 0640` with `/etc/trading` at `0750`. Manual creation:

```bash
sudo groupadd --system trading 2>/dev/null || true
sudo install -d -o root -g trading -m 0750 /etc/trading
openssl rand -hex 32 | sudo tee /etc/trading/backup_evidence.hmac.key >/dev/null
sudo chown root:trading /etc/trading/backup_evidence.hmac.key
sudo chmod 0640 /etc/trading/backup_evidence.hmac.key
```

- Verify signed backup evidence after installation or rotation:

```bash
sudo -u postgres env \
  BACKUP_EVIDENCE_REQUIRE_SIGNATURE=1 \
  BACKUP_EVIDENCE_HMAC_KEY_FILE=/etc/trading/backup_evidence.hmac.key \
  /opt/trading/ops/backup/backup_restore_evidence.sh

sudo -u trading env \
  ENGINE_MODE=live \
  PREFLIGHT_REQUIRE_BACKUP_EVIDENCE=1 \
  BACKUP_EVIDENCE_REQUIRE_SIGNATURE=1 \
  BACKUP_EVIDENCE_HMAC_KEY_FILE=/etc/trading/backup_evidence.hmac.key \
  /opt/trading/venv/bin/python /opt/trading/app/engine/runtime/prod_preflight.py --json
```

- Rotate the backup evidence key by writing a new protected key file, updating `BACKUP_EVIDENCE_HMAC_KEY_FILE`, restarting the runtime/preflight service or remounting the compose secret, generating fresh evidence with the new key, and rerunning preflight. Keep the previous key until fresh signed evidence and preflight verification both pass.
- For disk cleanup, prune only rebuildable Docker cache/images unless a human has verified the affected volumes are not live state:

```bash
docker builder prune --filter until=168h
docker image prune -a --filter until=168h
docker container prune --filter until=168h
```

Do not use `docker volume prune`, `docker system prune --volumes`, or manual removal under `/var/lib/docker/volumes/*timescaledb*`, `*redis*`, `*minio*`, app data volumes, or `/var/backups/trading` on a live host. Use `ops/backup/prune.sh` for backup retention cleanup so WAL and base-backup evidence remain consistent.
- Before live promotion, confirm the challenger has realized rows in `net_after_cost_labels` and that promotion reports show positive `net_cost_label_count`. Gross-only edge is not promotion evidence; promotion and competition gates fail closed without net-after-cost labels.
- Before live or paper promotion, confirm the challenger has at least `CHAMPION_PROMOTION_MIN_OBSERVATIONS` aligned realized return observations. This observation floor is enforced directly by `champion_manager` in paper/live/production-like runtimes and does not depend on `CHAMPION_PROMOTION_USE_STAT_GATE` or `CPCV_ENABLED`; those legacy/statistical controls are additional governance only.
- Before allowing any RL, bandit, sizing-policy, or execution-policy challenger beyond shadow, confirm `policy_ope_evidence` has a fresh passing doubly robust OPE row for the exact candidate. Missing propensities, insufficient support/effective sample size, optimistic model-only estimates, wide confidence intervals, or lower confidence bounds below `PROMOTION_OPE_MIN_POLICY_VALUE_LOWER_BOUND` block promotion in champion competition, direct registry promotion, direct strategy-governance live promotion, live learned-execution application, and live size-policy consumption.
- Shadow strategy outperformance must not directly flip `strategy_registry.stage` to `live`. Portfolio rebalancing only records `strategy_promotion_candidates`; `strategy_governance_job` can promote that candidate only with operator approval, positive realized PnL, passing `promotion_statistical_evidence`, fresh approved replay validation, passing `policy_ope_evidence`, system promotion-guard/cooldown approval, and `model_promotion_audit` promote records.
- Keep LOB deep-learning paths shadow-only. `EXEC_LOB_DEEPLOB_SHADOW_ENABLED=1` may log DeepLOB-style execution-timing or adverse-selection diagnostics only after `market_microstructure_signals` has sufficient fresh L2/top-of-book depth, latency assumptions are bounded, and recent broker-sim fills contain applied `lob_simulation` calibration evidence. Missing L2 rows, stale depth, absent latency assumptions, or insufficient simulator calibration block the shadow model path in production code through `lob_deeplob_shadow_readiness_snapshot`, `live_trading_preflight().lob_deeplob_shadow`, and `prod_preflight.py`.
- For PatchTST challengers, run `pretrain_patchtst_models` before `train_patchtst_models` when using masked self-supervised initialization. The final fine-tuned model must remain `stage=shadow`, carry `pretraining.artifact_alias`/`artifact_sha256`, and load successfully through the normal PatchTST artifact path; load-time schema checks fail closed on pretraining feature or sequence drift.
- For iTransformer challengers, run `train_itransformer_models` only as a shadow-default training job. It persists content-addressed artifacts, feature contracts, OOS rows in `model_oos_predictions`, and a `model_marketplace_scores` shadow row with `score_source=model_oos_predictions`; the champion manager can see that row but cannot promote it because live promotion still requires realized PnL/net-cost evidence, replay approval, statistical evidence, and registry promotion. The predictor's iTransformer adapter also refuses to load artifacts unless the resolved registry spec is `source_stage=champion`.
- Keep graph/relational learning shadow-only. `USE_GRAPH_RELATIONAL_FEATURES=1` may materialize `graph.relational_v1.*` snapshots for research, but live model serving rejects those feature ids and both direct registry promotion and champion competition block graph candidates when graph metadata, PIT safety, train/serve parity, or snapshot availability is missing. Fully valid graph metadata is still non-promotable until a reviewed live graph gate replaces the shadow-only blocker.
- Before promoting any generated candidate from LLM alpha discovery, symbolic alpha search, tsfresh/search feature discovery, Optuna, or alpha-discovery challengers, confirm `experiment_ledger` has a passing append-only row for the exact candidate/version. The row must include feature lineage, prompt/model hash when applicable, search space, configured trial budget, observed trial count, CPCV/PBO/DSR/FDR evidence, redundancy checks, and a passing promotion decision. LLM factor mining uses `LLM_FACTOR_CANDIDATES` as a strict total trial budget across propose, DSL validation, evaluation, critique, and revision prompts; parse rejections, evaluation rejections, prompt hashes, model name, lineage, and the loop final decision are ledgered. Accepted LLM factors register as `stage=shadow`, and direct live registration fails unless accepted statistical-gate evidence and ledger evidence are already present. `PROMOTION_EXPERIMENT_LEDGER_REQUIRED=1` is the production default; disabling it is a research-only break-glass action.
- Keep `compute_drift` running in supervised production. It now refreshes `production_monitoring_metrics` for feature drift, prediction drift, missing feature rates, target/label drift after labels mature, calibration ECE, conformal coverage, shadow-vs-live disagreement, and net-PnL degradation. Threshold breaches write `drift_retrain_events` rows with `action_taken` of `retrain_signal` or `shadow_review_signal`; they do not promote or live-ready a model.
- On the Compose production server, install this gate with `sudo bash ops/server/install_backup_evidence_gate.sh --compose --restart-postgres --run-evidence`. This applies the TimescaleDB WAL archive bind mount/settings, installs the 60-second evidence refresh timer, and runs version-matched backup/restore tools from the Timescale image.
- Keep the configured runtime log directory available for runtime and operator log tails. Local defaults use `var/log/`; deployment overrides such as `/app/logs` or `/var/lib/trading/logs` still take precedence.
- Use `/api/operator/runtime_watchdogs`, `/api/operator/provider_telemetry`, and `/api/operator/support_snapshot` through the dashboard as the first-line operational checks. Direct sidecar access requires `X-Operator-Token` and must not expose raw `.env`, dashboard tokens, DB credentials, provider keys, broker keys, or master keys.
- Run `python tools/validate_repo.py --live` only against an intentionally running stack when a live smoke test is required. Live validation preserves production dependency requirements for Postgres/Timescale and Redis and should fail if required services are unavailable.
- Re-run `python engine/runtime/prod_preflight.py --json` after dependency changes so external-service readiness and runtime smoke are captured together.
- For the compose deployment path, run `docker compose --env-file deploy/compose/.env -f deploy/compose/docker-compose.external-services.yml -f deploy/compose/docker-compose.stack.yml exec runtime python engine/runtime/prod_preflight.py --json` before calling the stack staging-ready.
