# Compose Stack

Use these compose assets when you want a containerized staging or production-like deployment path for the current repo architecture.

## Files

- `docker-compose.external-services.yml`
  Brings up Timescale/Postgres, Redis, and MinIO-style object storage.
- `docker-compose.stack.yml`
  Brings up the Python runtime and the Node operator sidecar on top of the external dependency network.
- `.env.example`
  Seed env file for both compose files.
- `Dockerfile.runtime`
  Runtime image for `start_system.py`.
- `Dockerfile.operator`
  Operator image for `boot/operator_server.js`.

## Bring Up

1. Copy `.env.example` to `.env` in this directory.
2. Set approved image tags, dependency credentials, provider credentials, dashboard/operator API tokens, `DASHBOARD_PUBLIC_PORT`, `DATA_SOURCE_MASTER_KEY_FILE`, and `BACKUP_EVIDENCE_HMAC_KEY_FILE`. Leave `TRADING_DATA_ROOT=/app/data` unless you are deliberately changing the container data mount; it must remain an absolute path. `DASHBOARD_API_TOKEN` and `OPERATOR_API_TOKEN` must be generated secrets, not placeholders. The operator sidecar is internal-only by default and does not publish port 4001. Keep `DOCKER_LOG_DRIVER=local`, `DOCKER_LOG_MAX_SIZE=50m`, and `DOCKER_LOG_MAX_FILE=5` unless the target host has a reviewed reason to change them; these cap Docker stdout/stderr while file logs under `/app/logs` are handled by host logrotate.
   Create the backup evidence HMAC key before `docker compose up` because the runtime mounts it as a Compose secret:

```bash
sudo groupadd --system trading 2>/dev/null || true
sudo install -d -o root -g trading -m 0750 /etc/trading
openssl rand -hex 32 | sudo tee /etc/trading/backup_evidence.hmac.key >/dev/null
sudo chown root:trading /etc/trading/backup_evidence.hmac.key
sudo chmod 0640 /etc/trading/backup_evidence.hmac.key
```
3. Create the data-source master-key file referenced by `DATA_SOURCE_MASTER_KEY_FILE`:

```bash
install -m 0600 /dev/null ../../data/.data_source_master_key
openssl rand -base64 32 > ../../data/.data_source_master_key
```

Production/live preflight rejects raw text, placeholders, short or low-entropy values, malformed base64, and empty key files.
4. Build and start the stack:

```bash
docker compose \
  --env-file deploy/compose/.env \
  -f deploy/compose/docker-compose.external-services.yml \
  -f deploy/compose/docker-compose.stack.yml \
  up -d --build
```

The external TimescaleDB service archives WAL to
`${TRADING_BACKUP_WAL_DIR:-/var/backups/trading/wal}`. On a production host,
install the backup evidence gate after the stack env is populated:

```bash
sudo bash ops/server/install_backup_evidence_gate.sh --compose --restart-postgres --run-evidence
```

That command creates the host backup layout, installs the backup and evidence
systemd timers, restarts/recreates the TimescaleDB service to apply the WAL
archive bind mount, and writes the first timestamped backup/WAL/restore
evidence report. It reuses the HMAC key at `BACKUP_EVIDENCE_HMAC_KEY_FILE`,
sets `BACKUP_EVIDENCE_REQUIRE_SIGNATURE=1`, and the runtime verifies
`latest_backup_restore_evidence.json` with `/run/secrets/backup_evidence_hmac_key`.

5. Run the production preflight against the runtime container:

```bash
docker compose \
  --env-file deploy/compose/.env \
  -f deploy/compose/docker-compose.external-services.yml \
  -f deploy/compose/docker-compose.stack.yml \
  exec runtime python engine/runtime/prod_preflight.py --json
```

To rotate the evidence key, create a replacement file with the same owner/mode,
update `BACKUP_EVIDENCE_HMAC_KEY_FILE` in `deploy/compose/.env`, restart the
runtime so the secret is remounted, then run
`sudo bash ops/server/install_backup_evidence_gate.sh --compose --run-evidence`
and rerun the production preflight. Do not remove the old key until a new signed
evidence JSON has been generated and preflight passes.

6. Run the live smoke against the exposed stack from the repo root:

```bash
python tools/validate_repo.py --live
```

For a local compose stack, live smoke reaches the operator through the dashboard bridge. Export the same auth and base URLs before running live smoke:

```bash
export PIPELINE_SMOKE_BASE="http://127.0.0.1:${DASHBOARD_PUBLIC_PORT:-8000}"
export PIPELINE_SMOKE_OPERATOR_BASE="${PIPELINE_SMOKE_BASE}/operator"
export DASHBOARD_API_TOKEN="$DASHBOARD_API_TOKEN"
export OPERATOR_API_TOKEN="$OPERATOR_API_TOKEN"
python tools/validate_repo.py --live
```

The optional soak probes (`tools/safe_mode_soak.py`, `tools/runtime_stability_probe.py`, and `tools/market_session_soak.py`) use the same `PIPELINE_SMOKE_BASE`, `PIPELINE_SMOKE_OPERATOR_BASE`, and token variables by default, so compose checks continue through the dashboard bridge unless you explicitly pass a direct sidecar URL for a local diagnostic.

## Resource Isolation

The compose files bound every production service with Docker CPU and memory
limits. The values are configurable through `deploy/compose/.env`; keep
`PREFLIGHT_CHECK_RESOURCE_LIMITS=1` so `prod_preflight.py` reports unbounded
services or inconsistent memory/thread settings before the stack is considered
production-ready.

Recommended starting values for a 16-core / 32-thread / 123 GiB host:

| Service | CPU limit | Memory limit | Other bound |
| --- | ---: | ---: | --- |
| runtime | `RUNTIME_CPUS=12` | `RUNTIME_MEM_LIMIT=48g` | `RUNTIME_SHM_SIZE=8g` |
| Timescale | `TIMESCALE_CPUS=8` | `TIMESCALE_MEM_LIMIT=32g` | `TIMESCALE_SHM_SIZE=2g` |
| Redis | `REDIS_CPUS=2` | `REDIS_MEM_LIMIT=8g` | `REDIS_MAXMEMORY=6gb` |
| MinIO | `MINIO_CPUS=2` | `MINIO_MEM_LIMIT=6g` | object store process limit |
| operator | `OPERATOR_CPUS=1` | `OPERATOR_MEM_LIMIT=2g` | internal sidecar only |

Those defaults cap containers at 25 logical CPUs and 96 GiB RAM, leaving about
7 logical CPUs and 27 GiB RAM for the OS, Docker, diagnostics, tests, IDEs, and
emergency shell work. The preflight minimums are
`TRADING_RESOURCE_MIN_HEADROOM_CPUS=6` and
`TRADING_RESOURCE_MIN_HEADROOM_MEMORY=24g`.

The Timescale container is also the source of truth for Postgres tuning. Keep
`PREFLIGHT_REQUIRE_DOCKER_POSTGRES_TUNING=1`; production preflight reads the
same `TIMESCALE_*` values passed to `postgres -c`, validates them against
`TIMESCALE_MEM_LIMIT`, `TRADING_RESOURCE_HOST_MEMORY`, and
`TRADING_RESOURCE_MIN_HEADROOM_MEMORY`, and compares reachable `pg_settings`
values to catch drift after the container starts.

Recommended Timescale/Postgres settings for this 123 GiB host profile:

| Setting family | Values |
| --- | --- |
| Memory | `TIMESCALE_MEM_LIMIT=32g`, `TIMESCALE_SHARED_BUFFERS=8GB`, `TIMESCALE_EFFECTIVE_CACHE_SIZE=22GB`, `TIMESCALE_WORK_MEM=48MB`, `TIMESCALE_MAINTENANCE_WORK_MEM=2GB`, `TIMESCALE_AUTOVACUUM_WORK_MEM=512MB`, `TIMESCALE_MAX_CONNECTIONS=100` |
| Parallelism | `TIMESCALE_MAX_WORKER_PROCESSES=16`, `TIMESCALE_MAX_PARALLEL_WORKERS=8`, `TIMESCALE_MAX_PARALLEL_WORKERS_PER_GATHER=4`, `TIMESCALE_MAX_PARALLEL_MAINTENANCE_WORKERS=4`, `TIMESCALE_TIMESCALEDB_MAX_BACKGROUND_WORKERS=8` |
| Autovacuum | `TIMESCALE_AUTOVACUUM=on`, `TIMESCALE_AUTOVACUUM_MAX_WORKERS=4`, `TIMESCALE_AUTOVACUUM_NAPTIME=10s`, `TIMESCALE_AUTOVACUUM_VACUUM_COST_LIMIT=4000`, `TIMESCALE_AUTOVACUUM_VACUUM_COST_DELAY=2ms` |
| WAL/checkpoint | `TIMESCALE_WAL_BUFFERS=64MB`, `TIMESCALE_MIN_WAL_SIZE=4GB`, `TIMESCALE_MAX_WAL_SIZE=16GB`, `TIMESCALE_WAL_KEEP_SIZE=1GB`, `TIMESCALE_MAX_SLOT_WAL_KEEP_SIZE=8GB`, `TIMESCALE_WAL_DISK_BUDGET=40g`, `TIMESCALE_CHECKPOINT_TIMEOUT=15min`, `TIMESCALE_CHECKPOINT_COMPLETION_TARGET=0.9`, `TIMESCALE_ARCHIVE_TIMEOUT=60s` |
| IO cost | `TIMESCALE_RANDOM_PAGE_COST=1.1`, `TIMESCALE_EFFECTIVE_IO_CONCURRENCY=200`, `TIMESCALE_MAINTENANCE_IO_CONCURRENCY=200` |

That profile estimates about 25 GiB of bounded Postgres memory pressure inside
the 32 GiB service limit and caps configured retained WAL at 25 GiB under the
40 GiB WAL budget. The WAL budget is not a substitute for backup evidence:
archive failures can still force `pg_wal` growth, so keep
`PREFLIGHT_REQUIRE_BACKUP_EVIDENCE=1` and the WAL RPO checks enabled.

Smaller fallback hosts should lower the Timescale service limit first, then
scale Postgres settings with that limit:

| Host profile | Timescale limit | Recommended DB settings |
| --- | ---: | --- |
| 32 GiB host | `TIMESCALE_MEM_LIMIT=12g` | `shared_buffers=3GB`, `effective_cache_size=9GB`, `work_mem=8MB`, `maintenance_work_mem=512MB`, `autovacuum_work_mem=256MB`, `max_wal_size=8GB`, `wal_disk_budget=24g`, `TIMESCALE_CPUS=4` |
| 64 GiB host | `TIMESCALE_MEM_LIMIT=20g` | `shared_buffers=5GB`, `effective_cache_size=15GB`, `work_mem=16MB`, `maintenance_work_mem=1GB`, `autovacuum_work_mem=384MB`, `max_wal_size=8GB`, `wal_disk_budget=32g`, `TIMESCALE_CPUS=6` |
| 123 GiB host | `TIMESCALE_MEM_LIMIT=32g` | use the committed `.env.example` values above |

Keep Redis `REDIS_MAXMEMORY` below the Redis container limit, and keep runtime
worker/thread caps (`RESOURCE_SCHEDULER_*`, `MODEL_TRAIN_*`,
`LGBM_*_N_JOBS`, `XGB_N_JOBS`, `META_LABEL_N_JOBS`, `TSFRESH_*_N_JOBS`,
`TSFRESH_SNAPSHOT_*`, `TUNE_N_TRIALS`, `TUNE_MAX_N_TRIALS`,
`RUNTIME_OMP_NUM_THREADS`, `RUNTIME_MKL_NUM_THREADS`,
`RUNTIME_OPENBLAS_NUM_THREADS`, `RUNTIME_NUMEXPR_NUM_THREADS`,
`TORCH_CPU_THREADS`, and `TORCH_INTEROP_THREADS`) at or below
`RUNTIME_CPUS`.

The committed `.env.example` also enables the bounded ingestion host profile:
`INGESTION_TUNING_PROFILE=host_32t_123g`, parent Timescale/price pools of 8,
async price batches of 512 with a 1024-envelope queue, and child feed-process
pools capped at PG 3 / Timescale 4 / price-storage 4. `prod_preflight.py` and
`start_ingestion.py` reject out-of-bound overrides, while `/api/health` exposes
writer queue depth, dropped rows, retry counts, flush latency, and DB write
duration so operators can distinguish real throughput from backlog growth.

## Offline Training Profile

The default `runtime` service runs with `RUNTIME_WORKLOAD_PROFILE=live`,
`ALLOW_TRAINING=0`, serial model-family workers, TSFresh multiprocessing off
(`TSFRESH_N_JOBS=0`), low TSFresh symbol/batch caps
(`TSFRESH_SNAPSHOT_SYMBOL_LIMIT=100`, `TSFRESH_SNAPSHOT_BATCH_SIZE=25`),
low tuning trial caps (`TUNE_N_TRIALS=10`), and low scheduler concurrency.
Production preflight and job launch both reject offline/training jobs in that profile unless the
operator sets `OFFLINE_TRAINING_LIVE_PROFILE_ACK` to
`I_UNDERSTAND_OFFLINE_TRAINING_IN_LIVE_PROFILE` with a non-placeholder owner
and reason. That acknowledgement is for exceptional maintenance only, not the
normal training path.

Use the `offline-worker` compose profile for research, backtests, feature
discovery, TSFresh snapshot materialization, and model training. It uses
`RUNTIME_WORKLOAD_PROFILE=offline`, `ALLOW_TRAINING=1`, higher CPU/memory
limits, larger bounded worker counts, larger TSFresh symbol/batch caps, and a
larger tuning trial budget. It also requires `OFFLINE_TS_PG_DSN` so offline
work targets a restored clone or otherwise isolated datastore instead of
silently sharing the live Timescale service.

Example offline run from the repo root:

```bash
export OFFLINE_TS_PG_DSN="host=offline-timescale port=5432 user=trading dbname=trading_offline password=..."
docker compose \
  --env-file deploy/compose/.env \
  -f deploy/compose/docker-compose.stack.yml \
  --profile offline \
  run --rm offline-worker python -m engine.strategy.jobs.pipeline_train_and_eval
```

Keep the live `runtime`, Redis, and Timescale resource limits unchanged while
offline jobs run. If the offline datastore is a clone on the same host, reserve
host headroom first and lower `OFFLINE_CPUS`/`OFFLINE_MEM_LIMIT` rather than
borrowing from the live services.

Run offline jobs outside market hours or during an approved research window.
Before starting, verify the live service remains bounded:

```bash
python -m engine.runtime.prod_preflight --json
docker compose --env-file deploy/compose/.env -f deploy/compose/docker-compose.stack.yml ps
docker stats --no-stream runtime timescaledb redis offline-worker
```

Expected signals: production preflight reports `workload_profile=live` and
`allow_training=0` for the runtime service; `docker stats` shows live runtime,
Timescale, and Redis inside their configured CPU/memory limits; offline jobs
show `RUNTIME_WORKLOAD_PROFILE=offline` in their environment and use the
`OFFLINE_*` resource settings.

## Disk Retention And Backup Accounting

The compose stack has two retention layers. Docker service stdout/stderr is
bounded by `DOCKER_LOG_DRIVER`, `DOCKER_LOG_MAX_SIZE`, and
`DOCKER_LOG_MAX_FILE`. Runtime file logs written through the `trading-logs`
volume are rotated by `deploy/logrotate/trading-system`: daily, `maxsize 50M`,
10 compressed rotations, `maxage 21`, and `copytruncate`.

Inspect backup accounting and disk pressure before cleanup:

```bash
sudo /opt/trading/app/ops/backup/accounting.sh
docker system df
docker builder du
```

Production preflight includes `disk_pressure` and, when backup evidence is
fresh or non-required, backup `accounting` with host path, container mount view,
container mount source, apparent bytes, allocated bytes, subdirectory sizes, and
retention status/settings.

Safe Docker cleanup is limited to rebuildable cache, old images, and stopped
containers:

```bash
docker builder prune --filter until=168h
docker image prune -a --filter until=168h
docker container prune --filter until=168h
```

Do not run `docker volume prune`, `docker system prune --volumes`, or manual
deletion of Timescale, Redis, MinIO, app data, or `/var/backups/trading` state
on a live host. Use `ops/backup/prune.sh` for backup retention so base backups,
WAL, restore-drill evidence, and signed evidence remain consistent.

## Provider Credentials

The compose runtime passes provider and broker bootstrap settings from `deploy/compose/.env` into the Python runtime container. Leave providers disabled for the first dependency-only bring-up, then enable only the sources you have validated.

- Keep `PROD_LOCK=1`, `ALLOW_TRAINING=0`, `ENGINE_MODE=safe`, `EXECUTION_MODE=safe`, `DISABLE_LIVE_EXECUTION=1`, and the initial kill-switch hold for the first bring-up. Keep `LIVE_BROKER`, `BROKER`, `BROKER_NAME`, and `BROKER_FAILOVER` pinned to the same intended live broker (`ibkr` in the template).
- Keep `LIVE_TRADING_CONFIRM=0` in the committed example and during dependency-only bring-up. Set `LIVE_TRADING_CONFIRM=I_UNDERSTAND_LIVE_TRADING` only in the target host's operator-controlled compose `.env` when live execution is intentionally being enabled.
- Keep `TRADING_IMPORT_SMOKE_IMPORT_JOBS=0` for runtime startup. Startup compiles registered job files and imports only bootstrap-critical modules so heavy model/data imports cannot block dashboard binding. Use `TRADING_IMPORT_SMOKE_IMPORT_JOBS=1` only for explicit diagnostic runs.
- Keep `TRADING_DEPENDENCY_PROFILE=cpu`, `RUNTIME_HARDWARE_PROFILE=cpu`, `TORCH_DEVICE=cpu`, `EMBED_DEVICE=cpu`, `NLP_DEVICE=cpu`, `FINBERT_DEVICE=cpu`, and `TS_FOUNDATION_DEVICE=cpu` for this AMD Ryzen AI Max+ 395 host. PyTorch does not currently have a validated accelerator path here, so `auto` resolves to CPU unless both `TRADING_DEPENDENCY_PROFILE=nvidia-cuda` and an explicit NVIDIA runtime profile are configured and CUDA is verified available. NVIDIA telemetry (`pynvml`/`nvidia-smi`), pinned-memory prefetch, GPU throttling, TF32, and cuDNN benchmark flags are all off by default through explicit `=0` env values. AMD/ROCm remains blocked until host-specific ROCm requirements, device permissions, and PyTorch HIP support are validated. See [../../docs/DEPENDENCY_PROFILES.md](../../docs/DEPENDENCY_PROFILES.md) for profile selection and rollback commands.
- Polygon/Massive: create or retrieve an API key in the provider dashboard, confirm your plan includes the market-data entitlements you intend to use, then set `POLYGON_API_KEY`. Set `POLYGON_REST_ENABLED=1` and/or `POLYGON_WS_ENABLED=1` only when that source should run. Official setup: [Polygon quickstart](https://polygon.io/docs/rest/quickstart).
- Tradier: use the API Access page in your Tradier profile to get a sandbox or live token, then set `TRADIER_API_TOKEN` and `TRADIER_ENABLED=1`. Use a sandbox token for staging validation unless you are deliberately validating production data access. Official setup: [Tradier API getting started](https://support.tradier.com/kb/guide/en/getting-started-with-tradier-api-JYIaTkOdD1/Steps/4577201).
- Alpaca paper validation belongs in a non-live staging run. Set `ENGINE_MODE=paper`/`EXECUTION_MODE=paper` and `ALPACA_BASE_URL=https://paper-api.alpaca.markets` only for that paper validation; live mode rejects the paper endpoint. Official setup: [Alpaca paper trading](https://docs.alpaca.markets/docs/paper-trading).

Do not commit populated `.env` files. If provider variables are blank or disabled, infrastructure and preflight dependency checks can still run, but live smoke should be expected to fail provider freshness checks.

Dashboard mutation endpoints require `DASHBOARD_API_TOKEN` in production/live mode, including loopback binds and same-origin `/operator/api/*` operator-bridge mutations. The operator bridge also requires dashboard auth for protected sidecar reads before forwarding the server-side sidecar token. The runtime rejects missing, placeholder, or too-short dashboard tokens before serving mutations or proxying to the operator sidecar; the localhost no-token fallback is only for explicit safe local development and should not be enabled in compose production-like stacks.

## Operator Mode In Containers

The operator container runs with `OPERATOR_DISABLE_INTERNAL_ENGINE_START=1`.

That is intentional. In the compose deployment path, the operator is a UI/proxy sidecar and not the lifecycle owner of the Python runtime process. Runtime lifecycle belongs to the container orchestrator, not to a sibling process trying to spawn `start_system.py` across containers.

Operator sidecar endpoints require `OPERATOR_API_TOKEN` for protected GET, HEAD, POST, and WebSocket access. Only `/api/operator/ping` is intentionally unauthenticated as a liveness probe. Loopback alone is not authorization. In compose, the sidecar is reachable on the internal Docker network as `operator:4001`; the runtime dashboard bridge forwards `X-Operator-Token` from server-side configuration after dashboard auth passes. If you intentionally publish the sidecar for a controlled local diagnostic, pass `X-Operator-Token` on every request and do not carry that override into production.

## Production Threshold

Do not treat this compose stack alone as proof that the full target-state plan is complete.

The current production threshold for this repo remains:

- compose stack starts cleanly
- runtime preflight passes
- `python tools/validate_repo.py --live` passes against the running stack
- `docs/handoff/codex_migration/FULL_SCOPE_VALIDATION.md` no longer has blocking `Partial in repo` or `Not implemented in repo` rows for your intended deployment claim

## 2026-06-15 Server Bring-Up Mode

Chosen deployment mode: compose.

Use compose for this production-functional server bring-up because the repository already defines the external dependency services, the Python runtime, and the Node operator sidecar in one deployment contract. The compose path keeps runtime lifecycle ownership with Docker, leaves the operator as a proxy/control sidecar, and starts the first dependency-only bring-up in `ENGINE_MODE=safe`, `EXECUTION_MODE=safe`, `PROD_LOCK=1`, `ALLOW_TRAINING=0`, `MODEL_SCORING_ENABLED=0`, `LIVE_BROKER=ibkr`, `BROKER_NAME=ibkr`, `BROKER=ibkr`, `BROKER_FAILOVER=ibkr`, `AUTO_BOOT_DAEMONS=0`, and `START_INGESTION_WITH_SERVER=0`.

Ingestion is intentionally not auto-started in this safe bring-up. Provider credentials are disabled by default, and explicit ingestion startup should be validated separately before enabling provider polling.

For the next production-function validation step, keep the broker identity pinned to `ibkr`, `EXECUTION_MODE=safe`, `DISABLE_LIVE_EXECUTION=1`, and `AUTO_PIPELINE_INCLUDE_EXECUTION=0`, then enable `AUTO_BOOT_DAEMONS=1`, `START_INGESTION_WITH_SERVER=1`, `MODEL_SCORING_ENABLED=1`, and `AUTO_PIPELINE=1`. This proves the data/scoring health path without enabling live broker order routing.

The exact bring-up command is:

```bash
docker compose \
  --env-file deploy/compose/.env \
  -f deploy/compose/docker-compose.external-services.yml \
  -f deploy/compose/docker-compose.stack.yml \
  up -d --build
```

The exact preflight command is:

```bash
docker compose \
  --env-file deploy/compose/.env \
  -f deploy/compose/docker-compose.external-services.yml \
  -f deploy/compose/docker-compose.stack.yml \
  exec runtime python engine/runtime/prod_preflight.py --json
```

The exact endpoint checks are:

```bash
DASHBOARD_API_TOKEN="$(awk -F= '$1=="DASHBOARD_API_TOKEN"{print substr($0,index($0,"=")+1)}' deploy/compose/.env)"
OPERATOR_API_TOKEN="$(awk -F= '$1=="OPERATOR_API_TOKEN"{print substr($0,index($0,"=")+1)}' deploy/compose/.env)"

curl -fsS -H "X-API-Token: ${DASHBOARD_API_TOKEN}" http://127.0.0.1:8000/api/readiness
curl -fsS -H "X-API-Token: ${DASHBOARD_API_TOKEN}" http://127.0.0.1:8000/api/operator/support_snapshot?mode=quick
curl -fsS -H "X-API-Token: ${DASHBOARD_API_TOKEN}" http://127.0.0.1:8000/api/execution/barrier
curl -fsS -H "X-API-Token: ${DASHBOARD_API_TOKEN}" http://127.0.0.1:8000/api/operator/provider_telemetry
curl -fsS -H "X-API-Token: ${DASHBOARD_API_TOKEN}" http://127.0.0.1:8000/api/operator/service_status
curl -fsS -H "X-API-Token: ${DASHBOARD_API_TOKEN}" http://127.0.0.1:8000/operator/api/operator/status
curl -fsS -H "X-API-Token: ${DASHBOARD_API_TOKEN}" http://127.0.0.1:8000/operator/api/operator/readiness
```

Do not enable real trading during this bring-up.
