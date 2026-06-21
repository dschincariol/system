# Compose Stack

Use these compose assets when you want a containerized staging or production-like deployment path for the current repo architecture.

## Files

- `docker-compose.external-services.yml`
  Brings up Timescale/Postgres, Redis, and MinIO-style object storage.
- `docker-compose.stack.yml`
  Brings up the Python runtime and the Node operator sidecar on top of the external dependency network.
- `docker-compose.amd-rocm.yml`
  Optional runtime-only ROCm overlay for AMD Strix Halo / `gfx1151` hosts.
- `.env.example`
  Seed env file for both compose files.
- `Dockerfile.runtime`
  Runtime image for `start_system.py`.
- `Dockerfile.operator`
  Operator image for `boot/operator_server.js`.

## Bring Up

1. Copy `.env.example` to `.env` in this directory.
2. Set approved image tags, dependency credentials, provider credentials, dashboard/operator API tokens, public ports, and `DATA_SOURCE_MASTER_KEY_FILE`. `DASHBOARD_API_TOKEN` and `OPERATOR_API_TOKEN` must be generated secrets, not placeholders.
3. Build and start the stack:

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
evidence report.

4. Run the production preflight against the runtime container:

```bash
docker compose \
  --env-file deploy/compose/.env \
  -f deploy/compose/docker-compose.external-services.yml \
  -f deploy/compose/docker-compose.stack.yml \
  exec runtime python engine/runtime/prod_preflight.py --json
```

5. Run the live smoke against the exposed stack from the repo root:

```bash
python tools/validate_repo.py --live
```

## Optional ROCm Runtime Profile

The base stack is CPU-only. For bart-style AMD Strix Halo hosts, use the ROCm
overlay only after `/dev/dri/renderD128`, `/dev/kfd`, and render/video group
access are validated:

```bash
export TRADING_REQUIREMENTS_FILE=requirements-amd-rocm.txt
export TRADING_ACCELERATION_PROFILE=amd-rocm
export TRADING_RENDER_GID="$(getent group render | cut -d: -f3)"
export TRADING_VIDEO_GID="$(getent group video | cut -d: -f3)"

docker compose \
  --env-file deploy/compose/.env \
  -f deploy/compose/docker-compose.external-services.yml \
  -f deploy/compose/docker-compose.stack.yml \
  -f deploy/compose/docker-compose.amd-rocm.yml \
  up -d --build
```

Then validate from inside the runtime container:

```bash
docker compose \
  --env-file deploy/compose/.env \
  -f deploy/compose/docker-compose.external-services.yml \
  -f deploy/compose/docker-compose.stack.yml \
  -f deploy/compose/docker-compose.amd-rocm.yml \
  exec runtime python tools/validate_rocm_acceleration.py --require-gpu
```

The overlay maps `/dev/dri` and `/dev/kfd` plus the render/video GIDs into the
runtime container only and switches the runtime build to AMD's
`rocm/pytorch:rocm7.2.4_ubuntu24.04_py3.12_pytorch_release_2.9.1` image; the
operator container remains device-free. See [ROCm Acceleration Profile](../../docs/ROCM_ACCELERATION.md)
for the package pins, fallback behavior, validation harness, 2 GB UMA / 66 GB
GTT caveat, and `gfx1151` maturity risk.

For a local compose stack with published ports, export the same auth and base URLs before running live smoke:

```bash
export PIPELINE_SMOKE_BASE="http://127.0.0.1:${DASHBOARD_PUBLIC_PORT:-8000}"
export PIPELINE_SMOKE_OPERATOR_BASE="http://127.0.0.1:${OPERATOR_PUBLIC_PORT:-4001}"
export DASHBOARD_API_TOKEN="$DASHBOARD_API_TOKEN"
export OPERATOR_API_TOKEN="$OPERATOR_API_TOKEN"
python tools/validate_repo.py --live
```

## Provider Credentials

The compose runtime passes provider and broker bootstrap settings from `deploy/compose/.env` into the Python runtime container. Leave providers disabled for the first dependency-only bring-up, then enable only the sources you have validated.

- Keep `PROD_LOCK=1`, `ALLOW_TRAINING=0`, `ENGINE_MODE=safe`, `EXECUTION_MODE=safe`, and `BROKER_NAME=sim` for the first bring-up.
- Keep `LIVE_TRADING_CONFIRM=0` in the committed example and during dependency-only bring-up. Set `LIVE_TRADING_CONFIRM=I_UNDERSTAND_LIVE_TRADING` only in the target host's operator-controlled compose `.env` when live execution is intentionally being enabled.
- Keep `TRADING_IMPORT_SMOKE_IMPORT_JOBS=0` for runtime startup. Startup compiles registered job files and imports only bootstrap-critical modules so heavy model/data imports cannot block dashboard binding. Use `TRADING_IMPORT_SMOKE_IMPORT_JOBS=1` only for explicit diagnostic runs.
- Polygon/Massive: create or retrieve an API key in the provider dashboard, confirm your plan includes the market-data entitlements you intend to use, then set `POLYGON_API_KEY`. Set `POLYGON_REST_ENABLED=1` and/or `POLYGON_WS_ENABLED=1` only when that source should run. Official setup: [Polygon quickstart](https://polygon.io/docs/rest/quickstart).
- Tradier: use the API Access page in your Tradier profile to get a sandbox or live token, then set `TRADIER_API_TOKEN` and `TRADIER_ENABLED=1`. Use a sandbox token for staging validation unless you are deliberately validating production data access. Official setup: [Tradier API getting started](https://support.tradier.com/kb/guide/en/getting-started-with-tradier-api-JYIaTkOdD1/Steps/4577201).
- Alpaca broker paper validation: set `BROKER_NAME=sim` and `BROKER=sim` for the first stack bring-up. For paper broker validation, set `ALPACA_BASE_URL=https://paper-api.alpaca.markets`, `ALPACA_KEY_ID`, and `ALPACA_SECRET_KEY` after the execution barrier and kill-switch posture have been reviewed. Official setup: [Alpaca paper trading](https://docs.alpaca.markets/docs/paper-trading).

Do not commit populated `.env` files. If provider variables are blank or disabled, infrastructure and preflight dependency checks can still run, but live smoke should be expected to fail provider freshness checks.

Dashboard mutation endpoints require `DASHBOARD_API_TOKEN` in production/live mode, including loopback binds. The runtime rejects missing, placeholder, or too-short dashboard tokens before serving mutations; the localhost no-token fallback is only for explicit safe local development and should not be enabled in compose production-like stacks.

## Operator Mode In Containers

The operator container runs with `OPERATOR_DISABLE_INTERNAL_ENGINE_START=1`.

That is intentional. In the compose deployment path, the operator is a UI/proxy sidecar and not the lifecycle owner of the Python runtime process. Runtime lifecycle belongs to the container orchestrator, not to a sibling process trying to spawn `start_system.py` across containers.

Operator mutation endpoints require `OPERATOR_API_TOKEN` when accessed through the published port. The live smoke sends that token as `X-Operator-Token`; set `PIPELINE_SMOKE_OPERATOR_TOKEN` only if the smoke token should differ from `OPERATOR_API_TOKEN`.

## Production Threshold

Do not treat this compose stack alone as proof that the full target-state plan is complete.

The current production threshold for this repo remains:

- compose stack starts cleanly
- runtime preflight passes
- `python tools/validate_repo.py --live` passes against the running stack
- `docs/handoff/codex_migration/FULL_SCOPE_VALIDATION.md` no longer has blocking `Partial in repo` or `Not implemented in repo` rows for your intended deployment claim

## 2026-06-15 Server Bring-Up Mode

Chosen deployment mode: compose.

Use compose for this production-functional server bring-up because the repository already defines the external dependency services, the Python runtime, and the Node operator sidecar in one deployment contract. The compose path keeps runtime lifecycle ownership with Docker, leaves the operator as a proxy/control sidecar, and starts the first dependency-only bring-up in `ENGINE_MODE=safe`, `EXECUTION_MODE=safe`, `PROD_LOCK=1`, `ALLOW_TRAINING=0`, `MODEL_SCORING_ENABLED=0`, `BROKER_NAME=sim`, `BROKER=sim`, `AUTO_BOOT_DAEMONS=0`, and `START_INGESTION_WITH_SERVER=0`.

Ingestion is intentionally not auto-started in this safe bring-up. Provider credentials are disabled by default, and explicit ingestion startup should be validated separately before enabling provider polling.

For the next production-function validation step, keep `BROKER_NAME=sim`, `BROKER=sim`, `EXECUTION_MODE=safe`, `DISABLE_LIVE_EXECUTION=1`, and `AUTO_PIPELINE_INCLUDE_EXECUTION=0`, then enable `AUTO_BOOT_DAEMONS=1`, `START_INGESTION_WITH_SERVER=1`, `MODEL_SCORING_ENABLED=1`, `AUTO_PIPELINE=1`, and `KILL_SWITCH_GLOBAL=0`. This proves the data/scoring/sim execution health path without enabling live broker order routing.

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
curl -fsS -H "X-Operator-Token: ${OPERATOR_API_TOKEN}" http://127.0.0.1:4001/api/operator/status
curl -fsS -H "X-Operator-Token: ${OPERATOR_API_TOKEN}" http://127.0.0.1:4001/api/operator/readiness
```

Do not enable real trading during this bring-up.
