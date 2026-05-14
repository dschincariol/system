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
2. Set approved image tags, dependency credentials, provider credentials, dashboard/operator API tokens, and public ports.
3. Build and start the stack:

```bash
docker compose \
  --env-file deploy/compose/.env \
  -f deploy/compose/docker-compose.external-services.yml \
  -f deploy/compose/docker-compose.stack.yml \
  up -d --build
```

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
- Keep `TRADING_IMPORT_SMOKE_IMPORT_JOBS=0` for runtime startup. Startup compiles registered job files and imports only bootstrap-critical modules so heavy model/data imports cannot block dashboard binding. Use `TRADING_IMPORT_SMOKE_IMPORT_JOBS=1` only for explicit diagnostic runs.
- Polygon/Massive: create or retrieve an API key in the provider dashboard, confirm your plan includes the market-data entitlements you intend to use, then set `POLYGON_API_KEY`. Set `POLYGON_REST_ENABLED=1` and/or `POLYGON_WS_ENABLED=1` only when that source should run. Official setup: [Polygon quickstart](https://polygon.io/docs/rest/quickstart).
- Tradier: use the API Access page in your Tradier profile to get a sandbox or live token, then set `TRADIER_API_TOKEN` and `TRADIER_ENABLED=1`. Use a sandbox token for staging validation unless you are deliberately validating production data access. Official setup: [Tradier API getting started](https://support.tradier.com/kb/guide/en/getting-started-with-tradier-api-JYIaTkOdD1/Steps/4577201).
- Alpaca broker paper validation: set `BROKER_NAME=sim` and `BROKER=sim` for the first stack bring-up. For paper broker validation, set `ALPACA_BASE_URL=https://paper-api.alpaca.markets`, `ALPACA_KEY_ID`, and `ALPACA_SECRET_KEY` after the execution barrier and kill-switch posture have been reviewed. Official setup: [Alpaca paper trading](https://docs.alpaca.markets/docs/paper-trading).

Do not commit populated `.env` files. If provider variables are blank or disabled, infrastructure and preflight dependency checks can still run, but live smoke should be expected to fail provider freshness checks.

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
