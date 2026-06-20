# Staging Prod-Preflight Evidence

Use this harness to prove `engine/runtime/prod_preflight.py --json` against an explicit non-prod Postgres target and keep a redacted JSON artifact for audit review.

## One-Time Setup

1. Copy `deploy/env/staging-prod-preflight.env.example` to `deploy/env/staging-prod-preflight.env`.
2. Fill only staging values in the local env file. Do not commit the populated file.
3. Keep `STAGING_PREFLIGHT_TARGET_ENV=staging`, `TS_STORAGE_BACKEND=postgres`, and `TS_PG_DSN` pointed at the staging Postgres database.
4. Keep `ENGINE_MODE=safe`, `EXECUTION_MODE=safe`, `PROD_LOCK=1`, `ALLOW_TRAINING=0`, `DISABLE_LIVE_EXECUTION=1`, and `KILL_SWITCH_GLOBAL=1` unless you are running a separate, intentionally confirmed production credential check.

## Run

From the repo root:

```bash
bash ops/server/run_staging_prod_preflight.sh
```

Equivalent direct command:

```bash
python -m engine.runtime.staging_prod_preflight \
  --env-file deploy/env/staging-prod-preflight.env \
  --target-env staging \
  --evidence-dir var/artifacts/preflight
```

The command writes a redacted artifact under `var/artifacts/preflight/staging/`. The repository ignores `var/`, so runtime evidence is preserved locally for review without being staged by default.

The GitHub production-backend gate also runs this harness against its designated CI Postgres service and uploads the redacted `var/artifacts/preflight/staging/*.json` file for release-signoff evidence.

## Guardrails

The harness refuses to launch unless the target environment is explicit, `TS_STORAGE_BACKEND` is Postgres, and `TS_PG_DSN` is present. It starts from a minimal process environment by default so ambient production database variables do not leak into the run.

Production-like signals in `APP_ENV`, `ENV`, `ENGINE_MODE`, `EXECUTION_MODE`, `OPERATOR_MODE`, Postgres/Redis DSNs, or default production credential paths stop the run. The only override is:

```bash
python -m engine.runtime.staging_prod_preflight \
  --env-file deploy/env/staging-prod-preflight.env \
  --target-env staging \
  --allow-production-target \
  --confirm-production-target I_UNDERSTAND_THIS_USES_PRODUCTION_CREDENTIALS
```

Use that override only when the review is intentionally validating production credentials. The evidence artifact records that the override was used.

## Evidence Contents

Each artifact includes:

- target environment, target id, and env files used
- redacted environment snapshot and Postgres target summary
- guardrail findings
- exact `prod_preflight.py --json` command
- subprocess exit code, stdout/stderr, and parsed prod-preflight JSON when available

Secrets are redacted by key name and by DSN/URL credential patterns before the artifact is written.
