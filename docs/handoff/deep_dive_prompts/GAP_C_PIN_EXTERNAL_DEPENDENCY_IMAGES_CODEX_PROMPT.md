# Codex Deep-Dive — Gap C: Pin external dependency images + enforce DB version compatibility

## Role
You are a senior platform engineer making the trading system's external dependency stack reproducible and version-safe for production. Implement immutable image pinning plus a runtime compatibility guard. Build on the existing compose/env pattern.

## Background (verified on the production host)
`deploy/compose/docker-compose.external-services.yml` sets all images from required env vars with NO defaults:
`${APP_RUNTIME_IMAGE:?}` (line 9), `${TIMESCALE_IMAGE:?}` (55), `${REDIS_IMAGE:?}` (165), `${MINIO_IMAGE:?}` (195), `${MINIO_CLIENT_IMAGE:?}` (222). `deploy/compose/.env.example` ships these empty (`TIMESCALE_IMAGE=`, `REDIS_IMAGE=`, `MINIO_IMAGE=`), with no pinned guidance.

Live containers (ground truth):
- `trading-timescaledb` → `timescale/timescaledb:latest-pg16`  ← **floating `latest`**, currently resolves to TimescaleDB **2.27.0** on PostgreSQL 16.13
- `trading-redis` → `redis:7-alpine`  (minor-floating)
- `trading-minio` → `minio/minio:RELEASE.2025-04-22T22-12-26Z`  (date-pinned)

Why this is a production hazard: a floating `latest-pg16` means the database engine can change underneath the app on any `docker pull`/recreate. The app emits version-sensitive TimescaleDB DDL (hypertable creation, `timescaledb.compress`/`compress_orderby` compression, retention) from `engine/runtime/schema/` and `engine/runtime/timescale_client.py`; TimescaleDB has shipped breaking compression/columnstore changes across 2.1x→2.2x. There is a known, related startup failure where schema-repair emits an invalid `compress_orderby` for `alpha_lifecycle` — version drift compounds exactly this class of bug.

## Objective
Pin every external dependency image immutably with safe defaults, and make the runtime fail-loud (not silently emit incompatible DDL) on an unsupported database version.

## Requirements (acceptance criteria)
1. **Immutable pins.** Pin timescaledb, redis, minio, and minio-client to explicit `tag@sha256:<digest>` references. Provide concrete recommended pins, including a specific TimescaleDB version (tag + digest) that you have verified is compatible with this repo's schema/compression code on pg16 — not `latest`.
2. **Safe pinned defaults/guidance.** Put the pinned references (or clearly-documented pinned defaults) into `deploy/compose/.env.example` and any profile env, so a fresh deploy cannot silently float to `latest`. Keep secrets out of these files. Preserve the existing `${VAR:?}` override pattern but make the documented/expected value pinned.
3. **Runtime compatibility guard (production code).** Add a startup/db-repair check that detects the connected TimescaleDB/Postgres version and refuses (or, where non-live, clearly degrades with a loud, structured warning) when it is outside a supported/tested matrix — rather than proceeding to emit incompatible DDL. Make it fail-closed for live mode. This must be enforced in the engine/db-repair path, not only in tests.
4. **Supported version matrix + consistency with repair.** Document a supported TimescaleDB/PG matrix, and verify the chosen pin applies the full schema + compression repair cleanly (coordinate with the known `alpha_lifecycle` `compress_orderby` issue so the pin, the compat guard, and the repair are mutually consistent and a clean DB provisions without error).
5. **Documented, safe upgrade path.** Document how to move a pin forward safely (backup/base-backup + restore-drill before bumping; how to re-verify the compat matrix).

## Constraints
- Use digest pinning for true immutability; keep image refs configurable via the existing env-var pattern but with pinned, documented defaults.
- Treat the live data carefully even though it is sim/disposable (no destructive recreate without an explicit, documented step).
- Pin redis and minio too (redis to a digest-pinned minor; keep minio's date pin but add a digest).

## Pointers
- `deploy/compose/docker-compose.external-services.yml` (image lines 9, 55, 165, 195, 222) and `docker-compose.stack.yml`
- `deploy/compose/.env.example`, `deploy/compose/README.md`
- `engine/runtime/timescale_client.py`, `engine/runtime/schema/` (version-sensitive DDL incl. `migrations/0002_hypertables.py`), `engine/runtime/db_repair.py`, `engine/runtime/jobs/repair_schema.py`

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.
