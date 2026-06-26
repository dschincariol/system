# Codex Deep-Dive — Gap B: Reconcile multi-generation deployment drift to one canonical topology

## Role
You are a senior platform engineer responsible for making this single-box trading deployment reproducible and unambiguous before production go-live. Define ONE canonical topology and safely reconcile both the host and the repo to it. Be non-destructive first; provide rollback.

## Background (verified on the production host)
The box carries several deployment generations side by side, with no single source of truth:

- **Current/intended model:** systemd `trading-engine.service` (runs `start_system.py`, dashboard :8000 + ingestion child) and `trading-operator.service` (runs `boot/operator_server.js`, :4001), with the **data layer as Docker containers** `trading-timescaledb` (:5432), `trading-redis` (:6379), `trading-minio` (:9000/1). The live engine env (`/etc/trading/trading.env`) points at the docker Postgres (`TS_PG_DSN host=127.0.0.1 port=5432`).
- **A conflicting older model is still installed:** `/etc/systemd/system/trading.target` declares `Requires=postgresql.service redis-server.service pgbouncer.service trading-cpu-power-policy.service` and `Wants=trading-api.service trading-jobs.service trading-stream-prices.service trading-ingest.service` — i.e. a **host-service** Postgres/redis/pgbouncer topology (host `pgbouncer` is in fact listening on :6432) that does NOT match the docker data layer the live engine uses.
- **~17 `trading-*` units coexist** (e.g. `trading-base-backup`, `trading-backup-evidence` + `.bak` + `.d` override, `trading-backup-prune`, `trading-cpu-power-policy`, `trading-prod-preflight`, `trading-swapfile`, `trading-zram-swap`, plus `.d`/`.bak` overrides) with unclear canonical-vs-legacy status.
- **Stale competing app containers** existed: `trading-runtime` (`python start_system.py`) and `trading-operator` (node operator), both `restart=unless-stopped` — a second engine that can bind :8000. (These were removed manually, but `deploy/compose/docker-compose.stack.yml` would recreate them on `compose up`.)

Risks: dual-engine `:8000` conflicts and reboot resurrection; ambiguous control surface (operator/automation unsure which units are authoritative); "production" running three half-wired topologies; data written to two different Postgres backends depending on which generation starts.

## Objective
Establish a single canonical deployment topology for this one-box production model and reconcile the host + repo to it, non-destructively (inventory → quarantine → remove) with explicit rollback.

## Requirements (acceptance criteria)
1. **Authoritative data-layer decision.** Determine and document which data layer is canonical (docker `trading-timescaledb/redis/minio` vs host `postgresql/redis/pgbouncer`). Make the engine env, compose, systemd units, and `trading.target` all consistent with that ONE choice; eliminate/decommission the other path. Resolve the `trading.target` naming/ownership collision (if an aggregate app target is desired, use a distinct name such as `trading-app.target` with `Wants=trading-engine.service trading-operator.service` and no host-service deps).
2. **Full inventory + classification.** Enumerate every `trading-*` systemd unit/target/timer/override and every `trading-*` docker container/compose service; classify each as canonical / legacy / required-for-compliance (backup + restore-drill evidence units), with evidence for each call. Do not delete anything blind.
3. **Safe decommission procedure with rollback.** For legacy artifacts: stop + disable + mask (or move to a timestamped quarantine dir) systemd units; set legacy docker app containers `restart=no` and remove; reconcile `docker-compose.stack.yml` so `compose up` cannot resurrect a second engine bound to :8000 (e.g. profile-gate the app services, or remove them in favor of the systemd units). Provide a documented rollback for every step.
4. **Single creator of deployment artifacts.** Ensure `deploy/install_trading_system.sh` (+ the compose files) are the only things that create deployment artifacts, that they own/declare the full canonical unit set, and that a fresh or repeated install leaves NO drift (idempotent; explicitly removes or masks obsolete units it previously may have shipped).
5. **Enforced in production config**, not only docs: the canonical topology is expressed in installed units/compose/installer, and a preflight verifies no conflicting legacy unit/container is active (e.g. `trading-prod-preflight` style check that fails on a competing :8000 owner or an enabled host-service data path when docker is canonical).

## Constraints
- Non-destructive first: inventory + quarantine before removal; never break the currently-working docker data layer or the live engine env mid-migration.
- Preserve backup, base-backup, WAL/restore-drill evidence units and timers required by compliance — classify them as canonical, do not remove.
- Keep changes reviewable and reversible; no silent mass `rm` of `/etc/systemd/system`.

## Pointers
- Host: `/etc/systemd/system/trading*.{service,target,timer}` and `*.d`/`*.bak`
- Repo: `deploy/systemd/`, `ops/server/systemd/` (canonical host-bootstrap unit source per `deploy/README.md`), `deploy/compose/docker-compose.stack.yml`, `deploy/compose/docker-compose.external-services.yml`
- `deploy/install_trading_system.sh`, `deploy/README.md`, `ops/server/README.md`, `deploy/LINUX_SERVER_CODEX_DEPLOY.md`, `deploy/PRODUCTION_FILE_MANIFEST.md`

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.
