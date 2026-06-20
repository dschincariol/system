# Codex Prompts — Database for Single-Server Production Deployment

Nine self-contained Codex prompts that take the system from its current
SQLite backbone to a production-ready persistence layer running on
**one Linux server**. No staged migration, no dual-write, no rollback
window — the system is **not yet in production**, so we are building
the right thing the first time.

## Target environment

- **Production / staging**: one Linux server, **Debian-family**
  (Ubuntu 22.04 LTS or Debian 12). RHEL-family adaptation is
  straightforward but not the default.
- **Development**: Linux host or Linux dev container. The Python
  application code, deployment infrastructure, and CI contract are
  Linux-only by design.
- Postgres 16 + TimescaleDB 2.x + Redis 7 + PgBouncer co-located on
  the same host. Communication via Unix sockets where possible for
  minimum latency.
- Application processes run under `systemd` units, talking to the
  databases over local sockets.
- All data and artifacts under `/var/lib/trading/`. Backups under
  `/var/backups/trading/` with optional offsite WAL archive.
- No managed services, no cloud lock-in. The whole stack can be
  reproduced from the bootstrap script on any equivalently-sized
  server.

See **[CROSS_PLATFORM.md](CROSS_PLATFORM.md)** for the Linux-only
platform policy and the remaining security-relevant path checks.

## Goals

1. **Database is not the bottleneck.** Hot-path reads complete in
   sub-millisecond. Write rates of the live ingestion pipeline
   (Polygon WS streaming, options polling, news, GDELT) sustain
   without backpressure.
2. **Audit-grade durability** for promotion, decision, and execution
   trails — tamper-evident hash chains, never-deleted rows.
3. **Operationally simple.** A single operator can install, monitor,
   back up, and restore the entire stack from the scripts this work
   produces.
4. **Right-sized.** No Kubernetes, no Kafka, no managed cloud DB. One
   well-configured server beats a fleet for a one-operator system.

## Run order

| # | Prompt | Theme | Depends on |
|---|--------|-------|------------|
| 01 | [Server bootstrap](01_server_bootstrap.md) | Install Postgres + Timescale + Redis + PgBouncer; filesystem layout; systemd units | — |
| 02 | [Postgres storage layer](02_postgres_storage_layer.md) | Replace SQLite implementation in `engine/runtime/storage.py` | 01 |
| 03 | [Schema with hypertables](03_schema_hypertables.md) | All 210 tables expressed in Postgres + Timescale; compression, retention, BRIN/GIN indexes; continuous aggregates | 01, 02 |
| 04 | [Redis hot-path cache](04_redis_hot_path_cache.md) | Write-through cache for the seven hot-read tables on the decision path | 02, 03 |
| 05 | [Object storage for artifacts](05_object_storage_artifacts.md) | Move model files, raw text bodies out of the DB onto a content-hashed local filesystem | 02 |
| 06 | [PgBouncer + observability](06_pgbouncer_observability.md) | Connection pooling in transaction mode; pg_stat_statements; slow-query log to journald | 01, 02 |
| 07 | [Backup, WAL archive, restore drill](07_backup_restore.md) | Continuous WAL archive, nightly base backup, scripted restore | 01 |
| 08 | [Tamper-evident audit hash chain](08_audit_hash_chain.md) | `prev_hash` / `row_hash` columns on audit tables; verifier job | 02, 03 |
| 09 | [Secrets via systemd-creds](09_secrets_systemd_creds.md) | Move credential encryption key out of code into systemd-managed encrypted credentials | 01 |

01–06 are the critical path to a fast, correct production deployment.
07–09 are operational hardening, runnable in any order after the
critical path.

## How to use a prompt with Codex

1. Open the prompt file. The body **is** the system prompt — paste it
   verbatim into Codex on a clean feature branch named
   `codex/db-<NN>-<slug>` (e.g., `codex/db-01-server-bootstrap`).
2. Each prompt enumerates the files Codex must read before writing.
   Skipping that pass produces shallow output.
3. Each prompt ends with an explicit acceptance checklist. Codex must
   self-verify and report any unticked item as a deliberate carve-out
   with reasoning.

## Conventions used in every prompt

- **Read-only files** are listed first; Codex reads but does not modify
  them. They exist to ground the work in current behavior.
- **Files to create or modify** are listed with one-line intent.
- **Acceptance criteria** are testable, not aspirational.
- **Performance targets** are explicit numeric SLOs where applicable.
- **Test plan** specifies new test files and the exact `pytest -q` /
  shell-test invocation.
- **Out of scope** is non-negotiable; anything listed there must not be
  touched even if Codex believes it would be an improvement.

## Definition of done (applies to every prompt)

- New code has unit tests; coverage on touched modules is non-decreasing.
- `pytest -q` passes on the affected paths.
- Any shell scripts have idempotent re-run behavior and explicit
  `set -euo pipefail`.
- Any systemd unit has `Restart=on-failure`, sensible
  `RestartSec=`, and unit-file linting passes
  (`systemd-analyze verify`).
- The prompt's acceptance checklist is fully ticked, or unticked items
  are reported as deliberate carve-outs with reasoning.
