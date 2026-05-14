# Codex DB Prompt 01 — Single-Server Linux Bootstrap

You are setting up a **single Debian-family Linux server** (Ubuntu
22.04 LTS or Debian 12) as the production host for a Python systematic
trading system. Your task is to produce a **reproducible, idempotent
bootstrap script** plus systemd integration that brings the server
from a fresh OS install to "ready to deploy the trading application."

This is greenfield: there is no production data to preserve. Optimize
for performance on a single host (Postgres + Redis + PgBouncer + the
trading app all co-located, communicating via Unix sockets).

## Cross-platform note

This prompt is **Linux-only by design**. The user develops on Windows
and deploys on Linux; this script runs **only on the staging and
production Linux servers**, never on the Windows dev machine. Do not
attempt cross-platform shims for `apt`, `systemd`, `ufw`, or
`systemd-creds`. The dev workflow uses WSL2 Ubuntu (which runs this
script unchanged) or Docker Compose; see
`docs/codex_prompts/database/CROSS_PLATFORM.md`.

## Goal

1. A single bootstrap script `ops/server/bootstrap.sh` that, run as
   root on a clean Ubuntu 22.04 / Debian 12 server, installs and
   configures every system-level dependency for the trading stack.
2. Filesystem layout under `/var/lib/trading/`, `/var/backups/trading/`,
   and `/etc/trading/` with correct ownership and permissions.
3. systemd units for every long-running trading-app process, with
   `Restart=on-failure`, journald logging, and resource limits.
4. Postgres 16 + TimescaleDB 2.x, Redis 7, and PgBouncer installed
   from official repositories, with tuned configuration for the host's
   RAM/CPU.
5. A first-time-setup verifier `ops/server/verify.sh` that confirms
   every component is healthy and reachable.

## Hardware assumptions (declare and parameterize)

The script must read host capacity at runtime and tune accordingly,
but the README and reference values target the canonical deployment:

- 8–16 vCPU
- 32–64 GB RAM
- ≥ 1 TB NVMe SSD on `/var/lib/trading/`
- A second disk or partition mounted at `/var/backups/trading/` for
  base backups and WAL archive (separate from the data path).

## Files to read first (read-only)

- `engine/runtime/storage.py` — to know what tables and connection
  patterns the application expects.
- `engine/runtime/job_registry.py` — to know which long-running
  processes will need systemd units.
- `boot/start_operator.bat` — Windows-side launcher; reference for
  what the equivalent Linux launcher must wire up.
- `engine/terminal/api/api_terminal.py` — UI process; will need its
  own systemd unit.
- Whatever Polygon WS streamer module exists
  (`engine/jobs/stream_prices_polygon_ws.py`) — long-running, needs
  its own unit.
- The data ingestion modules under `engine/data/ingest/` — to know
  which run as cron-style timers vs. long-running.

## Files to create

- `ops/server/bootstrap.sh` — main entrypoint. Sections:
  - `set_kernel_tuning` — sysctl: `vm.overcommit_memory=2`,
    `vm.swappiness=1`, `net.core.somaxconn=4096`,
    `fs.file-max=262144`, transparent hugepages disabled (Postgres
    recommendation).
  - `create_users_and_dirs` — system user `trading` (no shell),
    group `trading`. Directories `/var/lib/trading/{db,redis,artifacts,nlp_models,logs}`,
    `/var/backups/trading/{base,wal}`, `/etc/trading/`. Mode 0750.
  - `install_postgres_timescale` — add the official PGDG repo and
    Timescale repo; install `postgresql-16`,
    `timescaledb-2-postgresql-16`, `postgresql-contrib-16`.
  - `tune_postgres` — generate `/etc/postgresql/16/main/conf.d/trading.conf`
    using the values in `ops/server/config/postgres.conf.tmpl`. Tuning
    derived from host RAM (use the same heuristics as `pgtune`):
    `shared_buffers = RAM/4`, `effective_cache_size = RAM*3/4`,
    `work_mem = RAM*0.5%/max_connections`, `maintenance_work_mem = RAM/16`,
    `wal_buffers = 16MB`, `wal_compression = on`,
    `checkpoint_completion_target = 0.9`,
    `random_page_cost = 1.1` (NVMe), `effective_io_concurrency = 200`,
    `max_worker_processes = vCPU*2`,
    `max_parallel_workers_per_gather = vCPU/2`,
    `shared_preload_libraries = 'timescaledb,pg_stat_statements'`.
  - `install_redis` — `redis-server`. Configure Unix socket at
    `/var/run/redis/trading.sock`, AOF on (`appendfsync everysec`),
    `maxmemory-policy noeviction` (we never want eviction in front of
    a trading system; OOM is signal, not noise), `maxmemory` set to
    25% of RAM.
  - `install_pgbouncer` — install, configure transaction-pool mode at
    `/etc/pgbouncer/pgbouncer.ini`. Listen on Unix socket
    `/var/run/postgresql/.s.PGSQL.6432` and TCP localhost only.
    `pool_mode = transaction`, `default_pool_size = 25`,
    `max_client_conn = 200`.
  - `init_databases` — create three Postgres roles (`ts_ingest`,
    `ts_app`, `ts_reader`) with distinct passwords stored as
    systemd-creds (referenced by prompt 09). Create database
    `trading`. Enable extensions `timescaledb`, `pg_stat_statements`,
    `pg_trgm`, `pgcrypto`.
  - `install_python_runtime` — system Python 3.11 from `deadsnakes` if
    needed; create the project venv at `/opt/trading/venv`; install
    requirements.
  - `install_systemd_units` — copy units from `ops/server/systemd/` to
    `/etc/systemd/system/`; `daemon-reload`; enable but do not start
    yet (start happens after first deploy).
  - `setup_logrotate` — `/etc/logrotate.d/trading` for app logs and
    Postgres logs.
  - `setup_firewall` — `ufw` allow 22 (SSH) and the UI port; deny
    everything else by default.
- `ops/server/verify.sh` — runs after bootstrap; checks:
  - Postgres responds on socket, `SELECT 1`.
  - TimescaleDB extension is present: `SELECT extversion FROM pg_extension WHERE extname='timescaledb'`.
  - Redis responds on Unix socket: `PING → PONG`.
  - PgBouncer responds on its socket.
  - Filesystem layout present with correct ownership.
  - All systemd units pass `systemd-analyze verify`.
- `ops/server/config/postgres.conf.tmpl` — Jinja-style template
  consumed by `tune_postgres`.
- `ops/server/config/pgbouncer.ini.tmpl`
- `ops/server/config/redis.conf.tmpl`
- `ops/server/systemd/trading-api.service` — UI / terminal API.
- `ops/server/systemd/trading-jobs.service` — jobs manager.
- `ops/server/systemd/trading-stream-prices.service` — Polygon WS
  streamer.
- `ops/server/systemd/trading-ingest.service` — RSS / news / GDELT
  pollers.
- `ops/server/systemd/trading.target` — meta-unit that starts all of
  the above; `WantedBy=multi-user.target`.
- `ops/server/README.md` — operator-facing run book: install,
  verify, restart, where logs live.
- `tests/ops/test_bootstrap_idempotent.sh` — runs bootstrap twice in
  a Docker container; second run is a no-op.
- `tests/ops/test_systemd_units_lint.sh` — runs `systemd-analyze
  verify` against every unit.

## Files to modify

- None. This prompt is purely additive infrastructure.

## Implementation plan

1. **Idempotency first.** Every step in `bootstrap.sh` must check
   current state before acting. Use `apt list --installed`,
   `id -u trading`, `systemctl is-enabled`, `psql -c "SELECT 1 FROM
   pg_extension WHERE extname='timescaledb'"` — never assume a clean
   slate. Re-running the script must be a no-op when state is correct.
2. **Fail loud.** `set -euo pipefail` at the top. Trap errors and
   print the failing line with context.
3. **Parameterize but default sane.** All paths and tunables are
   shell variables at the top of the script with environment-variable
   override; values match the canonical deployment.
4. **Postgres tuning is auto-derived.** Read `/proc/meminfo` and
   `nproc`; render the conf template with computed values.
5. **systemd units use the trading user**, set `ProtectSystem=strict`,
   `ProtectHome=true`, `PrivateTmp=true`, `NoNewPrivileges=true`,
   `LimitNOFILE=65536`. Each unit `Type=notify` or `Type=simple` as
   appropriate. `Restart=on-failure`, `RestartSec=5s`.
6. **Roles and passwords.** Generate three random passwords; store
   them under `/etc/trading/secrets/` with mode 0400 owned by
   `trading`. (Prompt 09 will move these into systemd-creds.)
7. **Logs to journald.** No application file logging at this layer;
   the unit files use `StandardOutput=journal`. Operators read with
   `journalctl -u trading-*`.
8. **Backups directory primed but empty.** Backup setup itself is
   prompt 07; this prompt only creates the directory and ownership.

## Performance targets

- Bootstrap completes on a clean 16 vCPU / 32 GB host in under 10
  minutes (excluding `apt` mirror latency).
- Verifier completes in under 10 seconds.
- After bootstrap, an empty Postgres responds to `SELECT 1` over the
  Unix socket in < 1 ms (verified by `verify.sh`).
- Redis `PING` over Unix socket in < 0.5 ms.

## Acceptance criteria

- [ ] `bootstrap.sh` runs to completion on a clean Ubuntu 22.04
      Docker container without manual intervention.
- [ ] Re-running `bootstrap.sh` immediately after is a no-op
      (no `apt install` calls, no config rewrites unless changed).
- [ ] `verify.sh` exits 0 on a freshly bootstrapped host.
- [ ] All systemd units pass `systemd-analyze verify`.
- [ ] All systemd units have `Restart=on-failure`, `NoNewPrivileges=true`,
      `ProtectSystem=strict`.
- [ ] Postgres `shared_preload_libraries` includes `timescaledb` and
      `pg_stat_statements`.
- [ ] Redis is reachable only on its Unix socket and 127.0.0.1
      (`bind 127.0.0.1 -::1`); no external listener.
- [ ] PgBouncer is in `pool_mode = transaction`.
- [ ] `ufw` denies all inbound except SSH and the configured UI port.
- [ ] Filesystem layout exists with mode 0750 owned by `trading:trading`.
- [ ] No secrets are committed to the repository; `/etc/trading/secrets/`
      is created at bootstrap time with random values.

## Test plan

- `tests/ops/test_bootstrap_idempotent.sh` — runs the bootstrap in a
  Debian 12 Docker container twice; asserts no diff in
  `/etc/postgresql/16/main/conf.d/trading.conf` after the second run.
- `tests/ops/test_systemd_units_lint.sh` — `systemd-analyze verify`
  on every `*.service` and `*.target` file.
- Manual smoke (documented in `ops/server/README.md`): bootstrap →
  verify → connect via `psql` and `redis-cli` over their sockets.

Run: `bash tests/ops/test_bootstrap_idempotent.sh && bash
tests/ops/test_systemd_units_lint.sh`

## Out of scope

- The Postgres schema itself — that is prompt 03.
- The Python storage layer — that is prompt 02.
- Backups beyond directory creation — that is prompt 07.
- Secrets management beyond a minimal `/etc/trading/secrets/` —
  that is prompt 09.
- TLS termination, reverse proxy, public web access — production
  exposure is a follow-up; this prompt is server-side bring-up only.
- Container orchestration (Docker Compose, Kubernetes). The host is
  bare-metal / single-VM by design.
