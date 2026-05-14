# Cross-Platform Development: Windows Dev → Linux Staging/Prod

This system is **developed on Windows** and **deployed on Linux**
(staging and production). The nine database prompts in this directory
fall into two categories with very different cross-platform stories.

## TL;DR

- **Deployment infrastructure** (DB-01, DB-06, DB-07, DB-09) is
  Linux-only by design. Bash, systemd, PgBouncer, `systemd-creds`,
  `ufw`, Unix sockets — these run **only on the Linux servers**, never
  on your Windows machine. That is correct: production infrastructure
  belongs on production hosts.
- **Application code** (DB-02 storage layer, DB-03 schema, DB-04
  Redis cache, DB-05 artifact store, DB-08 audit chain) is **fully
  cross-platform Python**. The prompts have been updated to make
  every path, DSN, and socket configurable per platform via
  environment variables with platform-appropriate defaults.

If you follow the recommended dev workflow below, the same Python
code that runs on your Windows dev machine runs unmodified on the
Linux staging and production servers.

## Recommended dev workflow (pick one)

### Option A — WSL2 (recommended)

Run **WSL2 with Ubuntu 22.04** on your Windows machine. Inside WSL2,
run the dev `postgres-16 + timescaledb + redis-7 + pgbouncer` stack
exactly as the staging/prod servers do. Your Python code lives on
either the Windows side (mounted at `/mnt/c/...`) or the WSL2 side;
it talks to the WSL2 services over `localhost`.

Why this is the cleanest:

- One Linux command to set up the dev DB stack: re-run
  `ops/server/bootstrap.sh` inside WSL2 against a dev port range and
  a dev datadir. (DB-01's bootstrap is parameterized for this.)
- Bit-for-bit parity between dev and prod stacks. No "works on my
  machine" surprises.
- You still write code in your Windows IDE; nothing about your
  development habits has to change.
- WSL2 supports Unix sockets natively, so dev exercises the same
  socket transport prod uses.

### Option B — Docker Desktop dev container

`docker compose up` a stack of `postgres:16 + timescale/timescaledb +
redis:7 + pgbouncer/pgbouncer` containers exposing TCP ports on
`localhost`. Your Python code on Windows talks to them over TCP.

Tradeoffs:

- Easiest one-shot setup; no WSL2 learning curve.
- Dev uses TCP transport; prod uses Unix sockets. Functionally
  equivalent but you do not exercise the socket path in dev.
- Container resource overhead is real (a few GB of RAM).

### Option C — Native Windows Postgres + Memurai

Install Postgres-16 for Windows from the EnterpriseDB installer.
Install [Memurai](https://www.memurai.com/) (a Redis-compatible
Windows server) for the cache; native Microsoft Redis is unmaintained.
PgBouncer on Windows is technically possible but not standard.

Tradeoffs:

- No virtualization. Lowest resource overhead.
- Dev stack drifts further from prod (TCP-only, no PgBouncer locally,
  Memurai instead of Redis).
- Suitable when you only need to develop and unit-test the Python
  layer; integration testing of the full stack is harder.

**Recommendation:** **Option A (WSL2)** for any non-trivial work.
Option B is fine if you already have Docker Desktop and want to avoid
WSL2 setup. Option C only if neither of the above is available.

## Configuration via environment variables

The application code reads a small set of environment variables that
default to platform-appropriate values. Set them once per
environment; the same Python code runs everywhere.

| Variable | Linux default | Windows default | Purpose |
|---|---|---|---|
| `TS_PG_DSN` | `host=/var/run/postgresql port=6432 user=ts_app dbname=trading` | `host=127.0.0.1 port=5432 user=ts_app dbname=trading password=…` | Postgres connection string for the application pool. On Linux prod, points at PgBouncer's socket by default. On Windows dev, TCP localhost. |
| `TS_PG_PORT` | `6432` | `5432` | Port used by the default application DSN when `TS_PG_DSN` is unset. Set `TS_PG_PORT=5432` on Linux installs that connect directly to Postgres instead of PgBouncer. |
| `TS_PG_ADMIN_DSN` | `host=/var/run/postgresql port=5432 user=postgres dbname=postgres` | `host=127.0.0.1 port=5432 user=postgres dbname=postgres password=…` | Direct-Postgres DSN for migrations and admin tools (bypasses PgBouncer). |
| `TS_REDIS_URL` | `unix:///var/run/redis/trading.sock` | `redis://127.0.0.1:6379/0` | Redis client target. Linux uses Unix socket; Windows uses TCP. |
| `TS_DATA_ROOT` | `/var/lib/trading` | `%LOCALAPPDATA%\Trading` | Root for all data directories. |
| `TS_ARTIFACTS_ROOT` | `${TS_DATA_ROOT}/artifacts` | `${TS_DATA_ROOT}\artifacts` | Artifact store root (DB-05). |
| `TS_BACKUP_ROOT` | `/var/backups/trading` | (unused on Windows; backups are a prod concern) | Backup destination root (DB-07). |
| `TS_SECRETS_PROVIDER` | `systemd-creds` | `dpapi` (production-grade) or `plaintext` (dev) | Secret-loader implementation (DB-09). |
| `CREDENTIALS_DIRECTORY` | (set by systemd) | (unused) | Where systemd-creds drops decrypted secrets at service start. |
| `TS_DEV_SECRETS_DIR` | (unused) | `%LOCALAPPDATA%\Trading\secrets` | Plaintext-secret directory for dev when `TS_SECRETS_PROVIDER=plaintext`. |

The Python code reads each of these via `os.environ.get(name,
default_for_platform())` where `default_for_platform()` returns the
right value based on `sys.platform`.

## Path handling

Every file path in the application code uses `pathlib.Path`. No
string concatenation of `/`-delimited paths anywhere in `engine/`,
`services/`, or `ops/` Python modules. The audit test
`tests/test_no_string_paths.py` enforces this — it greps for
hardcoded `/var/`, `/etc/`, and `\\` in module bodies and fails the
build on hits outside the platform-defaults helper module.

## Per-prompt cross-platform notes

### DB-01 — Server bootstrap (Linux-only, by design)

Runs only on the Linux servers. Do not attempt on Windows. The bash
script, systemd units, sysctl tuning, and `ufw` rules all assume
Linux.

For dev, run the bootstrap inside **WSL2 Ubuntu** (Option A) — it
works unchanged. Or skip the bootstrap entirely and use Docker
Compose (Option B) / native installers (Option C).

### DB-02 — Postgres storage layer (cross-platform)

The Python implementation reads `TS_PG_DSN` with a platform default.
On Linux: Unix socket on `TS_PG_PORT` (default `6432`, PgBouncer).
Set `TS_PG_PORT=5432` for direct Postgres installs without replacing
the whole DSN. On Windows: TCP `127.0.0.1:5432`. No code change is
needed when moving the code between platforms; only the env var
differs.

The migrator and any admin tooling use `TS_PG_ADMIN_DSN` (also
platform-defaulted) so they reach Postgres directly even when
PgBouncer is in front on Linux.

### DB-03 — Schema with hypertables (cross-platform)

Pure SQL via Timescale. Runs on any Postgres+Timescale install
including Windows. No platform-specific concerns.

### DB-04 — Redis hot-path cache (cross-platform)

The Redis client builds from `TS_REDIS_URL`. On Linux:
`unix:///var/run/redis/trading.sock`. On Windows:
`redis://127.0.0.1:6379/0` (assumes Memurai or a Redis-in-Docker /
Redis-in-WSL2 backend).

The circuit breaker, fail-open behavior, write-through discipline,
and tests are all transport-agnostic.

### DB-05 — Object storage for artifacts (cross-platform)

The store reads `TS_ARTIFACTS_ROOT` and uses `pathlib.Path`
throughout. Atomic writes use `os.replace()` (works on both
platforms). Sharded directory structure
(`<00..ff>/<00..ff>/<sha256>`) works identically.

The `migrate_artifacts.py` tool accepts a `--source-root` argument
so a developer can migrate dev artifacts under `%LOCALAPPDATA%`
without hardcoded paths.

### DB-06 — PgBouncer + observability (Linux-only, by design)

PgBouncer runs only on the Linux servers. On Windows dev, the
Python code talks directly to Postgres on TCP via `TS_PG_DSN` — the
storage-layer pool sizing is sufficient at dev workload, no pooler
needed.

`pg_stat_statements` and the slow-log tail run only against the
Linux Postgres; the dev database does not run them. The
observability snapshotter is a no-op when `TS_PG_DSN` points at a
Postgres without `pg_stat_statements` installed (it logs a single
warning at startup and skips).

### DB-07 — Backup, WAL archive, restore (Linux-only, by design)

Pure production concern. The bash scripts and systemd timers run
only on the Linux servers.

For dev, periodic `pg_dump` of your dev database is sufficient; do
not attempt to wire WAL archiving on a dev machine.

### DB-08 — Audit hash chain (cross-platform)

Pure Python. The canonical serialization, hashing, advisory-lock
handling, and verifier all work on any platform Python supports.

### DB-09 — Secrets via systemd-creds (Linux: systemd-creds; Windows: pluggable)

The `services.secrets.loader` module is pluggable via
`TS_SECRETS_PROVIDER`. Three implementations:

1. **`systemd-creds` (Linux production):** reads from
   `${CREDENTIALS_DIRECTORY}/<name>`. This is what staging and
   production use.
2. **`dpapi` (Windows, production-grade dev):** uses Windows DPAPI
   (`win32crypt.CryptProtectData`) to encrypt secrets bound to the
   developer's Windows user account. Persisted under
   `%LOCALAPPDATA%\Trading\secrets\*.dpapi`.
3. **`plaintext` (Windows or Linux, dev only):** reads plaintext
   files from `TS_DEV_SECRETS_DIR`. Logs a loud warning at startup
   and refuses to run if `TS_ENV=production`.

The application code calls `load_secret("name")` and never knows
which provider answered. Choosing a provider is a deployment
decision, not a code change.

## CI considerations

The application-code tests run on both Linux runners (CI for
production parity) and Windows runners (CI for dev parity). The
infrastructure tests (DB-01 bootstrap, DB-07 backup scripts) run on
Linux only — they shell-out to bash and systemd, which Windows CI
cannot exercise.

`pytest` markers used:

- `@pytest.mark.linux_only` — skipped on Windows.
- `@pytest.mark.windows_only` — skipped on Linux.
- `@pytest.mark.requires_postgres` — skipped if `TS_PG_DSN` is
  unreachable.
- `@pytest.mark.requires_redis` — skipped if `TS_REDIS_URL` is
  unreachable.

A `tests/conftest.py` declares these markers and the relevant skip
conditions so the same `pytest -q` invocation works on either OS.

## Concretely: what to do today

1. **Install WSL2 Ubuntu 22.04** on your Windows dev machine.
2. Inside WSL2, run a dev variant of the DB-01 bootstrap (the script
   accepts `TS_ENV=dev` for non-production tuning, dev port range,
   and skipping `ufw`).
3. On the Windows side, set in your shell profile or `.env`:
   ```
   TS_PG_DSN=host=127.0.0.1 port=5432 user=ts_app dbname=trading password=...
   TS_PG_ADMIN_DSN=host=127.0.0.1 port=5432 user=postgres dbname=postgres password=...
   TS_REDIS_URL=redis://127.0.0.1:6379/0
   TS_DATA_ROOT=%LOCALAPPDATA%\Trading
   TS_SECRETS_PROVIDER=dpapi
   TS_ENV=dev
   ```
4. (WSL2 forwards 5432 and 6379 to Windows-side `localhost` by
   default since WSL2's networking; if not, use the WSL2 IP.)
5. Run `python -m engine.runtime.schema.migrator` from Windows to
   apply migrations to the dev DB inside WSL2.
6. `pytest -q` runs the full Python suite on Windows; CI runs it on
   Linux too; staging runs the same code; production runs the same
   code.

The same `pyproject.toml` / requirements file works everywhere; no
platform-conditional dependencies in the runtime. The only
Windows-specific package is `pywin32` (for the DPAPI secrets
provider), declared as an extras-only dependency
(`pip install -e '.[windows-dev]'`).
