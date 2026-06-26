# Single-Server Linux Bootstrap

This directory bootstraps one Debian-family production host for the trading system. It is Linux-only and targets Ubuntu 22.04 LTS (`jammy`) or Debian 12 (`bookworm`).

Canonical host sizing:

- 8-16 vCPU
- 32-64 GB RAM
- 1 TB or larger NVMe mounted under `/var/lib/trading/`
- Separate disk or partition mounted at `/var/backups/trading/`

The bootstrap reads RAM and CPU at runtime and renders host-specific PostgreSQL and Redis settings.

## Install

Run as root from the Linux deployment bundle or a checked-out copy of the
repository:

```bash
sudo bash ops/server/bootstrap.sh
```

The script is idempotent. Re-running it should leave already-correct packages,
config files, users, directories, roles, app files, dependency folders, and
systemd units unchanged. When run from a bundle outside `/opt/trading/app`, it
mirrors the source tree into `/opt/trading/app` while excluding local state,
secrets, virtualenvs, `node_modules`, logs, caches, databases, and diagnostics.

For remote installs, mirror a clean Linux checkout or filtered source directory
to the Linux host and follow `deploy/LINUX_SERVER_CODEX_DEPLOY.md`.

Heavy Python dependency installation can be skipped for infrastructure-only test runs:

```bash
sudo TRADING_INSTALL_PYTHON_REQUIREMENTS=0 bash ops/server/bootstrap.sh
```

## Verify

```bash
sudo bash ops/server/verify.sh
```

The verifier checks PostgreSQL, TimescaleDB, Redis over its Unix socket, PgBouncer over `/var/run/postgresql/.s.PGSQL.6432`, filesystem ownership, and systemd unit syntax.

## Memory Pressure

Run the memory-pressure hardening installer on `bart` and equivalent ZFS
single-server hosts:

```bash
python -m engine.runtime.memory_pressure --json --required
sudo bash ops/server/memory_pressure_hardening.sh install
sudo bash ops/server/memory_pressure_hardening.sh verify
```

The Python command is read-only and should pass before live promotion. It
reports RAM, total swap, zram, the managed `/swapfile-trading`, swappiness, ZFS
ARC max, and container memory headroom; a 512 MiB legacy `/swapfile` is a
production failure. The installer persists `vm.swappiness=10`, creates managed
zram and disk swapfile systemd units, and caps ZFS ARC at 48 GiB for the 128 GiB
host class. It is idempotent and reversible with
`sudo bash ops/server/memory_pressure_hardening.sh remove`. See
[MEMORY_PRESSURE_RUNBOOK.md](../../docs/MEMORY_PRESSURE_RUNBOOK.md) for the
size rationale, verifier contract, deleted `/tmp` file detector, and reclaim
procedure.

## ZFS Tuning

Run the T2.5 ZFS tuning automation before accepting the Docker data-root move
on `bart`:

```bash
sudo bash ops/server/zfs_tuning.sh apply
bash ops/server/zfs_tuning.sh verify
```

The script enables pool autotrim, disables dataset atime, enforces
`compression=lz4` on every existing dataset under the pool, verifies actual
on-disk ashift with `zdb`, and asserts the dedicated Timescale PGDATA dataset
properties consumed by the T1.3c relocation script. It records before/after
captures and refuses to suggest an in-place ashift repair. See
[DISK_RETENTION_RUNBOOK.md](../../docs/DISK_RETENTION_RUNBOOK.md).

## Disk Remediation

Bootstrap installs the disk-retention remediation entrypoints to
`/opt/trading/ops/server` with root ownership and executable mode:

```bash
sudo bash /opt/trading/ops/server/disk_remediation.sh diagnose
sudo bash /opt/trading/ops/server/disk_remediation.sh relocate-docker --dry-run
sudo bash /opt/trading/ops/server/disk_remediation.sh relocate-docker
```

The source of truth remains the checked-in `ops/server/disk_remediation.sh`.
`verify.sh` checks the deployed remediation and ZFS tuning scripts are present,
executable, and pass `bash -n`. See
[DISK_RETENTION_RUNBOOK.md](../../docs/DISK_RETENTION_RUNBOOK.md) before any
cleanup, rollback, or backup-root relocation command.

## Backup And Restore

Current runtime storage is Postgres-backed. Backup ownership for this host layer is:

- `ops/backup/wal_archive.sh` for continuous WAL archiving into `/var/backups/trading/wal/`
- `ops/backup/wal_archive_catchup.sh` for bounded one-shot catch-up of
  `pg_wal/archive_status/*.ready` segments through the audited archive script
- `ops/backup/base_backup.sh` for scheduled `pg_basebackup` plus `pg_verifybackup`
- `ops/backup/state_snapshot.sh` and `ops/backup/artifact_snapshot.sh` for configuration and artifact evidence
- `ops/backup/prune.sh` for retention pruning
- `ops/backup/restore.sh` and `ops/backup/restore_drill.sh` for clean-target restore verification
- `ops/backup/backup_restore_evidence.sh` for the pre-live and recurring
  evidence gate that verifies installed backup timers, reuses fresh base-backup
  and restore-drill evidence, reasserts/repairs the WAL archive target owner and
  mode, forces bounded WAL archival proof, checks `pg_stat_archiver`, and writes
  `/var/backups/trading/evidence/backup_restore_evidence_<timestamp>.txt`
  plus `latest_backup_restore_evidence.json`

Bootstrap installs the matching `trading-base-backup`, `trading-backup-evidence`, `trading-backup-prune`, and `trading-restore-drill` systemd units and timers. A backup is not considered operationally valid until a restore drill has produced a passing report. `trading-restore-drill.timer` runs weekly with a bounded randomized delay, while `trading-backup-evidence.timer` refreshes WAL archive proof every 60 seconds and reuses fresh base-backup/restore-drill evidence until the configured policy windows expire. Production installs set restore-drill evidence freshness to 604800 seconds (7 days); the evidence script hard default is 1209600 seconds (14 days) when no override is configured.
The timer path is check-only for heavyweight work: it does not run a base
backup, restore drill, or WAL catch-up unless the operator explicitly sets
`TS_BACKUP_EVIDENCE_RUN_BASE_BACKUP=1`,
`TS_BACKUP_EVIDENCE_RUN_RESTORE_DRILL=1`, or
`TS_BACKUP_EVIDENCE_RUN_WAL_CATCHUP=1`. WAL catch-up is deliberately
operator-only during normal timer health checks because a large `.ready` backlog
can consume backup-dataset headroom; repair the archive target/dataset first,
then run `wal_archive_catchup.sh`. Missing, stale, inaccessible, or overdue
evidence is reported as a failed non-zero gate result. The systemd
service is `Type=oneshot` with `TimeoutStartSec=8min` and `Restart=no`, so it must transition
to success or failure instead of remaining in `activating`.
Live preflight reads the latest evidence JSON, the base-backup directory, WAL
archive, and restore-drill reports. In live mode it fails closed when the
latest verified base backup, WAL archive verification, `pg_stat_archiver`
state, restore drill, or restore duration violates the configured
`BACKUP_EVIDENCE_*` policy. Live mode and production preflight with backup
evidence required also require a verifiable HMAC-SHA256 signature on
`latest_backup_restore_evidence.json`.

On an already-bootstrapped host, use the focused installer when only the
backup evidence gate assets need to be deployed:

```bash
sudo TRADING_POSTGRES_VERSION=17 bash ops/server/install_backup_evidence_gate.sh --restart-postgres --run-evidence
```

For this Compose production server, use the Compose-aware path instead:

```bash
sudo bash ops/server/install_backup_evidence_gate.sh --compose --restart-postgres --run-evidence
```

The focused installer deploys the backup scripts, backup evidence timers,
`/var/backups/trading` layout, PostgreSQL archive settings, and the evidence
environment entries consumed by live preflight.
It creates `/etc/trading/backup_evidence.hmac.key` when missing, preserves an
existing key for one-shot evidence generation, installs the matching encrypted
systemd credential as `/etc/credstore.encrypted/backup_evidence_hmac_key.cred`,
and writes `BACKUP_EVIDENCE_REQUIRE_SIGNATURE=1` plus
`BACKUP_EVIDENCE_HMAC_KEY_SECRET=backup_evidence_hmac_key` to
`/etc/trading/trading.env`.
It also writes check-only evidence defaults for the recurring timer:
`TS_BACKUP_EVIDENCE_RUN_BASE_BACKUP=0`,
`TS_BACKUP_EVIDENCE_RUN_RESTORE_DRILL=0`, and
`TS_BACKUP_EVIDENCE_RUN_WAL_CATCHUP=0`.
In `--compose` mode it reads `deploy/compose/.env`, stores only the backup
connection secret in `/etc/trading/provider.env`, runs version-matched
Postgres tools from the Timescale image, and requires a TimescaleDB container
recreate so the WAL archive bind mount and archive command take effect. The
installer also runs the archive command inside the container and then runs
`wal_archive_catchup.sh` once, so existing `.ready` WAL backlog is copied to
the ZFS backup dataset without staging on root. The backup root must be mounted
at `/var/backups/trading` and writable by the container `postgres` UID; on the
Timescale image used by this host that is expected to look like
`2750 70:trading` on the backup root and WAL target directories. The installer
writes `TS_BACKUP_WAL_TARGET_OWNER_UID`, `TS_BACKUP_WAL_TARGET_GROUP`, and
`TS_BACKUP_WAL_TARGET_DIR_MODE=2750` so the recurring evidence gate repairs
wrong owner/mode drift and records the signed `wal_archive_target` diagnosis
artifact. For drift that can break archiving, the diagnosis includes a
pre-repair `wal_archive.sh` probe as the expected archive owner, the observed
failure event/exit code, the current `pg_stat_archiver` failure fields, and the
`chown ...; chmod 2750 ...` fix applied by the gate.
Generated Compose systemd overrides run the evidence service as `root` with
primary group `trading`, set `TS_BACKUP_READ_GROUP=trading`, and publish
evidence as `0640`, so the service can enumerate the `0750 70:trading` backup
tree while non-group readers cannot.

Manual file-backed key creation is supported for explicit one-shot runs. Do not
use this group-readable file as the strict runtime source; production runtime
and preflight should consume the encrypted `backup_evidence_hmac_key`
credential.

```bash
sudo groupadd --system trading 2>/dev/null || true
sudo install -d -o root -g trading -m 0750 /etc/trading
openssl rand -hex 32 | sudo tee /etc/trading/backup_evidence.hmac.key >/dev/null
sudo chown root:trading /etc/trading/backup_evidence.hmac.key
sudo chmod 0640 /etc/trading/backup_evidence.hmac.key
sudo install -d -o root -g root -m 0700 /etc/credstore.encrypted
sudo systemd-creds encrypt --name=backup_evidence_hmac_key \
  /etc/trading/backup_evidence.hmac.key \
  /etc/credstore.encrypted/backup_evidence_hmac_key.cred
sudo chown root:root /etc/credstore.encrypted/backup_evidence_hmac_key.cred
sudo chmod 0400 /etc/credstore.encrypted/backup_evidence_hmac_key.cred
```

Verify the signed evidence path after install or key rotation:

```bash
sudo -u postgres env \
  BACKUP_EVIDENCE_REQUIRE_SIGNATURE=1 \
  BACKUP_EVIDENCE_HMAC_KEY_FILE=/etc/trading/backup_evidence.hmac.key \
  /opt/trading/ops/backup/backup_restore_evidence.sh

sudo bash /opt/trading/app/ops/server/run_prod_preflight.sh
```

For rotation, replace the encrypted `backup_evidence_hmac_key` credential,
restart the backup evidence and preflight/runtime services, run the evidence
job, and keep the old key until the new signed evidence passes preflight.

## Deployment Layout

- App checkout: `/opt/trading/app`
- Python venv: `/opt/trading/venv`
- Server ops scripts: `/opt/trading/ops/server`
- Data: `/var/lib/trading/`
- Backups staging: `/var/backups/trading/`
- Runtime config: `/etc/trading/`
- Encrypted credentials: `/etc/credstore.encrypted/`

No secrets live in this repository. Bootstrap installs the master key,
`ts_ingest`, `ts_app`, and `ts_reader` passwords, the Redis password, the
object-store secret key, the dashboard API token, and the operator API token as
systemd encrypted credentials under `/etc/credstore.encrypted/`; application
units receive only the credentials they declare with `LoadCredentialEncrypted=`.

Required encrypted credential names on the single-server systemd host are:

- `master_key`
- `pg_password_app`
- `pg_password_ingest`
- `pg_password_reader`
- `redis_password`
- `object_store_secret_key`
- `dashboard_api_token`
- `operator_api_token`

Use passwordless dependency URLs in `/etc/trading/trading.env` and point the
runtime at credential names instead of putting secret values in env files. For
example, set `TS_PG_DSN`/`TIMESCALE_DSN` without `password=`, set
`LIVE_CACHE_REDIS_PASSWORD_SECRET=redis_password`, and set
`OBJECT_STORE_SECRET_KEY_SECRET=object_store_secret_key`. For strict production
mutation auth and operator-sidecar access, set
`DASHBOARD_API_TOKEN_SECRET=dashboard_api_token` and
`OPERATOR_API_TOKEN_SECRET=operator_api_token`.

`python engine/runtime/prod_preflight.py --json` validates the credential
directory exposed by systemd through `CREDENTIALS_DIRECTORY`, checks the
Postgres role password credential required by the current process, and verifies
that the absolute runtime data root from `DB_PATH` exists and is readable,
writable, and searchable before schema initialization. Set
`DB_PATH=/var/lib/trading` on production systemd hosts; relative values fail
closed. This preflight is intentionally static: it checks credential files and
permissions without decrypting secret material.

Run the production preflight through systemd so decrypted credentials are
available to the process:

```bash
sudo systemctl start trading-prod-preflight.service
sudo journalctl -u trading-prod-preflight.service -n 200 --no-pager
```

For an ad hoc SSH check that streams the JSON result to the terminal, run:

```bash
sudo /opt/trading/app/ops/server/run_prod_preflight.sh
```

Both paths load `pg_password_app`, `redis_password`,
`object_store_secret_key`, `dashboard_api_token`, and `operator_api_token` with
`LoadCredentialEncrypted=` and run as the `trading` service account. A direct
shell invocation outside systemd is expected to fail in production unless it is
already inside a unit with `CREDENTIALS_DIRECTORY` set. For Compose
deployments, keep Postgres DSNs passwordless and mount the password through
`TIMESCALE_PASSWORD_FILE`/`TS_PG_PASSWORD_FILE`.

## Services

Bootstrap installs and enables `trading.target`, but it does not start the application units. Start after the first app deploy:

```bash
sudo systemctl start trading.target
```

Common operations:

```bash
sudo systemctl restart trading.target
sudo systemctl restart trading-jobs.service
sudo systemctl restart trading-stream-prices.service
sudo systemctl status trading.target
```

Logs are in journald:

```bash
journalctl -u trading-jobs.service -f
journalctl -u trading-stream-prices.service -f
journalctl -u trading-api.service -f
```

PostgreSQL and PgBouncer logs are also rotated from `/var/log/postgresql/`. Application file logs are not configured at this layer; systemd units send stdout and stderr to journald.

## CPU Power Policy

Host `bart` runs the trading runtime as a plugged-in, latency-sensitive
workstation. Bootstrap installs and enables
`trading-cpu-power-policy.service`, which applies the reviewed CPU performance
policy at boot before `trading.target`.

The policy uses `powerprofilesctl set performance` when a non-degraded
performance profile is available, sets AMD EPP sysfs values to `performance`
when present, and falls back to the `performance` governor only when profile and
EPP controls are unavailable. Re-running the service is idempotent.

Agent-runnable verification is read-only:

```bash
bash ops/server/cpu_power_policy.sh verify
```

The output reports power-profiles-daemon profile/degraded state, `amd_pstate`
status, scaling driver, governor, EPP, and `intended_state=PASS` or `FAIL`.
The production target requires the policy service, so starting
`trading.target` through systemd does not silently bypass this boot policy.
`trading-prod-preflight.service` and the bootstrap-generated production env set
`PREFLIGHT_REQUIRE_CPU_POWER_POLICY=1`, so production preflight also reruns the
read-only verifier and fails non-zero on post-boot drift. The recurring
`observability_snapshot` job records the same state as advisory component
health under `cpu_power_policy`; it does not reapply the policy.

This trades higher watts, heat, and fan activity for sustained clocks and lower
scheduling latency. The policy touches only CPU power state and does not set
ROCm/GPU clocks, power limits, or accelerator runtime profiles. Keep GPU
thermal and power limits in the ROCm-specific deployment layer and validate the
combined CPU+GPU thermal envelope during soak. Full revert instructions are in
[../../docs/CPU_POWER_POLICY.md](../../docs/CPU_POWER_POLICY.md).

## Network

`ufw` defaults to deny inbound and allow outbound. Bootstrap allows SSH (`22/tcp`) and the operator UI port (`4001/tcp` by default). Override with:

```bash
sudo TRADING_FIREWALL_UI_PORT=8000 bash ops/server/bootstrap.sh
```

Redis is bound to localhost and exposes `/var/run/redis/trading.sock`. PgBouncer listens on localhost and `/var/run/postgresql/.s.PGSQL.6432`.

## Tests

```bash
python -m pytest -q -m "not requires_rocm" tests/ops
for test_script in tests/ops/*.sh; do
  bash "$test_script"
done
```

GitHub Actions runs the same `tests/ops` discovery gate from
`.github/workflows/validate.yml`: all Python tests under `tests/ops` are
collected with host-only `requires_rocm` tests deselected on standard no-GPU
Linux runners, and every `tests/ops/*.sh` script is executed with `bash`.
Shell tests that need heavyweight local services, such as Docker or PostgreSQL
server binaries, self-skip when those capabilities are not present; fake-binary
and static-lint tests still fail CI on non-zero exit.

The Docker idempotency test uses Debian 12 and skips Python package installation
to avoid downloading the full ML dependency stack.
