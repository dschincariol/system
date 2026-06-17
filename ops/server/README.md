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

On Windows, build the mirrorable bundle first:

```powershell
powershell -ExecutionPolicy Bypass -File tools/build_linux_deploy_bundle.ps1
```

Then mirror `dist/linux-server/` to the Linux host and follow
`deploy/LINUX_SERVER_CODEX_DEPLOY.md`.

Heavy Python dependency installation can be skipped for infrastructure-only test runs:

```bash
sudo TRADING_INSTALL_PYTHON_REQUIREMENTS=0 bash ops/server/bootstrap.sh
```

## Verify

```bash
sudo bash ops/server/verify.sh
```

The verifier checks PostgreSQL, TimescaleDB, Redis over its Unix socket, PgBouncer over `/var/run/postgresql/.s.PGSQL.6432`, filesystem ownership, and systemd unit syntax.

## Backup And Restore

Current runtime storage is Postgres-backed. Backup ownership for this host layer is:

- `ops/backup/wal_archive.sh` for continuous WAL archiving into `/var/backups/trading/wal/`
- `ops/backup/base_backup.sh` for scheduled `pg_basebackup` plus `pg_verifybackup`
- `ops/backup/state_snapshot.sh` and `ops/backup/artifact_snapshot.sh` for configuration and artifact evidence
- `ops/backup/prune.sh` for retention pruning
- `ops/backup/restore.sh` and `ops/backup/restore_drill.sh` for clean-target restore verification
- `ops/backup/backup_restore_evidence.sh` for the pre-live evidence run that
  verifies installed backup timers, performs a base backup, forces WAL
  archival, runs a clean-target restore drill, and writes
  `/var/backups/trading/evidence/backup_restore_evidence_<timestamp>.txt`
  plus `latest_backup_restore_evidence.json`

Bootstrap installs the matching `trading-base-backup`, `trading-backup-evidence`, `trading-backup-prune`, and `trading-restore-drill` systemd units and timers. A backup is not considered operationally valid until a restore drill has produced a passing report. `trading-backup-evidence.timer` refreshes WAL archive proof every 60 seconds and reuses fresh base-backup/restore-drill evidence until the configured policy windows expire.
Live preflight reads the latest evidence JSON, the base-backup directory, WAL
archive, and restore-drill reports. In live mode it fails closed when the
latest verified base backup, WAL archive verification, restore drill, or
restore duration violates the configured `BACKUP_EVIDENCE_*` policy.

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
In `--compose` mode it reads `deploy/compose/.env`, stores only the backup
connection secret in `/etc/trading/provider.env`, runs version-matched
Postgres tools from the Timescale image, and requires a TimescaleDB container
recreate so the WAL archive bind mount and archive command take effect.

## Deployment Layout

- App checkout: `/opt/trading/app`
- Python venv: `/opt/trading/venv`
- Data: `/var/lib/trading/`
- Backups staging: `/var/backups/trading/`
- Runtime config: `/etc/trading/`
- Encrypted credentials: `/etc/credstore.encrypted/`

No secrets live in this repository. Bootstrap installs the master key and
`ts_ingest`, `ts_app`, and `ts_reader` passwords as systemd encrypted
credentials under `/etc/credstore.encrypted/`; application units receive only
the credentials they declare with `LoadCredentialEncrypted=`.

Required encrypted credential names on the single-server systemd host are:

- `master_key`
- `pg_password_app`
- `pg_password_ingest`
- `pg_password_reader`

`python engine/runtime/prod_preflight.py --json` validates the credential
directory exposed by systemd through `CREDENTIALS_DIRECTORY`, checks the
Postgres role password credential required by the current process, and verifies
that the runtime data root from `DB_PATH` exists and is readable, writable, and
searchable before schema initialization. This preflight is intentionally static:
it checks credential files and permissions without decrypting secret material.
For Compose deployments, the equivalent Postgres bootstrap secret is the
`password=` value already present in `TS_PG_DSN` inside the runtime container.

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

## Network

`ufw` defaults to deny inbound and allow outbound. Bootstrap allows SSH (`22/tcp`) and the operator UI port (`4001/tcp` by default). Override with:

```bash
sudo TRADING_FIREWALL_UI_PORT=8000 bash ops/server/bootstrap.sh
```

Redis is bound to localhost and exposes `/var/run/redis/trading.sock`. PgBouncer listens on localhost and `/var/run/postgresql/.s.PGSQL.6432`.

## Tests

```bash
bash tests/ops/test_bootstrap_idempotent.sh
bash tests/ops/test_systemd_units_lint.sh
```

The Docker idempotency test uses Debian 12 and skips Python package installation to avoid downloading the full ML dependency stack.
