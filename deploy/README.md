# Deployment Layer

The `deploy/` directory contains Linux-only installation and service automation for hosted environments.

## Structure

- `bin/`
  Install, legacy SQLite backup, service-control, and upgrade scripts.
- `compose/`
  External dependency stack plus runtime/operator compose assets for Timescale/Postgres, Redis, MinIO-style object storage, and the current app deployment path.
- `systemd/`
  Service and timer units.
- [install_trading_system.sh](install_trading_system.sh)
  Main install entrypoint for Linux-style environments.

## Maintenance Guidance

- For a single Linux production server, use the canonical host bootstrap in
  [ops/server/README.md](../ops/server/README.md), then follow
  [LINUX_SERVER_CODEX_DEPLOY.md](LINUX_SERVER_CODEX_DEPLOY.md).
- Keep deployment scripts aligned with real entrypoints:
  [start_system.py](../start_system.py), [boot/operator_server.js](../boot/operator_server.js), and [start_ingestion.py](../start_ingestion.py).
- If environment variables are added to startup code, update the install scripts, service units, and any deployment wrapper that injects those values.
- Production/live deployments must inject generated `DASHBOARD_API_TOKEN` and `OPERATOR_API_TOKEN` values. The Python runtime fails closed on missing, placeholder, or too-short dashboard mutation tokens, including same-origin `/operator/api/*` bridge mutations, and the no-token localhost mutation fallback is dev-only. The operator sidecar does not trust loopback for protected reads, writes, or WebSocket upgrades; the dashboard bridge forwards the server-side operator token only after dashboard auth passes.
- Production/live deployments must set `DB_PATH` to an absolute local data root such as `/var/lib/trading` or `/app/data` inside a container. `DB_PATH` is not a SQLite control-plane database target; relative values fail runtime config and production preflight.
- Use [compose/docker-compose.external-services.yml](compose/docker-compose.external-services.yml) when you need a local or staging dependency stack for Timescale/Postgres, Redis, and object storage. Copy [compose/.env.example](compose/.env.example) to `.env` in the compose directory and set approved image tags plus credentials before bringing it up.
- Use [compose/docker-compose.stack.yml](compose/docker-compose.stack.yml) together with the external-services file when you want the current runtime and operator deployed as containers. The operator sidecar is internal-only by default; see [compose/README.md](compose/README.md) for the exact command and the operator sidecar constraint in container mode.
- Use [../ops/backup](../ops/backup) and [../ops/server/systemd](../ops/server/systemd) for current Postgres base backups, WAL archive, pruning, and restore drills. [bin/backup_trading_db.sh](bin/backup_trading_db.sh) is retained for legacy/local SQLite-file backups only; it fails in production/live profiles and is not a production recovery plan for the Postgres-backed runtime.
- Use [../docs/DISK_RETENTION_RUNBOOK.md](../docs/DISK_RETENTION_RUNBOOK.md) before responding to root filesystem or Docker storage pressure. It separates safe Docker cache/image pruning from destructive live volume or backup deletion and documents the backup accounting command.
