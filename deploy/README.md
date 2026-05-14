# Deployment Layer

The `deploy/` directory contains installation and service automation for hosted environments.

## Structure

- `bin/`
  Install, backup, service-control, and upgrade scripts.
- `compose/`
  External dependency stack plus runtime/operator compose assets for Timescale/Postgres, Redis, MinIO-style object storage, and the current app deployment path.
- `systemd/`
  Service and timer units.
- [install_trading_system.sh](c:\Users\dschi\Documents\GitHub\Trading-System-\deploy\install_trading_system.sh)
  Main install entrypoint for Linux-style environments.

## Maintenance Guidance

- For a single Linux production server, use the canonical host bootstrap in
  [ops/server/README.md](c:\Users\dschi\Documents\GitHub\Trading-System-\ops\server\README.md).
  Build a filtered mirrorable folder with
  [tools/build_linux_deploy_bundle.ps1](c:\Users\dschi\Documents\GitHub\Trading-System-\tools\build_linux_deploy_bundle.ps1),
  then follow
  [LINUX_SERVER_CODEX_DEPLOY.md](c:\Users\dschi\Documents\GitHub\Trading-System-\deploy\LINUX_SERVER_CODEX_DEPLOY.md).
- Keep deployment scripts aligned with real entrypoints:
  [start_system.py](c:\Users\dschi\Documents\GitHub\Trading-System-\start_system.py), [boot/operator_server.js](c:\Users\dschi\Documents\GitHub\Trading-System-\boot\operator_server.js), and [start_ingestion.py](c:\Users\dschi\Documents\GitHub\Trading-System-\start_ingestion.py).
- If environment variables are added to startup code, update the install scripts, service units, and any deployment wrapper that injects those values.
- Use [compose/docker-compose.external-services.yml](c:\Users\dschi\Documents\GitHub\Trading-System-\deploy\compose\docker-compose.external-services.yml) when you need a local or staging dependency stack for Timescale/Postgres, Redis, and object storage. Copy [compose/.env.example](c:\Users\dschi\Documents\GitHub\Trading-System-\deploy\compose\.env.example) to `.env` in the compose directory and set approved image tags plus credentials before bringing it up.
- Use [compose/docker-compose.stack.yml](c:\Users\dschi\Documents\GitHub\Trading-System-\deploy\compose\docker-compose.stack.yml) together with the external-services file when you want the current runtime and operator deployed as containers. See [compose/README.md](c:\Users\dschi\Documents\GitHub\Trading-System-\deploy\compose\README.md) for the exact command and the operator sidecar constraint in container mode.
