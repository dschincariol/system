# Production File Manifest

This is the copy policy for the Linux deployment bundle built by
`tools/build_linux_deploy_bundle.ps1`.

## Copy To Linux

- Runtime entrypoints: `start_system.py`, `start_ingestion.py`, `dashboard_server.py`,
  `run_dev.py`, and root compatibility modules used by imports.
- Python application code: `engine/`, `routes/`, `services/`, and production
  operational modules under `ops/`.
- Browser/operator assets: `ui/`, `boot/`, `package.json`, and
  `package-lock.json`.
- Deployment assets: `ops/server/`, `ops/backup/`, `deploy/`, `.dockerignore`,
  `.env.example`, `requirements.txt`, `pyproject.toml`, and `ruff.toml`.
- Static seed/reference data that is not runtime state, including
  `data/model_configs.json`, `data/sec_company_tickers_exchange.json`, and
  `sources_rss.json`.
- Validation and smoke tooling: `tools/` and `tests/`, so Codex can verify the
  server after install without needing the Windows workstation.
- Operator-facing docs needed during deployment, including this file,
  `deploy/LINUX_SERVER_CODEX_DEPLOY.md`, `ops/server/README.md`, and
  `docs/PRODUCTION_CHECKLIST.md`.

## Keep Local On Windows

- Secrets and machine-local configuration: `.env`, `.env.*`,
  `operator.secrets.json`, and generated credential files.
- Python and Node dependency folders: `.venv/`, `venv/`, `env/`, and
  `node_modules/`.
- Runtime state: `logs/`, `logs-*`, `tmp/`, `data-staging/`, `data-isolation/`,
  local SQLite/Postgres scratch files, WAL/SHM files, PID files, and locks.
- Local database and model artifacts that should be recreated or restored on the
  server: `*.db`, `*.sqlite*`, local `models/` contents, local artifact mirrors,
  and local training datasets.
- Developer diagnostics: `pyright*.json`, `pyright*.txt`, `ruff_repo.json`,
  `*_hang.txt`, `*_hang_dump.txt`, import probes, trace files, and one-off test
  logs.
- Git/editor/cache metadata: `.git/`, `.pytest_cache/`, `.ruff_cache/`,
  `__pycache__/`, `.vscode/`, and OS backup files.

The Linux server should own production runtime state under `/var/lib/trading`,
secrets under `/etc/credstore.encrypted` and `/etc/trading`, and app code under
`/opt/trading/app`.
