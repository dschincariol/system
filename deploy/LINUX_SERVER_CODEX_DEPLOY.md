# Linux Server Codex Deploy Prompt

Use this after the Windows workstation has built and mirrored `dist/linux-server`
to the Linux host.

## Windows Bundle Step

From the Windows repo root:

```powershell
powershell -ExecutionPolicy Bypass -File tools/build_linux_deploy_bundle.ps1
```

Mirror the resulting `dist/linux-server/` directory to the Linux server with the
transfer method you normally use. Do not transfer `.env`, `.venv`, `node_modules`,
logs, local databases, or `tmp`.

## Prompt To Give Codex On The Linux Server

```text
You are on the production Linux server. The trading system deployment bundle is
already on this machine at: /path/to/linux-server

Goal: install and start the trading system in safe mode only. Do not enable live
trading, do not add real broker credentials, and do not change provider toggles
unless I explicitly provide values.

Use this exact deployment path:
1. cd /path/to/linux-server
2. Confirm the OS is Ubuntu 22.04 LTS or Debian 12.
3. Run sudo bash ops/server/bootstrap.sh.
4. Run sudo bash ops/server/verify.sh.
5. Start the app with sudo systemctl start trading.target.
6. Check sudo systemctl status trading.target --no-pager.
7. Check sudo systemctl status trading-jobs.service trading-api.service trading-ingest.service --no-pager.
8. Check recent logs with sudo journalctl -u trading-jobs.service -u trading-api.service -u trading-ingest.service -n 200 --no-pager.
9. Probe http://127.0.0.1:8000/api/readiness and http://127.0.0.1:4001/api/operator/status from the server.

If any step fails, stop and fix the specific failing deployment asset or config.
Report exactly what changed, the status of each service, and whether readiness
passed. Keep ENGINE_MODE=safe and EXECUTION_MODE=safe.
```

## Expected Installed Layout

- App code: `/opt/trading/app`
- Virtualenv: `/opt/trading/venv`
- Runtime data: `/var/lib/trading`
- Backups: `/var/backups/trading`
- Runtime env: `/etc/trading`
- Encrypted systemd credentials: `/etc/credstore.encrypted`
