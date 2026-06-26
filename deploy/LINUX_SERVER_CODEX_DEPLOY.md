# Linux Server Codex Deploy Prompt

Use this from a Linux checkout or from a Linux-built deployment mirror.
Non-Linux bundle creation and transfer workflows are not supported.

## Prepare The Linux Source

Mirror a clean Linux checkout or filtered source directory to the Linux server
with your normal Linux transfer tooling. Do not transfer `.env`, `.venv`,
`node_modules`, logs, local databases, or `tmp`.

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
9. Probe `http://127.0.0.1:8000/api/readiness` with `X-API-Token` and `http://127.0.0.1:4001/api/operator/status` with `X-Operator-Token` from the server.
10. If using the `deploy/systemd/trading-engine.service` and `deploy/systemd/trading-operator.service` units, run `sudo systemctl daemon-reload && sudo systemctl restart trading-engine trading-operator` after copying them, then verify `systemctl show trading-engine trading-operator -p WatchdogUSec -p MemoryMax -p OOMScoreAdjust -p TimeoutStartUSec` shows finite watchdog values, the documented memory limits, and the 5-minute startup timeout.

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
