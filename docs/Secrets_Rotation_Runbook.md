# Secrets Rotation Runbook

This is the single rotation reference for the trading server. Production secrets are systemd encrypted credentials in `/etc/credstore.encrypted/`, Docker Compose secrets, or root/service-owned files referenced by `*_FILE` variables. Application code reads systemd credentials from `${CREDENTIALS_DIRECTORY}` through `services.secrets.loader` and reads file-backed compose secrets from the configured `*_FILE` paths.

Strict production, supervised, and live runtimes reject inline secret values in process env and repo-local `.env` files when a file/provider source exists. Rotate any credential that was ever pasted into `.env`, `deploy/compose/.env`, `deploy/env/trading.env`, shell history, CI logs, or support bundles.

## File-Backed Compose Secret Rotation

Use this for secrets referenced by `DASHBOARD_API_TOKEN_FILE`,
`OPERATOR_API_TOKEN_FILE`, `TIMESCALE_PASSWORD_FILE`, `REDIS_PASSWORD_FILE`,
`MINIO_ROOT_USER_FILE`, `MINIO_ROOT_PASSWORD_FILE`, `POLYGON_API_KEY_FILE`,
`TRADIER_API_TOKEN_FILE`, `ALPACA_KEY_ID_FILE`, `ALPACA_SECRET_KEY_FILE`, and
similar file-backed entries.

1. Generate or obtain the replacement value from the backing service/provider.
2. Write it to a new file outside the repo checkout:
   ```bash
   sudo install -o root -g trading -m 0600 /dev/null /etc/trading/secrets/name.next
   printf '%s' "$NEW_SECRET_VALUE" | sudo tee /etc/trading/secrets/name.next >/dev/null
   sudo chmod 0600 /etc/trading/secrets/name.next
   ```
3. For database, Redis, MinIO/object-store, broker, and provider credentials, rotate the backing service/provider credential first, then atomically replace the file path target or encrypted systemd credential:
   ```bash
   sudo mv /etc/trading/secrets/name.next /etc/trading/secrets/name
   ```
4. Recreate containers or restart systemd units that consume the file so Docker secrets and process-level caches reload:
   ```bash
   docker compose --env-file deploy/compose/.env \
     -f deploy/compose/docker-compose.external-services.yml \
     -f deploy/compose/docker-compose.stack.yml up -d --force-recreate runtime operator
   ```
5. Run the production preflight and relevant readiness probe. Do not delete the old credential from the provider until the new runtime path passes. On systemd hosts, object-store access and secret keys should be exposed as `OBJECT_STORE_ACCESS_KEY_SECRET=object_store_access_key` and `OBJECT_STORE_SECRET_KEY_SECRET=object_store_secret_key`; do not leave either value inline in `/etc/trading/trading.env`.
6. Remove inline leftovers from repo-local env files and rerun:
   ```bash
   python - <<'PY'
   from engine.runtime.secret_sources import repo_local_secret_key_inventory
   import json
   print(json.dumps(repo_local_secret_key_inventory(), indent=2, sort_keys=True))
   PY
   ```

## Master Key Rotation

The active master key material must be canonical base64 text for exactly 32
random bytes. Generate new key material with:

```bash
openssl rand -base64 32
```

For file-based deployments, create the file with restrictive permissions:

```bash
sudo install -o trading -g trading -m 0600 /dev/null /var/lib/trading/.data_source_master_key
sudo -u trading sh -c 'openssl rand -base64 32 > /var/lib/trading/.data_source_master_key'
```

For systemd encrypted-credential deployments, use the scripted rotation below;
it generates the same base64 32-byte material and installs it as
`master_key.next`.

1. Confirm the current host is healthy:
   ```bash
   sudo /opt/trading/app/ops/server/verify.sh
   sudo systemctl status trading-jobs.service trading-ingest.service
   ```
2. Start the rotation:
   ```bash
   sudo bash /opt/trading/app/ops/server/credstore/rotate_master_key.sh
   ```
3. Watch the three phases in the script output:
   - `phase_1_reencrypt` creates `master_key.next.cred` and re-encrypts data-source credential rows with the next key. A failure exits `1` and leaves both `master_key.cred` and `master_key.next.cred` intact for investigation or retry.
   - `phase_2_verify` decrypts the rotated rows with `master_key.next` before any credential file swap. A failure exits `2` and also leaves both credential files intact.
   - `phase_3_swap_and_cleanup` atomically swaps `master_key.next.cred` into `master_key.cred`, verifies the live `master_key.cred`, purges or archives the previous key, prunes expired archives, and then restarts services. A failure in this phase exits `3` with a `PANIC` log line and requires operator escalation because the active key may already have changed.
4. Confirm credential reads and data-source decrypts:
   ```bash
   sudo journalctl -u trading-jobs.service -u trading-ingest.service -n 200 --no-pager
   ```
5. Confirm cleanup:
   - `/etc/credstore.encrypted/master_key.next.cred` must not exist after a successful run.
   - The previous encrypted master key is purged by default after the live-key verification succeeds. If a rollback grace period is required, set `TRADING_MASTER_KEY_ARCHIVE_RETENTION_HOURS` to a positive value before rotation; the old key is then stored under `/etc/credstore.encrypted/keys/archive/` with mode `0400`, owned by root.
   - Prune archived keys from cron or a systemd timer with the same retention:
     ```bash
     sudo TRADING_MASTER_KEY_ARCHIVE_RETENTION_HOURS=72 bash /opt/trading/app/ops/server/credstore/prune_archive.sh
     ```
   - A remaining `master_key.next.cred` means the rotation stopped before phase 3 and should be investigated before retrying.

## File-Based Master Key Rotation

Use this only for deployments that set `DATA_SOURCE_MASTER_KEY_FILE` directly
instead of loading `master_key` from systemd credentials. Keep a copy of the old
key until every encrypted data-source row is verified under the new key.

```bash
sudo install -o trading -g trading -m 0600 /dev/null /var/lib/trading/master_key.next
sudo -u trading sh -c 'openssl rand -base64 32 > /var/lib/trading/master_key.next'
sudo -u trading DATA_SOURCE_MASTER_KEY_FILE=/var/lib/trading/.data_source_master_key \
  TS_SECRETS_PROVIDER=plaintext TS_DEV_SECRETS_DIR=/var/lib/trading \
  python -c 'from services.secrets.rotation import re_encrypt_data_sources; print(re_encrypt_data_sources(old_key_name="master_key", new_key_name="master_key.next", final_key_version="master_key", delete_old_key=False))'
sudo install -o trading -g trading -m 0600 /var/lib/trading/master_key.next /var/lib/trading/.data_source_master_key
sudo -u trading DATA_SOURCE_MASTER_KEY_FILE=/var/lib/trading/.data_source_master_key \
  python -c 'from services.secrets.rotation import verify_data_sources_key; print({"verified": verify_data_sources_key(new_key_name="master_key")})'
sudo rm -f /var/lib/trading/master_key.next
```

## Backup Evidence HMAC Key Rotation

Systemd production hosts use `BACKUP_EVIDENCE_HMAC_KEY_SECRET=backup_evidence_hmac_key`
and load `/etc/credstore.encrypted/backup_evidence_hmac_key.cred` into both the
backup-evidence timer and production preflight. Install or replace that
credential with:

```bash
tmp="$(mktemp)"
openssl rand -hex 32 > "$tmp"
sudo install -d -o root -g root -m 0700 /etc/credstore.encrypted
sudo systemd-creds encrypt --name=backup_evidence_hmac_key \
  "$tmp" /etc/credstore.encrypted/backup_evidence_hmac_key.cred
rm -f "$tmp"
sudo chown root:root /etc/credstore.encrypted/backup_evidence_hmac_key.cred
sudo chmod 0400 /etc/credstore.encrypted/backup_evidence_hmac_key.cred
sudo systemctl restart trading-backup-evidence.service trading-prod-preflight.service
```

Compose deployments rotate the file referenced by
`BACKUP_EVIDENCE_HMAC_KEY_FILE` and then recreate the services that mount the
Docker secret. Direct non-Compose file sources must be readable by the service
process and mode `0600`; do not use group-readable runtime secret files.

## Postgres Role Password Rotation

1. Pick one role: `app`, `ingest`, or `reader`.
2. Rotate the password:
   ```bash
   sudo bash /opt/trading/app/ops/server/credstore/rotate_pg_role.sh app
   ```
3. The script changes the PostgreSQL role password, replaces the matching encrypted credential (`pg_password_app`, `pg_password_ingest`, or `pg_password_reader`), regenerates the PgBouncer userlist from PostgreSQL SCRAM verifiers, and reloads PgBouncer.
4. Verify PgBouncer authentication:
   ```bash
   sudo /opt/trading/app/ops/server/verify.sh
   ```
5. Check the affected service logs for reconnect failures. Existing pooled connections normally continue; if a service has already lost every PgBouncer connection and cannot reconnect with its start-time credential copy, restart only that service.
