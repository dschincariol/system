# Secrets Rotation Runbook

This is the single rotation reference for the trading server. Production secrets are systemd encrypted credentials in `/etc/credstore.encrypted/`; application code reads them from `${CREDENTIALS_DIRECTORY}` through `services.secrets.loader`.

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
