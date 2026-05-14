# Codex DB Prompt 09 — Secrets via systemd-creds

You are working in a Python systematic trading system whose
credential-encryption key today lives somewhere on disk, accessible to
any process running as the application user. The encrypted broker API
keys, data-provider keys, and other sensitive material in the
`data_sources` table are only as safe as that key file. Production on
a single Linux server makes this manageable: **systemd's encrypted
credentials** (`systemd-creds`) supply secrets to services at start
time, decrypt them in memory, expose them to the process via a path
under `${CREDENTIALS_DIRECTORY}`, and never write the plaintext to
disk. This is the right primitive for a one-server deployment.

This prompt moves the master encryption key (and the Postgres role
passwords from prompt 01) off raw disk and into systemd-managed
encrypted credentials, with a documented rotation procedure.

## Cross-platform note

The **systemd-creds backend is Linux-only** (correct: it is the
production secret-management mechanism on the staging/prod servers).
The **secrets loader API is cross-platform** via a pluggable
provider model so the same Python code runs on the developer's
Windows machine. Three providers ship:

1. `systemd-creds` — Linux production (this prompt's primary subject).
2. `dpapi` — Windows DPAPI (`win32crypt.CryptProtectData`),
   production-grade for dev.
3. `plaintext` — files with explicit warnings; refuses to start when
   `TS_ENV=production`.

Provider selection is via the `TS_SECRETS_PROVIDER` env var. Read
`docs/codex_prompts/database/CROSS_PLATFORM.md` first.

## Goal

1. Every plaintext secret on disk under `/etc/trading/secrets/`
   migrates to a systemd-creds file under
   `/etc/credstore.encrypted/` **on Linux servers**.
2. Application services declare their required credentials with
   `LoadCredentialEncrypted=` directives in their unit files.
3. The Python code reads secrets exclusively from
   `${CREDENTIALS_DIRECTORY}/<name>` — there is no fallback to the
   old plaintext path.
4. A documented and scripted rotation procedure for both the master
   encryption key and the Postgres role passwords.
5. An audit log of every credential read goes into a new
   `credential_access_log` table.

## Files to read first (read-only)

- `services/credential_encryption.py` — current credential-encryption
  surface. The master-key load path is the thing this prompt
  rewrites.
- `services/data_source_manager.py` — primary consumer of the master
  key.
- `engine/runtime/storage_pg.py` (prompt 02) — current DSN
  construction; the role password it needs comes from
  systemd-creds after this prompt.
- `ops/server/bootstrap.sh` (prompt 01) — installs the credential
  files; this prompt changes that step to use systemd-creds.
- `ops/server/systemd/*.service` (prompt 01) — the service units
  that need `LoadCredentialEncrypted=`.

## Files to create

- `services/secrets/__init__.py`
- `services/secrets/loader.py` — `load_secret(name: str) -> bytes`.
  Dispatches to the provider selected by `TS_SECRETS_PROVIDER`
  (default: `systemd-creds` on Linux, `dpapi` on Windows). Raises a
  typed `SecretNotAvailable` on miss. Logs each successful read to
  `credential_access_log` with `(name, pid, service_name, ts,
  provider)`.
- `services/secrets/providers/__init__.py`
- `services/secrets/providers/systemd_creds.py` — Linux production.
  Reads from `${CREDENTIALS_DIRECTORY}/<name>`.
- `services/secrets/providers/dpapi.py` — Windows. Reads
  `%LOCALAPPDATA%\Trading\secrets\<name>.dpapi`, decrypts via
  `win32crypt.CryptUnprotectData`. The `pywin32` dependency is
  declared as a `windows-dev` extra so Linux installs do not pull
  it.
- `services/secrets/providers/plaintext.py` — Reads
  `${TS_DEV_SECRETS_DIR}/<name>` as raw bytes. Emits a loud
  `RuntimeWarning` at module import. Raises `RuntimeError` if
  `TS_ENV=production` to block accidental production use.
- `services/secrets/rotation.py` — programmatic helpers for
  rotation: re-encrypt all `data_sources` rows from the old key to
  the new, verify, swap.
- `ops/server/credstore/install.sh` — one-shot script: prompts for
  (or accepts via env) each required secret value, runs
  `systemd-creds encrypt --name=<name> - /etc/credstore.encrypted/<name>.cred`,
  sets ownership root:root mode 0400.
- `ops/server/credstore/rotate_master_key.sh` — orchestrates
  rotation:
  1. Generate new master key.
  2. Install it as `master_key.next` via `systemd-creds`.
  3. Run the in-process `rotation.re_encrypt_data_sources(...)` to
     copy every encrypted blob to the new key.
  4. Atomically swap `master_key.next` → `master_key`.
  5. Restart the affected services.
  6. Verify a sample decrypt and emit success / failure.
- `ops/server/credstore/rotate_pg_role.sh <role>` — similar dance
  for Postgres role passwords; updates the PgBouncer userlist
  (prompt 06) and reloads PgBouncer.
- `engine/runtime/schema/migrations/0009_credential_access_log.py`
  — `credential_access_log(id BIGSERIAL PK, ts TIMESTAMPTZ DEFAULT
  now(), name TEXT, pid INTEGER, service_name TEXT, host TEXT,
  ok BOOLEAN, error TEXT NULL)`. Hypertable, 1-week chunks,
  retention 1 year.
- `tests/test_secrets_loader.py` — provider dispatch on each
  platform.
- `tests/test_secrets_provider_systemd.py` — Linux-only marker;
  skipped on Windows.
- `tests/test_secrets_provider_dpapi.py` — Windows-only marker;
  skipped on Linux.
- `tests/test_secrets_provider_plaintext.py` — refuses to load when
  `TS_ENV=production` regardless of platform.
- `tests/test_secrets_rotation.py`
- `tests/test_no_legacy_secret_paths.py` — guard test: no module
  references `/etc/trading/secrets/` (the deprecated plaintext path)
  after this prompt lands.

## Files to modify

- `services/credential_encryption.py` — drop the disk-key read; load
  the master key via `secrets.loader.load_secret("master_key")`.
- `services/data_source_manager.py` — read database role passwords
  via `load_secret(...)` rather than from environment variables or
  a config file.
- `engine/runtime/storage_pg.py` — DSN construction reads the
  Postgres password via `load_secret("pg_password_app")`.
- Every service unit under `ops/server/systemd/` — add
  `LoadCredentialEncrypted=master_key:/etc/credstore.encrypted/master_key.cred`
  and the relevant Postgres role password.
- `ops/server/bootstrap.sh` — call
  `ops/server/credstore/install.sh` instead of writing plaintext
  files.

## Implementation plan

1. **systemd-creds for the master key.** Encrypt the master key with
   the host's TPM if present, otherwise with the host machine-id key.
   `systemd-creds encrypt --tpm2-pcrs=7 - <out>` is the preferred
   form on TPM2 hosts.
2. **Per-service declarations.** Each `*.service` adds explicit
   `LoadCredentialEncrypted=` for only the credentials it needs
   (least privilege). The trading-stream-prices service does not
   load `master_key`, only the Postgres password it needs to insert.
3. **No fallback path.** The loader has no "look in `/etc/trading/secrets/`
   if not found" branch. Old plaintext paths are deleted by the
   bootstrap migration step.
4. **Rotation is offline-safe.** Rotation runs while services are
   running because `data_sources` decryption uses whichever key
   matches the row's `key_version` column. Both old and new keys
   are valid until rotation completes; then the old key is removed.
5. **`credential_access_log` writes are best-effort.** A logging
   failure must not block the credential read (which would deadlock
   startup). Failures go to journald.
6. **Documented procedure.** `docs/Secrets_Rotation_Runbook.md`
   describes the master-key and role-password rotations in five
   numbered steps each.

## Performance targets

- `load_secret(name)` returns in **< 1 ms** under normal conditions
  (the credential file is in tmpfs).
- Master-key rotation completes in **< 30 s** for a `data_sources`
  table containing 50 rows.
- Postgres role-password rotation propagates through PgBouncer
  reload in **< 2 s**.

## Acceptance criteria

- [ ] No module references `/etc/trading/secrets/` after this prompt
      (lint-tested).
- [ ] Every service unit declares the credentials it needs via
      `LoadCredentialEncrypted=`; none reads secrets from a config
      file.
- [ ] Removing a credential from `LoadCredentialEncrypted=` and
      restarting the service produces a clear `SecretNotAvailable`
      at startup, not a silent default.
- [ ] Master-key rotation succeeds end-to-end on a test instance
      with a populated `data_sources` table; old-key decryption is
      disabled at the end.
- [ ] Postgres role-password rotation results in working
      authentication through PgBouncer with no service restart
      needed (PgBouncer reload only).
- [ ] Every credential read (success or failure) appears in
      `credential_access_log`.
- [ ] `docs/Secrets_Rotation_Runbook.md` exists and is the single
      reference for rotation.
- [ ] On Windows, the `dpapi` provider round-trips a secret
      end-to-end (encrypt → store → decrypt → verify) using only
      `pywin32`, no Linux-only dependencies.
- [ ] The `plaintext` provider refuses to import when
      `TS_ENV=production` is set, on any platform.
- [ ] `pywin32` is declared as a `windows-dev` extras dependency,
      not in the base requirements; `pip install` on Linux does not
      attempt to build it.

## Test plan

- `tests/test_secrets_loader.py` — happy path; missing
  `CREDENTIALS_DIRECTORY` raises typed; missing file raises typed;
  successful read logs to the access log.
- `tests/test_secrets_rotation.py` — populate a fake `data_sources`
  with 5 rows under key A; rotate to key B; assert all rows decrypt
  with B and none with A.
- `tests/test_no_legacy_secret_paths.py` — AST scan; no string
  literal contains `/etc/trading/secrets/`.

Run: `pytest -q tests/test_secrets_loader.py tests/test_secrets_rotation.py
tests/test_no_legacy_secret_paths.py`

## Out of scope

- HashiCorp Vault. systemd-creds covers the single-server case at
  near-zero operational cost. Vault becomes worthwhile when there is
  a fleet to authorize against.
- Hardware HSMs for the master key beyond TPM2 binding. TPM2 PCR
  binding is the realistic ceiling for this deployment.
- Per-environment (dev / staging / prod) secret separation.
  Production is one host; dev runs with its own
  `CREDENTIALS_DIRECTORY` pointing at developer-local files.
- Audit-grade access control on credential reads (who is allowed to
  read what). systemd-creds binds credentials to services by unit
  file; that is the access-control surface for now.
