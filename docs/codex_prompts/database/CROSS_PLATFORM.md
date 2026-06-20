# Linux-Only Platform Policy

This repository supports Linux only. Development, CI, staging, and
production validation are expected to run on a Debian-family Linux host
or Linux container with Python 3.11, Node 20, bash, Postgres/Timescale,
Redis, and POSIX process semantics.

The deployment infrastructure is Linux-specific by design:

- systemd units and `systemd-creds` own service and secret management.
- Postgres defaults use the local Unix socket and PgBouncer port 6432.
- Redis defaults use `unix:///var/run/redis/trading.sock`.
- Runtime data defaults to `/var/lib/trading`.
- Backups default to `/var/backups/trading`.

Do not add platform shims, alternate launchers, or CI jobs for other
operating systems. If a non-Linux workstation is used to edit files,
run the repo inside a Linux VM, container, or remote Linux checkout.

Security-relevant path validation still checks both POSIX and
Windows-style path syntax. That is intentional: a Linux service must
still reject secret names such as drive-qualified paths, backslash
traversal, and parent-directory references before any provider touches
the filesystem.
