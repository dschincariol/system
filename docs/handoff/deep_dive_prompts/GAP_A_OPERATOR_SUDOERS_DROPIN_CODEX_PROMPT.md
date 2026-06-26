# Codex Deep-Dive — Gap A: Operator service-control sudoers drop-in (missing + path-mismatch)

## Role
You are a senior reliability engineer hardening the production deployment of this single-box trading system. Design and implement the optimal, least-privilege fix for the operator's privileged service-control path. Build on the existing architecture; do not introduce a parallel mechanism.

## Background (verified on the production host)
The app runs as system user `trading` via systemd units `trading-engine.service` and `trading-operator.service`. The operator web UI (`boot/operator_server.js`) exposes Start / Stop / Restart / Emergency-Stop / logs controls that bridge through `deploy/bin/service_ctl.sh`, which executes `sudo -n systemctl <action> <unit>` and `sudo -n journalctl …` / `sudo -n tail …`. That requires the sudoers drop-in `/etc/sudoers.d/trading-system`, written by `deploy/install_trading_system.sh` (the NOPASSWD block at lines ~286-312), granting the `trading` user passwordless, unit-scoped systemctl/journalctl/tail.

Two defects:
1. **Missing on the host.** `/etc/sudoers.d/trading-system` does not exist on the production box, so every operator-UI service-control and log action fails closed at `service_ctl.sh` (`require_sudo` → `sudo_non_interactive_required`, lines ~49-54). The operator UI's own Start/Stop/Emergency-Stop buttons are therefore broken.
2. **Binary-path mismatch even when present.** The drop-in authorizes `/bin/systemctl` and `/bin/journalctl`, but this host's canonical binary is `/usr/bin/systemctl` (with `/bin` a usrmerge symlink to `/usr/bin`). `service_ctl.sh` invokes `systemctl` via `$PATH`, which resolves to `/usr/bin/systemctl`; sudo matches the *resolved* path, so a `/bin/systemctl`-only NOPASSWD rule does not match and sudo denies. (The `tail` rules already list both `/bin` and `/usr/bin`; the `systemctl`/`journalctl` rules do not.)

## Objective
Make the operator's privileged service-control + log-read path reliable across usrmerge and non-usrmerge hosts, least-privilege, installed idempotently, validated, and self-detecting when broken.

## Requirements (acceptance criteria)
1. **Path correctness.** Ensure the invoked binary always matches an authorized sudoers entry. Either (a) list both `/usr/bin/systemctl` and `/bin/systemctl` (and both `journalctl` paths), or (b) make `service_ctl.sh` invoke a single resolved absolute path that the drop-in matches exactly. Keep scoping to the exact units and actions already supported — no new wildcards, no `ALL`.
2. **Validation at install.** Validate the generated drop-in with `visudo -cf` before activating it; abort install (non-zero) on invalid syntax. Never leave a half-written `/etc/sudoers.d/*` file.
3. **Idempotent (re)install + repair.** Provide a way to (re)install/repair just this drop-in without a full reinstall (an installer flag or a `deploy/bin` subcommand), and make it idempotent. Detect drift (missing/mismatched content) and repair it.
4. **Runtime fail-loud, not fail-silent.** Add a production preflight/health signal that probes the privileged path (e.g. `sudo -n systemctl is-active <unit>`) and surfaces a clear, operator-actionable degraded state when the drop-in is missing/mismatched — instead of an opaque per-action failure. Map `service_ctl.sh`'s `sudo_non_interactive_required` to actionable operator-UI copy and a preflight gate.
5. **Enforced in production code/config**, not only docs/tests: the installer writes the correct drop-in, and a runtime check verifies it.

## Constraints
- Least privilege: scope strictly to the two units and the existing actions; grant the `trading` user only. Do not grant the desktop user `david` anything here (the desktop Start/Stop button uses a separate, already-installed polkit rule).
- Preserve operator route conventions (snake_case canonical, camelCase aliases share handlers) and the structured-confirmation gates for high-impact routes.
- Do not weaken the existing fail-closed posture on missing tokens or live-mode guards.

## Pointers
- `deploy/install_trading_system.sh` (sudoers heredoc ~286-312)
- `deploy/bin/service_ctl.sh` (`require_sudo` ~49-54; `map_unit`/`map_logfile`; action dispatch ~104-119)
- `boot/operator_server.js` (SERVICE_CTL invocation ~2454-2490; service-control + logs routes)
- `boot/README.md`, `docs/README_OPERATOR_GUIDE.md`, `deploy/README.md`

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.
