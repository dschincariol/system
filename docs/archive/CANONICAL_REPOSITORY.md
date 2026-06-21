# Canonical Repository

As of 2026-06-21, `/home/david/gitsandbox/system/system` is the canonical
working repository for this project.

`/home/david/gitsandbox/Trading-System-` is legacy/archive-only. Do not use it
for new development. It may contain old source, tracked dependency artifacts,
database files, and historical branches, so it should be archived before any
future removal.

The former sibling path
`/home/david/gitsandbox/system/system-disk-retention-hardening` is not the
canonical working tree and was removed on 2026-06-21 after preservation and
triage. If that path ever reappears, it must be a registered Git worktree owned
by the canonical repository, not a loose source copy. `tools/validate_repo.py`
enforces that rule by failing when the sibling exists outside `git worktree
list` or contains Git-visible tracked/untracked changes.

## R.10 Duplicate Worktree Triage

The 2026-06-21 R.10 pass found:

- `system/system` is the canonical repo, currently on
  `codex/worktree-production-readiness`.
- `system/system-disk-retention-hardening` was already registered as a Git
  worktree, originally on `codex/disk-retention-hardening`.
- The committed disk-retention work is preserved on
  `codex/disk-retention-hardening` at `2c5e739`.
- The duplicate's 39 Git-visible uncommitted files were preserved on
  `codex/preserve-disk-retention-hardening-loose-state` at `13fe1bb`.
- The duplicate tree's disk bulk was ignored/generated local state, primarily
  `.venv-rocm/` and `.venv-rocm70-test/`.
- Before removal, local-only state that could contain non-reproducible values
  was archived outside the repo at
  `/home/david/gitsandbox/system/r10_duplicate_worktree_preservation_20260621/`.
  The archive contains `.env`, the empty `models/` directory structure, small
  SQLite/preflight DBs, and logs. The same directory contains `MANIFEST.txt`
  and `SHA256SUMS.txt`.
- Reproducible/generated state was discarded during removal, including
  `.venv-rocm/`, `.venv-rocm70-test/`, caches, `__pycache__/`, and
  `trading_system.egg-info/`.
- The guarded removal was executed from the canonical repo with:

```bash
python tools/git_worktree_triage.py --remove-duplicate --execute-removal --allow-ignored-local-state
```

That command ran:

```bash
git worktree remove --force /home/david/gitsandbox/system/system-disk-retention-hardening
```

Post-removal verification on 2026-06-21 showed `git worktree list --porcelain`
contains only `/home/david/gitsandbox/system/system`, `test ! -e
/home/david/gitsandbox/system/system-disk-retention-hardening` exits 0, and
`python tools/git_worktree_triage.py` reports `duplicate.exists=false` with no
layout violations.

If a replacement sibling worktree is intentionally created later, run:

```bash
cd /home/david/gitsandbox/system/system
python tools/git_worktree_triage.py
python tools/git_worktree_triage.py --remove-duplicate
```

The second command is the repo-owned dry-run default because this host's Git
does not support `git worktree remove --dry-run`. Actual removal requires an
explicit execute flag and should only be used after local-only state such as
`.env`, SQLite DBs, logs, caches, and virtualenvs has either been preserved
outside the repo or classified as disposable:

```bash
cd /home/david/gitsandbox/system/system
python tools/git_worktree_triage.py --remove-duplicate --execute-removal --allow-ignored-local-state
```

Equivalent low-level Git command after the same confirmation:

```bash
git worktree remove --force /home/david/gitsandbox/system/system-disk-retention-hardening
```

## Canonicalization Plan

1. Keep `/home/david/gitsandbox/system/system` as the only active working repo.
2. Preserve `/home/david/gitsandbox/Trading-System-` as a read-only legacy
   archive until its old source and local modifications have been reviewed.
3. Keep any branch-specific checkout under `git worktree list`; never create a
   multi-GB loose sibling copy of the repo.
4. Before removing the legacy directory, create a full archive that includes
   `.git`, because the local checkout is behind its remote branch and the
   remote-tracking refs are part of the preservation record.
5. Delete `/home/david/gitsandbox/Trading-System-` only after explicit owner
   confirmation.

## Safe Later Archive/Removal Commands

Run these only after confirming the legacy repo is no longer needed:

```bash
cd /home/david/gitsandbox/Trading-System-
git fetch --all --prune
git status --short --branch
git log --oneline @{u}..HEAD
git log --oneline HEAD..@{u}
cd /home/david/gitsandbox
archive="Trading-System--legacy-archive-$(date +%Y%m%d).tar.gz"
tar -czf "$archive" Trading-System-
sha256sum "$archive"
mv Trading-System- Trading-System-.archive-pending-delete
```

After a separate confirmation that the archive is valid and the renamed
directory is no longer needed:

```bash
rm -rf /home/david/gitsandbox/Trading-System-.archive-pending-delete
```
