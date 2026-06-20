# Canonical Repository

As of 2026-06-20, `/home/david/gitsandbox/system/system` is the canonical
working repository for this project.

`/home/david/gitsandbox/Trading-System-` is legacy/archive-only. Do not use it
for new development. It may contain old source, tracked dependency artifacts,
database files, and historical branches, so it should be archived before any
future removal.

The sibling checkout
`/home/david/gitsandbox/system/system-disk-retention-hardening` is also not the
canonical working tree. Treat it as a temporary or branch-specific checkout
unless it is explicitly promoted later.

## Canonicalization Plan

1. Keep `/home/david/gitsandbox/system/system` as the only active working repo.
2. Preserve `/home/david/gitsandbox/Trading-System-` as a read-only legacy
   archive until its old source and local modifications have been reviewed.
3. Before removing the legacy directory, create a full archive that includes
   `.git`, because the local checkout is behind its remote branch and the
   remote-tracking refs are part of the preservation record.
4. Delete `/home/david/gitsandbox/Trading-System-` only after explicit owner
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
