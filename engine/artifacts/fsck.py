"""Filesystem verifier and garbage collector for artifacts."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from engine.artifacts.paths import object_path, validate_sha256
from engine.artifacts.store import LocalArtifactStore, _json_param, _row_value


@dataclass(frozen=True)
class ArtifactFinding:
    finding_type: str
    sha256: str | None = None
    path: str | None = None
    detail: dict[str, Any] = field(default_factory=dict)
    severity: str = "error"


@dataclass(frozen=True)
class FsckResult:
    ok: bool
    findings: list[ArtifactFinding]


@dataclass(frozen=True)
class GcResult:
    deleted: list[str]
    skipped: list[str]


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def verify(store: LocalArtifactStore | None = None, *, log_findings: bool = True) -> FsckResult:
    store = store or LocalArtifactStore()
    findings: list[ArtifactFinding] = []
    with store._connection(readonly=False) as con:
        store._ensure_schema(con)
        rows = con.execute("SELECT sha256, size_bytes FROM artifacts ORDER BY sha256").fetchall() or []
        metadata_hashes = {str(_row_value(row, "sha256", 0, "") or "").lower() for row in rows}
        for row in rows:
            sha = validate_sha256(str(_row_value(row, "sha256", 0, "")))
            expected_size = int(_row_value(row, "size_bytes", 1, 0) or 0)
            path = object_path(sha, root=store.root)
            if not path.exists():
                findings.append(
                    ArtifactFinding("missing_object", sha256=sha, path=str(path), detail={"expected_size": expected_size})
                )
                continue
            actual_size = int(path.stat().st_size)
            if actual_size != expected_size:
                findings.append(
                    ArtifactFinding(
                        "size_mismatch",
                        sha256=sha,
                        path=str(path),
                        detail={"expected_size": expected_size, "actual_size": actual_size},
                    )
                )
                continue
            actual_hash = _hash_file(path)
            if actual_hash != sha:
                findings.append(
                    ArtifactFinding(
                        "hash_mismatch",
                        sha256=sha,
                        path=str(path),
                        detail={"actual_sha256": actual_hash},
                    )
                )
        alias_rows = con.execute(
            """
            SELECT aa.alias, aa.sha256, aa.set_at, a.sha256 AS artifact_sha256
            FROM artifact_aliases aa
            LEFT JOIN artifacts a ON a.sha256 = aa.sha256
            ORDER BY aa.alias, aa.set_at
            """
        ).fetchall() or []
        for row in alias_rows:
            alias = str(_row_value(row, "alias", 0, "") or "")
            raw_sha = str(_row_value(row, "sha256", 1, "") or "").strip().lower()
            set_at = str(_row_value(row, "set_at", 2, "") or "")
            target_sha = str(_row_value(row, "artifact_sha256", 3, "") or "").strip().lower()
            try:
                sha = validate_sha256(raw_sha)
                path = object_path(sha, root=store.root)
            except ValueError:
                findings.append(
                    ArtifactFinding(
                        "dangling_alias",
                        detail={
                            "alias": alias,
                            "target_sha256": raw_sha,
                            "set_at": set_at,
                            "reason": "invalid_sha256",
                        },
                    )
                )
                continue
            if not target_sha:
                findings.append(
                    ArtifactFinding(
                        "dangling_alias",
                        sha256=sha,
                        path=str(path),
                        detail={
                            "alias": alias,
                            "target_sha256": sha,
                            "set_at": set_at,
                            "reason": "missing_artifact_row",
                        },
                    )
                )
            elif not path.exists():
                findings.append(
                    ArtifactFinding(
                        "dangling_alias",
                        sha256=sha,
                        path=str(path),
                        detail={
                            "alias": alias,
                            "target_sha256": sha,
                            "set_at": set_at,
                            "reason": "missing_object_file",
                        },
                    )
                )
        for path in store._all_object_paths():
            sha = path.name.lower()
            try:
                validate_sha256(sha)
            except ValueError:
                continue
            if sha not in metadata_hashes:
                findings.append(ArtifactFinding("orphan_object", sha256=sha, path=str(path)))
        if log_findings:
            checked_at = datetime.now(timezone.utc).isoformat()
            for finding in findings:
                con.execute(
                    """
                    INSERT INTO artifact_fsck_findings(
                      checked_at, severity, finding_type, sha256, path, detail_json
                    )
                    VALUES(?,?,?,?,?,?)
                    """,
                    (
                        checked_at,
                        str(finding.severity),
                        str(finding.finding_type),
                        finding.sha256,
                        finding.path,
                        _json_param(con, dict(finding.detail or {})),
                    ),
                )
        store._commit(con)
    return FsckResult(ok=not findings, findings=findings)


def garbage_collect(
    store: LocalArtifactStore | None = None,
    *,
    older_than_days: int = 30,
    dry_run: bool = False,
) -> GcResult:
    store = store or LocalArtifactStore()
    cutoff = datetime.now(timezone.utc) - timedelta(days=max(0, int(older_than_days)))
    deleted: list[str] = []
    skipped: list[str] = []
    with store._connection(readonly=False) as con:
        store._ensure_schema(con)
        rows = con.execute(
            """
            SELECT sha256, ref_count, created_ts
            FROM artifacts
            ORDER BY created_ts ASC
            """
        ).fetchall() or []
        for row in rows:
            sha = validate_sha256(str(_row_value(row, "sha256", 0, "")))
            ref_count = int(_row_value(row, "ref_count", 1, 0) or 0)
            created_raw = _row_value(row, "created_ts", 2, None)
            try:
                created = created_raw if isinstance(created_raw, datetime) else datetime.fromisoformat(str(created_raw).replace("Z", "+00:00"))
                if created.tzinfo is None:
                    created = created.replace(tzinfo=timezone.utc)
                created = created.astimezone(timezone.utc)
            except Exception:
                created = datetime.now(timezone.utc)
            if ref_count > 0 or created > cutoff:
                skipped.append(sha)
                continue
            path = object_path(sha, root=store.root)
            if not dry_run and path.exists():
                path.unlink()
            deleted.append(sha)
            con.execute(
                """
                INSERT INTO artifact_fsck_findings(
                  checked_at, severity, finding_type, sha256, path, detail_json
                )
                VALUES(?,?,?,?,?,?)
                """,
                (
                    datetime.now(timezone.utc).isoformat(),
                    "info",
                    "gc_deleted" if not dry_run else "gc_would_delete",
                    sha,
                    str(path),
                    _json_param(con, {}),
                ),
            )
        store._commit(con)
    return GcResult(deleted=deleted, skipped=skipped)


__all__ = ["ArtifactFinding", "FsckResult", "GcResult", "garbage_collect", "verify"]
