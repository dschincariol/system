from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from engine.artifacts.fsck import garbage_collect, verify
from engine.artifacts.paths import object_path
from engine.artifacts.store import LocalArtifactStore


def _store(tmp_path: Path) -> tuple[LocalArtifactStore, Path]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    db_path = tmp_path / "artifacts.db"

    def connect():
        return sqlite3.connect(db_path)

    return LocalArtifactStore(root=tmp_path / "store", connect_factory=connect), db_path


def test_fsck_healthy_store_reports_zero_findings(tmp_path: Path) -> None:
    store, _db_path = _store(tmp_path)
    store.put(b"ok", content_type="text/plain", kind="model")

    result = verify(store)

    assert result.ok is True
    assert result.findings == []


def test_fsck_missing_file_size_hash_and_orphan_findings(tmp_path: Path) -> None:
    store, _db_path = _store(tmp_path)
    missing = store.put(b"missing", content_type="text/plain", kind="model")
    store.object_path(missing).unlink()
    assert [f.finding_type for f in verify(store, log_findings=False).findings] == ["missing_object"]

    store, _db_path = _store(tmp_path / "size")
    size = store.put(b"size", content_type="text/plain", kind="model")
    store.object_path(size).write_bytes(b"different-size")
    assert [f.finding_type for f in verify(store, log_findings=False).findings] == ["size_mismatch"]

    store, _db_path = _store(tmp_path / "hash")
    hashed = store.put(b"abcd", content_type="text/plain", kind="model")
    store.object_path(hashed).write_bytes(b"wxyz")
    assert [f.finding_type for f in verify(store, log_findings=False).findings] == ["hash_mismatch"]

    store, _db_path = _store(tmp_path / "orphan")
    orphan_sha = "0" * 64
    orphan_path = object_path(orphan_sha, root=store.root)
    orphan_path.parent.mkdir(parents=True, exist_ok=True)
    orphan_path.write_bytes(b"orphan")
    assert [f.finding_type for f in verify(store, log_findings=False).findings] == ["orphan_object"]


def test_fsck_reports_dangling_alias_when_artifact_row_is_missing(tmp_path: Path) -> None:
    store, db_path = _store(tmp_path)
    ref = store.put(b"aliased", content_type="text/plain", kind="model", alias="model:test:AAPL:current")
    con = sqlite3.connect(db_path)
    try:
        con.execute("DELETE FROM artifacts WHERE sha256=?", (ref.sha256,))
        con.commit()
    finally:
        con.close()

    result = verify(store)
    dangling = [finding for finding in result.findings if finding.finding_type == "dangling_alias"]

    assert len(dangling) == 1
    assert dangling[0].sha256 == ref.sha256
    assert dangling[0].detail["alias"] == "model:test:AAPL:current"
    assert dangling[0].detail["reason"] == "missing_artifact_row"
    con = sqlite3.connect(db_path)
    try:
        assert con.execute(
            "SELECT COUNT(*) FROM artifact_fsck_findings WHERE finding_type='dangling_alias'"
        ).fetchone()[0] == 1
    finally:
        con.close()


def test_gc_respects_ref_count_and_age(tmp_path: Path) -> None:
    store, db_path = _store(tmp_path)
    live = store.put(b"live", content_type="text/plain", kind="model", alias="model:x:AAPL:current")
    young = store.put(b"young", content_type="text/plain", kind="model")
    old = store.put(b"old", content_type="text/plain", kind="model")
    con = sqlite3.connect(db_path)
    try:
        con.execute("UPDATE artifacts SET created_ts='2000-01-01T00:00:00+00:00' WHERE sha256=?", (old.sha256,))
        con.execute("UPDATE artifacts SET created_ts='2000-01-01T00:00:00+00:00' WHERE sha256=?", (live.sha256,))
        con.commit()
    finally:
        con.close()

    result = garbage_collect(store, older_than_days=30)

    assert result.deleted == [old.sha256]
    assert live.sha256 in result.skipped
    assert young.sha256 in result.skipped
    assert not store.object_path(old).exists()
    assert store.object_path(live).exists()
    assert store.object_path(young).exists()
