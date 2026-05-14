from __future__ import annotations

import hashlib
import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from engine.artifacts.paths import object_path
from engine.artifacts.store import ArtifactCorruption, LocalArtifactStore


def _store(tmp_path: Path) -> tuple[LocalArtifactStore, Path]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    db_path = tmp_path / "artifacts.db"

    def connect():
        return sqlite3.connect(db_path)

    return LocalArtifactStore(root=tmp_path / "store", connect_factory=connect), db_path


def test_put_get_idempotent_and_sharded_layout(tmp_path: Path) -> None:
    store, db_path = _store(tmp_path)

    ref1 = store.put(b"alpha", content_type="text/plain", kind="model")
    ref2 = store.put(b"alpha", content_type="text/plain", kind="model")

    assert ref1.sha256 == ref2.sha256
    assert store.get_bytes(ref1) == b"alpha"
    expected_path = object_path(ref1.sha256, root=tmp_path / "store")
    assert expected_path.exists()
    assert expected_path.parent.name == ref1.sha256[2:4]
    assert expected_path.parent.parent.name == ref1.sha256[:2]
    assert len(list((tmp_path / "store" / "objects").glob("*/*/*"))) == 1

    con = sqlite3.connect(db_path)
    try:
        assert con.execute("SELECT COUNT(*) FROM artifacts").fetchone()[0] == 1
    finally:
        con.close()


def test_get_bytes_verifies_hash_and_reports_corruption(tmp_path: Path) -> None:
    store, _db_path = _store(tmp_path)
    ref = store.put(b"alpha", content_type="text/plain", kind="model")
    corrupted = b"bravo"
    store.object_path(ref).write_bytes(corrupted)

    with pytest.raises(ArtifactCorruption) as exc_info:
        store.get_bytes(ref)

    err = exc_info.value
    assert err.expected_sha256 == ref.sha256
    assert err.actual_sha256 == hashlib.sha256(corrupted).hexdigest()
    assert err.path == store.object_path(ref)
    assert store.get_bytes(ref, verify=False) == corrupted
    with pytest.raises(ArtifactCorruption):
        store.open(ref)
    with store.open(ref, verify=False) as handle:
        assert handle.read() == corrupted


def test_put_path_streams_file(tmp_path: Path) -> None:
    store, _db_path = _store(tmp_path)
    source = tmp_path / "large.bin"
    payload = (b"0123456789abcdef" * 8192) + b"tail"
    source.write_bytes(payload)

    ref = store.put_path(
        source,
        content_type="application/octet-stream",
        kind="model",
        alias="model:test:AAPL:current",
        metadata={"source": "unit"},
    )

    assert ref.size == len(payload)
    with store.open(ref) as handle:
        assert handle.read() == payload
    assert store.resolve("model:test:AAPL:current").sha256 == ref.sha256


def test_postgres_runtime_schema_is_migration_owned(tmp_path: Path) -> None:
    class PgLikeConn:
        __module__ = "engine.runtime.storage_pg"

        def __init__(self) -> None:
            self.statements: list[str] = []

        def execute(self, sql: str, params=None):
            del params
            self.statements.append(str(sql))
            return self

        def commit(self) -> None:
            pass

        def close(self) -> None:
            pass

    conn = PgLikeConn()
    LocalArtifactStore(root=tmp_path / "store", connect_factory=lambda: conn)

    assert conn.statements == []
