from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from engine.artifacts.store import LocalArtifactStore


def _store(tmp_path: Path) -> tuple[LocalArtifactStore, Path]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    db_path = tmp_path / "artifacts.db"

    def connect():
        return sqlite3.connect(db_path)

    return LocalArtifactStore(root=tmp_path / "store", connect_factory=connect), db_path


def test_alias_history_and_ref_counts(tmp_path: Path) -> None:
    store, db_path = _store(tmp_path)
    alias = "model:temporal_predictor:AAPL:current"
    first = store.put(b"first", content_type="application/octet-stream", kind="model", alias=alias)
    second = store.put(b"second", content_type="application/octet-stream", kind="model", alias=alias)

    assert store.resolve(alias).sha256 == second.sha256
    assert [ref.sha256 for ref in store.list_versions(alias)] == [second.sha256, first.sha256]
    assert store.list_aliases("model:temporal") == [alias]

    con = sqlite3.connect(db_path)
    try:
        rows = con.execute(
            "SELECT sha256 FROM artifact_aliases WHERE alias=? ORDER BY set_at ASC",
            (alias,),
        ).fetchall()
        assert [row[0] for row in rows] == [first.sha256, second.sha256]
        counts = dict(con.execute("SELECT sha256, ref_count FROM artifacts").fetchall())
        assert counts[first.sha256] == 0
        assert counts[second.sha256] == 1
    finally:
        con.close()
