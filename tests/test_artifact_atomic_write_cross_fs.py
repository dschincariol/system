from __future__ import annotations

import hashlib
import os
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from engine.artifacts import store as artifact_store_module
from engine.artifacts.store import LocalArtifactStore


def _store(tmp_path: Path) -> tuple[LocalArtifactStore, Path]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    db_path = tmp_path / "artifacts.db"

    def connect():
        return sqlite3.connect(db_path)

    return LocalArtifactStore(root=tmp_path / "store", connect_factory=connect), db_path


def _stat_with_dev(result: os.stat_result, st_dev: int) -> os.stat_result:
    values = list(result)
    values[2] = st_dev
    return os.stat_result(values)


def test_put_uses_dest_side_atomic_replace_when_devices_differ(tmp_path: Path, monkeypatch) -> None:
    store, _db_path = _store(tmp_path)
    real_stat = artifact_store_module.os.stat
    real_replace = artifact_store_module.os.replace
    real_warning = artifact_store_module._LOG.warning
    temp_root = str(store.root / "temp")
    objects_root = str(store.root / "objects")
    replace_calls: list[tuple[Path, Path]] = []
    warnings: list[str] = []

    def fake_stat(path, *args, **kwargs):
        result = real_stat(path, *args, **kwargs)
        if not isinstance(path, (str, bytes, os.PathLike)):
            return result
        text = os.fsdecode(os.fspath(path))
        if text.startswith(temp_root):
            return _stat_with_dev(result, 101)
        if text.startswith(objects_root):
            return _stat_with_dev(result, 202)
        return result

    def recording_replace(src, dst) -> None:
        replace_calls.append((Path(src), Path(dst)))
        real_replace(src, dst)

    def recording_warning(message, *args, **kwargs):
        warnings.append(str(message))
        return real_warning(message, *args, **kwargs)

    monkeypatch.setattr(artifact_store_module.os, "stat", fake_stat)
    monkeypatch.setattr(artifact_store_module.os, "replace", recording_replace)
    monkeypatch.setattr(artifact_store_module._LOG, "warning", recording_warning)

    payload = b"cross-device artifact bytes"
    ref = store.put(payload, content_type="application/octet-stream", kind="model")

    dest = store.object_path(ref)
    assert ref.sha256 == hashlib.sha256(payload).hexdigest()
    assert store.get_bytes(ref) == payload
    assert dest.read_bytes() == payload
    assert any(src.name.startswith(f"{dest.name}.tmp_") and dst == dest for src, dst in replace_calls)
    assert not any(src.parent == (store.root / "temp") and dst == dest for src, dst in replace_calls)
    assert not list(dest.parent.glob(f"{dest.name}.tmp_*"))
    assert any("copy-and-atomic-replace fallback" in message for message in warnings)
