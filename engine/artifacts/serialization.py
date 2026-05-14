"""Serialization helpers owned by the artifact layer.

All ``joblib.dump`` / ``torch.save`` / ``pickle.dump`` calls in the
codebase live here so blob production stays single-sourced. Callers
elsewhere ask for bytes, write them through the artifact store, or
feed them to ``Path.write_bytes`` for legacy file targets.

The ``tests/test_no_loose_blob_writes.py`` lint test enforces this by
AST-scanning every ``engine/`` module and rejecting blob serializer
calls outside ``engine/artifacts/``.
"""

from __future__ import annotations

import io
import pickle
from pathlib import Path
from typing import Any

try:
    import joblib
except Exception:  # pragma: no cover - optional dependency
    joblib = None  # type: ignore[assignment]


def dumps_pickle_artifact(value: Any, *, prefer_joblib: bool = False) -> bytes:
    if prefer_joblib and joblib is not None:
        buffer = io.BytesIO()
        joblib.dump(value, buffer)
        return buffer.getvalue()
    return pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)


def dump_pickle_artifact(value: Any, path: str | Path, *, prefer_joblib: bool = False) -> Path:
    target = Path(path).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = dumps_pickle_artifact(value, prefer_joblib=prefer_joblib)
    target.write_bytes(payload)
    return target


def loads_pickle_artifact(payload: bytes, *, prefer_joblib: bool = False) -> Any:
    if prefer_joblib and joblib is not None:
        buffer = io.BytesIO(bytes(payload or b""))
        return joblib.load(buffer)
    return pickle.loads(bytes(payload or b""))


def load_pickle_artifact(path: str | Path, *, prefer_joblib: bool = False) -> Any:
    return loads_pickle_artifact(Path(path).expanduser().read_bytes(), prefer_joblib=prefer_joblib)


def dumps_torch_payload(payload: Any) -> bytes:
    import torch

    buffer = io.BytesIO()
    torch.save(payload, buffer)
    return buffer.getvalue()


def loads_torch_payload(payload: bytes, *, map_location: str = "cpu", weights_only: bool = True) -> Any:
    import torch

    buffer = io.BytesIO(bytes(payload or b""))
    return torch.load(buffer, map_location=map_location, weights_only=weights_only)


__all__ = [
    "dump_pickle_artifact",
    "dumps_pickle_artifact",
    "dumps_torch_payload",
    "load_pickle_artifact",
    "loads_pickle_artifact",
    "loads_torch_payload",
]
