"""Serialization helpers owned by the artifact layer.

Blob serializer calls are centralized behind this facade so callers
elsewhere ask for bytes, write them through the artifact store, or
feed them to ``Path.write_bytes`` for legacy file targets.

The ``tests/test_no_loose_blob_writes.py`` lint test enforces this by
AST-scanning every ``engine/`` module and rejecting blob serializer
calls outside ``engine/artifacts/``.
"""

from __future__ import annotations

import io
import pickle
from importlib.util import find_spec
from pathlib import Path
from typing import Any


def dumps_pickle_artifact(value: Any, *, prefer_joblib: bool = False) -> bytes:
    if prefer_joblib and find_spec("joblib") is not None:
        from engine.artifacts.store import dumps_joblib_artifact

        return dumps_joblib_artifact(value)
    from engine.artifacts.store import dumps_pickle_artifact_payload

    return dumps_pickle_artifact_payload(value)


def dump_pickle_artifact(value: Any, path: str | Path, *, prefer_joblib: bool = False) -> Path:
    target = Path(path).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = dumps_pickle_artifact(value, prefer_joblib=prefer_joblib)
    target.write_bytes(payload)
    return target


def loads_pickle_artifact(payload: bytes, *, prefer_joblib: bool = False) -> Any:
    if prefer_joblib and find_spec("joblib") is not None:
        import joblib

        buffer = io.BytesIO(bytes(payload or b""))
        return joblib.load(buffer)
    return pickle.loads(bytes(payload or b""))


def load_pickle_artifact(path: str | Path, *, prefer_joblib: bool = False) -> Any:
    return loads_pickle_artifact(Path(path).expanduser().read_bytes(), prefer_joblib=prefer_joblib)


def dumps_torch_payload(payload: Any) -> bytes:
    from engine.artifacts.store import dumps_torch_artifact_payload

    return dumps_torch_artifact_payload(payload)


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
