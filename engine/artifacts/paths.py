"""Artifact filesystem layout helpers."""

from __future__ import annotations

import os
import re
import sys
import tempfile
from pathlib import Path

from engine.runtime.platform import default_data_root

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def _running_python_tests() -> bool:
    argv = " ".join(str(part or "") for part in sys.argv).lower()
    return bool(
        str(os.environ.get("TS_TESTING") or "").strip().lower() in {"1", "true", "yes", "on"}
        or "pytest" in argv
        or "unittest" in argv
        or "discover" in argv
        or "tests/" in argv
        or " tests" in argv
    )


def artifacts_root() -> Path:
    configured = str(os.environ.get("TS_ARTIFACTS_ROOT") or "").strip()
    if configured:
        return Path(configured).expanduser().resolve()
    for env_name in ("TRADING_DATA", "DATA_DIR"):
        runtime_data = str(os.environ.get(env_name) or "").strip()
        if runtime_data:
            return (Path(runtime_data).expanduser() / "artifacts").resolve()
    if _running_python_tests():
        root = Path(tempfile.gettempdir()) / f"trading-system-tests-{os.getpid()}"
        return (root / "artifacts").resolve()
    return (default_data_root() / "artifacts").resolve()


def validate_sha256(sha256: str) -> str:
    text = str(sha256 or "").strip().lower()
    if not _SHA256_RE.match(text):
        raise ValueError(f"invalid_sha256:{sha256!r}")
    return text


def object_path(sha256: str, *, root: Path | None = None) -> Path:
    digest = validate_sha256(sha256)
    base = Path(root).expanduser().resolve() if root is not None else artifacts_root()
    return base / "objects" / digest[:2] / digest[2:4] / digest


__all__ = ["artifacts_root", "object_path", "validate_sha256"]
