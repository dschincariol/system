"""SHA-256 hash helpers for audit hash-chain rows."""

from __future__ import annotations

import hashlib
from typing import Any, Mapping

from engine.audit.canonical import canonical_row_bytes

_EXPLICIT_EMPTY_PREV_HASH = b"\x00audit-empty-prev-hash\x00"


def compute_row_hash(prev_hash: bytes | None, row: Mapping[str, Any]) -> bytes:
    """Compute ``sha256(prev_hash || canonical_row_bytes(row))``."""

    h = hashlib.sha256()
    if prev_hash is not None:
        prev_hash_bytes = bytes(prev_hash)
        h.update(prev_hash_bytes if prev_hash_bytes else _EXPLICIT_EMPTY_PREV_HASH)
    h.update(canonical_row_bytes(row))
    return h.digest()
