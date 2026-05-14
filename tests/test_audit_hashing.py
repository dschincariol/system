from __future__ import annotations

from engine.audit.hashing import compute_row_hash


def test_row_hash_known_vectors() -> None:
    first = compute_row_hash(None, {"id": 1, "a": "x"})
    second = compute_row_hash(first, {"id": 2, "a": "y"})

    assert first.hex() == "122a79eed473af5d7c3b410cac8ae6fa31ef2f1dba2ff3d7c14e8c9bed0958c5"
    assert second.hex() == "b9ee2d98e53147f5f1513264a634670f9fd9b783cfc41f7a8cb92fb5a182a8da"


def test_empty_prev_hash_is_distinct_from_no_prev_hash() -> None:
    payload = {"id": 1, "a": "x"}

    assert compute_row_hash(None, payload) != compute_row_hash(b"", payload)
