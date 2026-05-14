from __future__ import annotations

from datetime import date, datetime, timezone, timedelta
from decimal import Decimal

import pytest

from engine.audit.canonical import canonical_row_bytes


def test_canonical_rows_are_byte_pinned() -> None:
    rows = [
        ({"id": 1, "b": 2, "a": 1}, b'{"a":1,"b":2,"id":1}'),
        ({"id": 2, "price": 1.0, "qty": Decimal("2.5000")}, b'{"id":2,"price":1,"qty":2.5}'),
        (
            {"id": 3, "ts": datetime(2026, 5, 2, 12, 30, 5, 123456, tzinfo=timezone(timedelta(hours=-4)))},
            b'{"id":3,"ts":"2026-05-02T16:30:05.123456Z"}',
        ),
        (
            {"id": 4, "trade_date": date(2026, 5, 2), "ok": True, "none": None},
            b'{"id":4,"none":null,"ok":true,"trade_date":"2026-05-02"}',
        ),
        (
            {"id": 5, "payload": {"z": [3, 2, 1], "a": {"b": False}}},
            b'{"id":5,"payload":{"a":{"b":false},"z":[3,2,1]}}',
        ),
        (
            {"id": 6, "bytes": b"abc", "row_hash": b"ignored", "prev_hash": b"ignored"},
            b'{"bytes":"616263","id":6}',
        ),
        ({"id": 7, "tags": {"beta", "alpha"}}, b'{"id":7,"tags":["alpha","beta"]}'),
        ({"id": 8, "unicode": "cafe", "quote": 'a"b'}, b'{"id":8,"quote":"a\\"b","unicode":"cafe"}'),
        ({"id": 9, "negzero": -0.0, "small": 0.0001200}, b'{"id":9,"negzero":0,"small":0.00012}'),
        ({"id": 10, "items": (Decimal("10.00"), 1.0, "x")}, b'{"id":10,"items":[10,1,"x"]}'),
    ]

    for row, expected in rows:
        assert canonical_row_bytes(row) == expected


def test_non_finite_float_is_rejected() -> None:
    with pytest.raises(ValueError):
        canonical_row_bytes({"id": 1, "bad": float("nan")})
