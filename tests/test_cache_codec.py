from __future__ import annotations

import pytest
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from engine.cache import codec


def test_codec_round_trip_envelope():
    payload = {"symbol": "AAPL", "weights": [0.1, 0.2], "meta": {"ok": True}}

    raw = codec.encode(payload, ts_ms=123)

    assert isinstance(raw, bytes)
    assert codec.decode(raw) == payload


def test_codec_version_mismatch_raises_typed_error():
    raw = codec.encode({"x": 1}, version=1)

    with pytest.raises(codec.UnsupportedCacheVersion):
        codec.decode(raw, expected_version=2)


def test_codec_uses_current_version_at_decode_time(monkeypatch):
    raw = codec.encode({"x": 1}, version=1)
    monkeypatch.setattr(codec, "CURRENT_VERSION", 2)

    with pytest.raises(codec.UnsupportedCacheVersion):
        codec.decode(raw)
