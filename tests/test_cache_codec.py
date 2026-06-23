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


def test_codec_reports_active_backend():
    assert codec.codec_name() in {"msgpack", "json", "unavailable"}
    assert codec.msgpack_available() == (codec.codec_name() == "msgpack")


def test_require_msgpack_raises_typed_codec_error_when_unavailable(monkeypatch):
    monkeypatch.setattr(codec, "_msgpack", None)

    with pytest.raises(codec.CacheCodecError, match="cache_msgpack_dependency_unavailable"):
        codec.require_msgpack()


def test_production_codec_refuses_json_downgrade_when_msgpack_is_missing(monkeypatch):
    monkeypatch.setenv("ENV", "prod")
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.delenv("CACHE_CODEC_ALLOW_JSON_FALLBACK", raising=False)
    monkeypatch.setattr(codec, "_msgpack", None)

    snapshot = codec.readiness_snapshot()

    assert snapshot["required"] is True
    assert snapshot["ok"] is False
    assert snapshot["codec"] == "unavailable"
    assert "cache_msgpack_dependency_unavailable" in snapshot["blockers"]
    with pytest.raises(codec.CacheCodecError, match="cache_msgpack_required_in_production"):
        codec.encode({"x": 1})


def test_explicit_development_json_fallback_round_trips_when_msgpack_is_missing(monkeypatch):
    monkeypatch.setenv("ENV", "dev")
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.delenv("ENGINE_SUPERVISED", raising=False)
    monkeypatch.delenv("CACHE_CODEC_REQUIRE_MSGPACK", raising=False)
    monkeypatch.setenv("CACHE_CODEC_ALLOW_JSON_FALLBACK", "1")
    monkeypatch.setattr(codec, "_msgpack", None)

    raw = codec.encode({"x": 1}, ts_ms=123)

    assert codec.codec_name() == "json"
    assert codec.readiness_snapshot()["ok"] is True
    assert raw.startswith(b"{")
    assert codec.decode(raw) == {"x": 1}


def test_codec_version_mismatch_raises_typed_error():
    raw = codec.encode({"x": 1}, version=1)

    with pytest.raises(codec.UnsupportedCacheVersion):
        codec.decode(raw, expected_version=2)


def test_codec_uses_current_version_at_decode_time(monkeypatch):
    raw = codec.encode({"x": 1}, version=1)
    monkeypatch.setattr(codec, "CURRENT_VERSION", 2)

    with pytest.raises(codec.UnsupportedCacheVersion):
        codec.decode(raw)
