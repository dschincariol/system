from __future__ import annotations

import ctypes
import gc
import sys
import types
import uuid

import pytest

pytestmark = [
    pytest.mark.windows_only,
    pytest.mark.skipif(not sys.platform.startswith("win"), reason="DPAPI provider is Windows-only"),
]


def test_dpapi_round_trip(monkeypatch, tmp_path):
    pytest.importorskip("win32crypt")
    from services.secrets.providers import dpapi

    monkeypatch.setenv("TS_DPAPI_SECRETS_DIR", str(tmp_path))
    dpapi.protect_secret("master_key", b"secret", secrets_dir=tmp_path)

    assert dpapi.load("master_key") == b"secret"


def test_dpapi_load_zeroes_plaintext_buffers_after_return(monkeypatch, tmp_path):
    from services.secrets.providers import dpapi

    expected = f"memory-probe-{uuid.uuid4().hex}".encode("ascii")
    captured: dict[str, object] = {}

    def _crypt_unprotect_data(_encrypted, _entropy, _reserved, _prompt, _flags):
        raw = bytearray(expected)
        captured["raw"] = raw
        captured["raw_addr"] = ctypes.addressof(ctypes.c_char.from_buffer(raw))
        captured["raw_len"] = len(raw)
        return "master_key", raw

    def _copy_plaintext(data: object) -> bytearray:
        plaintext = bytearray(data)
        captured["copy"] = plaintext
        captured["copy_addr"] = ctypes.addressof(ctypes.c_char.from_buffer(plaintext))
        captured["copy_len"] = len(plaintext)
        return plaintext

    monkeypatch.setenv("TS_DPAPI_SECRETS_DIR", str(tmp_path))
    monkeypatch.setitem(
        sys.modules,
        "win32crypt",
        types.SimpleNamespace(CryptUnprotectData=_crypt_unprotect_data),
    )
    monkeypatch.setattr(dpapi, "_copy_plaintext", _copy_plaintext)
    (tmp_path / "master_key.dpapi").write_bytes(b"encrypted")

    loaded = dpapi.load("master_key")
    assert loaded == expected

    loaded = None
    expected = None
    gc.collect()

    for prefix in ("raw", "copy"):
        addr = int(captured[f"{prefix}_addr"])
        length = int(captured[f"{prefix}_len"])
        assert ctypes.string_at(addr, length) == b"\x00" * length
