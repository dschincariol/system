from __future__ import annotations

import base64

from services import credential_encryption as credential_encryption


def test_data_source_master_key_env_satisfies_local_encryption(monkeypatch):
    monkeypatch.setenv("DATA_SOURCE_MASTER_KEY", base64.b64encode(b"x" * 32).decode("ascii"))

    def _fail_load_secret(_name: str) -> bytes:
        raise AssertionError("load_secret should not be used when DATA_SOURCE_MASTER_KEY is set")

    monkeypatch.setattr(credential_encryption, "load_secret", _fail_load_secret)

    blob = credential_encryption.encrypt_credentials({"api_key": "secret"})

    assert credential_encryption.decrypt_credentials(blob) == {"api_key": "secret"}
