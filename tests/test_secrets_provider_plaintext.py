from __future__ import annotations

import importlib
import base64
import sys

import pytest

from services import credential_encryption
from services.secrets.loader import SecretNotAvailable


def _fresh_plaintext_module():
    sys.modules.pop("services.secrets.providers.plaintext", None)
    return importlib.import_module("services.secrets.providers.plaintext")


def test_plaintext_provider_reads_raw_bytes(monkeypatch, tmp_path):
    monkeypatch.setenv("TS_DEV_SECRETS_DIR", str(tmp_path))
    monkeypatch.delenv("TS_ENV", raising=False)
    (tmp_path / "master_key").write_bytes(b"raw")

    with pytest.warns(RuntimeWarning):
        provider = _fresh_plaintext_module()
    assert provider.load("master_key") == b"raw"


def test_plaintext_provider_refuses_production(monkeypatch):
    monkeypatch.setenv("TS_ENV", "production")

    with pytest.raises(RuntimeError, match="plaintext_secrets_provider_forbidden_in_production"):
        with pytest.warns(RuntimeWarning):
            _fresh_plaintext_module()


def test_plaintext_provider_refuses_production_set_after_import(monkeypatch, tmp_path):
    monkeypatch.setenv("TS_DEV_SECRETS_DIR", str(tmp_path))
    monkeypatch.setenv("TS_SECRETS_PROVIDER", "plaintext")
    monkeypatch.delenv("TS_ENV", raising=False)
    (tmp_path / "master_key").write_bytes(b"raw")

    with pytest.warns(RuntimeWarning):
        _fresh_plaintext_module()

    monkeypatch.setenv("TS_ENV", "production")
    monkeypatch.setattr(
        "services.secrets.loader._insert_access_log",
        lambda **_kwargs: None,
    )

    from services.secrets.loader import load_secret

    with pytest.raises(RuntimeError, match="plaintext_secrets_provider_forbidden_in_production"):
        load_secret("master_key")


def test_missing_master_key_file_falls_back_to_secret_loader_with_info_log(monkeypatch, tmp_path, caplog):
    missing_path = tmp_path / "missing_master_key"

    monkeypatch.delenv("DATA_SOURCE_MASTER_KEY", raising=False)
    monkeypatch.setenv("DATA_SOURCE_MASTER_KEY_FILE", str(missing_path))

    def _raise_secret_not_available(name: str) -> bytes:
        raise SecretNotAvailable(f"secret_missing:{name}")

    monkeypatch.setattr(credential_encryption, "load_secret", _raise_secret_not_available)

    with caplog.at_level("INFO", logger=credential_encryption.__name__):
        with pytest.raises(SecretNotAvailable, match="secret_missing:master_key"):
            credential_encryption._master_key_bytes()

    assert "data_source_master_key_file_missing" in caplog.text


def test_corrupt_encoded_master_key_file_fails_closed(monkeypatch, tmp_path):
    key_path = tmp_path / "master_key"
    key_path.write_text(base64.b64encode(b"x" * 31).decode("ascii"), encoding="utf-8")

    monkeypatch.delenv("DATA_SOURCE_MASTER_KEY", raising=False)
    monkeypatch.setenv("DATA_SOURCE_MASTER_KEY_FILE", str(key_path))

    with pytest.raises(credential_encryption.MasterKeyLoadError, match="invalid_length"):
        credential_encryption._master_key_bytes()
