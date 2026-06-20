from __future__ import annotations

import base64

import pytest
from services import credential_encryption as credential_encryption


def _valid_key() -> str:
    return base64.b64encode(bytes(range(32))).decode("ascii")


def test_data_source_master_key_env_satisfies_local_encryption(monkeypatch):
    monkeypatch.setenv("DATA_SOURCE_MASTER_KEY", _valid_key())

    def _fail_load_secret(_name: str) -> bytes:
        raise AssertionError("load_secret should not be used when DATA_SOURCE_MASTER_KEY is set")

    monkeypatch.setattr(credential_encryption, "load_secret", _fail_load_secret)

    blob = credential_encryption.encrypt_credentials({"api_key": "secret"})

    assert credential_encryption.decrypt_credentials(blob) == {"api_key": "secret"}


def test_data_source_master_key_file_satisfies_prod_validation(monkeypatch, tmp_path):
    key_path = tmp_path / "master_key"
    key_path.write_text(_valid_key(), encoding="ascii")
    key_path.chmod(0o600)

    monkeypatch.setenv("ENV", "prod")
    monkeypatch.delenv("DATA_SOURCE_MASTER_KEY", raising=False)
    monkeypatch.setenv("DATA_SOURCE_MASTER_KEY_FILE", str(key_path))

    def _fail_load_secret(_name: str) -> bytes:
        raise AssertionError("load_secret should not be used when DATA_SOURCE_MASTER_KEY_FILE is set")

    monkeypatch.setattr(credential_encryption, "load_secret", _fail_load_secret)

    state = credential_encryption.validate_data_source_master_key(production=True, require_present=True)
    assert state["ok"] is True
    assert state["source"] == "file"
    assert state["format"] == "base64_32"

    blob = credential_encryption.encrypt_credentials({"api_key": "secret"})
    assert credential_encryption.decrypt_credentials(blob) == {"api_key": "secret"}


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("change-me", "placeholder_or_known_default"),
        ("short", "short"),
        ("!" * 43 + "=", "decode_failed"),
        (base64.b64encode(b"x" * 32).decode("ascii"), "low_entropy"),
        ("this-is-a-long-dev-only-master-key", "raw_text_forbidden_in_production"),
    ],
)
def test_prod_master_key_env_rejects_weak_values(monkeypatch, value, expected):
    monkeypatch.setenv("ENV", "prod")
    monkeypatch.setenv("DATA_SOURCE_MASTER_KEY", value)
    monkeypatch.delenv("DATA_SOURCE_MASTER_KEY_FILE", raising=False)

    with pytest.raises(credential_encryption.MasterKeyLoadError) as ctx:
        credential_encryption.validate_data_source_master_key(production=True, require_present=True)

    assert expected in str(ctx.value)


def test_prod_master_key_file_rejects_empty_file(monkeypatch, tmp_path):
    key_path = tmp_path / "master_key"
    key_path.write_text("", encoding="ascii")
    key_path.chmod(0o600)

    monkeypatch.setenv("ENV", "prod")
    monkeypatch.delenv("DATA_SOURCE_MASTER_KEY", raising=False)
    monkeypatch.setenv("DATA_SOURCE_MASTER_KEY_FILE", str(key_path))

    with pytest.raises(credential_encryption.MasterKeyLoadError, match="file_empty"):
        credential_encryption.validate_data_source_master_key(production=True, require_present=True)


def test_prod_master_key_file_rejects_insecure_permissions(monkeypatch, tmp_path):
    key_path = tmp_path / "master_key"
    key_path.write_text(_valid_key(), encoding="ascii")
    key_path.chmod(0o644)

    monkeypatch.setenv("ENV", "prod")
    monkeypatch.delenv("DATA_SOURCE_MASTER_KEY", raising=False)
    monkeypatch.setenv("DATA_SOURCE_MASTER_KEY_FILE", str(key_path))

    with pytest.raises(credential_encryption.MasterKeyLoadError, match="insecure_permissions"):
        credential_encryption.validate_data_source_master_key(production=True, require_present=True)


def test_dev_raw_master_key_is_explicitly_non_production(monkeypatch):
    monkeypatch.setenv("ENV", "dev")
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("DATA_SOURCE_MASTER_KEY", "local-dev-master-key-material")
    monkeypatch.delenv("DATA_SOURCE_MASTER_KEY_FILE", raising=False)

    state = credential_encryption.validate_data_source_master_key(production=False, require_present=True)
    assert state["ok"] is True
    assert state["format"] == "raw_dev_text"
    assert state["raw_text_allowed"] is True

    blob = credential_encryption.encrypt_credentials({"api_key": "secret"})
    assert credential_encryption.decrypt_credentials(blob) == {"api_key": "secret"}


def test_prod_master_key_missing_is_rejected(monkeypatch):
    monkeypatch.setenv("ENV", "prod")
    monkeypatch.delenv("DATA_SOURCE_MASTER_KEY", raising=False)
    monkeypatch.delenv("DATA_SOURCE_MASTER_KEY_FILE", raising=False)

    def _missing_secret(_name: str) -> bytes:
        raise RuntimeError("secret_missing")

    monkeypatch.setattr(credential_encryption, "load_secret", _missing_secret)

    with pytest.raises(credential_encryption.MasterKeyLoadError, match="secret_missing"):
        credential_encryption.validate_data_source_master_key(production=True, require_present=True)
