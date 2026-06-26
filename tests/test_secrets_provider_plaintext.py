from __future__ import annotations

import importlib
import base64
import sys

import pytest

from services import credential_encryption
from services.secrets.loader import SecretNotAvailable

_STRICT_RUNTIME_ENV_KEYS = (
    "PROD_LOCK",
    "ENGINE_SUPERVISED",
    "ENV",
    "APP_ENV",
    "TS_ENV",
    "NODE_ENV",
    "ENGINE_MODE",
    "EXECUTION_MODE",
    "OPERATOR_MODE",
)


def _fresh_plaintext_module():
    sys.modules.pop("services.secrets.providers.plaintext", None)
    return importlib.import_module("services.secrets.providers.plaintext")


def _clear_strict_runtime_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in _STRICT_RUNTIME_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


def _clear_plaintext_production_cache(provider: object) -> None:
    setattr(provider, "_production_check_at", 0.0)
    setattr(provider, "_production_forbidden", None)
    setattr(provider, "_production_check_signature", None)


def test_plaintext_provider_reads_raw_bytes(monkeypatch, tmp_path):
    monkeypatch.setenv("TS_DEV_SECRETS_DIR", str(tmp_path))
    _clear_strict_runtime_env(monkeypatch)
    (tmp_path / "master_key").write_bytes(b"raw")

    with pytest.warns(RuntimeWarning):
        provider = _fresh_plaintext_module()
    assert provider.load("master_key") == b"raw"


def test_plaintext_provider_delete_removes_secret(monkeypatch, tmp_path):
    monkeypatch.setenv("TS_DEV_SECRETS_DIR", str(tmp_path))
    _clear_strict_runtime_env(monkeypatch)
    secret_path = tmp_path / "old_key"
    secret_path.write_bytes(b"raw")

    with pytest.warns(RuntimeWarning):
        provider = _fresh_plaintext_module()
    assert provider.delete("old_key") is True
    assert not secret_path.exists()
    assert provider.delete("old_key") is False


def test_plaintext_provider_refuses_production(monkeypatch):
    _clear_strict_runtime_env(monkeypatch)
    monkeypatch.setenv("TS_ENV", "production")
    monkeypatch.setenv("TRADING_ENFORCE_SECRET_SOURCE_POLICY", "1")

    with pytest.raises(RuntimeError, match="plaintext_secrets_provider_forbidden_in_production"):
        with pytest.warns(RuntimeWarning):
            _fresh_plaintext_module()


def test_plaintext_provider_refuses_production_set_after_import(monkeypatch, tmp_path):
    monkeypatch.setenv("TS_DEV_SECRETS_DIR", str(tmp_path))
    monkeypatch.setenv("TS_SECRETS_PROVIDER", "plaintext")
    _clear_strict_runtime_env(monkeypatch)
    (tmp_path / "master_key").write_bytes(b"raw")

    with pytest.warns(RuntimeWarning):
        _fresh_plaintext_module()

    monkeypatch.setenv("TRADING_ENFORCE_SECRET_SOURCE_POLICY", "1")
    monkeypatch.setenv("TS_ENV", "production")
    monkeypatch.setattr(
        "services.secrets.loader._insert_access_log",
        lambda **_kwargs: None,
    )

    from services.secrets.loader import load_secret

    with pytest.raises(RuntimeError, match="plaintext_secrets_provider_forbidden_in_production"):
        load_secret("master_key")


@pytest.mark.parametrize(
    ("env_key", "env_value"),
    [
        ("ENV", "prod"),
        ("ENGINE_MODE", "live"),
        ("EXECUTION_MODE", "live"),
        ("PROD_LOCK", "1"),
        ("ENGINE_SUPERVISED", "1"),
    ],
)
def test_plaintext_provider_refuses_strict_runtime_triggers_on_operations(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    env_key: str,
    env_value: str,
) -> None:
    monkeypatch.setenv("TS_DEV_SECRETS_DIR", str(tmp_path))
    _clear_strict_runtime_env(monkeypatch)
    monkeypatch.setenv("TRADING_ENFORCE_SECRET_SOURCE_POLICY", "1")
    (tmp_path / "master_key").write_bytes(b"raw")

    with pytest.warns(RuntimeWarning):
        provider = _fresh_plaintext_module()

    monkeypatch.setenv(env_key, env_value)
    _clear_plaintext_production_cache(provider)

    with pytest.raises(RuntimeError) as load_exc:
        provider.load("master_key")
    assert str(load_exc.value) == "plaintext_secrets_provider_forbidden_in_production"

    _clear_plaintext_production_cache(provider)
    with pytest.raises(RuntimeError) as delete_exc:
        provider.delete("master_key")
    assert str(delete_exc.value) == "plaintext_secrets_provider_forbidden_in_production"


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
