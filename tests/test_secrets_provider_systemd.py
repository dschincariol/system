from __future__ import annotations

import sys

import pytest

pytestmark = [
    pytest.mark.linux_only,
    pytest.mark.skipif(sys.platform.startswith("win"), reason="systemd-creds provider is Linux-only"),
]


def test_systemd_provider_reads_credentials_directory(monkeypatch, tmp_path):
    from services.secrets.providers import systemd_creds

    (tmp_path / "master_key").write_bytes(b"secret")
    monkeypatch.setenv("CREDENTIALS_DIRECTORY", str(tmp_path))

    assert systemd_creds.load("master_key") == b"secret"


def test_systemd_provider_delete_removes_credential(monkeypatch, tmp_path):
    from services.secrets.providers import systemd_creds

    secret_path = tmp_path / "old_key"
    secret_path.write_bytes(b"secret")
    monkeypatch.setenv("CREDENTIALS_DIRECTORY", str(tmp_path))

    assert systemd_creds.delete("old_key") is True
    assert not secret_path.exists()
    assert systemd_creds.delete("old_key") is False


def test_systemd_provider_missing_file_is_typed(monkeypatch, tmp_path):
    from services.secrets.loader import SecretNotAvailable
    from services.secrets.providers import systemd_creds

    monkeypatch.setenv("CREDENTIALS_DIRECTORY", str(tmp_path))

    with pytest.raises(SecretNotAvailable, match="secret_missing:missing"):
        systemd_creds.load("missing")
