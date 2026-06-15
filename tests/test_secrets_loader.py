from __future__ import annotations

import importlib
import sys

import pytest

from services.secrets import loader


def test_default_provider_dispatches_by_platform(monkeypatch):
    monkeypatch.delenv("TS_SECRETS_PROVIDER", raising=False)
    monkeypatch.setattr(loader.sys, "platform", "linux")
    assert loader.selected_provider_name() == "systemd-creds"
    monkeypatch.setattr(loader.sys, "platform", "win32")
    assert loader.selected_provider_name() == "dpapi"


def test_plaintext_happy_path_logs_success(monkeypatch, tmp_path):
    secret_dir = tmp_path / "secrets"
    secret_dir.mkdir()
    (secret_dir / "master_key").write_bytes(b"alpha")
    events = []

    monkeypatch.setenv("TS_SECRETS_PROVIDER", "plaintext")
    monkeypatch.setenv("TS_DEV_SECRETS_DIR", str(secret_dir))
    monkeypatch.delenv("TS_ENV", raising=False)
    monkeypatch.setattr(loader, "_insert_access_log", lambda **kwargs: events.append(kwargs))
    sys.modules.pop("services.secrets.providers.plaintext", None)

    with pytest.warns(RuntimeWarning):
        assert loader.load_secret("master_key") == b"alpha"
    assert events == [{"name": "master_key", "provider": "plaintext", "ok": True, "error": ""}]


def test_plaintext_delete_secret_removes_file_and_logs_success(monkeypatch, tmp_path):
    secret_dir = tmp_path / "secrets"
    secret_dir.mkdir()
    secret_path = secret_dir / "old_key"
    secret_path.write_bytes(b"alpha")
    events = []

    monkeypatch.setenv("TS_SECRETS_PROVIDER", "plaintext")
    monkeypatch.setenv("TS_DEV_SECRETS_DIR", str(secret_dir))
    monkeypatch.delenv("TS_ENV", raising=False)
    monkeypatch.setattr(loader, "_insert_access_log", lambda **kwargs: events.append(kwargs))
    sys.modules.pop("services.secrets.providers.plaintext", None)

    with pytest.warns(RuntimeWarning):
        assert loader.delete_secret("old_key") is True
    assert not secret_path.exists()
    assert events == [{"name": "old_key", "provider": "plaintext", "ok": True, "error": ""}]


def test_access_log_write_failure_is_observable_without_blocking_secret_read(monkeypatch, tmp_path):
    secret_dir = tmp_path / "secrets"
    secret_dir.mkdir()
    (secret_dir / "master_key").write_bytes(b"alpha")
    health_events = []
    metric_events = []

    monkeypatch.setenv("TS_SECRETS_PROVIDER", "plaintext")
    monkeypatch.setenv("TS_DEV_SECRETS_DIR", str(secret_dir))
    monkeypatch.delenv("TS_ENV", raising=False)
    monkeypatch.setattr(
        loader,
        "_insert_access_log",
        lambda **_kwargs: (_ for _ in ()).throw(OSError("audit sink unavailable")),
    )
    monkeypatch.setattr(
        loader,
        "record_component_health",
        lambda component, **kwargs: health_events.append((component, kwargs)),
    )
    monkeypatch.setattr(
        loader,
        "emit_counter",
        lambda metric, value=1, **kwargs: metric_events.append((metric, value, kwargs)),
    )
    sys.modules.pop("services.secrets.providers.plaintext", None)

    with pytest.warns(RuntimeWarning):
        assert loader.load_secret("master_key") == b"alpha"

    assert health_events
    component, health = health_events[-1]
    assert component == "credential_access_log"
    assert health["ok"] is False
    assert health["status"] == "degraded"
    assert "OSError" in health["detail"]
    assert metric_events[-1][0] == "credential_access_log_write_failures"


def test_systemd_missing_credentials_directory_raises_typed(monkeypatch):
    events = []

    monkeypatch.setenv("TS_SECRETS_PROVIDER", "systemd-creds")
    monkeypatch.delenv("CREDENTIALS_DIRECTORY", raising=False)
    monkeypatch.setattr(loader, "_insert_access_log", lambda **kwargs: events.append(kwargs))

    with pytest.raises(loader.SecretNotAvailable, match="credentials_directory_missing|systemd_creds_linux_only"):
        loader.load_secret("master_key")
    assert events
    assert events[-1]["ok"] is False


def test_missing_plaintext_file_raises_typed(monkeypatch, tmp_path):
    events = []

    monkeypatch.setenv("TS_SECRETS_PROVIDER", "plaintext")
    monkeypatch.setenv("TS_DEV_SECRETS_DIR", str(tmp_path))
    monkeypatch.delenv("TS_ENV", raising=False)
    monkeypatch.setattr(loader, "_insert_access_log", lambda **kwargs: events.append(kwargs))
    sys.modules.pop("services.secrets.providers.plaintext", None)

    with pytest.warns(RuntimeWarning):
        with pytest.raises(loader.SecretNotAvailable, match="secret_missing:missing"):
            loader.load_secret("missing")
    assert events[-1]["ok"] is False


def test_unknown_provider_raises_typed(monkeypatch):
    monkeypatch.setenv("TS_SECRETS_PROVIDER", "nope")
    monkeypatch.setattr(loader, "_insert_access_log", lambda **_kwargs: None)

    with pytest.raises(loader.SecretNotAvailable, match="unknown_secrets_provider:nope"):
        loader.load_secret("master_key")


@pytest.mark.parametrize(
    "name",
    [
        "../etc/passwd",
        "subdir/secret",
        "subdir\\secret",
        "/absolute/path",
        "C:\\absolute\\path",
        "C:relative",
        ".",
        "..",
    ],
)
def test_loader_rejects_path_like_secret_names_before_provider(monkeypatch, name):
    monkeypatch.setenv("TS_SECRETS_PROVIDER", "plaintext")
    provider_called = False

    def _fail_provider_loader(_provider):
        nonlocal provider_called
        provider_called = True
        raise AssertionError("provider loader should not be called for invalid secret names")

    monkeypatch.setattr(loader, "_provider_loader", _fail_provider_loader)
    monkeypatch.setattr(loader, "_insert_access_log", lambda **_kwargs: None)

    with pytest.raises(loader.SecretNotAvailable, match="invalid_secret_name"):
        loader.load_secret(name)
    assert provider_called is False
