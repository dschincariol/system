from __future__ import annotations

import logging

import pytest


def _clear_pg_password_env(monkeypatch):
    for name in (
        "TS_PG_DSN",
        "TS_PG_PASSWORD",
        "TIMESCALE_PASSWORD",
        "TS_PG_PASSWORD_FILE",
        "TIMESCALE_PASSWORD_FILE",
        "TS_PG_PASSWORD_APP_FILE",
        "TS_PG_APP_PASSWORD_FILE",
        "TS_PG_PASSWORD_INGEST_FILE",
        "TS_PG_INGEST_PASSWORD_FILE",
        "TS_PG_PASSWORD_READER_FILE",
        "TS_PG_READER_PASSWORD_FILE",
        "TS_PG_PASSWORD_SECRET",
        "TIMESCALE_PASSWORD_SECRET",
        "TS_PG_PASSWORD_APP_SECRET",
        "TS_PG_APP_PASSWORD_SECRET",
        "TS_PG_PASSWORD_INGEST_SECRET",
        "TS_PG_INGEST_PASSWORD_SECRET",
        "TS_PG_PASSWORD_READER_SECRET",
        "TS_PG_READER_PASSWORD_SECRET",
        "TS_PG_PASSWORD_APP",
        "TS_PG_APP_PASSWORD",
        "TS_PG_PASSWORD_INGEST",
        "TS_PG_INGEST_PASSWORD",
        "TS_PG_PASSWORD_READER",
        "TS_PG_READER_PASSWORD",
        "PGPASSWORD",
        "PGPASSWORD_FILE",
        "PGPASSWORD_SECRET",
        "CREDENTIALS_DIRECTORY",
        "TS_DEV_SECRETS_DIR",
        "TS_SECRETS_PROVIDER",
    ):
        monkeypatch.delenv(name, raising=False)


def test_default_pg_dsn_uses_env_password_before_secret_provider(monkeypatch):
    from engine.runtime import platform
    from services.secrets import loader

    _clear_pg_password_env(monkeypatch)
    monkeypatch.setattr(platform, "is_linux", lambda: False)
    monkeypatch.setenv("TS_PG_USER", "trading")
    monkeypatch.setenv("TS_PG_PASSWORD", "local-password")
    monkeypatch.setattr(
        loader,
        "load_secret",
        lambda name: pytest.fail(f"unexpected secret provider lookup: {name}"),
    )

    assert platform.default_pg_dsn() == (
        "host=127.0.0.1 port=5432 user=trading dbname=trading password=local-password"
    )


def test_default_pg_dsn_uses_file_password_without_systemd_credentials(monkeypatch, tmp_path):
    from engine.runtime import platform
    from services.secrets import loader

    _clear_pg_password_env(monkeypatch)
    password_file = tmp_path / "timescale_password"
    password_file.write_text("file-backed-password\n", encoding="utf-8")
    password_file.chmod(0o600)

    monkeypatch.setattr(platform, "is_linux", lambda: False)
    monkeypatch.setenv("TS_PG_USER", "trading")
    monkeypatch.setenv("TS_SECRETS_PROVIDER", "systemd-creds")
    monkeypatch.setenv("TS_PG_PASSWORD_FILE", str(password_file))
    monkeypatch.setattr(
        loader,
        "load_secret",
        lambda name: pytest.fail(f"unexpected systemd credential lookup without credentials dir: {name}"),
    )

    assert platform.default_pg_dsn() == (
        "host=127.0.0.1 port=5432 user=trading dbname=trading password=file-backed-password"
    )


def test_default_pg_dsn_skips_unavailable_systemd_secret_ref_before_file_fallback(
    monkeypatch, tmp_path, caplog
):
    from engine.runtime import platform
    from services.secrets import loader

    _clear_pg_password_env(monkeypatch)
    password_file = tmp_path / "timescale_password"
    password_file.write_text("file-backed-password\n", encoding="utf-8")
    password_file.chmod(0o600)

    monkeypatch.setattr(platform, "is_linux", lambda: False)
    monkeypatch.setenv("TS_PG_USER", "trading")
    monkeypatch.setenv("TS_SECRETS_PROVIDER", "systemd-creds")
    monkeypatch.setenv("TS_PG_PASSWORD_SECRET", "pg_password_app")
    monkeypatch.setenv("TS_PG_PASSWORD_FILE", str(password_file))
    monkeypatch.setattr(
        loader,
        "load_secret",
        lambda name: pytest.fail(f"unexpected systemd credential lookup before file fallback: {name}"),
    )

    caplog.set_level(logging.WARNING)

    assert platform.default_pg_dsn() == (
        "host=127.0.0.1 port=5432 user=trading dbname=trading password=file-backed-password"
    )
    assert "credentials_directory_missing" not in caplog.text
    assert "SecretNotAvailable" not in caplog.text


def test_default_pg_dsn_without_any_password_source_reports_one_actionable_failure(
    monkeypatch, caplog
):
    from engine.runtime import platform
    from services.secrets import loader

    _clear_pg_password_env(monkeypatch)
    monkeypatch.setattr(platform, "is_linux", lambda: False)
    monkeypatch.setenv("TS_PG_USER", "trading")
    monkeypatch.setenv("TS_SECRETS_PROVIDER", "systemd-creds")
    monkeypatch.setenv("TS_PG_PASSWORD_SECRET", "pg_password_app")
    monkeypatch.setattr(
        loader,
        "load_secret",
        lambda name: pytest.fail(f"unexpected systemd credential lookup with no credentials dir: {name}"),
    )

    caplog.set_level(logging.WARNING)

    with pytest.raises(loader.SecretNotAvailable) as excinfo:
        platform.default_pg_dsn()

    message = str(excinfo.value)
    assert message.count("credentials_directory_missing") == 1
    assert "CREDENTIALS_DIRECTORY" in message
    assert "TS_PG_PASSWORD_FILE" in message
    assert "pg_password_app" in message
    assert "credentials_directory_missing" not in caplog.text
    assert "SecretNotAvailable" not in caplog.text


def test_default_pg_dsn_prefers_systemd_credential_when_directory_is_available(monkeypatch, tmp_path):
    from engine.runtime import platform

    _clear_pg_password_env(monkeypatch)
    credentials_dir = tmp_path / "creds"
    credentials_dir.mkdir()
    (credentials_dir / "pg_password_app").write_text("systemd-password\n", encoding="utf-8")
    fallback_file = tmp_path / "timescale_password"
    fallback_file.write_text("file-backed-password\n", encoding="utf-8")

    monkeypatch.setattr(platform, "is_linux", lambda: False)
    monkeypatch.setenv("TS_PG_USER", "trading")
    monkeypatch.setenv("TS_SECRETS_PROVIDER", "systemd-creds")
    monkeypatch.setenv("CREDENTIALS_DIRECTORY", str(credentials_dir))
    monkeypatch.setenv("TS_PG_PASSWORD_FILE", str(fallback_file))

    assert platform.default_pg_dsn() == (
        "host=127.0.0.1 port=5432 user=trading dbname=trading password=systemd-password"
    )


def test_default_pg_dsn_falls_back_to_file_when_systemd_secret_is_missing(monkeypatch, tmp_path):
    from engine.runtime import platform

    _clear_pg_password_env(monkeypatch)
    credentials_dir = tmp_path / "empty-creds"
    credentials_dir.mkdir()
    password_file = tmp_path / "timescale_password"
    password_file.write_text("file-backed-password\n", encoding="utf-8")
    password_file.chmod(0o600)

    monkeypatch.setattr(platform, "is_linux", lambda: False)
    monkeypatch.setenv("TS_PG_USER", "trading")
    monkeypatch.setenv("TS_SECRETS_PROVIDER", "systemd-creds")
    monkeypatch.setenv("CREDENTIALS_DIRECTORY", str(credentials_dir))
    monkeypatch.setenv("TS_PG_PASSWORD_FILE", str(password_file))

    assert platform.default_pg_dsn() == (
        "host=127.0.0.1 port=5432 user=trading dbname=trading password=file-backed-password"
    )
