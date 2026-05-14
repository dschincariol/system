from __future__ import annotations

import pytest


def test_default_pg_dsn_uses_env_password_before_secret_provider(monkeypatch):
    from engine.runtime import platform
    from services.secrets import loader

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
