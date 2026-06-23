from __future__ import annotations

import json
from pathlib import Path

import pytest


def _write_secret_file(path: Path, text: str = "file-backed-secret-value") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    path.chmod(0o600)
    return path


def _strict_env(repo_root: Path, **extra: str) -> dict[str, str]:
    env = {
        "PROD_LOCK": "1",
        "ENGINE_MODE": "live",
        "TRADING_ENFORCE_SECRET_SOURCE_POLICY": "1",
        "TRADING_SECRET_POLICY_REPO_ROOT": str(repo_root),
    }
    env.update(extra)
    return env


def _scrub_ambient_pg_secret_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in (
        "TS_PG_DSN",
        "TS_PG_PASSWORD",
        "TS_PG_PASSWORD_APP",
        "TS_PG_APP_PASSWORD",
        "PGPASSWORD",
    ):
        monkeypatch.delenv(key, raising=False)


def test_repo_env_inventory_reports_secret_keys_without_values(tmp_path: Path) -> None:
    from engine.runtime.secret_sources import repo_local_secret_key_inventory

    repo_env_value = "repo-local-secret-value"
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "SAFE_SETTING=visible",
                "ALPACA_SECRET_KEY=" + repo_env_value,
                "TIMESCALE_DSN=host=db user=trading password=pg-inline-secret dbname=trading",
                "",
            ]
        ),
        encoding="utf-8",
    )

    inventory = repo_local_secret_key_inventory(tmp_path)
    rendered = json.dumps(inventory, sort_keys=True)

    assert {item["key"] for item in inventory} == {"ALPACA_SECRET_KEY", "TIMESCALE_DSN"}
    assert repo_env_value not in rendered
    assert "pg-inline-secret" not in rendered


def test_repo_env_inventory_includes_codex_sim_backup_envs_without_values(tmp_path: Path) -> None:
    from engine.runtime.secret_sources import repo_local_secret_key_inventory

    inline_value = "codex-sim-backup-inline-token"
    (tmp_path / ".env.codex-sim-paper.bak").write_text(
        f"DASHBOARD_API_TOKEN={inline_value}\n",
        encoding="utf-8",
    )
    compose_dir = tmp_path / "deploy" / "compose"
    compose_dir.mkdir(parents=True)
    (compose_dir / ".env.codex-sim-paper.bak").write_text(
        f"REDIS_PASSWORD={inline_value}\n",
        encoding="utf-8",
    )

    inventory = repo_local_secret_key_inventory(tmp_path)
    rendered = json.dumps(inventory, sort_keys=True)

    assert {item["key"] for item in inventory} == {"DASHBOARD_API_TOKEN", "REDIS_PASSWORD"}
    assert ".env.codex-sim-paper.bak" in rendered
    assert "deploy/compose/.env.codex-sim-paper.bak" in rendered
    assert inline_value not in rendered


def test_strict_policy_rejects_inline_process_secret_without_value_leak(tmp_path: Path) -> None:
    from engine.runtime.secret_sources import secret_source_policy_snapshot

    inline_value = "inline-dashboard-token-value"
    snapshot = secret_source_policy_snapshot(
        environ=_strict_env(tmp_path, DASHBOARD_API_TOKEN=inline_value),
        repo_root=tmp_path,
    )
    rendered = json.dumps(snapshot, sort_keys=True)

    assert snapshot["ok"] is False
    assert "inline_secret_env:DASHBOARD_API_TOKEN" in snapshot["blockers"]
    assert inline_value not in rendered


def test_strict_policy_rejects_repo_local_env_secret_without_value_leak(tmp_path: Path) -> None:
    from engine.runtime.secret_sources import secret_source_policy_snapshot

    repo_env_value = "repo-dashboard-token-value"
    (tmp_path / ".env").write_text(f"DASHBOARD_API_TOKEN={repo_env_value}\n", encoding="utf-8")

    snapshot = secret_source_policy_snapshot(environ=_strict_env(tmp_path), repo_root=tmp_path)
    rendered = json.dumps(snapshot, sort_keys=True)

    assert snapshot["ok"] is False
    assert "repo_local_inline_secret:DASHBOARD_API_TOKEN" in snapshot["blockers"]
    assert ".env" in rendered
    assert repo_env_value not in rendered


def test_strict_policy_accepts_secret_file_source(tmp_path: Path) -> None:
    from engine.runtime.secret_sources import secret_source_policy_snapshot

    secret_file = _write_secret_file(tmp_path / "secrets" / "dashboard_api_token")
    snapshot = secret_source_policy_snapshot(
        environ=_strict_env(tmp_path, DASHBOARD_API_TOKEN_FILE=str(secret_file)),
        repo_root=tmp_path,
        validate_files=True,
    )

    assert snapshot["ok"] is True
    assert snapshot["blockers"] == []
    assert any(source.get("env") == "DASHBOARD_API_TOKEN_FILE" for source in snapshot["approved_sources"])


def test_strict_policy_rejects_missing_secret_file_source(tmp_path: Path) -> None:
    from engine.runtime.secret_sources import secret_source_policy_snapshot

    missing_file = tmp_path / "secrets" / "dashboard_api_token"
    snapshot = secret_source_policy_snapshot(
        environ=_strict_env(tmp_path, DASHBOARD_API_TOKEN_FILE=str(missing_file)),
        repo_root=tmp_path,
        validate_files=True,
    )

    assert snapshot["ok"] is False
    assert "secret_file_invalid:DASHBOARD_API_TOKEN" in snapshot["blockers"]
    assert any(
        violation.get("source_env") == "DASHBOARD_API_TOKEN_FILE"
        and violation.get("issue") == "missing"
        for violation in snapshot["violations"]
    )


def test_strict_policy_rejects_empty_secret_file_source(tmp_path: Path) -> None:
    from engine.runtime.secret_sources import secret_source_policy_snapshot

    empty_file = _write_secret_file(tmp_path / "secrets" / "dashboard_api_token", "")
    snapshot = secret_source_policy_snapshot(
        environ=_strict_env(tmp_path, DASHBOARD_API_TOKEN_FILE=str(empty_file)),
        repo_root=tmp_path,
        validate_files=True,
    )

    assert snapshot["ok"] is False
    assert "secret_file_invalid:DASHBOARD_API_TOKEN" in snapshot["blockers"]
    assert any(violation.get("issue") == "empty" for violation in snapshot["violations"])


def test_strict_policy_rejects_placeholder_secret_file_source(tmp_path: Path) -> None:
    from engine.runtime.secret_sources import secret_source_policy_snapshot

    snapshot = secret_source_policy_snapshot(
        environ=_strict_env(tmp_path, DASHBOARD_API_TOKEN_FILE="/dev/null"),
        repo_root=tmp_path,
        validate_files=True,
    )

    assert snapshot["ok"] is False
    assert "secret_file_invalid:DASHBOARD_API_TOKEN" in snapshot["blockers"]
    assert any(violation.get("issue") == "placeholder_path" for violation in snapshot["violations"])


def test_strict_policy_rejects_unreadable_secret_file_source(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import engine.runtime.secret_sources as secret_sources

    secret_file = _write_secret_file(tmp_path / "secrets" / "dashboard_api_token")
    original_access = secret_sources.os.access

    def fake_access(path: object, mode: int) -> bool:
        if Path(path) == secret_file:
            return False
        return original_access(path, mode)

    monkeypatch.setattr(secret_sources.os, "access", fake_access)

    snapshot = secret_sources.secret_source_policy_snapshot(
        environ=_strict_env(tmp_path, DASHBOARD_API_TOKEN_FILE=str(secret_file)),
        repo_root=tmp_path,
        validate_files=True,
    )

    assert snapshot["ok"] is False
    assert "secret_file_invalid:DASHBOARD_API_TOKEN" in snapshot["blockers"]
    assert any(violation.get("issue") == "not_readable" for violation in snapshot["violations"])


def test_strict_policy_validates_missing_docker_secret_mount(tmp_path: Path) -> None:
    from engine.runtime.secret_sources import secret_source_policy_snapshot

    snapshot = secret_source_policy_snapshot(
        environ=_strict_env(
            tmp_path,
            DASHBOARD_API_TOKEN_FILE="/run/secrets/trading_unit_test_missing_dashboard_api_token",
        ),
        repo_root=tmp_path,
        validate_files=True,
    )

    assert snapshot["ok"] is False
    assert "secret_file_invalid:DASHBOARD_API_TOKEN" in snapshot["blockers"]
    assert any(violation.get("issue") == "missing" for violation in snapshot["violations"])


def test_disabled_optional_provider_placeholders_do_not_block_strict_policy(tmp_path: Path) -> None:
    from engine.runtime.secret_sources import secret_source_policy_snapshot

    snapshot = secret_source_policy_snapshot(
        environ=_strict_env(
            tmp_path,
            POLYGON_REST_ENABLED="0",
            POLYGON_WS_ENABLED="0",
            TRADIER_ENABLED="0",
            BROKER="ibkr",
            BROKER_NAME="ibkr",
            LIVE_BROKER="ibkr",
            POLYGON_API_KEY_FILE="/dev/null",
            POLYGON_KEY_FILE="/dev/null",
            TRADIER_API_TOKEN_FILE="/dev/null",
            ALPACA_KEY_ID_FILE="/dev/null",
            ALPACA_SECRET_KEY_FILE="/dev/null",
        ),
        repo_root=tmp_path,
        validate_files=True,
    )

    assert snapshot["ok"] is True
    assert snapshot["blockers"] == []


def test_enabled_polygon_requires_valid_secret_source(tmp_path: Path) -> None:
    from engine.runtime.secret_sources import secret_source_policy_snapshot

    missing = secret_source_policy_snapshot(
        environ=_strict_env(tmp_path, POLYGON_REST_ENABLED="1"),
        repo_root=tmp_path,
        validate_files=True,
    )
    assert missing["ok"] is False
    assert "required_secret_source_missing:POLYGON_API_KEY" in missing["blockers"]

    placeholder = secret_source_policy_snapshot(
        environ=_strict_env(tmp_path, POLYGON_REST_ENABLED="1", POLYGON_API_KEY_FILE="/dev/null"),
        repo_root=tmp_path,
        validate_files=True,
    )
    assert placeholder["ok"] is False
    assert "secret_file_invalid:POLYGON_API_KEY" in placeholder["blockers"]

    secret_file = _write_secret_file(tmp_path / "secrets" / "polygon_api_key")
    allowed = secret_source_policy_snapshot(
        environ=_strict_env(tmp_path, POLYGON_REST_ENABLED="1", POLYGON_API_KEY_FILE=str(secret_file)),
        repo_root=tmp_path,
        validate_files=True,
    )
    assert allowed["ok"] is True
    assert allowed["blockers"] == []


def test_enabled_tradier_requires_valid_secret_source(tmp_path: Path) -> None:
    from engine.runtime.secret_sources import secret_source_policy_snapshot

    missing = secret_source_policy_snapshot(
        environ=_strict_env(tmp_path, TRADIER_ENABLED="1"),
        repo_root=tmp_path,
        validate_files=True,
    )
    assert missing["ok"] is False
    assert "required_secret_source_missing:TRADIER_API_TOKEN" in missing["blockers"]

    secret_file = _write_secret_file(tmp_path / "secrets" / "tradier_api_token")
    allowed = secret_source_policy_snapshot(
        environ=_strict_env(tmp_path, TRADIER_ENABLED="1", TRADIER_API_TOKEN_FILE=str(secret_file)),
        repo_root=tmp_path,
        validate_files=True,
    )
    assert allowed["ok"] is True


def test_enabled_alpaca_requires_key_id_and_secret_sources(tmp_path: Path) -> None:
    from engine.runtime.secret_sources import secret_source_policy_snapshot

    key_file = _write_secret_file(tmp_path / "secrets" / "alpaca_key_id")
    missing_secret = secret_source_policy_snapshot(
        environ=_strict_env(tmp_path, BROKER="alpaca", ALPACA_KEY_ID_FILE=str(key_file)),
        repo_root=tmp_path,
        validate_files=True,
    )
    assert missing_secret["ok"] is False
    assert "required_secret_source_missing:ALPACA_SECRET_KEY" in missing_secret["blockers"]

    secret_file = _write_secret_file(tmp_path / "secrets" / "alpaca_secret_key")
    allowed = secret_source_policy_snapshot(
        environ=_strict_env(
            tmp_path,
            BROKER="alpaca",
            ALPACA_KEY_ID_FILE=str(key_file),
            ALPACA_SECRET_KEY_FILE=str(secret_file),
        ),
        repo_root=tmp_path,
        validate_files=True,
    )
    assert allowed["ok"] is True
    assert allowed["blockers"] == []


def test_config_schema_rejects_inline_secret_sources(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from engine.runtime.config_schema import ConfigError, validate_production_secret_sources

    inline_value = "inline-config-token-value"
    _scrub_ambient_pg_secret_env(monkeypatch)
    monkeypatch.setenv("PROD_LOCK", "1")
    monkeypatch.setenv("ENGINE_MODE", "live")
    monkeypatch.setenv("TRADING_ENFORCE_SECRET_SOURCE_POLICY", "1")
    monkeypatch.setenv("TRADING_SECRET_POLICY_REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("DASHBOARD_API_TOKEN", inline_value)

    with pytest.raises(ConfigError) as excinfo:
        validate_production_secret_sources({"strict_runtime": True})

    message = str(excinfo.value)
    assert "inline_secret_env:DASHBOARD_API_TOKEN" in message
    assert inline_value not in message


def test_config_schema_accepts_file_secret_sources(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from engine.runtime.config_schema import validate_production_secret_sources

    secret_file = _write_secret_file(tmp_path / "secrets" / "dashboard_api_token")
    _scrub_ambient_pg_secret_env(monkeypatch)
    monkeypatch.setenv("PROD_LOCK", "1")
    monkeypatch.setenv("ENGINE_MODE", "live")
    monkeypatch.setenv("TRADING_ENFORCE_SECRET_SOURCE_POLICY", "1")
    monkeypatch.setenv("TRADING_SECRET_POLICY_REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("DASHBOARD_API_TOKEN_FILE", str(secret_file))
    monkeypatch.delenv("DASHBOARD_API_TOKEN", raising=False)

    snapshot = validate_production_secret_sources({"strict_runtime": True})

    assert snapshot["ok"] is True


def test_token_loaders_accept_provider_default_secret_names(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from engine.api.auth_config import dashboard_api_token_from_env
    from engine.runtime.live_trading_preflight import operator_api_token_from_env

    secrets_dir = tmp_path / "systemd-creds"
    dashboard_value = "dashboard-provider-token-value"
    operator_value = "operator-provider-token-value"
    _write_secret_file(secrets_dir / "dashboard_api_token", dashboard_value)
    _write_secret_file(secrets_dir / "operator_api_token", operator_value)

    monkeypatch.setenv("TS_SECRETS_PROVIDER", "systemd-creds")
    monkeypatch.setenv("CREDENTIALS_DIRECTORY", str(secrets_dir))
    monkeypatch.delenv("DASHBOARD_API_TOKEN", raising=False)
    monkeypatch.delenv("DASHBOARD_API_TOKEN_FILE", raising=False)
    monkeypatch.delenv("DASHBOARD_API_TOKEN_SECRET", raising=False)
    monkeypatch.delenv("OPERATOR_API_TOKEN", raising=False)
    monkeypatch.delenv("OPERATOR_API_TOKEN_FILE", raising=False)
    monkeypatch.delenv("OPERATOR_API_TOKEN_SECRET", raising=False)

    assert dashboard_api_token_from_env() == dashboard_value
    assert operator_api_token_from_env() == operator_value


def test_runtime_test_isolation_scrubs_dashboard_token_indirections(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from engine.runtime import test_isolation

    monkeypatch.setenv("DASHBOARD_API_TOKEN", "inline-dashboard-token")
    monkeypatch.setenv("DASHBOARD_API_TOKEN_FILE", "data/secrets/dashboard_api_token")
    monkeypatch.setenv("DASHBOARD_API_TOKEN_SECRET", "dashboard_api_token")
    monkeypatch.setattr(test_isolation, "_BASE_TEST_ENV", None)

    test_isolation.reset_runtime_test_env()

    assert "DASHBOARD_API_TOKEN" not in test_isolation.os.environ
    assert "DASHBOARD_API_TOKEN_FILE" not in test_isolation.os.environ
    assert "DASHBOARD_API_TOKEN_SECRET" not in test_isolation.os.environ


def test_live_preflight_secret_policy_fails_inline_and_passes_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from engine.runtime.live_trading_preflight import production_secret_sources_snapshot

    inline_value = "inline-live-token-value"
    _scrub_ambient_pg_secret_env(monkeypatch)
    monkeypatch.setenv("PROD_LOCK", "1")
    monkeypatch.setenv("ENGINE_MODE", "live")
    monkeypatch.setenv("TRADING_ENFORCE_SECRET_SOURCE_POLICY", "1")
    monkeypatch.setenv("TRADING_SECRET_POLICY_REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("DASHBOARD_API_TOKEN", inline_value)

    blocked = production_secret_sources_snapshot()
    blocked_rendered = json.dumps(blocked, sort_keys=True)
    assert blocked["ok"] is False
    assert "inline_secret_env:DASHBOARD_API_TOKEN" in blocked["blockers"]
    assert inline_value not in blocked_rendered

    secret_file = _write_secret_file(tmp_path / "secrets" / "dashboard_api_token")
    monkeypatch.delenv("DASHBOARD_API_TOKEN", raising=False)
    monkeypatch.setenv("DASHBOARD_API_TOKEN_FILE", str(secret_file))

    allowed = production_secret_sources_snapshot()
    assert allowed["ok"] is True
    assert allowed["blockers"] == []
