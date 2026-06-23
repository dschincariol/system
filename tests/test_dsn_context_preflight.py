from __future__ import annotations

import json
import socket
from pathlib import Path


def _resolver_ok(host: str, port: int, *args, **kwargs):
    return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", int(port or 0)))]


def test_host_context_accepts_loopback_passwordless_dsn() -> None:
    from engine.runtime.dsn_context import dsn_context_snapshot

    snapshot = dsn_context_snapshot(
        environ={
            "TRADING_DSN_CONTEXT": "host",
            "TRADING_DSN_PREFLIGHT_REQUIRED": "1",
            "TS_PG_DSN": "host=127.0.0.1 port=5432 user=trading dbname=trading",
            "TIMESCALE_DSN": "postgresql://trading@127.0.0.1:5432/trading",
            "REDIS_URL": "redis://127.0.0.1:6379/0",
            "OBJECT_STORE_ENDPOINT": "http://127.0.0.1:9000",
        },
        resolver=_resolver_ok,
    )

    assert snapshot["ok"] is True
    assert snapshot["blockers"] == []
    assert {entry["key"] for entry in snapshot["entries"]} == {
        "TS_PG_DSN",
        "TIMESCALE_DSN",
        "REDIS_URL",
        "OBJECT_STORE_ENDPOINT",
    }


def test_host_context_rejects_compose_service_hostname_without_secret_leak() -> None:
    from engine.runtime.dsn_context import dsn_context_snapshot

    canary = "dsn-context-inline-secret-canary"
    snapshot = dsn_context_snapshot(
        environ={
            "TRADING_DSN_CONTEXT": "host",
            "TRADING_DSN_PREFLIGHT_REQUIRED": "1",
            "TIMESCALE_DSN": f"postgresql://trading:{canary}@timescaledb:5432/trading",
        },
        resolver=_resolver_ok,
    )
    rendered = json.dumps(snapshot, sort_keys=True)

    assert snapshot["ok"] is False
    assert "dsn_context_invalid:TIMESCALE_DSN:container_hostname_in_host_context" in snapshot["blockers"]
    assert "timescaledb" in rendered
    assert canary not in rendered


def test_container_context_rejects_loopback_service_dsn() -> None:
    from engine.runtime.dsn_context import dsn_context_snapshot

    snapshot = dsn_context_snapshot(
        environ={
            "TRADING_DSN_CONTEXT": "container",
            "TRADING_DSN_PREFLIGHT_REQUIRED": "1",
            "TS_PG_DSN": "host=127.0.0.1 port=5432 user=trading dbname=trading",
        },
        resolver=_resolver_ok,
    )

    assert snapshot["ok"] is False
    assert "dsn_context_invalid:TS_PG_DSN:loopback_hostname_in_container_context" in snapshot["blockers"]


def test_unresolvable_hostname_is_masked_and_blocking() -> None:
    from engine.runtime.dsn_context import dsn_context_snapshot

    def resolver_fail(host: str, port: int, *args, **kwargs):
        raise socket.gaierror(-2, "Name or service not known")

    canary = "masked-dsn-password-canary"
    snapshot = dsn_context_snapshot(
        environ={
            "TRADING_DSN_CONTEXT": "host",
            "TRADING_DSN_PREFLIGHT_REQUIRED": "1",
            "TIMESCALE_PRICES_DSN": f"postgresql://trading:{canary}@db.invalid.local:5432/trading",
        },
        resolver=resolver_fail,
    )
    rendered = json.dumps(snapshot, sort_keys=True)

    assert snapshot["ok"] is False
    assert any(item.startswith("dsn_context_invalid:TIMESCALE_PRICES_DSN:hostname_unresolvable") for item in snapshot["blockers"])
    assert "db.invalid.local" in rendered
    assert canary not in rendered


def test_live_environment_contract_includes_dsn_context_blocker(monkeypatch) -> None:
    import engine.runtime.live_trading_preflight as preflight

    def fake_snapshot():
        return {
            "ok": False,
            "required": True,
            "context": "host",
            "blockers": ["dsn_context_invalid:TIMESCALE_DSN:container_hostname_in_host_context"],
            "warnings": [],
            "entries": [{"key": "TIMESCALE_DSN", "host": "timescaledb"}],
        }

    monkeypatch.setattr(preflight, "dsn_context_preflight_snapshot", fake_snapshot)
    monkeypatch.delenv("PROD_LOCK", raising=False)
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("EXECUTION_MODE", "safe")
    monkeypatch.setenv("DASHBOARD_HOST", "127.0.0.1")

    state = preflight.live_environment_contract_snapshot(engine_mode="safe", execution_mode="safe")

    assert state["ok"] is False
    assert "dsn_context_invalid:TIMESCALE_DSN:container_hostname_in_host_context" in state["blockers"]
    assert state["dsn_context"]["context"] == "host"


def test_secret_file_loader_reads_ts_pg_password_file(tmp_path: Path) -> None:
    from engine.runtime.secret_sources import read_secret_text_from_env

    canary = "pg-file-secret-canary"
    secret_file = tmp_path / "pg_password"
    secret_file.write_text(canary + "\n", encoding="utf-8")
    secret_file.chmod(0o600)

    loaded = read_secret_text_from_env(
        "TS_PG_PASSWORD",
        environ={"TS_PG_PASSWORD_FILE": str(secret_file)},
    )

    assert loaded == canary
