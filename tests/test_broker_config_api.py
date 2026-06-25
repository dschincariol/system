from __future__ import annotations

import base64
import sqlite3
from urllib.parse import urlparse

from engine.api import api_broker_config


class _NoCloseConnection:
    def __init__(self, con: sqlite3.Connection) -> None:
        self._con = con
        self.rollbacks = 0

    def execute(self, *args, **kwargs):
        return self._con.execute(*args, **kwargs)

    def rollback(self) -> None:
        self.rollbacks += 1
        self._con.rollback()

    def close(self) -> None:
        pass


def _patch_broker_storage(monkeypatch):
    con = sqlite3.connect(":memory:")
    wrapped = _NoCloseConnection(con)
    monkeypatch.setattr(api_broker_config, "connect_ro", lambda: wrapped)
    monkeypatch.setattr(api_broker_config, "run_write_txn", lambda fn, **_kwargs: fn(wrapped))
    return con


def _allow_broker_live_probe_tests(monkeypatch) -> None:
    monkeypatch.setenv("EXECUTION_MODE", "paper")
    monkeypatch.setenv("ENGINE_MODE", "paper")


def test_broker_config_masks_credentials_blocks_activation_until_test_and_audits(monkeypatch):
    _allow_broker_live_probe_tests(monkeypatch)
    monkeypatch.setenv("DATA_SOURCE_MASTER_KEY", base64.b64encode(bytes(range(32))).decode("ascii"))
    _patch_broker_storage(monkeypatch)
    probe_calls = []

    def fake_alpaca_probe(config, credentials):
        probe_calls.append((dict(config), dict(credentials)))
        return {
            "ok": True,
            "state": "connected",
            "test_kind": "alpaca_account_read",
            "account_status": "ACTIVE",
            "reasons": [],
        }

    monkeypatch.setitem(api_broker_config._BROKER_CONNECTION_PROBES, "alpaca", fake_alpaca_probe)

    saved = api_broker_config.api_post_broker_config(
        urlparse("/api/broker/config"),
        {
            "active_broker": "alpaca",
            "paper_live_mode": "paper",
            "base_url": "https://paper-api.alpaca.markets",
            "credentials": {"api_key": "KEY12345", "secret": "SECRET98765"},
            "active": False,
            "disabled": True,
            "actor": "pytest",
        },
        {},
    )
    assert saved["ok"] is True
    cfg = saved["config"]
    assert cfg["secrets_masked"] is True
    assert cfg["credentials_configured"] is True
    assert cfg["masked_credentials"]["api_key"] != "KEY12345"
    assert "KEY12345" not in str(cfg)
    assert "SECRET98765" not in str(cfg)

    blocked = api_broker_config.api_post_broker_config(
        urlparse("/api/broker/config"),
        {
            "active_broker": "alpaca",
            "paper_live_mode": "paper",
            "base_url": "https://paper-api.alpaca.markets",
            "active": True,
            "disabled": False,
            "actor": "pytest",
        },
        {},
    )
    assert blocked["ok"] is False
    assert blocked["error"] == "broker_test_required"

    tested = api_broker_config.api_post_broker_test_connection(
        urlparse("/api/broker/test_connection"),
        {
            "active_broker": "alpaca",
            "paper_live_mode": "paper",
            "base_url": "https://paper-api.alpaca.markets",
            "actor": "pytest",
        },
        {},
    )
    assert tested["ok"] is True
    assert tested["broker"] == "alpaca"
    assert tested["test_kind"] == "alpaca_account_read"
    assert tested["non_mutating"] is True
    assert tested["credential_status"]["configured"] is True
    assert "KEY12345" not in str(tested)
    assert "SECRET98765" not in str(tested)
    assert probe_calls
    assert probe_calls[-1][1]["api_key"] == "KEY12345"

    activated = api_broker_config.api_post_broker_config(
        urlparse("/api/broker/config"),
        {
            "active_broker": "alpaca",
            "paper_live_mode": "paper",
            "base_url": "https://paper-api.alpaca.markets",
            "active": True,
            "disabled": False,
            "actor": "pytest",
        },
        {},
    )
    assert activated["ok"] is True
    assert activated["config"]["active"] is True
    assert activated["config"]["disabled"] is False

    audit = api_broker_config.api_get_broker_audit(urlparse("/api/broker/audit"), {}, {})
    actions = [row["action"] for row in audit["rows"]]
    assert "activation_blocked" in actions
    assert "test_connection" in actions
    assert "activation" in actions
    assert "config_update" in actions
    assert "KEY12345" not in str(audit)


def test_broker_config_activation_rejects_stale_connection_test(monkeypatch):
    _allow_broker_live_probe_tests(monkeypatch)
    monkeypatch.setenv("DATA_SOURCE_MASTER_KEY", base64.b64encode(bytes(range(32))).decode("ascii"))
    monkeypatch.setenv("BROKER_CONNECTION_TEST_MAX_AGE_S", "60")
    _patch_broker_storage(monkeypatch)
    monkeypatch.setitem(
        api_broker_config._BROKER_CONNECTION_PROBES,
        "alpaca",
        lambda _config, _credentials: {"ok": True, "state": "connected", "reasons": []},
    )

    monkeypatch.setattr(api_broker_config, "_now_ms", lambda: 1_000_000)
    tested = api_broker_config.api_post_broker_test_connection(
        urlparse("/api/broker/test_connection"),
        {
            "active_broker": "alpaca",
            "paper_live_mode": "paper",
            "base_url": "https://paper-api.alpaca.markets",
            "credentials": {"api_key": "KEY12345", "secret": "SECRET98765"},
            "actor": "pytest",
        },
        {},
    )
    assert tested["ok"] is True

    monkeypatch.setattr(api_broker_config, "_now_ms", lambda: 1_000_000 + 120_000)
    blocked = api_broker_config.api_post_broker_config(
        urlparse("/api/broker/config"),
        {
            "active_broker": "alpaca",
            "paper_live_mode": "paper",
            "base_url": "https://paper-api.alpaca.markets",
            "active": True,
            "disabled": False,
            "actor": "pytest",
        },
        {},
    )

    assert blocked["ok"] is False
    assert blocked["error"] == "broker_test_stale"
    assert blocked["test_max_age_ms"] == 60_000


def test_broker_config_test_connection_records_failure_without_raw_secret(monkeypatch):
    _allow_broker_live_probe_tests(monkeypatch)
    monkeypatch.setenv("DATA_SOURCE_MASTER_KEY", base64.b64encode(bytes(range(32))).decode("ascii"))
    _patch_broker_storage(monkeypatch)

    monkeypatch.setitem(
        api_broker_config._BROKER_CONNECTION_PROBES,
        "alpaca",
        lambda _config, _credentials: {
            "ok": False,
            "state": "auth_failed",
            "test_kind": "alpaca_account_read",
            "reasons": ["alpaca_auth_failed"],
        },
    )

    result = api_broker_config.api_post_broker_test_connection(
        urlparse("/api/broker/test_connection"),
        {
            "active_broker": "alpaca",
            "paper_live_mode": "paper",
            "base_url": "https://paper-api.alpaca.markets",
            "credentials": {"api_key": "BADKEY12345", "secret": "BADSECRET98765"},
            "actor": "pytest",
        },
        {},
    )

    assert result["ok"] is False
    assert result["state"] == "auth_failed"
    assert result["reasons"] == ["alpaca_auth_failed"]
    assert "BADKEY12345" not in str(result)
    assert "BADSECRET98765" not in str(result)

    audit = api_broker_config.api_get_broker_audit(urlparse("/api/broker/audit"), {}, {})
    assert audit["rows"][0]["action"] == "test_connection"
    assert audit["rows"][0]["success"] is False
    assert "BADKEY12345" not in str(audit)


def test_broker_config_activation_rejects_config_mismatch_after_test(monkeypatch):
    _allow_broker_live_probe_tests(monkeypatch)
    monkeypatch.setenv("DATA_SOURCE_MASTER_KEY", base64.b64encode(bytes(range(32))).decode("ascii"))
    _patch_broker_storage(monkeypatch)
    monkeypatch.setitem(
        api_broker_config._BROKER_CONNECTION_PROBES,
        "alpaca",
        lambda _config, _credentials: {"ok": True, "state": "connected", "reasons": []},
    )

    tested = api_broker_config.api_post_broker_test_connection(
        urlparse("/api/broker/test_connection"),
        {
            "active_broker": "alpaca",
            "paper_live_mode": "paper",
            "base_url": "https://paper-api.alpaca.markets",
            "credentials": {"api_key": "KEY12345", "secret": "SECRET98765"},
            "actor": "pytest",
        },
        {},
    )
    assert tested["ok"] is True

    blocked = api_broker_config.api_post_broker_config(
        urlparse("/api/broker/config"),
        {
            "active_broker": "alpaca",
            "paper_live_mode": "paper",
            "base_url": "https://api.alpaca.markets",
            "active": True,
            "disabled": False,
            "actor": "pytest",
        },
        {},
    )

    assert blocked["ok"] is False
    assert blocked["error"] == "broker_test_required"
    assert blocked["expected_config_fingerprint"] != tested["config_fingerprint"]


def test_broker_test_connection_safe_mode_skips_live_broker_probe(monkeypatch):
    monkeypatch.setenv("EXECUTION_MODE", "safe")
    monkeypatch.setenv("ENGINE_MODE", "safe")
    _patch_broker_storage(monkeypatch)
    probe_calls = []

    def forbidden_probe(_config, _credentials):
        probe_calls.append(True)
        raise AssertionError("safe mode must not dial a live broker")

    monkeypatch.setitem(api_broker_config._BROKER_CONNECTION_PROBES, "ibkr", forbidden_probe)

    result = api_broker_config.api_post_broker_test_connection(
        urlparse("/api/broker/test_connection"),
        {
            "active_broker": "ibkr",
            "paper_live_mode": "safe",
            "host": "127.0.0.1",
            "port": "7497",
            "client_id": "7",
            "actor": "pytest",
        },
        {},
    )

    assert result["ok"] is True
    assert result["broker"] == "ibkr"
    assert result["state"] == "safe_mode_live_probe_skipped"
    assert result["test_kind"] == "safe_mode_no_live_broker_probe"
    assert result["live_probe_skipped"] is True
    assert result["activation_eligible"] is False
    assert result["audit_persisted"] is True
    assert probe_calls == []

    blocked = api_broker_config.api_post_broker_config(
        urlparse("/api/broker/config"),
        {
            "active_broker": "ibkr",
            "paper_live_mode": "safe",
            "host": "127.0.0.1",
            "port": "7497",
            "client_id": "7",
            "active": True,
            "disabled": False,
            "actor": "pytest",
        },
        {},
    )
    assert blocked["ok"] is False
    assert blocked["error"] == "broker_test_required"


def test_broker_test_connection_rolls_back_cleanly_when_audit_write_fails(monkeypatch):
    monkeypatch.setenv("EXECUTION_MODE", "safe")
    monkeypatch.setenv("ENGINE_MODE", "safe")

    class FailingAuditConnection:
        def __init__(self) -> None:
            self.rollbacks = 0

        def execute(self, sql, params=None):
            if "INSERT INTO broker_config_audit" in str(sql):
                raise RuntimeError("audit insert failed")
            return _EmptyCursor()

        def rollback(self) -> None:
            self.rollbacks += 1

    class _EmptyCursor:
        def fetchone(self):
            return None

        def fetchall(self):
            return []

    con = FailingAuditConnection()
    monkeypatch.setattr(api_broker_config, "connect_ro", lambda: _NoCloseConnection(sqlite3.connect(":memory:")))
    monkeypatch.setattr(api_broker_config, "run_write_txn", lambda fn, **_kwargs: fn(con))

    result = api_broker_config.api_post_broker_test_connection(
        urlparse("/api/broker/test_connection"),
        {"active_broker": "sim", "paper_live_mode": "safe", "actor": "pytest"},
        {},
    )

    assert result["ok"] is False
    assert result["state"] == "audit_write_failed"
    assert result["audit_persisted"] is False
    assert result["reasons"] == ["broker_test_audit_write_failed"]
    assert con.rollbacks == 1
    assert "InFailedSqlTransaction" not in str(result)
