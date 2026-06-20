from __future__ import annotations

import base64
import sqlite3
from urllib.parse import urlparse

from engine.api import api_broker_config


class _NoCloseConnection:
    def __init__(self, con: sqlite3.Connection) -> None:
        self._con = con

    def execute(self, *args, **kwargs):
        return self._con.execute(*args, **kwargs)

    def close(self) -> None:
        pass


def _patch_broker_storage(monkeypatch):
    con = sqlite3.connect(":memory:")
    wrapped = _NoCloseConnection(con)
    monkeypatch.setattr(api_broker_config, "connect_ro", lambda: wrapped)
    monkeypatch.setattr(api_broker_config, "run_write_txn", lambda fn, **_kwargs: fn(wrapped))
    return con


def test_broker_config_masks_credentials_blocks_activation_until_test_and_audits(monkeypatch):
    monkeypatch.setenv("DATA_SOURCE_MASTER_KEY", base64.b64encode(bytes(range(32))).decode("ascii"))
    _patch_broker_storage(monkeypatch)

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
    assert actions.count("config_update") >= 2
    assert "KEY12345" not in str(audit)


def test_broker_config_activation_rejects_stale_connection_test(monkeypatch):
    monkeypatch.setenv("DATA_SOURCE_MASTER_KEY", base64.b64encode(bytes(range(32))).decode("ascii"))
    monkeypatch.setenv("BROKER_CONNECTION_TEST_MAX_AGE_S", "60")
    _patch_broker_storage(monkeypatch)

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
