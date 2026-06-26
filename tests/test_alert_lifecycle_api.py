from __future__ import annotations

import sqlite3

from engine.api import api_read, api_write


class _NoCloseConnection:
    def __init__(self, con: sqlite3.Connection) -> None:
        self._con = con

    def execute(self, *args, **kwargs):
        return self._con.execute(*args, **kwargs)

    def close(self) -> None:
        pass


def _create_alerts_table(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE alerts (
          id INTEGER PRIMARY KEY,
          ts_ms INTEGER,
          severity TEXT,
          symbol TEXT,
          horizon_s INTEGER,
          expected_z REAL,
          confidence REAL,
          event_title TEXT,
          rule_id TEXT,
          explain_json TEXT
        )
        """
    )


def _alert_lifecycle_counts(con: sqlite3.Connection) -> dict[str, int]:
    return {
        table: int(con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] or 0)
        for table in ("alert_acks", "alert_resolutions", "alert_lifecycle_events")
    }


def test_alert_ack_expiry_shelving_and_lifecycle_are_read_back(monkeypatch):
    con = sqlite3.connect(":memory:")
    wrapped = _NoCloseConnection(con)
    _create_alerts_table(con)
    con.execute(
        """
        INSERT INTO alerts
        (id, ts_ms, severity, symbol, horizon_s, expected_z, confidence, event_title, rule_id, explain_json)
        VALUES (1, 1000, 'HIGH', 'SPY', 60, 2.5, 0.9, 'Risk elevated', 'risk_rule', '{}')
        """
    )

    monkeypatch.setattr(api_write, "run_write_txn", lambda fn, **_kwargs: fn(wrapped))
    monkeypatch.setattr(api_read, "_db_connect", lambda: wrapped)
    monkeypatch.setattr(api_read, "cache_get_or_load", lambda _ns, _key, loader, **_kwargs: loader())
    monkeypatch.setattr(api_write, "_now_ms", lambda: 1_000_000)
    monkeypatch.setattr(api_read.time, "time", lambda: 2_000.0)

    ack = api_write.ack_alert(1, who="operator", source="test", reason="investigating", timeout_ms=1)
    shelf = api_write.shelve_alert(
        1,
        who="operator",
        reason="known upstream outage",
        source="test",
        duration_ms=3_600_000,
        severity="HIGH",
    )
    assert ack["ok"] is True
    assert shelf["ok"] is True

    payload = api_read.get_alerts()
    assert payload["ok"] is True
    row = payload["rows"][0]
    assert row["severity"] == "HIGH"
    assert row["acked"] is False
    assert row["ack_expired"] is True
    assert row["lifecycle_state"] == "shelved"
    assert row["shelved"] is True
    assert row["shelve_reason"] == "known upstream outage"
    assert row["notification_policy"]["suppressed"] is True
    assert row["notification_policy"]["rate_limit_ms"] == 10 * 60 * 1000
    assert row["notification_policy"]["next_escalation_ts_ms"] == shelf["expires_ts_ms"]
    states = [item["state"] for item in row["lifecycle"]]
    assert "acknowledged" in states
    assert "retriggered" in states
    assert "shelved" in states


def test_alert_lifecycle_unknown_id_returns_not_found_without_audit_rows(monkeypatch):
    con = sqlite3.connect(":memory:")
    wrapped = _NoCloseConnection(con)
    _create_alerts_table(con)
    api_write._ensure_alert_lifecycle_schema(con)
    monkeypatch.setattr(api_write, "run_write_txn", lambda fn, **_kwargs: fn(wrapped))
    monkeypatch.setattr(api_write, "_now_ms", lambda: 1_000_000)

    before = _alert_lifecycle_counts(con)
    ack = api_write.ack_alert(999_999, who="operator", source="test", reason="investigating", timeout_ms=60_000)
    after_ack = _alert_lifecycle_counts(con)
    resolved = api_write.resolve_alert(999_999, who="operator", reason="done", source="test")
    after_resolve = _alert_lifecycle_counts(con)
    shelf = api_write.shelve_alert(
        999_999,
        who="operator",
        reason="known upstream outage",
        source="test",
        duration_ms=60_000,
        severity="WARN",
    )
    after_shelf = _alert_lifecycle_counts(con)

    assert before == {"alert_acks": 0, "alert_resolutions": 0, "alert_lifecycle_events": 0}
    assert ack == {"ok": False, "error": "not_found", "alert_id": 999_999, "meta": {"status": 404}}
    assert resolved == {"ok": False, "error": "not_found", "alert_id": 999_999, "meta": {"status": 404}}
    assert shelf == {"ok": False, "error": "not_found", "alert_id": 999_999, "meta": {"status": 404}}
    assert after_ack == before
    assert after_resolve == before
    assert after_shelf == before
    assert con.execute("SELECT COUNT(*) FROM alert_shelves").fetchone()[0] == 0


def test_alert_ack_and_resolve_existing_id_preserve_state_idempotency(monkeypatch):
    con = sqlite3.connect(":memory:")
    wrapped = _NoCloseConnection(con)
    _create_alerts_table(con)
    con.execute(
        """
        INSERT INTO alerts
        (id, ts_ms, severity, symbol, horizon_s, expected_z, confidence, event_title, rule_id, explain_json)
        VALUES (1, 1000, 'HIGH', 'SPY', 60, 2.5, 0.9, 'Risk elevated', 'risk_rule', '{}')
        """
    )
    monkeypatch.setattr(api_write, "run_write_txn", lambda fn, **_kwargs: fn(wrapped))
    monkeypatch.setattr(api_write, "_now_ms", lambda: 1_000_000)

    before = _alert_lifecycle_counts(con) if con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='alert_acks'"
    ).fetchone() else {"alert_acks": 0, "alert_resolutions": 0, "alert_lifecycle_events": 0}
    first_ack = api_write.ack_alert(1, who="operator", source="test", reason="investigating", timeout_ms=60_000)
    second_ack = api_write.ack_alert(1, who="operator2", source="test", reason="still investigating", timeout_ms=120_000)
    first_resolve = api_write.resolve_alert(1, who="operator", reason="done", source="test")
    second_resolve = api_write.resolve_alert(1, who="operator2", reason="still done", source="test")
    after = _alert_lifecycle_counts(con)

    assert before == {"alert_acks": 0, "alert_resolutions": 0, "alert_lifecycle_events": 0}
    assert first_ack["ok"] is True
    assert second_ack["ok"] is True
    assert first_resolve["ok"] is True
    assert second_resolve["ok"] is True
    assert after["alert_acks"] == 1
    assert after["alert_resolutions"] == 1
    assert after["alert_lifecycle_events"] == 4
    assert con.execute("SELECT acked_by, reason, expires_ts_ms FROM alert_acks WHERE alert_id = 1").fetchone() == (
        "operator2",
        "still investigating",
        1_120_000,
    )
    assert con.execute("SELECT resolved_by, reason FROM alert_resolutions WHERE alert_id = 1").fetchone() == (
        "operator",
        "done",
    )


def test_alert_shelving_requires_reason(monkeypatch):
    con = sqlite3.connect(":memory:")
    wrapped = _NoCloseConnection(con)
    monkeypatch.setattr(api_write, "run_write_txn", lambda fn, **_kwargs: fn(wrapped))

    result = api_write.shelve_alert(1, who="operator", reason="", duration_ms=60_000)

    assert result["ok"] is False
    assert result["error"] == "shelve_reason_required"


def test_alert_shelving_requires_expiry(monkeypatch):
    con = sqlite3.connect(":memory:")
    wrapped = _NoCloseConnection(con)
    monkeypatch.setattr(api_write, "run_write_txn", lambda fn, **_kwargs: fn(wrapped))

    result = api_write.shelve_alert(1, who="operator", reason="known upstream outage", severity="WARN")

    assert result["ok"] is False
    assert result["error"] == "shelve_expiry_required"
    assert result["meta"]["status"] == 422


def test_alert_shelving_enforces_severity_constraints(monkeypatch):
    con = sqlite3.connect(":memory:")
    wrapped = _NoCloseConnection(con)
    con.execute("CREATE TABLE alerts (id INTEGER PRIMARY KEY, severity TEXT)")
    con.execute("INSERT INTO alerts (id, severity) VALUES (1, 'CRIT')")
    con.execute("INSERT INTO alerts (id, severity) VALUES (2, 'HIGH')")
    monkeypatch.setattr(api_write, "run_write_txn", lambda fn, **_kwargs: fn(wrapped))
    monkeypatch.setattr(api_write, "_now_ms", lambda: 1_000_000)
    monkeypatch.delenv("ALERT_SHELVE_ALLOW_CRIT", raising=False)

    crit = api_write.shelve_alert(
        1,
        who="operator",
        reason="critical alert investigation",
        duration_ms=60_000,
        severity="CRIT",
    )
    high_too_long = api_write.shelve_alert(
        2,
        who="operator",
        reason="known high severity upstream outage",
        duration_ms=5 * 60 * 60 * 1000,
        severity="HIGH",
    )

    assert crit["ok"] is False
    assert crit["error"] == "critical_shelving_blocked"
    assert crit["meta"]["status"] == 422
    assert high_too_long["ok"] is False
    assert high_too_long["error"] == "shelve_expiry_too_long"
    assert high_too_long["severity"] == "HIGH"
    assert high_too_long["meta"]["status"] == 422


def test_alert_shelving_uses_stored_severity_when_payload_omits_it(monkeypatch):
    con = sqlite3.connect(":memory:")
    wrapped = _NoCloseConnection(con)
    con.execute("CREATE TABLE alerts (id INTEGER PRIMARY KEY, severity TEXT)")
    con.execute("INSERT INTO alerts (id, severity) VALUES (1, 'CRIT')")
    monkeypatch.setattr(api_write, "run_write_txn", lambda fn, **_kwargs: fn(wrapped))
    monkeypatch.setattr(api_write, "_now_ms", lambda: 1_000_000)
    monkeypatch.delenv("ALERT_SHELVE_ALLOW_CRIT", raising=False)

    result = api_write.shelve_alert(
        1,
        who="operator",
        reason="critical alert investigation",
        duration_ms=60_000,
    )

    assert result["ok"] is False
    assert result["error"] == "critical_shelving_blocked"
    assert result["severity"] == "CRIT"
    assert con.execute("SELECT COUNT(*) FROM alert_shelves").fetchone()[0] == 0
