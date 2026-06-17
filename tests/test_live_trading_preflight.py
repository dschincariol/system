from __future__ import annotations

import json
import time

import pytest

from engine.runtime.live_trading_preflight import (
    DEFAULT_LIVE_CONFIRM_PHRASE,
    live_trading_preflight,
)


@pytest.fixture(autouse=True)
def fresh_backup_restore_evidence(monkeypatch, tmp_path):
    now = time.time()
    monkeypatch.setenv("DB_PATH", str(tmp_path / "live_trading_preflight.db"))
    monkeypatch.setenv("TIMESCALE_ENABLED", "0")
    evidence_path = tmp_path / "latest_backup_restore_evidence.json"
    evidence_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "generated_at_ts": now,
                "status": "pass",
                "base_backup": {"status": "pass", "verified_at_ts": now},
                "wal_archive": {"status": "pass", "verified_at_ts": now},
                "restore_drill": {
                    "status": "pass",
                    "verified_at_ts": now,
                    "time_to_recover_s": 60,
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("BACKUP_EVIDENCE_PATH", str(evidence_path))
    monkeypatch.setenv("BACKUP_EVIDENCE_BASE_BACKUP_MAX_AGE_S", "3600")
    monkeypatch.setenv("BACKUP_EVIDENCE_RPO_S", "3600")
    monkeypatch.setenv("BACKUP_EVIDENCE_RESTORE_DRILL_MAX_AGE_S", "3600")
    monkeypatch.setenv("BACKUP_EVIDENCE_RTO_S", "300")
    monkeypatch.setenv("EXECUTION_MODE", "live")
    monkeypatch.setenv("KILL_SWITCH_GLOBAL", "1")
    monkeypatch.setenv("LIVE_TRADING_REQUIRE_CONFIRMATION", "1")
    monkeypatch.delenv("LIVE_TRADING_CONFIRM_PHRASE", raising=False)


def _set_live_broker_contract(monkeypatch, broker: str = "alpaca", *, credentials: bool = True) -> None:
    broker = str(broker or "alpaca").strip().lower()
    monkeypatch.setenv("BROKER", broker)
    monkeypatch.setenv("BROKER_NAME", broker)
    monkeypatch.setenv("LIVE_BROKER", broker)
    monkeypatch.setenv("BROKER_FAILOVER", broker)
    if broker == "alpaca":
        monkeypatch.setenv("ALPACA_BASE_URL", "https://api.alpaca.markets")
        if credentials:
            monkeypatch.setenv("ALPACA_KEY_ID", "alpaca-key")
            monkeypatch.setenv("ALPACA_SECRET_KEY", "alpaca-secret")
    elif broker == "ibkr" and credentials:
        monkeypatch.setenv("IBKR_HOST", "127.0.0.1")
        monkeypatch.setenv("IBKR_PORT", "7497")
        monkeypatch.setenv("IBKR_CLIENT_ID", "42")


def _clear_execution_mode_cache() -> None:
    from engine.cache import keys, store

    store.invalidate(keys.execution_mode())


def test_live_trading_preflight_requires_token_and_confirmation(monkeypatch):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE", "1")
    _set_live_broker_contract(monkeypatch)
    state = live_trading_preflight(
        engine_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="",
        live_confirm="",
    )

    assert state["ok"] is False
    assert "dashboard_api_token_required_for_live" in state["blockers"]
    assert "live_trading_confirmation_required" in state["blockers"]


def test_live_trading_preflight_accepts_explicit_live_acknowledgement(monkeypatch):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE", "1")
    _set_live_broker_contract(monkeypatch)
    state = live_trading_preflight(
        engine_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="live-token-1234567890",
        live_confirm=DEFAULT_LIVE_CONFIRM_PHRASE,
    )

    assert state["ok"] is True
    assert state["blockers"] == []


def test_live_trading_preflight_uses_fixed_confirmation_phrase(monkeypatch):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE", "1")
    monkeypatch.setenv("LIVE_TRADING_CONFIRM_PHRASE", "TRADE")
    _set_live_broker_contract(monkeypatch)

    state = live_trading_preflight(
        engine_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="live-token-1234567890",
        live_confirm="TRADE",
    )

    assert state["ok"] is False
    assert state["confirmation_phrase"] == DEFAULT_LIVE_CONFIRM_PHRASE
    assert "live_trading_confirmation_phrase_override_forbidden" in state["blockers"]
    assert "live_trading_confirmation_required" in state["blockers"]


def test_live_trading_preflight_rejects_disabled_confirmation_requirement(monkeypatch):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE", "1")
    monkeypatch.setenv("LIVE_TRADING_REQUIRE_CONFIRMATION", "0")
    _set_live_broker_contract(monkeypatch)

    state = live_trading_preflight(
        engine_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="live-token-1234567890",
        live_confirm=DEFAULT_LIVE_CONFIRM_PHRASE,
    )

    assert state["ok"] is False
    assert "live_trading_confirmation_cannot_be_disabled" in state["blockers"]
    assert state["confirmation"]["required"] is True


def test_live_trading_preflight_requires_execution_mode_live(monkeypatch):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE", "1")
    _set_live_broker_contract(monkeypatch)
    monkeypatch.setenv("EXECUTION_MODE", "safe")

    state = live_trading_preflight(
        engine_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="live-token-1234567890",
        live_confirm=DEFAULT_LIVE_CONFIRM_PHRASE,
    )

    assert state["ok"] is False
    assert state["reason"] == "execution_mode_live_required"
    assert "execution_mode_live_required" in state["blockers"]


@pytest.mark.parametrize("broker", ["sim", "paper", "sandbox"])
def test_live_trading_preflight_rejects_sim_as_live_broker(monkeypatch, broker):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE", "1")
    _set_live_broker_contract(monkeypatch, broker)

    state = live_trading_preflight(
        engine_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="live-token-1234567890",
        live_confirm=DEFAULT_LIVE_CONFIRM_PHRASE,
    )

    assert state["ok"] is False
    assert state["reason"] == "broker_must_be_live"
    assert "sim_broker_forbidden_in_live" in state["blockers"]
    assert state["broker_contract"]["broker"] == "sim"


def test_live_trading_preflight_requires_initial_global_kill_switch_hold(monkeypatch):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE", "1")
    _set_live_broker_contract(monkeypatch)
    monkeypatch.setenv("KILL_SWITCH_GLOBAL", "0")

    state = live_trading_preflight(
        engine_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="live-token-1234567890",
        live_confirm=DEFAULT_LIVE_CONFIRM_PHRASE,
    )

    assert state["ok"] is False
    assert state["reason"] == "kill_switch_global_initial_hold_required"
    assert state["initial_kill_switch_hold"]["required"] is True


def test_live_trading_preflight_rejects_live_armed_db_without_audit(monkeypatch):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE", "1")
    monkeypatch.setenv("KILL_SWITCH_GLOBAL", "0")
    _set_live_broker_contract(monkeypatch)

    from engine.execution import execution_mode
    from engine.runtime import storage

    con = storage.connect()
    try:
        execution_mode._ensure_schema(con)
        execution_mode._ensure_row(con)
        con.execute(
            """
            UPDATE execution_mode
            SET mode='live', armed=1, updated_ts_ms=?, actor='manual-db-edit', reason='tamper'
            WHERE id=1
            """,
            (int(time.time() * 1000),),
        )
        con.commit()
    finally:
        con.close()
    _clear_execution_mode_cache()

    state = live_trading_preflight(
        engine_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="live-token-1234567890",
        live_confirm=DEFAULT_LIVE_CONFIRM_PHRASE,
    )

    assert state["ok"] is False
    assert state["reason"] == "execution_mode_live_armed_audit_missing"
    assert "execution_mode_live_armed_audit_missing" in state["blockers"]
    assert state["execution_arming_audit"]["required"] is True


def test_live_trading_preflight_accepts_live_armed_db_with_audit(monkeypatch):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE", "1")
    monkeypatch.setenv("KILL_SWITCH_GLOBAL", "0")
    monkeypatch.setenv("LIVE_TRADING_CONFIRM", DEFAULT_LIVE_CONFIRM_PHRASE)
    _set_live_broker_contract(monkeypatch)

    from engine.execution import execution_mode
    from engine.runtime import storage

    storage.init_db()
    execution_mode.set_execution_mode("live", actor="ops", reason="signoff")
    execution_mode.set_execution_armed(1, actor="ops", reason="signoff")

    state = live_trading_preflight(
        engine_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="live-token-1234567890",
        live_confirm=DEFAULT_LIVE_CONFIRM_PHRASE,
    )

    assert state["ok"] is True
    assert state["execution_arming_audit"]["required"] is True
    assert state["execution_arming_audit"]["audit"]["row_hash_present"] is True
    assert state["initial_kill_switch_hold"]["signed_off"] is True


def test_execution_mode_refuses_live_arming_without_confirmation(monkeypatch):
    monkeypatch.delenv("LIVE_TRADING_CONFIRM", raising=False)

    from engine.cache.wrappers import execution_mode as cached_execution_mode
    from engine.execution import execution_mode
    from engine.runtime import storage

    storage.init_db()
    execution_mode.set_execution_mode("live", actor="ops", reason="signoff")

    with pytest.raises(RuntimeError) as direct_error:
        execution_mode.set_execution_armed(1, actor="ops", reason="signoff")
    assert "live_trading_confirmation_required" in str(direct_error.value)

    with pytest.raises(RuntimeError) as cached_error:
        cached_execution_mode.set_execution_mode("live", actor="ops", reason="signoff", armed=1)
    assert "live_trading_confirmation_required" in str(cached_error.value)

    state = execution_mode.get_execution_mode()
    assert state["mode"] == "live"
    assert int(state["armed"]) == 0


def test_live_trading_preflight_rejects_alpaca_paper_endpoint_in_live(monkeypatch):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE", "1")
    _set_live_broker_contract(monkeypatch)
    monkeypatch.setenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")

    state = live_trading_preflight(
        engine_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="live-token-1234567890",
        live_confirm=DEFAULT_LIVE_CONFIRM_PHRASE,
    )

    assert state["ok"] is False
    assert "alpaca_paper_endpoint_for_live" in state["blockers"]
    assert state["broker_preflight"]["checks"][0]["invalid"] == ["ALPACA_BASE_URL"]


def test_live_trading_preflight_blocks_disable_live_execution_truthy(monkeypatch):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "on")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE", "1")
    _set_live_broker_contract(monkeypatch)
    state = live_trading_preflight(
        engine_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="live-token-1234567890",
        live_confirm=DEFAULT_LIVE_CONFIRM_PHRASE,
    )

    assert state["ok"] is False
    assert state["reason"] == "disable_live_execution_env"
    assert "disable_live_execution_env" in state["blockers"]


def test_live_trading_preflight_requires_token_for_remote_bind_even_when_safe():
    state = live_trading_preflight(
        engine_mode="safe",
        dashboard_host="0.0.0.0",
        dashboard_api_token="",
        live_confirm="",
    )

    assert state["ok"] is False
    assert state["reason"] == "dashboard_api_token_required_for_remote_bind"


def test_live_trading_preflight_rejects_placeholder_token(monkeypatch):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE", "1")
    _set_live_broker_contract(monkeypatch)
    state = live_trading_preflight(
        engine_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="change-me",
        live_confirm=DEFAULT_LIVE_CONFIRM_PHRASE,
    )

    assert state["ok"] is False
    assert "dashboard_api_token_invalid_for_live:default_dashboard_api_token" in state["blockers"]


def test_live_trading_preflight_rejects_weak_dashboard_token(monkeypatch):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE", "1")
    monkeypatch.setenv("DASHBOARD_API_TOKEN_MIN_LENGTH", "24")
    _set_live_broker_contract(monkeypatch)
    state = live_trading_preflight(
        engine_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="short-live-token",
        live_confirm=DEFAULT_LIVE_CONFIRM_PHRASE,
    )

    assert state["ok"] is False
    assert "dashboard_api_token_invalid_for_live:weak_dashboard_api_token" in state["blockers"]


def test_live_trading_preflight_blocks_missing_backup_restore_evidence(monkeypatch, tmp_path):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE", "1")
    _set_live_broker_contract(monkeypatch)
    monkeypatch.setenv("BACKUP_EVIDENCE_PATH", str(tmp_path / "missing.json"))
    monkeypatch.setenv("TS_BACKUP_BASE_DIR", str(tmp_path / "base"))
    monkeypatch.setenv("TS_BACKUP_WAL_DIR", str(tmp_path / "wal"))
    monkeypatch.setenv("TS_RESTORE_DRILL_DIR", str(tmp_path / "drills"))

    state = live_trading_preflight(
        engine_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="live-token-1234567890",
        live_confirm=DEFAULT_LIVE_CONFIRM_PHRASE,
    )

    assert state["ok"] is False
    assert state["reason"] == "backup_evidence_base_backup_missing"
    assert "backup_evidence_restore_drill_missing" in state["blockers"]
    assert state["backup_restore_evidence"]["required"] is True


def test_live_trading_preflight_blocks_stale_wal_evidence(monkeypatch, tmp_path):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE", "1")
    _set_live_broker_contract(monkeypatch)
    old_ts = time.time() - 7200
    evidence_path = tmp_path / "stale_backup_restore_evidence.json"
    evidence_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "status": "pass",
                "base_backup": {"status": "pass", "verified_at_ts": time.time()},
                "wal_archive": {"status": "pass", "verified_at_ts": old_ts},
                "restore_drill": {
                    "status": "pass",
                    "verified_at_ts": time.time(),
                    "time_to_recover_s": 60,
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("BACKUP_EVIDENCE_PATH", str(evidence_path))
    monkeypatch.setenv("BACKUP_EVIDENCE_RPO_S", "120")

    state = live_trading_preflight(
        engine_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="live-token-1234567890",
        live_confirm=DEFAULT_LIVE_CONFIRM_PHRASE,
    )

    assert state["ok"] is False
    assert "backup_evidence_wal_archive_stale" in state["blockers"]


def test_live_trading_preflight_blocks_disabled_prelive_reconcile_in_live(monkeypatch):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE", "0")
    _set_live_broker_contract(monkeypatch)
    state = live_trading_preflight(
        engine_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="live-token-1234567890",
        live_confirm=DEFAULT_LIVE_CONFIRM_PHRASE,
    )

    assert state["ok"] is False
    assert state["reason"] == "prelive_reconcile_disabled_for_live"
    assert "prelive_reconcile_disabled_for_live" in state["blockers"]
    assert state["prelive_reconcile"]["enabled"] is False


def test_live_trading_preflight_break_glass_requires_actor_and_reason(monkeypatch):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE", "0")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE_BREAK_GLASS", "1")
    _set_live_broker_contract(monkeypatch)
    state = live_trading_preflight(
        engine_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="live-token-1234567890",
        live_confirm=DEFAULT_LIVE_CONFIRM_PHRASE,
    )

    assert state["ok"] is False
    assert "EXECUTION_PRELIVE_RECONCILE_BREAK_GLASS_ACTOR required" in state["blockers"]
    assert "EXECUTION_PRELIVE_RECONCILE_BREAK_GLASS_REASON required" in state["blockers"]


def test_live_trading_preflight_accepts_audited_prelive_break_glass(monkeypatch):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE", "0")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE_BREAK_GLASS", "1")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE_BREAK_GLASS_ACTOR", "ops@example.com")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE_BREAK_GLASS_REASON", "broker reconciliation service outage during incident INC-1")
    _set_live_broker_contract(monkeypatch)
    state = live_trading_preflight(
        engine_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="live-token-1234567890",
        live_confirm=DEFAULT_LIVE_CONFIRM_PHRASE,
    )

    assert state["ok"] is True
    assert state["reason"] == "ok"
    assert state["prelive_reconcile"]["override"] is True
    assert state["prelive_reconcile"]["audit"]["actor"] == "ops@example.com"


def test_live_trading_preflight_rejects_alpaca_sim_failover(monkeypatch):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE", "1")
    _set_live_broker_contract(monkeypatch)
    monkeypatch.setenv("BROKER_FAILOVER", "alpaca,sim")

    state = live_trading_preflight(
        engine_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="live-token-1234567890",
        live_confirm=DEFAULT_LIVE_CONFIRM_PHRASE,
    )

    assert state["ok"] is False
    assert state["reason"] == "sim_after_live_broker_forbidden"
    assert "sim_after_live_broker_forbidden" in state["blockers"]
    assert state["broker_preflight"]["chain_policy"]["status"] == "live_failover_chain_invalid"


def test_live_trading_preflight_requires_ibkr_explicit_config(monkeypatch):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE", "1")
    _set_live_broker_contract(monkeypatch, "ibkr", credentials=False)
    monkeypatch.delenv("IBKR_HOST", raising=False)
    monkeypatch.delenv("IBKR_PORT", raising=False)
    monkeypatch.delenv("IBKR_CLIENT_ID", raising=False)

    state = live_trading_preflight(
        engine_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="live-token-1234567890",
        live_confirm=DEFAULT_LIVE_CONFIRM_PHRASE,
    )

    assert state["ok"] is False
    assert state["reason"] == "ibkr_credentials_missing"
    assert "ibkr_credentials_missing" in state["blockers"]
    ibkr_check = state["broker_preflight"]["checks"][0]
    assert ibkr_check["status"] == "missing_credentials"
    assert set(ibkr_check["credentials"]["missing"]) == {"IBKR_HOST", "IBKR_PORT", "IBKR_CLIENT_ID"}


def test_live_trading_preflight_blocks_ibkr_reachability_failure(monkeypatch):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE", "1")
    _set_live_broker_contract(monkeypatch, "ibkr")

    from engine.execution import broker_ibkr_gateway

    monkeypatch.setattr(
        broker_ibkr_gateway,
        "ping_broker_connection",
        lambda **_kwargs: {
            "ok": False,
            "broker": "ibkr",
            "state": "reconnect_failed",
            "status": "reconnect_failed",
            "error": "unit_test_unreachable",
        },
    )

    state = live_trading_preflight(
        engine_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="live-token-1234567890",
        live_confirm=DEFAULT_LIVE_CONFIRM_PHRASE,
    )

    assert state["ok"] is False
    assert state["reason"] == "ibkr_reachability_failed"
    assert "ibkr_reachability_failed" in state["blockers"]
    ibkr_check = state["broker_preflight"]["checks"][0]
    assert ibkr_check["reachability"]["error"] == "unit_test_unreachable"
