from __future__ import annotations

import json
import hashlib
import hmac
import time
from datetime import datetime, timezone

import pytest

from engine.runtime.live_trading_preflight import (
    DEFAULT_LIVE_CONFIRM_PHRASE,
    cpcv_leakage_gate_snapshot,
    live_trading_preflight,
)


GOOD_CLOCK_HEALTH = {
    "ok": True,
    "required": True,
    "mode": "live",
    "reason": "ok",
    "blockers": [],
    "healthy_sources": ["chronyc"],
    "skew_sources": ["chronyc"],
    "max_observed_skew_ms": 1.0,
    "timezone": {"ok": True, "required_timezone": "UTC", "actual_timezone": "UTC"},
}


def _set_cpcv_leakage_gate(monkeypatch) -> None:
    monkeypatch.setenv("CPCV_ENABLED", "1")
    monkeypatch.setenv("CHAMPION_PROMOTION_USE_STAT_GATE", "1")
    monkeypatch.setenv("CPCV_EMBARGO_PCT", "0.01")
    monkeypatch.setenv("CPCV_MAX_PBO", "0.5")


def _sign_backup_evidence(payload: dict, key: str, *, key_id: str = "live-test-key") -> dict:
    signed = dict(payload)
    signed.pop("signature", None)
    payload_bytes = json.dumps(
        signed,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    payload_sha256 = hashlib.sha256(payload_bytes).hexdigest()
    algorithm = "hmac-sha256"
    signed_at = (
        datetime.fromtimestamp(time.time(), tz=timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )
    metadata_bytes = json.dumps(
        {
            "algorithm": algorithm,
            "key_id": key_id,
            "payload_sha256": payload_sha256,
            "signed_at": signed_at,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    signed["signature"] = {
        "status": "signed",
        "algorithm": algorithm,
        "key_id": key_id,
        "signed_at": signed_at,
        "payload_sha256": payload_sha256,
        "value": hmac.new(key.encode("utf-8"), payload_bytes + b"\n" + metadata_bytes, hashlib.sha256).hexdigest(),
    }
    return signed


@pytest.fixture(autouse=True)
def fresh_backup_restore_evidence(monkeypatch, tmp_path):
    now = time.time()
    monkeypatch.setenv("DB_PATH", str(tmp_path / "live_trading_preflight.db"))
    monkeypatch.setenv("TIMESCALE_ENABLED", "0")
    monkeypatch.setenv("TS_ARTIFACTS_ROOT", str(tmp_path / "artifacts"))
    evidence_path = tmp_path / "latest_backup_restore_evidence.json"
    evidence_path.write_text(
        json.dumps(
            _sign_backup_evidence(
                {
                    "schema_version": 1,
                    "generated_at": (
                        datetime.fromtimestamp(now, tz=timezone.utc)
                        .replace(microsecond=0)
                        .isoformat()
                        .replace("+00:00", "Z")
                    ),
                    "generated_at_ts": now,
                    "status": "pass",
                    "base_backup": {
                        "status": "pass",
                        "backup_dir": "/var/backups/trading/base/base_20260617",
                        "verify_log": "/var/backups/trading/base/base_20260617/pg_verifybackup.out",
                        "verified_at_ts": now,
                    },
                    "wal_archive": {
                        "status": "pass",
                        "wal_file": "/var/backups/trading/wal/0000000100000000000000AA",
                        "verified_at_ts": now,
                    },
                    "wal_archiver": {
                        "status": "pass",
                        "source": "pg_stat_archiver",
                        "archive_mode": "on",
                        "archive_command": '/opt/trading/ops/backup/wal_archive.sh "%p" "%f"',
                        "archived_count": 10,
                        "last_archived_wal": "0000000100000000000000AA",
                        "last_archived_at_ts": now,
                        "failed_count": 0,
                        "last_failed_wal": "",
                        "last_failed_at_ts": None,
                    },
                    "wal_archive_target": {
                        "status": "pass",
                        "source": "filesystem_repair",
                        "root": "/var/backups/trading",
                        "wal_dir": "/var/backups/trading/wal",
                        "tmp_dir": "/var/backups/trading/wal/.tmp",
                        "expected_owner_uid": 70,
                        "expected_group": "trading",
                        "expected_group_gid": 70,
                        "expected_dir_mode": "2750",
                        "repaired": False,
                        "issue_count": 0,
                        "verified_at_ts": now,
                    },
                    "restore_drill": {
                        "status": "pass",
                        "report": "/var/backups/trading/drills/restore_drill_20260617.txt",
                        "verified_at_ts": now,
                        "time_to_recover_s": 60,
                    },
                },
                "live-test-signing-key",
            )
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("BACKUP_EVIDENCE_PATH", str(evidence_path))
    monkeypatch.setenv("BACKUP_EVIDENCE_HMAC_KEY", "live-test-signing-key")
    monkeypatch.setenv("BACKUP_EVIDENCE_BASE_BACKUP_MAX_AGE_S", "3600")
    monkeypatch.setenv("BACKUP_EVIDENCE_RPO_S", "3600")
    monkeypatch.setenv("BACKUP_EVIDENCE_RESTORE_DRILL_MAX_AGE_S", "3600")
    monkeypatch.setenv("BACKUP_EVIDENCE_RTO_S", "300")
    import engine.runtime.live_trading_preflight as preflight

    monkeypatch.setattr(
        preflight,
        "wal_archiver_runtime_snapshot",
        lambda *, engine_mode=None, required=None, now_ts=None: {
            "ok": True,
            "required": engine_mode == "live",
            "reason": "ok",
            "blockers": [],
            "warnings": [],
            "archive_mode": "on",
            "archive_command": '/opt/trading/ops/backup/wal_archive.sh "%p" "%f"',
            "last_archived_wal": "0000000100000000000000AA",
            "age_s": 1,
            "failed_count": 0,
        },
    )
    monkeypatch.setattr(preflight, "clock_health_snapshot", lambda *, engine_mode=None: dict(GOOD_CLOCK_HEALTH))
    monkeypatch.setenv("EXECUTION_MODE", "live")
    monkeypatch.setenv("KILL_SWITCH_GLOBAL", "1")
    monkeypatch.setenv("LIVE_TRADING_REQUIRE_CONFIRMATION", "1")
    monkeypatch.setenv("OPERATOR_API_TOKEN", "operator-token-1234567890")
    monkeypatch.delenv("OPERATOR_PUBLIC_PORT", raising=False)
    monkeypatch.delenv("OPERATOR_BIND_HOST", raising=False)
    monkeypatch.delenv("LIVE_TRADING_CONFIRM_PHRASE", raising=False)
    monkeypatch.setenv("DECISION_ENGINE_ENABLED", "1")
    monkeypatch.setenv("DECISION_MIN_CONFIDENCE", "0.70")
    monkeypatch.setenv("DECISION_MIN_ABS_PREDICTION", "0.80")
    monkeypatch.setenv("UNCERTAINTY_SIZING_PRODUCTION_POLICY", "strict")
    monkeypatch.setenv("UNCERTAINTY_HIGH_THRESHOLD", "0.70")
    monkeypatch.setenv("UNCERTAINTY_HARD_THRESHOLD", "0.95")
    monkeypatch.setenv("UNCERTAINTY_MAX_AGE_MS", "300000")
    monkeypatch.setenv("OOD_SUPPRESS_THRESHOLD", "1.50")
    monkeypatch.setenv("OOD_HARD_THRESHOLD", "3.00")
    monkeypatch.setenv("MODEL_NAME", "live_test_model")
    from engine.artifacts.store import LocalArtifactStore

    artifact_alias = "model:live_test_model:current"
    artifact_ref = LocalArtifactStore().put(
        b"live-test-artifact",
        content_type="application/octet-stream",
        kind="model",
        alias=artifact_alias,
        metadata={"model_name": "live_test_model"},
    )
    monkeypatch.setenv(
        "MODEL_INSTANCE_CONFIG_JSON",
        json.dumps(
            [
                {
                    "family": "embed_regressor",
                    "model_name": "live_test_model",
                    "horizons_s": [300],
                    "feature_ids": ["f_0"],
                    "symbol_universe": ["*"],
                    "model_kind": "ridge",
                    "enabled": True,
                    "prediction_enabled": True,
                    "experimental": False,
                    "artifact_alias": artifact_alias,
                    "artifact_sha256": artifact_ref.sha256,
                }
            ],
            separators=(",", ":"),
            sort_keys=True,
        ),
    )

def _set_live_broker_contract(
    monkeypatch,
    broker: str = "alpaca",
    *,
    credentials: bool = True,
    evidence: bool = True,
) -> None:
    broker = str(broker or "alpaca").strip().lower()
    _set_cpcv_leakage_gate(monkeypatch)
    monkeypatch.setenv("BROKER", broker)
    monkeypatch.setenv("BROKER_NAME", broker)
    monkeypatch.setenv("LIVE_BROKER", broker)
    monkeypatch.setenv("BROKER_FAILOVER", broker)
    monkeypatch.setenv("BROKER_SHUTDOWN_POLICY", "cancel_only")
    if broker == "alpaca":
        monkeypatch.setenv("ALPACA_BASE_URL", "https://api.alpaca.markets")
        if credentials:
            monkeypatch.setenv("ALPACA_KEY_ID", "alpaca-key")
            monkeypatch.setenv("ALPACA_SECRET_KEY", "alpaca-secret")
    elif broker == "ibkr" and credentials:
        monkeypatch.setenv("IBKR_HOST", "127.0.0.1")
        monkeypatch.setenv("IBKR_PORT", "7497")
        monkeypatch.setenv("IBKR_CLIENT_ID", "42")
    if evidence:
        _write_position_reconcile_evidence(broker=broker)


def _write_position_reconcile_evidence(
    *,
    broker: str = "alpaca",
    ok: bool = True,
    status: str = "ok",
    ts_ms: int | None = None,
    detail: dict | None = None,
) -> None:
    from engine.execution import position_reconcile
    from engine.runtime import storage

    now_ms = int(ts_ms if ts_ms is not None else time.time() * 1000)
    payload = dict(detail or {"status": status})
    storage.init_db()
    con = storage.connect()
    try:
        position_reconcile._ensure_schema(con)
        position_reconcile._append_reconcile_audit(
            con,
            ts_ms=now_ms,
            broker=str(broker),
            ok=bool(ok),
            status=str(status),
            mismatched_n=int(payload.get("mismatched_n") or 0),
            max_abs_qty_diff=float(payload.get("max_abs_qty_diff") or 0.0),
            total_abs_qty_diff=float(payload.get("total_abs_qty_diff") or 0.0),
            detail=payload,
        )
        if bool(getattr(con, "in_transaction", False)):
            con.commit()
    finally:
        con.close()


def _set_live_arming_contract(monkeypatch) -> None:
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE", "1")
    monkeypatch.setenv("DASHBOARD_API_TOKEN", "live-token-1234567890")
    monkeypatch.setenv("LIVE_TRADING_CONFIRM", DEFAULT_LIVE_CONFIRM_PHRASE)
    monkeypatch.setenv("KILL_SWITCH_GLOBAL", "1")
    _set_live_broker_contract(monkeypatch)


def _clear_execution_mode_cache() -> None:
    from engine.cache import keys, store

    store.invalidate(keys.execution_mode())


def _arm_live_execution_with_audit(monkeypatch) -> None:
    _set_live_arming_contract(monkeypatch)

    from engine.execution import execution_mode
    from engine.runtime import storage

    storage.init_db()
    execution_mode.set_execution_mode("live", actor="ops", reason="ready_to_arm")
    time.sleep(0.005)
    execution_mode.set_execution_armed(1, actor="ops", reason="operator_signoff")
    monkeypatch.setenv("KILL_SWITCH_GLOBAL", "0")
    _clear_execution_mode_cache()


def _live_preflight_state() -> dict:
    return live_trading_preflight(
        engine_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="live-token-1234567890",
        live_confirm=DEFAULT_LIVE_CONFIRM_PHRASE,
    )


def _latest_live_armed_audit_ts(con) -> int:
    row = con.execute(
        """
        SELECT ts_ms
        FROM execution_mode_audit
        WHERE new_mode='live' AND COALESCE(new_armed, 0)=1
        ORDER BY ts_ms DESC
        LIMIT 1
        """
    ).fetchone()
    assert row is not None
    return int(row[0] or 0)


def test_live_trading_preflight_blocks_disabled_cpcv_leakage_gate_in_live(monkeypatch):
    for key in (
        "CPCV_ENABLED",
        "CHAMPION_PROMOTION_USE_STAT_GATE",
        "CPCV_EMBARGO_PCT",
        "CPCV_MAX_PBO",
    ):
        monkeypatch.delenv(key, raising=False)

    state = live_trading_preflight(
        engine_mode="live",
        execution_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="live-token-1234567890",
        live_confirm=DEFAULT_LIVE_CONFIRM_PHRASE,
    )

    assert state["ok"] is False
    assert "cpcv_leakage_gate_disabled_in_live" in state["blockers"]
    assert "champion_promotion_stat_gate_disabled_in_live" in state["blockers"]
    assert state["cpcv_leakage_gate"]["required"] is True
    assert state["cpcv_leakage_gate"]["ok"] is False


def test_live_trading_preflight_accepts_configured_cpcv_leakage_gate(monkeypatch):
    _set_cpcv_leakage_gate(monkeypatch)

    state = live_trading_preflight(
        engine_mode="live",
        execution_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="live-token-1234567890",
        live_confirm=DEFAULT_LIVE_CONFIRM_PHRASE,
    )

    snapshot = state["cpcv_leakage_gate"]
    assert snapshot["ok"] is True
    assert snapshot["required"] is True
    assert snapshot["cpcv_enabled"] is True
    assert snapshot["stat_gate_enabled"] is True
    assert snapshot["embargo_pct"] > 0.0
    assert snapshot["max_pbo"] > 0.0
    assert "cpcv_leakage_gate_disabled_in_live" not in state["blockers"]
    assert "champion_promotion_stat_gate_disabled_in_live" not in state["blockers"]
    assert "cpcv_embargo_pct_not_configured" not in state["blockers"]
    assert "cpcv_max_pbo_not_configured" not in state["blockers"]


def test_live_trading_preflight_blocks_unconfigured_cpcv_thresholds_in_live(monkeypatch):
    _set_cpcv_leakage_gate(monkeypatch)
    monkeypatch.setenv("CPCV_EMBARGO_PCT", "0")
    monkeypatch.setenv("CPCV_MAX_PBO", "0")

    snapshot = cpcv_leakage_gate_snapshot(engine_mode="live")

    assert snapshot["ok"] is False
    assert "cpcv_embargo_pct_not_configured" in snapshot["blockers"]
    assert "cpcv_max_pbo_not_configured" in snapshot["blockers"]


@pytest.mark.parametrize("mode", ["safe", "paper", "shadow"])
def test_cpcv_leakage_gate_not_required_outside_live(monkeypatch, mode):
    for key in (
        "CPCV_ENABLED",
        "CHAMPION_PROMOTION_USE_STAT_GATE",
        "CPCV_EMBARGO_PCT",
        "CPCV_MAX_PBO",
    ):
        monkeypatch.delenv(key, raising=False)

    state = live_trading_preflight(
        engine_mode=mode,
        execution_mode=mode,
        dashboard_host="127.0.0.1",
        dashboard_api_token="",
        live_confirm="",
    )

    assert state["cpcv_leakage_gate"]["required"] is False
    assert state["cpcv_leakage_gate"]["ok"] is True
    assert "cpcv_leakage_gate_disabled_in_live" not in state["blockers"]
    assert "champion_promotion_stat_gate_disabled_in_live" not in state["blockers"]


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


def test_live_trading_preflight_requires_operator_sidecar_token(monkeypatch):
    _set_live_arming_contract(monkeypatch)
    monkeypatch.delenv("OPERATOR_API_TOKEN", raising=False)
    monkeypatch.delenv("OPERATOR_API_TOKEN_SECRET", raising=False)

    state = live_trading_preflight(
        engine_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="live-token-1234567890",
        live_confirm=DEFAULT_LIVE_CONFIRM_PHRASE,
    )

    assert state["ok"] is False
    assert "missing_operator_api_token" in state["blockers"]
    assert state["operator_sidecar_security"]["operator_api_token_issue"] == "missing_operator_api_token"


def test_live_trading_preflight_blocks_public_operator_sidecar(monkeypatch):
    _set_live_arming_contract(monkeypatch)
    monkeypatch.setenv("OPERATOR_BIND_HOST", "0.0.0.0")
    monkeypatch.setenv("OPERATOR_PUBLIC_PORT", "4001")
    monkeypatch.delenv("OPERATOR_SIDECAR_INTERNAL_ONLY", raising=False)

    state = live_trading_preflight(
        engine_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="live-token-1234567890",
        live_confirm=DEFAULT_LIVE_CONFIRM_PHRASE,
    )

    assert state["ok"] is False
    assert "operator_bind_host_public_without_internal_only" in state["blockers"]
    assert "operator_sidecar_public_port_forbidden" in state["blockers"]


def test_live_trading_preflight_blocks_ignored_internal_operator_public_intent(monkeypatch):
    _set_live_arming_contract(monkeypatch)
    monkeypatch.setenv("OPERATOR_SIDECAR_INTERNAL_ONLY", "1")
    monkeypatch.setenv("OPERATOR_PUBLIC_PORT", "4001")
    monkeypatch.setenv("OPERATOR_ALLOW_DANGEROUS_PUBLIC_BIND", "1")

    state = live_trading_preflight(
        engine_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="live-token-1234567890",
        live_confirm=DEFAULT_LIVE_CONFIRM_PHRASE,
    )

    assert state["ok"] is False
    blockers = set(state["blockers"])
    assert "operator_public_port_ignored_internal_only" in blockers
    assert "operator_public_bind_flag_ignored_internal_only" in blockers
    sidecar = state["operator_sidecar_security"]
    assert sidecar["internal_only"] is True
    assert sidecar["operator_public_port_configured"] is True


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
    assert state["live_ai_safety"]["ok"] is True
    assert state["clock_health"]["ok"] is True


def test_live_trading_preflight_blocks_unsynchronized_clock(monkeypatch):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE", "1")
    _set_live_broker_contract(monkeypatch)
    import engine.runtime.live_trading_preflight as preflight

    monkeypatch.setattr(
        preflight,
        "clock_health_snapshot",
        lambda *, engine_mode=None: {
            "ok": False,
            "required": True,
            "mode": engine_mode or "live",
            "reason": "clock_unsynchronized",
            "blockers": ["clock_unsynchronized"],
            "sources": [{"name": "timedatectl", "available": True, "synchronized": False}],
        },
    )

    state = live_trading_preflight(
        engine_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="live-token-1234567890",
        live_confirm=DEFAULT_LIVE_CONFIRM_PHRASE,
    )

    assert state["ok"] is False
    assert "clock_unsynchronized" in state["blockers"]
    assert state["clock_health"]["reason"] == "clock_unsynchronized"


def test_live_trading_preflight_rejects_disabled_decision_gate(monkeypatch):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE", "1")
    monkeypatch.setenv("DECISION_ENGINE_ENABLED", "0")
    _set_live_broker_contract(monkeypatch)

    state = live_trading_preflight(
        engine_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="live-token-1234567890",
        live_confirm=DEFAULT_LIVE_CONFIRM_PHRASE,
    )

    assert state["ok"] is False
    assert "live_decision_gate_disabled" in state["blockers"]
    assert state["live_ai_safety"]["reason"] == "live_decision_gate_disabled"


def test_live_trading_preflight_rejects_missing_uncertainty_threshold(monkeypatch):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE", "1")
    monkeypatch.delenv("UNCERTAINTY_HARD_THRESHOLD", raising=False)
    _set_live_broker_contract(monkeypatch)

    state = live_trading_preflight(
        engine_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="live-token-1234567890",
        live_confirm=DEFAULT_LIVE_CONFIRM_PHRASE,
    )

    assert state["ok"] is False
    assert "live_uncertainty_threshold_missing:UNCERTAINTY_HARD_THRESHOLD" in state["blockers"]


def test_live_trading_preflight_rejects_model_resolution_fallback(monkeypatch):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE", "1")
    monkeypatch.setenv("MODEL_NAME", "missing_live_model")
    _set_live_broker_contract(monkeypatch)

    state = live_trading_preflight(
        engine_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="live-token-1234567890",
        live_confirm=DEFAULT_LIVE_CONFIRM_PHRASE,
    )

    assert state["ok"] is False
    assert "live_model_resolution_fallback" in state["blockers"]


def test_live_trading_preflight_rejects_missing_model_artifact(monkeypatch):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE", "1")
    monkeypatch.setenv(
        "MODEL_INSTANCE_CONFIG_JSON",
        json.dumps(
            [
                {
                    "family": "embed_regressor",
                    "model_name": "live_test_model",
                    "horizons_s": [300],
                    "feature_ids": ["f_0"],
                    "symbol_universe": ["*"],
                    "model_kind": "ridge",
                    "enabled": True,
                    "prediction_enabled": True,
                    "artifact_alias": "model:live_test_model:missing",
                }
            ],
            separators=(",", ":"),
            sort_keys=True,
        ),
    )
    _set_live_broker_contract(monkeypatch)

    state = live_trading_preflight(
        engine_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="live-token-1234567890",
        live_confirm=DEFAULT_LIVE_CONFIRM_PHRASE,
    )

    assert state["ok"] is False
    assert "live_model_artifact_missing" in state["blockers"]


def test_live_trading_preflight_rejects_rl_fallback_agent(monkeypatch):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE", "1")
    monkeypatch.setenv("RL_ALLOW_FALLBACK_AGENT", "1")
    _set_live_broker_contract(monkeypatch)

    state = live_trading_preflight(
        engine_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="live-token-1234567890",
        live_confirm=DEFAULT_LIVE_CONFIRM_PHRASE,
    )

    assert state["ok"] is False
    assert "live_rl_fallback_agent_allowed" in state["blockers"]


def test_live_trading_preflight_accepts_dashboard_token_secret(monkeypatch):
    import engine.runtime.live_trading_preflight as preflight

    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE", "1")
    monkeypatch.setenv("LIVE_TRADING_CONFIRM", DEFAULT_LIVE_CONFIRM_PHRASE)
    monkeypatch.delenv("DASHBOARD_API_TOKEN", raising=False)
    monkeypatch.setenv("DASHBOARD_API_TOKEN_SECRET", "dashboard_api_token")
    monkeypatch.setattr(preflight, "dashboard_api_token_from_env", lambda: "live-token-1234567890")
    _set_live_broker_contract(monkeypatch)

    state = live_trading_preflight(
        engine_mode="live",
        dashboard_host="127.0.0.1",
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
    monkeypatch.setenv("KILL_SWITCH_GLOBAL", "1")
    monkeypatch.setenv("DASHBOARD_API_TOKEN", "live-token-1234567890")
    monkeypatch.setenv("LIVE_TRADING_CONFIRM", DEFAULT_LIVE_CONFIRM_PHRASE)
    _set_live_broker_contract(monkeypatch)

    from engine.execution import execution_mode
    from engine.runtime import storage

    storage.init_db()
    execution_mode.set_execution_mode("live", actor="ops", reason="signoff")
    execution_mode.set_execution_armed(1, actor="ops", reason="signoff")
    monkeypatch.setenv("KILL_SWITCH_GLOBAL", "0")

    state = live_trading_preflight(
        engine_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="live-token-1234567890",
        live_confirm=DEFAULT_LIVE_CONFIRM_PHRASE,
    )

    assert state["ok"] is True
    assert state["execution_arming_audit"]["required"] is True
    assert state["execution_arming_audit"]["audit"]["row_hash_present"] is True
    assert state["execution_arming_audit"]["audit_chain"]["ok"] is True
    assert state["execution_arming_audit"]["audit_chain"]["rows_verified"] >= 2
    assert state["initial_kill_switch_hold"]["signed_off"] is True


def test_live_trading_preflight_blocks_tampered_execution_mode_audit_row_hash(monkeypatch):
    _arm_live_execution_with_audit(monkeypatch)

    from engine.runtime import storage

    con = storage.connect()
    try:
        latest_ts = _latest_live_armed_audit_ts(con)
        con.execute("UPDATE execution_mode_audit SET row_hash=? WHERE ts_ms=?", (b"\x01" * 32, latest_ts))
        con.commit()
    finally:
        con.close()

    state = _live_preflight_state()

    assert state["ok"] is False
    assert "execution_mode_audit_row_hash_mismatch" in state["blockers"]
    assert state["execution_arming_audit"]["audit_chain"]["findings"][0]["finding"] == "row_hash_mismatch"


@pytest.mark.parametrize(
    ("column", "value", "expected_mismatch"),
    [
        ("actor", "intruder", "actor"),
        ("reason", "tampered-signoff", "reason"),
        ("new_mode", "paper", "mode"),
        ("new_armed", 0, "armed"),
    ],
)
def test_live_trading_preflight_blocks_tampered_execution_mode_audit_payload_fields(
    monkeypatch,
    column,
    value,
    expected_mismatch,
):
    _arm_live_execution_with_audit(monkeypatch)

    from engine.runtime import storage

    con = storage.connect()
    try:
        latest_ts = _latest_live_armed_audit_ts(con)
        con.execute(f"UPDATE execution_mode_audit SET {column}=? WHERE ts_ms=?", (value, latest_ts))
        con.commit()
    finally:
        con.close()

    state = _live_preflight_state()

    assert state["ok"] is False
    assert "execution_mode_audit_row_hash_mismatch" in state["blockers"]
    if expected_mismatch in {"actor", "reason"}:
        assert "execution_mode_live_armed_audit_mismatch" in state["blockers"]
        assert expected_mismatch in state["execution_arming_audit"]["audit"]["mismatches"]
    else:
        assert "execution_mode_live_armed_audit_missing" in state["blockers"]


def test_live_trading_preflight_blocks_missing_execution_mode_audit_previous_hash(monkeypatch):
    _arm_live_execution_with_audit(monkeypatch)

    from engine.runtime import storage

    con = storage.connect()
    try:
        latest_ts = _latest_live_armed_audit_ts(con)
        con.execute("UPDATE execution_mode_audit SET prev_hash=NULL WHERE ts_ms=?", (latest_ts,))
        con.commit()
    finally:
        con.close()

    state = _live_preflight_state()

    assert state["ok"] is False
    assert "execution_mode_audit_prev_hash_missing" in state["blockers"]
    findings = state["execution_arming_audit"]["audit_chain"]["findings"]
    assert any(finding["finding"] == "prev_hash_missing" for finding in findings)


def test_live_trading_preflight_blocks_execution_mode_audit_timestamp_order_tamper(monkeypatch):
    _arm_live_execution_with_audit(monkeypatch)

    from engine.runtime import storage

    con = storage.connect()
    try:
        rows = con.execute("SELECT ts_ms, new_armed FROM execution_mode_audit ORDER BY ts_ms ASC").fetchall()
        assert len(rows) >= 2
        first_ts = int(rows[0][0] or 0)
        latest_ts = _latest_live_armed_audit_ts(con)
        con.execute("UPDATE execution_mode_audit SET ts_ms=? WHERE ts_ms=?", (first_ts - 10, latest_ts))
        con.commit()
    finally:
        con.close()

    state = _live_preflight_state()

    assert state["ok"] is False
    assert "execution_mode_audit_chain_order_broken" in state["blockers"]
    assert "execution_mode_live_armed_latest_audit_missing" in state["blockers"]
    findings = state["execution_arming_audit"]["audit_chain"]["findings"]
    assert any(finding["finding"] == "chain_order_broken" for finding in findings)


def test_live_trading_preflight_blocks_missing_latest_live_armed_audit_row(monkeypatch):
    _arm_live_execution_with_audit(monkeypatch)

    from engine.runtime import storage

    con = storage.connect()
    try:
        latest_ts = _latest_live_armed_audit_ts(con)
        con.execute("DELETE FROM execution_mode_audit WHERE ts_ms=?", (latest_ts,))
        con.commit()
    finally:
        con.close()

    state = _live_preflight_state()

    assert state["ok"] is False
    assert "execution_mode_live_armed_audit_missing" in state["blockers"]


def test_execution_mode_refuses_live_arming_with_tampered_existing_audit_chain(monkeypatch):
    _set_live_arming_contract(monkeypatch)

    from engine.execution import execution_mode
    from engine.runtime import storage

    storage.init_db()
    execution_mode.set_execution_mode("live", actor="ops", reason="ready_to_arm")

    con = storage.connect()
    try:
        con.execute(
            """
            UPDATE execution_mode_audit
            SET actor='intruder'
            WHERE new_mode='live' AND COALESCE(new_armed, 0)=0
            """
        )
        con.commit()
    finally:
        con.close()
    _clear_execution_mode_cache()

    with pytest.raises(RuntimeError) as exc:
        execution_mode.set_execution_armed(1, actor="ops", reason="operator_signoff")

    assert "execution_mode_audit_row_hash_mismatch" in str(exc.value)
    state = execution_mode.get_execution_mode()
    assert state["mode"] == "live"
    assert int(state["armed"]) == 0


def test_execution_mode_refuses_live_arming_before_initial_kill_switch_hold(monkeypatch):
    _set_live_arming_contract(monkeypatch)
    monkeypatch.setenv("KILL_SWITCH_GLOBAL", "0")

    from engine.execution import execution_mode
    from engine.runtime import storage

    storage.init_db()
    execution_mode.set_execution_mode("live", actor="ops", reason="pending_hold")

    with pytest.raises(RuntimeError) as exc:
        execution_mode.set_execution_armed(1, actor="ops", reason="premature_arm")

    assert "kill_switch_global_initial_hold_required" in str(exc.value)
    state = execution_mode.get_execution_mode()
    assert state["mode"] == "live"
    assert int(state["armed"]) == 0

    con = storage.connect(readonly=True)
    try:
        armed_audits = con.execute(
            "SELECT COUNT(*) FROM execution_mode_audit WHERE new_mode='live' AND COALESCE(new_armed, 0)=1"
        ).fetchone()[0]
    finally:
        con.close()
    assert int(armed_audits or 0) == 0


def test_operator_execution_arm_writes_hash_chain_audit(monkeypatch):
    _set_live_arming_contract(monkeypatch)

    from engine.api import api_operator_handlers
    from engine.execution import execution_mode
    from engine.runtime import storage

    storage.init_db()
    execution_mode.set_execution_mode("live", actor="ops", reason="ready_to_arm")

    result = api_operator_handlers.api_post_operator_execution_arm(
        None,
        {"armed": 1, "actor": "ops", "reason": "operator_signoff"},
        {"API_HANDLERS": {"api_get_execution_barrier": lambda *_args: {"allowed": False}}},
    )

    assert result["ok"] is True
    assert int(result["armed"]) == 1

    con = storage.connect(readonly=True)
    try:
        row = con.execute(
            """
            SELECT new_mode, new_armed, actor, reason, row_hash
            FROM execution_mode_audit
            WHERE new_mode='live' AND COALESCE(new_armed, 0)=1
            ORDER BY ts_ms DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        con.close()

    assert row is not None
    assert row[0] == "live"
    assert int(row[1] or 0) == 1
    assert row[2] == "ops"
    assert row[3] == "operator_signoff"
    assert bool(row[4])


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


@pytest.mark.parametrize("raw", [None, "", "none", "null", "maybe", "n", "f"])
def test_live_trading_preflight_blocks_disable_live_execution_unset_or_unknown(monkeypatch, raw):
    if raw is None:
        monkeypatch.delenv("DISABLE_LIVE_EXECUTION", raising=False)
    else:
        monkeypatch.setenv("DISABLE_LIVE_EXECUTION", raw)
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


def test_live_trading_preflight_blocks_unsigned_backup_evidence_when_required(monkeypatch, tmp_path):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE", "1")
    now = time.time()
    evidence_path = tmp_path / "unsigned_backup_restore_evidence.json"
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
    monkeypatch.delenv("BACKUP_EVIDENCE_HMAC_KEY", raising=False)
    monkeypatch.delenv("BACKUP_EVIDENCE_SIGNING_KEY", raising=False)
    monkeypatch.delenv("BACKUP_EVIDENCE_HMAC_KEY_FILE", raising=False)
    monkeypatch.delenv("BACKUP_EVIDENCE_SIGNING_KEY_FILE", raising=False)
    _set_live_broker_contract(monkeypatch)

    state = live_trading_preflight(
        engine_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="live-token-1234567890",
        live_confirm=DEFAULT_LIVE_CONFIRM_PHRASE,
    )

    assert state["ok"] is False
    assert "backup_evidence_unsigned" in state["blockers"]
    assert state["backup_restore_evidence"]["signature"]["required"] is True


def test_live_trading_preflight_blocks_tampered_signed_backup_evidence(monkeypatch, tmp_path):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE", "1")
    now = time.time()
    payload = _sign_backup_evidence(
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
        },
        "live-test-signing-key",
    )
    payload["restore_drill"]["time_to_recover_s"] = 30
    evidence_path = tmp_path / "tampered_backup_restore_evidence.json"
    evidence_path.write_text(json.dumps(payload, separators=(",", ":"), sort_keys=True), encoding="utf-8")
    monkeypatch.setenv("BACKUP_EVIDENCE_PATH", str(evidence_path))
    _set_live_broker_contract(monkeypatch)

    state = live_trading_preflight(
        engine_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="live-token-1234567890",
        live_confirm=DEFAULT_LIVE_CONFIRM_PHRASE,
    )

    assert state["ok"] is False
    assert "backup_evidence_signature_invalid" in state["blockers"]
    assert state["backup_restore_evidence"]["signature"]["status"] == "invalid"


def test_live_trading_preflight_blocks_runtime_wal_archiver_failure(monkeypatch):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE", "1")
    _set_live_broker_contract(monkeypatch)
    import engine.runtime.live_trading_preflight as preflight

    monkeypatch.setattr(
        preflight,
        "wal_archiver_runtime_snapshot",
        lambda *, engine_mode=None, required=None, now_ts=None: {
            "ok": False,
            "required": True,
            "reason": "wal_archiver_failure_unrecovered",
            "blockers": ["wal_archiver_failure_unrecovered"],
            "warnings": [],
            "archive_mode": "on",
            "archive_command": '/opt/trading/ops/backup/wal_archive.sh "%p" "%f"',
            "last_archived_wal": "0000000100000000000000AA",
            "failed_count": 1,
        },
    )

    state = live_trading_preflight(
        engine_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="live-token-1234567890",
        live_confirm=DEFAULT_LIVE_CONFIRM_PHRASE,
    )

    assert state["ok"] is False
    assert "wal_archiver_failure_unrecovered" in state["blockers"]
    assert state["wal_archiver_runtime"]["required"] is True


def test_live_trading_preflight_blocks_missing_position_reconcile_evidence(monkeypatch):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE", "1")
    _set_live_broker_contract(monkeypatch, evidence=False)

    state = live_trading_preflight(
        engine_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="live-token-1234567890",
        live_confirm=DEFAULT_LIVE_CONFIRM_PHRASE,
    )

    assert state["ok"] is False
    assert "position_reconcile_not_exercised" in state["blockers"]
    assert state["position_reconcile_evidence"]["required"] is True
    assert state["position_reconcile_evidence"]["exercised"] is False


def test_live_trading_preflight_blocks_stale_position_reconcile_evidence(monkeypatch):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE", "1")
    monkeypatch.setenv("POSITION_RECONCILE_EVIDENCE_MAX_AGE_S", "60")
    _set_live_broker_contract(monkeypatch, evidence=False)
    _write_position_reconcile_evidence(
        broker="alpaca",
        ts_ms=int((time.time() - 3600) * 1000),
    )

    state = live_trading_preflight(
        engine_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="live-token-1234567890",
        live_confirm=DEFAULT_LIVE_CONFIRM_PHRASE,
    )

    assert state["ok"] is False
    assert "position_reconcile_stale" in state["blockers"]
    assert state["position_reconcile_evidence"]["stale"] is True


def test_live_trading_preflight_blocks_unhealthy_position_reconcile_evidence(monkeypatch):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE", "1")
    _set_live_broker_contract(monkeypatch, evidence=False)
    _write_position_reconcile_evidence(
        broker="alpaca",
        ok=False,
        status="mismatch",
        detail={
            "status": "mismatch",
            "mismatched_n": 2,
            "mismatched": [
                {
                    "symbol": "AAPL",
                    "broker_qty": 1.0,
                    "expected_qty": 0.0,
                    "diff_qty": 1.0,
                    "mismatch_type": "broker_orphan",
                },
                {
                    "symbol": "MSFT",
                    "broker_qty": 2.0,
                    "expected_qty": 1.0,
                    "diff_qty": 1.0,
                    "mismatch_type": "quantity_mismatch",
                },
            ],
            "broker_orphan_n": 1,
            "expected_orphan_n": 0,
            "orphan_position_n": 1,
            "quantity_mismatch_n": 1,
            "max_abs_qty_diff": 1.0,
            "total_abs_qty_diff": 2.0,
        },
    )

    state = live_trading_preflight(
        engine_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="live-token-1234567890",
        live_confirm=DEFAULT_LIVE_CONFIRM_PHRASE,
    )

    assert state["ok"] is False
    assert "position_reconcile_orphan_positions" in state["blockers"]
    assert "position_reconcile_mismatched_positions" in state["blockers"]
    assert "position_reconcile_unhealthy" in state["blockers"]
    assert state["position_reconcile_evidence"]["orphan_position_n"] == 1


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


@pytest.mark.parametrize(
    ("env_key", "reason"),
    [
        ("LIVE_BROKER", "live_broker_required_for_live"),
        ("BROKER", "broker_required_for_live"),
        ("BROKER_NAME", "broker_name_required_for_live"),
        ("BROKER_FAILOVER", "broker_failover_required_for_live"),
    ],
)
def test_live_trading_preflight_requires_explicit_live_broker_identity(monkeypatch, env_key, reason):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE", "1")
    _set_live_broker_contract(monkeypatch)
    monkeypatch.delenv(env_key, raising=False)

    state = live_trading_preflight(
        engine_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="live-token-1234567890",
        live_confirm=DEFAULT_LIVE_CONFIRM_PHRASE,
    )

    assert state["ok"] is False
    assert reason in state["blockers"]
    assert state["broker_contract"]["ok"] is False


def test_live_trading_preflight_requires_live_broker_even_with_intended_alias(monkeypatch):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE", "1")
    _set_live_broker_contract(monkeypatch)
    monkeypatch.delenv("LIVE_BROKER", raising=False)
    monkeypatch.setenv("INTENDED_LIVE_BROKER", "ibkr")

    state = live_trading_preflight(
        engine_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="live-token-1234567890",
        live_confirm=DEFAULT_LIVE_CONFIRM_PHRASE,
    )

    assert state["ok"] is False
    assert "live_broker_required_for_live" in state["blockers"]
    assert state["broker_contract"]["env"]["LIVE_BROKER"] == ""
    assert state["broker_contract"]["env"]["INTENDED_LIVE_BROKER"] == "ibkr"


def test_live_trading_preflight_rejects_mixed_live_broker_failover(monkeypatch):
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE", "1")
    _set_live_broker_contract(monkeypatch, "alpaca")
    monkeypatch.setenv("BROKER_FAILOVER", "alpaca,ibkr")

    state = live_trading_preflight(
        engine_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="live-token-1234567890",
        live_confirm=DEFAULT_LIVE_CONFIRM_PHRASE,
    )

    assert state["ok"] is False
    assert "mixed_live_broker_chain_forbidden" in state["blockers"]
    assert "broker_failover_chain_mismatch" in state["blockers"]
    assert state["broker_contract"]["chain_policy"]["status"] == "live_failover_chain_invalid"


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
