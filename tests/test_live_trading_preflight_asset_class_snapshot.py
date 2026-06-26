from __future__ import annotations

import importlib

import pytest


pytestmark = pytest.mark.safety_critical


def _reload_preflight():
    return importlib.reload(importlib.import_module("engine.runtime.live_trading_preflight"))


def test_asset_class_live_enablement_defaults_disabled_or_shadow(monkeypatch):
    for key in (
        "ENGINE_MODE",
        "FX_LIVE_TRADING_ENABLED",
        "CRYPTO_LIVE_TRADING_ENABLED",
        "FUTURES_LIVE_TRADING_ENABLED",
        "OPTIONS_INSTRUMENTS_MODE",
        "OPTIONS_AS_INSTRUMENTS_MODE",
        "OPTIONS_LIVE_ORDERS_ENABLED",
        "OPTIONS_ENABLE_LIVE_ORDERS",
        "OPTIONS_AS_INSTRUMENTS_LIVE",
    ):
        monkeypatch.delenv(key, raising=False)

    preflight = _reload_preflight()
    snapshot = preflight.asset_class_live_enablement_snapshot()
    classes = snapshot["classes"]

    assert snapshot["engine_mode"] == "safe"
    assert snapshot["any_live_permitted"] is False
    assert set(classes) == {"fx", "crypto", "futures", "options"}
    assert classes["fx"]["live_permitted"] is False
    assert classes["fx"]["flag"] == "FX_LIVE_TRADING_ENABLED"
    assert classes["fx"]["default_posture"] == "disabled"
    assert classes["crypto"]["live_permitted"] is False
    assert classes["futures"]["live_permitted"] is False
    assert classes["options"]["live_permitted"] is False
    assert classes["options"]["live_options_requested"] is False
    assert classes["options"]["flag_value"] == "shadow"
    assert classes["options"]["default_posture"] == "shadow"


def test_asset_class_live_enablement_flags_flip_to_live_permitted(monkeypatch):
    monkeypatch.setenv("FX_LIVE_TRADING_ENABLED", "1")
    monkeypatch.setenv("CRYPTO_LIVE_TRADING_ENABLED", "1")
    monkeypatch.setenv("FUTURES_LIVE_TRADING_ENABLED", "1")
    monkeypatch.setenv("OPTIONS_INSTRUMENTS_MODE", "live")

    preflight = _reload_preflight()
    snapshot = preflight.asset_class_live_enablement_snapshot()
    classes = snapshot["classes"]

    assert snapshot["any_live_permitted"] is True
    assert classes["fx"]["live_permitted"] is True
    assert classes["crypto"]["live_permitted"] is True
    assert classes["futures"]["live_permitted"] is True
    assert classes["options"]["live_permitted"] is True


def test_asset_class_options_live_permitted_via_live_orders_flag(monkeypatch):
    monkeypatch.delenv("OPTIONS_INSTRUMENTS_MODE", raising=False)
    monkeypatch.delenv("OPTIONS_AS_INSTRUMENTS_MODE", raising=False)
    monkeypatch.setenv("OPTIONS_LIVE_ORDERS_ENABLED", "1")

    preflight = _reload_preflight()
    snapshot = preflight.asset_class_live_enablement_snapshot()
    classes = snapshot["classes"]

    assert classes["options"]["live_permitted"] is True
    assert classes["options"]["live_options_requested"] is True
    assert classes["options"]["flag_value"] == "shadow"
    assert snapshot["any_live_permitted"] is True


def test_asset_class_live_enablement_snapshot_reports_explicit_engine_mode(monkeypatch):
    monkeypatch.delenv("ENGINE_MODE", raising=False)

    preflight = _reload_preflight()
    snapshot = preflight.asset_class_live_enablement_snapshot(engine_mode="live")

    assert snapshot["engine_mode"] == "live"


def test_live_trading_preflight_surfaces_asset_class_snapshot_without_new_blockers(monkeypatch):
    preflight = _reload_preflight()

    monkeypatch.setattr(
        preflight,
        "live_environment_contract_snapshot",
        lambda **_kwargs: {
            "ok": False,
            "blockers": ["existing_contract_blocker"],
            "confirmation": {"required": False},
            "broker_contract": {},
            "broker_preflight": {},
            "initial_kill_switch_hold": {},
            "operator_sidecar_security": {},
            "public_network_exposure": {},
            "secret_sources": {},
            "dsn_context": {},
        },
    )
    monkeypatch.setattr(preflight, "live_execution_disabled", lambda: False)
    monkeypatch.setattr(
        preflight,
        "prelive_reconcile_policy_snapshot",
        lambda **_kwargs: {"ok": True, "required": True, "blockers": []},
    )
    monkeypatch.setattr(
        preflight,
        "position_reconcile_evidence_snapshot",
        lambda **_kwargs: {"ok": True, "required": True, "blockers": []},
    )
    monkeypatch.setattr(
        preflight,
        "broker_shutdown_policy_snapshot",
        lambda **_kwargs: {"ok": True, "required": True, "blockers": []},
    )
    monkeypatch.setattr(
        preflight,
        "backup_restore_evidence_snapshot",
        lambda **_kwargs: {"ok": True, "required": True, "blockers": []},
    )
    monkeypatch.setattr(
        preflight,
        "wal_archiver_runtime_snapshot",
        lambda **_kwargs: {"ok": True, "required": True, "blockers": []},
    )
    monkeypatch.setattr(
        preflight,
        "clock_health_snapshot",
        lambda **_kwargs: {"ok": True, "required": True, "blockers": [], "reason": "ok"},
    )
    monkeypatch.setattr(
        preflight,
        "_execution_arming_audit_snapshot",
        lambda **_kwargs: {"ok": True, "required": True, "blockers": []},
    )
    monkeypatch.setattr(
        preflight,
        "cpcv_leakage_gate_snapshot",
        lambda **_kwargs: {"ok": True, "required": True, "blockers": []},
    )
    monkeypatch.setattr(
        preflight,
        "live_ai_safety_snapshot",
        lambda **_kwargs: {"ok": True, "required": True, "blockers": []},
    )
    monkeypatch.setattr(
        preflight,
        "lob_deeplob_shadow_readiness_snapshot",
        lambda **_kwargs: {"ok": True, "enabled": False, "blockers": []},
    )
    monkeypatch.setattr(
        preflight,
        "live_options_readiness_snapshot",
        lambda **_kwargs: {"ok": True, "required": False, "blockers": []},
    )

    state = preflight.live_trading_preflight(engine_mode="live", execution_mode="live")

    assert state["blockers"] == ["existing_contract_blocker"]
    assert state["ok"] is False
    assert state["asset_class_live_enablement"]["engine_mode"] == "live"
    assert set(state["asset_class_live_enablement"]["classes"]) == {"fx", "crypto", "futures", "options"}
