from __future__ import annotations

import importlib
import os
import sqlite3
import sys
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

pytestmark = pytest.mark.safety_critical


NOW_MS = 1_789_500_000_000
ENV_CLEAR = {
    "KILL_SWITCH_GLOBAL": "0",
    "TRADING_KILL_SWITCH": "0",
    "KILL_SWITCH": "0",
    "KILL_SWITCH_SYMBOLS": "",
    "KILL_SWITCH_REGIMES": "",
    "KILL_SWITCH_MODELS": "",
    "DISABLE_LIVE_EXECUTION": "0",
    "EXECUTION_MODE": "paper",
    "ENGINE_MODE": "paper",
    "EXECUTION_BLOCK_EVENT_BUS_CRITICAL_BACKPRESSURE": "0",
}


def _snapshot(enabled: int, *, read_source: str = "redis") -> dict:
    return {
        "state": [
            {
                "scope": "global",
                "key": "global",
                "enabled": int(enabled),
                "reason": "unit_test",
                "actor": "pytest",
                "meta": {},
                "created_ts_ms": NOW_MS - 1_000,
                "updated_ts_ms": NOW_MS - 500,
            }
        ],
        "source": "engine.cache.wrappers.kill_switch:db",
        "loaded_ts_ms": NOW_MS,
        "max_age_ms": 30_000,
        "cache_fresh": True,
        "read_source": read_source,
        "cache_status": "fresh",
    }


def _annotate(snapshot: dict, env: dict[str, str], *, read_source: str = "redis") -> dict:
    kill_switch_cache = importlib.import_module("engine.cache.wrappers.kill_switch")
    return kill_switch_cache.annotate_effective_state(
        {**snapshot, "read_source": read_source},
        environ=env,
        persisted_read_source=read_source,
    )


def _execution_allowed_patchers(kill_switch):
    return (
        patch.object(kill_switch, "_get_lifecycle_state", return_value={"state": "LIVE"}),
        patch.object(kill_switch, "REQUIRE_FRESH_DATA", False),
        patch.object(kill_switch, "REQUIRE_FRESH_JOBS", False),
        patch.object(kill_switch, "_capital_risk_trigger", return_value=None),
        patch.object(kill_switch, "_model_risk_trigger", return_value=None),
        patch("engine.strategy.capital_guard.trading_allowed", return_value=True),
        patch("engine.runtime.storage.init_db", return_value=None),
    )


def _execution_allowed_with_patches(kill_switch, con):
    with ExitStack() as stack:
        for patcher in _execution_allowed_patchers(kill_switch):
            stack.enter_context(patcher)
        return kill_switch.execution_allowed(con=con)


def test_env_armed_persisted_disarmed_reports_effective_env_provenance() -> None:
    env = {**ENV_CLEAR, "KILL_SWITCH_GLOBAL": "1"}

    payload = _annotate(_snapshot(0), env, read_source="redis")

    effective = payload["effective"]
    assert effective["armed"] is True
    assert effective["sources"] == ["env"]
    assert effective["env_armed"] is True
    assert effective["persisted_armed"] is False
    assert effective["persisted_state"] == "disarmed"
    assert effective["summary"] == "armed via env; persisted disarmed"
    assert effective["provenance"]["env"]["armed"] is True
    assert effective["provenance"]["env"]["keys"] == ["KILL_SWITCH_GLOBAL"]
    assert effective["provenance"]["redis"]["read"] is True
    assert effective["provenance"]["redis"]["armed"] is False


def test_persisted_armed_env_disarmed_reports_effective_db_provenance() -> None:
    payload = _annotate(_snapshot(1, read_source="db_load"), ENV_CLEAR, read_source="db_load")

    effective = payload["effective"]
    assert effective["armed"] is True
    assert effective["sources"] == ["db"]
    assert effective["env_armed"] is False
    assert effective["persisted_armed"] is True
    assert effective["persisted_state"] == "armed"
    assert effective["summary"] == "armed via db; persisted armed via db"
    assert effective["provenance"]["env"]["armed"] is False
    assert effective["provenance"]["db"]["armed"] is True
    assert effective["provenance"]["db"]["active"][0]["scope"] == "global"


def test_env_and_persisted_disarmed_reports_effective_disarmed() -> None:
    payload = _annotate(_snapshot(0, read_source="db_load"), ENV_CLEAR, read_source="db_load")

    effective = payload["effective"]
    assert effective["armed"] is False
    assert effective["sources"] == []
    assert effective["env_armed"] is False
    assert effective["persisted_armed"] is False
    assert effective["summary"] == "disarmed; persisted disarmed"
    assert effective["provenance"]["db"]["read"] is True
    assert effective["provenance"]["db"]["armed"] is False


def test_direct_snapshot_surfaces_env_effective_over_persisted_disarmed() -> None:
    kill_switch = importlib.reload(importlib.import_module("engine.execution.kill_switch"))
    con = sqlite3.connect(":memory:")
    try:
        kill_switch._ensure_schema(con)
        con.execute(
            """
            INSERT INTO kill_switch_state(scope, key, enabled, reason, actor, meta_json, created_ts_ms, updated_ts_ms)
            VALUES(?,?,?,?,?,?,?,?)
            """,
            ("global", "global", 0, "persisted_clear_unit_test", "pytest", "{}", NOW_MS - 1_000, NOW_MS),
        )
        con.commit()

        with patch.dict(os.environ, {**ENV_CLEAR, "KILL_SWITCH_GLOBAL": "1"}, clear=False):
            payload = kill_switch.snapshot(con=con)

        assert payload["state"][0]["enabled"] == 0
        assert payload["effective"]["armed"] is True
        assert payload["effective"]["sources"] == ["env"]
        assert payload["effective"]["summary"] == "armed via env; persisted disarmed"
        assert payload["effective"]["provenance"]["env"]["keys"] == ["KILL_SWITCH_GLOBAL"]
    finally:
        con.close()


def test_execution_allowed_still_blocks_on_env_or_persisted_without_reason_changes() -> None:
    kill_switch = importlib.reload(importlib.import_module("engine.execution.kill_switch"))
    con = sqlite3.connect(":memory:")
    try:
        kill_switch._ensure_schema(con)
        con.execute(
            """
            INSERT INTO kill_switch_state(scope, key, enabled, reason, actor, meta_json, created_ts_ms, updated_ts_ms)
            VALUES(?,?,?,?,?,?,?,?)
            """,
            ("global", "global", 1, "persisted_halt_unit_test", "pytest", "{}", NOW_MS - 1_000, NOW_MS),
        )
        con.commit()

        with patch.dict(os.environ, {**ENV_CLEAR, "KILL_SWITCH_GLOBAL": "1"}, clear=False):
            allowed, reason, _meta = _execution_allowed_with_patches(kill_switch, con)
        assert allowed is False
        assert reason == "kill_switch_env_global"

        with patch.dict(os.environ, ENV_CLEAR, clear=False), patch(
            "engine.cache.wrappers.kill_switch.read_kill_switch",
            return_value=_snapshot(1, read_source="db_load"),
        ):
            allowed, reason, meta = _execution_allowed_with_patches(kill_switch, con)
        assert allowed is False
        assert reason == "kill_switch_db_global"
        assert meta["key"] == "global"
    finally:
        con.close()


def test_execution_gate_keeps_env_and_persisted_block_reasons_with_effective_metadata() -> None:
    env_payload = _annotate(_snapshot(0), {**ENV_CLEAR, "KILL_SWITCH_GLOBAL": "1"})
    persisted_payload = _annotate(_snapshot(1), ENV_CLEAR)

    with patch.dict(os.environ, {**ENV_CLEAR, "KILL_SWITCH_GLOBAL": "1"}, clear=False):
        gates = importlib.reload(importlib.import_module("engine.runtime.gates"))
        with patch.object(gates, "_now_ms", return_value=NOW_MS), patch.object(
            gates,
            "_get_lifecycle_state",
            return_value={"state": "LIVE"},
        ):
            env_block = gates.execution_gate_snapshot(
                get_execution_mode_fn=lambda: {"mode": "paper", "armed": 0},
                kill_switches=env_payload,
                risk_state_getter=lambda _key, default=None: default,
            )
    assert env_block["allowed"] is False
    assert env_block["reason"] == "kill_switch_env_global"
    assert env_block["env_kill_switch"]["armed"] is True

    with patch.dict(os.environ, ENV_CLEAR, clear=False):
        gates = importlib.reload(importlib.import_module("engine.runtime.gates"))
        with patch.object(gates, "_now_ms", return_value=NOW_MS), patch.object(
            gates,
            "_get_lifecycle_state",
            return_value={"state": "LIVE"},
        ):
            persisted_block = gates.execution_gate_snapshot(
                get_execution_mode_fn=lambda: {"mode": "paper", "armed": 0},
                kill_switches=persisted_payload,
                risk_state_getter=lambda _key, default=None: default,
            )
    assert persisted_block["allowed"] is False
    assert persisted_block["reason"] == "kill_switch_active"
    assert persisted_block["active"] == ["global:global"]
