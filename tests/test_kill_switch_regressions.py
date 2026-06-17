from __future__ import annotations

import importlib
import os
import sqlite3
import sys
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


class ManagedConnection:
    def __init__(self) -> None:
        self._db_path = REPO_ROOT / "tmp" / "kill_switch_regressions_memory.sqlite"
        self._con = sqlite3.connect(":memory:")

    @property
    def in_transaction(self) -> bool:
        return bool(self._con.in_transaction)

    def begin_managed_write(self) -> None:
        self._con.execute("BEGIN IMMEDIATE")

    def execute(self, *args, **kwargs):
        return self._con.execute(*args, **kwargs)

    def executescript(self, *args, **kwargs):
        return self._con.executescript(*args, **kwargs)

    def commit(self) -> None:
        self._con.commit()

    def rollback(self) -> None:
        self._con.rollback()

    def close(self) -> None:
        self._con.close()


def test_execution_allowed_auto_expires_stale_cached_switch():
    kill_switch = importlib.reload(importlib.import_module("engine.execution.kill_switch"))
    con = ManagedConnection()
    now_ms = 1_710_000_000_000
    expired_until_ms = now_ms - 1_000

    try:
        kill_switch._ensure_schema(con)
        con.execute(
            """
            INSERT INTO kill_switch_state(scope, key, enabled, reason, actor, meta_json, created_ts_ms, updated_ts_ms)
            VALUES(?,?,?,?,?,?,?,?)
            """,
            (
                "global",
                "global",
                1,
                "expired_unit_test",
                "test",
                f'{{"until_ts_ms":{expired_until_ms}}}',
                now_ms - 10_000,
                now_ms - 10_000,
            ),
        )
        con.commit()

        cached_snapshot = {
            "state": [
                {
                    "scope": "global",
                    "key": "global",
                    "enabled": 1,
                    "reason": "expired_unit_test",
                    "actor": "test",
                    "meta": {"until_ts_ms": expired_until_ms},
                    "created_ts_ms": now_ms - 10_000,
                    "updated_ts_ms": now_ms - 10_000,
                }
            ]
        }

        with patch.object(kill_switch, "_now_ms", return_value=now_ms), patch.object(
            kill_switch, "_get_lifecycle_state", return_value={"state": "LIVE"}
        ), patch.object(kill_switch, "REQUIRE_FRESH_DATA", False), patch.object(
            kill_switch, "REQUIRE_FRESH_JOBS", False
        ), patch.object(kill_switch, "_capital_risk_trigger", return_value=None), patch.object(
            kill_switch, "_model_risk_trigger", return_value=None
        ), patch("engine.strategy.capital_guard.trading_allowed", return_value=True), patch(
            "engine.cache.wrappers.kill_switch.read_kill_switch", return_value=cached_snapshot
        ), patch(
            "engine.cache.wrappers.kill_switch.prime_kill_switch", return_value=None
        ), patch(
            "engine.runtime.storage.init_db", return_value=None
        ), patch.object(
            kill_switch, "append_event", return_value=None
        ), patch(
            "engine.runtime.lifecycle_state.set_state", return_value=None
        ):
            allowed, reason, meta = kill_switch.execution_allowed(con=con)

        assert allowed is True
        assert reason == "ok"
        assert meta == {"scope": None, "key": None}
        row = con.execute(
            "SELECT enabled, reason, actor, meta_json FROM kill_switch_state WHERE scope=? AND key=?",
            ("global", "global"),
        ).fetchone()
        assert row is not None
        assert int(row[0] or 0) == 0
        assert str(row[1] or "") == "auto_expire"
    finally:
        con.close()


def test_execution_allowed_blocks_live_mode_when_disable_live_execution_truthy():
    kill_switch = importlib.reload(importlib.import_module("engine.execution.kill_switch"))
    previous = {
        "DISABLE_LIVE_EXECUTION": os.environ.get("DISABLE_LIVE_EXECUTION"),
        "EXECUTION_MODE": os.environ.get("EXECUTION_MODE"),
        "ENGINE_MODE": os.environ.get("ENGINE_MODE"),
    }
    try:
        os.environ["DISABLE_LIVE_EXECUTION"] = "yes"
        os.environ["EXECUTION_MODE"] = "live"
        os.environ["ENGINE_MODE"] = "live"
        with patch.object(kill_switch, "_get_lifecycle_state", return_value={"state": "LIVE"}):
            allowed, reason, meta = kill_switch.execution_allowed(con=None)

        assert allowed is False
        assert reason == "disable_live_execution_env"
        assert meta["scope"] == "global"
        assert meta["key"] == "DISABLE_LIVE_EXECUTION"
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
