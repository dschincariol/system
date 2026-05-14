"""
FILE: training_guard.py

Core engine module for `training_guard`.
"""

# engine/training_guard.py
"""
Training guard (SQLite-backed).

This is a runtime safety gate:
- enabled     : training permitted
- disabled    : training blocked
- maintenance : training blocked (maintenance window)

No dependencies on dev_core. Uses engine.runtime.storage only.
"""

import time
from typing import Optional, Dict, Any

from engine.runtime.storage import connect as _connect
from engine.runtime.storage import init_db as _init_db
from engine.runtime.storage import run_write_txn

_ALLOWED = {"enabled", "disabled", "maintenance"}

SCHEMA = ""


def _now_ms() -> int:
    return int(time.time() * 1000)


def _ensure_schema() -> None:
    _init_db()


def _get_state_row(key: str, default: str) -> tuple[str, int]:
    _ensure_schema()
    con = _connect()
    try:
        r = con.execute(
            "SELECT value, updated_ts_ms FROM risk_state WHERE key=?",
            (str(key),),
        ).fetchone()
        if not r:
            return (str(default), 0)
        return (str(r[0] if r[0] is not None else default), int(r[1] or 0))
    finally:
        con.close()


def _set_state(key: str, value: str) -> None:
    _ensure_schema()

    def _write(con):
        con.execute(
            """
            INSERT OR REPLACE INTO risk_state(key, value, updated_ts_ms)
            VALUES (?,?,?)
            """,
            (str(key), str(value), _now_ms()),
        )

    run_write_txn(_write)


def training_allowed() -> bool:
    mode, _ = _get_state_row("training_state", "enabled")
    return str(mode) == "enabled"


def set_training_state(enabled: bool, reason: Optional[str] = None) -> None:
    set_training_mode("enabled" if enabled else "disabled", reason=reason)


def set_training_mode(mode: str, reason: Optional[str] = None) -> None:
    mode = (mode or "").strip().lower()
    if mode not in _ALLOWED:
        mode = "disabled"
    _set_state("training_state", mode)
    if reason is not None:
        _set_state("training_reason", str(reason))


def get_training_status() -> Dict[str, Any]:
    mode, mode_ts = _get_state_row("training_state", "enabled")
    reason, reason_ts = _get_state_row("training_reason", "")
    ts_ms = max(int(mode_ts or 0), int(reason_ts or 0))
    return {
        "mode": str(mode),
        "allowed": (str(mode) == "enabled"),
        "reason": str(reason),
        "updated_ts_ms": int(ts_ms),
    }
