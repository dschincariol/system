"""
FILE: execution_mode.py

Execution subsystem module for `execution_mode`.
"""

"""
Execution Mode (Unified)

Modes:
- paper
- shadow
- live

Live safety:
- Requires mode='live' AND armed=1
- Truthy DISABLE_LIVE_EXECUTION always blocks real trading
- Armed persisted and audited
- Provider health gate optional
"""

import os
import threading
import time
from typing import Dict, Any, Optional, Tuple

from engine.audit.chain import append_chain_row
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.live_execution_control import (
    DISABLE_LIVE_EXECUTION_REASON,
    live_execution_disabled,
)
from engine.runtime.logging import get_logger
from engine.runtime.storage import DB_PATH, _table_exists, connect, init_db, run_write_txn
from engine.runtime.state_cache import cache_get, cache_set, cache_invalidate_namespace
from engine.runtime.event_log import append_event
from engine.runtime.live_trading_preflight import live_trading_preflight
from engine.execution.execution_costs import (
    DEFAULT_FEES_BPS,
    DEFAULT_SLIPPAGE_BPS,
    DEFAULT_SPREAD_BPS_CAP,
)

# ============================================================
# Constants
# ============================================================

MODES = ("paper", "shadow", "live")
LOG = get_logger("engine.execution.execution_mode")
_WARNED_NONFATAL_KEYS: set[str] = set()
_EXECUTION_MODE_SCHEMA_READY_LOCK = threading.Lock()
_EXECUTION_MODE_SCHEMA_READY_PATH = ""


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=30,
        component="engine.execution.execution_mode",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _schema_path_key(con=None) -> str:
    db_path = getattr(con, "_db_path", None) or DB_PATH
    try:
        return str(db_path.resolve())
    except Exception:
        return str(db_path)


def _mark_schema_ready(con=None) -> None:
    global _EXECUTION_MODE_SCHEMA_READY_PATH
    ready_key = _schema_path_key(con)
    with _EXECUTION_MODE_SCHEMA_READY_LOCK:
        _EXECUTION_MODE_SCHEMA_READY_PATH = ready_key


def _schema_ready_for_reads(con) -> bool:
    ready_key = _schema_path_key(con)
    with _EXECUTION_MODE_SCHEMA_READY_LOCK:
        if _EXECUTION_MODE_SCHEMA_READY_PATH == ready_key:
            return True
    try:
        if (
            _table_exists(con, "execution_mode")
            and _table_exists(con, "execution_mode_audit")
            and _has_column(con, "execution_mode", "armed")
        ):
            _mark_schema_ready(con)
            return True
    except Exception as e:
        _warn_nonfatal(
            "EXECUTION_MODE_SCHEMA_PROBE_FAILED",
            e,
            once_key=f"schema_probe:{ready_key}",
            db_path=str(ready_key),
        )
    return False

DEFAULT_MODE = os.environ.get("EXECUTION_MODE_DEFAULT", "paper").strip().lower()
if DEFAULT_MODE not in MODES:
    DEFAULT_MODE = "paper"

DEFAULT_ARMED = 0

_PROVIDER_HEALTH: Dict[str, Any] = {}


# ============================================================
# Helpers
# ============================================================

def _put_provider_health(health: Dict[str, Any]) -> None:
    global _PROVIDER_HEALTH
    if isinstance(health, dict):
        _PROVIDER_HEALTH = dict(health)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _norm_mode(mode: str) -> str:
    m = str(mode or "").strip().lower()
    return m if m in MODES else "paper"


def _has_column(con, table: str, col: str) -> bool:
    try:
        rows = con.execute(f"PRAGMA table_info({table})").fetchall() or []
        for r in rows:
            if str(r[1]) == str(col):
                return True
    except Exception as e:
        _warn_nonfatal(
            "EXECUTION_MODE_TABLE_INFO_FAILED",
            e,
            once_key=f"table_info_{table}_{col}",
            table=str(table),
            column=str(col),
        )
    return False


def _ensure_schema(con) -> None:
    con.executescript(
        """
CREATE TABLE IF NOT EXISTS execution_mode (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  mode TEXT NOT NULL,
  armed INTEGER NOT NULL DEFAULT 0,
  updated_ts_ms INTEGER NOT NULL,
  actor TEXT NOT NULL,
  reason TEXT
);

CREATE TABLE IF NOT EXISTS execution_mode_audit (
  ts_ms INTEGER NOT NULL,
  prev_mode TEXT NOT NULL,
  new_mode TEXT NOT NULL,
  actor TEXT NOT NULL,
  reason TEXT,
  prev_armed INTEGER,
  new_armed INTEGER,
  prev_hash BLOB,
  row_hash BLOB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_execution_mode_audit_ts
  ON execution_mode_audit(ts_ms);
"""
    )

    # additive safety in case older DB exists
    if not _has_column(con, "execution_mode", "armed"):
        try:
            con.execute("ALTER TABLE execution_mode ADD COLUMN armed INTEGER NOT NULL DEFAULT 0;")
        except Exception as e:
            _warn_nonfatal(
                "EXECUTION_MODE_ADD_ARMED_COLUMN_FAILED",
                e,
                once_key="add_armed_column",
            )
    for column_name in ("prev_hash", "row_hash"):
        if not _has_column(con, "execution_mode_audit", column_name):
            try:
                con.execute(f"ALTER TABLE execution_mode_audit ADD COLUMN {column_name} BLOB;")
            except Exception as e:
                _warn_nonfatal(
                    "EXECUTION_MODE_ADD_AUDIT_CHAIN_COLUMN_FAILED",
                    e,
                    once_key=f"add_audit_chain_column:{column_name}",
                    column=str(column_name),
                )
    _mark_schema_ready(con)


def _ensure_row(con) -> None:
    r = con.execute("SELECT mode FROM execution_mode WHERE id=1").fetchone()
    if not r:
        # Execution mode is stored as a singleton row so every caller reads the
        # same authoritative mode/armed state.
        con.execute(
            "INSERT INTO execution_mode(id, mode, armed, updated_ts_ms, actor, reason) VALUES (1,?,?,?,?,?)",
            (DEFAULT_MODE, DEFAULT_ARMED, _now_ms(), "system", "init"),
        )


def _begin_owned_write(con) -> bool:
    if bool(getattr(con, "in_transaction", False)):
        return False
    begin = getattr(con, "begin_managed_write", None)
    if callable(begin):
        begin()
        return True
    raise RuntimeError("managed_write_begin_unavailable")


# ============================================================
# Public API
# ============================================================

def get_execution_mode(con=None) -> Dict[str, Any]:
    if con is None:
        try:
            from engine.cache.wrappers.execution_mode import read_execution_mode

            return read_execution_mode()
        except Exception as e:
            _warn_nonfatal(
                "EXECUTION_MODE_REDIS_CACHE_READ_FAILED",
                e,
                once_key="get_mode_redis_read",
            )
    owns = False
    if con is None:
        cached = cache_get("execution_mode", "singleton")
        if cached is not None:
            return cached
        con = connect()
        owns = True
    try:
        init_db()
        if not _schema_ready_for_reads(con):
            _ensure_schema(con)
        r = con.execute(
            "SELECT mode, armed, updated_ts_ms, actor, reason FROM execution_mode WHERE id=1"
        ).fetchone()
        if not r:
            _ensure_row(con)
            r = con.execute(
                "SELECT mode, armed, updated_ts_ms, actor, reason FROM execution_mode WHERE id=1"
            ).fetchone()
        out = {
            "mode": str(r[0]),
            "armed": int(r[1] or 0),
            "updated_ts_ms": int(r[2] or 0),
            "actor": str(r[3] or ""),
            "reason": str(r[4] or ""),
        }
        cache_set("execution_mode", "singleton", out, ttl_s=3600.0)
        return out
    finally:
        if owns:
            con.close()


def set_execution_mode(
    mode: str,
    actor: str = "operator",
    reason: str = "",
    con=None,
    keep_armed: bool = False,
) -> Dict[str, Any]:

    mode = _norm_mode(mode)
    actor = str(actor or "operator")
    reason = str(reason or "")

    def _apply(db) -> Dict[str, Any]:
        init_db()
        _ensure_schema(db)
        _ensure_row(db)

        now_ms = _now_ms()
        prev = db.execute("SELECT mode, armed FROM execution_mode WHERE id=1").fetchone()
        prev_mode = str(prev[0])
        prev_armed = int(prev[1] or 0)

        new_armed = prev_armed
        if not keep_armed:
            # Mode transitions clear arming unless a caller explicitly opts to
            # preserve it, which keeps live execution a deliberate action.
            if mode != "live":
                new_armed = 0
            if mode == "live":
                new_armed = 0

        db.execute(
            "UPDATE execution_mode SET mode=?, armed=?, updated_ts_ms=?, actor=?, reason=? WHERE id=1",
            (mode, int(new_armed), now_ms, actor, reason),
        )

        append_chain_row(
            "execution_mode_audit",
            {
                "ts_ms": int(now_ms),
                "prev_mode": prev_mode,
                "new_mode": mode,
                "actor": actor,
                "reason": reason,
                "prev_armed": int(prev_armed),
                "new_armed": int(new_armed),
            },
            db,
        )

        return {
            "mode": str(mode),
            "armed": int(new_armed or 0),
            "updated_ts_ms": int(now_ms),
            "actor": str(actor or ""),
            "reason": str(reason or ""),
            "_prev_mode": str(prev_mode),
            "_prev_armed": int(prev_armed),
            "_now_ms": int(now_ms),
            "_new_armed": int(new_armed),
        }

    if con is None:
        st = run_write_txn(
            _apply,
            table="execution_mode",
            operation="set_execution_mode",
            context={"mode": str(mode), "actor": str(actor)},
        )
        publish_side_effects = True
    else:
        owns_txn = False
        try:
            owns_txn = _begin_owned_write(con)
            st = _apply(con)
            if owns_txn:
                con.commit()
        except Exception:
            if owns_txn and bool(getattr(con, "in_transaction", False)):
                con.rollback()
            raise
        publish_side_effects = bool(owns_txn)

    cache_payload = {
        "mode": str(st["mode"]),
        "armed": int(st["armed"]),
        "updated_ts_ms": int(st["updated_ts_ms"]),
        "actor": str(st["actor"]),
        "reason": str(st["reason"]),
    }
    if publish_side_effects:
        cache_set("execution_mode", "singleton", cache_payload, ttl_s=3600.0)
        try:
            from engine.cache.wrappers.execution_mode import prime_execution_mode

            prime_execution_mode(cache_payload)
        except Exception as e:
            _warn_nonfatal(
                "EXECUTION_MODE_REDIS_CACHE_PRIME_FAILED",
                e,
                once_key="set_mode_redis_prime",
                mode=str(mode),
                actor=str(actor),
            )
        cache_invalidate_namespace("api_read", prefix="execution_stats")
        cache_invalidate_namespace("api_read", prefix="execution_metrics")

        try:
            # Also write the shared event log so dashboards and recovery tools
            # can observe mode changes without polling the singleton row only.
            append_event(
                event_type="execution_mode_change",
                event_source="engine.execution.execution_mode",
                event_version=1,
                entity_type="execution_mode",
                entity_id="singleton",
                correlation_id=str(int(st["_now_ms"])),
                payload={
                    "ts_ms": int(st["_now_ms"]),
                    "prev_mode": str(st["_prev_mode"]),
                    "new_mode": str(mode),
                    "prev_armed": int(st["_prev_armed"]),
                    "new_armed": int(st["_new_armed"]),
                    "actor": str(actor or ""),
                    "reason": str(reason or ""),
                },
                ts_ms=int(st["_now_ms"]),
            )
        except Exception as e:
            _warn_nonfatal(
                "EXECUTION_MODE_APPEND_EVENT_FAILED",
                e,
                once_key="set_mode_append_event",
                mode=str(mode),
                actor=str(actor),
            )
    elif con is not None:
        register = getattr(con, "register_after_commit", None)
        if callable(register):
            register(lambda: __import__(
                "engine.cache.wrappers.execution_mode",
                fromlist=["prime_execution_mode"],
            ).prime_execution_mode(cache_payload))

    return cache_payload


def set_execution_armed(
    armed: int,
    actor: str = "operator",
    reason: str = "",
    con=None,
) -> Dict[str, Any]:

    actor = str(actor or "operator")
    reason = str(reason or "")

    def _apply(db) -> Dict[str, Any]:
        init_db()
        _ensure_schema(db)
        _ensure_row(db)

        now_ms = _now_ms()
        cur = db.execute("SELECT mode, armed FROM execution_mode WHERE id=1").fetchone()
        mode = str(cur[0])
        prev_mode = str(mode)
        prev_armed = int(cur[1] or 0)

        new_armed = 1 if int(armed) == 1 else 0
        if mode != "live":
            new_armed = 0

        db.execute(
            "UPDATE execution_mode SET armed=?, updated_ts_ms=?, actor=?, reason=? WHERE id=1",
            (new_armed, now_ms, actor, reason),
        )

        append_chain_row(
            "execution_mode_audit",
            {
                "ts_ms": int(now_ms),
                "prev_mode": mode,
                "new_mode": mode,
                "actor": actor,
                "reason": reason,
                "prev_armed": int(prev_armed),
                "new_armed": int(new_armed),
            },
            db,
        )

        return {
            "mode": str(mode),
            "armed": int(new_armed or 0),
            "updated_ts_ms": int(now_ms),
            "actor": str(actor or ""),
            "reason": str(reason or ""),
            "_prev_mode": str(prev_mode),
            "_prev_armed": int(prev_armed),
            "_now_ms": int(now_ms),
            "_new_armed": int(new_armed),
        }

    if con is None:
        st = run_write_txn(
            _apply,
            table="execution_mode",
            operation="set_execution_armed",
            context={"armed": int(armed), "actor": str(actor)},
        )
        publish_side_effects = True
    else:
        owns_txn = False
        try:
            owns_txn = _begin_owned_write(con)
            st = _apply(con)
            if owns_txn:
                con.commit()
        except Exception:
            if owns_txn and bool(getattr(con, "in_transaction", False)):
                con.rollback()
            raise
        publish_side_effects = bool(owns_txn)

    cache_payload = {
        "mode": str(st["mode"]),
        "armed": int(st["armed"]),
        "updated_ts_ms": int(st["updated_ts_ms"]),
        "actor": str(st["actor"]),
        "reason": str(st["reason"]),
    }
    if publish_side_effects:
        cache_set("execution_mode", "singleton", cache_payload, ttl_s=3600.0)
        try:
            from engine.cache.wrappers.execution_mode import prime_execution_mode

            prime_execution_mode(cache_payload)
        except Exception as e:
            _warn_nonfatal(
                "EXECUTION_MODE_REDIS_CACHE_PRIME_FAILED",
                e,
                once_key="set_armed_redis_prime",
                mode=str(st["mode"]),
                actor=str(actor),
            )
        cache_invalidate_namespace("api_read", prefix="execution_stats")
        cache_invalidate_namespace("api_read", prefix="execution_metrics")

        try:
            append_event(
                event_type="execution_mode_change",
                event_source="engine.execution.execution_mode",
                event_version=1,
                entity_type="execution_mode",
                entity_id="singleton",
                correlation_id=str(int(st["_now_ms"])),
                payload={
                    "ts_ms": int(st["_now_ms"]),
                    "prev_mode": str(st["_prev_mode"]),
                    "new_mode": str(st["mode"]),
                    "prev_armed": int(st["_prev_armed"]),
                    "new_armed": int(st["_new_armed"]),
                    "actor": str(actor or ""),
                    "reason": str(reason or ""),
                },
                ts_ms=int(st["_now_ms"]),
            )
        except Exception as e:
            _warn_nonfatal(
                "EXECUTION_MODE_APPEND_EVENT_FAILED",
                e,
                once_key="set_armed_append_event",
                mode=str(st["mode"]),
                actor=str(actor),
            )
    elif con is not None:
        register = getattr(con, "register_after_commit", None)
        if callable(register):
            register(lambda: __import__(
                "engine.cache.wrappers.execution_mode",
                fromlist=["prime_execution_mode"],
            ).prime_execution_mode(cache_payload))

    return cache_payload


def execution_allowed_for_real_trading(
    con=None,
    symbol: Optional[str] = None,
    regime: Optional[str] = None,
) -> Tuple[bool, str, Dict[str, Any]]:

    owns = False
    if con is None:
        con = connect()
        owns = True

    try:
        state = get_execution_mode(con=con)
        mode = str(state.get("mode", "paper"))
        armed = int(state.get("armed", 0))

        if mode != "live":
            return False, "mode_not_live", {"mode": mode, "armed": armed}

        if armed != 1:
            return False, "live_not_armed", {"mode": mode, "armed": armed}

        try:
            from engine.runtime.lifecycle_state import LIVE as _RUNTIME_LIVE
            from engine.runtime.lifecycle_state import get_state as _get_runtime_state

            runtime_snapshot = dict(_get_runtime_state() or {})
            runtime_state = str(runtime_snapshot.get("state") or "").strip().upper()
        except Exception as exc:
            _warn_nonfatal(
                "EXECUTION_MODE_RUNTIME_STATE_LOAD_FAILED",
                exc,
                once_key="execution_mode_runtime_state_load",
            )
            return False, "runtime_state_unavailable", {
                "mode": mode,
                "armed": armed,
            }

        if runtime_state != str(_RUNTIME_LIVE):
            return False, f"runtime_state_{runtime_state.lower() or 'unknown'}", {
                "mode": mode,
                "armed": armed,
                "runtime_state": runtime_snapshot,
            }

        if live_execution_disabled():
            return False, DISABLE_LIVE_EXECUTION_REASON, {
                "mode": mode,
                "armed": armed,
                "runtime_state": runtime_snapshot,
            }

        live_preflight = live_trading_preflight(engine_mode=mode)
        if not bool(live_preflight.get("ok")):
            return False, str(live_preflight.get("reason") or "live_trading_preflight_failed"), {
                "mode": mode,
                "armed": armed,
                "runtime_state": runtime_snapshot,
                "live_trading_preflight": live_preflight,
            }

        if _PROVIDER_HEALTH:
            if _PROVIDER_HEALTH.get("ok") is False:
                return False, "provider_health_bad", {
                    "mode": mode,
                    "armed": armed,
                    "provider": _PROVIDER_HEALTH,
                }

        return True, "ok", {"mode": mode, "armed": armed}
    finally:
        if owns:
            con.close()


def get_execution_overlays() -> Dict[str, Any]:
    return {
        "mode": {"default": DEFAULT_MODE},
        "cost_model": {
            "fees_bps": float(DEFAULT_FEES_BPS),
            "slippage_bps": float(DEFAULT_SLIPPAGE_BPS),
            "spread_bps_cap": float(DEFAULT_SPREAD_BPS_CAP),
        },
        "portfolio_exec_realism": {
            "enabled": os.environ.get("PORTFOLIO_USE_EXEC_REALISM", "1") == "1",
            "max_price_age_s": float(os.environ.get("PORTFOLIO_EXEC_MAX_PRICE_AGE_S", "120")),
            "stale_half_factor": float(os.environ.get("PORTFOLIO_EXEC_STALE_HALF_FACTOR", "0.50")),
            "stress_th": float(os.environ.get("PORTFOLIO_EXEC_STRESS_TH", "0.75")),
            "stress_factor": float(os.environ.get("PORTFOLIO_EXEC_STRESS_FACTOR", "0.60")),
        },
    }
