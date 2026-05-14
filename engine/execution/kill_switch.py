"""Persistent and fail-closed execution kill-switch controls.

This module combines lifecycle gating, environment overrides, database-backed
switches, freshness checks, and automatic capital/model risk triggers into the
single execution barrier used by live-order paths.
"""

import json
import logging
import os
import threading
import time
from typing import Any, Dict, Optional, Tuple, List

from engine.audit.chain import append_chain_row
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.storage import DB_PATH, connect, _table_exists, run_write_txn
from engine.runtime.event_log import append_event

LOGGER = logging.getLogger(__name__)
_WARNED_NONFATAL_KEYS: set[str] = set()
_KILL_SWITCH_SCHEMA_READY_LOCK = threading.Lock()
_KILL_SWITCH_SCHEMA_READY_PATH = ""


def _warn_nonfatal(code: str, error: Exception, *, once_key: str | None = None, **extra: Any) -> None:
    key = str(once_key or "")
    if key:
        if key in _WARNED_NONFATAL_KEYS:
            return
        _WARNED_NONFATAL_KEYS.add(key)
    log_failure(
        LOGGER,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.execution.kill_switch",
        extra=extra or None,
        include_health=False,
        persist=False,
    )


def _schema_path_key(con=None) -> str:
    db_path = getattr(con, "_db_path", None) or DB_PATH
    try:
        return str(db_path.resolve())
    except Exception as e:
        _warn_nonfatal(
            "KILL_SWITCH_SCHEMA_PATH_RESOLVE_FAILED",
            e,
            once_key=f"schema_path_resolve:{db_path}",
            db_path=str(db_path),
        )
        return str(db_path)


def _mark_schema_ready(con=None) -> None:
    global _KILL_SWITCH_SCHEMA_READY_PATH
    ready_key = _schema_path_key(con)
    with _KILL_SWITCH_SCHEMA_READY_LOCK:
        _KILL_SWITCH_SCHEMA_READY_PATH = ready_key


def _schema_ready_for_reads(con) -> bool:
    ready_key = _schema_path_key(con)
    with _KILL_SWITCH_SCHEMA_READY_LOCK:
        if _KILL_SWITCH_SCHEMA_READY_PATH == ready_key:
            return True
    try:
        if (
            _table_exists(con, "kill_switch_state")
            and _table_exists(con, "kill_switch_audit")
            and _table_exists(con, "risk_events")
            and _table_exists(con, "portfolio_kill_snapshots")
        ):
            _mark_schema_ready(con)
            return True
    except Exception as e:
        _warn_nonfatal(
            "KILL_SWITCH_SCHEMA_PROBE_FAILED",
            e,
            once_key=f"schema_probe:{ready_key}",
            db_path=str(ready_key),
        )
    return False

try:
    from engine.runtime.lifecycle_state import get_state as _get_lifecycle_state  # type: ignore
except Exception:
    _get_lifecycle_state = None  # type: ignore

if _get_lifecycle_state is None:
    # Fail-closed behaviour if lifecycle module missing
    def _get_lifecycle_state():
        return {"state": "BOOTING"}

SCOPES = {"global", "symbol", "regime", "model"}

ENV_GLOBAL_KEYS = ("KILL_SWITCH_GLOBAL", "TRADING_KILL_SWITCH", "KILL_SWITCH")
ENV_SYMBOLS_KEY = "KILL_SWITCH_SYMBOLS"   # CSV: "SPY,BTC,ETH"
ENV_REGIMES_KEY = "KILL_SWITCH_REGIMES"   # CSV: "low_vol,high_vol,trend,shock"
ENV_MODELS_KEY = "KILL_SWITCH_MODELS"     # CSV: "baseline,model_a,model_b"

# ------            -- ------------------------------------------------------
# Circuit breakers (production safety)
# ------            -- ------------------------------------------------------
REQUIRE_FRESH_DATA = os.environ.get("KILL_SWITCH_REQUIRE_FRESH_DATA", "1") == "1"
REQUIRE_FRESH_JOBS = os.environ.get("KILL_SWITCH_REQUIRE_FRESH_JOBS", "1") == "1"

MAX_PRICE_STALE_S = int(os.environ.get("KILL_SWITCH_MAX_PRICE_STALE_S", "300"))          # 5m
MAX_EVENT_STALE_S = int(os.environ.get("KILL_SWITCH_MAX_EVENT_STALE_S", "3600"))         # 1h
MAX_PRED_STALE_S  = int(os.environ.get("KILL_SWITCH_MAX_PRED_STALE_S", "900"))           # 15m

# Heartbeat freshness (job_heartbeats.ts_ms)
MAX_JOB_STALE_S   = int(os.environ.get("KILL_SWITCH_MAX_JOB_STALE_S", "600"))            # 10m
REQUIRED_JOBS_CSV = os.environ.get("KILL_SWITCH_REQUIRED_JOBS", "ingestion_runtime,process_events").strip()

# Capital-aware kill switch (additive, non-breaking)
CAPITAL_AWARE_KILL_SWITCH = os.environ.get("CAPITAL_AWARE_KILL_SWITCH", "1") == "1"
KILL_SWITCH_DAILY_DRAWDOWN_PCT = float(os.environ.get("KILL_SWITCH_DAILY_DRAWDOWN_PCT", "0.05"))
KILL_SWITCH_ROLLING_DRAWDOWN_PCT = float(os.environ.get("KILL_SWITCH_ROLLING_DRAWDOWN_PCT", "0.12"))
KILL_SWITCH_ROLLING_DRAWDOWN_LOOKBACK_DAYS = int(os.environ.get("KILL_SWITCH_ROLLING_DRAWDOWN_LOOKBACK_DAYS", "7"))
KILL_SWITCH_VAR_LOOKBACK_POINTS = int(os.environ.get("KILL_SWITCH_VAR_LOOKBACK_POINTS", "250"))
KILL_SWITCH_VAR_CONFIDENCE = float(os.environ.get("KILL_SWITCH_VAR_CONFIDENCE", "0.99"))
KILL_SWITCH_VAR_MIN_HISTORY = int(os.environ.get("KILL_SWITCH_VAR_MIN_HISTORY", "30"))
KILL_SWITCH_CONCENTRATION_MAX_SINGLE = float(os.environ.get("KILL_SWITCH_CONCENTRATION_MAX_SINGLE", "0.35"))
KILL_SWITCH_CONCENTRATION_MAX_TOP3 = float(os.environ.get("KILL_SWITCH_CONCENTRATION_MAX_TOP3", "0.70"))
KILL_SWITCH_CONCENTRATION_MAX_SINGLE = float(os.environ.get("KILL_SWITCH_CONCENTRATION_MAX_SINGLE", "0.35"))
KILL_SWITCH_CONCENTRATION_MAX_TOP3 = float(os.environ.get("KILL_SWITCH_CONCENTRATION_MAX_TOP3", "0.70"))
KILL_SWITCH_COOLDOWN_MINUTES = float(os.environ.get("KILL_SWITCH_COOLDOWN_MINUTES", "60"))
MODEL_AWARE_KILL_SWITCH = os.environ.get("MODEL_AWARE_KILL_SWITCH", "1") == "1"
KILL_SWITCH_MODEL_MAX_DRAWDOWN = float(os.environ.get("KILL_SWITCH_MODEL_MAX_DRAWDOWN", "0"))
KILL_SWITCH_MODEL_MAX_CONSECUTIVE_LOSSES = int(os.environ.get("KILL_SWITCH_MODEL_MAX_CONSECUTIVE_LOSSES", "0"))
KILL_SWITCH_MODEL_LOOKBACK_ROWS = int(os.environ.get("KILL_SWITCH_MODEL_LOOKBACK_ROWS", "250"))

def _now_ms() -> int:
    return int(time.time() * 1000)

def _maybe_auto_expire(con, scope: str, key: str, st):
    """
    Auto-clear DB kill switch if meta_json contains until_ts_ms
    and the time has passed.
    Expiry is best-effort convenience on top of the persisted switch state.
    """
    if not st:
        return st

    try:
        enabled = int(st[0] or 0)
    except Exception as e:
        _warn_nonfatal(
            "KILL_SWITCH_AUTO_EXPIRE_STATE_PARSE_FAILED",
            e,
            once_key=f"auto_expire_state_parse:{scope}:{key}",
            scope=str(scope),
            key=str(key),
        )
        state = st
        return state

    if enabled != 1:
        return st

    meta_json = st[3]
    if not meta_json:
        return st

    try:
        meta = json.loads(meta_json)
        until_ms = int(meta.get("until_ts_ms") or 0)
    except Exception as e:
        _warn_nonfatal(
            "KILL_SWITCH_AUTO_EXPIRE_META_PARSE_FAILED",
            e,
            once_key=f"auto_expire_meta_parse:{scope}:{key}",
            scope=str(scope),
            key=str(key),
        )
        state = st
        return state

    if until_ms <= 0:
        return st

    if _now_ms() <= until_ms:
        return st

    # expired → clear
    try:
        clear(
            scope,
            key,
            reason="auto_expire",
            actor="system",
            meta={"until_ts_ms": int(until_ms)},
            con=con,
        )

    except Exception as e:
        _warn_nonfatal(
            "KILL_SWITCH_AUTO_EXPIRE_CLEAR_FAILED",
            e,
            once_key=f"auto_expire:{scope}:{key}",
            scope=str(scope),
            key=str(key),
        )

    try:
        return _read_state(con, scope, key)
    except Exception as e:
        _warn_nonfatal(
            "KILL_SWITCH_AUTO_EXPIRE_REREAD_FAILED",
            e,
            once_key=f"auto_expire_reread:{scope}:{key}",
            scope=str(scope),
            key=str(key),
        )
        state = None
        return state

def _s(x: Any) -> str:
    return str(x or "").strip()

def _norm_scope(scope: str) -> str:
    s = _s(scope).lower()
    if s not in SCOPES:
        raise ValueError(f"invalid kill switch scope: {scope}")
    return s

def _norm_key(scope: str, key: str) -> str:
    k = _s(key)
    if not k:
        raise ValueError("kill switch key required")
    if scope == "global":
        return "global"
    return k

def _env_truthy(v: Optional[str]) -> bool:
    if v is None:
        return False
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "y", "on")

def _env_global_enabled() -> bool:
    for k in ENV_GLOBAL_KEYS:
        if _env_truthy(os.environ.get(k)):
            return True
    return False

def _parse_csv_env(name: str) -> Dict[str, bool]:
    raw = _s(os.environ.get(name))
    if not raw:
        return {}
    out: Dict[str, bool] = {}
    for part in raw.split(","):
        p = _s(part)
        if p:
            out[p] = True
    return out

def _env_symbol_enabled(symbol: str) -> bool:
    sym = _s(symbol)
    if not sym:
        return False
    return _parse_csv_env(ENV_SYMBOLS_KEY).get(sym, False)

def _env_regime_enabled(regime: str) -> bool:
    r = _s(regime)
    if not r:
        return False
    m = _parse_csv_env(ENV_REGIMES_KEY)
    if r in m:
        return True
    rl = r.lower()
    for k in m.keys():
        if k.lower() == rl:
            return True
    return False

def _norm_model_id(model_id: Optional[str]) -> str:
    mid = _s(model_id)
    return mid or "baseline"

def _env_model_enabled(model_id: Optional[str]) -> bool:
    mid = _norm_model_id(model_id)
    if not mid:
        return False
    m = _parse_csv_env(ENV_MODELS_KEY)
    if mid in m:
        return True
    ml = mid.lower()
    for k in m.keys():
        if k.lower() == ml:
            return True
    return False

def _read_state(con, scope: str, key: str) -> Optional[Tuple[int, str, str, str, int, int]]:
    row = con.execute(
        """
        SELECT enabled, reason, actor, meta_json, created_ts_ms, updated_ts_ms
        FROM kill_switch_state
        WHERE scope=? AND key=?
        """,
        (scope, key),
    ).fetchone()
    if not row:
        return None
    try:
        enabled = int(row[0] or 0)
    except Exception:
        enabled = 0
    return (
        enabled,
        _s(row[1]),
        _s(row[2]),
        _s(row[3]),
        int(row[4] or 0),
        int(row[5] or 0),
    )


def _read_state_hot(con, scope: str, key: str) -> Optional[Tuple[int, str, str, str, int, int]]:
    """Read persisted switch state from Redis on non-transactional hot paths."""

    if not bool(getattr(con, "in_transaction", False)):
        try:
            from engine.cache.wrappers.kill_switch import read_kill_switch

            snapshot_payload = read_kill_switch() or {}
            for row in list(snapshot_payload.get("state") or []):
                if not isinstance(row, dict):
                    continue
                if _s(row.get("scope")) != _s(scope) or _s(row.get("key")) != _s(key):
                    continue
                meta_json = json.dumps(dict(row.get("meta") or {}), separators=(",", ":"), sort_keys=True)
                cached_state = (
                    1 if int(row.get("enabled") or 0) else 0,
                    _s(row.get("reason")),
                    _s(row.get("actor")),
                    meta_json,
                    int(row.get("created_ts_ms") or 0),
                    int(row.get("updated_ts_ms") or 0),
                )
                return _maybe_auto_expire(con, scope, key, cached_state)
            return None
        except Exception as e:
            _warn_nonfatal(
                "KILL_SWITCH_REDIS_CACHE_READ_FAILED",
                e,
                once_key=f"redis_read:{scope}:{key}",
                scope=str(scope),
                key=str(key),
            )
    return _read_state(con, scope, key)


def _ensure_schema(con) -> None:
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS kill_switch_state (
          scope TEXT NOT NULL,
          key TEXT NOT NULL,
          enabled INTEGER NOT NULL DEFAULT 0,
          reason TEXT,
          actor TEXT NOT NULL DEFAULT 'system',
          meta_json TEXT,
          created_ts_ms INTEGER NOT NULL,
          updated_ts_ms INTEGER NOT NULL,
          PRIMARY KEY (scope, key)
        );

        CREATE INDEX IF NOT EXISTS idx_kill_switch_scope_enabled
          ON kill_switch_state(scope, enabled);

        CREATE TABLE IF NOT EXISTS kill_switch_audit (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER NOT NULL,
          action TEXT NOT NULL,
          scope TEXT NOT NULL,
          key TEXT NOT NULL,
          enabled INTEGER NOT NULL,
          actor TEXT NOT NULL,
          reason TEXT,
          meta_json TEXT,
          prev_hash BLOB,
          row_hash BLOB NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_kill_switch_audit_ts
          ON kill_switch_audit(ts_ms);

        CREATE TABLE IF NOT EXISTS risk_events (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER NOT NULL,
          trigger_type TEXT NOT NULL,
          reason TEXT,
          equity REAL,
          drawdown_pct REAL,
          var_pct REAL,
          concentration REAL,
          positions INTEGER,
          metadata_json TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_risk_events_ts
          ON risk_events(ts_ms);

        CREATE TABLE IF NOT EXISTS portfolio_kill_snapshots (
          ts_ms INTEGER PRIMARY KEY,
          equity REAL,
          gross_exposure REAL,
          net_exposure REAL,
          per_symbol_weights_json TEXT,
          positions_json TEXT,
          rolling_drawdown REAL,
          var_pct REAL,
          metadata_json TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_portfolio_kill_snapshots_ts
          ON portfolio_kill_snapshots(ts_ms);
        """
    )
    _mark_schema_ready(con)


def _begin_owned_write(con) -> bool:
    if bool(getattr(con, "in_transaction", False)):
        return False
    begin = getattr(con, "begin_managed_write", None)
    if callable(begin):
        begin()
        return True
    raise RuntimeError("managed_write_begin_unavailable")

def set_kill_switch(
    scope: str,
    key: str,
    enabled: int,
    reason: Optional[str] = None,
    actor: str = "system",
    meta: Optional[Dict[str, Any]] = None,
    action: str = "SET",
    con=None,
) -> None:
    """Persist and audit a kill-switch state change.

    Parameters
    ----------
    scope : {"global", "symbol", "regime", "model"}
        Scope namespace for the switch.
    key : str
        Scope-specific identifier. Global switches normalize to ``"global"``.
    enabled : int
        Truthy values enable the switch; falsy values clear it.
    reason : str, optional
        Human-readable trigger or operator reason recorded with the state.
    actor : str, default="system"
        Actor name recorded in audit rows and emitted events.
    meta : dict, optional
        JSON-serializable metadata persisted in ``meta_json``. If it contains
        ``until_ts_ms`` (epoch milliseconds), later reads may auto-expire the
        switch after that timestamp.
    action : str, default="SET"
        Audit/event verb describing the change source.
    con : storage connection, optional
        Existing write connection. When omitted, the function manages its own
        transaction and only publishes side effects after a committed write.

    Returns
    -------
    None

    Raises
    ------
    Exception
        Propagates schema or write failures so callers fail closed rather than
        assuming execution is safe.

    Side Effects
    ------------
    Upserts ``kill_switch_state``, appends ``kill_switch_audit``, emits a
    runtime event after committed writes, and updates lifecycle state for the
    global switch scope.
    """
    from engine.runtime.storage import init_db

    active_caller_txn = bool(con is not None and getattr(con, "in_transaction", False))
    if not active_caller_txn:
        init_db()
    scope_n = _norm_scope(scope)
    key_n = _norm_key(scope_n, key)
    en = 1 if int(enabled) else 0
    now_ms = _now_ms()
    meta_json = json.dumps(meta or {}, separators=(",", ":"), sort_keys=True)
    actor_s = _s(actor) or "system"
    reason_s = _s(reason)

    def _apply(db) -> None:
        cur = db.execute(
            """
            SELECT enabled, created_ts_ms
            FROM kill_switch_state
            WHERE scope=? AND key=?
            """,
            (scope_n, key_n),
        ).fetchone()

        if cur is None:
            db.execute(
                """
                INSERT INTO kill_switch_state
                  (scope, key, enabled, reason, actor, meta_json, created_ts_ms, updated_ts_ms)
                VALUES (?,?,?,?,?,?,?,?)
                """,
                (scope_n, key_n, en, reason_s, actor_s, meta_json, now_ms, now_ms),
            )
        else:
            created_ms = int(cur[1] or now_ms)
            db.execute(
                """
                UPDATE kill_switch_state
                SET enabled=?, reason=?, actor=?, meta_json=?, updated_ts_ms=?
                WHERE scope=? AND key=?
                """,
                (en, reason_s, actor_s, meta_json, now_ms, scope_n, key_n),
            )
            if created_ms <= 0:
                db.execute(
                    "UPDATE kill_switch_state SET created_ts_ms=? WHERE scope=? AND key=?",
                    (now_ms, scope_n, key_n),
                )

        append_chain_row(
            "kill_switch_audit",
            {
                "ts_ms": int(now_ms),
                "action": _s(action).upper() or "SET",
                "scope": scope_n,
                "key": key_n,
                "enabled": int(en),
                "actor": actor_s,
                "reason": reason_s,
                "meta_json": meta_json,
            },
            db,
        )

    if con is None:
        schema_con = connect()
        try:
            _ensure_schema(schema_con)
            schema_con.commit()
        finally:
            schema_con.close()
        run_write_txn(
            _apply,
            table="kill_switch_state",
            operation="set_kill_switch",
            context={"scope": str(scope_n), "key": str(key_n), "enabled": int(en)},
        )
        publish_side_effects = True
    else:
        if not bool(getattr(con, "in_transaction", False)):
            _ensure_schema(con)
        owns_txn = False
        try:
            owns_txn = _begin_owned_write(con)
            _apply(con)
            if owns_txn:
                con.commit()
        except Exception:
            if owns_txn and bool(getattr(con, "in_transaction", False)):
                con.rollback()
            raise
        publish_side_effects = bool(owns_txn)

    if not publish_side_effects and con is not None:
        register = getattr(con, "register_after_commit", None)
        if callable(register):
            register(lambda: __import__(
                "engine.cache.wrappers.kill_switch",
                fromlist=["prime_kill_switch"],
            ).prime_kill_switch())

    if publish_side_effects:
        try:
            from engine.cache.wrappers.kill_switch import prime_kill_switch

            prime_kill_switch()
        except Exception as e:
            _warn_nonfatal(
                "KILL_SWITCH_REDIS_CACHE_PRIME_FAILED",
                e,
                once_key=f"redis_prime:{scope_n}:{key_n}",
                scope=str(scope_n),
                key=str(key_n),
            )
        try:
            append_event(
                event_type=("kill_switch_enabled" if int(en) == 1 else "kill_switch_cleared"),
                event_source="engine.execution.kill_switch",
                event_version=1,
                entity_type="kill_switch",
                entity_id=f"{scope_n}:{key_n}",
                correlation_id=f"{scope_n}:{key_n}",
                payload={
                    "ts_ms": int(now_ms),
                    "scope": str(scope_n),
                    "key": str(key_n),
                    "enabled": int(en),
                    "reason": (str(reason_s) if reason_s is not None else None),
                    "actor": str(actor_s),
                    "action": str(_s(action).upper() or "SET"),
                    "meta": json.loads(meta_json or "{}") if meta_json else {},
                },
                ts_ms=int(now_ms),
            )
        except Exception as e:
            _warn_nonfatal(
                "KILL_SWITCH_APPEND_EVENT_FAILED",
                e,
                once_key=f"append_event:{scope_n}:{key_n}",
                scope=str(scope_n),
                key=str(key_n),
                enabled=int(en),
            )

        try:
            if scope_n == "global":
                from engine.runtime.lifecycle_state import (
                    set_state as _lifecycle_set_state,
                    KILL_SWITCH as _KILL_SWITCH,
                    DEGRADED as _DEGRADED,
                )

                if int(en) == 1:
                    _lifecycle_set_state(_KILL_SWITCH, reason_s or actor_s or "kill_switch_enabled")
                elif _s(action).upper() == "CLEAR":
                    _lifecycle_set_state(_DEGRADED, reason_s or "kill_switch_cleared")
        except Exception as e:
            _warn_nonfatal(
                "KILL_SWITCH_LIFECYCLE_UPDATE_FAILED",
                e,
                once_key=f"lifecycle_update:{scope_n}:{key_n}",
                scope=str(scope_n),
                key=str(key_n),
            )

def activate(scope: str, key: str, reason: str, actor: str = "system", meta: Optional[Dict[str, Any]] = None, action: str = "AUTO", con=None) -> None:
    """Enable a kill switch using the standard persisted/audited workflow.

    Parameters
    ----------
    scope : str
        Kill-switch scope passed through to :func:`set_kill_switch`.
    key : str
        Scope-specific identifier.
    reason : str
        Operator or automatic trigger description.
    actor : str, default="system"
        Actor recorded in audit rows and events.
    meta : dict, optional
        Additional JSON-serializable metadata recorded with the switch.
    action : str, default="AUTO"
        Audit verb describing the activation source.
    con : storage connection, optional
        Existing write connection to reuse.
    """
    set_kill_switch(scope, key, 1, reason=reason, actor=actor, meta=meta, action=action, con=con)

def clear(scope: str, key: str, reason: Optional[str] = None, actor: str = "system", meta: Optional[Dict[str, Any]] = None, con=None) -> None:
    """Clear a persisted kill switch using the standard audited workflow.

    Parameters
    ----------
    scope : str
        Kill-switch scope passed through to :func:`set_kill_switch`.
    key : str
        Scope-specific identifier.
    reason : str, optional
        Optional operator note explaining the clear action.
    actor : str, default="system"
        Actor recorded in audit rows and events.
    meta : dict, optional
        Additional JSON-serializable metadata recorded with the clear action.
    con : storage connection, optional
        Existing write connection to reuse.
    """
    set_kill_switch(scope, key, 0, reason=reason, actor=actor, meta=meta, action="CLEAR", con=con)

def _latest_ts_ms(con, table: str, ts_col: str = "ts_ms") -> int:
    try:
        row = con.execute(f"SELECT MAX({ts_col}) FROM {table}").fetchone()
        return int(row[0] or 0) if row and row[0] is not None else 0
    except Exception as e:
        _warn_nonfatal(
            "KILL_SWITCH_LATEST_TS_MS_FAILED",
            e,
            once_key=f"latest_ts_ms:{table}:{ts_col}",
            table=str(table),
            ts_col=str(ts_col),
        )
        ts_ms = 0
        return ts_ms

def _job_heartbeat_ts_ms(con, job_name: str) -> int:
    try:
        row = con.execute(
            "SELECT ts_ms FROM job_heartbeats WHERE job_name=?",
            (str(job_name),),
        ).fetchone()
        return int(row[0] or 0) if row and row[0] is not None else 0
    except Exception as e:
        _warn_nonfatal(
            "KILL_SWITCH_JOB_HEARTBEAT_TS_FAILED",
            e,
            once_key=f"job_heartbeat_ts:{job_name}",
            job_name=str(job_name),
        )
        ts_ms = 0
        return ts_ms

def _required_jobs() -> list[str]:
    raw = _s(REQUIRED_JOBS_CSV)
    if not raw:
        return []
    out = []
    for p in raw.split(","):
        s = _s(p)
        if s:
            out.append(s)
    return out


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if v != v:
            return float(default)
        return float(v)
    except Exception as e:
        _warn_nonfatal(
            "KILL_SWITCH_SAFE_FLOAT_FAILED",
            e,
            once_key="safe_float_failed",
            value_type=type(x).__name__,
        )
        fallback = float(default)
        return fallback


def _equity_series(con, lookback_ms: Optional[int] = None, limit: Optional[int] = None) -> List[Tuple[int, float]]:
    q = "SELECT ts_ms, equity FROM equity_history"
    args: List[Any] = []
    if lookback_ms is not None and int(lookback_ms) > 0:
        q += " WHERE ts_ms >= ?"
        args.append(int(_now_ms() - int(lookback_ms)))
    q += " ORDER BY ts_ms ASC"
    if limit is not None and int(limit) > 0:
        q += f" LIMIT {int(limit)}"
    rows = con.execute(q, tuple(args)).fetchall()
    out: List[Tuple[int, float]] = []
    for ts_ms, equity in rows or []:
        try:
            out.append((int(ts_ms or 0), float(equity or 0.0)))
        except Exception as e:
            _warn_nonfatal(
                "KILL_SWITCH_EQUITY_SERIES_ROW_FAILED",
                e,
                once_key="equity_series_row_failed",
            )
            continue
    return out


def _drawdown_pct(series: List[Tuple[int, float]]) -> float:
    if not series:
        return 0.0
    peak = 0.0
    worst = 0.0
    for _ts_ms, equity in series:
        eq = _safe_float(equity, 0.0)
        if eq > peak:
            peak = eq
        if peak > 0.0:
            dd = (peak - eq) / peak
            if dd > worst:
                worst = dd
    return float(max(0.0, worst))


def _returns_from_series(series: List[Tuple[int, float]]) -> List[float]:
    out: List[float] = []
    prev = None
    for _ts_ms, equity in series:
        eq = _safe_float(equity, 0.0)
        if prev is not None and prev > 0.0 and eq > 0.0:
            out.append(float((eq / prev) - 1.0))
        prev = eq
    return out


def _percentile(sorted_vals: List[float], q: float) -> float:
    if not sorted_vals:
        return 0.0
    qn = max(0.0, min(1.0, float(q)))
    idx = int(round((len(sorted_vals) - 1) * qn))
    if idx < 0:
        idx = 0
    if idx >= len(sorted_vals):
        idx = len(sorted_vals) - 1
    return float(sorted_vals[idx])


def _model_pnl_rows(con, model_id: str, limit: Optional[int] = None) -> List[Tuple[int, float]]:
    mid = _norm_model_id(model_id)
    q = """
        SELECT
          ts_ms,
          COALESCE(
            realized_pnl + COALESCE(unrealized_pnl, 0.0) - COALESCE(fees, 0.0) - COALESCE(json_extract(extra_json, '$.slippage_cost'), 0.0),
            0.0
          ) AS total_pnl
        FROM pnl_attribution
        WHERE COALESCE(NULLIF(TRIM(model_id), ''), 'baseline') = ?
        ORDER BY ts_ms DESC
    """
    if limit is not None and int(limit) > 0:
        q += f" LIMIT {int(limit)}"
    try:
        rows = con.execute(q, (mid,)).fetchall()
    except Exception as e:
        _warn_nonfatal(
            "KILL_SWITCH_MODEL_PNL_ROWS_QUERY_FAILED",
            e,
            once_key=f"model_pnl_rows:{mid}",
            model_id=str(mid),
        )
        pnl_rows: List[Tuple[int, float]] = []
        return pnl_rows
    out: List[Tuple[int, float]] = []
    for ts_ms, total_pnl in reversed(rows or []):
        try:
            out.append((int(ts_ms or 0), float(total_pnl or 0.0)))
        except Exception as e:
            _warn_nonfatal(
                "KILL_SWITCH_MODEL_PNL_ROW_FAILED",
                e,
                once_key=f"model_pnl_row:{mid}",
                model_id=str(mid),
            )
            continue
    return out


def _model_drawdown_amount(series: List[Tuple[int, float]]) -> float:
    cumulative = 0.0
    peak = 0.0
    worst = 0.0
    for _ts_ms, pnl in series:
        cumulative += float(_safe_float(pnl, 0.0))
        if cumulative > peak:
            peak = cumulative
        dd = peak - cumulative
        if dd > worst:
            worst = dd
    return float(max(0.0, worst))


def _model_consecutive_losses(series: List[Tuple[int, float]]) -> int:
    losses = 0
    for _ts_ms, pnl in reversed(series or []):
        if float(_safe_float(pnl, 0.0)) < 0.0:
            losses += 1
            continue
        break
    return int(losses)


def _portfolio_concentration(con) -> Dict[str, Any]:
    if not _table_exists(con, "portfolio_state"):
        return {
            "gross": 0.0,
            "top1_weight": 0.0,
            "top1_symbol": None,
            "top3_weight": 0.0,
            "positions": 0,
        }

    rows = con.execute(
        """
        SELECT symbol,
               MAX(CASE
                     WHEN UPPER(COALESCE(side, '')) IN ('LONG','BUY') THEN 'LONG'
                     WHEN UPPER(COALESCE(side, '')) IN ('SHORT','SELL') THEN 'SHORT'
                     ELSE 'FLAT'
                   END) AS side,
               SUM(CASE
                     WHEN UPPER(COALESCE(side, '')) IN ('LONG','BUY') THEN ABS(COALESCE(weight, 0.0))
                     WHEN UPPER(COALESCE(side, '')) IN ('SHORT','SELL') THEN ABS(COALESCE(weight, 0.0))
                     ELSE 0.0
                   END) AS weight
        FROM portfolio_state
        GROUP BY symbol
        """
    ).fetchall()
    weights: List[Tuple[str, float]] = []
    for sym, side, weight in rows or []:
        w = abs(_safe_float(weight, 0.0))
        if str(side or "").upper() == "FLAT":
            w = 0.0
        if w > 0.0:
            weights.append((str(sym or ""), float(w)))
    weights.sort(key=lambda t: t[1], reverse=True)
    gross = float(sum(w for _sym, w in weights))
    top1 = float(weights[0][1]) if weights else 0.0
    top3 = float(sum(w for _sym, w in weights[:3]))
    return {
        "gross": float(gross),
        "top1_weight": float(top1),
        "top1_symbol": (weights[0][0] if weights else None),
        "top3_weight": float(top3),
        "positions": int(len(weights)),
    }


def _position_rows(con) -> List[Dict[str, Any]]:
    if not _table_exists(con, "portfolio_state"):
        return []

    out: List[Dict[str, Any]] = []
    try:
        rows = con.execute(
            """
            SELECT
              symbol,
              MAX(CASE
                    WHEN UPPER(COALESCE(side, '')) IN ('LONG','BUY') THEN 'LONG'
                    WHEN UPPER(COALESCE(side, '')) IN ('SHORT','SELL') THEN 'SHORT'
                    ELSE 'FLAT'
                  END) AS side,
              SUM(CASE
                    WHEN UPPER(COALESCE(side, '')) IN ('LONG','BUY') THEN ABS(COALESCE(weight, 0.0))
                    WHEN UPPER(COALESCE(side, '')) IN ('SHORT','SELL') THEN ABS(COALESCE(weight, 0.0))
                    ELSE 0.0
                  END) AS weight,
              MAX(opened_ts_ms) AS opened_ts_ms,
              MAX(updated_ts_ms) AS updated_ts_ms,
              MAX(source_alert_id) AS source_alert_id,
              '{}' AS explain_json
            FROM portfolio_state
            GROUP BY symbol
            ORDER BY symbol
            """
        ).fetchall()
    except Exception as e:
        _warn_nonfatal(
            "KILL_SWITCH_POSITION_ROWS_QUERY_FAILED",
            e,
            once_key="position_rows_query",
        )
        positions: List[Dict[str, Any]] = []
        return positions

    for row in rows or []:
        out.append(
            {
                "symbol": _s(row[0]),
                "side": _s(row[1]),
                "weight": float(_safe_float(row[2], 0.0)),
                "opened_ts_ms": int(row[3] or 0),
                "updated_ts_ms": int(row[4] or 0),
                "source_alert_id": row[5],
                "explain_json": _s(row[6]),
            }
        )
    return out


def _portfolio_snapshot(con, meta: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    now_ms = _now_ms()
    eq = 0.0
    if _table_exists(con, "equity_history"):
        try:
            row = con.execute("SELECT equity FROM equity_history ORDER BY ts_ms DESC LIMIT 1").fetchone()
            if row:
                eq = float(_safe_float(row[0], 0.0))
        except Exception as e:
            _warn_nonfatal("KILL_SWITCH_PORTFOLIO_SNAPSHOT_EQUITY_READ_FAILED", e, once_key="portfolio_snapshot_equity")

    positions = _position_rows(con)
    gross = float(sum(abs(float(_safe_float((p or {}).get("weight", 0.0), 0.0))) for p in positions))
    net = float(sum(float(_safe_float((p or {}).get("weight", 0.0), 0.0)) for p in positions))
    per_symbol_weights = {str((p or {}).get("symbol") or ""): float(_safe_float((p or {}).get("weight", 0.0), 0.0)) for p in positions if str((p or {}).get("symbol") or "")}

    rolling_series = _equity_series(
        con,
        lookback_ms=max(0, int(KILL_SWITCH_ROLLING_DRAWDOWN_LOOKBACK_DAYS)) * 86400000,
    )
    rolling_drawdown = _drawdown_pct(rolling_series) if len(rolling_series) >= 2 else 0.0

    var_series = _equity_series(con, limit=KILL_SWITCH_VAR_LOOKBACK_POINTS)
    returns = _returns_from_series(var_series)
    var_pct = 0.0
    if len(returns) >= int(KILL_SWITCH_VAR_MIN_HISTORY):
        losses = sorted([-float(r) for r in returns])
        var_pct = max(0.0, _percentile(losses, float(KILL_SWITCH_VAR_CONFIDENCE)))

    return {
        "ts_ms": int(now_ms),
        "equity": float(eq),
        "gross_exposure": float(gross),
        "net_exposure": float(net),
        "per_symbol_weights": per_symbol_weights,
        "positions": positions,
        "rolling_drawdown": float(rolling_drawdown),
        "var_pct": float(var_pct),
        "metadata": dict(meta or {}),
    }


def _persist_portfolio_kill_snapshot(con, snapshot: Dict[str, Any]) -> None:
    try:
        if not _table_exists(con, "portfolio_kill_snapshots"):
            return
        con.execute(
            """
            INSERT OR REPLACE INTO portfolio_kill_snapshots (
              ts_ms, equity, gross_exposure, net_exposure, per_symbol_weights_json, positions_json,
              rolling_drawdown, var_pct, metadata_json
            ) VALUES (?,?,?,?,?,?,?,?,?)
            """,
            (
                int(snapshot.get("ts_ms") or _now_ms()),
                float(_safe_float(snapshot.get("equity", 0.0), 0.0)),
                float(_safe_float(snapshot.get("gross_exposure", 0.0), 0.0)),
                float(_safe_float(snapshot.get("net_exposure", 0.0), 0.0)),
                json.dumps(snapshot.get("per_symbol_weights") or {}, separators=(",", ":"), sort_keys=True),
                json.dumps(snapshot.get("positions") or [], separators=(",", ":"), sort_keys=True),
                float(_safe_float(snapshot.get("rolling_drawdown", 0.0), 0.0)),
                float(_safe_float(snapshot.get("var_pct", 0.0), 0.0)),
                json.dumps(snapshot.get("metadata") or {}, separators=(",", ":"), sort_keys=True),
            ),
        )
    except Exception as e:
        _warn_nonfatal("KILL_SWITCH_PERSIST_PORTFOLIO_SNAPSHOT_FAILED", e, once_key="persist_portfolio_snapshot")


def _persist_risk_event(con, trigger_type: str, reason: str, snapshot: Dict[str, Any], meta: Optional[Dict[str, Any]] = None) -> None:
    try:
        if not _table_exists(con, "risk_events"):
            return
        meta_payload = dict(meta or {})
        meta_payload.setdefault("snapshot_ts_ms", int(snapshot.get("ts_ms") or _now_ms()))
        con.execute(
            """
            INSERT INTO risk_events (
              ts_ms, trigger_type, reason, equity, drawdown_pct, var_pct, concentration, positions, metadata_json
            ) VALUES (?,?,?,?,?,?,?,?,?)
            """,
            (
                int(snapshot.get("ts_ms") or _now_ms()),
                _s(trigger_type),
                _s(reason),
                float(_safe_float(snapshot.get("equity", 0.0), 0.0)),
                float(_safe_float(snapshot.get("rolling_drawdown", 0.0), 0.0)),
                float(_safe_float(snapshot.get("var_pct", 0.0), 0.0)),
                float(_safe_float((meta or {}).get("concentration", 0.0), 0.0)),
                int(len(snapshot.get("positions") or [])),
                json.dumps(meta_payload, separators=(",", ":"), sort_keys=True),
            ),
        )
    except Exception as e:
        _warn_nonfatal(
            "KILL_SWITCH_PERSIST_RISK_EVENT_FAILED",
            e,
            once_key=f"persist_risk_event:{_s(trigger_type)}",
            trigger_type=str(trigger_type),
        )


def _capital_risk_trigger(con) -> Optional[Dict[str, Any]]:
    if not CAPITAL_AWARE_KILL_SWITCH:
        return None

    now_ms = _now_ms()

    day_series = _equity_series(con, lookback_ms=86400000)
    if len(day_series) >= 2:
        day_dd = _drawdown_pct(day_series)
        if float(KILL_SWITCH_DAILY_DRAWDOWN_PCT) > 0.0 and day_dd >= float(KILL_SWITCH_DAILY_DRAWDOWN_PCT):
            return {
                "reason": f"capital_daily_drawdown_breach pct={day_dd:.4f}",
                "meta": {
                    "trigger": "daily_drawdown",
                    "daily_drawdown_pct": float(day_dd),
                    "threshold": float(KILL_SWITCH_DAILY_DRAWDOWN_PCT),
                    "points": int(len(day_series)),
                    "ts_ms": int(now_ms),
                },
            }

    rolling_series = _equity_series(
        con,
        lookback_ms=max(0, int(KILL_SWITCH_ROLLING_DRAWDOWN_LOOKBACK_DAYS)) * 86400000,
    )
    if len(rolling_series) >= 2:
        rolling_dd = _drawdown_pct(rolling_series)
        if float(KILL_SWITCH_ROLLING_DRAWDOWN_PCT) > 0.0 and rolling_dd >= float(KILL_SWITCH_ROLLING_DRAWDOWN_PCT):
            return {
                "reason": f"capital_rolling_drawdown_breach pct={rolling_dd:.4f}",
                "meta": {
                    "trigger": "rolling_drawdown",
                    "rolling_drawdown_pct": float(rolling_dd),
                    "threshold": float(KILL_SWITCH_ROLLING_DRAWDOWN_PCT),
                    "lookback_days": int(KILL_SWITCH_ROLLING_DRAWDOWN_LOOKBACK_DAYS),
                    "points": int(len(rolling_series)),
                    "ts_ms": int(now_ms),
                },
            }

    var_series = _equity_series(con, limit=KILL_SWITCH_VAR_LOOKBACK_POINTS)
    returns = _returns_from_series(var_series)
    if len(returns) >= int(KILL_SWITCH_VAR_MIN_HISTORY):
        losses = sorted([-float(r) for r in returns])
        var_pct = max(0.0, _percentile(losses, float(KILL_SWITCH_VAR_CONFIDENCE)))
        latest_ret = float(returns[-1])
        latest_loss = max(0.0, -latest_ret)
        if var_pct > 0.0 and latest_loss > var_pct:
            return {
                "reason": f"capital_var_breach loss={latest_loss:.4f} var={var_pct:.4f}",
                "meta": {
                    "trigger": "var_breach",
                    "latest_loss_pct": float(latest_loss),
                    "var_pct": float(var_pct),
                    "confidence": float(KILL_SWITCH_VAR_CONFIDENCE),
                    "lookback_points": int(len(var_series)),
                    "returns_n": int(len(returns)),
                    "ts_ms": int(now_ms),
                },
            }

    conc = _portfolio_concentration(con)
    if float(KILL_SWITCH_CONCENTRATION_MAX_SINGLE) > 0.0 and float(conc.get("top1_weight", 0.0)) >= float(KILL_SWITCH_CONCENTRATION_MAX_SINGLE):
        return {
            "reason": f"capital_concentration_single_breach weight={float(conc.get('top1_weight', 0.0)):.4f}",
            "meta": {
                "trigger": "concentration_single",
                "symbol": conc.get("top1_symbol"),
                "weight": float(conc.get("top1_weight", 0.0)),
                "threshold": float(KILL_SWITCH_CONCENTRATION_MAX_SINGLE),
                "gross": float(conc.get("gross", 0.0)),
                "positions": int(conc.get("positions", 0)),
                "ts_ms": int(now_ms),
            },
        }

    if float(KILL_SWITCH_CONCENTRATION_MAX_TOP3) > 0.0 and float(conc.get("top3_weight", 0.0)) >= float(KILL_SWITCH_CONCENTRATION_MAX_TOP3):
        return {
            "reason": f"capital_concentration_top3_breach weight={float(conc.get('top3_weight', 0.0)):.4f}",
            "meta": {
                "trigger": "concentration_top3",
                "top3_weight": float(conc.get("top3_weight", 0.0)),
                "threshold": float(KILL_SWITCH_CONCENTRATION_MAX_TOP3),
                "gross": float(conc.get("gross", 0.0)),
                "positions": int(conc.get("positions", 0)),
                "ts_ms": int(now_ms),
            },
        }

    return None


def _model_risk_trigger(con, model_id: Optional[str]) -> Optional[Dict[str, Any]]:
    if not MODEL_AWARE_KILL_SWITCH:
        return None

    mid = _norm_model_id(model_id)
    if not mid:
        return None

    if float(KILL_SWITCH_MODEL_MAX_DRAWDOWN) <= 0.0 and int(KILL_SWITCH_MODEL_MAX_CONSECUTIVE_LOSSES) <= 0:
        return None

    rows = _model_pnl_rows(con, mid, limit=KILL_SWITCH_MODEL_LOOKBACK_ROWS)
    if not rows:
        return None

    now_ms = _now_ms()
    drawdown = _model_drawdown_amount(rows)
    consecutive_losses = _model_consecutive_losses(rows)
    latest_ts_ms = int((rows[-1][0] if rows else 0) or 0)

    if float(KILL_SWITCH_MODEL_MAX_DRAWDOWN) > 0.0 and drawdown >= float(KILL_SWITCH_MODEL_MAX_DRAWDOWN):
        return {
            "reason": f"model_drawdown_breach model_id={mid} drawdown={drawdown:.4f}",
            "meta": {
                "trigger": "model_drawdown",
                "model_id": str(mid),
                "drawdown": float(drawdown),
                "threshold": float(KILL_SWITCH_MODEL_MAX_DRAWDOWN),
                "rows": int(len(rows)),
                "lookback_rows": int(KILL_SWITCH_MODEL_LOOKBACK_ROWS),
                "latest_ts_ms": int(latest_ts_ms),
                "ts_ms": int(now_ms),
            },
        }

    if int(KILL_SWITCH_MODEL_MAX_CONSECUTIVE_LOSSES) > 0 and consecutive_losses >= int(KILL_SWITCH_MODEL_MAX_CONSECUTIVE_LOSSES):
        return {
            "reason": f"model_consecutive_losses_breach model_id={mid} losses={consecutive_losses}",
            "meta": {
                "trigger": "model_consecutive_losses",
                "model_id": str(mid),
                "consecutive_losses": int(consecutive_losses),
                "threshold": int(KILL_SWITCH_MODEL_MAX_CONSECUTIVE_LOSSES),
                "rows": int(len(rows)),
                "lookback_rows": int(KILL_SWITCH_MODEL_LOOKBACK_ROWS),
                "latest_ts_ms": int(latest_ts_ms),
                "ts_ms": int(now_ms),
            },
        }

    return None

def execution_allowed(
    con=None,
    symbol: Optional[str] = None,
    regime: Optional[str] = None,
    model_id: Optional[str] = None,
) -> Tuple[bool, str, Dict[str, Any]]:
    """Evaluate whether execution is currently permitted.

    Parameters
    ----------
    con : storage connection, optional
        Existing database connection used for freshness checks and persisted
        switch reads. A temporary connection is opened when omitted.
    symbol : str, optional
        Symbol-specific scope checked after global gates.
    regime : str, optional
        Regime-specific scope checked after global gates.
    model_id : str, optional
        Model-specific scope checked after global gates and model-risk rules.

    Returns
    -------
    tuple[bool, str, dict]
        Three-tuple ``(allowed, reason, meta)``. ``reason`` is a stable
        machine-readable block code. ``meta`` always includes ``scope`` and
        ``key`` and may include richer trigger details for automatic risk
        activations.

    Notes
    -----
    Checks are ordered to fail closed: lifecycle state, environment overrides,
    capital guard, data freshness, job heartbeat freshness, automatic
    capital/model risk triggers, and finally persisted database switches. Data
    freshness thresholds are measured in seconds via ``MAX_PRICE_STALE_S``,
    ``MAX_EVENT_STALE_S``, ``MAX_PRED_STALE_S``, and ``MAX_JOB_STALE_S``.

    Side Effects
    ------------
    Automatic capital or model breaches can persist risk snapshots/events and
    activate new kill switches with cooldown metadata before returning
    ``allowed=False``.
    """
    sym = _s(symbol)
    reg = _s(regime)
    mid = _norm_model_id(model_id) if model_id is not None else ""

    # ------------------------------------------------------------------
    # Runtime lifecycle guard (STRICT runtime state enforcement)
    # Execution only allowed in LIVE state
    # ------------------------------------------------------------------
    if callable(_get_lifecycle_state):
        try:
            lifecycle = _get_lifecycle_state() or {}
        except Exception:
            lifecycle = {}

        lifecycle_state = str(lifecycle.get("state") or "").strip().upper()

        if lifecycle_state == "WARMING":
            lifecycle_state = "WARMING_UP"

        if lifecycle_state == "SHUTTING_DOWN":
            lifecycle_state = "SHUTDOWN"

        if lifecycle_state == "KILL_SWITCH":
            try:
                st = _read_state_hot(con, "global", "global")
                st = _maybe_auto_expire(con, "global", "global", st)
                if st and int(st[0]) == 1:
                    return False, "kill_switch_db_global", {
                        "scope": "global",
                        "key": "global",
                        "reason": st[1],
                        "actor": st[2],
                    }
            except Exception as e:
                _warn_nonfatal(
                    "KILL_SWITCH_LIFECYCLE_DB_REASON_LOOKUP_FAILED",
                    e,
                    once_key="lifecycle_db_reason_lookup",
                )
            return False, "kill_switch_lifecycle", {
                "scope": "global",
                "key": lifecycle_state or "UNKNOWN"
            }

        if lifecycle_state != "LIVE":
            return False, "runtime_state_block", {
                "scope": "global",
                "key": lifecycle_state or "UNKNOWN"
            }

    # ENV overrides (fail-closed)
    if _env_global_enabled():
        return False, "kill_switch_env_global", {"scope": "global", "key": "global"}

    if sym and _env_symbol_enabled(sym):
        return False, "kill_switch_env_symbol", {"scope": "symbol", "key": sym}

    if reg and _env_regime_enabled(reg):
        return False, "kill_switch_env_regime", {"scope": "regime", "key": reg}

    if mid and _env_model_enabled(mid):
        return False, "kill_switch_env_model", {"scope": "model", "key": mid}

    owns = False
    if con is None:
        con = connect()
        owns = True

    try:
        try:
            from engine.runtime.storage import init_db
            init_db()
        except Exception as e:
            _warn_nonfatal("KILL_SWITCH_INIT_DB_FAILED", e, once_key="init_db")
        _ensure_schema(con)

        # Capital guard
        try:
            from engine.strategy.capital_guard import trading_allowed as _capital_trading_allowed
            if not _capital_trading_allowed(con=con):
                return False, "capital_guard_block", {"scope": "global", "key": "global"}
        except Exception as e:
            _warn_nonfatal(
                "KILL_SWITCH_CAPITAL_GUARD_CHECK_FAILED",
                e,
                once_key="capital_guard_check",
            )
            guard_error = (False, "capital_guard_error", {"scope": "global", "key": "global"})
            return guard_error

        # Data freshness
        if REQUIRE_FRESH_DATA:
            now = _now_ms()

            p_ts = _latest_ts_ms(con, "prices")
            if p_ts <= 0 or (now - p_ts) > (MAX_PRICE_STALE_S * 1000):
                return False, "stale_prices", {"scope": "global", "key": "global"}

            e_ts = _latest_ts_ms(con, "events")
            if e_ts <= 0 or (now - e_ts) > (MAX_EVENT_STALE_S * 1000):
                return False, "stale_events", {"scope": "global", "key": "global"}

            pr_ts = _latest_ts_ms(con, "predictions")
            if pr_ts <= 0 or (now - pr_ts) > (MAX_PRED_STALE_S * 1000):
                return False, "stale_predictions", {"scope": "global", "key": "global"}

        # Job freshness
        if REQUIRE_FRESH_JOBS:
            now = _now_ms()
            for j in _required_jobs():
                hb = _job_heartbeat_ts_ms(con, j)
                if hb <= 0 or (now - hb) > (MAX_JOB_STALE_S * 1000):
                    return False, "stale_job_heartbeat", {"scope": "global", "key": j}

        # Capital-aware kill switch
        breach = _capital_risk_trigger(con)
        if breach:
            reason = _s((breach or {}).get("reason")) or "capital_risk_breach"
            meta = dict((breach or {}).get("meta") or {})
            now_ms = _now_ms()
            cooldown_ms = int(max(0.0, float(KILL_SWITCH_COOLDOWN_MINUTES)) * 60_000.0)
            if cooldown_ms > 0:
                meta.setdefault("cooldown_minutes", float(KILL_SWITCH_COOLDOWN_MINUTES))
                meta.setdefault("until_ts_ms", int(now_ms + cooldown_ms))
            meta.setdefault("trigger_type", _s(meta.get("trigger") or "capital_risk"))

            snapshot_payload = _portfolio_snapshot(con, meta=meta)
            meta.setdefault("equity", float(snapshot_payload.get("equity", 0.0)))
            meta.setdefault("positions", int(len(snapshot_payload.get("positions") or [])))
            meta.setdefault("concentration", float(_safe_float((meta or {}).get("weight", meta.get("top3_weight", 0.0)), 0.0)))

            try:
                _persist_portfolio_kill_snapshot(con, snapshot_payload)
            except Exception as e:
                _warn_nonfatal("KILL_SWITCH_PERSIST_PORTFOLIO_KILL_SNAPSHOT_FAILED", e, once_key="persist_portfolio_kill_snapshot")
            try:
                _persist_risk_event(
                    con,
                    trigger_type=_s(meta.get("trigger_type") or meta.get("trigger") or "capital_risk"),
                    reason=reason,
                    snapshot=snapshot_payload,
                    meta=meta,
                )
            except Exception as e:
                _warn_nonfatal("KILL_SWITCH_PERSIST_CAPITAL_RISK_EVENT_FAILED", e, once_key="persist_capital_risk_event")
            try:
                activate(
                    "global",
                    "global",
                    reason=reason,
                    actor="risk_engine",
                    meta=meta,
                    action="AUTO",
                    con=con,
                )
            except Exception as e:
                _warn_nonfatal("KILL_SWITCH_ACTIVATE_CAPITAL_BREACH_FAILED", e, once_key="activate_capital_breach")
            return False, "capital_aware_kill_switch", {"scope": "global", "key": "global", "reason": reason, "meta": meta}

        model_breach = _model_risk_trigger(con, mid) if mid else None
        if model_breach:
            reason = _s((model_breach or {}).get("reason")) or "model_risk_breach"
            meta = dict((model_breach or {}).get("meta") or {})
            now_ms = _now_ms()
            cooldown_ms = int(max(0.0, float(KILL_SWITCH_COOLDOWN_MINUTES)) * 60_000.0)
            if cooldown_ms > 0:
                meta.setdefault("cooldown_minutes", float(KILL_SWITCH_COOLDOWN_MINUTES))
                meta.setdefault("until_ts_ms", int(now_ms + cooldown_ms))
            meta.setdefault("trigger_type", _s(meta.get("trigger") or "model_risk"))
            try:
                activate(
                    "model",
                    mid,
                    reason=reason,
                    actor="risk_engine",
                    meta=meta,
                    action="AUTO",
                    con=con,
                )
            except Exception as e:
                _warn_nonfatal(
                    "KILL_SWITCH_ACTIVATE_MODEL_BREACH_FAILED",
                    e,
                    once_key=f"activate_model_breach:{mid}",
                    model_id=str(mid),
                )
            return False, "model_aware_kill_switch", {"scope": "model", "key": mid, "reason": reason, "meta": meta}

        # DB switches
        st = _read_state_hot(con, "global", "global")
        st = _maybe_auto_expire(con, "global", "global", st)
        if st and int(st[0]) == 1:
            return False, "kill_switch_db_global", {"scope": "global", "key": "global", "reason": st[1], "actor": st[2]}

        if mid:
            st = _read_state_hot(con, "model", mid)
            st = _maybe_auto_expire(con, "model", mid, st)
            if st and int(st[0]) == 1:
                return False, "kill_switch_db_model", {"scope": "model", "key": mid, "reason": st[1], "actor": st[2]}

        if reg:
            st = _read_state_hot(con, "regime", reg)
            st = _maybe_auto_expire(con, "regime", reg, st)
            if st and int(st[0]) == 1:
                return False, "kill_switch_db_regime", {"scope": "regime", "key": reg, "reason": st[1], "actor": st[2]}

        if sym:
            st = _read_state_hot(con, "symbol", sym)
            st = _maybe_auto_expire(con, "symbol", sym, st)
            if st and int(st[0]) == 1:
                return False, "kill_switch_db_symbol", {"scope": "symbol", "key": sym, "reason": st[1], "actor": st[2]}

        return True, "ok", {"scope": None, "key": None}
    finally:
        if owns:
            con.close()

def snapshot(con=None) -> Dict[str, Any]:
    """Return the persisted kill-switch table as a normalized snapshot.

    Parameters
    ----------
    con : storage connection, optional
        Existing database connection. A temporary connection is opened when
        omitted.

    Returns
    -------
    dict
        Mapping with a single ``state`` list. Each row contains ``scope``,
        ``key``, ``enabled``, ``reason``, ``actor``, ``meta``,
        ``created_ts_ms``, and ``updated_ts_ms``.

    Notes
    -----
    This snapshot only reflects database-backed switch rows. Environment-only
    overrides and implicit freshness gates are not materialized here.
    """
    if con is None:
        try:
            from engine.cache.wrappers.kill_switch import read_kill_switch

            return read_kill_switch()
        except Exception as e:
            _warn_nonfatal(
                "KILL_SWITCH_REDIS_CACHE_SNAPSHOT_FAILED",
                e,
                once_key="snapshot_redis_read",
            )
    owns = False
    if con is None:
        con = connect()
        owns = True
    try:
        if not _schema_ready_for_reads(con):
            _ensure_schema(con)
        rows = con.execute(
            """
            SELECT scope, key, enabled, reason, actor, meta_json, created_ts_ms, updated_ts_ms
            FROM kill_switch_state
            ORDER BY scope, key
            """
        ).fetchall()
        out = []
        for r in rows or []:
            out.append(
                {
                    "scope": _s(r[0]),
                    "key": _s(r[1]),
                    "enabled": int(r[2] or 0),
                    "reason": _s(r[3]),
                    "actor": _s(r[4]),
                    "meta": json.loads(r[5] or "{}") if r[5] else {},
                    "created_ts_ms": int(r[6] or 0),
                    "updated_ts_ms": int(r[7] or 0),
                }
            )
        return {"state": out}
    finally:
        if owns:
            con.close()
