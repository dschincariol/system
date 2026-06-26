"""Persistent and fail-closed execution kill-switch controls.

This module combines lifecycle gating, environment overrides, database-backed
switches, freshness checks, and automatic capital/model risk triggers into the
single execution barrier used by live-order paths.
"""

import json
import logging
import math
import os
from pathlib import Path
import threading
import time
from typing import Any, Dict, Optional, Tuple, List

from engine.audit.chain import append_chain_row
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.live_execution_control import (
    DISABLE_LIVE_EXECUTION_ENV,
    DISABLE_LIVE_EXECUTION_REASON,
    live_execution_disabled,
)
from engine.runtime.storage import DB_PATH, connect, _table_exists, run_write_txn
from engine.runtime.event_log import append_event

LOGGER = logging.getLogger(__name__)
_WARNED_NONFATAL_KEYS: set[str] = set()
_KILL_SWITCH_SCHEMA_READY_LOCK = threading.Lock()
_KILL_SWITCH_SCHEMA_READY_PATH = ""
_ACTIVATION_FAILURE_STATE_FILE = "kill_switch_activation_failure_state.json"
_ACTIVATION_FAILURE_EVIDENCE_FILE = "kill_switch_activation_failures.jsonl"


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


def _activation_failure_dir() -> Path:
    raw_dir = str(os.environ.get("KILL_SWITCH_FAILURE_DIR") or "").strip()
    if raw_dir:
        return Path(raw_dir).expanduser()
    data_dir = str(os.environ.get("TRADING_DATA") or os.environ.get("DATA_DIR") or "").strip()
    if data_dir:
        return Path(data_dir).expanduser() / "runtime_evidence"
    try:
        return Path(DB_PATH).expanduser().resolve().parent / "runtime_evidence"
    except Exception as e:
        _warn_nonfatal(
            "KILL_SWITCH_ACTIVATION_FAILURE_DIR_RESOLVE_FAILED",
            e,
            once_key="activation_failure_dir_resolve",
            db_path=str(DB_PATH),
        )
        return Path(".").resolve() / "runtime_evidence"


def _activation_failure_state_path() -> Path:
    return _activation_failure_dir() / _ACTIVATION_FAILURE_STATE_FILE


def _activation_failure_evidence_path() -> Path:
    return _activation_failure_dir() / _ACTIVATION_FAILURE_EVIDENCE_FILE


def _fsync_parent(path: Path) -> None:
    try:
        fd = os.open(str(path.parent), os.O_RDONLY)
    except Exception as e:
        _warn_nonfatal(
            "KILL_SWITCH_ACTIVATION_FAILURE_PARENT_OPEN_FAILED",
            e,
            once_key=f"activation_failure_parent_open:{path.parent}",
            path=str(path),
        )
        return
    try:
        os.fsync(fd)
    except Exception as e:
        _warn_nonfatal(
            "KILL_SWITCH_ACTIVATION_FAILURE_PARENT_FSYNC_FAILED",
            e,
            once_key=f"activation_failure_parent_fsync:{path.parent}",
            path=str(path),
        )
    finally:
        try:
            os.close(fd)
        except Exception as e:
            _warn_nonfatal(
                "KILL_SWITCH_ACTIVATION_FAILURE_PARENT_CLOSE_FAILED",
                e,
                once_key=f"activation_failure_parent_close:{path.parent}",
                path=str(path),
            )


def _write_json_atomic(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.{_now_ms()}.tmp")
    with tmp_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, separators=(",", ":"), sort_keys=True, default=str)
        fh.write("\n")
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(str(tmp_path), str(path))
    _fsync_parent(path)


def _append_jsonl(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, separators=(",", ":"), sort_keys=True, default=str))
        fh.write("\n")
        fh.flush()
        os.fsync(fh.fileno())


def activation_failure_snapshot() -> Dict[str, Any]:
    """Return unresolved emergency activation failure evidence, if present."""
    path = _activation_failure_state_path()
    try:
        if not path.exists():
            return {"active": False}
        with path.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
    except Exception as e:
        _warn_nonfatal(
            "KILL_SWITCH_ACTIVATION_FAILURE_SNAPSHOT_READ_FAILED",
            e,
            once_key=f"activation_failure_snapshot_read:{path}",
            path=str(path),
        )
        return {
            "active": True,
            "reason": "kill_switch_activation_failure_snapshot_unreadable",
            "error_type": type(e).__name__,
            "error": str(e),
            "path": str(path),
        }
    if not isinstance(payload, dict):
        return {"active": True, "reason": "kill_switch_activation_failure_snapshot_invalid", "path": str(path)}
    payload.setdefault("active", True)
    payload.setdefault("path", str(path))
    return payload


def _record_activation_failure(
    *,
    scope: str,
    key: str,
    reason: str,
    actor: str,
    meta: Dict[str, Any],
    action: str,
    trigger_kind: str,
    error: BaseException,
) -> Dict[str, Any]:
    ts_ms = _now_ms()
    payload: Dict[str, Any] = {
        "active": True,
        "status": "UNRESOLVED",
        "ts_ms": int(ts_ms),
        "scope": str(scope),
        "key": str(key),
        "reason": str(reason),
        "actor": str(actor or "risk_engine"),
        "action": str(action or "AUTO").upper(),
        "trigger_kind": str(trigger_kind),
        "reason_code": "kill_switch_activation_write_failed",
        "error_type": type(error).__name__,
        "error": str(error),
        "meta": dict(meta or {}),
        "db_path": str(DB_PATH),
        "evidence_path": str(_activation_failure_evidence_path()),
        "state_path": str(_activation_failure_state_path()),
    }
    marker_error: BaseException | None = None
    try:
        _write_json_atomic(_activation_failure_state_path(), payload)
        _append_jsonl(_activation_failure_evidence_path(), payload)
    except Exception as e:
        marker_error = e
        payload["marker_write_failed"] = True
        payload["marker_error_type"] = type(e).__name__
        payload["marker_error"] = str(e)

    log_failure(
        LOGGER,
        event="kill_switch_activation_failed",
        code="KILL_SWITCH_ACTIVATION_WRITE_FAILED",
        message="Automatic breach kill-switch activation could not be durably written.",
        error=error,
        level=logging.ERROR,
        component="engine.execution.kill_switch",
        extra=payload,
        include_health=False,
        persist=True,
        flush=True,
    )

    if marker_error is not None:
        log_failure(
            LOGGER,
            event="kill_switch_activation_failure_marker_failed",
            code="KILL_SWITCH_ACTIVATION_FAILURE_MARKER_FAILED",
            message="Kill-switch activation failure marker could not be written.",
            error=marker_error,
            level=logging.CRITICAL,
            component="engine.execution.kill_switch",
            extra=payload,
            include_health=False,
            persist=False,
            flush=True,
        )

    try:
        append_event(
            event_type="kill_switch_activation_failed",
            event_source="engine.execution.kill_switch",
            event_version=1,
            entity_type="kill_switch",
            entity_id=f"{scope}:{key}",
            correlation_id=f"{scope}:{key}:{trigger_kind}",
            payload=payload,
            ts_ms=int(ts_ms),
            best_effort=False,
        )
        try:
            from engine.runtime.event_log import flush_event_log_buffer

            flush_event_log_buffer(max_batches=8, wait_inflight_s=1.0)
        except Exception as e:
            _warn_nonfatal(
                "KILL_SWITCH_ACTIVATION_FAILURE_EVENT_FLUSH_FAILED",
                e,
                once_key="activation_failure_event_flush",
            )
    except Exception as e:
        _warn_nonfatal(
            "KILL_SWITCH_ACTIVATION_FAILURE_EVENT_WRITE_FAILED",
            e,
            once_key="activation_failure_event_write",
        )

    try:
        from engine.runtime.risk_state import set_state as _risk_set_state

        _risk_set_state(
            "kill_switch_activation_failure",
            json.dumps(payload, separators=(",", ":"), sort_keys=True, default=str),
        )
    except Exception as e:
        _warn_nonfatal(
            "KILL_SWITCH_ACTIVATION_FAILURE_RISK_STATE_WRITE_FAILED",
            e,
            once_key="activation_failure_risk_state_write",
        )

    try:
        from engine.runtime.lifecycle_state import DEGRADED as _DEGRADED, set_state as _lifecycle_set_state

        _lifecycle_set_state(_DEGRADED, "kill_switch_activation_write_failed")
    except Exception as e:
        _warn_nonfatal(
            "KILL_SWITCH_ACTIVATION_FAILURE_LIFECYCLE_DEGRADE_FAILED",
            e,
            once_key="activation_failure_lifecycle_degrade",
        )

    return payload


def _clear_activation_failure_marker(scope: str, key: str) -> None:
    path = _activation_failure_state_path()
    payload = activation_failure_snapshot()
    if not bool(payload.get("active")):
        return
    payload_scope = _s(payload.get("scope"))
    payload_key = _s(payload.get("key"))
    if payload_scope and payload_key and (payload_scope != _s(scope) or payload_key != _s(key)):
        return
    resolved = dict(payload)
    resolved.update(
        {
            "active": False,
            "status": "RESOLVED",
            "resolved_ts_ms": int(_now_ms()),
            "resolved_by": "kill_switch_activation_committed",
        }
    )
    try:
        _append_jsonl(_activation_failure_evidence_path(), resolved)
        if path.exists():
            path.unlink()
        _fsync_parent(path)
    except Exception as e:
        _warn_nonfatal(
            "KILL_SWITCH_ACTIVATION_FAILURE_MARKER_CLEAR_FAILED",
            e,
            once_key=f"activation_failure_marker_clear:{scope}:{key}",
            scope=str(scope),
            key=str(key),
        )


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


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or str(raw).strip() == "":
        return int(default)
    return int(str(raw).strip())


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or str(raw).strip() == "":
        return float(default)
    return float(str(raw).strip())


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

MAX_PRICE_STALE_S = _env_int("KILL_SWITCH_MAX_PRICE_STALE_S", 300)          # 5m
MAX_EVENT_STALE_S = _env_int("KILL_SWITCH_MAX_EVENT_STALE_S", 3600)         # 1h
MAX_PRED_STALE_S  = _env_int("KILL_SWITCH_MAX_PRED_STALE_S", 900)           # 15m

# Heartbeat freshness (job_heartbeats.ts_ms)
MAX_JOB_STALE_S   = _env_int("KILL_SWITCH_MAX_JOB_STALE_S", 600)            # 10m
REQUIRED_JOBS_CSV = os.environ.get("KILL_SWITCH_REQUIRED_JOBS", "ingestion_runtime,process_events").strip()

# Capital-aware kill switch (additive, non-breaking)
CAPITAL_AWARE_KILL_SWITCH = os.environ.get("CAPITAL_AWARE_KILL_SWITCH", "1") == "1"
KILL_SWITCH_DAILY_DRAWDOWN_PCT = _env_float("KILL_SWITCH_DAILY_DRAWDOWN_PCT", 0.05)
KILL_SWITCH_ROLLING_DRAWDOWN_PCT = _env_float("KILL_SWITCH_ROLLING_DRAWDOWN_PCT", 0.12)
KILL_SWITCH_ROLLING_DRAWDOWN_LOOKBACK_DAYS = _env_int("KILL_SWITCH_ROLLING_DRAWDOWN_LOOKBACK_DAYS", 7)
KILL_SWITCH_VAR_LOOKBACK_POINTS = _env_int("KILL_SWITCH_VAR_LOOKBACK_POINTS", 250)
KILL_SWITCH_VAR_CONFIDENCE = _env_float("KILL_SWITCH_VAR_CONFIDENCE", 0.99)
KILL_SWITCH_VAR_MIN_HISTORY = _env_int("KILL_SWITCH_VAR_MIN_HISTORY", 30)
KILL_SWITCH_MAX_EQUITY_AGE_S = _env_int("KILL_SWITCH_MAX_EQUITY_AGE_S", 300)
KILL_SWITCH_DAILY_EQUITY_MIN_POINTS = _env_int("KILL_SWITCH_DAILY_EQUITY_MIN_POINTS", 2)
KILL_SWITCH_ROLLING_EQUITY_MIN_POINTS = _env_int("KILL_SWITCH_ROLLING_EQUITY_MIN_POINTS", 5)
KILL_SWITCH_VAR_EQUITY_MIN_POINTS = _env_int(
    "KILL_SWITCH_VAR_EQUITY_MIN_POINTS",
    max(2, int(KILL_SWITCH_VAR_MIN_HISTORY) + 1),
)
KILL_SWITCH_CONCENTRATION_MAX_SINGLE = _env_float("KILL_SWITCH_CONCENTRATION_MAX_SINGLE", 0.35)
KILL_SWITCH_CONCENTRATION_MAX_TOP3 = _env_float("KILL_SWITCH_CONCENTRATION_MAX_TOP3", 0.70)
KILL_SWITCH_COOLDOWN_MINUTES = _env_float("KILL_SWITCH_COOLDOWN_MINUTES", 60.0)
MODEL_AWARE_KILL_SWITCH = os.environ.get("MODEL_AWARE_KILL_SWITCH", "1") == "1"
KILL_SWITCH_MODEL_MAX_DRAWDOWN = _env_float("KILL_SWITCH_MODEL_MAX_DRAWDOWN", 0.0)
KILL_SWITCH_MODEL_MAX_CONSECUTIVE_LOSSES = _env_int("KILL_SWITCH_MODEL_MAX_CONSECUTIVE_LOSSES", 0)
KILL_SWITCH_MODEL_LOOKBACK_ROWS = _env_int("KILL_SWITCH_MODEL_LOOKBACK_ROWS", 250)

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

    if _protected_manual_halt(st):
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

def _env_live_mode_requested() -> bool:
    for name in ("EXECUTION_MODE", "ENGINE_MODE", "OPERATOR_MODE", "MODE"):
        if str(os.environ.get(name, "") or "").strip().lower() == "live":
            return True
    return False

def _db_live_mode_requested(con: Any) -> bool:
    if con is None:
        return False
    try:
        if not _table_exists(con, "execution_mode"):
            return False
        row = con.execute("SELECT mode FROM execution_mode WHERE id=1").fetchone()
        return bool(row and str(row[0] or "").strip().lower() == "live")
    except Exception as e:
        _warn_nonfatal(
            "KILL_SWITCH_EXECUTION_MODE_LIVE_CHECK_FAILED",
            e,
            once_key="execution_mode_live_check",
        )
        return False

def _live_execution_disabled_block(con: Any = None) -> Optional[Tuple[bool, str, Dict[str, Any]]]:
    if not live_execution_disabled():
        return None
    if not (_env_live_mode_requested() or _db_live_mode_requested(con)):
        return None
    return False, DISABLE_LIVE_EXECUTION_REASON, {
        "scope": "global",
        "key": DISABLE_LIVE_EXECUTION_ENV,
        "env": DISABLE_LIVE_EXECUTION_ENV,
    }

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


def _parse_meta_json(meta_json: Any) -> Dict[str, Any]:
    if not meta_json:
        return {}
    try:
        parsed = json.loads(str(meta_json))
    except Exception as e:
        _warn_nonfatal(
            "KILL_SWITCH_META_JSON_PARSE_FAILED",
            e,
            once_key=f"meta_json_parse:{hash(str(meta_json))}",
        )
        return {}
    return dict(parsed) if isinstance(parsed, dict) else {}


def _state_owned_by(st: Optional[Tuple[int, str, str, str, int, int]], *, actor: str, trigger: str) -> bool:
    if not st or int(st[0] or 0) != 1:
        return False
    if _s(st[2]) != _s(actor):
        return False
    meta = _parse_meta_json(st[3])
    return _s(meta.get("trigger")) == _s(trigger)


def _protected_manual_halt(st: Optional[Tuple[int, str, str, str, int, int]]) -> bool:
    if not st or int(st[0] or 0) != 1:
        return False
    actor = _s(st[2]).lower()
    reason = _s(st[1]).lower()
    meta = _parse_meta_json(st[3])
    meta_text = json.dumps(meta, separators=(",", ":"), sort_keys=True).lower() if meta else ""
    protected_tokens = (
        "operator",
        "manual",
        "emergency",
        "startup",
        "preflight",
        "break_glass",
        "break-glass",
        "initial_hold",
        "global_hold",
    )
    return any(token in actor or token in reason or token in meta_text for token in protected_tokens)


def _read_state_hot(con, scope: str, key: str) -> Optional[Tuple[int, str, str, str, int, int]]:
    """Read persisted switch state from Redis on non-transactional hot paths."""

    if not bool(getattr(con, "in_transaction", False)):
        try:
            from engine.cache.wrappers.kill_switch import read_kill_switch

            snapshot_payload = read_kill_switch() or {}
            for row in list(snapshot_payload.get("state") or []):
                if not isinstance(row, dict):
                    continue
                try:
                    enabled = int(row.get("enabled") or 0) == 1
                except Exception:
                    enabled = False
                if not enabled:
                    continue
                row_scope = _s(row.get("scope")).lower()
                row_key = _s(row.get("key")).lower()
                row_reason = _s(row.get("reason")).lower()
                if row_scope == "global" and (
                    row_key == "provider_unavailable"
                    or row_reason == "kill_switch_provider_unavailable"
                ):
                    meta_json = json.dumps(dict(row.get("meta") or {}), separators=(",", ":"), sort_keys=True)
                    return (
                        1,
                        _s(row.get("reason")) or "kill_switch_provider_unavailable",
                        _s(row.get("actor")) or "engine.cache.wrappers.kill_switch",
                        meta_json,
                        int(row.get("created_ts_ms") or 0),
                        int(row.get("updated_ts_ms") or _now_ms()),
                    )
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


def _snapshot_row_state(row: Dict[str, Any]) -> Optional[Tuple[str, Tuple[int, str, str, str, int, int]]]:
    if not isinstance(row, dict):
        return None
    if _s(row.get("scope")).lower() != "global":
        return None
    try:
        enabled = 1 if int(row.get("enabled") or 0) else 0
    except Exception:
        enabled = 0
    if enabled != 1:
        return None
    key = _s(row.get("key")) or "global"
    meta_json = json.dumps(dict(row.get("meta") or {}), separators=(",", ":"), sort_keys=True)
    return (
        key,
        (
            1,
            _s(row.get("reason")),
            _s(row.get("actor")),
            meta_json,
            int(row.get("created_ts_ms") or 0),
            int(row.get("updated_ts_ms") or 0),
        ),
    )


def _active_global_state_hot(con) -> Optional[Tuple[str, Tuple[int, str, str, str, int, int]]]:
    """Return the active global DB switch, preferring the manual/global row."""

    if not bool(getattr(con, "in_transaction", False)):
        try:
            from engine.cache.wrappers.kill_switch import read_kill_switch

            rows = []
            for row in list((read_kill_switch() or {}).get("state") or []):
                item = _snapshot_row_state(row)
                if item is not None:
                    rows.append(item)
            rows.sort(key=lambda item: (0 if item[0] == "global" else 1, -int(item[1][5] or 0), item[0]))
            for key, st in rows:
                current = _maybe_auto_expire(con, "global", key, st)
                if current and int(current[0] or 0) == 1:
                    return key, current
        except Exception as e:
            _warn_nonfatal(
                "KILL_SWITCH_ACTIVE_GLOBAL_CACHE_READ_FAILED",
                e,
                once_key="active_global_cache_read",
            )

    owns = False
    db = con
    if db is None:
        try:
            db = connect()
            owns = True
        except Exception as e:
            _warn_nonfatal(
                "KILL_SWITCH_ACTIVE_GLOBAL_CONNECT_FAILED",
                e,
                once_key="active_global_connect",
            )
            return None
    try:
        if not _schema_ready_for_reads(db):
            return None
        rows = db.execute(
            """
            SELECT key, enabled, reason, actor, meta_json, created_ts_ms, updated_ts_ms
            FROM kill_switch_state
            WHERE scope='global' AND enabled=1
            ORDER BY CASE WHEN key='global' THEN 0 ELSE 1 END, updated_ts_ms DESC, key
            """
        ).fetchall()
        for row in rows or []:
            key = _s(row[0]) or "global"
            st = (
                1 if int(row[1] or 0) else 0,
                _s(row[2]),
                _s(row[3]),
                _s(row[4]),
                int(row[5] or 0),
                int(row[6] or 0),
            )
            current = _maybe_auto_expire(db, "global", key, st)
            if current and int(current[0] or 0) == 1:
                return key, current
    except Exception as e:
        _warn_nonfatal(
            "KILL_SWITCH_ACTIVE_GLOBAL_DB_READ_FAILED",
            e,
            once_key="active_global_db_read",
        )
    finally:
        if owns and db is not None:
            try:
                db.close()
            except Exception as e:
                _warn_nonfatal("KILL_SWITCH_ACTIVE_GLOBAL_CLOSE_FAILED", e, once_key="active_global_close")
    return None


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
            from engine.execution.kill_switch_reactivity import notify_kill_switch_state_changed

            notify_kill_switch_state_changed(enabled=bool(en), ts_ms=int(now_ms))
        except Exception as e:
            _warn_nonfatal(
                "KILL_SWITCH_REACTIVITY_NOTIFY_FAILED",
                e,
                once_key=f"reactivity_notify:{scope_n}:{key_n}:{en}",
                scope=str(scope_n),
                key=str(key_n),
                enabled=int(en),
            )
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
    scope_n = _norm_scope(scope)
    key_n = _norm_key(scope_n, key)
    set_kill_switch(scope, key, 1, reason=reason, actor=actor, meta=meta, action=action, con=con)

    def _clear_after_commit() -> None:
        _clear_activation_failure_marker(scope_n, key_n)

    if con is not None and bool(getattr(con, "in_transaction", False)):
        register = getattr(con, "register_after_commit", None)
        if callable(register):
            register(_clear_after_commit)
        return
    _clear_after_commit()

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


def activate_owned(
    scope: str,
    key: str,
    reason: str,
    *,
    owner_actor: str,
    trigger: str,
    meta: Optional[Dict[str, Any]] = None,
    action: str = "AUTO",
    con=None,
) -> bool:
    """Activate a switch without overwriting an active row owned by another actor."""

    scope_n = _norm_scope(scope)
    key_n = _norm_key(scope_n, key)
    owns = False
    db = con
    if db is None:
        db = connect()
        owns = True
    try:
        if not bool(getattr(db, "in_transaction", False)):
            _ensure_schema(db)
        st = _read_state(db, scope_n, key_n)
        if st and int(st[0] or 0) == 1 and not _state_owned_by(st, actor=owner_actor, trigger=trigger):
            return False
        payload = dict(meta or {})
        payload["actor"] = _s(owner_actor) or "system"
        payload["trigger"] = _s(trigger)
        activate(scope_n, key_n, reason=reason, actor=owner_actor, meta=payload, action=action, con=db)
        return True
    finally:
        if owns:
            db.close()


def clear_owned(
    scope: str,
    key: str,
    *,
    owner_actor: str,
    trigger: str,
    reason: Optional[str] = None,
    meta: Optional[Dict[str, Any]] = None,
    con=None,
) -> bool:
    """Clear only when the active DB row is owned by the expected actor/trigger."""

    scope_n = _norm_scope(scope)
    key_n = _norm_key(scope_n, key)
    owns = False
    db = con
    if db is None:
        db = connect()
        owns = True
    try:
        if not bool(getattr(db, "in_transaction", False)):
            _ensure_schema(db)
        st = _read_state(db, scope_n, key_n)
        if not _state_owned_by(st, actor=owner_actor, trigger=trigger):
            return False
        payload = dict(meta or {})
        payload["actor"] = _s(owner_actor) or "system"
        payload["trigger"] = _s(trigger)
        payload["cleared_owned_halt"] = True
        clear(scope_n, key_n, reason=reason, actor=owner_actor, meta=payload, con=db)
        return True
    finally:
        if owns:
            db.close()


def clear_manual_halt(
    scope: str = "global",
    key: str = "global",
    *,
    reason: str,
    actor: str = "operator",
    meta: Optional[Dict[str, Any]] = None,
    con=None,
) -> Dict[str, Any]:
    """Explicit operator workflow for clearing non-rules kill-switch holds."""

    scope_n = _norm_scope(scope)
    key_n = _norm_key(scope_n, key)
    actor_s = _s(actor) or "operator"
    reason_s = _s(reason) or "operator_manual_halt_clear"

    owns = False
    db = con
    if db is None:
        db = connect()
        owns = True
    try:
        if not bool(getattr(db, "in_transaction", False)):
            _ensure_schema(db)
        st = _read_state(db, scope_n, key_n)
        if not st or int(st[0] or 0) != 1:
            return {
                "ok": False,
                "error": "manual_halt_not_active",
                "scope": scope_n,
                "key": key_n,
            }
        previous_meta = _parse_meta_json(st[3])
        if _state_owned_by(st, actor="rules_engine", trigger=_s(previous_meta.get("trigger"))):
            return {
                "ok": False,
                "error": "manual_clear_refused_rules_owned_halt",
                "scope": scope_n,
                "key": key_n,
                "active_actor": st[2],
                "active_trigger": _s(previous_meta.get("trigger")),
            }
        payload = dict(meta or {})
        payload.update(
            {
                "manual_clear": True,
                "cleared_by": actor_s,
                "previous_actor": st[2],
                "previous_reason": st[1],
                "previous_meta": previous_meta,
            }
        )
        clear(scope_n, key_n, reason=reason_s, actor=actor_s, meta=payload, con=db)
        return {
            "ok": True,
            "scope": scope_n,
            "key": key_n,
            "actor": actor_s,
            "reason": reason_s,
            "previous_actor": st[2],
            "previous_reason": st[1],
        }
    finally:
        if owns:
            db.close()

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


def _equity_window_status(
    con,
    *,
    window: str,
    now_ms: int,
    min_points: int,
    max_age_s: int,
    lookback_ms: Optional[int] = None,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    min_points_i = max(1, int(min_points))
    max_age_i = max(0, int(max_age_s))
    window_s = _s(window) or "equity"
    base: Dict[str, Any] = {
        "ok": False,
        "window": window_s,
        "source": "equity_history",
        "table_present": None,
        "query_available": False,
        "points": 0,
        "min_points": int(min_points_i),
        "invalid_points": 0,
        "latest_ts_ms": None,
        "latest_age_s": None,
        "max_equity_age_s": int(max_age_i),
        "lookback_ms": (int(lookback_ms) if lookback_ms is not None else None),
        "limit": (int(limit) if limit is not None else None),
        "series": [],
    }

    try:
        if not _table_exists(con, "equity_history"):
            base.update(
                {
                    "table_present": False,
                    "reason_code": "KILL_SWITCH_EQUITY_HISTORY_MISSING",
                    "reason": "equity_history table missing",
                }
            )
            return base
    except Exception as e:
        _warn_nonfatal(
            "KILL_SWITCH_EQUITY_SERIES_TABLE_CHECK_FAILED",
            e,
            once_key="equity_series_table_check",
        )
        base.update(
            {
                "table_present": None,
                "reason_code": "KILL_SWITCH_EQUITY_TABLE_CHECK_ERROR",
                "reason": "equity_history table check failed",
                "error_type": type(e).__name__,
                "error": str(e),
            }
        )
        return base

    args: List[Any] = []
    where = ""
    if lookback_ms is not None and int(lookback_ms) > 0:
        where = " WHERE ts_ms >= ?"
        args.append(int(now_ms) - int(lookback_ms))

    q = f"SELECT ts_ms, equity FROM equity_history{where} ORDER BY ts_ms ASC"
    if limit is not None and int(limit) > 0:
        q = (
            "SELECT ts_ms, equity FROM ("
            f"SELECT ts_ms, equity FROM equity_history{where} ORDER BY ts_ms DESC LIMIT ?"
            ") AS recent_equity ORDER BY ts_ms ASC"
        )
        args.append(int(limit))

    try:
        rows = con.execute(q, tuple(args)).fetchall()
    except Exception as e:
        _warn_nonfatal(
            "KILL_SWITCH_EQUITY_SERIES_QUERY_FAILED",
            e,
            once_key="equity_series_query",
        )
        base.update(
            {
                "table_present": True,
                "reason_code": "KILL_SWITCH_EQUITY_QUERY_ERROR",
                "reason": "equity_history query failed",
                "error_type": type(e).__name__,
                "error": str(e),
            }
        )
        return base

    out: List[Tuple[int, float]] = []
    invalid_points = 0
    for ts_ms, equity in rows or []:
        try:
            ts_i = int(ts_ms or 0)
            eq_f = float(equity)
            if ts_i <= 0 or not math.isfinite(eq_f) or eq_f <= 0.0:
                invalid_points += 1
                continue
            out.append((ts_i, eq_f))
        except Exception as e:
            invalid_points += 1
            _warn_nonfatal(
                "KILL_SWITCH_EQUITY_SERIES_ROW_FAILED",
                e,
                once_key="equity_series_row_failed",
            )
            continue

    base.update(
        {
            "table_present": True,
            "query_available": True,
            "points": int(len(out)),
            "invalid_points": int(invalid_points),
            "series": out,
        }
    )

    if not out:
        base.update(
            {
                "reason_code": "KILL_SWITCH_EQUITY_WINDOW_EMPTY",
                "reason": f"equity_history window empty: {window_s}",
            }
        )
        return base

    latest_ts_ms = int(out[-1][0])
    latest_age_s = max(0.0, (float(now_ms) - float(latest_ts_ms)) / 1000.0)
    base.update({"latest_ts_ms": latest_ts_ms, "latest_age_s": latest_age_s})

    if max_age_i > 0 and latest_age_s > float(max_age_i):
        base.update(
            {
                "reason_code": "KILL_SWITCH_EQUITY_LATEST_STALE",
                "reason": f"latest equity too stale: age_s={latest_age_s:.1f} max_age_s={max_age_i}",
            }
        )
        return base

    if len(out) < min_points_i:
        base.update(
            {
                "reason_code": "KILL_SWITCH_EQUITY_WINDOW_INSUFFICIENT_POINTS",
                "reason": f"equity_history insufficient points: {window_s} points={len(out)} min={min_points_i}",
            }
        )
        return base

    base.update({"ok": True, "reason_code": "KILL_SWITCH_EQUITY_WINDOW_OK", "reason": "ok"})
    return base


def _public_equity_window_status(status: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(status or {})
    out.pop("series", None)
    return out


def _equity_series(con, lookback_ms: Optional[int] = None, limit: Optional[int] = None) -> List[Tuple[int, float]]:
    status = _equity_window_status(
        con,
        window="ad_hoc",
        now_ms=_now_ms(),
        min_points=1,
        max_age_s=0,
        lookback_ms=lookback_ms,
        limit=limit,
    )
    out = list(status.get("series") or [])
    return out


def capital_equity_freshness_snapshot(
    con=None,
    *,
    live_mode: Optional[bool] = None,
    include_series: bool = False,
) -> Dict[str, Any]:
    owns = False
    db = con
    if db is None:
        db = connect()
        owns = True

    try:
        now_ms = _now_ms()
        live_required = bool(_env_live_mode_requested() or _db_live_mode_requested(db)) if live_mode is None else bool(live_mode)
        max_age_s = max(1, int(KILL_SWITCH_MAX_EQUITY_AGE_S))
        rolling_lookback_ms = max(0, int(KILL_SWITCH_ROLLING_DRAWDOWN_LOOKBACK_DAYS)) * 86_400_000
        var_min_points = max(
            2,
            int(KILL_SWITCH_VAR_EQUITY_MIN_POINTS),
            int(KILL_SWITCH_VAR_MIN_HISTORY) + 1,
        )

        latest = _equity_window_status(
            db,
            window="latest",
            now_ms=now_ms,
            min_points=1,
            max_age_s=max_age_s,
            limit=1,
        )
        daily = _equity_window_status(
            db,
            window="daily",
            now_ms=now_ms,
            min_points=max(2, int(KILL_SWITCH_DAILY_EQUITY_MIN_POINTS)),
            max_age_s=max_age_s,
            lookback_ms=86_400_000,
        )
        rolling = _equity_window_status(
            db,
            window="rolling",
            now_ms=now_ms,
            min_points=max(2, int(KILL_SWITCH_ROLLING_EQUITY_MIN_POINTS)),
            max_age_s=max_age_s,
            lookback_ms=rolling_lookback_ms,
        )
        var = _equity_window_status(
            db,
            window="var",
            now_ms=now_ms,
            min_points=var_min_points,
            max_age_s=max_age_s,
            limit=max(1, int(KILL_SWITCH_VAR_LOOKBACK_POINTS)),
        )

        windows = {"latest": latest, "daily": daily, "rolling": rolling, "var": var}
        blockers = [
            f"{name}:{status.get('reason_code')}"
            for name, status in windows.items()
            if not bool(status.get("ok"))
        ]
        out: Dict[str, Any] = {
            "ok": not blockers,
            "required": bool(live_required),
            "ts_ms": int(now_ms),
            "source": "equity_history",
            "max_equity_age_s": int(max_age_s),
            "windows": {
                name: (dict(status) if include_series else _public_equity_window_status(status))
                for name, status in windows.items()
            },
            "blockers": blockers,
            "reason_code": (str(windows[blockers[0].split(':', 1)[0]].get("reason_code")) if blockers else "KILL_SWITCH_EQUITY_FRESHNESS_OK"),
        }
        out["reason"] = "ok" if out["ok"] else "; ".join(blockers)
        return out
    finally:
        if owns and db is not None:
            db.close()


def _capital_equity_availability_breach(con, *, now_ms: int, live_mode_requested: bool) -> Optional[Dict[str, Any]]:
    if not live_mode_requested:
        return None
    snapshot = capital_equity_freshness_snapshot(con, live_mode=True, include_series=False)
    if bool(snapshot.get("ok")):
        return None
    reason_code = str(snapshot.get("reason_code") or "KILL_SWITCH_EQUITY_FRESHNESS_UNAVAILABLE")
    return {
        "reason": f"capital_equity_unavailable reason={reason_code}",
        "meta": {
            "trigger": "equity_availability",
            "reason_code": reason_code,
            "equity_freshness": snapshot,
            "ts_ms": int(now_ms),
        },
    }


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

    try:
        from engine.strategy.drawdown_state import evaluate_current_drawdown

        drawdown_state = evaluate_current_drawdown(con, now_ms=int(now_ms))
        drawdown_state_payload = drawdown_state.to_dict()
    except Exception as e:
        _warn_nonfatal(
            "KILL_SWITCH_DRAWDOWN_STATE_EVALUATION_FAILED",
            e,
            once_key="drawdown_state_evaluation",
        )
        drawdown_state_payload = {
            "ok": False,
            "reason_code": "DRAWDOWN_STATE_EVALUATION_FAILED",
            "error_type": type(e).__name__,
            "error": str(e),
        }

    live_mode_requested = bool(_env_live_mode_requested() or _db_live_mode_requested(con))
    if not bool(drawdown_state_payload.get("ok")):
        if not live_mode_requested:
            return None
        reason_code = str(drawdown_state_payload.get("reason_code") or "DRAWDOWN_STATE_UNAVAILABLE")
        return {
            "reason": f"capital_drawdown_state_unavailable reason={reason_code}",
            "meta": {
                "trigger": "drawdown_state_unavailable",
                "reason_code": reason_code,
                "drawdown_state": drawdown_state_payload,
                "ts_ms": int(now_ms),
            },
        }

    equity_availability = _capital_equity_availability_breach(
        con,
        now_ms=int(now_ms),
        live_mode_requested=live_mode_requested,
    )
    if equity_availability is not None:
        return equity_availability

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

    activation_failure = activation_failure_snapshot()
    if bool(activation_failure.get("active")):
        return False, "kill_switch_activation_failed", {
            "scope": _s(activation_failure.get("scope")) or "global",
            "key": _s(activation_failure.get("key")) or "global",
            "reason": _s(activation_failure.get("reason")) or "kill_switch_activation_write_failed",
            "activation_failure": dict(activation_failure),
        }

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
                active_global = _active_global_state_hot(con)
                if active_global is not None:
                    active_key, st = active_global
                    return False, "kill_switch_db_global", {
                        "scope": "global",
                        "key": active_key,
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

    disabled_live_block = _live_execution_disabled_block(con)
    if disabled_live_block is not None:
        return disabled_live_block

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
        disabled_live_block = _live_execution_disabled_block(con)
        if disabled_live_block is not None:
            return disabled_live_block

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
                meta = {"scope": "global", "key": "global"}
                try:
                    from engine.runtime.risk_state import get_state as _risk_get_state

                    raw_diag = _s(_risk_get_state("capital_drawdown_diagnostic_json", ""))
                    if raw_diag:
                        drawdown_state = json.loads(raw_diag)
                        if isinstance(drawdown_state, dict):
                            meta["drawdown_state"] = drawdown_state
                            meta["reason_code"] = _s(drawdown_state.get("reason_code"))
                    stop_reason = _s(_risk_get_state("stop_reason", ""))
                    if stop_reason:
                        meta["stop_reason"] = stop_reason
                except Exception as e:
                    _warn_nonfatal(
                        "KILL_SWITCH_CAPITAL_GUARD_DIAGNOSTIC_READ_FAILED",
                        e,
                        once_key="capital_guard_diagnostic_read",
                    )
                return False, "capital_guard_block", meta
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

            try:
                snapshot_payload = _portfolio_snapshot(con, meta=meta)
            except Exception as e:
                _warn_nonfatal(
                    "KILL_SWITCH_PORTFOLIO_SNAPSHOT_FOR_CAPITAL_BREACH_FAILED",
                    e,
                    once_key="portfolio_snapshot_for_capital_breach",
                )
                snapshot_payload = {
                    "ts_ms": int(now_ms),
                    "equity": 0.0,
                    "gross_exposure": 0.0,
                    "net_exposure": 0.0,
                    "per_symbol_weights": {},
                    "positions": [],
                    "rolling_drawdown": 0.0,
                    "var_pct": 0.0,
                    "metadata": dict(meta or {}),
                }
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
            activation_failure = None
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
                activation_failure = _record_activation_failure(
                    scope="global",
                    key="global",
                    reason=reason,
                    actor="risk_engine",
                    meta=meta,
                    action="AUTO",
                    trigger_kind="capital",
                    error=e,
                )
            if activation_failure is not None:
                return False, "capital_kill_switch_activation_failed", {
                    "scope": "global",
                    "key": "global",
                    "reason": reason,
                    "meta": meta,
                    "activation_failure": activation_failure,
                }
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
            activation_failure = None
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
                activation_failure = _record_activation_failure(
                    scope="model",
                    key=mid,
                    reason=reason,
                    actor="risk_engine",
                    meta=meta,
                    action="AUTO",
                    trigger_kind="model",
                    error=e,
                )
            if activation_failure is not None:
                return False, "model_kill_switch_activation_failed", {
                    "scope": "model",
                    "key": mid,
                    "reason": reason,
                    "meta": meta,
                    "activation_failure": activation_failure,
                }
            return False, "model_aware_kill_switch", {"scope": "model", "key": mid, "reason": reason, "meta": meta}

        # DB switches
        active_global = _active_global_state_hot(con)
        if active_global is not None:
            active_key, st = active_global
            if _s(st[1]) == "kill_switch_provider_unavailable":
                return False, "kill_switch_provider_unavailable", {
                    "scope": "global",
                    "key": active_key or "provider_unavailable",
                    "reason": st[1],
                    "actor": st[2],
                }
            return False, "kill_switch_db_global", {"scope": "global", "key": active_key, "reason": st[1], "actor": st[2]}

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
    def _with_activation_failure(payload: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(payload or {"state": []})
        failure = activation_failure_snapshot()
        if bool(failure.get("active")):
            out["activation_failure"] = dict(failure)
        return out

    if con is None:
        try:
            from engine.cache.wrappers.kill_switch import read_kill_switch

            return _with_activation_failure(read_kill_switch())
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
        try:
            from engine.cache.wrappers.kill_switch import annotate_effective_state

            payload = annotate_effective_state({"state": out}, persisted_read_source="db")
        except Exception as e:
            _warn_nonfatal(
                "KILL_SWITCH_EFFECTIVE_STATE_ANNOTATION_FAILED",
                e,
                once_key="snapshot_effective_state_annotation",
            )
            payload = {"state": out}
        return _with_activation_failure(payload)
    finally:
        if owns:
            con.close()
