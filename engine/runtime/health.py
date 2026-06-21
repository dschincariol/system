"""
FILE: health.py

Runtime subsystem module for `health`.
"""

# engine/runtime/health.py
"""
Runtime Health + Preflight

Extracted from dashboard_server.py
No HTTP logic.
Pure health + startup validation.
"""
import os
import time
import json
import copy
import threading
from collections import Counter
from pathlib import Path
from typing import Dict, Any, Iterable, List, Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.hardware import runtime_hardware_snapshot
from engine.runtime.logging import get_logger
from engine.runtime.platform import default_backup_root_dir, default_local_log_dir
from engine.runtime.data_quality import (
    build_data_pipeline_gate_snapshot,
    get_feature_validation_snapshot,
    get_model_input_validation_snapshot,
    get_scoring_pipeline_snapshot,
)
from engine.runtime.storage import (
    DB_PATH,
    SCHEMA_VERSION as STORAGE_SCHEMA_VERSION,
    connect_ro_direct as _db_connect,
    get_db_validation_snapshot,
    get_timeseries_storage_snapshot,
)
from engine.runtime.lifecycle_state import get_state as _lc_get_state, WARMING_UP as _WARMING_UP, LIVE as _LIVE, DEGRADED as _DEGRADED
from engine.runtime.ingestion_status import get_pipeline_status, get_all_pipeline_statuses, pipeline_health_summary
from engine.runtime.metrics import emit_gauge
from engine.runtime.observability import get_component_health_snapshot
from engine.runtime.telemetry_read_router import fetch_event_log_summary, fetch_provider_health_rows
from engine.runtime.tracing import trace_event
from engine.runtime.runtime_meta import meta_get
from engine.runtime.ipc import market_data_status
from engine.runtime.health_disk import default_disk_pressure_paths as _health_default_disk_pressure_paths
from engine.runtime.health_disk import disk_path_snapshot as _health_disk_path_snapshot
from engine.runtime.health_disk import disk_pressure_snapshot as _health_disk_pressure_snapshot
from engine.runtime.health_disk import nearest_existing_path as _health_nearest_existing_path
from engine.runtime.health_normalization import dedupe_strs as _health_dedupe_strs
from engine.runtime.health_normalization import dict_or_empty as _health_dict_or_empty
from engine.runtime.health_normalization import float_or as _health_float_or
from engine.runtime.health_normalization import int_or as _health_int_or
from engine.runtime.health_normalization import json_dict_or_empty as _health_json_dict_or_empty
from engine.runtime.health_normalization import json_list_or_empty as _health_json_list_or_empty
from engine.runtime.health_normalization import json_meta_get as _health_json_meta_get
from engine.runtime.health_normalization import trace_section as _health_trace_section
from engine.runtime.health_normalization import warn_nonfatal as _health_warn_nonfatal
from engine.runtime.health_readiness import get_readiness_snapshot as _health_get_readiness_snapshot
from engine.runtime.health_snapshot import HealthSnapshotCheck
from engine.runtime.health_snapshot import HealthSnapshotContext
from engine.runtime.health_snapshot import build_context as _health_build_snapshot_context
from engine.runtime.health_snapshot import new_payload as _health_new_snapshot_payload
from engine.runtime.health_snapshot import pending_payload as _health_pending_snapshot_payload
from engine.runtime.health_snapshot import run_checks as _health_run_checks
from engine.runtime.health_snapshot import stale_payload as _health_stale_snapshot_payload
from engine.runtime.health_storage_checks import get_index_names as _health_get_index_names
from engine.runtime.health_storage_checks import get_table_cols as _health_get_table_cols
from engine.runtime.health_storage_checks import schema_audit as _health_schema_audit
from engine.runtime.health_storage_checks import sqlite_wal_path as _health_sqlite_wal_path
from engine.runtime.health_storage_checks import table_exists as _health_table_exists

log = get_logger("runtime.health")

HealthSnapshotContext.__module__ = __name__
HealthSnapshotCheck.__module__ = __name__

# ---------------------------------------------------
# ENV THRESHOLDS
# ---------------------------------------------------

HEALTH_PRICES_MAX_AGE_S = float(os.environ.get("HEALTH_PRICES_MAX_AGE_S", "120"))
HEALTH_EVENTS_MAX_AGE_S = float(os.environ.get("HEALTH_EVENTS_MAX_AGE_S", "600"))
HEALTH_OPTIONS_MAX_AGE_S = float(os.environ.get("HEALTH_OPTIONS_MAX_AGE_S", "1800"))
HEALTH_PREDICTIONS_MAX_AGE_S = float(os.environ.get("HEALTH_PREDICTIONS_MAX_AGE_S", "600"))
HEALTH_JOBS_MAX_STALE_S = float(os.environ.get("HEALTH_JOBS_MAX_STALE_S", "180"))
HEALTH_COMPETITION_MAX_AGE_S = float(os.environ.get("HEALTH_COMPETITION_MAX_AGE_S", "1800"))
HEALTH_ATTRIBUTION_MIN_RATIO = float(os.environ.get("HEALTH_ATTRIBUTION_MIN_RATIO", "0.95"))
HEALTH_RUNTIME_PRICE_CACHE_MAX_AGE_S = float(os.environ.get("HEALTH_RUNTIME_PRICE_CACHE_MAX_AGE_S", "120"))
HEALTH_EVENT_BUS_MAX_LAG_MS = float(os.environ.get("HEALTH_EVENT_BUS_MAX_LAG_MS", "5000"))
HEALTH_EVENT_BUS_MAX_QUEUE_DEPTH = int(os.environ.get("HEALTH_EVENT_BUS_MAX_QUEUE_DEPTH", "2000"))
HEALTH_MODEL_SERVING_WINDOW_S = float(os.environ.get("HEALTH_MODEL_SERVING_WINDOW_S", "900"))
HEALTH_MODEL_SERVING_MIN_SAMPLE = int(os.environ.get("HEALTH_MODEL_SERVING_MIN_SAMPLE", "20"))
HEALTH_MODEL_SERVING_MAX_FALLBACK_RATE = float(os.environ.get("HEALTH_MODEL_SERVING_MAX_FALLBACK_RATE", "0.05"))
HEALTH_MODEL_SERVING_MAX_ROWS = int(os.environ.get("HEALTH_MODEL_SERVING_MAX_ROWS", "500"))
HEALTH_ALERT_LIFECYCLE_WINDOW_S = float(os.environ.get("HEALTH_ALERT_LIFECYCLE_WINDOW_S", "86400"))
PROVIDER_CIRCUIT_BREAKER_ERRORS = int(os.environ.get("PROVIDER_CIRCUIT_BREAKER_ERRORS", "3"))
PROVIDER_READINESS_DEFAULT_MAX_AGE_S = float(
    os.environ.get("PROVIDER_READINESS_MAX_AGE_S", str(HEALTH_PRICES_MAX_AGE_S))
)

_PROVIDER_CREDENTIAL_SECRET_NAMES: Dict[str, tuple[str, ...]] = {
    "polygon": ("POLYGON_API_KEY",),
    "polygon_ws": ("POLYGON_API_KEY",),
    "tradier": ("TRADIER_API_TOKEN",),
}
_PROVIDER_READINESS_ENFORCED_MODES = {"paper", "live"}

HEALTH_MIN_LABELS = int(os.environ.get("HEALTH_MIN_LABELS", "10"))
HEALTH_MIN_MODEL_SUPPORT = int(os.environ.get("HEALTH_MIN_MODEL_SUPPORT", "10"))

PREFLIGHT_ENABLE = os.environ.get("PREFLIGHT_ENABLE", "1") == "1"
PREFLIGHT_PRICES_MAX_AGE_S = float(os.environ.get("PREFLIGHT_PRICES_MAX_AGE_S", "300"))
HEALTH_RUN_QUICK_CHECK = os.environ.get("HEALTH_RUN_QUICK_CHECK", "0") == "1"
HEALTH_INCLUDE_EXECUTION_BARRIER = os.environ.get("HEALTH_INCLUDE_EXECUTION_BARRIER", "0") == "1"
HEALTH_EMIT_METRICS = os.environ.get("HEALTH_EMIT_METRICS", "0") == "1"
DISK_PRESSURE_WARN_FREE_PCT = float(os.environ.get("DISK_PRESSURE_WARN_FREE_PCT", "15"))
DISK_PRESSURE_CRITICAL_FREE_PCT = float(os.environ.get("DISK_PRESSURE_CRITICAL_FREE_PCT", "5"))
DISK_PRESSURE_WARN_FREE_BYTES = int(float(os.environ.get("DISK_PRESSURE_WARN_FREE_BYTES", str(20 * 1024 * 1024 * 1024))))
DISK_PRESSURE_CRITICAL_FREE_BYTES = int(float(os.environ.get("DISK_PRESSURE_CRITICAL_FREE_BYTES", str(5 * 1024 * 1024 * 1024))))

_PREFLIGHT_CACHE: Dict = {
    "ok": False,
    "notes": [],
    "tables_ok": False,
    "health_ok": False,
    "ts_ms": 0,
}
_HEALTH_SNAPSHOT_CACHE_LOCK = threading.Lock()
_HEALTH_SNAPSHOT_CACHE: Dict[str, Any] = {
    "ts_ms": 0,
    "payload": None,
}
_HEALTH_SNAPSHOT_REFRESH_LOCK = threading.Lock()
_HEALTH_SNAPSHOT_CACHE_TTL_MS = int(float(os.environ.get("HEALTH_SNAPSHOT_CACHE_TTL_S", "2.5")) * 1000.0)
_HEALTH_SNAPSHOT_TRACE = str(os.environ.get("HEALTH_SNAPSHOT_TRACE", "")).strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)


def _warn(scope: str, err: Exception, **extra) -> None:
    _health_warn_nonfatal(log, log_failure, scope, err, **extra)


def _trace_section(name: str, started: float, **extra: Any) -> None:
    _health_trace_section(
        name,
        started,
        enabled=_HEALTH_SNAPSHOT_TRACE,
        logger=log,
        warn=_warn,
        perf_counter=time.perf_counter,
        **extra,
    )


def _sqlite_wal_path(db_path: Path) -> Optional[Path]:
    return _health_sqlite_wal_path(db_path)


def _int_or(value: Any, default: int = 0) -> int:
    return _health_int_or(value, default, warn=_warn)


def _float_or(value: Any, default: float = 0.0) -> float:
    return _health_float_or(value, default, warn=_warn)


def _dedupe_strs(values: List[str]) -> List[str]:
    return _health_dedupe_strs(values)


def _csv_env_set(name: str, default: str) -> set[str]:
    raw = str(os.environ.get(name, default) or default)
    return {
        str(part or "").strip().lower()
        for part in raw.split(",")
        if str(part or "").strip()
    }


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.environ.get(str(name), "")
    if raw is None or str(raw).strip() == "":
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _nearest_existing_path(path: Path) -> Path:
    return _health_nearest_existing_path(path, warn=_warn)


def _disk_path_snapshot(label: str, path: Path) -> Dict[str, Any]:
    return _health_disk_path_snapshot(
        label,
        path,
        warn_free_pct=DISK_PRESSURE_WARN_FREE_PCT,
        critical_free_pct=DISK_PRESSURE_CRITICAL_FREE_PCT,
        warn_free_bytes=DISK_PRESSURE_WARN_FREE_BYTES,
        critical_free_bytes=DISK_PRESSURE_CRITICAL_FREE_BYTES,
        warn=_warn,
    )


def _default_disk_pressure_paths() -> list[tuple[str, Path]]:
    return _health_default_disk_pressure_paths(
        environ=os.environ,
        db_path=Path(DB_PATH),
        default_log_dir=default_local_log_dir,
        default_backup_dir=default_backup_root_dir,
    )


def get_disk_pressure_snapshot(
    paths: Optional[Iterable[tuple[str, str | Path]]] = None,
) -> Dict[str, Any]:
    return _health_disk_pressure_snapshot(
        paths,
        default_paths=_default_disk_pressure_paths,
        warn_free_pct=DISK_PRESSURE_WARN_FREE_PCT,
        critical_free_pct=DISK_PRESSURE_CRITICAL_FREE_PCT,
        warn_free_bytes=DISK_PRESSURE_WARN_FREE_BYTES,
        critical_free_bytes=DISK_PRESSURE_CRITICAL_FREE_BYTES,
        warn=_warn,
    )


def _json_meta_get(key: str) -> Dict[str, Any]:
    return _health_json_meta_get(key, meta_get=meta_get, warn=_warn)


def _dict_or_empty(value: Any) -> Dict[str, Any]:
    return _health_dict_or_empty(value)


def _json_dict_or_empty(raw: Any) -> Dict[str, Any]:
    return _health_json_dict_or_empty(raw, warn=_warn)


def _json_list_or_empty(raw: Any) -> List[Any]:
    return _health_json_list_or_empty(raw, warn=_warn)


def _risk_state_value_readonly(con, key: str, default: str = "") -> str:
    owns_con = con is None
    db = con or _db_connect()
    try:
        if not _table_exists(db, "risk_state"):
            return str(default)
        row = db.execute(
            "SELECT value FROM risk_state WHERE key=?",
            (str(key),),
        ).fetchone()
        return str(row[0]) if row and row[0] is not None else str(default)
    finally:
        if owns_con:
            try:
                db.close()
            except Exception as e:
                _warn("health.risk_state_value.close", e, key=str(key))


def _portfolio_runtime_snapshot(con=None) -> Dict[str, Any]:
    try:
        payload = _json_dict_or_empty(_risk_state_value_readonly(con, "portfolio_runtime_health", ""))
    except Exception as e:
        _warn("health.portfolio_runtime.load", e)
        return {
            "ok": False,
            "available": False,
            "degraded": False,
            "detail": f"portfolio_runtime_error:{type(e).__name__}:{e}",
            "updated_ts_ms": None,
            "degraded_reasons": [],
            "degraded_codes": [],
        }

    if not payload:
        return {
            "ok": True,
            "available": False,
            "degraded": False,
            "detail": "portfolio_runtime_unreported",
            "updated_ts_ms": None,
            "degraded_reasons": [],
            "degraded_codes": [],
        }

    degraded_reasons = list(payload.get("degraded_reasons") or [])
    degraded_codes = [
        str((row or {}).get("code") or "").strip()
        for row in degraded_reasons
        if isinstance(row, dict) and str((row or {}).get("code") or "").strip()
    ]
    degraded = bool(payload.get("degraded")) or bool(degraded_reasons)
    updated_ts_ms = _int_or(payload.get("updated_ts_ms")) or None
    return {
        "ok": not degraded,
        "available": True,
        "degraded": degraded,
        "detail": ("portfolio_runtime_degraded" if degraded else "ok"),
        "updated_ts_ms": updated_ts_ms,
        "age_s": (round((int(time.time() * 1000) - int(updated_ts_ms)) / 1000.0, 1) if updated_ts_ms else None),
        "degraded_reasons": degraded_reasons,
        "degraded_codes": degraded_codes,
        "orders_n": _int_or(payload.get("orders_n")),
        "changed_symbols_n": _int_or(payload.get("changed_symbols_n")),
        "changed_symbols": [str(symbol) for symbol in list(payload.get("changed_symbols") or []) if str(symbol or "").strip()],
        "execution_blocked": bool(payload.get("execution_blocked")),
        "execution_blocked_codes": [
            str(code)
            for code in list(payload.get("execution_blocked_codes") or [])
            if str(code or "").strip()
        ],
    }


def _execution_degraded_snapshot(con=None) -> Dict[str, Any]:
    try:
        from engine.runtime.gates import get_execution_degraded_snapshot  # type: ignore

        payload = dict(
            get_execution_degraded_snapshot(
                risk_state_getter=lambda key, default="": _risk_state_value_readonly(con, str(key), str(default)),
            )
            or {}
        )
        if not isinstance(payload, dict):
            payload = {}
        payload.setdefault("active", False)
        payload.setdefault("severity", "WARNING")
        payload.setdefault("reason", "")
        payload.setdefault("reason_codes", [])
        payload.setdefault("sources", [])
        return payload
    except Exception as e:
        _warn("health.execution_degraded.load", e)
        return {
            "active": True,
            "severity": "CRITICAL",
            "reason": f"execution_degraded_error:{type(e).__name__}:{e}",
            "reason_codes": ["execution_degraded_error"],
            "sources": [],
        }


def _refresh_execution_barrier_snapshot(
    execution_degraded: Dict[str, Any],
    *,
    con=None,
) -> Dict[str, Any]:
    return _refresh_execution_barrier_snapshot_with_con(
        execution_degraded,
        con=con,
    )


def _read_kill_switch_snapshot_readonly(con=None) -> Dict[str, Any]:
    owns_con = con is None
    db = con or _db_connect()
    try:
        cache_meta: Dict[str, Any] = {}
        cache_diag_allowed = owns_con
        if not cache_diag_allowed:
            try:
                db_path = getattr(db, "_db_path", None)
                cache_diag_allowed = bool(db_path and Path(db_path).resolve() == Path(DB_PATH).resolve())
            except Exception:
                cache_diag_allowed = False
        if cache_diag_allowed:
            try:
                from engine.cache.wrappers.kill_switch import kill_switch_cache_diagnostics  # type: ignore

                cached = dict(kill_switch_cache_diagnostics() or {})
                for field in (
                    "loaded_ts_ms",
                    "source",
                    "max_age_ms",
                    "cache_age_ms",
                    "cache_fresh",
                    "read_source",
                    "cache_status",
                ):
                    if field in cached:
                        cache_meta[field] = cached.get(field)
            except Exception as e:
                _warn("health.kill_switch_cache_diagnostics", e)
                cache_meta = {
                    "source": "engine.runtime.health:cache_diagnostics_error",
                    "cache_fresh": False,
                    "cache_status": "diagnostics_error",
                    "error": f"{type(e).__name__}: {e}",
                }

        activation_failure = {"active": False}
        try:
            from engine.execution.kill_switch import activation_failure_snapshot  # type: ignore

            activation_failure = dict(activation_failure_snapshot() or {"active": False})
        except Exception as e:
            _warn("health.kill_switch_activation_failure_snapshot", e)

        row = db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            ("kill_switch_state",),
        ).fetchone()
        if not row:
            payload = {"state": [], **dict(cache_meta)}
            if bool(activation_failure.get("active")):
                payload["activation_failure"] = dict(activation_failure)
            return payload

        rows = db.execute(
            """
            SELECT scope, key, enabled, reason, actor, meta_json, created_ts_ms, updated_ts_ms
            FROM kill_switch_state
            ORDER BY scope, key
            """
        ).fetchall() or []
        out = []
        for row in rows:
            meta = {}
            try:
                meta = json.loads(row[5] or "{}") if row[5] else {}
            except Exception as e:
                _warn("health.kill_switch_snapshot.meta_decode", e)
                meta = {}
            out.append(
                {
                    "scope": str(row[0] or "").strip(),
                    "key": str(row[1] or "").strip(),
                    "enabled": int(row[2] or 0),
                    "reason": str(row[3] or "").strip(),
                    "actor": str(row[4] or "").strip(),
                    "meta": meta if isinstance(meta, dict) else {},
                    "created_ts_ms": int(row[6] or 0),
                    "updated_ts_ms": int(row[7] or 0),
                }
            )
        payload = {"state": out, **dict(cache_meta)}
        if bool(activation_failure.get("active")):
            payload["activation_failure"] = dict(activation_failure)
        return payload
    finally:
        if owns_con:
            db.close()


def get_kill_switch_snapshot_readonly(con=None) -> Dict[str, Any]:
    """Return kill-switch rows without triggering schema writes on read paths."""
    return _read_kill_switch_snapshot_readonly(con=con)


def _refresh_execution_barrier_snapshot_with_con(
    execution_degraded: Dict[str, Any],
    *,
    con=None,
) -> Dict[str, Any]:
    try:
        from engine.runtime.gates import execution_gate_snapshot  # type: ignore
    except Exception as e:
        _warn("health.execution_barrier.import", e)
        return {
            "ok": False,
            "allowed": False,
            "mode": "unknown",
            "reason": f"execution_barrier_import_error:{type(e).__name__}:{e}",
        }

    get_execution_mode_fn = None
    kill_switches = None
    try:
        from engine.api.internal_access import get_execution_mode as _get_execution_mode  # type: ignore

        get_execution_mode_fn = _get_execution_mode
    except Exception:
        get_execution_mode_fn = None
    try:
        kill_switches = _read_kill_switch_snapshot_readonly(con=con)
    except Exception as e:
        _warn("health.execution_barrier.kill_switch_snapshot", e)
        kill_switches = None

    try:
        return dict(
            execution_gate_snapshot(
                get_execution_mode_fn=get_execution_mode_fn,
                kill_switches=kill_switches,
                execution_degraded=dict(execution_degraded or {}),
                risk_state_getter=lambda key, default="": _risk_state_value_readonly(con, str(key), str(default)),
            )
            or {}
        )
    except Exception as e:
        _warn("health.execution_barrier.refresh", e)
        return {
            "ok": False,
            "allowed": False,
            "mode": "unknown",
            "reason": f"execution_barrier_error:{type(e).__name__}:{e}",
        }


def _competition_health_snapshot(now_ms: int) -> Dict[str, Any]:
    runtime = _json_meta_get("competition_runtime")
    replay_status = _json_meta_get("competition_replay_validation_status")
    cycle_status = _json_meta_get("competition_cycle_status")
    self_critic = _json_meta_get("competition_self_critic")
    attribution = _json_meta_get("attribution_completeness")

    updated_ts_ms = max(
        _int_or(runtime.get("updated_ts_ms")),
        _int_or(replay_status.get("updated_ts_ms")),
        _int_or(cycle_status.get("snapshot", {}).get("updated_ts_ms") if isinstance(cycle_status.get("snapshot"), dict) else 0),
        _int_or(cycle_status.get("capital_plan", {}).get("updated_ts_ms") if isinstance(cycle_status.get("capital_plan"), dict) else 0),
    )
    age_s = None if updated_ts_ms <= 0 else round((now_ms - int(updated_ts_ms)) / 1000.0, 1)
    replay_updated_ts_ms = _int_or(replay_status.get("updated_ts_ms"))
    replay_age_s = None if replay_updated_ts_ms <= 0 else round((now_ms - int(replay_updated_ts_ms)) / 1000.0, 1)

    attr_ratio = float((attribution or {}).get("authoritative_model_present_ratio") or 0.0)
    blocked_n = len((self_critic or {}).get("blocked_keys") or []) if isinstance(self_critic, dict) else 0
    replay_ready = str(replay_status.get("status") or "") == "ready"
    replay_fresh = bool(replay_updated_ts_ms > 0 and (now_ms - replay_updated_ts_ms) <= int(HEALTH_COMPETITION_MAX_AGE_S * 1000.0))
    cycle_ready = str(cycle_status.get("status") or "") == "ready"
    champion_present = bool((runtime.get("champion") or {}).get("model_name")) if isinstance(runtime.get("champion"), dict) else False

    ok = bool(
        replay_ready
        and replay_fresh
        and cycle_ready
        and champion_present
        and attr_ratio >= float(HEALTH_ATTRIBUTION_MIN_RATIO)
    )

    reasons: List[str] = []
    if not replay_ready:
        reasons.append("competition_replay_not_ready")
    if replay_ready and not replay_fresh:
        reasons.append("competition_replay_stale")
    if not cycle_ready:
        reasons.append(f"competition_cycle_status:{str(cycle_status.get('status') or 'missing')}")
    if not champion_present:
        reasons.append("competition_champion_missing")
    if attr_ratio < float(HEALTH_ATTRIBUTION_MIN_RATIO):
        reasons.append("competition_attribution_incomplete")

    return {
        "ok": ok,
        "updated_ts_ms": int(updated_ts_ms) if updated_ts_ms > 0 else None,
        "age_s": age_s,
        "max_age_s": float(HEALTH_COMPETITION_MAX_AGE_S),
        "champion_present": bool(champion_present),
        "replay_status": str(replay_status.get("status") or "missing"),
        "replay_age_s": replay_age_s,
        "replay_model_count": _int_or(replay_status.get("model_count")),
        "cycle_status": str(cycle_status.get("status") or "missing"),
        "self_critic_blocked": int(blocked_n),
        "authoritative_model_ratio": float(attr_ratio),
        "authoritative_model_min_ratio": float(HEALTH_ATTRIBUTION_MIN_RATIO),
        "reasons": reasons,
    }


def _pnl_decomposition_quality_snapshot(con) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "quality_available": False,
        "warning_row_count": 0,
        "max_residual_share": 0.0,
        "latest_warning_ts_ms": None,
        "latest_reconstruction_error": {},
        "quality_status": "ok",
        "detail": "pnl_decomposition_quality_columns_missing",
    }
    if not _table_exists(con, "pnl_decomposition"):
        out["detail"] = "pnl_decomposition_table_missing"
        return out
    cols = {str(col).strip().lower() for col in (_get_table_cols(con, "pnl_decomposition") or [])}
    required = {"reconstruction_error", "residual_share", "quality_status"}
    if not required.issubset(cols):
        return out
    try:
        row = con.execute(
            """
            SELECT
              SUM(CASE WHEN quality_status='warn' THEN 1 ELSE 0 END),
              MAX(COALESCE(residual_share, 0.0)),
              MAX(CASE WHEN quality_status='warn' THEN ts_ms ELSE 0 END)
            FROM pnl_decomposition
            """
        ).fetchone() or (0, 0.0, 0)
        latest = con.execute(
            """
            SELECT ts_ms, source_alert_id, symbol, reconstruction_error, residual_share, quality_status
            FROM pnl_decomposition
            ORDER BY ts_ms DESC, ABS(COALESCE(reconstruction_error, 0.0)) DESC
            LIMIT 1
            """
        ).fetchone()
    except Exception as e:
        _warn("health.pnl_decomposition_quality.query", e)
        out["detail"] = f"pnl_decomposition_quality_query_failed:{type(e).__name__}:{e}"
        return out
    out["quality_available"] = True
    out["warning_row_count"] = _int_or(row[0])
    out["max_residual_share"] = _float_or(row[1])
    out["latest_warning_ts_ms"] = (_int_or(row[2]) or None)
    if latest:
        out["latest_reconstruction_error"] = {
            "ts_ms": (_int_or(latest[0]) or None),
            "source_alert_id": (_int_or(latest[1]) or None),
            "symbol": str(latest[2] or ""),
            "reconstruction_error": _float_or(latest[3]),
            "residual_share": _float_or(latest[4]),
            "quality_status": str(latest[5] or "ok"),
        }
    out["quality_status"] = "warn" if int(out["warning_row_count"] or 0) > 0 else "ok"
    out["detail"] = "ok"
    return out


def _pnl_attribution_orphan_snapshot(con) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "available": False,
        "ok": True,
        "snapshot_ts_ms": None,
        "orphan_row_count": 0,
        "latest_orphan_ts_ms": None,
        "sample_keys": [],
        "detail": "pnl_attribution_table_missing",
    }
    if not _table_exists(con, "pnl_attribution"):
        return out
    pnl_cols = {str(col).strip().lower() for col in (_get_table_cols(con, "pnl_attribution") or [])}
    if not {"ts_ms", "source_alert_id", "model_id", "symbol"}.issubset(pnl_cols):
        out["available"] = True
        out["ok"] = False
        out["detail"] = "pnl_attribution_columns_missing"
        return out

    def _latest_pnl_rows_as_unaudited(detail: str) -> Dict[str, Any]:
        try:
            latest_row = con.execute("SELECT MAX(ts_ms) FROM pnl_attribution").fetchone() or (None,)
            snapshot_ts_ms = _int_or(latest_row[0])
            out["available"] = True
            out["snapshot_ts_ms"] = int(snapshot_ts_ms) if snapshot_ts_ms > 0 else None
            if snapshot_ts_ms <= 0:
                out["detail"] = "empty"
                return out
            count_row = con.execute(
                """
                SELECT COUNT(*), MAX(ts_ms)
                FROM pnl_attribution
                WHERE ts_ms = ?
                """,
                (int(snapshot_ts_ms),),
            ).fetchone() or (0, None)
            sample_rows = con.execute(
                """
                SELECT ts_ms, source_alert_id, model_id, symbol
                FROM pnl_attribution
                WHERE ts_ms = ?
                ORDER BY source_alert_id DESC, COALESCE(NULLIF(TRIM(model_id), ''), 'baseline') ASC, UPPER(TRIM(symbol)) ASC
                LIMIT 5
                """,
                (int(snapshot_ts_ms),),
            ).fetchall() or []
        except Exception as e:
            _warn("health.pnl_attribution_unaudited.query", e)
            out["ok"] = False
            out["detail"] = f"{detail}:{type(e).__name__}:{e}"
            return out
        out["orphan_row_count"] = _int_or(count_row[0])
        out["latest_orphan_ts_ms"] = (_int_or(count_row[1]) or None)
        out["sample_keys"] = [
            {
                "ts_ms": (_int_or(row[0]) or None),
                "source_alert_id": (_int_or(row[1]) or None),
                "model_id": str(row[2] or "").strip() or "baseline",
                "symbol": str(row[3] or "").strip().upper(),
            }
            for row in sample_rows
        ]
        out["ok"] = int(out["orphan_row_count"] or 0) == 0
        out["audit_detail"] = str(detail)
        out["detail"] = "ok" if out["ok"] else "orphan_pnl_rows_detected"
        return out

    if not _table_exists(con, "trade_attribution_ledger"):
        return _latest_pnl_rows_as_unaudited("trade_attribution_ledger_table_missing")
    ledger_cols = {str(col).strip().lower() for col in (_get_table_cols(con, "trade_attribution_ledger") or [])}
    if not {"id", "ts_ms", "source_alert_id", "model_id", "symbol", "suppression_reason"}.issubset(ledger_cols):
        return _latest_pnl_rows_as_unaudited("trade_attribution_ledger_columns_missing")
    try:
        latest_row = con.execute("SELECT MAX(ts_ms) FROM pnl_attribution").fetchone() or (None,)
        snapshot_ts_ms = _int_or(latest_row[0])
        out["available"] = True
        out["snapshot_ts_ms"] = int(snapshot_ts_ms) if snapshot_ts_ms > 0 else None
        if snapshot_ts_ms <= 0:
            out["detail"] = "empty"
            return out
        count_row = con.execute(
            """
            SELECT COUNT(*), MAX(p.ts_ms)
            FROM pnl_attribution p
            LEFT JOIN trade_attribution_ledger t
              ON t.ts_ms = p.ts_ms
             AND t.source_alert_id = p.source_alert_id
             AND COALESCE(NULLIF(TRIM(t.model_id), ''), 'baseline') = COALESCE(NULLIF(TRIM(p.model_id), ''), 'baseline')
             AND UPPER(TRIM(t.symbol)) = UPPER(TRIM(p.symbol))
             AND t.suppression_reason IS NULL
            WHERE p.ts_ms = ?
              AND t.id IS NULL
            """,
            (int(snapshot_ts_ms),),
        ).fetchone() or (0, None)
        sample_rows = con.execute(
            """
            SELECT p.ts_ms, p.source_alert_id, p.model_id, p.symbol
            FROM pnl_attribution p
            LEFT JOIN trade_attribution_ledger t
              ON t.ts_ms = p.ts_ms
             AND t.source_alert_id = p.source_alert_id
             AND COALESCE(NULLIF(TRIM(t.model_id), ''), 'baseline') = COALESCE(NULLIF(TRIM(p.model_id), ''), 'baseline')
             AND UPPER(TRIM(t.symbol)) = UPPER(TRIM(p.symbol))
             AND t.suppression_reason IS NULL
            WHERE p.ts_ms = ?
              AND t.id IS NULL
            ORDER BY p.source_alert_id DESC, COALESCE(NULLIF(TRIM(p.model_id), ''), 'baseline') ASC, UPPER(TRIM(p.symbol)) ASC
            LIMIT 5
            """,
            (int(snapshot_ts_ms),),
        ).fetchall() or []
    except Exception as e:
        _warn("health.pnl_attribution_orphans.query", e)
        out["detail"] = f"pnl_attribution_orphan_query_failed:{type(e).__name__}:{e}"
        return out
    out["orphan_row_count"] = _int_or(count_row[0])
    out["latest_orphan_ts_ms"] = (_int_or(count_row[1]) or None)
    out["sample_keys"] = [
        {
            "ts_ms": (_int_or(row[0]) or None),
            "source_alert_id": (_int_or(row[1]) or None),
            "model_id": str(row[2] or "").strip() or "baseline",
            "symbol": str(row[3] or "").strip().upper(),
        }
        for row in sample_rows
    ]
    out["ok"] = int(out["orphan_row_count"] or 0) == 0
    out["detail"] = "ok" if out["ok"] else "orphan_pnl_rows_detected"
    return out


def _latest_position_reconcile_snapshot(con, now_ms: int) -> Dict[str, Any]:
    try:
        from engine.execution.position_reconcile import position_reconcile_evidence_snapshot

        return position_reconcile_evidence_snapshot(
            engine_mode=os.environ.get("ENGINE_MODE", "safe"),
            con=con,
            now_ms=int(now_ms),
        )
    except Exception as e:
        _warn("health.position_reconcile.query", e)
        mode_name = str(os.environ.get("ENGINE_MODE", "safe") or "safe").strip().lower() or "safe"
        required = mode_name in ("paper", "live")
        return {
            "available": False,
            "ok": not required,
            "required": bool(required),
            "fatal_reconcile": bool(required),
            "status": "error",
            "reason": "ok" if not required else "position_reconcile_not_exercised",
            "blockers": ([] if not required else ["position_reconcile_not_exercised"]),
            "detail": f"position_reconcile_error:{type(e).__name__}:{e}",
        }


def _attribution_health_snapshot(con, now_ms: int) -> Dict[str, Any]:
    completeness = _json_meta_get("attribution_completeness")
    repair = _json_meta_get("trade_attribution_historical_repair")
    poll_state = _json_meta_get("execution_poll_and_attrib_last")
    quality = _pnl_decomposition_quality_snapshot(con)
    orphans = _pnl_attribution_orphan_snapshot(con)

    updated_ts_ms = max(
        _int_or(poll_state.get("ts_ms")),
        _int_or(repair.get("ts_ms")),
        _int_or((repair.get("rebuild") or {}).get("last_snapshot_ts_ms") if isinstance(repair.get("rebuild"), dict) else 0),
    )
    age_s = None if updated_ts_ms <= 0 else round((now_ms - int(updated_ts_ms)) / 1000.0, 1)
    authoritative_ratio = float((completeness or {}).get("authoritative_model_present_ratio") or 0.0)
    repair_ok = bool((repair or {}).get("ok")) if isinstance(repair, dict) and repair else None
    integrity_ok = bool(orphans.get("ok", True))
    return {
        "ok": authoritative_ratio >= float(HEALTH_ATTRIBUTION_MIN_RATIO) and integrity_ok,
        "updated_ts_ms": int(updated_ts_ms) if updated_ts_ms > 0 else None,
        "age_s": age_s,
        "authoritative_model_ratio": float(authoritative_ratio),
        "authoritative_model_min_ratio": float(HEALTH_ATTRIBUTION_MIN_RATIO),
        "rows": _int_or((completeness or {}).get("rows")),
        "historical_repair_ok": repair_ok,
        "historical_repair": repair if isinstance(repair, dict) else {},
        "warning_row_count": _int_or(quality.get("warning_row_count")),
        "max_residual_share": _float_or(quality.get("max_residual_share")),
        "latest_warning_ts_ms": quality.get("latest_warning_ts_ms"),
        "latest_reconstruction_error": dict(quality.get("latest_reconstruction_error") or {}),
        "quality_status": str(quality.get("quality_status") or "ok"),
        "quality_available": bool(quality.get("quality_available")),
        "orphans": orphans,
        "integrity_ok": integrity_ok,
    }


def _model_serving_snapshot(con, now_ms: int) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "ok": True,
        "degraded": False,
        "available": False,
        "window_s": float(HEALTH_MODEL_SERVING_WINDOW_S),
        "min_sample_size": int(HEALTH_MODEL_SERVING_MIN_SAMPLE),
        "fallback_rate_warn_threshold": float(HEALTH_MODEL_SERVING_MAX_FALLBACK_RATE),
        "sample_count": 0,
        "fallback_count": 0,
        "fallback_rate": 0.0,
        "last_fallback_ts_ms": None,
        "top_fallback_reasons": [],
        "detail": "tracked_predictions_table_missing",
    }
    if not _table_exists(con, "tracked_predictions"):
        return out
    cutoff_ts_ms = int(now_ms - max(1.0, float(HEALTH_MODEL_SERVING_WINDOW_S)) * 1000.0)
    try:
        rows = con.execute(
            """
            SELECT ts_ms, metadata_json
            FROM tracked_predictions
            WHERE ts_ms >= ?
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            (int(cutoff_ts_ms), int(max(1, HEALTH_MODEL_SERVING_MAX_ROWS))),
        ).fetchall() or []
    except Exception as e:
        _warn("health.model_serving.query", e)
        out["detail"] = f"model_serving_query_failed:{type(e).__name__}:{e}"
        return out
    reason_counts: Counter[str] = Counter()
    fallback_count = 0
    last_fallback_ts_ms = 0
    for ts_ms, metadata_json in rows:
        meta = _json_dict_or_empty(metadata_json)
        requested_model_name = str(meta.get("requested_model_name") or "").strip()
        resolved_model_name = str(meta.get("resolved_model_name") or "").strip()
        requested_family = str(meta.get("requested_model_family") or "").strip()
        served_family = str(meta.get("served_model_family") or meta.get("model_family") or "").strip()
        fallback_active = bool(meta.get("serve_fallback_active"))
        if not fallback_active:
            fallback_active = bool(
                (requested_model_name and resolved_model_name and requested_model_name != resolved_model_name)
                or (requested_family and served_family and requested_family != served_family)
            )
        if not fallback_active:
            continue
        fallback_count += 1
        last_fallback_ts_ms = max(int(last_fallback_ts_ms), _int_or(ts_ms))
        reason = str(meta.get("fallback_reason") or "unspecified").strip() or "unspecified"
        reason_counts[reason] += 1
    sample_count = int(len(rows))
    fallback_rate = float(fallback_count) / float(sample_count) if sample_count > 0 else 0.0
    degraded = bool(
        sample_count >= int(HEALTH_MODEL_SERVING_MIN_SAMPLE)
        and fallback_rate > float(HEALTH_MODEL_SERVING_MAX_FALLBACK_RATE)
    )
    out.update(
        {
            "ok": not degraded,
            "degraded": degraded,
            "available": True,
            "sample_count": int(sample_count),
            "fallback_count": int(fallback_count),
            "fallback_rate": float(fallback_rate),
            "last_fallback_ts_ms": (int(last_fallback_ts_ms) if last_fallback_ts_ms > 0 else None),
            "top_fallback_reasons": [
                {"reason": str(reason), "count": int(count)}
                for reason, count in reason_counts.most_common(5)
            ],
            "detail": (
                "insufficient_sample"
                if sample_count < int(HEALTH_MODEL_SERVING_MIN_SAMPLE)
                else (
                    f"fallback_rate_high:{round(fallback_rate, 4)}"
                    if degraded
                    else "ok"
                )
            ),
        }
    )
    return out


def _alert_lifecycle_snapshot(con, now_ms: int) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "ok": True,
        "warning": False,
        "available": False,
        "window_s": float(HEALTH_ALERT_LIFECYCLE_WINDOW_S),
        "recent_alerts": 0,
        "seen_count": 0,
        "consumed_count": 0,
        "expired_unconsumed_count": 0,
        "oldest_unconsumed_ts_ms": None,
        "oldest_unconsumed_age_ms": None,
        "oldest_unconsumed_age_s": None,
        "last_expired_ts_ms": None,
        "detail": "alert_lifecycle_columns_missing",
    }
    if not _table_exists(con, "alerts"):
        out["detail"] = "alerts_table_missing"
        return out
    cols = {str(col).strip().lower() for col in (_get_table_cols(con, "alerts") or [])}
    required = {
        "portfolio_first_seen_ts_ms",
        "portfolio_last_seen_ts_ms",
        "portfolio_consumed_ts_ms",
        "portfolio_expired_ts_ms",
        "portfolio_status",
    }
    if not required.issubset(cols):
        return out
    cutoff_ts_ms = int(now_ms - max(1.0, float(HEALTH_ALERT_LIFECYCLE_WINDOW_S)) * 1000.0)
    try:
        row = con.execute(
            """
            SELECT
              COUNT(*),
              SUM(CASE WHEN portfolio_status='seen' THEN 1 ELSE 0 END),
              SUM(CASE WHEN portfolio_status='consumed' THEN 1 ELSE 0 END),
              SUM(CASE WHEN portfolio_status='expired' AND COALESCE(portfolio_consumed_ts_ms, 0) <= 0 THEN 1 ELSE 0 END),
              MAX(CASE WHEN portfolio_status='expired' THEN portfolio_expired_ts_ms ELSE 0 END)
            FROM alerts
            WHERE ts_ms >= ?
            """,
            (int(cutoff_ts_ms),),
        ).fetchone() or (0, 0, 0, 0, 0)
        oldest = con.execute(
            """
            SELECT MIN(ts_ms)
            FROM alerts
            WHERE COALESCE(portfolio_consumed_ts_ms, 0) <= 0
              AND COALESCE(portfolio_status, 'new') IN ('new', 'seen')
            """
        ).fetchone()
    except Exception as e:
        _warn("health.alert_lifecycle.query", e)
        out["detail"] = f"alert_lifecycle_query_failed:{type(e).__name__}:{e}"
        return out
    oldest_ts_ms = _int_or((oldest or [0])[0])
    oldest_age_ms = max(0, int(now_ms) - int(oldest_ts_ms)) if oldest_ts_ms > 0 else None
    expired_unconsumed_count = _int_or(row[3])
    out.update(
        {
            "ok": expired_unconsumed_count <= 0,
            "warning": expired_unconsumed_count > 0,
            "available": True,
            "recent_alerts": _int_or(row[0]),
            "seen_count": _int_or(row[1]),
            "consumed_count": _int_or(row[2]),
            "expired_unconsumed_count": int(expired_unconsumed_count),
            "oldest_unconsumed_ts_ms": (int(oldest_ts_ms) if oldest_ts_ms > 0 else None),
            "oldest_unconsumed_age_ms": oldest_age_ms,
            "oldest_unconsumed_age_s": (round(float(oldest_age_ms) / 1000.0, 1) if oldest_age_ms is not None else None),
            "last_expired_ts_ms": (_int_or(row[4]) or None),
            "detail": (
                f"expired_unconsumed_alerts:{expired_unconsumed_count}"
                if expired_unconsumed_count > 0
                else "ok"
            ),
        }
    )
    return out


def _options_ingestion_snapshot(now_ms: int) -> Dict[str, Any]:
    status = get_pipeline_status("options_poll")
    mode_name = str(os.environ.get("ENGINE_MODE", "safe") or "safe").strip().lower() or "safe"
    if not status:
        return {
            "ok": True,
            "available": False,
            "degraded": False,
            "critical": False,
            "status": "unknown",
            "detail": "",
            "last_ingested_ts_ms": None,
            "age_s": None,
            "max_age_s": float(HEALTH_OPTIONS_MAX_AGE_S),
            "fresh_symbols": [],
            "cached_symbols": [],
            "failed_symbols": [],
            "disabled_symbols": [],
            "critical_symbols": [],
            "critical_unavailable_symbols": [],
        }

    meta = _dict_or_empty(status.get("meta"))
    last_ingested_ts_ms = _int_or(status.get("last_ingested_ts_ms"))
    age_s = None if last_ingested_ts_ms <= 0 else round((now_ms - int(last_ingested_ts_ms)) / 1000.0, 1)
    stale = bool(last_ingested_ts_ms > 0 and (now_ms - int(last_ingested_ts_ms)) > int(HEALTH_OPTIONS_MAX_AGE_S * 1000.0))
    critical_unavailable = sorted(str(v) for v in (meta.get("critical_unavailable_symbols") or []) if str(v).strip())
    critical_symbols = sorted(str(v) for v in (meta.get("critical_symbols") or []) if str(v).strip())
    failed_symbols = sorted(str(v) for v in (meta.get("failed_symbols") or []) if str(v).strip())
    disabled_symbols = sorted(str(v) for v in (meta.get("disabled_symbols") or []) if str(v).strip())
    cached_symbols = sorted(str(v) for v in (meta.get("cached_symbols") or []) if str(v).strip())
    fresh_symbols = sorted(str(v) for v in (meta.get("fresh_symbols") or []) if str(v).strip())
    symbol_status = _dict_or_empty(meta.get("symbol_status"))
    meta_critical = bool(meta.get("critical"))

    config_unavailable = False
    if critical_unavailable:
        config_unavailable = True
        for symbol in critical_unavailable:
            row = symbol_status.get(str(symbol)) if isinstance(symbol_status, dict) else None
            if not isinstance(row, dict):
                config_unavailable = False
                break
            status_name = str(row.get("status") or "").strip().lower()
            error_text = str(row.get("error") or "").strip().lower()
            if status_name not in {"disabled", "disabled_cached"} or "config_error" not in error_text:
                config_unavailable = False
                break

    if config_unavailable and mode_name == "safe":
        return {
            "ok": True,
            "available": False,
            "degraded": False,
            "critical": False,
            "status": "unavailable",
            "detail": "options_provider_unconfigured",
            "stale": False,
            "failed": False,
            "last_ingested_ts_ms": (int(last_ingested_ts_ms) if last_ingested_ts_ms > 0 else None),
            "age_s": age_s,
            "max_age_s": float(HEALTH_OPTIONS_MAX_AGE_S),
            "fresh_symbols": fresh_symbols,
            "cached_symbols": cached_symbols,
            "failed_symbols": failed_symbols,
            "disabled_symbols": disabled_symbols,
            "critical_symbols": critical_symbols,
            "critical_unavailable_symbols": critical_unavailable,
            "symbol_status": symbol_status,
        }

    detail = ""
    if critical_unavailable:
        detail = f"options_symbols_unavailable:{','.join(critical_unavailable[:8])}"
    elif stale:
        detail = "options_ingestion_stale"
    elif meta_critical:
        detail = "options_ingestion_critical"
    elif not bool(status.get("ok")):
        detail = "options_ingestion_failed"

    failed = bool(not status.get("ok"))
    degraded = bool(critical_unavailable or stale or meta_critical)
    ok = bool(status.get("ok")) and not degraded

    return {
        "ok": ok,
        "available": True,
        "degraded": degraded,
        "critical": bool(critical_unavailable or meta_critical),
        "status": ("degraded" if degraded else ("ok" if bool(status.get("ok")) else "failed")),
        "detail": detail,
        "stale": bool(stale),
        "failed": bool(failed),
        "last_ingested_ts_ms": (int(last_ingested_ts_ms) if last_ingested_ts_ms > 0 else None),
        "age_s": age_s,
        "max_age_s": float(HEALTH_OPTIONS_MAX_AGE_S),
        "fresh_symbols": fresh_symbols,
        "cached_symbols": cached_symbols,
        "failed_symbols": failed_symbols,
        "disabled_symbols": disabled_symbols,
        "critical_symbols": critical_symbols,
        "critical_unavailable_symbols": critical_unavailable,
        "symbol_status": symbol_status,
    }


def _provider_readiness_enforced(mode: str) -> bool:
    return str(mode or "").strip().lower() in _PROVIDER_READINESS_ENFORCED_MODES


def _provider_readiness_max_age_s(provider_name: str) -> float:
    provider = str(provider_name or "").strip().lower()
    if provider == "tradier":
        return float(HEALTH_OPTIONS_MAX_AGE_S)
    return float(PROVIDER_READINESS_DEFAULT_MAX_AGE_S)


def _explicit_required_provider_names() -> Optional[List[str]]:
    for env_name in (
        "PROVIDER_READINESS_REQUIRED_PROVIDERS",
        "LIVE_PROVIDER_READINESS_REQUIRED_PROVIDERS",
    ):
        raw = os.environ.get(env_name)
        if raw is None:
            continue
        lowered = str(raw or "").strip().lower()
        if lowered in {"", "none", "off", "disabled"}:
            return []
        return _dedupe_strs(
            [
                str(part or "").strip().lower()
                for part in str(raw or "").split(",")
                if str(part or "").strip()
            ]
        )
    return None


def _required_providers_from_env() -> List[str]:
    out: List[str] = []
    price_chain = [
        str(part or "").strip().lower()
        for part in str(os.environ.get("LIVE_PRICE_PROVIDER_CHAIN", "") or "").split(",")
        if str(part or "").strip()
    ]
    options_chain = [
        str(part or "").strip().lower()
        for part in str(os.environ.get("OPTIONS_PROVIDER_CHAIN", "") or "").split(",")
        if str(part or "").strip()
    ]
    out.extend(price_chain)
    out.extend(options_chain)
    if _env_flag("POLYGON_WS_ENABLED", False):
        out.append("polygon_ws")
    if _env_flag("POLYGON_REST_ENABLED", False):
        out.append("polygon")
    if _env_flag("IBKR_ENABLED", False):
        out.append("ibkr")
    if _env_flag("TRADIER_ENABLED", False):
        out.append("tradier")
    if _env_flag("YFINANCE_ENABLED", False):
        out.append("yfinance")
    if _env_flag("CCXT_ENABLED", False):
        out.append("ccxt")
    return _dedupe_strs(out)


def _manager_provider_sources() -> tuple[Dict[str, Dict[str, Any]], List[str], str]:
    """Return enabled managed provider sources without exposing credentials."""
    sources_by_provider: Dict[str, Dict[str, Any]] = {}
    required: List[str] = []
    error = ""
    try:
        from services.data_source_manager import load_sources_from_db

        for row in load_sources_from_db(include_credentials=False) or []:
            if not isinstance(row, dict) or not bool(row.get("enabled")):
                continue
            source_type = str(row.get("source_type") or "").strip()
            if source_type not in {"price_provider", "options_provider"}:
                continue
            provider_name = str(row.get("provider_name") or row.get("source_key") or "").strip().lower()
            if not provider_name:
                continue
            required.append(provider_name)
            current = dict(sources_by_provider.get(provider_name) or {})
            fields = _dedupe_strs(
                list(current.get("credential_fields") or [])
                + [str(field) for field in list(row.get("credential_fields") or []) if str(field).strip()]
            )
            sources_by_provider[provider_name] = {
                "source_key": str(row.get("source_key") or provider_name),
                "source_type": source_type,
                "job_name": str(row.get("job_name") or ""),
                "credential_fields": fields,
                "credentials_configured": bool(row.get("credentials_configured")),
                "credentials_stored": bool(row.get("credentials_stored")),
                "credential_error": str(row.get("credential_error") or ""),
                "status": str(row.get("status") or ""),
                "last_error": str(row.get("last_error") or ""),
                "last_success_ts_ms": int(row.get("last_success_ts_ms") or 0),
                "error_count": int(row.get("error_count") or 0),
                "config_hash": str(row.get("config_hash") or ""),
            }
    except Exception as exc:
        error = f"{type(exc).__name__}:{exc}"
        _warn("health.provider_readiness.manager_sources", exc)
    return sources_by_provider, _dedupe_strs(required), error


def _provider_credential_available(provider_name: str, source: Dict[str, Any]) -> tuple[bool, str, List[str]]:
    provider = str(provider_name or "").strip().lower()
    credential_fields = [str(field) for field in list(source.get("credential_fields") or []) if str(field).strip()]
    secret_names = [str(name) for name in _PROVIDER_CREDENTIAL_SECRET_NAMES.get(provider, tuple()) if str(name).strip()]
    if not credential_fields and not secret_names:
        return True, "not_required", []
    if str(source.get("credential_error") or "").strip():
        return False, "data_source_manager_error", secret_names
    if bool(source.get("credentials_configured")):
        return True, "data_source_manager", secret_names
    try:
        from engine.data._credentials import get_data_credential

        for secret_name in secret_names:
            if str(get_data_credential(secret_name) or "").strip():
                return True, "secret_loader", secret_names
    except Exception as exc:
        _warn("health.provider_readiness.secret_loader", exc, provider=provider)
    return False, "missing", secret_names


def _provider_auth_error(value: Any) -> bool:
    text = str(value or "").strip().lower()
    if not text:
        return False
    return any(
        marker in text
        for marker in (
            "401",
            "403",
            "unauthorized",
            "forbidden",
            "auth",
            "credential",
            "token_missing",
            "api_key",
            "api token",
            "not set",
        )
    )


def _provider_telemetry_from_health(health: Optional[Dict[str, Any]], now_ms: int) -> Dict[str, Dict[str, Any]]:
    providers = _dict_or_empty((health or {}).get("providers") if isinstance(health, dict) else {})
    by_provider = _dict_or_empty(providers.get("by_provider"))
    out: Dict[str, Dict[str, Any]] = {}
    for provider_name, raw in by_provider.items():
        if not isinstance(raw, dict):
            continue
        provider = str(provider_name or "").strip().lower()
        if not provider:
            continue
        row = dict(raw)
        last_ts_ms = _int_or(row.get("last_ts_ms") or row.get("ts_ms"))
        age_s = row.get("age_s")
        if age_s is None and row.get("age_ms") is not None:
            age_s = round(_int_or(row.get("age_ms")) / 1000.0, 1)
        if age_s is None and last_ts_ms > 0:
            age_s = round(max(0, int(now_ms) - int(last_ts_ms)) / 1000.0, 1)
        row["provider"] = provider
        row["last_ts_ms"] = last_ts_ms or None
        row["age_s"] = (float(age_s) if age_s is not None else None)
        row["ok"] = bool(row.get("ok"))
        row["error_count"] = _int_or(row.get("error_count"))
        row["circuit_open"] = bool(row.get("circuit_open"))
        out[provider] = row
    return out


def _provider_telemetry_from_rows(now_ms: int) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for row in fetch_provider_health_rows():
        provider = str(row.get("provider") or "").strip().lower()
        if not provider:
            continue
        ts_ms = _int_or(row.get("ts_ms"))
        age_s = None if ts_ms <= 0 else round(max(0, int(now_ms) - int(ts_ms)) / 1000.0, 1)
        last_success_ts_ms = _int_or(row.get("last_success_ts_ms"))
        error_count = _int_or(row.get("error_count"))
        latest_row_ok = bool(row.get("ok"))
        circuit_open = bool(
            int(PROVIDER_CIRCUIT_BREAKER_ERRORS) > 0
            and error_count >= int(PROVIDER_CIRCUIT_BREAKER_ERRORS)
            and not latest_row_ok
        )
        out[provider] = {
            "provider": provider,
            "ok": latest_row_ok,
            "age_s": age_s,
            "last_ts_ms": ts_ms or None,
            "last_success_ts_ms": (last_success_ts_ms if last_success_ts_ms > 0 else None),
            "last_success_age_s": (
                round(max(0, int(now_ms) - int(last_success_ts_ms)) / 1000.0, 1)
                if last_success_ts_ms > 0
                else None
            ),
            "error_count": error_count,
            "circuit_open": circuit_open,
            "circuit_breaker_error_threshold": int(PROVIDER_CIRCUIT_BREAKER_ERRORS),
            "latency_ms": row.get("latency_ms"),
            "n_symbols": row.get("n_symbols"),
            "error": row.get("error"),
        }
    return out


def provider_readiness_snapshot(
    *,
    mode: Optional[str] = None,
    health: Optional[Dict[str, Any]] = None,
    required_providers: Optional[Iterable[str]] = None,
    now_ms: Optional[int] = None,
) -> Dict[str, Any]:
    """Assess required market-data providers without exposing credential values."""

    now = int(now_ms if now_ms is not None else time.time() * 1000)
    mode_name = str(mode if mode is not None else os.environ.get("ENGINE_MODE", "safe") or "safe").strip().lower() or "safe"
    enforced = _provider_readiness_enforced(mode_name)
    sources_by_provider, manager_required, manager_error = _manager_provider_sources()

    if required_providers is not None:
        required = _dedupe_strs(
            [
                str(provider or "").strip().lower()
                for provider in required_providers
                if str(provider or "").strip()
            ]
        )
    else:
        explicit = _explicit_required_provider_names()
        required = list(explicit) if explicit is not None else list(manager_required or _required_providers_from_env())

    telemetry = _provider_telemetry_from_health(health, now)
    if not telemetry:
        telemetry = _provider_telemetry_from_rows(now)

    by_provider: Dict[str, Dict[str, Any]] = {}
    blockers: List[str] = []

    if enforced and not required:
        blockers.append("provider_readiness_required_providers_empty")

    for provider in required:
        provider_name = str(provider or "").strip().lower()
        if not provider_name:
            continue
        source = dict(sources_by_provider.get(provider_name) or {})
        telem = dict(telemetry.get(provider_name) or {})
        provider_blockers: List[str] = []
        max_age_s = _provider_readiness_max_age_s(provider_name)
        credential_ok, credential_source, credential_secret_names = _provider_credential_available(provider_name, source)
        credential_error = str(source.get("credential_error") or "").strip()
        if not credential_ok:
            provider_blockers.append(f"provider_unauthenticated:{provider_name}")
        if credential_error:
            provider_blockers.append(f"provider_credential_error:{provider_name}")

        age_s = telem.get("age_s")
        age_value = None if age_s is None else float(age_s)
        has_telemetry = bool(telem)
        telemetry_ok = bool(telem.get("ok")) if has_telemetry else False
        telemetry_error = str(telem.get("error") or source.get("last_error") or "").strip()
        stale = bool((not has_telemetry) or age_value is None or age_value > float(max_age_s))
        circuit_open = bool(telem.get("circuit_open"))

        if not has_telemetry:
            provider_blockers.append(f"provider_telemetry_missing:{provider_name}")
        elif stale:
            provider_blockers.append(f"provider_stale:{provider_name}")
        if circuit_open:
            provider_blockers.append(f"provider_circuit_open:{provider_name}")
        if has_telemetry and not telemetry_ok:
            if _provider_auth_error(telemetry_error):
                provider_blockers.append(f"provider_unauthenticated:{provider_name}")
            else:
                provider_blockers.append(f"provider_unhealthy:{provider_name}")

        provider_blockers = _dedupe_strs(provider_blockers)
        provider_ok = not provider_blockers
        blockers.extend(provider_blockers)
        by_provider[provider_name] = {
            "ok": provider_ok,
            "required": True,
            "mode": mode_name,
            "source_key": str(source.get("source_key") or ""),
            "source_type": str(source.get("source_type") or ""),
            "job_name": str(source.get("job_name") or ""),
            "credential_required": bool(
                source.get("credential_fields")
                or provider_name in _PROVIDER_CREDENTIAL_SECRET_NAMES
            ),
            "credential_configured": bool(credential_ok),
            "credential_source": credential_source,
            "credential_secret_names": list(credential_secret_names),
            "credential_error": credential_error,
            "telemetry_present": has_telemetry,
            "telemetry_ok": telemetry_ok,
            "age_s": age_value,
            "max_age_s": float(max_age_s),
            "last_ts_ms": telem.get("last_ts_ms"),
            "last_success_ts_ms": telem.get("last_success_ts_ms"),
            "last_success_age_s": telem.get("last_success_age_s"),
            "error_count": _int_or(telem.get("error_count") or source.get("error_count")),
            "circuit_open": circuit_open,
            "circuit_breaker_error_threshold": int(PROVIDER_CIRCUIT_BREAKER_ERRORS),
            "error": telemetry_error,
            "blockers": provider_blockers,
        }

    blockers = _dedupe_strs(blockers)
    ok = bool(not blockers)
    return {
        "ok": ok,
        "required": bool(enforced),
        "mode": mode_name,
        "reason": "ok" if ok else blockers[0],
        "blockers": blockers,
        "required_providers": list(required),
        "healthy_required": sum(1 for row in by_provider.values() if bool(row.get("ok"))),
        "total_required": len(required),
        "by_provider": by_provider,
        "manager_error": manager_error,
        "ts_ms": int(now),
    }


def _ingestion_source_definitions() -> Dict[str, Dict[str, Any]]:
    critical_sources = _csv_env_set("CRITICAL_INGESTION_SOURCES", "prices,options")
    enabled_jobs: set[str] = {
        str(name or "").strip()
        for name in str(os.environ.get("INGESTION_CHILD_JOBS", "") or "").split(",")
        if str(name or "").strip()
    }
    try:
        from services.data_source_manager import desired_ingestion_jobs

        enabled_jobs.update(
            str(name or "").strip()
            for name in (desired_ingestion_jobs(read_only=True) or [])
            if str(name or "").strip()
        )
    except Exception as e:
        _warn("health.enabled_ingestion_jobs", e)
    enabled_market_jobs: set[str] = set()
    try:
        from engine.data.provider_registry import get_enabled_market_data_job_names

        enabled_market_jobs = {
            str(name or "").strip()
            for name in (get_enabled_market_data_job_names() or [])
            if str(name or "").strip()
        }
    except Exception as e:
        _warn("health.enabled_market_jobs", e)
    news_poll_s = max(15.0, _float_or(os.environ.get("NEWS_POLL_SECONDS"), 120.0))
    gdelt_poll_s = max(30.0, _float_or(os.environ.get("GDELT_POLL_SECONDS"), 300.0))
    reddit_poll_s = max(15.0, _float_or(os.environ.get("SOCIAL_POLL_SLEEP_S"), 60.0))
    stocktwits_poll_s = max(15.0, _float_or(os.environ.get("SOCIAL_POLL_SLEEP_S"), 30.0))
    macro_poll_s = max(300.0, _float_or(os.environ.get("MACRO_POLL_SECONDS"), 21600.0))
    weather_forecast_poll_s = max(300.0, _float_or(os.environ.get("WEATHER_POLL_SECONDS"), 21600.0))
    weather_alerts_poll_s = max(60.0, _float_or(os.environ.get("WEATHER_ALERTS_POLL_SECONDS"), 900.0))
    sec_poll_s = max(300.0, _float_or(os.environ.get("SEC_POLL_SECONDS"), 900.0))
    form4_poll_s = max(300.0, _float_or(os.environ.get("FORM4_POLL_SECONDS"), 1800.0))
    price_poll_s = max(1.0, _float_or(os.environ.get("POLL_SECONDS"), 30.0))
    options_poll_s = max(30.0, _float_or(os.environ.get("OPTIONS_POLL_SECONDS"), 300.0))
    sec_components: List[Dict[str, Any]] = [
        {
            "name": "poll_sec_filings",
            "kind": "pipeline",
            "required": True,
            "expected_cadence_s": float(sec_poll_s),
            "stale_after_s": float(max(sec_poll_s * 3.0, 3600.0)),
        },
    ]
    if "ingest_form4" in enabled_jobs or "form4" in critical_sources:
        sec_components.append(
            {
                "name": "ingest_form4",
                "kind": "pipeline",
                "required": True,
                "expected_cadence_s": float(form4_poll_s),
                "stale_after_s": float(max(form4_poll_s * 3.0, 3600.0)),
            }
        )
    alt_data_aliases = {
        "alt",
        "alt_data",
        "alternative_data",
        "congressional",
        "quiver",
        "quiver_gov",
        "fundamentals",
        "fundamentals_pit",
        "13f",
        "inst_13f",
        "etf_flows",
        "finra",
        "finra_short",
        "cftc",
        "cftc_cot",
        "crypto_funding",
        "crypto_positioning",
    }
    alt_data_critical = bool(alt_data_aliases & critical_sources)
    alt_job_specs = [
        ("ingest_congressional_trades", "CONGRESSIONAL_POLL_SECONDS", 21600.0, {"congressional", "alt_data", "alt"}),
        ("ingest_quiver_gov", "QUIVER_GOV_POLL_SECONDS", 21600.0, {"quiver", "quiver_gov", "alt_data", "alt"}),
        ("ingest_fundamentals_pit", "FUNDAMENTALS_PIT_POLL_SECONDS", 86400.0, {"fundamentals", "fundamentals_pit", "alt_data", "alt"}),
        ("ingest_13f", "INST_13F_POLL_SECONDS", 86400.0, {"13f", "inst_13f", "alt_data", "alt"}),
        ("ingest_etf_flows", "ETF_FLOWS_POLL_SECONDS", 86400.0, {"etf_flows", "alt_data", "alt"}),
        ("ingest_finra_short_interest", "FINRA_SHORT_INTEREST_POLL_SECONDS", 86400.0, {"finra", "finra_short", "alt_data", "alt"}),
        ("ingest_finra_short_volume", "FINRA_SHORT_VOLUME_POLL_SECONDS", 86400.0, {"finra", "finra_short", "alt_data", "alt"}),
        ("ingest_cftc_cot", "CFTC_COT_POLL_SECONDS", 86400.0, {"cftc", "cftc_cot", "alt_data", "alt"}),
        ("ingest_crypto_funding", "CRYPTO_FUNDING_POLL_SECONDS", 28800.0, {"crypto_funding", "crypto_positioning", "alt_data", "alt"}),
    ]
    alt_components: List[Dict[str, Any]] = []
    alt_cadences: List[float] = []
    for job_name, env_name, default_s, aliases in alt_job_specs:
        include_job = job_name in enabled_jobs or bool(set(aliases) & critical_sources)
        if not include_job:
            continue
        cadence_s = max(300.0, _float_or(os.environ.get(env_name), float(default_s)))
        alt_cadences.append(float(cadence_s))
        alt_components.append(
            {
                "name": job_name,
                "kind": "pipeline",
                "required": True,
                "expected_cadence_s": float(cadence_s),
                "stale_after_s": float(max(cadence_s * 3.0, 3600.0)),
            }
        )
    alt_expected_cadence_s = min(alt_cadences) if alt_cadences else 86400.0
    alt_stale_after_s = max([cadence * 3.0 for cadence in alt_cadences] or [86400.0])
    return {
        "prices": {
            "critical": "prices" in critical_sources,
            "policy": "all_required",
            "expected_cadence_s": float(min(price_poll_s, float(HEALTH_PRICES_MAX_AGE_S))),
            "stale_after_s": float(HEALTH_PRICES_MAX_AGE_S),
            "components": [
                {
                    "name": "prices",
                    "kind": "derived_prices",
                    "required": True,
                    "expected_cadence_s": float(min(price_poll_s, float(HEALTH_PRICES_MAX_AGE_S))),
                    "stale_after_s": float(HEALTH_PRICES_MAX_AGE_S),
                },
            ],
        },
        "options": {
            "critical": ("options" in critical_sources and "options_poll" in enabled_market_jobs),
            "policy": "all_required",
            "expected_cadence_s": float(options_poll_s),
            "stale_after_s": float(HEALTH_OPTIONS_MAX_AGE_S),
            "components": [
                {
                    "name": "options_poll",
                    "kind": "derived_options",
                    "required": True,
                    "expected_cadence_s": float(options_poll_s),
                    "stale_after_s": float(HEALTH_OPTIONS_MAX_AGE_S),
                },
            ],
        },
        "news": {
            "critical": "news" in critical_sources,
            "policy": "all_required",
            "expected_cadence_s": float(news_poll_s),
            "stale_after_s": float(max(HEALTH_EVENTS_MAX_AGE_S, news_poll_s * 3.0)),
            "components": [
                {
                    "name": "ingest_now",
                    "kind": "pipeline",
                    "required": True,
                    "expected_cadence_s": float(news_poll_s),
                    "stale_after_s": float(max(HEALTH_EVENTS_MAX_AGE_S, news_poll_s * 3.0)),
                },
                {
                    "name": "poll_gdelt",
                    "kind": "pipeline",
                    "required": False,
                    "expected_cadence_s": float(gdelt_poll_s),
                    "stale_after_s": float(max(900.0, gdelt_poll_s * 3.0)),
                },
            ],
        },
        "social": {
            "critical": "social" in critical_sources,
            "policy": "any_fresh",
            "expected_cadence_s": float(min(reddit_poll_s, stocktwits_poll_s)),
            "stale_after_s": float(max(reddit_poll_s, stocktwits_poll_s) * 3.0),
            "components": [
                {
                    "name": "poll_social_reddit",
                    "kind": "pipeline",
                    "required": False,
                    "expected_cadence_s": float(reddit_poll_s),
                    "stale_after_s": float(max(180.0, reddit_poll_s * 3.0)),
                },
                {
                    "name": "poll_social_stocktwits",
                    "kind": "pipeline",
                    "required": False,
                    "expected_cadence_s": float(stocktwits_poll_s),
                    "stale_after_s": float(max(180.0, stocktwits_poll_s * 3.0)),
                },
            ],
        },
        "macro": {
            "critical": "macro" in critical_sources,
            "policy": "all_required",
            "expected_cadence_s": float(macro_poll_s),
            "stale_after_s": float(max(macro_poll_s * 2.0, 21600.0)),
            "components": [
                {
                    "name": "poll_macro",
                    "kind": "pipeline",
                    "required": True,
                    "expected_cadence_s": float(macro_poll_s),
                    "stale_after_s": float(max(macro_poll_s * 2.0, 21600.0)),
                },
            ],
        },
        "sec": {
            "critical": bool({"sec", "form4", "filings"} & critical_sources),
            "policy": "all_required",
            "expected_cadence_s": float(min(sec_poll_s, form4_poll_s)),
            "stale_after_s": float(max(sec_poll_s * 3.0, form4_poll_s * 3.0, 3600.0)),
            "components": sec_components,
        },
        "alt_data": {
            "critical": bool(alt_data_critical),
            "policy": "all_required",
            "expected_cadence_s": float(alt_expected_cadence_s),
            "stale_after_s": float(max(alt_stale_after_s, 3600.0)),
            "components": alt_components,
        },
        "weather": {
            "critical": "weather" in critical_sources,
            "policy": "all_required",
            "expected_cadence_s": float(min(weather_forecast_poll_s, weather_alerts_poll_s)),
            "stale_after_s": float(max(weather_forecast_poll_s * 2.0, weather_alerts_poll_s * 3.0)),
            "components": [
                {
                    "name": "poll_weather_forecasts",
                    "kind": "pipeline",
                    "required": True,
                    "expected_cadence_s": float(weather_forecast_poll_s),
                    "stale_after_s": float(max(weather_forecast_poll_s * 2.0, 21600.0)),
                },
                {
                    "name": "poll_weather_alerts",
                    "kind": "pipeline",
                    "required": True,
                    "expected_cadence_s": float(weather_alerts_poll_s),
                    "stale_after_s": float(max(weather_alerts_poll_s * 3.0, 1800.0)),
                },
            ],
        },
    }


def _component_freshness_snapshot(
    *,
    now_ms: int,
    source_name: str,
    component_spec: Dict[str, Any],
    pipeline_statuses: Dict[str, Dict[str, Any]],
    prices_snapshot: Optional[Dict[str, Any]] = None,
    options_snapshot: Optional[Dict[str, Any]] = None,
    ingestion_runtime_snapshot: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    component_name = str(component_spec.get("name") or "").strip()
    kind = str(component_spec.get("kind") or "pipeline").strip().lower()
    required = bool(component_spec.get("required"))
    expected_cadence_s = float(component_spec.get("expected_cadence_s") or 0.0)
    stale_after_s = float(component_spec.get("stale_after_s") or expected_cadence_s or 0.0)
    status: Dict[str, Any] = {}
    updated_ts_ms = 0
    last_ingested_ts_ms = 0
    ok_value: Optional[bool] = None
    extra_reason_codes: List[str] = []
    detail = ""

    if kind == "derived_prices":
        snap = dict(prices_snapshot or {})
        runtime = dict(ingestion_runtime_snapshot or {})
        updated_ts_ms = max(
            _int_or(snap.get("last_ts_ms")),
            _int_or(runtime.get("last_publish_ts_ms")),
        )
        last_ingested_ts_ms = _int_or(snap.get("last_ts_ms"))
        ok_value = bool(snap.get("ok"))
        detail = "prices_not_ok" if ok_value is False else ""
    elif kind == "derived_options":
        snap = dict(options_snapshot or {})
        status = dict(pipeline_statuses.get(component_name) or {})
        if not bool(snap.get("available", True)) and bool(snap.get("ok")):
            updated_ts_ms = max(
                _int_or(status.get("updated_ts_ms")),
                _int_or(snap.get("last_ingested_ts_ms")),
                int(now_ms),
            )
            last_ingested_ts_ms = max(
                _int_or(status.get("last_ingested_ts_ms")),
                _int_or(snap.get("last_ingested_ts_ms")),
            )
            ok_value = True
            detail = str(snap.get("detail") or "options_unavailable").strip()
            return {
                "name": component_name,
                "kind": kind,
                "required": required,
                "ok": True,
                "updated_ts_ms": int(updated_ts_ms) if updated_ts_ms > 0 else None,
                "last_ingested_ts_ms": int(last_ingested_ts_ms) if last_ingested_ts_ms > 0 else None,
                "last_update_ts_ms": int(updated_ts_ms) if updated_ts_ms > 0 else None,
                "expected_cadence_s": float(expected_cadence_s),
                "stale_after_s": float(stale_after_s),
                "freshness_lag_s": 0.0,
                "stale_threshold_breach": False,
                "stale": False,
                "failed": False,
                "detail": detail,
                "reason_codes": [],
            }
        updated_ts_ms = max(
            _int_or(status.get("updated_ts_ms")),
            _int_or(snap.get("last_ingested_ts_ms")),
        )
        last_ingested_ts_ms = max(
            _int_or(status.get("last_ingested_ts_ms")),
            _int_or(snap.get("last_ingested_ts_ms")),
        )
        stale_value = bool(snap.get("stale")) if "stale" in snap else None
        if "ok" in snap:
            ok_value = bool(snap.get("ok"))
        elif "ok" in status:
            ok_value = bool(status.get("ok"))
        elif stale_value:
            ok_value = False
        else:
            ok_value = bool(updated_ts_ms > 0)
        detail = str(snap.get("detail") or "").strip()
        if detail and not bool(snap.get("stale")):
            extra_reason_codes.append(f"ingestion_source_degraded:{source_name}:{component_name}:{detail}")
    else:
        status = dict(pipeline_statuses.get(component_name) or {})
        updated_ts_ms = _int_or(status.get("updated_ts_ms"))
        last_ingested_ts_ms = _int_or(status.get("last_ingested_ts_ms"))
        if status:
            ok_value = bool(status.get("ok"))
        detail = str(status.get("last_error") or "").strip()

    freshness_ts_ms = int(updated_ts_ms or last_ingested_ts_ms or 0)
    lag_s = None if freshness_ts_ms <= 0 else round(max(0, now_ms - freshness_ts_ms) / 1000.0, 1)
    stale_threshold_breach = bool(
        freshness_ts_ms <= 0
        or (now_ms - freshness_ts_ms) > int(max(1.0, stale_after_s) * 1000.0)
    )
    failed = bool(ok_value is False and not stale_threshold_breach)

    reason_codes: List[str] = list(extra_reason_codes)
    if freshness_ts_ms <= 0:
        reason_codes.append(f"ingestion_source_missing:{source_name}:{component_name}")
    elif stale_threshold_breach:
        reason_codes.append(f"ingestion_source_stale:{source_name}:{component_name}")
    if failed:
        reason_codes.append(f"ingestion_source_failed:{source_name}:{component_name}")

    return {
        "name": component_name,
        "kind": kind,
        "required": required,
        "ok": ok_value,
        "updated_ts_ms": int(updated_ts_ms) if updated_ts_ms > 0 else None,
        "last_ingested_ts_ms": int(last_ingested_ts_ms) if last_ingested_ts_ms > 0 else None,
        "last_update_ts_ms": int(freshness_ts_ms) if freshness_ts_ms > 0 else None,
        "expected_cadence_s": float(expected_cadence_s),
        "stale_after_s": float(stale_after_s),
        "freshness_lag_s": lag_s,
        "stale_threshold_breach": bool(stale_threshold_breach),
        "stale": bool(stale_threshold_breach),
        "failed": bool(failed),
        "detail": detail,
        "reason_codes": _dedupe_strs(reason_codes),
    }


def _build_ingestion_freshness_snapshot(
    *,
    now_ms: int,
    prices_snapshot: Optional[Dict[str, Any]],
    options_snapshot: Optional[Dict[str, Any]],
    ingestion_runtime_snapshot: Optional[Dict[str, Any]],
    pipeline_statuses: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    definitions = _ingestion_source_definitions()
    sources: Dict[str, Dict[str, Any]] = {}
    reason_codes: List[str] = []
    runtime_reason_codes: List[str] = []
    advisory_reason_codes: List[str] = []

    for source_name, source_spec in definitions.items():
        components = [
            _component_freshness_snapshot(
                now_ms=now_ms,
                source_name=source_name,
                component_spec=component_spec,
                pipeline_statuses=pipeline_statuses,
                prices_snapshot=prices_snapshot,
                options_snapshot=options_snapshot,
                ingestion_runtime_snapshot=ingestion_runtime_snapshot,
            )
            for component_spec in list(source_spec.get("components") or [])
        ]
        required_components = [row for row in components if bool(row.get("required"))] or list(components)
        policy = str(source_spec.get("policy") or "all_required").strip().lower()

        if not components:
            source_stale = bool(source_spec.get("critical"))
            source_last_update_ts_ms = 0
            source_lag_s = None
            source_failed = False
        elif policy == "any_fresh":
            fresh_components = [row for row in components if not bool(row.get("stale_threshold_breach"))]
            healthy_fresh_components = [row for row in fresh_components if not bool(row.get("failed"))]
            source_stale = len(fresh_components) == 0
            fresh_ts_values = [
                _int_or(row.get("last_update_ts_ms"))
                for row in (fresh_components or components)
                if _int_or(row.get("last_update_ts_ms")) > 0
            ]
            source_last_update_ts_ms = max(fresh_ts_values) if fresh_ts_values else 0
            source_lag_s = (
                None if source_last_update_ts_ms <= 0
                else round(max(0, now_ms - source_last_update_ts_ms) / 1000.0, 1)
            )
            source_failed = len(fresh_components) > 0 and len(healthy_fresh_components) == 0
        else:
            source_stale = any(bool(row.get("stale_threshold_breach")) for row in required_components)
            required_ts_values = [_int_or(row.get("last_update_ts_ms")) for row in required_components]
            source_last_update_ts_ms = (
                min(ts for ts in required_ts_values if ts > 0)
                if len([ts for ts in required_ts_values if ts > 0]) == len(required_components)
                else 0
            )
            source_lag_s = (
                None if source_last_update_ts_ms <= 0
                else round(max(0, now_ms - source_last_update_ts_ms) / 1000.0, 1)
            )
            source_failed = any(bool(row.get("failed")) for row in required_components)

        source_reason_codes: List[str] = []
        for row in components:
            source_reason_codes.extend(list(row.get("reason_codes") or []))
        if source_stale:
            source_reason_codes.append(f"ingestion_source_stale:{source_name}")
        elif source_failed:
            source_reason_codes.append(f"ingestion_source_degraded:{source_name}")

        if bool(source_spec.get("critical")) and source_stale:
            runtime_reason_codes.append(f"critical_source_stale:{source_name}")
        elif source_stale:
            advisory_reason_codes.append(f"source_stale:{source_name}")

        if source_failed:
            advisory_reason_codes.append(f"source_degraded:{source_name}")
            if bool(source_spec.get("critical")):
                runtime_reason_codes.append(f"critical_source_failed:{source_name}")

        latest_update_ts_ms = max(
            [_int_or(row.get("last_update_ts_ms")) for row in components if _int_or(row.get("last_update_ts_ms")) > 0] or [0]
        )
        sources[source_name] = {
            "source": source_name,
            "critical": bool(source_spec.get("critical")),
            "policy": policy,
            "ok": not source_stale and not source_failed,
            "status": ("stale" if source_stale else ("degraded" if source_failed else "ok")),
            "expected_cadence_s": float(source_spec.get("expected_cadence_s") or 0.0),
            "stale_after_s": float(source_spec.get("stale_after_s") or 0.0),
            "last_update_ts_ms": int(source_last_update_ts_ms) if source_last_update_ts_ms > 0 else None,
            "latest_update_ts_ms": int(latest_update_ts_ms) if latest_update_ts_ms > 0 else None,
            "freshness_lag_s": source_lag_s,
            "stale_threshold_breach": bool(source_stale),
            "stale": bool(source_stale),
            "failed": bool(source_failed),
            "reason_codes": _dedupe_strs(source_reason_codes),
            "pipeline_names": [str(row.get("name") or "") for row in components],
            "pipelines": {str(row.get("name") or ""): row for row in components},
        }
        reason_codes.extend(list(sources[source_name].get("reason_codes") or []))

    stale_sources = sorted(name for name, row in sources.items() if bool(row.get("stale")))
    failed_sources = sorted(name for name, row in sources.items() if bool(row.get("failed")))
    critical_sources = sorted(name for name, row in sources.items() if bool(row.get("critical")))
    stale_critical_sources = sorted(name for name in stale_sources if bool((sources.get(name) or {}).get("critical")))
    failed_critical_sources = sorted(name for name in failed_sources if bool((sources.get(name) or {}).get("critical")))

    return {
        "ok": len(stale_critical_sources) == 0 and len(failed_critical_sources) == 0,
        "critical_ok": len(stale_critical_sources) == 0 and len(failed_critical_sources) == 0,
        "all_sources_ok": len(stale_sources) == 0 and len(failed_sources) == 0,
        "degraded": len(stale_critical_sources) > 0 or len(failed_critical_sources) > 0,
        "critical_sources": critical_sources,
        "stale_sources": stale_sources,
        "failed_sources": failed_sources,
        "stale_critical_sources": stale_critical_sources,
        "failed_critical_sources": failed_critical_sources,
        "runtime_reason_codes": _dedupe_strs(runtime_reason_codes),
        "advisory_reason_codes": _dedupe_strs(advisory_reason_codes),
        "reason_codes": _dedupe_strs(reason_codes + runtime_reason_codes + advisory_reason_codes),
        "sources": sources,
    }


def _shared_ingestion_runtime_snapshot(con, now_ms: int, effective_prices_max_age_s: float) -> Dict[str, Any]:
    meta_state = _json_meta_get("ingestion_state")
    market_state = {}
    try:
        # Reuse the caller-owned read handle here. `connect_ro()` uses a
        # thread-local pooled connection, so opening a second IPC read in this
        # same thread and letting it close would invalidate the outer health
        # snapshot handle mid-request.
        market_state = dict(market_data_status(con=con) or {})
    except Exception as e:
        _warn("health.shared_ingestion_runtime.market_data_status", e)
        market_state = {}

    meta_market = _dict_or_empty(meta_state.get("market_state"))
    children = _dict_or_empty(meta_state.get("children"))
    hb_ts_ms = 0
    try:
        hb_row = con.execute(
            """
            SELECT ts_ms
            FROM job_heartbeats
            WHERE job_name = ?
            LIMIT 1
            """,
            ("ingestion_runtime",),
        ).fetchone()
        hb_ts_ms = _int_or((hb_row or [0])[0])
    except Exception:
        _warn("health.shared_ingestion_runtime.job_heartbeat", Exception("job_heartbeat_query_failed"))
        hb_ts_ms = 0

    running = bool(market_state.get("running")) or bool(meta_state.get("running")) or any(
        bool((row or {}).get("running")) for row in children.values() if isinstance(row, dict)
    )
    last_tick_ts_ms = max(
        _int_or(market_state.get("last_price_ts_ms")),
        _int_or(meta_market.get("last_price_ts_ms")),
        _int_or(meta_state.get("last_event_ts_ms")),
    )
    prices_table_last_ts_ms = 0
    max_age_ms = max(1, int(float(effective_prices_max_age_s) * 1000.0))
    try:
        row = con.execute(
            """
            SELECT MAX(ts_ms)
            FROM prices
            WHERE price IS NOT NULL
            """
        ).fetchone()
        prices_table_last_ts_ms = _int_or((row or [0])[0])
        last_tick_ts_ms = max(last_tick_ts_ms, prices_table_last_ts_ms)
    except Exception:
        _warn("health.shared_ingestion_runtime.last_price_query", Exception("last_price_query_failed"))
    price_age_ms = 10**12
    if last_tick_ts_ms > 0:
        price_age_ms = max(0, int(now_ms - last_tick_ts_ms))
    else:
        for candidate in (
            _int_or(market_state.get("price_age_ms")),
            _int_or(meta_market.get("price_age_ms")),
        ):
            if candidate > 0:
                price_age_ms = int(candidate)
                break
    stale = bool(last_tick_ts_ms <= 0 or price_age_ms > max_age_ms)
    healthy_providers = max(
        _int_or(market_state.get("healthy_providers")),
        _int_or(meta_market.get("healthy_providers")),
    )
    providers = _dict_or_empty(market_state.get("providers"))
    if not providers:
        providers = _dict_or_empty(meta_market.get("providers"))
    if healthy_providers <= 0 or not providers:
        try:
            rows = con.execute(
                """
                SELECT p.provider, p.ts_ms, p.ok, p.latency_ms, p.n_symbols, p.error
                FROM price_provider_health p
                INNER JOIN (
                    SELECT provider, MAX(ts_ms) AS max_ts_ms
                    FROM price_provider_health
                    GROUP BY provider
                ) latest
                  ON latest.provider = p.provider
                 AND latest.max_ts_ms = p.ts_ms
                """
            ).fetchall() or []
        except Exception:
            _warn("health.shared_ingestion_runtime.provider_health_query", Exception("provider_health_query_failed"))
            rows = []

        derived_providers: Dict[str, Any] = {}
        derived_healthy = 0
        for provider, ts_ms, ok, latency_ms, n_symbols, error in rows:
            provider_ts_ms = _int_or(ts_ms)
            provider_age_ms = max(0, int(now_ms - provider_ts_ms)) if provider_ts_ms > 0 else 10**12
            provider_ok = bool(_int_or(ok) == 1 and provider_age_ms <= max_age_ms)
            derived_providers[str(provider)] = {
                "ok": provider_ok,
                "age_ms": int(provider_age_ms),
                "last_ts_ms": provider_ts_ms,
                "latency_ms": (None if latency_ms is None else _int_or(latency_ms)),
                "n_symbols": _int_or(n_symbols),
                "error": (None if error is None else str(error)),
            }
            if provider_ok:
                derived_healthy += 1

        if derived_providers:
            providers = derived_providers
            healthy_providers = max(healthy_providers, derived_healthy)

    if hb_ts_ms > 0 and (now_ms - hb_ts_ms) <= max(int(HEALTH_JOBS_MAX_STALE_S * 1000.0), max_age_ms):
        running = True

    return {
        "running": running,
        "stale": stale,
        "healthy_providers": healthy_providers,
        "last_tick_ts_ms": last_tick_ts_ms,
        "last_publish_ts_ms": max(
            _int_or(market_state.get("updated_ts_ms"), _int_or(meta_state.get("ts_ms"))),
            int(hb_ts_ms),
        ),
        "price_age_ms": int(price_age_ms),
        "max_price_age_ms": int(max_age_ms),
        "active_child": str(market_state.get("active_child") or ""),
        "child_pid": _int_or(market_state.get("child_pid")),
        "children": children,
        "providers": providers,
        "provider_status": str(meta_state.get("provider_status") or ""),
        "last_error": str(meta_state.get("last_error") or ""),
        "source": "shared_runtime_meta+ipc",
    }


def _effective_prices_max_age_s(con) -> float:
    # Polling-only providers need a looser freshness budget than streaming feeds,
    # so health derives an effective threshold from heartbeat capabilities.
    effective_s = float(HEALTH_PRICES_MAX_AGE_S)
    try:
        rows = con.execute(
            """
            SELECT extra_json
            FROM job_heartbeats
            WHERE job_name != ?
            """,
            ("ingestion_runtime",),
        ).fetchall() or []
    except Exception:
        rows = []

    for row in rows:
        if isinstance(row, dict):
            extra_json = row.get("extra_json")
        else:
            try:
                extra_json = row["extra_json"]
            except Exception:
                extra_json = row[0] if isinstance(row, (tuple, list)) else row
        try:
            extra = json.loads(str(extra_json or "{}"))
            if not isinstance(extra, dict):
                continue
        except Exception as e:
            _warn("health.feed_status.extra_parse", e, extra_json=repr(extra_json))
            continue

        providers = extra.get("providers") if isinstance(extra.get("providers"), dict) else {}
        if not providers:
            continue

        has_streaming = False
        has_polling = False
        for provider_row in providers.values():
            if not isinstance(provider_row, dict):
                continue
            caps = _dict_or_empty(provider_row.get("capabilities"))
            if bool(caps.get("streaming")):
                has_streaming = True
            if bool(caps.get("polling")):
                has_polling = True

        poll_seconds = max(0.0, float(extra.get("poll_seconds") or 0.0))
        if has_polling and not has_streaming and poll_seconds > 0:
            effective_s = max(effective_s, max(poll_seconds * 2.5, 45.0))

    return float(effective_s)


# ---------------------------------------------------
# INTERNAL HELPERS
# ---------------------------------------------------

def _table_exists(con, table: str) -> bool:
    return _health_table_exists(con, table, warn=_warn)


def _get_table_cols(con, table: str):
    return _health_get_table_cols(con, table, warn=_warn)


def _get_index_names(con) -> set[str]:
    return _health_get_index_names(con, warn=_warn)


def _prediction_flow_snapshot(con, now_ms: int) -> Dict[str, Any]:
    max_age_s = float(HEALTH_PREDICTIONS_MAX_AGE_S)
    cutoff_ts_ms = int(now_ms - max(1.0, max_age_s) * 1000.0)
    out = {
        "ok": False,
        "count": 0,
        "recent_count": 0,
        "history_count": 0,
        "history_recent_count": 0,
        "last_ts_ms": None,
        "history_last_ts_ms": None,
        "age_s": None,
        "max_age_s": max_age_s,
        "detail": "",
        "ensemble": {
            "count": 0,
            "recent_count": 0,
            "last_ts": None,
            "age_s": None,
            "latest_weight_ts": None,
        },
    }

    if not _table_exists(con, "predictions"):
        out["detail"] = "predictions_table_missing"
        return out

    try:
        row = con.execute(
            """
            SELECT COUNT(*), MAX(ts_ms)
            FROM predictions
            """
        ).fetchone() or (0, None)
        out["count"] = int(row[0] or 0)
        last_ts_ms = int(row[1] or 0)
        out["last_ts_ms"] = last_ts_ms or None
    except Exception as e:
        out["detail"] = f"predictions_query_failed:{type(e).__name__}:{e}"
        _warn("health.predictions.query", e)
        return out

    try:
        row = con.execute(
            """
            SELECT COUNT(*)
            FROM predictions
            WHERE ts_ms >= ?
            """
            ,
            (int(cutoff_ts_ms),),
        ).fetchone() or (0,)
        out["recent_count"] = int(row[0] or 0)
    except Exception as e:
        out["detail"] = f"predictions_recent_query_failed:{type(e).__name__}:{e}"
        _warn("health.predictions.recent_query", e)
        return out

    history_last_ts_ms = 0
    if _table_exists(con, "prediction_history"):
        try:
            row = con.execute(
                """
                SELECT COUNT(*), MAX(ts_ms)
                FROM prediction_history
                """
            ).fetchone() or (0, None)
            out["history_count"] = int(row[0] or 0)
            history_last_ts_ms = int(row[1] or 0)
            out["history_last_ts_ms"] = history_last_ts_ms or None
        except Exception as e:
            out["detail"] = f"prediction_history_query_failed:{type(e).__name__}:{e}"
            _warn("health.prediction_history.query", e)
            return out

        try:
            row = con.execute(
                """
                SELECT COUNT(*)
                FROM prediction_history
                WHERE ts_ms >= ?
                """
                ,
                (int(cutoff_ts_ms),),
            ).fetchone() or (0,)
            out["history_recent_count"] = int(row[0] or 0)
        except Exception as e:
            out["detail"] = f"prediction_history_recent_query_failed:{type(e).__name__}:{e}"
            _warn("health.prediction_history.recent_query", e)
            return out

    effective_last_ts_ms = max(int(out["last_ts_ms"] or 0), int(history_last_ts_ms or 0))
    if effective_last_ts_ms > 0:
        out["age_s"] = round(max(0, now_ms - effective_last_ts_ms) / 1000.0, 1)

    if _table_exists(con, "ensemble_predictions"):
        try:
            row = con.execute(
                """
                SELECT COUNT(*), MAX(ts)
                FROM ensemble_predictions
                """
            ).fetchone() or (0, None)
            ensemble_last_ts = int(row[1] or 0)
            out["ensemble"]["count"] = int(row[0] or 0)
            out["ensemble"]["last_ts"] = ensemble_last_ts or None
            if ensemble_last_ts > 0:
                out["ensemble"]["age_s"] = round(max(0, now_ms - ensemble_last_ts) / 1000.0, 1)
        except Exception as e:
            _warn("health.ensemble_predictions.query", e)
        try:
            row = con.execute(
                """
                SELECT COUNT(*)
                FROM ensemble_predictions
                WHERE ts >= ?
                """
                ,
                (int(cutoff_ts_ms),),
            ).fetchone() or (0,)
            out["ensemble"]["recent_count"] = int(row[0] or 0)
        except Exception as e:
            _warn("health.ensemble_predictions.recent_query", e)
    if _table_exists(con, "ensemble_blend_weights"):
        try:
            row = con.execute(
                """
                SELECT MAX(created_ts)
                FROM ensemble_blend_weights
                """
            ).fetchone() or (None,)
            latest_weight_ts = int(row[0] or 0)
            out["ensemble"]["latest_weight_ts"] = latest_weight_ts or None
        except Exception as e:
            _warn("health.ensemble_blend_weights.query", e)

    if out["count"] <= 0:
        out["detail"] = "predictions_empty"
        return out

    if effective_last_ts_ms <= 0:
        out["detail"] = "predictions_missing_timestamp"
        return out

    if out["age_s"] is None or float(out["age_s"]) > max_age_s:
        out["detail"] = "predictions_stale"
        return out

    if int(out["recent_count"] or 0) <= 0 and int(out["history_recent_count"] or 0) <= 0:
        out["detail"] = "predictions_not_flowing"
        return out

    out["ok"] = True
    out["detail"] = "ok"
    return out


def get_startup_validation_snapshot(
    *,
    health: Optional[Dict[str, Any]] = None,
    db_validation: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    ts_ms = int(time.time() * 1000)
    health = dict(health or {})
    db_validation = dict(db_validation or {})

    if not health:
        try:
            health = dict(get_health_snapshot() or {})
        except Exception as e:
            health = {
                "ok": False,
                "error": f"health_snapshot_error:{type(e).__name__}:{e}",
                "reasons": [f"health_snapshot_error:{type(e).__name__}:{e}"],
            }

    if not db_validation:
        try:
            db_validation = dict(get_db_validation_snapshot() or {})
        except Exception as e:
            db_validation = {
                "ok": False,
                "error": f"db_validation_error:{type(e).__name__}:{e}",
                "missing_tables": [],
                "quick_check": "error",
            }

    from engine.runtime.startup_gates import evaluate_runtime_startup_gates

    gate_snapshot = evaluate_runtime_startup_gates(
        repo_root=Path(__file__).resolve().parents[2],
        health=health,
        db_validation=db_validation,
    )
    missing_tables = [str(x) for x in (db_validation.get("missing_tables") or []) if str(x).strip()]
    gates = dict(gate_snapshot.get("gates") or {})
    blocking_gates = [str(name) for name in list(gate_snapshot.get("blocking_gates") or []) if str(name).strip()]

    failed_gate_details = [
        dict(gates.get(name) or {})
        for name in blocking_gates
        if isinstance(gates.get(name), dict)
    ]

    return {
        "ok": bool(gate_snapshot.get("ok")),
        "ts_ms": ts_ms,
        "mode": str(os.environ.get("ENGINE_MODE", "safe") or "safe").strip().lower() or "safe",
        "checks": gates,
        "gates": gates,
        "blocking_checks": blocking_gates,
        "blocking_gates": blocking_gates,
        "failed_gate_details": failed_gate_details,
        "reasons": _dedupe_strs(list(gate_snapshot.get("reasons") or [])),
        "critical_systems_missing": _dedupe_strs(list(gate_snapshot.get("impacted_components") or [])),
        "impacted_components": list(gate_snapshot.get("impacted_components") or []),
        "health_reasons": list(health.get("reasons") or []),
        "health_ok": bool(health.get("ok")),
        "config_contract": dict(gate_snapshot.get("config_contract") or {}),
        "config_errors": list(gate_snapshot.get("config_errors") or []),
        "db_validation": {
            "ok": bool(db_validation.get("ok")),
            "quick_check": str(db_validation.get("quick_check") or ""),
            "missing_tables": missing_tables,
            "missing_columns": dict(
                db_validation.get("missing_columns")
                or db_validation.get("missing_cols")
                or {}
            ),
            "missing_indexes": list(db_validation.get("missing_indexes") or []),
            "schema_version": db_validation.get("schema_version"),
            "expected_schema_version": db_validation.get("expected_schema_version"),
            "schema_version_ok": bool(db_validation.get("schema_version_ok", True)),
            "schema_status": str(db_validation.get("schema_status") or ""),
        },
    }


# ---------------------------------------------------
# SCHEMA AUDIT
# ---------------------------------------------------

def get_schema_audit():
    # Schema audit is stricter than ordinary health checks because it is used to
    # decide whether the runtime is structurally safe to operate.
    return _health_schema_audit(
        get_db_validation_snapshot=get_db_validation_snapshot,
        storage_schema_version=STORAGE_SCHEMA_VERSION,
        warn=_warn,
    )


# ---------------------------------------------------
# HEALTH SNAPSHOT
# ---------------------------------------------------

def _health_snapshot_pending_payload(
    *,
    now_ms: int,
    reason: str,
    cached_ts_ms: int = 0,
) -> Dict[str, Any]:
    return _health_pending_snapshot_payload(
        now_ms=now_ms,
        reason=reason,
        cached_ts_ms=cached_ts_ms,
        environ=os.environ,
    )


def _stale_health_snapshot_payload(payload: Dict[str, Any], *, now_ms: int, cached_ts_ms: int) -> Dict[str, Any]:
    return _health_stale_snapshot_payload(
        payload,
        now_ms=now_ms,
        cached_ts_ms=cached_ts_ms,
    )


def _new_health_snapshot_payload(now_ms: int) -> Dict[str, Any]:
    return _health_new_snapshot_payload(now_ms, db_path=Path(DB_PATH))


def _run_health_checks(ctx: HealthSnapshotContext, checks: Iterable[HealthSnapshotCheck]) -> None:
    _health_run_checks(ctx, checks, warn=_warn)


def _check_runtime_hardware(ctx: HealthSnapshotContext) -> None:
    out = ctx.out
    section_started = time.perf_counter()
    try:
        out["runtime_hardware"] = runtime_hardware_snapshot()
    except Exception as e:
        _warn("health.runtime_hardware", e)
        out["runtime_hardware"] = {
            "ok": False,
            "error": f"{type(e).__name__}: {e}",
        }
    _trace_section("runtime_hardware", section_started, ok=bool((out.get("runtime_hardware") or {}).get("ok")))


def _check_disk_pressure(ctx: HealthSnapshotContext) -> None:
    out = ctx.out
    section_started = time.perf_counter()
    try:
        out["disk_pressure"] = get_disk_pressure_snapshot()
    except Exception as e:
        _warn("health.disk_pressure", e)
        out["disk_pressure"] = {
            "ok": False,
            "status": "error",
            "critical": [f"disk_pressure_error:{type(e).__name__}:{e}"],
            "warnings": [],
            "paths": [],
        }
    _trace_section("disk_pressure", section_started, ok=bool((out.get("disk_pressure") or {}).get("ok")))


def _check_db(ctx: HealthSnapshotContext) -> None:
    con = ctx.con
    out = ctx.out
    section_started = time.perf_counter()
    try:
        if DB_PATH.exists():
            out["db"]["exists"] = True
            out["db"]["size_bytes"] = int(DB_PATH.stat().st_size)
        wal_path = _sqlite_wal_path(DB_PATH)
        if wal_path is not None:
            if wal_path.exists():
                out["db"]["wal_bytes"] = int(wal_path.stat().st_size)

        if HEALTH_RUN_QUICK_CHECK:
            row = con.execute("PRAGMA quick_check;").fetchone()
            if row:
                qc = str(row[0])
                out["db"]["quick_check"] = qc
                if qc.lower() != "ok":
                    out["db"]["ok"] = False
        else:
            out["db"]["quick_check"] = "skipped"

        out["db"]["initialized"] = bool(
            ctx.table_exists("schema_version")
            or ctx.table_exists("prices")
            or ctx.table_exists("job_locks")
        )
    except Exception as e:
        out["db"]["ok"] = False
        out["db"]["error"] = str(e)
    _trace_section("db", section_started, ok=bool((out.get("db") or {}).get("ok")))


def _check_event_log(ctx: HealthSnapshotContext) -> None:
    now_ms = ctx.now_ms
    out = ctx.out
    section_started = time.perf_counter()
    try:
        event_log_summary = dict(fetch_event_log_summary() or {})
        last_ts_ms = event_log_summary.get("last_ts_ms")
        age_s = None if not last_ts_ms else round((now_ms - int(last_ts_ms)) / 1000.0, 1)
        out["event_log"] = {
            "ok": bool(event_log_summary.get("ok")),
            "count": int(event_log_summary.get("count") or 0),
            "last_ts_ms": (int(last_ts_ms) if last_ts_ms is not None else None),
            "age_s": age_s,
        }
    except Exception as e:
        _warn("health.event_log", e)
        out["event_log"] = {
            "ok": False,
            "count": 0,
            "last_ts_ms": None,
            "age_s": None,
        }
    _trace_section("event_log", section_started, ok=bool((out.get("event_log") or {}).get("ok")))


def _check_prices(ctx: HealthSnapshotContext) -> None:
    con = ctx.con
    now_ms = ctx.now_ms
    out = ctx.out
    section_started = time.perf_counter()
    try:
        effective_prices_max_age_s = _effective_prices_max_age_s(con)
        ctx.scratch["effective_prices_max_age_s"] = effective_prices_max_age_s
        row = con.execute("SELECT MAX(ts_ms) FROM prices").fetchone()
        if row and row[0]:
            age_s = (now_ms - int(row[0])) / 1000.0
            out["prices"] = {
                "ok": age_s < effective_prices_max_age_s,
                "age_s": round(age_s, 1),
                "max_age_s": round(effective_prices_max_age_s, 1),
                "last_ts_ms": int(row[0]),
            }
        else:
            out["prices"] = {
                "ok": False,
                "age_s": None,
                "max_age_s": round(effective_prices_max_age_s, 1),
                "last_ts_ms": None,
            }
    except Exception as e:
        _warn("health.prices", e)
        ctx.scratch["effective_prices_max_age_s"] = HEALTH_PRICES_MAX_AGE_S
        out["prices"] = {
            "ok": False,
            "age_s": None,
            "max_age_s": HEALTH_PRICES_MAX_AGE_S,
            "last_ts_ms": None,
        }
    _trace_section("prices", section_started, ok=bool((out.get("prices") or {}).get("ok")))


def _check_events(ctx: HealthSnapshotContext) -> None:
    con = ctx.con
    now_ms = ctx.now_ms
    out = ctx.out
    section_started = time.perf_counter()
    try:
        row = con.execute("SELECT MAX(ts_ms) FROM events").fetchone()
        if row and row[0]:
            age_s = (now_ms - int(row[0])) / 1000.0
            out["events"] = {
                "ok": age_s < HEALTH_EVENTS_MAX_AGE_S,
                "age_s": round(age_s, 1),
                "max_age_s": HEALTH_EVENTS_MAX_AGE_S,
                "last_ts_ms": int(row[0]),
            }
        else:
            out["events"] = {
                "ok": False,
                "age_s": None,
                "max_age_s": HEALTH_EVENTS_MAX_AGE_S,
                "last_ts_ms": None,
            }
    except Exception as e:
        _warn("health.events", e)
        out["events"] = {
            "ok": False,
            "age_s": None,
            "max_age_s": HEALTH_EVENTS_MAX_AGE_S,
            "last_ts_ms": None,
        }
    _trace_section("events", section_started, ok=bool((out.get("events") or {}).get("ok")))


def _check_labels(ctx: HealthSnapshotContext) -> None:
    con = ctx.con
    out = ctx.out
    section_started = time.perf_counter()
    try:
        row = con.execute("SELECT COUNT(*) FROM labels").fetchone()
        label_n = int(row[0] or 0)
        out["labels"] = {
            "ok": label_n >= HEALTH_MIN_LABELS,
            "count": label_n,
            "min_required": HEALTH_MIN_LABELS,
        }
    except Exception as e:
        _warn("health.labels", e)
        out["labels"] = {
            "ok": False,
            "count": 0,
            "min_required": HEALTH_MIN_LABELS,
        }
    _trace_section("labels", section_started, ok=bool((out.get("labels") or {}).get("ok")))


def _check_model(ctx: HealthSnapshotContext) -> None:
    con = ctx.con
    out = ctx.out
    section_started = time.perf_counter()
    try:
        row = con.execute("SELECT SUM(n) FROM model_stats_regime").fetchone()
        model_n = int(row[0] or 0)
        out["model"] = {
            "ok": model_n >= HEALTH_MIN_MODEL_SUPPORT,
            "support_n": model_n,
            "min_required": HEALTH_MIN_MODEL_SUPPORT,
        }
    except Exception as e:
        _warn("health.model", e)
        out["model"] = {
            "ok": False,
            "support_n": 0,
            "min_required": HEALTH_MIN_MODEL_SUPPORT,
        }
    _trace_section("model", section_started, ok=bool((out.get("model") or {}).get("ok")))


def _check_competition(ctx: HealthSnapshotContext) -> None:
    out = ctx.out
    section_started = time.perf_counter()
    try:
        out["competition"] = _competition_health_snapshot(ctx.now_ms)
    except Exception:
        out["competition"] = {
            "ok": False,
            "reasons": ["competition_health_error"],
            "replay_status": "error",
            "cycle_status": "error",
        }
    _trace_section("competition", section_started, ok=bool((out.get("competition") or {}).get("ok")))


def _check_attribution(ctx: HealthSnapshotContext) -> None:
    out = ctx.out
    section_started = time.perf_counter()
    try:
        out["attribution"] = _attribution_health_snapshot(ctx.con, ctx.now_ms)
    except Exception:
        out["attribution"] = {
            "ok": False,
            "authoritative_model_ratio": 0.0,
            "authoritative_model_min_ratio": float(HEALTH_ATTRIBUTION_MIN_RATIO),
        }
    _trace_section("attribution", section_started, ok=bool((out.get("attribution") or {}).get("ok")))


def _check_position_reconcile(ctx: HealthSnapshotContext) -> None:
    out = ctx.out
    section_started = time.perf_counter()
    try:
        out["position_reconcile"] = _latest_position_reconcile_snapshot(ctx.con, ctx.now_ms)
    except Exception as e:
        _warn("health.position_reconcile", e)
        out["position_reconcile"] = {
            "available": False,
            "ok": True,
            "fatal_reconcile": False,
            "status": "error",
            "detail": f"position_reconcile_error:{type(e).__name__}:{e}",
        }
    _trace_section(
        "position_reconcile",
        section_started,
        ok=bool((out.get("position_reconcile") or {}).get("ok")),
        available=bool((out.get("position_reconcile") or {}).get("available")),
    )


def _check_training(ctx: HealthSnapshotContext) -> None:
    out = ctx.out
    section_started = time.perf_counter()
    try:
        lifecycle = dict(_lc_get_state() or {})
        runtime_state = str(lifecycle.get("state") or "").strip().upper()
        mode_name = str(os.environ.get("ENGINE_MODE", "safe") or "safe").strip().lower() or "safe"
        training_allowed = mode_name not in ("live", "shadow")
        out["training"] = {
            "mode": "enabled" if training_allowed else "restricted",
            "allowed": bool(training_allowed),
            "reason": "" if training_allowed else "engine_mode_restrictive",
            "source": "health_fast_path",
            "runtime_state": runtime_state,
        }
    except Exception:
        out["training"] = {"mode": "unknown", "allowed": False}
    _trace_section("training", section_started, allowed=bool((out.get("training") or {}).get("allowed")))


def _check_job_heartbeats(ctx: HealthSnapshotContext) -> None:
    con = ctx.con
    now_ms = ctx.now_ms
    out = ctx.out
    section_started = time.perf_counter()
    try:
        row = con.execute(
            "SELECT job_name, MAX(heartbeat_ts_ms) FROM job_locks GROUP BY job_name"
        ).fetchall() or []

        jobs = {}
        for job_name, hb_ts in row:
            if not hb_ts:
                continue
            age_s = (now_ms - int(hb_ts)) / 1000.0
            ok = age_s < HEALTH_JOBS_MAX_STALE_S
            jobs[str(job_name)] = {
                "ok": ok,
                "age_s": round(age_s, 1),
                "max_age_s": HEALTH_JOBS_MAX_STALE_S,
                "last_heartbeat_ts_ms": int(hb_ts),
                "running": True,
                "source": "job_locks",
            }

        out["jobs"] = jobs

    except Exception:
        out["jobs"] = {}
    _trace_section("jobs", section_started, count=len(out.get("jobs") or {}))


def _check_ingestion_runtime_and_sources(ctx: HealthSnapshotContext) -> None:
    con = ctx.con
    now_ms = ctx.now_ms
    out = ctx.out
    effective_prices_max_age_s = float(
        ctx.scratch.get("effective_prices_max_age_s") or _effective_prices_max_age_s(con)
    )
    ctx.scratch["effective_prices_max_age_s"] = effective_prices_max_age_s

    section_started = time.perf_counter()
    out["ingestion_runtime"] = _shared_ingestion_runtime_snapshot(
        con,
        now_ms=now_ms,
        effective_prices_max_age_s=effective_prices_max_age_s,
    )
    _trace_section(
        "ingestion_runtime",
        section_started,
        running=bool((out.get("ingestion_runtime") or {}).get("running")),
        stale=bool((out.get("ingestion_runtime") or {}).get("stale")),
    )
    section_started = time.perf_counter()
    try:
        out["ingestion_pipelines"] = pipeline_health_summary(
            stale_after_s=max(float(HEALTH_EVENTS_MAX_AGE_S), 900.0)
        )
    except Exception as e:
        _warn("health.ingestion_pipelines", e)
        out["ingestion_pipelines"] = {"ok": False, "total": 0, "healthy": 0, "stale": 0, "pipelines": {}}
    _trace_section("ingestion_pipelines", section_started, ok=bool((out.get("ingestion_pipelines") or {}).get("ok")))
    section_started = time.perf_counter()
    try:
        out["options_ingestion"] = _options_ingestion_snapshot(now_ms)
    except Exception as e:
        _warn("health.options_ingestion", e)
        out["options_ingestion"] = {
            "ok": False,
            "available": False,
            "degraded": True,
            "critical": True,
            "status": "error",
            "detail": "options_ingestion_health_error",
        }
    _trace_section("options_ingestion", section_started, ok=bool((out.get("options_ingestion") or {}).get("ok")))
    section_started = time.perf_counter()
    try:
        pipeline_statuses = get_all_pipeline_statuses()
    except Exception as e:
        _warn("health.ingestion_source_statuses", e)
        pipeline_statuses = {}
    try:
        out["ingestion_freshness"] = _build_ingestion_freshness_snapshot(
            now_ms=now_ms,
            prices_snapshot=dict(out.get("prices") or {}),
            options_snapshot=dict(out.get("options_ingestion") or {}),
            ingestion_runtime_snapshot=dict(out.get("ingestion_runtime") or {}),
            pipeline_statuses=dict(pipeline_statuses or {}),
        )
        out["ingestion_sources"] = dict((out.get("ingestion_freshness") or {}).get("sources") or {})
    except Exception as e:
        _warn("health.ingestion_freshness", e)
        out["ingestion_freshness"] = {
            "ok": False,
            "critical_ok": False,
            "all_sources_ok": False,
            "degraded": True,
            "critical_sources": ["prices", "options"],
            "stale_sources": ["prices", "options"],
            "failed_sources": [],
            "stale_critical_sources": ["prices", "options"],
            "failed_critical_sources": [],
            "runtime_reason_codes": ["critical_source_stale:prices", "critical_source_stale:options"],
            "advisory_reason_codes": [],
            "reason_codes": ["critical_source_stale:prices", "critical_source_stale:options"],
            "sources": {},
        }
        out["ingestion_sources"] = {}
    _trace_section(
        "ingestion_freshness",
        section_started,
        ok=bool((out.get("ingestion_freshness") or {}).get("ok")),
        critical_ok=bool((out.get("ingestion_freshness") or {}).get("critical_ok")),
    )
    try:
        jobs = dict(out.get("jobs") or {})
        ingestion_runtime = dict(out.get("ingestion_runtime") or {})
        if bool(ingestion_runtime.get("running")) and not jobs.get("ingestion_runtime"):
            age_s = None
            last_publish_ts_ms = _int_or(ingestion_runtime.get("last_publish_ts_ms"))
            if last_publish_ts_ms > 0:
                age_s = round((now_ms - last_publish_ts_ms) / 1000.0, 1)
            jobs["ingestion_runtime"] = {
                "ok": not bool(ingestion_runtime.get("stale")),
                "age_s": age_s,
                "max_age_s": HEALTH_JOBS_MAX_STALE_S,
                "last_heartbeat_ts_ms": last_publish_ts_ms or None,
                "running": True,
                "source": "shared_ingestion_runtime",
            }
            out["jobs"] = jobs
    except Exception as e:
        _warn("health.jobs.backfill_ingestion_runtime", e)
    try:
        jobs = dict(out.get("jobs") or {})
        stale_jobs = sorted(
            name for name, row in jobs.items() if not bool((row or {}).get("ok"))
        )
        required_job_names = ["ingestion_runtime"]
        required_missing = sorted(name for name in required_job_names if name not in jobs)
        required_stale = sorted(
            name for name in required_job_names if name in jobs and not bool((jobs.get(name) or {}).get("ok"))
        )
        out["job_summary"] = {
            "total": len(jobs),
            "stale": len(stale_jobs),
            "stale_jobs": stale_jobs,
            "required_jobs": required_job_names,
            "required_missing": required_missing,
            "required_stale": required_stale,
            "ok_raw": len(stale_jobs) == 0,
            "ok": len(required_missing) == 0 and len(required_stale) == 0,
        }
    except Exception as e:
        _warn("health.job_summary", e)
        out["job_summary"] = {
            "total": 0,
            "stale": 0,
            "stale_jobs": [],
            "required_jobs": ["ingestion_runtime"],
            "required_missing": ["ingestion_runtime"],
            "required_stale": [],
            "ok_raw": False,
            "ok": False,
        }


def _check_provider_health(ctx: HealthSnapshotContext) -> None:
    con = ctx.con
    now_ms = ctx.now_ms
    out = ctx.out
    effective_prices_max_age_s = float(
        ctx.scratch.get("effective_prices_max_age_s") or _effective_prices_max_age_s(con)
    )
    ctx.scratch["effective_prices_max_age_s"] = effective_prices_max_age_s

    section_started = time.perf_counter()
    try:
        providers = {}
        healthy_n = 0
        ingestion_runtime = dict(out.get("ingestion_runtime") or {})
        runtime_mode_name = str(os.environ.get("ENGINE_MODE", "safe") or "safe").strip().lower() or "safe"
        strict_provider_telemetry = runtime_mode_name in ("paper", "shadow", "live")

        for row in fetch_provider_health_rows():
            ts_ms = row.get("ts_ms")
            if ts_ms is None:
                continue
            age_s = (now_ms - int(ts_ms)) / 1000.0
            provider_name = str(row.get("provider") or "")
            provider_meta = None
            provider_fatal = None
            try:
                provider_meta_raw = str(meta_get(f"provider_session_{provider_name}_last_failure", "") or "").strip()
                if provider_meta_raw:
                    provider_meta = json.loads(provider_meta_raw)
            except Exception as e:
                _warn("health.providers.session_last_failure", e, provider=provider_name)
                provider_meta = None
            try:
                provider_fatal_raw = str(meta_get(f"provider_session_{provider_name}_fatal", "") or "").strip()
                if provider_fatal_raw:
                    provider_fatal = json.loads(provider_fatal_raw)
            except Exception as e:
                _warn("health.providers.session_fatal", e, provider=provider_name)
                provider_fatal = None
            error_count = _int_or(row.get("error_count"))
            last_success_ts_ms = _int_or(row.get("last_success_ts_ms"))
            last_success_age_s = (
                round(max(0, now_ms - int(last_success_ts_ms)) / 1000.0, 1)
                if last_success_ts_ms > 0
                else None
            )
            latest_row_ok = bool(row.get("ok")) and age_s < effective_prices_max_age_s
            circuit_open = bool(
                int(PROVIDER_CIRCUIT_BREAKER_ERRORS) > 0
                and error_count >= int(PROVIDER_CIRCUIT_BREAKER_ERRORS)
                and not latest_row_ok
            )
            p_ok = bool(latest_row_ok and not provider_fatal and not circuit_open)
            providers[provider_name] = {
                "ok": p_ok,
                "age_s": round(age_s, 1),
                "last_ts_ms": int(ts_ms),
                "last_success_ts_ms": int(last_success_ts_ms) if last_success_ts_ms > 0 else None,
                "last_success_age_s": last_success_age_s,
                "error_count": int(error_count),
                "circuit_open": bool(circuit_open),
                "circuit_breaker_error_threshold": int(PROVIDER_CIRCUIT_BREAKER_ERRORS),
                "latency_ms": (
                    None
                    if row.get("latency_ms") is None
                    else int(float(row.get("latency_ms") or 0))
                ),
                "n_symbols": int(row.get("n_symbols") or 0),
                "error": (
                    None
                    if row.get("error") is None
                    else str(row.get("error"))
                ),
                "session_last_failure": provider_meta,
                "session_fatal": provider_fatal,
            }
            if p_ok:
                healthy_n += 1

        if healthy_n <= 0:
            shared_providers = _dict_or_empty(ingestion_runtime.get("providers"))
            for provider_name, provider_info in shared_providers.items():
                if not isinstance(provider_info, dict):
                    continue
                age_s = round(_int_or(provider_info.get("age_ms"), 10**12) / 1000.0, 1)
                circuit_open = bool(provider_info.get("circuit_open"))
                provider_fatal = provider_info.get("session_fatal")
                p_ok = bool(provider_info.get("ok")) and not circuit_open and not provider_fatal
                providers[str(provider_name)] = {
                    "ok": p_ok,
                    "age_s": age_s,
                    "last_ts_ms": _int_or(provider_info.get("last_ts_ms")),
                    "last_success_ts_ms": (
                        _int_or(provider_info.get("last_success_ts_ms"))
                        if provider_info.get("last_success_ts_ms") is not None
                        else None
                    ),
                    "last_success_age_s": (
                        round(_int_or(provider_info.get("last_success_age_ms")) / 1000.0, 1)
                        if provider_info.get("last_success_age_ms") is not None
                        else None
                    ),
                    "error_count": _int_or(provider_info.get("error_count")),
                    "circuit_open": circuit_open,
                    "circuit_breaker_error_threshold": int(PROVIDER_CIRCUIT_BREAKER_ERRORS),
                    "latency_ms": (_int_or(provider_info.get("latency_ms")) if provider_info.get("latency_ms") is not None else None),
                    "n_symbols": _int_or(provider_info.get("n_symbols")),
                    "error": (None if provider_info.get("error") is None else str(provider_info.get("error"))),
                    "session_last_failure": provider_info.get("session_last_failure"),
                    "session_fatal": provider_fatal,
                }
                if p_ok:
                    healthy_n += 1

        if healthy_n <= 0:
            try:
                price_row = con.execute(
                    "SELECT MAX(ts_ms) FROM prices WHERE price IS NOT NULL"
                ).fetchone()
                last_price_ts_ms = int((price_row or [0])[0] or 0)
                if last_price_ts_ms > 0:
                    price_age_s = (now_ms - int(last_price_ts_ms)) / 1000.0
                    derived_ok = bool((out.get("prices") or {}).get("ok"))
                    counts_as_healthy = bool(derived_ok and not strict_provider_telemetry)
                    providers["derived_from_prices"] = {
                        "ok": counts_as_healthy,
                        "age_s": round(price_age_s, 1),
                        "last_ts_ms": int(last_price_ts_ms),
                        "synthetic": True,
                        "strict_provider_telemetry": bool(strict_provider_telemetry),
                        "latency_ms": None,
                        "n_symbols": 0,
                        "error": None if counts_as_healthy else "provider_health_missing",
                        "session_last_failure": None,
                        "session_fatal": None,
                    }
                    if counts_as_healthy:
                        healthy_n = max(healthy_n, 1)
            except Exception as e:
                _warn("health.providers.derived_from_prices", e)

        out["providers"] = {
            "ok": healthy_n > 0,
            "healthy": healthy_n,
            "total": len(providers),
            "active_provider": str(meta_get("price_provider_active", "") or ""),
            "by_provider": providers,
        }
    except Exception as e:
        _warn("health.providers", e)
        out["providers"] = {
            "ok": False,
            "healthy": 0,
            "total": 0,
            "active_provider": "",
            "by_provider": {},
        }
    _trace_section(
        "providers",
        section_started,
        ok=bool((out.get("providers") or {}).get("ok")),
        healthy=int((out.get("providers") or {}).get("healthy") or 0),
    )


def _check_provider_readiness(ctx: HealthSnapshotContext) -> None:
    out = ctx.out
    section_started = time.perf_counter()
    try:
        out["provider_readiness"] = provider_readiness_snapshot(
            mode=os.environ.get("ENGINE_MODE", "safe"),
            health=out,
            now_ms=ctx.now_ms,
        )
    except Exception as e:
        _warn("health.provider_readiness", e)
        mode_name = str(os.environ.get("ENGINE_MODE", "safe") or "safe").strip().lower() or "safe"
        required = _provider_readiness_enforced(mode_name)
        out["provider_readiness"] = {
            "ok": not required,
            "required": bool(required),
            "mode": mode_name,
            "reason": "provider_readiness_error" if required else "not_required",
            "blockers": (["provider_readiness_error"] if required else []),
            "required_providers": [],
            "healthy_required": 0,
            "total_required": 0,
            "by_provider": {},
            "error": f"{type(e).__name__}:{e}",
            "ts_ms": int(ctx.now_ms),
        }
    _trace_section(
        "provider_readiness",
        section_started,
        ok=bool((out.get("provider_readiness") or {}).get("ok")),
        required=bool((out.get("provider_readiness") or {}).get("required")),
    )


def _check_portfolio(ctx: HealthSnapshotContext) -> None:
    con = ctx.con
    out = ctx.out
    section_started = time.perf_counter()
    try:
        row = con.execute("SELECT COUNT(*) FROM portfolio_state").fetchone()
        state_n = int(row[0] or 0)

        _mode = os.environ.get("ENGINE_MODE", "").strip().lower() or "safe"

        if _mode == "safe":
            out["portfolio"] = {
                "ok": True,
                "positions": state_n,
            }
        else:
            out["portfolio"] = {
                "ok": state_n > 0,
                "positions": state_n,
            }
    except Exception as e:
        _warn("health.portfolio", e)
        out["portfolio"] = {
            "ok": False,
            "positions": 0,
        }
    _trace_section("portfolio", section_started, ok=bool((out.get("portfolio") or {}).get("ok")))


def _check_execution_activity(ctx: HealthSnapshotContext) -> None:
    con = ctx.con
    now_ms = ctx.now_ms
    out = ctx.out
    section_started = time.perf_counter()
    try:
        fills_table = None
        if ctx.table_exists("broker_fills_v2"):
            fills_table = "broker_fills_v2"
        elif ctx.table_exists("broker_fills"):
            fills_table = "broker_fills"

        if fills_table:
            row = con.execute(
                f"SELECT COUNT(*), MAX(ts_ms) FROM {fills_table}"
            ).fetchone()
            n_fills = int((row or [0, None])[0] or 0)
            last_fill_ts_ms = (row or [0, None])[1]
            fill_age_s = None if not last_fill_ts_ms else round((now_ms - int(last_fill_ts_ms)) / 1000.0, 1)
            out["execution"] = {
                "ok": True,
                "fills_table": fills_table,
                "n_fills": n_fills,
                "last_fill_ts_ms": int(last_fill_ts_ms) if last_fill_ts_ms else None,
                "last_fill_age_s": fill_age_s,
            }
        else:
            out["execution"] = {
                "ok": False,
                "fills_table": None,
                "n_fills": 0,
                "last_fill_ts_ms": None,
                "last_fill_age_s": None,
            }
    except Exception:
        out["execution"] = {
            "ok": False,
            "fills_table": None,
            "n_fills": 0,
            "last_fill_ts_ms": None,
            "last_fill_age_s": None,
        }
    _trace_section("execution", section_started, ok=bool((out.get("execution") or {}).get("ok")))


def _check_execution_barrier(ctx: HealthSnapshotContext) -> None:
    con = ctx.con
    out = ctx.out
    section_started = time.perf_counter()
    try:
        if HEALTH_INCLUDE_EXECUTION_BARRIER:
            from engine.runtime.gates import execution_gate_snapshot
            from engine.api.internal_access import get_execution_mode as _get_execution_mode

            kill_switches = _read_kill_switch_snapshot_readonly(con=con)
            out["kill_switches"] = dict(kill_switches)
            snap = execution_gate_snapshot(
                get_execution_mode_fn=_get_execution_mode,
                kill_switches=kill_switches,
                risk_state_getter=lambda key, default="": _risk_state_value_readonly(con, str(key), str(default)),
            )
            if isinstance(snap, dict):
                out["execution_barrier"] = snap
            else:
                out["execution_barrier"] = {"allowed": False, "reason": "execution_barrier_invalid"}
        else:
            mode_name = str(os.environ.get("ENGINE_MODE", "safe") or "safe").strip().lower() or "safe"
            lifecycle = dict(_lc_get_state() or {})
            runtime_state = str(lifecycle.get("state") or "").strip().upper()
            runtime_detail = str(lifecycle.get("detail") or "").strip()
            allowed = mode_name == "live" and runtime_state == _LIVE
            out["execution_barrier"] = {
                "ok": True,
                "mode": mode_name,
                "armed": 0 if mode_name != "live" else None,
                "allow_execution": bool(allowed),
                "allowed": bool(allowed),
                "reason": "health_fast_path",
                "source": "engine_mode+lifecycle_state",
                "runtime_state": runtime_state,
                "runtime_detail": runtime_detail,
            }
    except Exception:
        out["execution_barrier"] = {"allowed": False, "reason": "execution_barrier_error"}
    _trace_section("execution_barrier", section_started, allowed=bool((out.get("execution_barrier") or {}).get("allowed")))


def _check_recent_errors(ctx: HealthSnapshotContext) -> None:
    con = ctx.con
    out = ctx.out
    section_started = time.perf_counter()
    try:
        rows = con.execute(
            """
            SELECT ts_ms, severity, message
            FROM alerts
            ORDER BY ts_ms DESC
            LIMIT 10
            """
        ).fetchall() or []

        out["recent_errors"] = [
            {
                "ts_ms": int(r[0] or 0),
                "severity": str(r[1] or ""),
                "message": str(r[2] or ""),
            }
            for r in rows
        ]
    except Exception:
        out["recent_errors"] = []
    _trace_section("recent_errors", section_started, count=len(out.get("recent_errors") or []))


def _check_predictions(ctx: HealthSnapshotContext) -> None:
    out = ctx.out
    section_started = time.perf_counter()
    try:
        out["predictions"] = _prediction_flow_snapshot(ctx.con, ctx.now_ms)
    except Exception as e:
        _warn("health.predictions", e)
        out["predictions"] = {
            "ok": False,
            "count": 0,
            "recent_count": 0,
            "history_count": 0,
            "history_recent_count": 0,
            "last_ts_ms": None,
            "history_last_ts_ms": None,
            "age_s": None,
            "max_age_s": float(HEALTH_PREDICTIONS_MAX_AGE_S),
            "detail": f"prediction_health_error:{type(e).__name__}:{e}",
        }
    _trace_section("predictions", section_started, ok=bool((out.get("predictions") or {}).get("ok")))


def _check_model_serving(ctx: HealthSnapshotContext) -> None:
    out = ctx.out
    section_started = time.perf_counter()
    try:
        out["model_serving"] = _model_serving_snapshot(ctx.con, ctx.now_ms)
    except Exception as e:
        _warn("health.model_serving", e)
        out["model_serving"] = {
            "ok": False,
            "degraded": False,
            "available": False,
            "sample_count": 0,
            "fallback_count": 0,
            "fallback_rate": 0.0,
            "detail": f"model_serving_error:{type(e).__name__}:{e}",
        }
    _trace_section("model_serving", section_started, ok=bool((out.get("model_serving") or {}).get("ok", True)))


def _check_alert_lifecycle(ctx: HealthSnapshotContext) -> None:
    out = ctx.out
    section_started = time.perf_counter()
    try:
        out["alert_lifecycle"] = _alert_lifecycle_snapshot(ctx.con, ctx.now_ms)
    except Exception as e:
        _warn("health.alert_lifecycle", e)
        out["alert_lifecycle"] = {
            "ok": False,
            "warning": False,
            "available": False,
            "recent_alerts": 0,
            "seen_count": 0,
            "consumed_count": 0,
            "expired_unconsumed_count": 0,
            "detail": f"alert_lifecycle_error:{type(e).__name__}:{e}",
        }
    _trace_section("alert_lifecycle", section_started, ok=bool((out.get("alert_lifecycle") or {}).get("ok", True)))


def _check_execution_supervisor(ctx: HealthSnapshotContext) -> None:
    out = ctx.out
    section_started = time.perf_counter()
    try:
        from engine.execution.execution_quality_supervisor import get_execution_quality_snapshot
        out["execution_supervisor"] = get_execution_quality_snapshot(readonly=True)
    except Exception:
        out["execution_supervisor"] = {
            "ok": False,
            "state": "unknown",
            "alerts": [],
            "score": 0.0,
        }
    _trace_section("execution_supervisor", section_started, ok=bool((out.get("execution_supervisor") or {}).get("ok")))


def _check_broker_connection(ctx: HealthSnapshotContext) -> None:
    out = ctx.out
    section_started = time.perf_counter()
    try:
        from engine.execution.execution_broker_watchdog import get_broker_connection_health
        out["broker_connection"] = get_broker_connection_health(readonly=True)
    except Exception:
        out["broker_connection"] = {
            "ok": False,
            "state": "unknown",
            "broker": os.environ.get("BROKER_NAME", os.environ.get("BROKER", "sim")),
        }
    _trace_section("broker_connection", section_started, ok=bool((out.get("broker_connection") or {}).get("ok")))


def _check_model_cache(ctx: HealthSnapshotContext) -> None:
    out = ctx.out
    section_started = time.perf_counter()
    try:
        from engine.runtime.model_cache import (
            get_snapshot as _get_model_cache_snapshot,
            warm_model_catalog as _warm_model_catalog,
        )

        model_cache_snapshot = dict(_get_model_cache_snapshot() or {})
        if (
            out["db"].get("initialized")
            and not bool(model_cache_snapshot.get("loaded"))
        ):
            model_cache_snapshot = dict(
                _warm_model_catalog(force=False, readonly=True) or _get_model_cache_snapshot() or {}
            )
        out["model_cache"] = model_cache_snapshot
    except Exception as e:
        _warn("health.model_cache", e)
        out["model_cache"] = {
            "ok": False,
            "loaded": False,
            "rows": 0,
            "last_error": f"model_cache_error:{type(e).__name__}:{e}",
            "ts_ms": int(ctx.now_ms),
        }
    _trace_section("model_cache", section_started, ok=bool((out.get("model_cache") or {}).get("ok")))


def _check_runtime_price_cache(ctx: HealthSnapshotContext) -> None:
    out = ctx.out
    section_started = time.perf_counter()
    try:
        from engine.runtime.price_cache import get_cache_health_snapshot

        out["runtime_price_cache"] = dict(
            get_cache_health_snapshot(stale_after_s=float(HEALTH_RUNTIME_PRICE_CACHE_MAX_AGE_S)) or {}
        )
    except Exception as e:
        _warn("health.runtime_price_cache", e)
        out["runtime_price_cache"] = {
            "ok": False,
            "initialized": False,
            "stale": True,
            "detail": f"runtime_price_cache_error:{type(e).__name__}:{e}",
            "ts_ms": int(ctx.now_ms),
        }
    _trace_section(
        "runtime_price_cache",
        section_started,
        ok=bool((out.get("runtime_price_cache") or {}).get("ok")),
    )


def _check_event_bus(ctx: HealthSnapshotContext) -> None:
    out = ctx.out
    section_started = time.perf_counter()
    try:
        from engine.runtime.event_bus import get_event_bus

        event_bus_stats = dict(get_event_bus().get_stats() or {})
        queue_size = int(event_bus_stats.get("queue_size") or 0)
        normal_dropped_count = int(event_bus_stats.get("normal_dropped_count") or event_bus_stats.get("dropped_count") or 0)
        critical_inline_dispatch_count = int(event_bus_stats.get("critical_inline_dispatch_count") or 0)
        critical_handler_failures = int(event_bus_stats.get("critical_handler_failures") or 0)
        critical_backpressure_active = bool(event_bus_stats.get("critical_backpressure_active"))
        normal_overflow_active = bool(event_bus_stats.get("normal_overflow_active"))
        avg_lag_ms = float(event_bus_stats.get("avg_dispatch_lag_ms") or 0.0)
        event_bus_ok = bool(
            event_bus_stats.get("started")
            and queue_size <= int(HEALTH_EVENT_BUS_MAX_QUEUE_DEPTH)
            and avg_lag_ms <= float(HEALTH_EVENT_BUS_MAX_LAG_MS)
            and not normal_overflow_active
            and not critical_backpressure_active
            and critical_handler_failures <= 0
        )
        detail_parts: List[str] = []
        if not bool(event_bus_stats.get("started")):
            detail_parts.append("event_bus_not_started")
        elif queue_size > int(HEALTH_EVENT_BUS_MAX_QUEUE_DEPTH) or avg_lag_ms > float(HEALTH_EVENT_BUS_MAX_LAG_MS):
            detail_parts.append(
                f"event_bus_backlog:queue_size={queue_size}:avg_dispatch_lag_ms={round(avg_lag_ms, 2)}"
            )
        if normal_overflow_active:
            detail_parts.append("event_bus_normal_overflow_active")
        if critical_backpressure_active:
            detail_parts.append("event_bus_critical_backpressure_active")
        if normal_dropped_count > 0:
            detail_parts.append(f"event_bus_normal_drops:{normal_dropped_count}")
        if critical_inline_dispatch_count > 0:
            detail_parts.append(f"event_bus_critical_backpressure:{critical_inline_dispatch_count}")
        if critical_handler_failures > 0:
            detail_parts.append(
                f"event_bus_critical_handler_failures:{critical_handler_failures}:"
                f"last_failed_event_type={str(event_bus_stats.get('last_failed_event_type') or '')}"
            )
        out["event_bus"] = {
            **event_bus_stats,
            "ok": bool(event_bus_ok),
            "detail": ("ok" if event_bus_ok else ";".join(detail_parts or ["event_bus_degraded"])),
            "max_queue_depth": int(HEALTH_EVENT_BUS_MAX_QUEUE_DEPTH),
            "max_lag_ms": float(HEALTH_EVENT_BUS_MAX_LAG_MS),
        }
    except Exception as e:
        _warn("health.event_bus", e)
        out["event_bus"] = {
            "ok": False,
            "started": False,
            "detail": f"event_bus_error:{type(e).__name__}:{e}",
            "queue_size": 0,
            "avg_dispatch_lag_ms": None,
            "ts_ms": int(ctx.now_ms),
        }
    _trace_section("event_bus", section_started, ok=bool((out.get("event_bus") or {}).get("ok")))


def _check_async_price_persistence(ctx: HealthSnapshotContext) -> None:
    out = ctx.out
    section_started = time.perf_counter()
    try:
        from engine.runtime.async_writer import get_async_writer

        out["async_price_persistence"] = dict(get_async_writer().get_snapshot() or {})
    except Exception as e:
        _warn("health.async_price_persistence", e)
        out["async_price_persistence"] = {
            "ok": False,
            "enabled": False,
            "detail": f"async_price_persistence_error:{type(e).__name__}:{e}",
            "ts_ms": int(ctx.now_ms),
        }
    _trace_section(
        "async_price_persistence",
        section_started,
        ok=bool((out.get("async_price_persistence") or {}).get("ok")),
    )


def _check_pg_price_storage(ctx: HealthSnapshotContext) -> None:
    out = ctx.out
    section_started = time.perf_counter()
    try:
        from engine.runtime.storage_pg_prices import get_price_storage

        out["pg_price_storage"] = dict(get_price_storage().get_snapshot() or {})
    except Exception as e:
        _warn("health.pg_price_storage", e)
        out["pg_price_storage"] = {
            "ok": False,
            "enabled": False,
            "detail": f"pg_price_storage_error:{type(e).__name__}:{e}",
            "ts_ms": int(ctx.now_ms),
        }
    _trace_section("pg_price_storage", section_started, ok=bool((out.get("pg_price_storage") or {}).get("ok")))


def _check_price_migration_validation(ctx: HealthSnapshotContext) -> None:
    out = ctx.out
    section_started = time.perf_counter()
    try:
        from engine.runtime.price_migration_validation import get_price_migration_validation_snapshot

        out["price_migration_validation"] = dict(get_price_migration_validation_snapshot() or {})
    except Exception as e:
        _warn("health.price_migration_validation", e)
        out["price_migration_validation"] = {
            "ok": False,
            "enabled": False,
            "detail": f"price_migration_validation_error:{type(e).__name__}:{e}",
            "ts_ms": int(ctx.now_ms),
        }
    _trace_section(
        "price_migration_validation",
        section_started,
        ok=bool((out.get("price_migration_validation") or {}).get("ok")),
    )


def _check_timeseries_storage(ctx: HealthSnapshotContext) -> None:
    out = ctx.out
    section_started = time.perf_counter()
    try:
        timeseries_storage = dict(get_timeseries_storage_snapshot() or {})
        feature_store_snapshot = dict(timeseries_storage.get("feature_store") or {})
        telemetry_append_buffer_snapshot = dict(timeseries_storage.get("telemetry_append_buffer") or {})
        telemetry_mirror_snapshot = dict(timeseries_storage.get("telemetry_mirror") or {})
        timescale_snapshot = dict(timeseries_storage)
        timescale_snapshot.pop("feature_store", None)
        timescale_snapshot.pop("telemetry_append_buffer", None)
        timescale_snapshot.pop("telemetry_mirror", None)
        out["timeseries_storage"] = timeseries_storage
        out["timescale"] = timescale_snapshot
        out["feature_store"] = feature_store_snapshot
        out["telemetry_append_buffer"] = telemetry_append_buffer_snapshot
        out["telemetry_mirror"] = telemetry_mirror_snapshot
    except Exception as e:
        _warn("health.timeseries_storage", e)
        error_detail = f"timeseries_storage_error:{type(e).__name__}:{e}"
        out["timeseries_storage"] = {
            "ok": False,
            "enabled": False,
            "degraded": True,
            "degraded_reasons": ["timeseries_storage_error"],
            "detail": error_detail,
        }
        out["timescale"] = {
            "ok": False,
            "enabled": False,
            "degraded": True,
            "degraded_reasons": ["timescale_error"],
            "detail": error_detail,
        }
        out["feature_store"] = {
            "ok": False,
            "enabled": False,
            "degraded": True,
            "degraded_reasons": ["feature_store_error"],
            "detail": error_detail,
        }
        out["telemetry_append_buffer"] = {
            "ok": False,
            "enabled": False,
            "degraded": True,
            "degraded_reasons": ["telemetry_append_buffer_error"],
            "detail": error_detail,
        }
        out["telemetry_mirror"] = {
            "ok": False,
            "enabled": False,
            "detail": error_detail,
        }
    _trace_section("timeseries_storage", section_started, ok=bool((out.get("timeseries_storage") or {}).get("ok")))


def _check_telemetry_migration_validation(ctx: HealthSnapshotContext) -> None:
    out = ctx.out
    section_started = time.perf_counter()
    try:
        from engine.runtime.telemetry_migration_validation import get_telemetry_migration_validation_snapshot

        out["telemetry_migration_validation"] = dict(get_telemetry_migration_validation_snapshot() or {})
    except Exception as e:
        _warn("health.telemetry_migration_validation", e)
        out["telemetry_migration_validation"] = {
            "ok": False,
            "enabled": False,
            "detail": f"telemetry_migration_validation_error:{type(e).__name__}:{e}",
            "ts_ms": int(ctx.now_ms),
        }
    _trace_section(
        "telemetry_migration_validation",
        section_started,
        ok=bool((out.get("telemetry_migration_validation") or {}).get("ok")),
    )


def _check_portfolio_runtime(ctx: HealthSnapshotContext) -> None:
    out = ctx.out
    section_started = time.perf_counter()
    try:
        out["portfolio_runtime"] = _portfolio_runtime_snapshot(con=ctx.con)
    except Exception as e:
        _warn("health.portfolio_runtime", e)
        out["portfolio_runtime"] = {
            "ok": False,
            "available": False,
            "degraded": False,
            "detail": f"portfolio_runtime_error:{type(e).__name__}:{e}",
            "updated_ts_ms": None,
            "degraded_reasons": [],
            "degraded_codes": [],
        }
    _trace_section("portfolio_runtime", section_started, ok=bool((out.get("portfolio_runtime") or {}).get("ok")))


def _check_execution_degraded(ctx: HealthSnapshotContext) -> None:
    out = ctx.out
    section_started = time.perf_counter()
    try:
        out["execution_degraded"] = _execution_degraded_snapshot(con=ctx.con)
    except Exception as e:
        _warn("health.execution_degraded", e)
        out["execution_degraded"] = {
            "active": True,
            "severity": "CRITICAL",
            "reason": f"execution_degraded_error:{type(e).__name__}:{e}",
            "reason_codes": ["execution_degraded_error"],
            "sources": [],
        }
    _trace_section(
        "execution_degraded",
        section_started,
        active=bool((out.get("execution_degraded") or {}).get("active")),
    )


def _check_execution_barrier_refresh(ctx: HealthSnapshotContext) -> None:
    out = ctx.out
    if not (HEALTH_INCLUDE_EXECUTION_BARRIER or bool((out.get("execution_degraded") or {}).get("active"))):
        return
    section_started = time.perf_counter()
    out["execution_barrier"] = _refresh_execution_barrier_snapshot(
        dict(out.get("execution_degraded") or {}),
        con=ctx.con,
    )
    _trace_section(
        "execution_barrier_refresh",
        section_started,
        allowed=bool((out.get("execution_barrier") or {}).get("allowed")),
    )


def _check_component_health(ctx: HealthSnapshotContext) -> None:
    out = ctx.out
    section_started = time.perf_counter()
    try:
        component_health = dict(get_component_health_snapshot() or {})
        out["component_health"] = component_health
        out["inference_runtime"] = dict(
            component_health.get("inference")
            or {
                "component": "inference",
                "ok": False,
                "status": "unknown",
                "detail": "component_health_unreported",
                "updated_ts_ms": None,
                "age_s": None,
                "stale": True,
            }
        )
        out["execution_runtime"] = dict(
            component_health.get("execution")
            or {
                "component": "execution",
                "ok": False,
                "status": "unknown",
                "detail": "component_health_unreported",
                "updated_ts_ms": None,
                "age_s": None,
                "stale": True,
            }
        )
        out["ingestion_observability"] = dict(
            component_health.get("ingestion")
            or {
                "component": "ingestion",
                "ok": False,
                "status": "unknown",
                "detail": "component_health_unreported",
                "updated_ts_ms": None,
                "age_s": None,
                "stale": True,
            }
        )
    except Exception as e:
        _warn("health.component_health", e)
        out["component_health"] = {}
        out["inference_runtime"] = {
            "component": "inference",
            "ok": False,
            "status": "error",
            "detail": f"component_health_error:{type(e).__name__}:{e}",
            "updated_ts_ms": None,
            "age_s": None,
            "stale": True,
        }
        out["execution_runtime"] = {
            "component": "execution",
            "ok": False,
            "status": "error",
            "detail": f"component_health_error:{type(e).__name__}:{e}",
            "updated_ts_ms": None,
            "age_s": None,
            "stale": True,
        }
        out["ingestion_observability"] = {
            "component": "ingestion",
            "ok": False,
            "status": "error",
            "detail": f"component_health_error:{type(e).__name__}:{e}",
            "updated_ts_ms": None,
            "age_s": None,
            "stale": True,
        }
    _trace_section(
        "component_health",
        section_started,
        inference_ok=bool((out.get("inference_runtime") or {}).get("ok")),
        execution_ok=bool((out.get("execution_runtime") or {}).get("ok")),
        ingestion_ok=bool((out.get("ingestion_observability") or {}).get("ok")),
    )


def _check_data_pipeline_gates(ctx: HealthSnapshotContext) -> None:
    out = ctx.out
    now_ms = ctx.now_ms
    section_started = time.perf_counter()
    try:
        out["feature_validation"] = dict(get_feature_validation_snapshot() or {})
        out["model_input_validation"] = dict(get_model_input_validation_snapshot() or {})
        out["scoring_pipeline"] = dict(get_scoring_pipeline_snapshot() or {})
        component_health = dict(out.get("component_health") or {})
        out["feature_runtime"] = dict(
            component_health.get("feature_engine")
            or {
                "component": "feature_engine",
                "ok": False,
                "status": "unknown",
                "detail": "component_health_unreported",
                "updated_ts_ms": None,
                "age_s": None,
                "stale": True,
            }
        )
        out["model_input_runtime"] = dict(
            component_health.get("model_inputs")
            or {
                "component": "model_inputs",
                "ok": False,
                "status": "unknown",
                "detail": "component_health_unreported",
                "updated_ts_ms": None,
                "age_s": None,
                "stale": True,
            }
        )
        out["scoring_runtime"] = dict(
            component_health.get("scoring_pipeline")
            or {
                "component": "scoring_pipeline",
                "ok": False,
                "status": "unknown",
                "detail": "component_health_unreported",
                "updated_ts_ms": None,
                "age_s": None,
                "stale": True,
            }
        )
        out["data_pipeline_gates"] = dict(
            build_data_pipeline_gate_snapshot(
                now_ms=now_ms,
                ingestion_runtime=dict(out.get("ingestion_runtime") or {}),
                ingestion_freshness=dict(out.get("ingestion_freshness") or {}),
            )
            or {}
        )
    except Exception as e:
        _warn("health.data_pipeline_gates", e)
        out["feature_validation"] = {}
        out["model_input_validation"] = {}
        out["scoring_pipeline"] = {}
        out["feature_runtime"] = {
            "component": "feature_engine",
            "ok": False,
            "status": "error",
            "detail": f"data_pipeline_gates_error:{type(e).__name__}:{e}",
            "updated_ts_ms": None,
            "age_s": None,
            "stale": True,
        }
        out["model_input_runtime"] = {
            "component": "model_inputs",
            "ok": False,
            "status": "error",
            "detail": f"data_pipeline_gates_error:{type(e).__name__}:{e}",
            "updated_ts_ms": None,
            "age_s": None,
            "stale": True,
        }
        out["scoring_runtime"] = {
            "component": "scoring_pipeline",
            "ok": False,
            "status": "error",
            "detail": f"data_pipeline_gates_error:{type(e).__name__}:{e}",
            "updated_ts_ms": None,
            "age_s": None,
            "stale": True,
        }
        out["data_pipeline_gates"] = {
            "ok": False,
            "updated_ts_ms": int(now_ms),
            "gates": {},
            "failed_gates": [
                "ingestion_active",
                "ingestion_not_stale",
                "critical_features_valid",
                "model_inputs_valid",
                "scoring_pipeline_operational",
            ],
            "detail": f"data_pipeline_gates_error:{type(e).__name__}:{e}",
        }
    _trace_section(
        "data_pipeline_gates",
        section_started,
        ok=bool((out.get("data_pipeline_gates") or {}).get("ok")),
        failed=len((out.get("data_pipeline_gates") or {}).get("failed_gates") or []),
    )


def _adjust_safe_mode_attribution(ctx: HealthSnapshotContext) -> None:
    out = ctx.out
    mode_name = str(os.environ.get("ENGINE_MODE", "safe") or "safe").strip().lower()
    attribution_snapshot = dict(out.get("attribution") or {})
    execution_snapshot = dict(out.get("execution") or {})
    attribution_rows = _int_or(attribution_snapshot.get("rows"))
    attribution_orphans = dict(attribution_snapshot.get("orphans") or {})
    attribution_state_present = bool(
        attribution_rows > 0
        or _int_or(attribution_orphans.get("orphan_row_count")) > 0
        or _int_or(attribution_orphans.get("snapshot_ts_ms")) > 0
    )
    execution_fills = _int_or(execution_snapshot.get("n_fills"))
    if mode_name == "safe" and (not attribution_state_present) and execution_fills <= 0:
        attribution_snapshot["ok"] = True
        attribution_snapshot["not_applicable"] = True
        attribution_snapshot["reason"] = "no_executed_fills"
        out["attribution"] = attribution_snapshot


def _check_startup_validation(ctx: HealthSnapshotContext) -> None:
    out = ctx.out
    section_started = time.perf_counter()
    try:
        out["startup_validation"] = get_startup_validation_snapshot(
            health=out,
            db_validation=get_db_validation_snapshot(include_quick_check=False),
        )
    except Exception as e:
        _warn("health.startup_validation", e)
        out["startup_validation"] = {
            "ok": False,
            "reasons": [f"startup_validation_error:{type(e).__name__}:{e}"],
            "blocking_checks": [
                "config_valid",
                "database_reachable",
                "schema_valid",
                "core_services_initialized",
            ],
            "blocking_gates": [
                "config_valid",
                "database_reachable",
                "schema_valid",
                "core_services_initialized",
            ],
            "critical_systems_missing": [],
        }
    _trace_section("startup_validation", section_started, ok=bool((out.get("startup_validation") or {}).get("ok")))


_HEALTH_SNAPSHOT_CHECKS: tuple[HealthSnapshotCheck, ...] = (
    HealthSnapshotCheck("runtime_hardware", _check_runtime_hardware),
    HealthSnapshotCheck("disk_pressure", _check_disk_pressure),
    HealthSnapshotCheck("db", _check_db),
    HealthSnapshotCheck("event_log", _check_event_log),
    HealthSnapshotCheck("prices", _check_prices),
    HealthSnapshotCheck("events", _check_events),
    HealthSnapshotCheck("labels", _check_labels),
    HealthSnapshotCheck("model", _check_model),
    HealthSnapshotCheck("competition", _check_competition),
    HealthSnapshotCheck("attribution", _check_attribution),
    HealthSnapshotCheck("position_reconcile", _check_position_reconcile),
    HealthSnapshotCheck("training", _check_training),
    HealthSnapshotCheck("jobs", _check_job_heartbeats),
    HealthSnapshotCheck("ingestion_runtime", _check_ingestion_runtime_and_sources),
    HealthSnapshotCheck("providers", _check_provider_health),
    HealthSnapshotCheck("provider_readiness", _check_provider_readiness),
    HealthSnapshotCheck("portfolio", _check_portfolio),
    HealthSnapshotCheck("execution", _check_execution_activity),
    HealthSnapshotCheck("execution_barrier", _check_execution_barrier),
    HealthSnapshotCheck("recent_errors", _check_recent_errors),
    HealthSnapshotCheck("predictions", _check_predictions),
    HealthSnapshotCheck("model_serving", _check_model_serving),
    HealthSnapshotCheck("alert_lifecycle", _check_alert_lifecycle),
    HealthSnapshotCheck("execution_supervisor", _check_execution_supervisor),
    HealthSnapshotCheck("broker_connection", _check_broker_connection),
    HealthSnapshotCheck("model_cache", _check_model_cache),
    HealthSnapshotCheck("runtime_price_cache", _check_runtime_price_cache),
    HealthSnapshotCheck("event_bus", _check_event_bus),
    HealthSnapshotCheck("async_price_persistence", _check_async_price_persistence),
    HealthSnapshotCheck("pg_price_storage", _check_pg_price_storage),
    HealthSnapshotCheck("price_migration_validation", _check_price_migration_validation),
    HealthSnapshotCheck("timeseries_storage", _check_timeseries_storage),
    HealthSnapshotCheck("telemetry_migration_validation", _check_telemetry_migration_validation),
    HealthSnapshotCheck("portfolio_runtime", _check_portfolio_runtime),
    HealthSnapshotCheck("execution_degraded", _check_execution_degraded),
    HealthSnapshotCheck("execution_barrier_refresh", _check_execution_barrier_refresh),
    HealthSnapshotCheck("component_health", _check_component_health),
    HealthSnapshotCheck("data_pipeline_gates", _check_data_pipeline_gates),
    HealthSnapshotCheck("safe_mode_attribution", _adjust_safe_mode_attribution),
    HealthSnapshotCheck("startup_validation", _check_startup_validation),
)


def _build_health_snapshot_context(con: Any, now_ms: int) -> HealthSnapshotContext:
    return _health_build_snapshot_context(
        con,
        now_ms,
        db_path=Path(DB_PATH),
        warn=_warn,
    )


def _finalize_health_snapshot(ctx: HealthSnapshotContext) -> Dict[str, Any]:
    out = ctx.out
    mode_name = str(os.environ.get("ENGINE_MODE", "safe") or "safe").strip().lower()

    db_ok = bool((out.get("db") or {}).get("ok"))
    event_log_ok = bool((out.get("event_log") or {}).get("ok"))
    prices_ok = bool((out.get("prices") or {}).get("ok"))
    events_ok = bool((out.get("events") or {}).get("ok"))
    jobs_ok = bool((out.get("job_summary") or {}).get("ok"))
    providers_ok = bool((out.get("providers") or {}).get("ok"))
    provider_readiness = dict(out.get("provider_readiness") or {})
    provider_readiness_required = bool(provider_readiness.get("required"))
    provider_readiness_ok = bool((not provider_readiness_required) or provider_readiness.get("ok"))
    competition_ok = bool((out.get("competition") or {}).get("ok"))
    attribution_ok = bool((out.get("attribution") or {}).get("ok"))
    options_ingestion_ok = bool((out.get("options_ingestion") or {}).get("ok"))
    startup_validation_ok = bool((out.get("startup_validation") or {}).get("ok"))
    timeseries_storage_snapshot = dict(out.get("timeseries_storage") or {})
    timescale_snapshot = dict(out.get("timescale") or {})
    feature_store_snapshot = dict(out.get("feature_store") or {})
    portfolio_runtime_snapshot = dict(out.get("portfolio_runtime") or {})
    position_reconcile_snapshot = dict(out.get("position_reconcile") or {})
    execution_degraded_snapshot = dict(out.get("execution_degraded") or {})
    timeseries_ok = bool(timeseries_storage_snapshot.get("ok", True))
    portfolio_runtime_ok = not bool(portfolio_runtime_snapshot.get("degraded"))
    position_reconcile_required = mode_name in ("paper", "live")
    position_reconcile_ok = bool((not position_reconcile_required) or position_reconcile_snapshot.get("ok"))
    position_reconcile_blocking = bool(position_reconcile_required and not position_reconcile_ok)
    execution_degraded_active = bool(execution_degraded_snapshot.get("active"))
    execution_degraded_critical = bool(
        execution_degraded_active
        and str(execution_degraded_snapshot.get("severity") or "").strip().upper() == "CRITICAL"
    )
    ingestion_freshness = dict(out.get("ingestion_freshness") or {})
    critical_ingestion_ok = bool(ingestion_freshness.get("critical_ok"))
    ingestion_runtime_reason_codes = list(ingestion_freshness.get("runtime_reason_codes") or [])
    ingestion_advisory_reason_codes = list(ingestion_freshness.get("advisory_reason_codes") or [])
    data_pipeline_gates = dict(out.get("data_pipeline_gates") or {})
    data_pipeline_gates_ok = bool(data_pipeline_gates.get("ok"))
    failed_data_pipeline_gates = [
        str(name)
        for name in list(data_pipeline_gates.get("failed_gates") or [])
        if str(name).strip()
    ]
    observed_data_pipeline_gate_failures = [
        name
        for name in failed_data_pipeline_gates
        if (
            (name == "critical_features_valid" and bool(out.get("feature_validation")))
            or (name == "model_inputs_valid" and bool(out.get("model_input_validation")))
            or (name == "scoring_pipeline_operational" and bool(out.get("scoring_pipeline")))
        )
    ]
    data_pipeline_runtime_ok = len(observed_data_pipeline_gate_failures) == 0

    barrier = out.get("execution_barrier") or {}
    barrier_ok = bool(barrier.get("allowed"))

    exec_sup = out.get("execution_supervisor") or {}
    exec_sup_ok = bool(exec_sup.get("ok"))
    exec_sup_critical = str(exec_sup.get("state") or "").lower().strip() == "critical"
    exec_sup_failed_gates = [
        str(name)
        for name in list(exec_sup.get("failed_gates") or [])
        if str(name).strip()
    ]
    exec_sup_alert_types = {
        str(alert.get("alert_type") or "")
        for alert in list(exec_sup.get("alerts") or [])
        if str(alert.get("alert_type") or "").strip()
    }
    exec_sup_account_state = dict(exec_sup.get("account_state") or {})
    exec_sup_integrity = dict(exec_sup.get("integrity") or {})

    broker_connection = out.get("broker_connection") or {}
    broker_ok = bool(broker_connection.get("ok")) and str(broker_connection.get("state") or "").lower().strip() not in (
        "disconnected",
        "connect_failed",
        "reconnect_failed",
    )

    out["startup"] = {
        "mode": mode_name,
        "db_ok": db_ok,
        "event_log_ok": event_log_ok,
        "prices_ok": prices_ok,
        "events_ok": events_ok,
        "jobs_ok": jobs_ok,
        "providers_ok": providers_ok,
        "provider_readiness_ok": provider_readiness_ok,
        "competition_ok": competition_ok,
        "attribution_ok": attribution_ok,
        "critical_ingestion_ok": critical_ingestion_ok,
        "data_pipeline_gates_ok": data_pipeline_gates_ok,
        "data_pipeline_runtime_ok": data_pipeline_runtime_ok,
        "options_ingestion_ok": options_ingestion_ok,
        "timeseries_ok": timeseries_ok,
        "portfolio_runtime_ok": portfolio_runtime_ok,
        "position_reconcile_ok": position_reconcile_ok,
        "execution_degraded": execution_degraded_active,
        "startup_validation_ok": startup_validation_ok,
        "execution_barrier_ok": barrier_ok,
        "broker_ok": broker_ok,
        "execution_supervisor_ok": exec_sup_ok,
        "execution_gates_ok": len(exec_sup_failed_gates) == 0,
    }

    if not db_ok:
        out["reasons"].append("db_not_initialized")

    if not event_log_ok:
        out["reasons"].append("event_log_not_ok")

    if not prices_ok:
        out["reasons"].append("no_prices")

    if not events_ok and mode_name in ("shadow", "live"):
        out["reasons"].append("events_not_ok")

    if not jobs_ok:
        out["reasons"].append("jobs_not_running")

    if not providers_ok:
        out["reasons"].append("providers_not_ok")
    if not provider_readiness_ok:
        out["reasons"].append("provider_readiness_not_ok")
        out["reasons"].extend(list(provider_readiness.get("blockers") or []))

    if not competition_ok and mode_name in ("shadow", "live"):
        out["reasons"].append("competition_not_ok")
    if not attribution_ok:
        out["reasons"].append("attribution_not_ok")
        if _int_or((((out.get("attribution") or {}).get("orphans") or {}).get("orphan_row_count"))) > 0:
            out["reasons"].append("pnl_attribution_orphans_detected")

    if not bool((out.get("ingestion_runtime") or {}).get("running")):
        out["reasons"].append("ingestion_not_running")

    if bool((out.get("ingestion_runtime") or {}).get("stale")):
        out["reasons"].append("ingestion_stale")

    out["reasons"].extend(ingestion_runtime_reason_codes)
    out["reasons"].extend(ingestion_advisory_reason_codes)
    out["reasons"].extend([f"data_gate:{name}" for name in observed_data_pipeline_gate_failures])
    if not options_ingestion_ok:
        out["reasons"].append("options_ingestion_not_ok")
    if not timeseries_ok:
        out["reasons"].append("timeseries_storage_not_ok")
        out["reasons"].extend(
            [f"timescale_degraded:{reason}" for reason in list(timescale_snapshot.get("degraded_reasons") or [])]
        )
        out["reasons"].extend(
            [f"feature_store_degraded:{reason}" for reason in list(feature_store_snapshot.get("degraded_reasons") or [])]
        )
    if bool(portfolio_runtime_snapshot.get("degraded")):
        out["reasons"].append("portfolio_runtime_degraded")
        out["reasons"].extend(list(portfolio_runtime_snapshot.get("degraded_codes") or []))
    if position_reconcile_blocking:
        out["reasons"].append("position_reconcile_not_ok")
        out["reasons"].append(
            f"position_reconcile:{position_reconcile_snapshot.get('status') or 'failed'}"
        )
        out["reasons"].extend(list(position_reconcile_snapshot.get("blockers") or []))
    if not startup_validation_ok:
        out["reasons"].extend(list((out.get("startup_validation") or {}).get("reasons") or []))
    if not barrier_ok and mode_name in ("shadow", "live"):
        out["reasons"].append(f"execution_barrier:{barrier.get('reason', 'blocked')}")
    if execution_degraded_active:
        out["reasons"].append(
            f"execution_degraded:{str(execution_degraded_snapshot.get('reason') or 'execution_degraded')}"
        )
        out["reasons"].extend(list(execution_degraded_snapshot.get("reason_codes") or []))

    if not exec_sup_ok and mode_name in ("shadow", "live"):
        out["reasons"].append("execution_supervisor_unavailable")
    out["reasons"].extend([f"execution_gate:{name}" for name in exec_sup_failed_gates])
    for alert_type in (
        "duplicate_order_risk_detected",
        "missing_fills_detected",
        "fill_missing_local_order_reference",
        "broker_submission_unrecorded_needs_reconcile",
        "order_position_mismatch",
        "invalid_account_balance_state",
        "pricing_unavailable_for_unrealized_pnl",
    ):
        if alert_type in exec_sup_alert_types:
            out["reasons"].append(alert_type)
    if exec_sup_account_state and not bool(exec_sup_account_state.get("ok", True)):
        out["reasons"].append("invalid_account_balance_state")
    if int(exec_sup_integrity.get("pricing_unavailable_count") or 0) > 0:
        out["reasons"].append("pricing_unavailable_for_unrealized_pnl")
    if exec_sup_critical:
        out["reasons"].append("execution_supervisor_critical")

    if not broker_ok and mode_name == "live":
        out["reasons"].append("broker_connection_unavailable")

    if ctx.check_failures:
        out["reasons"].extend(ctx.check_failures)

    startup_ok = (
        db_ok
        and event_log_ok
        and prices_ok
        and jobs_ok
        and providers_ok
        and provider_readiness_ok
        and critical_ingestion_ok
    )
    if mode_name == "live":
        out["ok"] = (
            startup_ok
            and startup_validation_ok
            and events_ok
            and data_pipeline_runtime_ok
            and barrier_ok
            and broker_ok
            and competition_ok
            and attribution_ok
            and timeseries_ok
            and portfolio_runtime_ok
            and position_reconcile_ok
            and exec_sup_ok
            and (not execution_degraded_critical)
            and (not exec_sup_critical)
        )
    elif mode_name == "shadow":
        out["ok"] = (
            startup_ok
            and startup_validation_ok
            and events_ok
            and data_pipeline_runtime_ok
            and barrier_ok
            and competition_ok
            and attribution_ok
            and timeseries_ok
            and portfolio_runtime_ok
            and exec_sup_ok
            and (not execution_degraded_critical)
            and (not exec_sup_critical)
        )
    else:
        out["ok"] = (
            startup_ok
            and startup_validation_ok
            and data_pipeline_runtime_ok
            and timeseries_ok
            and portfolio_runtime_ok
            and (not exec_sup_critical)
        )

    if ctx.check_failures:
        out["ok"] = False

    if HEALTH_EMIT_METRICS:
        try:
            emit_gauge(
                "job_health",
                1.0 if bool(out.get("ok")) else 0.0,
                component="engine.runtime.health",
                extra_tags={"metric_scope": "health_snapshot"},
            )
            emit_gauge(
                "provider_uptime",
                float((out.get("providers") or {}).get("healthy") or 0.0),
                component="engine.runtime.health",
                extra_tags={"metric_scope": "healthy_providers"},
            )
            for component_name in ("ingestion", "inference", "execution"):
                component_row = dict((out.get("component_health") or {}).get(component_name) or {})
                emit_gauge(
                    "component_health_snapshot",
                    1.0 if bool(component_row.get("ok")) else 0.0,
                    component="engine.runtime.health",
                    extra_tags={"observed_component": str(component_name)},
                )
            trace_event(
                "health_snapshot",
                component="engine.runtime.health",
                entity_type="health",
                entity_id="runtime",
                payload={
                    "ok": bool(out.get("ok")),
                    "reasons": list(out.get("reasons") or []),
                    "prices": out.get("prices") or {},
                    "providers": out.get("providers") or {},
                    "job_summary": out.get("job_summary") or {},
                },
            )
        except Exception as e:
            _warn("health.runtime_event.emit", e)

    root_cause_candidates = list(out.get("reasons") or [])

    critical_blockers = []
    if not bool(out.get("db", {}).get("ok")):
        critical_blockers.append("db_not_ok")
    if not bool(out.get("prices", {}).get("ok")):
        critical_blockers.append("prices_not_ok")
    if not bool(out.get("providers", {}).get("ok")):
        critical_blockers.append("providers_not_ok")
    if not bool((out.get("provider_readiness") or {}).get("ok")) and bool(
        (out.get("provider_readiness") or {}).get("required")
    ):
        critical_blockers.append("provider_readiness_not_ok")
        critical_blockers.extend(list((out.get("provider_readiness") or {}).get("blockers") or []))
    if not bool(out.get("job_summary", {}).get("ok")):
        critical_blockers.append("jobs_not_ok")
    if not bool(out.get("competition", {}).get("ok")) and mode_name in ("shadow", "live"):
        critical_blockers.append("competition_not_ok")
    if not bool(out.get("attribution", {}).get("ok")):
        critical_blockers.append("attribution_not_ok")
    if not timeseries_ok:
        critical_blockers.append("timeseries_storage_not_ok")
    if bool(portfolio_runtime_snapshot.get("degraded")):
        critical_blockers.append("portfolio_runtime_degraded")
    if position_reconcile_blocking:
        critical_blockers.append("position_reconcile_not_ok")
    if execution_degraded_critical:
        critical_blockers.append("execution_degraded")
    if mode_name in ("shadow", "live") and not exec_sup_ok:
        critical_blockers.append("execution_supervisor_unavailable")
    if exec_sup_critical:
        critical_blockers.append("execution_supervisor_critical")

    if not bool((out.get("ingestion_runtime") or {}).get("running")):
        critical_blockers.append("ingestion_not_running")

    if bool((out.get("ingestion_runtime") or {}).get("stale")):
        critical_blockers.append("ingestion_stale")

    critical_blockers.extend(ingestion_runtime_reason_codes)
    critical_blockers.extend(ctx.check_failures)

    if not out.get("db", {}).get("ok"):
        system_stage = "BOOT"
    elif (
        not out.get("prices", {}).get("ok")
        or bool((out.get("ingestion_runtime") or {}).get("stale"))
        or not critical_ingestion_ok
    ):
        system_stage = "INGESTION"
    elif not out.get("labels", {}).get("ok"):
        system_stage = "FEATURES"
    else:
        system_stage = "EXECUTION"

    data_flow_ok = bool(
        out.get("db", {}).get("ok")
        and out.get("prices", {}).get("ok")
        and out.get("providers", {}).get("ok")
        and (
            (not bool((out.get("provider_readiness") or {}).get("required")))
            or bool((out.get("provider_readiness") or {}).get("ok"))
        )
        and out.get("job_summary", {}).get("ok")
        and not bool((out.get("ingestion_runtime") or {}).get("stale"))
        and bool(critical_ingestion_ok)
        and bool((out.get("attribution") or {}).get("ok"))
        and (mode_name not in ("shadow", "live") or bool((out.get("competition") or {}).get("ok")))
        and not bool(ctx.check_failures)
    )
    out["reasons"] = _dedupe_strs(list(out.get("reasons") or []))
    out["root_cause_candidates"] = _dedupe_strs(root_cause_candidates)
    out["critical_blockers"] = _dedupe_strs(critical_blockers)
    out["system_stage"] = system_stage
    out["data_flow_ok"] = data_flow_ok
    try:
        out["lifecycle"] = dict(_lc_get_state() or {})
    except Exception as e:
        _warn("health.lifecycle_state", e)
        out["lifecycle"] = {"state": "UNKNOWN", "detail": "", "first_price_ts_ms": ""}

    return out



def get_health_snapshot():
    # Canonical runtime health snapshot consumed by lifecycle, APIs, readiness,
    # and preflight. Keep this as a small driver; checks live in the registry
    # above so individual probes can be tested without constructing the whole
    # runtime health stack.
    now_ms = int(time.time() * 1000)
    cache_ttl_ms = max(0, int(_HEALTH_SNAPSHOT_CACHE_TTL_MS))
    cached_ts_ms = 0
    cached_payload: Any = None

    if cache_ttl_ms > 0:
        try:
            with _HEALTH_SNAPSHOT_CACHE_LOCK:
                cached_ts_ms = _int_or(_HEALTH_SNAPSHOT_CACHE.get("ts_ms"))
                cached_payload = _HEALTH_SNAPSHOT_CACHE.get("payload")
                if (
                    isinstance(cached_payload, dict)
                    and cached_ts_ms > 0
                    and (now_ms - cached_ts_ms) <= cache_ttl_ms
                ):
                    return copy.deepcopy(cached_payload)
        except Exception as e:
            _warn("health.snapshot_cache.read", e)

    refresh_lock = _HEALTH_SNAPSHOT_REFRESH_LOCK
    refresh_lock_acquired = refresh_lock.acquire(blocking=False)
    if not refresh_lock_acquired:
        if isinstance(cached_payload, dict) and cached_ts_ms > 0:
            return _stale_health_snapshot_payload(
                cached_payload,
                now_ms=now_ms,
                cached_ts_ms=cached_ts_ms,
            )
        return _health_snapshot_pending_payload(
            now_ms=now_ms,
            reason="health_snapshot_refresh_in_flight",
            cached_ts_ms=cached_ts_ms,
        )

    con = None
    try:
        con = _db_connect()
        ctx = _build_health_snapshot_context(con, now_ms)
        _run_health_checks(ctx, _HEALTH_SNAPSHOT_CHECKS)
        out = _finalize_health_snapshot(ctx)

        section_started = time.perf_counter()
        if cache_ttl_ms > 0:
            try:
                with _HEALTH_SNAPSHOT_CACHE_LOCK:
                    _HEALTH_SNAPSHOT_CACHE["ts_ms"] = int(time.time() * 1000)
                    _HEALTH_SNAPSHOT_CACHE["payload"] = copy.deepcopy(out)
            except Exception as e:
                _warn("health.snapshot_cache.write", e)

        _trace_section(
            "finalize",
            section_started,
            ok=bool(out.get("ok")),
            reasons=len(out.get("reasons") or []),
        )

        return out

    finally:
        try:
            if con is not None:
                con.close()
        finally:
            refresh_lock.release()


def get_readiness_snapshot(
    health: Optional[Dict[str, Any]] = None,
    preflight: Optional[Dict[str, Any]] = None,
    system_state: Optional[Dict[str, Any]] = None,
    graph: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    return _health_get_readiness_snapshot(
        health=health,
        preflight=preflight,
        system_state=system_state,
        graph=graph,
        environ=os.environ,
        live_state=_LIVE,
    )

    ts_ms = int(time.time() * 1000)

    health = dict(health or {})
    preflight = dict(preflight or {})
    system_state = dict(system_state or {})
    graph = dict(graph or {})

    prices = health.get("prices") or {}
    providers = health.get("providers") or {}
    provider_readiness = dict(health.get("provider_readiness") or {}) if isinstance(health.get("provider_readiness"), dict) else {}
    labels = health.get("labels") or {}
    model = health.get("model") or {}
    execution_barrier = health.get("execution_barrier") or {}
    broker_connection = health.get("broker_connection") or {}
    db = health.get("db") or {}
    job_summary = health.get("job_summary") or {}
    startup_validation = dict(health.get("startup_validation") or {}) if isinstance(health.get("startup_validation"), dict) else {}
    timeseries_storage = dict(health.get("timeseries_storage") or {}) if isinstance(health.get("timeseries_storage"), dict) else {}
    feature_store = dict(health.get("feature_store") or {}) if isinstance(health.get("feature_store"), dict) else dict(timeseries_storage.get("feature_store") or {})
    portfolio_runtime = dict(health.get("portfolio_runtime") or {}) if isinstance(health.get("portfolio_runtime"), dict) else {}
    position_reconcile = dict(health.get("position_reconcile") or {}) if isinstance(health.get("position_reconcile"), dict) else {}
    execution_degraded = dict(health.get("execution_degraded") or {}) if isinstance(health.get("execution_degraded"), dict) else {}
    execution_supervisor = dict(health.get("execution_supervisor") or {}) if isinstance(health.get("execution_supervisor"), dict) else {}

    mode_name = str(
        (system_state.get("mode") or system_state.get("execution_mode") or os.environ.get("ENGINE_MODE") or "safe")
    ).strip().lower() or "safe"

    require_models = mode_name in ("shadow", "live")
    require_risk = mode_name in ("shadow", "live")
    require_broker = mode_name == "live"

    provider_readiness_required = bool(provider_readiness.get("required"))
    provider_readiness_ok = bool((not provider_readiness_required) or provider_readiness.get("ok"))
    data_feed_ok = bool(prices.get("ok")) and bool(providers.get("ok")) and provider_readiness_ok
    models_ok = bool(labels.get("ok")) and bool(model.get("ok"))
    risk_ok = bool(execution_barrier.get("allowed"))
    db_ok = bool(db.get("ok"))
    db_initialized = db_ok and bool(db.get("initialized"))
    jobs_ok = bool(job_summary.get("ok"))
    jobs_running = bool(job_summary.get("total")) and jobs_ok
    require_timeseries = bool(timeseries_storage.get("enabled")) or bool(feature_store.get("enabled"))
    timeseries_ok = (not require_timeseries) or bool(timeseries_storage.get("ok"))
    portfolio_runtime_ok = not bool(portfolio_runtime.get("degraded"))
    position_reconcile_required = mode_name in ("paper", "live")
    position_reconcile_ok = bool((not position_reconcile_required) or position_reconcile.get("ok"))
    position_reconcile_blocking = bool(position_reconcile_required and not position_reconcile_ok)
    execution_degraded_active = bool(execution_degraded.get("active"))
    execution_degraded_severity = str(execution_degraded.get("severity") or "WARNING").strip().upper() or "WARNING"
    execution_degraded_critical = bool(execution_degraded_active and execution_degraded_severity == "CRITICAL")
    execution_supervisor_ok = bool(execution_supervisor.get("ok"))
    execution_supervisor_state = str(execution_supervisor.get("state") or "unknown").strip().lower() or "unknown"
    execution_supervisor_critical = execution_supervisor_state == "critical"
    execution_supervisor_failed_gates = [
        str(name)
        for name in list(execution_supervisor.get("failed_gates") or [])
        if str(name).strip()
    ]

    broker_state = str(broker_connection.get("state") or "").strip().lower()
    broker_ok = bool(broker_connection.get("ok")) and broker_state not in (
        "disconnected",
        "connect_failed",
        "reconnect_failed",
    )

    preflight_ok = bool(preflight.get("ok")) if preflight else True
    graph_ok = bool(graph.get("ok")) if graph else True
    startup_validation_ok = bool(startup_validation.get("ok")) if startup_validation else True
    startup_blocking_gates = [
        str(name)
        for name in list(startup_validation.get("blocking_gates") or startup_validation.get("blocking_checks") or [])
        if str(name).strip()
    ]

    state_name = str(system_state.get("state") or "").strip().upper()
    system_live = state_name == _LIVE if system_state else False

    issues: List[Dict[str, Any]] = []

    if not db_initialized:
        issues.append({
            "code": "db_not_initialized",
            "level": "error",
            "message": "Database is not initialized.",
            "detail": f"db_ok={db_ok} exists={bool(db.get('exists'))} initialized={bool(db.get('initialized'))} path={db.get('db_path')}",
        })

    if not jobs_running:
        issues.append({
            "code": "jobs_not_running",
            "level": "error",
            "message": "Required startup jobs are not running.",
            "detail": f"total={job_summary.get('total')} stale={job_summary.get('stale')} stale_jobs={job_summary.get('stale_jobs')}",
        })

    if not bool(prices.get("ok")):
        issues.append({
            "code": "no_prices",
            "level": "error",
            "message": "No fresh prices are available.",
            "detail": f"last_ts_ms={prices.get('last_ts_ms')} age_s={prices.get('age_s')} max_age_s={prices.get('max_age_s')}",
        })

    if not data_feed_ok:
        issues.append({
            "code": "data_feed_not_ready",
            "level": "error",
            "message": "Data feed readiness failed.",
            "detail": (
                f"prices_ok={bool(prices.get('ok'))} providers_ok={bool(providers.get('ok'))} "
                f"provider_readiness_ok={provider_readiness_ok} age_s={prices.get('age_s')} "
                f"healthy={providers.get('healthy')}/{providers.get('total')}"
            ),
        })

    if provider_readiness_required and not provider_readiness_ok:
        issues.append({
            "code": "provider_readiness_failed",
            "level": "error",
            "message": "Required provider readiness failed.",
            "detail": (
                f"required_providers={list(provider_readiness.get('required_providers') or [])} "
                f"blockers={list(provider_readiness.get('blockers') or [])}"
            ),
        })

    if require_models and not models_ok:
        issues.append({
            "code": "models_not_ready",
            "level": "error",
            "message": "Models readiness failed.",
            "detail": f"labels_ok={bool(labels.get('ok'))} label_count={labels.get('count')} model_ok={bool(model.get('ok'))} support_n={model.get('support_n')}",
        })

    if require_risk and not risk_ok:
        issues.append({
            "code": "risk_not_ready",
            "level": "error",
            "message": "Risk gate is blocking trading.",
            "detail": str(execution_barrier.get("reason") or "blocked"),
        })

    if require_broker and not broker_ok:
        issues.append({
            "code": "broker_not_ready",
            "level": "error",
            "message": "Broker connection is not ready.",
            "detail": f"state={broker_connection.get('state')} broker={broker_connection.get('broker')}",
        })

    if require_timeseries and not timeseries_ok:
        issues.append({
            "code": "timeseries_storage_not_ready",
            "level": "error",
            "message": "Timeseries storage sidecars are not ready.",
            "detail": (
                f"timeseries_ok={bool(timeseries_storage.get('ok'))} detail={timeseries_storage.get('detail') or 'timeseries_storage_not_ready'} "
                f"timescale_enabled={bool(timeseries_storage.get('enabled'))} feature_store_enabled={bool(feature_store.get('enabled'))}"
            ),
        })

    if not portfolio_runtime_ok:
        issues.append({
            "code": "portfolio_runtime_degraded",
            "level": "error",
            "message": "Portfolio runtime is degraded.",
            "detail": (
                f"detail={portfolio_runtime.get('detail') or 'portfolio_runtime_degraded'} "
                f"codes={list(portfolio_runtime.get('degraded_codes') or [])}"
            ),
        })

    if position_reconcile_blocking:
        issues.append({
            "code": "position_reconcile_failed",
            "level": "error",
            "message": "Persisted position reconcile gate failed.",
            "detail": (
                f"status={position_reconcile.get('status') or 'failed'} "
                f"broker={position_reconcile.get('broker') or 'unknown'} "
                f"mismatched_n={position_reconcile.get('mismatched_n')} "
                f"blockers={list(position_reconcile.get('blockers') or [])} "
                f"detail={position_reconcile.get('detail') or 'position_reconcile_failed'}"
            ),
        })

    if execution_degraded_active:
        issues.append({
            "code": "execution_degraded",
            "level": ("error" if execution_degraded_critical else "warn"),
            "message": "Execution runtime is degraded.",
            "detail": (
                f"severity={execution_degraded_severity} reason={execution_degraded.get('reason') or 'execution_degraded'} "
                f"codes={list(execution_degraded.get('reason_codes') or [])}"
            ),
        })

    if mode_name in ("shadow", "live") and not execution_supervisor_ok:
        issues.append({
            "code": "execution_supervisor_unavailable",
            "level": "error",
            "message": "Execution supervisor snapshot is unavailable.",
            "detail": str(execution_supervisor.get("detail") or "execution_supervisor_unavailable"),
        })

    if execution_supervisor_critical or execution_supervisor_failed_gates:
        issues.append({
            "code": "execution_health_gate_failed",
            "level": "error",
            "message": "Execution safety gates failed.",
            "detail": (
                f"state={execution_supervisor.get('state') or 'unknown'} "
                f"failed_gates={execution_supervisor_failed_gates}"
            ),
        })

    if not preflight_ok:
        issues.append({
            "code": "preflight_failed",
            "level": "error",
            "message": "Startup preflight failed.",
            "detail": "; ".join(str(x) for x in (preflight.get("notes") or [])) or "preflight_failed",
        })

    if not startup_validation_ok:
        issues.append({
            "code": "startup_gates_failed",
            "level": "error",
            "message": "Startup gate validation failed.",
            "detail": (
                f"blocking_gates={startup_blocking_gates}"
                if startup_blocking_gates
                else "; ".join(str(x) for x in (startup_validation.get("reasons") or []))
            ),
        })

    if not graph_ok:
        issues.append({
            "code": "graph_invalid",
            "level": "warn",
            "message": "Runtime dependency graph validation failed.",
            "detail": str(graph.get("error") or "graph_invalid"),
        })

    if system_state and not system_live:
        issues.append({
            "code": "system_state_not_live",
            "level": "warn",
            "message": "System state is not LIVE.",
            "detail": state_name or "UNKNOWN",
        })

    startup_ok = (
        db_ok
        and data_feed_ok
        and jobs_ok
        and timeseries_ok
        and portfolio_runtime_ok
        and position_reconcile_ok
        and (execution_supervisor_ok if mode_name in ("shadow", "live") else True)
        and (not execution_supervisor_critical)
        and (not execution_degraded_critical)
    )

    steps = [
        {
            "id": "database",
            "label": "Verify Database",
            "ok": db_ok,
            "blocked": not db_ok,
            "detail": f"db_ok={db_ok} path={db.get('db_path')} error={db.get('error')}",
        },
        {
            "id": "data_feed",
            "label": "Verify Data Feed",
            "ok": data_feed_ok,
            "blocked": not data_feed_ok,
            "detail": (
                f"prices_ok={bool(prices.get('ok'))} providers={providers.get('healthy')}/{providers.get('total')} "
                f"provider_readiness_ok={provider_readiness_ok} age_s={prices.get('age_s')}"
            ),
        },
        {
            "id": "provider_readiness",
            "label": "Verify Providers",
            "ok": provider_readiness_ok,
            "blocked": bool(provider_readiness_required and not provider_readiness_ok),
            "detail": (
                "not_required"
                if not provider_readiness_required
                else f"required={list(provider_readiness.get('required_providers') or [])} blockers={list(provider_readiness.get('blockers') or [])}"
            ),
        },
        {
            "id": "jobs",
            "label": "Verify Jobs",
            "ok": jobs_ok,
            "blocked": not jobs_ok,
            "detail": f"total={job_summary.get('total')} stale={job_summary.get('stale')} stale_jobs={job_summary.get('stale_jobs')}",
        },
        {
            "id": "models",
            "label": "Verify Models",
            "ok": models_ok,
            "blocked": bool(require_models and not models_ok),
            "detail": f"required={require_models} labels_ok={bool(labels.get('ok'))} label_count={labels.get('count')} model_ok={bool(model.get('ok'))} support_n={model.get('support_n')}",
        },
        {
            "id": "risk",
            "label": "Verify Risk",
            "ok": risk_ok,
            "blocked": bool(require_risk and not risk_ok),
            "detail": f"required={require_risk} execution_allowed={risk_ok} reason={execution_barrier.get('reason') or 'ok'}",
        },
        {
            "id": "broker",
            "label": "Verify Broker",
            "ok": broker_ok,
            "blocked": bool(require_broker and not broker_ok),
            "detail": f"required={require_broker} broker_ok={bool(broker_connection.get('ok'))} state={broker_connection.get('state') or 'unknown'} broker={broker_connection.get('broker') or 'unknown'}",
        },
        {
            "id": "timeseries_storage",
            "label": "Verify Sidecars",
            "ok": timeseries_ok,
            "blocked": bool(require_timeseries and not timeseries_ok),
            "detail": (
                f"required={require_timeseries} timeseries_ok={bool(timeseries_storage.get('ok'))} "
                f"detail={timeseries_storage.get('detail') or 'optional_disabled'}"
            ),
        },
        {
            "id": "portfolio_runtime",
            "label": "Verify Portfolio Runtime",
            "ok": portfolio_runtime_ok,
            "blocked": not portfolio_runtime_ok,
            "detail": (
                f"degraded={bool(portfolio_runtime.get('degraded'))} "
                f"detail={portfolio_runtime.get('detail') or 'ok'}"
            ),
        },
        {
            "id": "position_reconcile",
            "label": "Verify Positions",
            "ok": position_reconcile_ok,
            "blocked": position_reconcile_blocking,
            "detail": (
                f"available={bool(position_reconcile.get('available'))} "
                f"status={position_reconcile.get('status') or 'unavailable'} "
                f"broker={position_reconcile.get('broker') or 'unknown'}"
            ),
        },
        {
            "id": "execution_health",
            "label": "Verify Execution Health",
            "ok": (not execution_degraded_active) and execution_supervisor_ok and (not execution_supervisor_critical),
            "blocked": execution_degraded_critical or execution_supervisor_critical or (mode_name in ("shadow", "live") and not execution_supervisor_ok),
            "detail": (
                f"degraded_active={execution_degraded_active} severity={execution_degraded_severity} "
                f"reason={execution_degraded.get('reason') or 'ok'} "
                f"exec_supervisor_ok={execution_supervisor_ok} "
                f"exec_supervisor_state={execution_supervisor.get('state') or 'unknown'} "
                f"failed_gates={execution_supervisor_failed_gates}"
            ),
        },
        {
            "id": "startup_gates",
            "label": "Verify Startup Gates",
            "ok": startup_validation_ok,
            "blocked": not startup_validation_ok,
            "detail": (
                "All startup gates passed."
                if startup_validation_ok
                else f"blocking_gates={startup_blocking_gates or list(startup_validation.get('reasons') or [])}"
            ),
        },
    ]

    ready_without_state = (
        startup_ok
        and startup_validation_ok
        and preflight_ok
        and graph_ok
        and (models_ok if require_models else True)
        and (risk_ok if require_risk else True)
        and (broker_ok if require_broker else True)
        and (timeseries_ok if require_timeseries else True)
        and portfolio_runtime_ok
        and position_reconcile_ok
        and (execution_supervisor_ok if mode_name in ("shadow", "live") else True)
        and (not execution_supervisor_critical)
        and (not execution_degraded_critical)
    )
    final_ready = ready_without_state and (system_live if system_state else True)

    waiting_on = []
    if not db_ok:
        waiting_on.append("database")
    if not data_feed_ok:
        waiting_on.append("data_feed")
    if provider_readiness_required and not provider_readiness_ok:
        waiting_on.append("provider_readiness")
    if not jobs_ok:
        waiting_on.append("jobs")
    if require_models and not models_ok:
        waiting_on.append("models")
    if require_risk and not risk_ok:
        waiting_on.append("risk")
    if require_broker and not broker_ok:
        waiting_on.append("broker")
    if require_timeseries and not timeseries_ok:
        waiting_on.append("timeseries_storage")
    if not portfolio_runtime_ok:
        waiting_on.append("portfolio_runtime")
    if position_reconcile_blocking:
        waiting_on.append("position_reconcile")
    if execution_degraded_critical:
        waiting_on.append("execution_degraded")
    if mode_name in ("shadow", "live") and not execution_supervisor_ok:
        waiting_on.append("execution_supervisor")
    if execution_supervisor_critical or execution_supervisor_failed_gates:
        waiting_on.append("execution_health")
    if not startup_validation_ok:
        waiting_on.extend(startup_blocking_gates or ["startup_gates"])
    if not preflight_ok:
        waiting_on.append("preflight")
    if not graph_ok:
        waiting_on.append("graph")
    if system_state and not system_live:
        waiting_on.append("system_state")

    steps.append({
        "id": "enable_trading",
        "label": "Enable Trading",
        "ok": final_ready,
        "blocked": not final_ready,
        "detail": "All startup gates passed." if final_ready else f"waiting_on={','.join(waiting_on) or 'system_state'}",
    })

    reasons = []
    for item in issues:
        if str(item.get("level") or "").lower() != "error":
            continue
        reasons.append({
            "code": str(item.get("code") or "unknown"),
            "message": str(item.get("message") or ""),
            "detail": str(item.get("detail") or ""),
        })

    return {
        "ok": final_ready,
        "ready": final_ready,
        "degraded": not final_ready,
        "status": ("READY" if final_ready else "DEGRADED"),
        "ts_ms": ts_ms,
        "mode": mode_name,
        "data_feed_ok": data_feed_ok,
        "provider_readiness_ok": provider_readiness_ok,
        "models_ok": models_ok,
        "risk_ok": risk_ok,
        "broker_ok": broker_ok,
        "timeseries_ok": timeseries_ok,
        "portfolio_runtime_ok": portfolio_runtime_ok,
        "position_reconcile_ok": position_reconcile_ok,
        "preflight_ok": preflight_ok,
        "graph_ok": graph_ok,
        "startup_validation_ok": startup_validation_ok,
        "system_live": system_live,
        "system_state": state_name or "UNKNOWN",
        "position_reconcile": dict(position_reconcile or {}),
        "provider_readiness": provider_readiness,
        "execution_degraded": dict(execution_degraded or {}),
        "startup_validation": startup_validation,
        "waiting_on": waiting_on,
        "issues": issues,
        "reasons": reasons,
        "steps": steps,
    }


# ---------------------------------------------------
# PREFLIGHT
# ---------------------------------------------------

def run_preflight() -> Dict:
    global _PREFLIGHT_CACHE

    # Preflight is a reduced startup gate: strict enough to block obviously bad
    # launches, but lighter than the full ongoing health snapshot.

    ts_ms = int(time.time() * 1000)
    out = {
        "ok": True,
        "notes": [],
        "tables_ok": True,
        "health_ok": True,
        "ts_ms": ts_ms,
    }

    if not PREFLIGHT_ENABLE:
        out["notes"].append("preflight disabled")
        _PREFLIGHT_CACHE = out
        return out

    try:
        # ---------------------------
        # Schema validation
        # ---------------------------
        schema = get_schema_audit()
        out["schema"] = schema
        if not schema.get("ok"):
            out["ok"] = False
            out["tables_ok"] = False

            if schema.get("missing_tables"):
                out["notes"].append(
                    f"missing_tables={schema.get('missing_tables')}"
                )

            if schema.get("missing_cols"):
                out["notes"].append(
                    f"missing_cols={schema.get('missing_cols')}"
                )

            if schema.get("missing_indexes"):
                out["notes"].append(
                    f"missing_indexes={schema.get('missing_indexes')}"
                )

            if not bool(schema.get("schema_version_ok", True)):
                out["notes"].append(
                    "schema_version_mismatch="
                    f"db:{schema.get('schema_version')} "
                    f"expected:{schema.get('expected_schema_version')} "
                    f"status:{schema.get('schema_version_status')}"
                )

        # ---------------------------
        # Health validation
        # ---------------------------
        health = get_health_snapshot()
        out["health"] = health
        startup_validation = dict((health or {}).get("startup_validation") or {})
        if not startup_validation:
            startup_validation = get_startup_validation_snapshot(health=health, db_validation=out.get("schema"))
        out["startup_validation"] = startup_validation

        prices = health.get("prices", {}) or {}
        out["prices"] = prices
        prices_ok = bool(prices.get("ok"))

        barrier = dict(health.get("execution_barrier") or {})
        if not barrier:
            barrier = {"allowed": False, "ok": False, "reason": "execution_barrier_missing"}
        out["execution_barrier"] = barrier

        startup_validation_ok = bool(startup_validation.get("ok"))

        age_s = float(prices.get("age_s") or 1e9)

        # In SAFE mode, tolerate stale prices at boot so auto-boot daemons can start
        _mode = os.environ.get("ENGINE_MODE", "").strip().lower()
        if not _mode:
            _mode = "safe"

        try:
            from engine.execution.kill_switch import capital_equity_freshness_snapshot

            live_equity_required = any(
                str(os.environ.get(name) or "").strip().lower() == "live"
                for name in ("ENGINE_MODE", "EXECUTION_MODE", "OPERATOR_MODE", "MODE")
            )
            equity_freshness = dict(capital_equity_freshness_snapshot(live_mode=live_equity_required) or {})
        except Exception as e:
            _warn("health.capital_equity_freshness", e)
            equity_freshness = {
                "ok": False,
                "required": any(
                    str(os.environ.get(name) or "").strip().lower() == "live"
                    for name in ("ENGINE_MODE", "EXECUTION_MODE", "OPERATOR_MODE", "MODE")
                ),
                "reason_code": "capital_equity_freshness_error",
                "reason": f"{type(e).__name__}: {e}",
            }
        out["capital_equity_freshness"] = equity_freshness

        if _mode in ("live", "shadow"):
            barrier_ok = bool(barrier.get("allowed"))
        else:
            barrier_ok = not str(barrier.get("reason") or "").startswith("execution_barrier_error")
        equity_ok = (not bool(equity_freshness.get("required"))) or bool(equity_freshness.get("ok"))
        out["health_ok"] = prices_ok and barrier_ok and startup_validation_ok and equity_ok

        if age_s > PREFLIGHT_PRICES_MAX_AGE_S:
            out["ok"] = False
            out["health_ok"] = False
            if _mode == "safe":
                out["notes"].append(f"prices too stale (SAFE blocked): {age_s:.1f}s")
            else:
                out["notes"].append(f"prices too stale: {age_s:.1f}s")

        if not startup_validation_ok:
            out["ok"] = False
            out["health_ok"] = False
            blocking_checks = list(startup_validation.get("blocking_checks") or [])
            critical_missing = list(startup_validation.get("critical_systems_missing") or [])
            notes = list(startup_validation.get("reasons") or [])
            if blocking_checks:
                out["notes"].append(f"startup_validation_blocking={','.join(blocking_checks)}")
            if critical_missing:
                out["notes"].append(f"startup_validation_missing={','.join(critical_missing)}")
            out["notes"].extend(notes[:8])

        if not equity_ok:
            out["ok"] = False
            out["health_ok"] = False
            out["notes"].append(
                "capital_equity_freshness="
                f"{equity_freshness.get('reason_code') or equity_freshness.get('reason') or 'unavailable'}"
            )

    except Exception as e:
        out["ok"] = False
        out["health_ok"] = False
        out["notes"].append(str(e))

    _PREFLIGHT_CACHE = out
    _mode = os.environ.get("ENGINE_MODE", "").strip().lower() or "safe"

    # Add derived status field
    prices_ok = bool((out.get("prices") or {}).get("ok"))

    if _mode == "safe" and not prices_ok:
        out["status"] = "WARMING_UP"
    else:
        out["status"] = "LIVE" if prices_ok else "DEGRADED"

    try:
        lc = _lc_get_state()
    except Exception:
        lc = {"state": "BOOTING", "detail": "", "first_price_ts_ms": ""}

    out["lifecycle"] = lc

    # Deterministic status: SAFE warms up until first tick is latched,
    # but must honor explicit lifecycle degradation instead of stalling forever.
    mode = (os.environ.get("ENGINE_MODE", "") or "safe").strip().lower()
    first_tick = str((lc or {}).get("first_price_ts_ms") or "").strip()
    prices_ok = bool((out.get("prices") or {}).get("ok"))
    lifecycle_state_name = str((lc or {}).get("state") or "").strip().upper()

    if mode == "safe":
        if lifecycle_state_name == _DEGRADED:
            out["status"] = _DEGRADED
        elif lifecycle_state_name == _LIVE or first_tick:
            out["status"] = _LIVE
        else:
            out["status"] = _WARMING_UP
        out["ok"] = bool(out.get("tables_ok"))
    else:
        # In live/shadow: require schema + health alignment
        out["status"] = _LIVE if (prices_ok and bool(out.get("health_ok"))) else _DEGRADED
        out["ok"] = bool(out.get("tables_ok")) and bool(out.get("health_ok"))

    return out


def preflight_cached(max_age_s: float = 30.0) -> Dict:
    now = int(time.time() * 1000)
    cache = dict(_PREFLIGHT_CACHE or {})
    ts = int(cache.get("ts_ms") or 0)
    age_s = (now - ts) / 1000.0 if ts else 1e9

    if age_s > float(max_age_s):
        return run_preflight()

    return cache
