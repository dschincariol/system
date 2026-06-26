"""Periodic Postgres, PgBouncer, and slow-log observability snapshot."""

from __future__ import annotations

import json
import os
import hashlib
import threading
import time
from pathlib import Path
from typing import Any

from engine.runtime.cpu_power_policy import verify_cpu_power_policy
from engine.runtime.memory_pressure import host_memory_pressure_snapshot
from engine.runtime.observability import record_component_health
from engine.runtime.observability.pg_stats import snapshot_pg_observability
from engine.runtime.observability.slow_log import start_slow_log_tail_thread
from engine.runtime.platform import default_postgres_log_path
from engine.runtime.storage import (
    acquire_job_lock,
    init_db,
    put_job_heartbeat,
    release_job_lock,
    touch_job_lock,
)

JOB_NAME = "observability_snapshot"
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()
INTERVAL_S = float(os.environ.get("OBSERVABILITY_SNAPSHOT_INTERVAL_S", "60"))
HEARTBEAT_EVERY_S = float(os.environ.get("OBSERVABILITY_SNAPSHOT_HEARTBEAT_S", "15"))
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))

_SLOW_LOG_STOP = threading.Event()
_SLOW_LOG_THREAD: threading.Thread | None = None
_TRUTHY = {"1", "true", "yes", "on", "y"}
_PRODUCTION_VALUES = {"prod", "production", "live"}
_STORAGE_ALERT_FINGERPRINTS: dict[str, str] = {}
_WAL_ARCHIVER_LAST_FAILED_COUNT: int | None = None


def _default_slow_log_path() -> str:
    configured = str(os.environ.get("OBSERVABILITY_POSTGRES_LOG") or "").strip()
    if configured:
        return configured
    return default_postgres_log_path()


def _ensure_slow_log_tail() -> None:
    global _SLOW_LOG_THREAD
    if _SLOW_LOG_THREAD is not None and _SLOW_LOG_THREAD.is_alive():
        return
    path = _default_slow_log_path()
    if not path:
        return
    if not Path(path).exists():
        return
    _SLOW_LOG_STOP.clear()
    _SLOW_LOG_THREAD = start_slow_log_tail_thread(path, stop_event=_SLOW_LOG_STOP, start_at_end=True)


def _emit_redis_circuit_state(ts_ms: int) -> int:
    try:
        from engine.cache.circuit import cache_circuit
        from engine.runtime.metrics_store import write_runtime_metric

        circuit = cache_circuit()
        state = str(circuit.state)
        state_value = {"closed": 0.0, "half_open": 0.5, "open": 1.0}.get(state, -1.0)
        write_runtime_metric(
            "redis.circuit.state",
            value_num=state_value,
            value_text=state,
            tags={"name": str(getattr(circuit, "name", "redis"))},
            ts_ms=int(ts_ms),
        )
        return 1
    except Exception:
        return 0


def _snapshot_cpu_power_policy(ts_ms: int) -> dict[str, Any]:
    state = dict(verify_cpu_power_policy() or {})
    required = bool(state.get("required"))
    reason = str(state.get("reason") or "")
    skipped_unavailable = bool(not required and reason == "cpu_power_policy_unavailable")
    ok = bool(state.get("ok")) or skipped_unavailable
    status = "skipped" if skipped_unavailable else str(state.get("status") or ("ok" if ok else "error"))
    detail = reason or str(state.get("summary") or status)
    compact_state = {key: value for key, value in state.items() if key not in {"stdout", "stderr"}}
    health_extra = {
        key: value
        for key, value in compact_state.items()
        if key not in {"component", "detail", "ok", "status", "updated_ts_ms"}
    }
    health_extra["policy_ok"] = bool(state.get("ok"))
    health_extra["policy_status"] = str(state.get("status") or "")
    record_component_health(
        "cpu_power_policy",
        ok=ok,
        status=status,
        detail=detail,
        observed_ts_ms=ts_ms,
        extra=health_extra,
    )
    compact_state["health_ok"] = bool(ok)
    compact_state["health_status"] = status
    return compact_state


def _snapshot_memory_pressure_policy(ts_ms: int) -> dict[str, Any]:
    state = dict(host_memory_pressure_snapshot() or {})
    required = bool(state.get("required"))
    ok = bool(state.get("ok")) or not required
    status = str(state.get("status") or ("pass" if ok else "fail"))
    detail = str(state.get("reason") or status)
    health_extra = {
        key: value
        for key, value in state.items()
        if key not in {"component", "detail", "ok", "status", "updated_ts_ms"}
    }
    health_extra["policy_ok"] = bool(state.get("ok"))
    health_extra["policy_status"] = status
    record_component_health(
        "memory_pressure_policy",
        ok=ok,
        status=status,
        detail=detail,
        observed_ts_ms=ts_ms,
        extra=health_extra,
    )
    state["health_ok"] = bool(ok)
    state["health_status"] = status
    return state


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in _TRUTHY


def _production_storage_alerts_enabled() -> bool:
    if _truthy(os.environ.get("PREFLIGHT_REQUIRE_ZFS_STORAGE")):
        return True
    if _truthy(os.environ.get("PROD_LOCK")) or _truthy(os.environ.get("ENGINE_SUPERVISED")):
        return True
    for name in ("ENV", "APP_ENV", "ENGINE_MODE", "EXECUTION_MODE", "OPERATOR_MODE"):
        if str(os.environ.get(name) or "").strip().lower() in _PRODUCTION_VALUES:
            return True
    return False


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return int(default)
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return int(default)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _storage_alert_fingerprint(*, severity: str, rule_id: str, detail: dict[str, Any]) -> str:
    raw = json.dumps(
        {
            "severity": str(severity or "").strip().upper(),
            "rule_id": str(rule_id or "").strip(),
            "detail": dict(detail or {}),
        },
        default=str,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _clear_inactive_storage_alert_fingerprints(active_rule_ids: set[str]) -> None:
    inactive = [rule_id for rule_id in _STORAGE_ALERT_FINGERPRINTS if rule_id not in active_rule_ids]
    for rule_id in inactive:
        _STORAGE_ALERT_FINGERPRINTS.pop(rule_id, None)


def _emit_wal_alert_state_metric(
    *,
    ts_ms: int,
    enabled: bool,
    blockers: list[str] | None = None,
    warnings: list[str] | None = None,
    emitted: int = 0,
) -> None:
    blocker_count = len(list(blockers or []))
    warning_count = len(list(warnings or []))
    if not enabled:
        status = "disabled"
        value = -1.0
    elif blocker_count:
        status = "critical"
        value = 2.0
    elif warning_count:
        status = "warning"
        value = 1.0
    else:
        status = "ok"
        value = 0.0
    try:
        from engine.runtime.metrics_store import write_runtime_metric

        write_runtime_metric(
            "postgres.wal.alert_state",
            value_num=value,
            value_text=status,
            tags={
                "source": JOB_NAME,
                "enabled": "1" if enabled else "0",
                "blockers": str(blocker_count),
                "warnings": str(warning_count),
                "alerts_emitted": str(int(emitted)),
            },
            ts_ms=ts_ms,
        )
    except Exception:
        return


def _wal_archiver_transition_warnings(wal_archiver: dict[str, Any], *, ts_ms: int) -> list[str]:
    global _WAL_ARCHIVER_LAST_FAILED_COUNT

    warnings: list[str] = []
    failed_count = _safe_int(wal_archiver.get("failed_count"), 0)
    previous_failed_count = _WAL_ARCHIVER_LAST_FAILED_COUNT
    last_failed_wal = str(wal_archiver.get("last_failed_wal") or "")
    last_archived_wal = str(wal_archiver.get("last_archived_wal") or "")
    if previous_failed_count is not None and failed_count > previous_failed_count:
        warnings.append(
            "wal_archiver_failed_count_increased "
            f"previous={previous_failed_count} current={failed_count} "
            f"last_failed_wal={last_failed_wal or 'unknown'}"
        )
    _WAL_ARCHIVER_LAST_FAILED_COUNT = int(failed_count)

    last_failed_ts = _safe_float(wal_archiver.get("last_failed_at_ts"), 0.0)
    last_archived_ts = _safe_float(wal_archiver.get("last_archived_at_ts"), 0.0)
    if last_failed_ts <= 0:
        return warnings

    policy = dict(wal_archiver.get("policy") or {})
    recent_s = _safe_float(
        policy.get("wal_archive_max_age_s")
        or os.environ.get("BACKUP_EVIDENCE_WAL_RPO_S")
        or os.environ.get("BACKUP_EVIDENCE_RPO_S")
        or os.environ.get("BACKUP_RPO_S")
        or 120.0,
        120.0,
    )
    age_s = max(0.0, (float(ts_ms) / 1000.0) - float(last_failed_ts))
    if age_s <= max(1.0, recent_s) and last_archived_ts >= last_failed_ts:
        warnings.append(
            "wal_archiver_recent_failure "
            f"age_s={age_s:.1f} last_failed_wal={last_failed_wal or 'unknown'} "
            f"last_archived_wal={last_archived_wal or 'unknown'}"
        )
    return warnings


def _confirmed_wal_archiver_outage_blockers(wal_archiver: dict[str, Any]) -> list[str]:
    if bool(wal_archiver.get("skipped")):
        return []
    unconfirmed = {
        "wal_archiver_stats_unavailable",
        "wal_archiver_stats_missing",
    }
    confirmed_prefixes = (
        "wal_archiver_failed",
        "wal_archiver_failure_unrecovered",
        "wal_archiver_last_archive_missing",
        "wal_archiver_stale",
        "wal_archiver_timeout",
        "wal_archiver_archive_mode_disabled",
        "wal_archiver_archive_command_unaudited",
    )
    out: list[str] = []
    for raw in list(wal_archiver.get("blockers") or []):
        item = str(raw or "").strip()
        if not item or item in unconfirmed or item.startswith("wal_archiver_probe_failed:"):
            continue
        if any(item.startswith(prefix) for prefix in confirmed_prefixes):
            out.append(item)
    return out


def _emit_storage_runtime_alert(
    *,
    ts_ms: int,
    severity: str,
    rule_id: str,
    title: str,
    detail: dict[str, Any],
) -> int:
    fingerprint = _storage_alert_fingerprint(severity=severity, rule_id=rule_id, detail=detail)
    if _STORAGE_ALERT_FINGERPRINTS.get(rule_id) == fingerprint:
        return 0
    try:
        from engine.runtime.alerts import emit_runtime_alert

        result = emit_runtime_alert(
            event_title=title,
            symbol="SYSTEM",
            severity=severity,
            rule_id=rule_id,
            horizon_s=0,
            expected_z=0.0,
            confidence=1.0,
            explain={"source": JOB_NAME, "rule_id": rule_id},
            detail=detail,
            source=JOB_NAME,
            dedupe_scope=f"{rule_id}:{fingerprint[:16]}",
            ts_ms=ts_ms,
            return_details=True,
        )
        _STORAGE_ALERT_FINGERPRINTS[rule_id] = fingerprint
        result_dict = dict(result or {})
        inserted = bool(result_dict.get("inserted"))
        if inserted:
            try:
                from engine.runtime.alerts_notify import send_runtime_alert_notification

                send_runtime_alert_notification(
                    dict(result_dict.get("payload") or {}),
                    actor="system",
                    source=JOB_NAME,
                )
            except Exception:  # no-op-guard: allow - alert row is already recorded; notification is best effort.
                pass
        return 1 if inserted else 0
    except Exception:
        return 0


def _emit_storage_wal_alerts(ts_ms: int) -> dict[str, Any]:
    production_alerts_enabled = _production_storage_alerts_enabled()
    emitted = 0
    snapshots: dict[str, Any] = {
        "enabled": True,
        "production_storage_alerts_enabled": bool(production_alerts_enabled),
    }
    blockers: list[str] = []
    warnings: list[str] = []
    active_rule_ids: set[str] = set()

    if production_alerts_enabled:
        try:
            from engine.runtime.storage_placement import check_storage_placement

            storage = dict(check_storage_placement() or {})
        except Exception as exc:
            storage = {"ok": False, "errors": [f"storage_placement_probe_failed:{type(exc).__name__}: {exc}"]}
        snapshots["storage_placement"] = storage
        storage_errors = [str(item) for item in list(storage.get("errors") or []) if str(item).strip()]
        storage_warnings = [str(item) for item in list(storage.get("warnings") or []) if str(item).strip()]
        if storage_errors:
            blockers.extend(f"storage_placement:{item}" for item in storage_errors)
            active_rule_ids.add("STORAGE_PLACEMENT_INVALID")
            emitted += _emit_storage_runtime_alert(
                ts_ms=ts_ms,
                severity="CRIT",
                rule_id="STORAGE_PLACEMENT_INVALID",
                title="Storage placement invalid",
                detail={"errors": storage_errors, "storage_placement": storage},
            )
        elif storage_warnings:
            warnings.extend(f"storage_placement:{item}" for item in storage_warnings)
    else:
        snapshots["storage_placement"] = {"skipped": True, "reason": "production_storage_alerts_disabled"}

    try:
        from engine.runtime.backup_evidence import pg_wal_disk_risk_snapshot, wal_archiver_runtime_snapshot

        wal_archiver = dict(
            wal_archiver_runtime_snapshot(
                engine_mode=os.environ.get("ENGINE_MODE", "safe"),
                required=True,
            )
            or {}
        )
        if production_alerts_enabled:
            pg_wal = dict(
                pg_wal_disk_risk_snapshot(
                    engine_mode=os.environ.get("ENGINE_MODE", "safe"),
                    required=True,
                )
                or {}
            )
        else:
            pg_wal = {"skipped": True, "reason": "production_storage_alerts_disabled"}
    except Exception as exc:
        wal_archiver = {"ok": False, "blockers": [f"wal_archiver_probe_failed:{type(exc).__name__}: {exc}"]}
        pg_wal = (
            {"ok": False, "blockers": [f"pg_wal_probe_failed:{type(exc).__name__}: {exc}"]}
            if production_alerts_enabled
            else {"skipped": True, "reason": "production_storage_alerts_disabled"}
        )
    snapshots["wal_archiver_runtime"] = wal_archiver
    snapshots["pg_wal_disk_risk"] = pg_wal

    wal_blockers = [str(item) for item in list(wal_archiver.get("blockers") or []) if str(item).strip()]
    wal_warnings = [str(item) for item in list(wal_archiver.get("warnings") or []) if str(item).strip()]
    if production_alerts_enabled:
        wal_warnings.extend(_wal_archiver_transition_warnings(wal_archiver, ts_ms=ts_ms))
    wal_alert_blockers = (
        wal_blockers
        if production_alerts_enabled
        else _confirmed_wal_archiver_outage_blockers(wal_archiver)
    )
    if wal_alert_blockers:
        blockers.extend(f"wal_archiver:{item}" for item in wal_alert_blockers)
        active_rule_ids.add("WAL_ARCHIVER_OUTAGE")
        emitted += _emit_storage_runtime_alert(
            ts_ms=ts_ms,
            severity="CRIT",
            rule_id="WAL_ARCHIVER_OUTAGE",
            title="WAL archiver outage risk",
            detail={
                "blockers": wal_alert_blockers,
                "last_failed_wal": str(wal_archiver.get("last_failed_wal") or ""),
                "last_archived_wal": str(wal_archiver.get("last_archived_wal") or ""),
                "failed_count": _safe_int(wal_archiver.get("failed_count"), 0),
                "wal_archiver_runtime": wal_archiver,
            },
        )
    elif production_alerts_enabled and wal_warnings:
        warnings.extend(f"wal_archiver:{item}" for item in wal_warnings)

    pg_wal_blockers = [str(item) for item in list(pg_wal.get("blockers") or []) if str(item).strip()]
    pg_wal_warnings = [str(item) for item in list(pg_wal.get("warnings") or []) if str(item).strip()]
    if production_alerts_enabled and pg_wal_blockers:
        blockers.extend(f"pg_wal:{item}" for item in pg_wal_blockers)
        active_rule_ids.add("PG_WAL_DISK_RISK")
        emitted += _emit_storage_runtime_alert(
            ts_ms=ts_ms,
            severity="CRIT",
            rule_id="PG_WAL_DISK_RISK",
            title="pg_wal disk risk",
            detail={
                "blockers": pg_wal_blockers,
                "wal_bytes": _safe_int(pg_wal.get("wal_bytes"), 0),
                "ready_count": _safe_int(pg_wal.get("ready_count"), 0),
                "local_space": dict(pg_wal.get("local_space") or {}),
                "pg_wal_disk_risk": pg_wal,
            },
        )
    elif production_alerts_enabled and pg_wal_warnings:
        warnings.extend(f"pg_wal:{item}" for item in pg_wal_warnings)

    try:
        from engine.runtime.health import get_disk_pressure_snapshot
        from engine.runtime.storage_placement import storage_pressure_paths

        disk_pressure = dict(get_disk_pressure_snapshot(storage_pressure_paths(os.environ)) or {})
    except Exception as exc:
        disk_pressure = {"ok": False, "critical": [f"disk_pressure_probe_failed:{type(exc).__name__}: {exc}"]}
    snapshots["disk_pressure"] = disk_pressure
    disk_critical = [str(item) for item in list(disk_pressure.get("critical") or []) if str(item).strip()]
    disk_warnings = [str(item) for item in list(disk_pressure.get("warnings") or []) if str(item).strip()]
    if disk_critical:
        blockers.extend(f"free_space:{item}" for item in disk_critical)
        active_rule_ids.add("STORAGE_FREE_SPACE_CRITICAL")
        emitted += _emit_storage_runtime_alert(
            ts_ms=ts_ms,
            severity="CRIT",
            rule_id="STORAGE_FREE_SPACE_CRITICAL",
            title="Storage free space critical",
            detail={"critical": disk_critical, "disk_pressure": disk_pressure},
        )
    elif disk_warnings:
        warnings.extend(f"free_space:{item}" for item in disk_warnings)

    if production_alerts_enabled and not blockers and warnings:
        active_rule_ids.add("STORAGE_WAL_WARNING")
        emitted += _emit_storage_runtime_alert(
            ts_ms=ts_ms,
            severity="WARN",
            rule_id="STORAGE_WAL_WARNING",
            title="Storage/WAL warning",
            detail={"warnings": warnings, "snapshots": snapshots},
        )

    ok = not blockers
    _clear_inactive_storage_alert_fingerprints(active_rule_ids)
    _emit_wal_alert_state_metric(
        ts_ms=ts_ms,
        enabled=True,
        blockers=blockers,
        warnings=warnings,
        emitted=emitted,
    )
    record_component_health(
        "storage_wal_guards",
        ok=ok,
        status=("ok" if ok else "error"),
        detail=("ok" if ok else "; ".join(blockers[:5])),
        observed_ts_ms=ts_ms,
        extra={
            "alerts_emitted": int(emitted),
            "warnings": warnings[:10],
            "blockers": blockers[:10],
        },
    )
    snapshots["ok"] = ok
    snapshots["emitted"] = int(emitted)
    snapshots["warnings"] = warnings
    snapshots["blockers"] = blockers
    return snapshots


def run_once() -> dict[str, Any]:
    ts_ms = int(time.time() * 1000)
    snapshot = snapshot_pg_observability(ts_ms=ts_ms)
    redis_emitted = _emit_redis_circuit_state(ts_ms)
    cpu_power_policy = _snapshot_cpu_power_policy(ts_ms)
    memory_pressure_policy = _snapshot_memory_pressure_policy(ts_ms)
    storage_wal_alerts = _emit_storage_wal_alerts(ts_ms)
    ok = bool(snapshot.get("ok"))
    record_component_health(
        JOB_NAME,
        ok=ok,
        status=("ok" if ok else "error"),
        detail=str(snapshot.get("reason") or ""),
        observed_ts_ms=ts_ms,
        extra={
            "emitted": int(snapshot.get("emitted") or 0) + int(redis_emitted),
            "skipped": bool(snapshot.get("skipped")),
            "memory_pressure_ok": bool(memory_pressure_policy.get("health_ok", True)),
            "storage_wal_guards_ok": bool(storage_wal_alerts.get("ok", True)),
        },
    )
    out = dict(snapshot)
    out["redis_emitted"] = int(redis_emitted)
    out["cpu_power_policy"] = dict(cpu_power_policy or {})
    out["memory_pressure_policy"] = dict(memory_pressure_policy or {})
    out["storage_wal_alerts"] = dict(storage_wal_alerts or {})
    return out


def main() -> int:
    init_db()
    run_once_mode = str(os.environ.get("OBSERVABILITY_SNAPSHOT_RUN_ONCE", "0") or "0").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )

    if os.environ.get("ENGINE_SUPERVISED") != "1" or run_once_mode:
        print(json.dumps(run_once(), separators=(",", ":"), sort_keys=True))
        return 0

    if not acquire_job_lock(JOB_NAME, OWNER, PID, stale_after_s=LOCK_STALE_AFTER_S):
        print("observability_snapshot lock already held")
        return 2

    last_hb_s = 0.0
    try:
        _ensure_slow_log_tail()
        while True:
            now_s = time.time()
            if (now_s - last_hb_s) >= HEARTBEAT_EVERY_S:
                last_hb_s = now_s
                touch_job_lock(JOB_NAME, OWNER, PID)
                put_job_heartbeat(
                    JOB_NAME,
                    OWNER,
                    PID,
                    extra_json=json.dumps(
                        {
                            "interval_s": float(INTERVAL_S),
                            "heartbeat_every_s": float(HEARTBEAT_EVERY_S),
                        },
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                )

            print(json.dumps(run_once(), separators=(",", ":"), sort_keys=True))
            time.sleep(max(1.0, float(INTERVAL_S)))
    finally:
        _SLOW_LOG_STOP.set()
        release_job_lock(JOB_NAME, OWNER, PID)


if __name__ == "__main__":
    raise SystemExit(main())
