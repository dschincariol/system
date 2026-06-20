"""Periodically re-prime the Redis kill-switch snapshot from storage."""

from __future__ import annotations

import json
import os
import time
from typing import Any

from engine.cache.wrappers.kill_switch import refresh_kill_switch_cache
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import (
    acquire_job_lock,
    init_db,
    put_job_heartbeat,
    release_job_lock,
    touch_job_lock,
)


JOB_NAME = "kill_switch_cache_refresh"
OWNER = os.environ.get("JOB_OWNER", os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")))
PID = os.getpid()
INTERVAL_S = max(1.0, float(os.environ.get("KILL_SWITCH_CACHE_REPRIME_INTERVAL_S", "10") or 10.0))
HEARTBEAT_EVERY_S = max(1.0, float(os.environ.get("KILL_SWITCH_CACHE_REPRIME_HEARTBEAT_S", "10") or 10.0))
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180") or 180)
LOG = get_logger("runtime.jobs.kill_switch_cache_refresh")


def _now_ms() -> int:
    return int(time.time() * 1000)


def _warn_nonfatal(code: str, error: BaseException, **extra: Any) -> None:
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=30,
        component="engine.runtime.jobs.kill_switch_cache_refresh",
        extra=dict(extra or {}) or None,
        include_health=False,
        persist=False,
    )


def _heartbeat(payload: dict[str, Any]) -> None:
    put_job_heartbeat(
        JOB_NAME,
        OWNER,
        PID,
        extra_json=json.dumps(payload, separators=(",", ":"), sort_keys=True, default=str),
        best_effort=True,
    )


def _run_once() -> dict[str, Any]:
    snapshot = refresh_kill_switch_cache()
    return {
        "ok": True,
        "ts_ms": _now_ms(),
        "source": str(snapshot.get("source") or ""),
        "read_source": str(snapshot.get("read_source") or ""),
        "cache_status": str(snapshot.get("cache_status") or ""),
        "cache_age_ms": snapshot.get("cache_age_ms"),
        "cache_fresh": bool(snapshot.get("cache_fresh")),
        "max_age_ms": snapshot.get("max_age_ms"),
        "active_count": sum(
            1
            for row in list(snapshot.get("state") or [])
            if isinstance(row, dict) and int(row.get("enabled") or 0) == 1
        ),
    }


def main() -> int:
    init_db()
    run_once = str(os.environ.get("KILL_SWITCH_CACHE_REPRIME_RUN_ONCE", "0") or "0").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if os.environ.get("ENGINE_SUPERVISED") != "1" or run_once:
        payload = _run_once()
        print(json.dumps(payload, indent=2, sort_keys=True, default=str))
        return 0 if bool(payload.get("ok")) else 1

    if not acquire_job_lock(JOB_NAME, OWNER, PID, stale_after_s=LOCK_STALE_AFTER_S):
        print("kill_switch_cache_refresh lock already held")
        return 2

    last_hb_s = 0.0
    payload: dict[str, Any] = {"ok": False, "ts_ms": _now_ms(), "cache_status": "starting"}
    try:
        while True:
            try:
                now_s = time.time()
                touch_job_lock(JOB_NAME, OWNER, PID, best_effort=True)
                payload = _run_once()
                if (now_s - last_hb_s) >= HEARTBEAT_EVERY_S:
                    last_hb_s = now_s
                    _heartbeat(payload)
            except Exception as exc:
                payload = {
                    "ok": False,
                    "ts_ms": _now_ms(),
                    "cache_status": "refresh_error",
                    "error": f"{type(exc).__name__}: {exc}",
                }
                _warn_nonfatal("KILL_SWITCH_CACHE_REFRESH_LOOP_FAILED", exc)
                try:
                    _heartbeat(payload)
                except Exception as heartbeat_exc:
                    _warn_nonfatal("KILL_SWITCH_CACHE_REFRESH_HEARTBEAT_FAILED", heartbeat_exc)
            time.sleep(float(INTERVAL_S))
    finally:
        release_job_lock(JOB_NAME, OWNER, PID)


if __name__ == "__main__":
    raise SystemExit(main())
