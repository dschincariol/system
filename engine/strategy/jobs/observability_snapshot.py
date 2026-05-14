"""Periodic Postgres, PgBouncer, and slow-log observability snapshot."""

from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Any

from engine.runtime.observability import record_component_health
from engine.runtime.observability.pg_stats import snapshot_pg_observability
from engine.runtime.observability.slow_log import start_slow_log_tail_thread
from engine.runtime.platform import is_linux
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


def _default_slow_log_path() -> str:
    configured = str(os.environ.get("OBSERVABILITY_POSTGRES_LOG") or "").strip()
    if configured:
        return configured
    if is_linux():
        return "/var/log/postgresql/postgresql-16-main.log"
    return ""


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


def run_once() -> dict[str, Any]:
    ts_ms = int(time.time() * 1000)
    snapshot = snapshot_pg_observability(ts_ms=ts_ms)
    redis_emitted = _emit_redis_circuit_state(ts_ms)
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
        },
    )
    out = dict(snapshot)
    out["redis_emitted"] = int(redis_emitted)
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
