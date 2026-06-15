"""
Supervised daemon that keeps live inference/data-quality gates fresh.

The runtime health gate is intentionally based on real inference calls rather
than synthetic writes. This job periodically asks the production inference
engine to score one or more configured symbols; the inference path records
feature validation, model-input validation, component health, prediction
tracking, and scoring-pipeline status.
"""

from __future__ import annotations

import json
import os
import sys
import time
from typing import Any

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import (
    acquire_job_lock,
    init_db,
    put_job_heartbeat,
    release_job_lock,
    touch_job_lock,
)


JOB_NAME = "inference_health_probe"
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()

ENABLED = str(os.environ.get("INFERENCE_HEALTH_PROBE_ENABLED", "1")).strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
SYMBOLS = [
    symbol.strip().upper()
    for symbol in str(os.environ.get("INFERENCE_HEALTH_PROBE_SYMBOLS", "AMD")).split(",")
    if symbol.strip()
]
INTERVAL_S = max(5.0, float(os.environ.get("INFERENCE_HEALTH_PROBE_INTERVAL_S", "20")))
TIMEOUT_S = max(2.0, float(os.environ.get("INFERENCE_HEALTH_PROBE_TIMEOUT_S", "20")))
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))
LOG = get_logger("engine.runtime.jobs.inference_health_probe")


def _warn_nonfatal(code: str, error: BaseException, **extra: Any) -> None:
    log_failure(
        LOG,
        event="inference_health_probe_nonfatal",
        code=str(code),
        message=str(code),
        error=error,
        level=30,
        component="engine.runtime.jobs.inference_health_probe",
        extra=extra or None,
        persist=False,
    )


def _run_probe() -> dict[str, Any]:
    from engine.execution.execution_broker_watchdog import refresh_broker_connection_health
    from engine.execution.execution_quality_supervisor import refresh_execution_quality_supervisor
    from engine.inference_engine import predict

    started_ms = int(time.time() * 1000)
    rows: list[dict[str, Any]] = []
    ok_count = 0
    broker_health: dict[str, Any] = {}
    execution_quality: dict[str, Any] = {}

    try:
        broker_health = dict(refresh_broker_connection_health() or {})
    except Exception as exc:
        _warn_nonfatal("INFERENCE_HEALTH_PROBE_BROKER_REFRESH_FAILED", exc)

    try:
        execution_quality = dict(refresh_execution_quality_supervisor() or {})
    except Exception as exc:
        _warn_nonfatal("INFERENCE_HEALTH_PROBE_EXECUTION_QUALITY_REFRESH_FAILED", exc)

    for symbol in list(SYMBOLS or []):
        try:
            result = dict(predict(str(symbol), persist=True, timeout_s=float(TIMEOUT_S)) or {})
            ok = str(result.get("status") or "").lower() == "ok" and not bool(result.get("safe_output"))
            rows.append(
                {
                    "symbol": str(symbol),
                    "ok": bool(ok),
                    "status": str(result.get("status") or ""),
                    "model_name": str(result.get("model_name") or ""),
                    "model_version": str(result.get("model_version") or ""),
                    "model_kind": str(result.get("model_kind") or ""),
                    "safe_output": bool(result.get("safe_output")),
                    "fallback_reason": str(result.get("fallback_reason") or ""),
                    "prediction_ts_ms": int(result.get("ts_ms") or 0),
                }
            )
            if ok:
                ok_count += 1
        except Exception as exc:
            _warn_nonfatal(
                "INFERENCE_HEALTH_PROBE_SYMBOL_FAILED",
                exc,
                symbol=str(symbol),
            )
            rows.append(
                {
                    "symbol": str(symbol),
                    "ok": False,
                    "status": "error",
                    "error": f"{type(exc).__name__}:{exc}",
                }
            )

    return {
        "ok": bool(ok_count > 0),
        "ts_ms": int(time.time() * 1000),
        "latency_ms": int(time.time() * 1000) - int(started_ms),
        "symbols": list(SYMBOLS or []),
        "ok_count": int(ok_count),
        "broker_connection": {
            "ok": bool(broker_health.get("ok")),
            "broker": str(broker_health.get("broker") or ""),
            "state": str(broker_health.get("state") or ""),
            "detail": str(broker_health.get("detail") or broker_health.get("error") or ""),
            "ts_ms": int(broker_health.get("ts_ms") or 0),
        },
        "execution_quality": {
            "ok": bool(execution_quality.get("ok")),
            "state": str(execution_quality.get("state") or ""),
            "failed_gates": list(execution_quality.get("failed_gates") or []),
            "ts_ms": int(execution_quality.get("ts_ms") or 0),
        },
        "rows": rows,
    }


def main() -> None:
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("inference_health_probe must be launched by supervisor")
        raise SystemExit(1)

    init_db()

    if not ENABLED:
        print("inference_health_probe disabled")
        raise SystemExit(0)

    if not SYMBOLS:
        print("inference_health_probe has no symbols configured", file=sys.stderr, flush=True)
        raise SystemExit(2)

    if not acquire_job_lock(JOB_NAME, OWNER, PID, stale_after_s=LOCK_STALE_AFTER_S):
        print("inference_health_probe lock already held")
        raise SystemExit(2)

    try:
        while True:
            payload: dict[str, Any] = {"phase": "start", "symbols": list(SYMBOLS or [])}
            try:
                touch_job_lock(JOB_NAME, OWNER, PID)
                payload = _run_probe()
            except KeyboardInterrupt:
                raise
            except SystemExit:
                raise
            except Exception as exc:
                _warn_nonfatal("INFERENCE_HEALTH_PROBE_LOOP_FAILED", exc)
                payload = {
                    "ok": False,
                    "ts_ms": int(time.time() * 1000),
                    "error": f"{type(exc).__name__}:{exc}",
                    "symbols": list(SYMBOLS or []),
                }
            finally:
                try:
                    touch_job_lock(JOB_NAME, OWNER, PID)
                    put_job_heartbeat(
                        JOB_NAME,
                        OWNER,
                        PID,
                        extra_json=json.dumps(payload, separators=(",", ":"), sort_keys=True),
                    )
                except Exception as exc:
                    _warn_nonfatal("INFERENCE_HEALTH_PROBE_HEARTBEAT_FAILED", exc)

            time.sleep(float(INTERVAL_S))
    finally:
        try:
            release_job_lock(JOB_NAME, OWNER, PID)
        except Exception as exc:
            _warn_nonfatal("INFERENCE_HEALTH_PROBE_LOCK_RELEASE_FAILED", exc)


if __name__ == "__main__":
    main()
