"""
FILE: lifecycle.py

Runtime subsystem module for `lifecycle`.
"""

# engine/runtime/lifecycle.py
"""
Global Runtime Lifecycle State Machine

States:
  BOOTING
  WARMING
  LIVE
  DEGRADED
  KILL
  SHUTDOWN
"""

import os
import logging
import threading
import time
from typing import Callable, Dict, Optional

from engine.runtime.logging import get_logger, log_event
from engine.runtime.runtime_meta import meta_get
from engine.runtime.lifecycle_state import (
    get_state as _lc_get_state,
    set_state as _lc_set_state,
    BOOTING,
    WARMING_UP,
    LIVE,
    DEGRADED,
    KILL_SWITCH,
    SHUTTING_DOWN,
)


LOG = get_logger("runtime.lifecycle")
_TRACE_DEPENDENCIES = str(os.environ.get("LIFECYCLE_MONITOR_TRACE", "")).strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)


def _trace_dependency(name: str, started: float, *, extra: Optional[Dict[str, object]] = None) -> None:
    if not _TRACE_DEPENDENCIES:
        return
    payload = {
        "dependency": str(name),
        "elapsed_ms": round((time.perf_counter() - float(started)) * 1000.0, 2),
    }
    if extra:
        payload.update({str(k): v for k, v in dict(extra).items()})
    log_event(
        LOG,
        20,
        "lifecycle_monitor_dependency_trace",
        component="engine.runtime.lifecycle",
        extra=payload,
    )


def _kill_enabled(kill: Dict) -> bool:
    if not isinstance(kill, dict):
        return bool(kill)

    rows = kill.get("state")
    if isinstance(rows, list):
        for row in rows:
            if isinstance(row, dict) and int(row.get("enabled") or 0) == 1:
                return True

    if isinstance(kill.get("kill_switches"), dict):
        for row in (kill.get("kill_switches") or {}).values():
            if isinstance(row, dict):
                if int(row.get("enabled") or 0) == 1:
                    return True
            elif bool(row):
                return True

    if kill.get("enabled") is True:
        return True

    return False


def _now_ms() -> int:
    return int(time.time() * 1000)


def _warmup_started_ms(current: Dict) -> int:
    raw = str((current or {}).get("warmup_started_ts_ms") or meta_get("warmup_started_ts_ms", "") or "").strip()
    if raw:
        try:
            return int(raw)
        except Exception as e:
            log_event(
                LOG,
                logging.WARNING,
                "LIFECYCLE_TS_PARSE_FAILED",
                component="engine.runtime.lifecycle",
                extra={"raw": str(raw), "error": f"{type(e).__name__}: {e}"},
            )
            return 0
    raw = str((current or {}).get("updated_ts_ms") or 0).strip()
    try:
        return int(raw or 0)
    except Exception as e:
        log_event(
            LOG,
            logging.WARNING,
            "LIFECYCLE_UPDATED_TS_PARSE_FAILED",
            component="engine.runtime.lifecycle",
            extra={"raw": str(raw), "error": f"{type(e).__name__}: {e}"},
        )
        return 0


def snapshot() -> Dict:
    return dict(_lc_get_state() or {})


def lifecycle_snapshot() -> Dict:
    return snapshot()


def mark_shutdown():
    _lc_set_state(SHUTTING_DOWN, "lifecycle_monitor_shutdown")


def start_lifecycle_monitor(
    get_health: Callable[[], Dict],
    get_jobs: Callable[[], list],
    get_kill_switches: Callable[[], Dict],
    interval_s: float = 2.0,
    stop_event: Optional[threading.Event] = None,
    claim_booting: bool = True,
):
    """
    Background thread that computes lifecycle state continuously.
    """

    warmup_timeout_ms = max(1000, int(float(os.environ.get("WARMUP_TIMEOUT_S", "120")) * 1000.0))
    sleep_interval_s = max(0.05, float(interval_s))

    def _run_iteration() -> None:
        try:
            started = time.perf_counter()
            health = get_health() or {}
            _trace_dependency(
                "get_health",
                started,
                extra={
                    "health_ok": bool(isinstance(health, dict) and health.get("ok")),
                    "reason_count": len((health or {}).get("reasons") or []) if isinstance(health, dict) else 0,
                },
            )

            started = time.perf_counter()
            jobs = get_jobs() or []
            _trace_dependency("get_jobs", started, extra={"job_count": len(jobs or [])})

            started = time.perf_counter()
            kill = get_kill_switches() or {}
            _trace_dependency(
                "get_kill_switches",
                started,
                extra={
                    "kill_enabled": bool(_kill_enabled(kill)),
                    "has_state_rows": bool(isinstance(kill, dict) and kill.get("state")),
                },
            )
            current = dict(_lc_get_state() or {})

            if _kill_enabled(kill):
                _lc_set_state(KILL_SWITCH, "kill_switch_active")
            elif str(current.get("state") or "") == SHUTTING_DOWN:
                pass
            else:
                ingestion_freshness = dict((health or {}).get("ingestion_freshness") or {})
                critical_freshness_stale = bool(ingestion_freshness.get("degraded"))
                freshness_reason_codes = [
                    str(code or "").strip()
                    for code in (ingestion_freshness.get("runtime_reason_codes") or [])
                    if str(code or "").strip()
                ]
                prices_ok = bool((health.get("prices") or {}).get("ok"))
                first_tick = str(current.get("first_price_ts_ms") or "").strip()
                current_state = str(current.get("state") or "").strip().upper()
                started_ms = _warmup_started_ms(current)
                elapsed_ms = max(0, _now_ms() - int(started_ms or _now_ms()))
                freshness_detail = ",".join(freshness_reason_codes[:3]) if freshness_reason_codes else ""

                if first_tick and prices_ok and not critical_freshness_stale:
                    live_detail = "market_data_healthy" if current_state == LIVE else "first_market_data_tick"
                    _lc_set_state(LIVE, live_detail)
                elif not first_tick and elapsed_ms < warmup_timeout_ms:
                    _lc_set_state(
                        WARMING_UP,
                        "awaiting_first_price_tick",
                    )
                elif not first_tick:
                    detail = freshness_detail or "warmup_timeout_awaiting_first_price_tick"
                    _lc_set_state(DEGRADED, detail)
                else:
                    if critical_freshness_stale:
                        detail = freshness_detail or "critical_ingestion_stale"
                    elif first_tick and prices_ok:
                        detail = "market_data_healthy"
                    else:
                        _lc_set_state(
                            WARMING_UP,
                            "awaiting_first_price_tick",
                        )
                        return
                    _lc_set_state(DEGRADED if critical_freshness_stale else LIVE, detail)

        except Exception as e:
            _lc_set_state(DEGRADED, f"lifecycle_monitor_error:{type(e).__name__}:{e}")
            log_event(
                LOG,
                40,
                "lifecycle_monitor_error",
                component="engine.runtime.lifecycle",
                extra={"error": f"{type(e).__name__}: {e}"},
            )

    def _loop():
        log_event(
            LOG,
            20,
            "lifecycle_monitor_started",
            component="engine.runtime.lifecycle",
            extra={
                "interval_s": float(sleep_interval_s),
                "warmup_timeout_ms": int(warmup_timeout_ms),
                "claim_booting": bool(claim_booting),
            },
        )

        while not (stop_event.is_set() if stop_event is not None else False):
            if stop_event is not None:
                if stop_event.wait(timeout=float(sleep_interval_s)):
                    break
            else:
                time.sleep(float(sleep_interval_s))

            _run_iteration()

        log_event(
            LOG,
            20,
            "lifecycle_monitor_stopped",
            component="engine.runtime.lifecycle",
            extra={},
        )

    if claim_booting:
        _lc_set_state(BOOTING, "lifecycle_monitor_start")
    _run_iteration()

    t = threading.Thread(target=_loop, daemon=True, name="lifecycle_monitor")
    t.start()
    return t
