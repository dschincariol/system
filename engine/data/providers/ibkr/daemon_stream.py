from __future__ import annotations
"""
IBKR Market Data Streaming Daemon

Compatibility wrapper around the canonical provider-session daemon.
"""

"""
FILE: daemon_stream.py

Market-data provider integration module for `daemon_stream`.
"""

import json
import logging
import os
import signal
import threading

from engine.data.provider_sessions import IBKRSession, ProviderSessionManager
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.lifecycle_state import DEGRADED, LIVE, WARMING_UP, set_state
from engine.runtime.logging import get_logger
from engine.runtime.platform import default_ibkr_host
from engine.runtime.runtime_meta import meta_set
from engine.runtime.storage import (
    acquire_job_lock,
    init_db,
    put_job_heartbeat,
    release_job_lock,
    touch_job_lock,
)
from services.data_source_manager import get_manager

JOB_NAME = "stream_prices_ibkr"
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))
PROVIDER_NAME = "ibkr"
HEARTBEAT_EVERY_S = float(os.environ.get("STREAM_PRICES_HEARTBEAT_S", "2.0"))
DEAD_AFTER_MS = int(os.environ.get("IBKR_STREAM_DEAD_AFTER_MS", os.environ.get("POLL_PROVIDER_DEAD_AFTER_MS", "15000")))
RECONNECT_BASE_S = float(os.environ.get("IBKR_STREAM_RECONNECT_BASE_S", "1.0"))
RECONNECT_MAX_S = float(os.environ.get("IBKR_STREAM_RECONNECT_MAX_S", "30.0"))
STARTUP_GRACE_MS = int(os.environ.get("IBKR_STREAM_STARTUP_GRACE_MS", "30000"))

log = get_logger("runtime.stream_prices_ibkr")
_STOP_EVENT = threading.Event()
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        log,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=30,
        component="engine.data.providers.ibkr.daemon_stream",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _request_stop(signum=None, _frame=None) -> None:
    try:
        log_failure(
            log,
            event="ibkr_daemon_stop_requested",
            code="IBKR_DAEMON_STOP_REQUESTED",
            message="IBKR stream stop requested.",
            error=None,
            level=logging.WARNING,
            component="engine.data.providers.ibkr.daemon_stream",
            extra={"signal": signum},
            persist=False,
        )
    except Exception as e:
        _warn_nonfatal("IBKR_DAEMON_STOP_LOG_FAILED", e, once_key="request_stop_log", signal=signum)
    _STOP_EVENT.set()


for _sig in (getattr(signal, "SIGINT", None), getattr(signal, "SIGTERM", None)):
    if _sig is None:
        continue
    try:
        signal.signal(_sig, _request_stop)
    except Exception as e:
        _warn_nonfatal("IBKR_DAEMON_SIGNAL_REGISTER_FAILED", e, once_key=f"signal_register_{_sig}")


def _build_manager() -> ProviderSessionManager:
    host = str(os.environ.get("IBKR_HOST", default_ibkr_host()) or default_ibkr_host()).strip()
    port = int(str(os.environ.get("IBKR_PORT", "7497") or "7497").strip())
    client_id = int(str(os.environ.get("IBKR_CLIENT_ID", "1") or "1").strip())
    data_type = int(str(os.environ.get("IBKR_MARKET_DATA_TYPE", "1") or "1").strip())

    # The session manager is the real control plane for liveness, reconnects,
    # symbol reconciliation, and gap fill. The daemon mostly wires config and
    # exports heartbeats/lifecycle state.
    session = IBKRSession(host=host, port=port, client_id=client_id, data_type=data_type)
    return ProviderSessionManager(
        session,
        provider_name=PROVIDER_NAME,
        heartbeat_interval_s=max(1.0, HEARTBEAT_EVERY_S),
        dead_after_ms=DEAD_AFTER_MS,
        reconnect_base_s=RECONNECT_BASE_S,
        reconnect_max_s=RECONNECT_MAX_S,
        startup_grace_ms=STARTUP_GRACE_MS,
    )


def main() -> None:
    source_manager = get_manager()
    if not source_manager.is_job_enabled(JOB_NAME, default=True):
        source_manager.record_job_status(JOB_NAME, ok=True, message="ibkr stream disabled by data source control plane")
        return

    init_db()

    if not acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
        raise SystemExit(2)

    manager = None
    try:
        set_state(WARMING_UP, "ibkr_session_starting")
        manager = _build_manager()
        manager.start()

        while not _STOP_EVENT.wait(timeout=HEARTBEAT_EVERY_S):
            if not source_manager.is_job_enabled(JOB_NAME, default=True):
                source_manager.record_job_status(JOB_NAME, ok=True, message="ibkr stream disabled by data source control plane")
                break
            touch_job_lock(JOB_NAME, OWNER, PID)
            telemetry = manager.provider_telemetry() or {}
            ok = bool(manager.ok())
            if ok:
                meta_set("price_provider_active", PROVIDER_NAME, best_effort=True)
                set_state(LIVE, "ibkr_stream_active")
            else:
                # These branches keep lifecycle state informative during warmup:
                # connected/authenticated-without-ticks is very different from
                # fully disconnected or misconfigured startup.
                connected = bool(telemetry.get("connected"))
                authenticated = bool(telemetry.get("authenticated"))
                desired_count = int(telemetry.get("desired_symbol_count") or 0)
                subscribed_count = int(telemetry.get("subscribed_symbol_count") or 0)
                if connected and authenticated and desired_count > 0 and subscribed_count > 0:
                    set_state(WARMING_UP, "ibkr_authenticated_waiting_for_first_tick")
                elif connected and desired_count <= 0:
                    set_state(DEGRADED, "ibkr_no_symbols_subscribed")
                else:
                    set_state(WARMING_UP, "ibkr_session_connecting")

            put_job_heartbeat(
                JOB_NAME,
                OWNER,
                PID,
                extra_json=json.dumps(
                    {
                        "provider": PROVIDER_NAME,
                        "ok": ok,
                        "telemetry": telemetry,
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ),
            )
            source_manager.record_job_status(
                JOB_NAME,
                ok=bool(ok),
                message="ibkr stream heartbeat",
                error=str((telemetry or {}).get("last_error") or ""),
                meta={"telemetry": telemetry},
            )
    finally:
        try:
            if manager is not None:
                manager.close()
        except Exception as e:
            _warn_nonfatal("IBKR_DAEMON_MANAGER_CLOSE_FAILED", e, once_key="manager_close")
        try:
            release_job_lock(JOB_NAME, OWNER, PID)
        except Exception as e:
            _warn_nonfatal("IBKR_DAEMON_RELEASE_LOCK_FAILED", e, once_key="release_lock", job_name=JOB_NAME)


if __name__ == "__main__":
    main()
