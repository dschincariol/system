"""
FILE: session_manager.py

Provider session management module for `session_manager`.
"""

import json
import logging
import random
import threading
import os
import time
from typing import Any, Dict, Iterable, Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.metrics import emit_counter, emit_gauge, emit_timing
from engine.runtime.runtime_meta import meta_set
from engine.runtime.tracing import trace_event
from engine.data.default_symbols import load_default_symbols

from .base_session import BaseProviderSession, now_ms


log = logging.getLogger("provider_session_manager")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(provider_name: str, code: str, error: Exception, *, once_key: str | None = None, **extra: Any) -> None:
    key = str(once_key or "")
    if key:
        if key in _WARNED_NONFATAL_KEYS:
            return
        _WARNED_NONFATAL_KEYS.add(key)
    log_failure(
        log,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.data.provider_sessions.session_manager",
        extra={"provider": str(provider_name), **(extra or {})},
        include_health=False,
        persist=False,
    )


def _warn_state(provider_name: str, code: str, message: str, **extra: Any) -> None:
    log_failure(
        log,
        event=str(code).lower(),
        code=str(code),
        message=str(message),
        error=None,
        level=logging.WARNING,
        component="engine.data.provider_sessions.session_manager",
        extra={"provider": str(provider_name), **(extra or {})},
        include_health=False,
        persist=False,
    )


def _auth_ready_from_telemetry(telemetry: Dict[str, Any]) -> bool:
    capabilities = telemetry.get("capabilities") if isinstance(telemetry.get("capabilities"), dict) else {}
    auth_mode = str(capabilities.get("authentication") or "").strip().lower()
    return bool(telemetry.get("authenticated")) or auth_mode in {"", "none", "provider_object"}


def _interval_elapsed(last_ts_ms: int, interval_s: float, now_ts_ms: Optional[int] = None) -> bool:
    current_ts_ms = int(now_ts_ms if now_ts_ms is not None else now_ms())
    if int(last_ts_ms or 0) <= 0:
        return True
    return (current_ts_ms - int(last_ts_ms)) >= int(max(0.25, float(interval_s)) * 1000.0)


class ProviderSessionManager:
    def __init__(
        self,
        session: BaseProviderSession,
        *,
        provider_name: Optional[str] = None,
        heartbeat_interval_s: float = 2.0,
        dead_after_ms: int = 8000,
        reconnect_base_s: float = 1.0,
        reconnect_max_s: float = 30.0,
        max_reconnect_attempts: int = 0,
        startup_grace_ms: int = 30000,
    ) -> None:
        self.session = session
        self.provider_name = str(provider_name or session.provider_name)
        self.heartbeat_interval_s = float(max(0.25, heartbeat_interval_s))
        self.dead_after_ms = int(max(1000, dead_after_ms))
        self.reconnect_base_s = float(max(0.25, reconnect_base_s))
        self.reconnect_max_s = float(max(self.reconnect_base_s, reconnect_max_s))
        env_max_reconnect_attempts = os.environ.get("PROVIDER_MAX_RECONNECT_ATTEMPTS")
        if max_reconnect_attempts and int(max_reconnect_attempts) > 0:
            self.max_reconnect_attempts = int(max_reconnect_attempts)
        elif env_max_reconnect_attempts is not None and str(env_max_reconnect_attempts).strip() != "":
            self.max_reconnect_attempts = max(1, int(str(env_max_reconnect_attempts).strip()))
        else:
            self.max_reconnect_attempts = 20
        self.startup_grace_ms = int(max(1000, startup_grace_ms))
        self.meta_write_interval_s = float(max(1.0, float(os.environ.get("PROVIDER_SESSION_META_INTERVAL_S", "15.0"))))
        self.metric_emit_interval_s = float(max(1.0, float(os.environ.get("PROVIDER_SESSION_METRIC_INTERVAL_S", "15.0"))))

        self._lock = threading.RLock()
        self._stop = False
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._started = False
        self._reconnect_attempts = 0
        self._last_gap_fill_ts_ms = 0
        self._last_gap_fill_error: Optional[str] = None
        self._last_reconcile_ts_ms = 0
        self._forced_reconnect_reason: Optional[str] = None
        self._next_backoff_s = self.reconnect_base_s
        self._manager_state = "created"
        self._last_connect_ok_ts_ms = 0
        self._last_reconnect_attempt_ts_ms = 0
        self._last_reconnect_success_ts_ms = 0
        self._last_stale_ts_ms = 0
        self._last_disconnect_reason: Optional[str] = None
        self._last_meta_write_attempt_ts_ms = 0
        self._last_metric_emit_ts_ms = 0

    def reconnect(self, reason: str = "manual_reconnect") -> None:
        with self._lock:
            self._forced_reconnect_reason = str(reason)
            self._manager_state = "reconnecting"
        try:
            trace_event(
                "provider_session_manager_reconnect",
                component="engine.data.provider_sessions.session_manager",
                entity_type="provider",
                entity_id=self.provider_name,
                payload={"reason": str(reason)},
                provider=self.provider_name,
            )
        except Exception as e:
            _warn_nonfatal(self.provider_name, "PROVIDER_SESSION_TRACE_RECONNECT_FAILED", e, once_key="trace_reconnect", reason=str(reason))
        _warn_state(
            self.provider_name,
            "PROVIDER_SESSION_RECONNECT_ATTEMPT",
            "Provider reconnect requested.",
            reason=str(reason),
            attempt="manual",
        )
        try:
            self.session.note_reconnecting(str(reason))
            self.session.close()
        except Exception as e:
            _warn_nonfatal(self.provider_name, "PROVIDER_SESSION_RECONNECT_CLOSE_FAILED", e, reason=str(reason))
        self._write_meta(force=True)

    def ensure_subscriptions(self, symbols: Iterable[str]) -> None:
        desired = {str(x).strip() for x in (symbols or []) if str(x).strip()}
        self.session.replace_desired_symbols(desired)
        self.start()
        telemetry = self.session.telemetry_snapshot() or {}
        auth_mode = str(
            ((telemetry.get("capabilities") or {}) if isinstance(telemetry.get("capabilities"), dict) else {}).get("authentication")
            or ""
        ).strip().lower()
        ready = bool(telemetry.get("connected")) and (
            bool(telemetry.get("authenticated"))
            or auth_mode in {"", "none", "provider_object"}
        )
        if ready:
            self._reconcile_subscriptions()
        self._write_meta()

    def last_msg_age_ms(self) -> int:
        telemetry = self.session.telemetry_snapshot()
        return int(telemetry.get("last_msg_age_ms") or 10**9)

    def snapshot(self) -> Dict[str, Dict[str, Any]]:
        return self.session.snapshot()

    def provider_telemetry(self) -> Dict[str, Any]:
        t = self.session.telemetry_snapshot()
        session_state = str(t.get("connection_state") or "disconnected").strip().lower()
        manager_state = str(self._manager_state or "").strip().lower()
        if manager_state in {"connecting", "reconnecting", "backoff", "starting", "error"}:
            connection_state = "reconnecting"
        elif manager_state == "failed":
            connection_state = "stale" if session_state == "stale" else "disconnected"
        elif session_state in {"stale", "reconnecting", "connected"}:
            connection_state = session_state
        elif bool(t.get("connected")):
            connection_state = "connected"
        else:
            connection_state = "disconnected"
        t["manager_provider"] = self.provider_name
        t["manager_dead_after_ms"] = int(self.dead_after_ms)
        t["manager_reconnect_attempts"] = int(self._reconnect_attempts)
        t["manager_last_gap_fill_ts_ms"] = int(self._last_gap_fill_ts_ms or 0)
        t["manager_last_gap_fill_error"] = self._last_gap_fill_error
        t["manager_last_reconcile_ts_ms"] = int(self._last_reconcile_ts_ms or 0)
        t["manager_forced_reconnect_reason"] = self._forced_reconnect_reason
        t["manager_state"] = self._manager_state
        t["manager_last_connect_ok_ts_ms"] = int(self._last_connect_ok_ts_ms or 0)
        t["connection_state"] = str(connection_state)
        t["manager_last_reconnect_attempt_ts_ms"] = int(self._last_reconnect_attempt_ts_ms or 0)
        t["manager_last_reconnect_success_ts_ms"] = int(self._last_reconnect_success_ts_ms or 0)
        t["manager_last_stale_ts_ms"] = int(self._last_stale_ts_ms or 0)
        t["manager_last_disconnect_reason"] = self._last_disconnect_reason
        return t

    def ok(self) -> bool:
        # "ok" is the session-manager view of health used by routing and
        # ingestion watchdogs, combining connection state with freshness.
        telemetry = self.provider_telemetry()
        age_ms = int(telemetry.get("last_msg_age_ms") or 10**9)
        connected = bool(telemetry.get("connected"))
        desired_count = int(telemetry.get("desired_symbol_count") or 0)
        startup_age_ms = max(0, now_ms() - int(telemetry.get("last_connect_ts_ms") or 0))
        heartbeat_age_ms = max(0, now_ms() - int(telemetry.get("last_heartbeat_ts_ms") or 0))
        capabilities = telemetry.get("capabilities") if isinstance(telemetry.get("capabilities"), dict) else {}
        is_streaming = bool(capabilities.get("streaming"))
        is_polling_only = bool(capabilities.get("polling")) and not is_streaming
        if connected and desired_count == 0:
            return True
        if connected and startup_age_ms <= self.startup_grace_ms:
            return True
        if is_polling_only:
            return bool(connected and heartbeat_age_ms <= self.dead_after_ms)
        return bool(connected and age_ms <= self.dead_after_ms)

    def start(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return

            try:
                if not self.session.desired_symbols():
                    seed = {s.strip() for s in load_default_symbols() if s.strip()}
                    if seed:
                        self.session.replace_desired_symbols(seed)
            except Exception as e:
                _warn_nonfatal(self.provider_name, "PROVIDER_SESSION_DEFAULT_SYMBOL_SEED_FAILED", e, once_key="default_symbol_seed")

            self._stop = False
            self._stop_event.clear()
            self._manager_state = "starting"
            self._thread = threading.Thread(
                target=self._run,
                name=f"provider_session_manager_{self.provider_name}",
                daemon=True,
            )
            self._started = True
            self._thread.start()
        self._write_meta(force=True)

    def _run(self) -> None:
        while not self._stop_event.is_set():
            with self._lock:
                if self._stop:
                    break
            try:
                telemetry = self.session.telemetry_snapshot()
                if not bool(telemetry.get("connected")):
                    self._connect_once()

                self._reconcile_subscriptions()

                try:
                    dbg = self.session.telemetry_snapshot()
                    log.info("PROVIDER_DEBUG %s %s", self.provider_name, dbg)
                except Exception as e:
                    _warn_nonfatal(self.provider_name, "PROVIDER_SESSION_DEBUG_LOG_FAILED", e, once_key="debug_log")

                # The manager loop owns freshness enforcement and reconnect
                # escalation. Individual provider sessions only expose telemetry.
                heartbeat = self.session.heartbeat() or {}
                age_ms = int(heartbeat.get("last_msg_age_ms") or 10**9)
                startup_age_ms = max(0, now_ms() - int(heartbeat.get("last_connect_ts_ms") or 0))
                heartbeat_age_ms = max(0, now_ms() - int(heartbeat.get("last_heartbeat_ts_ms") or 0))
                capabilities = heartbeat.get("capabilities") if isinstance(heartbeat.get("capabilities"), dict) else {}
                is_streaming = bool(capabilities.get("streaming"))
                is_polling_only = bool(capabilities.get("polling")) and not is_streaming
                auth_ready = _auth_ready_from_telemetry(heartbeat)
                if bool(heartbeat.get("connected")):
                    self._manager_state = "healthy" if auth_ready else "connecting"
                else:
                    self._manager_state = "disconnected"

                self._emit_loop_metrics(heartbeat, age_ms)

                if bool(heartbeat.get("connected")) and startup_age_ms > self.startup_grace_ms:
                    if is_polling_only and heartbeat_age_ms > self.dead_after_ms:
                        err = f"{self.provider_name}_polling_heartbeat_stale age_ms={heartbeat_age_ms}"
                        self._manager_state = "stale"
                        self._last_stale_ts_ms = now_ms()
                        self.session.note_stale(err)
                        _warn_state(
                            self.provider_name,
                            "PROVIDER_SESSION_STALE_DETECTED",
                            "Polling provider heartbeat is stale.",
                            kind="polling_heartbeat",
                            age_ms=int(heartbeat_age_ms),
                            dead_after_ms=int(self.dead_after_ms),
                        )
                        raise RuntimeError(err)
                    if (not is_polling_only) and age_ms > self.dead_after_ms:
                        err = f"{self.provider_name}_stream_stale age_ms={age_ms}"
                        self._manager_state = "stale"
                        self._last_stale_ts_ms = now_ms()
                        self.session.note_stale(err)
                        _warn_state(
                            self.provider_name,
                            "PROVIDER_SESSION_STALE_DETECTED",
                            "Streaming provider feed is stale.",
                            kind="stream",
                            age_ms=int(age_ms),
                            dead_after_ms=int(self.dead_after_ms),
                            last_msg_ts_ms=int(heartbeat.get("last_msg_ts_ms") or 0),
                        )
                        raise RuntimeError(err)

                self._write_meta()
                if self._stop_event.wait(timeout=self.heartbeat_interval_s):
                    break
            except Exception as e:
                try:
                    log.exception("provider %s manager loop failure", self.provider_name)
                except Exception as log_err:
                    _warn_nonfatal(self.provider_name, "PROVIDER_SESSION_MANAGER_LOG_FAILED", log_err, once_key="manager_loop_log")
                self.session.note_error(e)
                self._manager_state = "error"
                self._write_meta(force=True)
                if not self._handle_failure(e):
                    break

    def close(self) -> None:
        thread = None
        with self._lock:
            self._stop = True
            self._stop_event.set()
            self._manager_state = "closed"
            thread = self._thread
        try:
            self.session.close()
        except Exception as e:
            _warn_nonfatal(self.provider_name, "PROVIDER_SESSION_CLOSE_FAILED", e, scope="close")
        if thread is not None and thread.is_alive() and thread is not threading.current_thread():
            try:
                thread.join(timeout=max(1.0, float(self.heartbeat_interval_s) * 2.0))
            except Exception as e:
                _warn_nonfatal(self.provider_name, "PROVIDER_SESSION_THREAD_JOIN_FAILED", e, scope="close")
        with self._lock:
            self._thread = None
            self._started = False
        self._write_meta(force=True)

    def _connect_once(self) -> None:
        t0 = now_ms()
        # Connection attempts are centralized here so backoff/retry policy is
        # consistent across concrete provider session implementations.
        self._manager_state = "connecting"
        try:
            log.info(
                "provider=%s connect endpoint=%s attempt=%s",
                self.provider_name,
                getattr(self.session, "endpoint", ""),
                int(self._reconnect_attempts or 0),
            )
        except Exception as e:
            _warn_nonfatal(self.provider_name, "PROVIDER_SESSION_CONNECT_LOG_FAILED", e, once_key="connect_log")
        self.session.apply_rate_limit("connect")
        self.session.connect()
        self.session.apply_rate_limit("authenticate")
        self.session.authenticate()
        self.session.detect_capabilities()

        desired = self.session.desired_symbols()
        if not desired:
            try:
                meta_set(
                    f"provider_session_{self.provider_name}_last_failure",
                    json.dumps(
                        {
                            "provider": self.provider_name,
                            "error": "no_symbols_subscribed",
                            "failure_kind": "config",
                            "attempt": 0,
                            "ts_ms": int(now_ms()),
                        },
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                    best_effort=True,
                )
            except Exception as e:
                _warn_nonfatal(
                    self.provider_name,
                    "PROVIDER_SESSION_NO_SYMBOLS_META_WRITE_FAILED",
                    e,
                    once_key="no_symbols_meta_write",
                )
            raise RuntimeError("no_symbols_subscribed")

        self.session.subscribe(sorted(desired))

        verify_deadline = time.time() + float(
            os.environ.get("PROVIDER_SUBSCRIBE_VERIFY_S", "2.0")
        )
        last_error = ""
        last_state = ""
        while time.time() < verify_deadline:
            if self._stop_event.wait(timeout=0.1):
                break
            telemetry = self.session.telemetry_snapshot() or {}
            last_error = str(telemetry.get("last_error") or "").strip()
            last_state = str(telemetry.get("connection_state") or "").strip().lower()
            connected = bool(telemetry.get("connected"))
            auth_ready = _auth_ready_from_telemetry(telemetry)
            desired_count = int(telemetry.get("desired_symbol_count") or len(desired))
            subscribed_count = int(telemetry.get("subscribed_symbol_count") or 0)
            subscriptions_ready = desired_count <= 0 or subscribed_count > 0

            if connected and auth_ready and subscriptions_ready:
                break

            if last_error:
                raise RuntimeError(last_error)
        else:
            if last_error:
                raise RuntimeError(last_error)
            detail = f"{self.provider_name}_subscribe_verify_timeout"
            if last_state:
                detail = f"{detail}:{last_state}"
            raise TimeoutError(detail)

        self._reconnect_attempts = 0
        self._next_backoff_s = self.reconnect_base_s
        self._last_connect_ok_ts_ms = now_ms()
        self._last_reconnect_success_ts_ms = int(self._last_connect_ok_ts_ms)
        self._last_disconnect_reason = None
        self._reconcile_subscriptions()
        self._run_gap_fill_if_needed()
        self._manager_state = "connected"
        self._write_meta(force=True)

        try:
            meta_set(
                f"provider_session_{self.provider_name}_last_failure",
                json.dumps(
                    {
                        "provider": self.provider_name,
                        "error": "",
                        "failure_kind": "",
                        "attempt": 0,
                        "ts_ms": int(now_ms()),
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ),
                best_effort=True,
            )
            meta_set(f"provider_session_{self.provider_name}_fatal", "", best_effort=True)
            meta_set("price_provider_active", self.provider_name, best_effort=True)
        except Exception as e:
            _warn_nonfatal(self.provider_name, "PROVIDER_SESSION_META_CLEAR_FAILED", e, once_key="meta_clear")

        try:
            emit_counter(
                "provider_connect_ok",
                1,
                component="engine.data.provider_sessions.session_manager",
                provider=self.provider_name,
            )
            emit_timing(
                "provider_connect_latency_ms",
                now_ms() - t0,
                component="engine.data.provider_sessions.session_manager",
                provider=self.provider_name,
            )
            trace_event(
                "provider_session_manager_connect_ok",
                component="engine.data.provider_sessions.session_manager",
                entity_type="provider",
                entity_id=self.provider_name,
                payload={"latency_ms": int(now_ms() - t0)},
                provider=self.provider_name,
            )
        except Exception as e:
            _warn_nonfatal(self.provider_name, "PROVIDER_SESSION_CONNECT_METRICS_FAILED", e, once_key="connect_metrics")
        try:
            if int(self.session.telemetry_snapshot().get("reconnect_count") or 0) > 0:
                log.info(
                    "provider=%s reconnect_success latency_ms=%s",
                    self.provider_name,
                    int(now_ms() - t0),
                )
            else:
                log.info(
                    "provider=%s connect_success latency_ms=%s",
                    self.provider_name,
                    int(now_ms() - t0),
                )
        except Exception as e:
            _warn_nonfatal(self.provider_name, "PROVIDER_SESSION_CONNECT_SUCCESS_LOG_FAILED", e, once_key="connect_success_log")

    def _emit_loop_metrics(self, heartbeat: Dict[str, Any], age_ms: int) -> None:
        current_ts_ms = now_ms()
        if not _interval_elapsed(self._last_metric_emit_ts_ms, self.metric_emit_interval_s, current_ts_ms):
            return
        self._last_metric_emit_ts_ms = int(current_ts_ms)
        try:
            emit_gauge(
                "provider_uptime",
                1.0 if bool(heartbeat.get("connected")) else 0.0,
                component="engine.data.provider_sessions.session_manager",
                provider=self.provider_name,
            )
            emit_gauge(
                "provider_queue_depth",
                int(heartbeat.get("subscribed_symbol_count") or 0),
                component="engine.data.provider_sessions.session_manager",
                provider=self.provider_name,
            )
            emit_gauge(
                "market_data_latency_ms",
                age_ms,
                component="engine.data.provider_sessions.session_manager",
                provider=self.provider_name,
            )
        except Exception as metric_error:
            _warn_nonfatal(
                self.provider_name,
                "PROVIDER_SESSION_METRIC_EMISSION_FAILED",
                metric_error,
                once_key="metric_log",
            )

    def _handle_failure(self, error: Exception) -> bool:
        if self._stop_event.is_set():
            return False

        err_s = str(error)
        err_l = err_s.lower()
        if "api_key_missing" in err_l or "missing_api_key" in err_l or "no_symbols_subscribed" in err_l:
            failure_kind = "config"
        elif "auth" in err_l or "not authorized" in err_l or "authentication failed" in err_l:
            failure_kind = "auth"
        elif "timed out" in err_l or "timeout" in err_l:
            failure_kind = "timeout"
        elif "handshake" in err_l or "dns" in err_l or "connection refused" in err_l:
            failure_kind = "network"
        else:
            failure_kind = "runtime"

        self._reconnect_attempts += 1
        self._last_reconnect_attempt_ts_ms = now_ms()
        self._last_disconnect_reason = err_s[:400]
        self.session.increment_reconnect_count()
        self.session.note_reconnecting(err_s)
        self._manager_state = "reconnecting"
        self._write_meta(force=True)

        try:
            meta_set(
                f"provider_session_{self.provider_name}_last_failure",
                json.dumps(
                    {
                        "provider": self.provider_name,
                        "error": err_s,
                        "failure_kind": failure_kind,
                        "attempt": int(self._reconnect_attempts),
                        "max_reconnect_attempts": int(self.max_reconnect_attempts),
                        "ts_ms": int(now_ms()),
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ),
                best_effort=True,
            )
        except Exception as e:
            _warn_nonfatal(self.provider_name, "PROVIDER_SESSION_FAILURE_META_WRITE_FAILED", e, once_key="failure_meta_write")

        fatal_failure = failure_kind in {"auth", "config"}

        if fatal_failure or (self.max_reconnect_attempts and self._reconnect_attempts >= self.max_reconnect_attempts):
            log.error("provider %s exceeded reconnect policy: %s", self.provider_name, error)
            self._manager_state = "failed"
            self._write_meta(force=True)
            try:
                meta_set(
                    f"provider_session_{self.provider_name}_fatal",
                    json.dumps(
                        {
                            "provider": self.provider_name,
                            "attempt": int(self._reconnect_attempts),
                            "max_reconnect_attempts": int(self.max_reconnect_attempts),
                            "error": err_s,
                            "failure_kind": failure_kind,
                            "ts_ms": int(now_ms()),
                        },
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                    best_effort=True,
                )
            except Exception as e:
                _warn_nonfatal(self.provider_name, "PROVIDER_SESSION_FATAL_META_WRITE_FAILED", e, once_key="fatal_meta_write")
            try:
                meta_set("price_provider_active", "", best_effort=True)
            except Exception as e:
                _warn_nonfatal(self.provider_name, "PROVIDER_SESSION_ACTIVE_PROVIDER_CLEAR_FAILED", e, once_key="active_provider_clear")
            return False

        sleep_s = min(self.reconnect_max_s, max(self.reconnect_base_s, self._next_backoff_s))
        jitter_mult = random.uniform(0.8, 1.2)
        sleep_s = min(self.reconnect_max_s, max(self.reconnect_base_s, sleep_s * jitter_mult))

        try:
            emit_counter(
                "provider_reconnect_attempt",
                1,
                component="engine.data.provider_sessions.session_manager",
                provider=self.provider_name,
            )
            trace_event(
                "provider_session_manager_reconnect_attempt",
                component="engine.data.provider_sessions.session_manager",
                entity_type="provider",
                entity_id=self.provider_name,
                payload={
                    "error": err_s,
                    "failure_kind": failure_kind,
                    "attempt": int(self._reconnect_attempts),
                },
                provider=self.provider_name,
            )
        except Exception as e:
            _warn_nonfatal(self.provider_name, "PROVIDER_SESSION_RECONNECT_METRICS_FAILED", e, once_key="reconnect_metrics")
        _warn_state(
            self.provider_name,
            "PROVIDER_SESSION_RECONNECT_BACKOFF",
            "Provider reconnect backoff scheduled.",
            attempt=int(self._reconnect_attempts),
            max_attempts=int(self.max_reconnect_attempts),
            failure_kind=failure_kind,
            sleep_s=float(sleep_s),
            error=err_s,
        )

        self._manager_state = "backoff"
        self._write_meta(force=True)

        try:
            self.session.close()
        except Exception as e:
            _warn_nonfatal(self.provider_name, "PROVIDER_SESSION_BACKOFF_CLOSE_FAILED", e, once_key="backoff_close")

        if self._stop_event.wait(timeout=sleep_s):
            return False

        self._next_backoff_s = min(self.reconnect_max_s, max(self.reconnect_base_s, self._next_backoff_s * 2.0))
        return not self._stop_event.is_set()

    def _reconcile_subscriptions(self) -> None:
        desired = self.session.desired_symbols()
        subscribed = self.session.subscribed_symbols()
        to_add = desired - subscribed
        to_remove = subscribed - desired
        if to_remove:
            self.session.apply_rate_limit("unsubscribe")
            self.session.unsubscribe(sorted(to_remove))
        if to_add:
            self.session.apply_rate_limit("subscribe")
            self.session.subscribe(sorted(to_add))
        self._last_reconcile_ts_ms = now_ms()

        if to_add or to_remove:
            try:
                emit_counter(
                    "provider_subscription_reconcile",
                    int(len(to_add) + len(to_remove)),
                    component="engine.data.provider_sessions.session_manager",
                    provider=self.provider_name,
                )
            except Exception:
                log.exception("provider %s emit_counter failed", self.provider_name)

    def _run_gap_fill_if_needed(self) -> None:
        telemetry = self.session.telemetry_snapshot()
        since_ts_ms = int(telemetry.get("last_disconnect_ts_ms") or 0)
        desired = self.session.desired_symbols()
        if not desired or since_ts_ms <= 0:
            return
        try:
            self.session.apply_rate_limit("gap_fill")
            rows = self.session.perform_gap_fill(sorted(desired), since_ts_ms) or {}
            if rows:
                self.session.merge_snapshot(rows)
            self._last_gap_fill_ts_ms = now_ms()
            self._last_gap_fill_error = None
            try:
                emit_counter(
                    "provider_gap_fill_ok",
                    int(len(rows or {})),
                    component="engine.data.provider_sessions.session_manager",
                    provider=self.provider_name,
                )
            except Exception:
                log.exception("provider %s emit_counter failed", self.provider_name)
        except Exception as e:
            self._last_gap_fill_error = str(e)[:400]
            self.session.note_error(e)
            try:
                emit_counter(
                    "provider_gap_fill_error",
                    1,
                    component="engine.data.provider_sessions.session_manager",
                    provider=self.provider_name,
                )
            except Exception:
                log.exception("provider %s emit_counter failed", self.provider_name)

    def _write_meta(self, *, force: bool = False) -> None:
        current_ts_ms = now_ms()
        if not bool(force) and not _interval_elapsed(self._last_meta_write_attempt_ts_ms, self.meta_write_interval_s, current_ts_ms):
            return
        self._last_meta_write_attempt_ts_ms = int(current_ts_ms)
        try:
            meta_set(
                f"provider_session_{self.provider_name}",
                json.dumps(self.provider_telemetry(), separators=(",", ":"), sort_keys=True),
                best_effort=True,
            )
        except Exception as e:
            _warn_nonfatal(self.provider_name, "PROVIDER_SESSION_META_WRITE_FAILED", e, once_key="meta_log")
