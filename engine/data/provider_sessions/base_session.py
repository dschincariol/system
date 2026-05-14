"""
FILE: base_session.py

Provider session management module for `base_session`.
"""

import os
import logging
import threading
import time
from typing import Any, Dict, Iterable, Optional, Set

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger

LOG = get_logger("engine.data.provider_sessions.base_session")


def now_ms() -> int:
    return int(time.time() * 1000)


class BaseProviderSession:
    provider_name = "provider"

    def __init__(self, provider_name: Optional[str] = None) -> None:
        if provider_name is not None and str(provider_name).strip():
            self.provider_name = str(provider_name).strip()
        else:
            self.provider_name = str(getattr(self, "provider_name", "provider"))

        self._lock = threading.RLock()
        self._desired_symbols: Set[str] = set()
        self._subscribed_symbols: Set[str] = set()
        self._connected = False
        self._authenticated = False
        self._connection_state = "disconnected"
        self._last_state_change_ts_ms = 0
        self._last_stale_ts_ms = 0
        self._last_msg_ts_ms = 0
        self._last_heartbeat_ts_ms = 0
        self._last_connect_ts_ms = 0
        self._last_disconnect_ts_ms = 0
        self._last_error: Optional[str] = None
        self._reconnect_count = 0
        # Capabilities are the contract consumed by the session manager and
        # ingestion supervisor. Concrete sessions should override fields here
        # instead of inventing ad hoc side channels.
        self._capabilities: Dict[str, Any] = {
            "streaming": True,
            "polling": False,
            "heartbeat": True,
            "gap_fill": False,
            "historical_catchup": False,
            "subscription_reconciliation": True,
            "authentication": "none",
            "supports_quotes": True,
            "supports_trades": True,
            "supports_level1": True,
            "supports_level2": False,
            "supports_historical": False,
            "supports_snapshot": True,
            "rate_limit_per_min": None,
            "capability_source": "static",
        }
        self._rate_limit_window_started_ts_ms = 0
        self._rate_limit_count = 0
        self._dedup_drop_count = 0
        self._gap_event_count = 0
        self._last_symbol_event_key: Dict[str, str] = {}

    def connect(self) -> None:
        raise NotImplementedError

    def authenticate(self) -> None:
        self._authenticated = True

    def subscribe(self, symbols: Iterable[str]) -> None:
        raise NotImplementedError

    def unsubscribe(self, symbols: Iterable[str]) -> None:
        raise NotImplementedError

    def heartbeat(self) -> Dict[str, Any]:
        self._last_heartbeat_ts_ms = now_ms()
        return self.telemetry_snapshot()

    def reconnect(self) -> None:
        # Reconnect intentionally replays the full lifecycle so concrete session
        # classes only need to implement the primitive steps correctly once.
        self._reconnect_count += 1
        self.close()
        self.connect()
        self.authenticate()
        self.detect_capabilities()
        desired = self.desired_symbols()
        if desired:
            self.subscribe(desired)

    def close(self) -> None:
        raise NotImplementedError

    def apply_rate_limit(self, operation: str = "request") -> None:
        # Session-level rate limiting keeps reconnect/gap-fill paths from racing
        # ahead of provider quotas just because multiple callers share a session.
        rate_limit_per_min = self.telemetry_snapshot().get("capabilities", {}).get("rate_limit_per_min")
        if rate_limit_per_min in (None, 0, "0", ""):
            return
        try:
            limit = max(1, int(rate_limit_per_min))
        except Exception as e:
            log_failure(
                LOG,
                event="base_session_rate_limit_parse_failed",
                code="BASE_SESSION_RATE_LIMIT_PARSE_FAILED",
                message="Provider session rate limit parse failed.",
                error=e,
                level=logging.WARNING,
                component="engine.data.provider_sessions.base_session",
                extra={"provider_name": self.provider_name},
                persist=False,
            )
            return

        try:
            wait_timeout_s = float(os.environ.get("PROVIDER_RATE_LIMIT_WAIT_TIMEOUT_S", "90"))
        except Exception:
            wait_timeout_s = 90.0
        deadline = time.time() + max(1.0, float(wait_timeout_s))

        while True:
            with self._lock:
                ts = now_ms()
                window_start = int(self._rate_limit_window_started_ts_ms or 0)
                if window_start <= 0 or (ts - window_start) >= 60_000:
                    self._rate_limit_window_started_ts_ms = ts
                    self._rate_limit_count = 0
                    window_start = ts

                if self._rate_limit_count < limit:
                    self._rate_limit_count += 1
                    return

                sleep_ms = max(50, 60_000 - (ts - window_start))

            if time.time() >= deadline:
                raise TimeoutError(
                    f"{self.provider_name}_{str(operation or 'request')}_rate_limit_wait_timeout"
                )

            remaining_s = max(0.0, deadline - time.time())
            time.sleep(min(float(sleep_ms) / 1000.0, remaining_s))

    def snapshot(self) -> Dict[str, Dict[str, Any]]:
        return {}

    def perform_gap_fill(self, symbols: Iterable[str], since_ts_ms: int) -> Dict[str, Dict[str, Any]]:
        return {}

    def detect_capabilities(self) -> Dict[str, Any]:
        return self.telemetry_snapshot().get("capabilities") or {}

    def desired_symbols(self) -> Set[str]:
        with self._lock:
            return set(self._desired_symbols)

    def subscribed_symbols(self) -> Set[str]:
        with self._lock:
            return set(self._subscribed_symbols)

    def replace_desired_symbols(self, symbols: Iterable[str]) -> None:
        clean = {str(x).strip() for x in (symbols or []) if str(x).strip()}
        with self._lock:
            self._desired_symbols = set(clean)

    def update_subscribed_symbols(self, symbols: Iterable[str]) -> None:
        clean = {str(x).strip() for x in (symbols or []) if str(x).strip()}
        with self._lock:
            self._subscribed_symbols = set(clean)

    def note_connected(self) -> None:
        with self._lock:
            ts = now_ms()
            self._connected = True
            self._connection_state = "connected"
            self._last_state_change_ts_ms = ts
            self._last_connect_ts_ms = ts
            self._last_heartbeat_ts_ms = ts
            self._last_error = None

    def note_disconnected(self, error: Optional[str] = None) -> None:
        with self._lock:
            self._connected = False
            self._authenticated = False
            ts = now_ms()
            self._connection_state = "disconnected"
            self._last_state_change_ts_ms = ts
            self._last_disconnect_ts_ms = ts
            if error:
                self._last_error = str(error)[:400]

    def note_reconnecting(self, reason: Optional[str] = None) -> None:
        with self._lock:
            ts = now_ms()
            self._connected = False
            self._authenticated = False
            self._connection_state = "reconnecting"
            self._last_state_change_ts_ms = ts
            self._last_disconnect_ts_ms = ts
            if reason:
                self._last_error = str(reason)[:400]

    def note_stale(self, error: Optional[str] = None) -> None:
        with self._lock:
            ts = now_ms()
            self._connection_state = "stale"
            self._last_state_change_ts_ms = ts
            self._last_stale_ts_ms = ts
            if error:
                self._last_error = str(error)[:400]

    def note_authenticated(self) -> None:
        with self._lock:
            self._authenticated = True

    def note_message(self, ts_ms: Optional[int] = None) -> None:
        with self._lock:
            ts = int(ts_ms or now_ms())
            if ts <= 0:
                ts = now_ms()
            self._last_msg_ts_ms = max(int(self._last_msg_ts_ms or 0), ts)
            self._last_heartbeat_ts_ms = now_ms()
            if self._connected and self._connection_state != "connected":
                self._connection_state = "connected"
                self._last_state_change_ts_ms = int(self._last_heartbeat_ts_ms)

    def note_error(self, error: Any) -> None:
        with self._lock:
            self._last_error = str(error)[:400]

    def merge_snapshot(self, rows: Dict[str, Dict[str, Any]]) -> None:
        return

    def note_dedup_drop(self) -> None:
        with self._lock:
            self._dedup_drop_count += 1

    def note_gap_event(self) -> None:
        with self._lock:
            self._gap_event_count += 1

    def should_drop_duplicate_event(self, symbol: str, event_key: str) -> bool:
        # Dedup happens at the session edge so downstream storage does not have
        # to distinguish between genuine feed churn and exact transport repeats.
        sym = str(symbol or "").strip()
        key = str(event_key or "")
        if not sym or not key:
            return False
        with self._lock:
            prev = self._last_symbol_event_key.get(sym)
            if prev == key:
                self._dedup_drop_count += 1
                return True
            self._last_symbol_event_key[sym] = key
            return False

    def increment_reconnect_count(self) -> None:
        with self._lock:
            self._reconnect_count += 1

    def set_capability(self, key: str, value: Any) -> None:
        with self._lock:
            self._capabilities[str(key)] = value

    def telemetry_snapshot(self) -> Dict[str, Any]:
        with self._lock:
            # This snapshot is the canonical health payload the supervisor writes
            # into heartbeats/runtime meta. Keep it cheap and serialization-safe.
            return {
                "provider": str(self.provider_name),
                "connected": bool(self._connected),
                "authenticated": bool(self._authenticated),
                "connection_state": str(self._connection_state or "disconnected"),
                "last_state_change_ts_ms": int(self._last_state_change_ts_ms or 0),
                "last_stale_ts_ms": int(self._last_stale_ts_ms or 0),
                "last_msg_age_ms": int((now_ms() - self._last_msg_ts_ms) if self._last_msg_ts_ms else 10**9),
                "last_msg_ts_ms": int(self._last_msg_ts_ms or 0),
                "last_heartbeat_ts_ms": int(self._last_heartbeat_ts_ms or 0),
                "last_connect_ts_ms": int(self._last_connect_ts_ms or 0),
                "last_disconnect_ts_ms": int(self._last_disconnect_ts_ms or 0),
                "last_error": self._last_error,
                "reconnect_count": int(self._reconnect_count),
                "desired_symbols": sorted(self._desired_symbols),
                "desired_symbol_count": int(len(self._desired_symbols)),
                "subscribed_symbols": sorted(self._subscribed_symbols),
                "subscribed_symbol_count": int(len(self._subscribed_symbols)),
                "capabilities": dict(self._capabilities),
                "dedup_drop_count": int(self._dedup_drop_count),
                "gap_event_count": int(self._gap_event_count),
                "rate_limit_window_started_ts_ms": int(self._rate_limit_window_started_ts_ms or 0),
                "rate_limit_count": int(self._rate_limit_count),
            }
