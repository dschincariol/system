"""Asynchronous market regime detection and caching."""

from __future__ import annotations

import logging
import math
import os
import queue
import threading
import time
from typing import Any, Dict, Mapping

from engine.data.feature_store import get_live_features
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect, init_db, run_write_txn

LOG = get_logger("engine.regime_detector")
_WARNED_NONFATAL_KEYS: set[str] = set()

UNKNOWN_REGIME = "unknown"
REGIME_QUEUE_MAXSIZE = max(128, int(os.environ.get("REGIME_DETECTOR_QUEUE_MAXSIZE", "2048")))

_VOLATILITY_SYNONYMS = {
    "calm": "low",
    "high": "high",
    "high_vol": "high",
    "low": "low",
    "low_vol": "low",
    "medium": "normal",
    "mid": "normal",
    "normal": "normal",
}
_TREND_SYNONYMS = {
    "bear": "bearish",
    "bearish": "bearish",
    "bull": "bullish",
    "bullish": "bullish",
    "flat": "range",
    "neutral": "range",
    "range": "range",
    "ranging": "range",
    "sideways": "range",
}
_LIQUIDITY_SYNONYMS = {
    "ample": "deep",
    "deep": "deep",
    "high": "deep",
    "liquid": "deep",
    "low": "thin",
    "normal": "normal",
    "thick": "deep",
    "thin": "thin",
}


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="regime_detector_nonfatal",
        code=str(code),
        message=str(code),
        error=error,
        level=logging.WARNING,
        component="engine.regime_detector",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _normalize_symbol(value: Any) -> str:
    return str(value or "").strip().upper()


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    if not math.isfinite(out):
        return float(default)
    return float(out)


def _normalize_snapshot(value: Any) -> Dict[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    features = value.get("features")
    out: Dict[str, Any] = {
        "symbol": _normalize_symbol(value.get("symbol")),
        "ts_ms": int(_safe_int(value.get("ts_ms"), 0)),
        "feature_set_tag": str(value.get("feature_set_tag") or "").strip(),
    }
    if isinstance(features, Mapping):
        out["features"] = {
            str(key): float(_safe_float(raw, 0.0))
            for key, raw in features.items()
        }
    elif isinstance(value, Mapping):
        out["features"] = {
            str(key): float(_safe_float(raw, 0.0))
            for key, raw in value.items()
            if key not in {"symbol", "ts_ms", "feature_set_tag", "feature_names", "vector", "source_timestamps"}
        }
    return out


def _normalize_regime_label(kind: str, value: Any) -> str:
    text = str(value or "").strip().lower()
    if kind == "volatility":
        return _VOLATILITY_SYNONYMS.get(text, UNKNOWN_REGIME)
    if kind == "trend":
        return _TREND_SYNONYMS.get(text, UNKNOWN_REGIME)
    if kind == "liquidity":
        return _LIQUIDITY_SYNONYMS.get(text, UNKNOWN_REGIME)
    return UNKNOWN_REGIME


def regime_signature(regime: Mapping[str, Any] | None) -> str:
    """Build a stable regime key from canonical volatility, trend, and liquidity labels."""
    return "|".join(
        [
            _normalize_regime_label("volatility", (regime or {}).get("volatility_regime")),
            _normalize_regime_label("trend", (regime or {}).get("trend_regime")),
            _normalize_regime_label("liquidity", (regime or {}).get("liquidity_regime")),
        ]
    )


def unknown_regime_state(
    symbol: Any = None,
    *,
    ts_ms: Any = None,
    source: str | None = None,
) -> Dict[str, Any]:
    """Return the sentinel regime payload used when classification is unavailable."""
    state = {
        "time": int(_safe_int(ts_ms, 0)),
        "symbol": _normalize_symbol(symbol),
        "volatility_regime": UNKNOWN_REGIME,
        "trend_regime": UNKNOWN_REGIME,
        "liquidity_regime": UNKNOWN_REGIME,
    }
    state["regime_key"] = regime_signature(state)
    if source:
        state["source"] = str(source)
    return state


def normalize_regime_state(
    regime: Any,
    *,
    symbol: Any = None,
    ts_ms: Any = None,
    source: str | None = None,
) -> Dict[str, Any]:
    """Normalize a regime payload into canonical labels and metadata fields."""
    if not isinstance(regime, Mapping):
        return unknown_regime_state(symbol=symbol, ts_ms=ts_ms, source=source)

    normalized = {
        "time": int(
            _safe_int(
                regime.get("time"),
                _safe_int(regime.get("ts_ms"), _safe_int(ts_ms, 0)),
            )
        ),
        "symbol": _normalize_symbol(regime.get("symbol") or symbol),
        "volatility_regime": _normalize_regime_label("volatility", regime.get("volatility_regime")),
        "trend_regime": _normalize_regime_label("trend", regime.get("trend_regime")),
        "liquidity_regime": _normalize_regime_label("liquidity", regime.get("liquidity_regime")),
    }
    normalized["regime_key"] = regime_signature(normalized)
    resolved_source = str(regime.get("source") or source or "").strip()
    if resolved_source:
        normalized["source"] = resolved_source
    return normalized


def has_known_regime(regime: Mapping[str, Any] | None) -> bool:
    """Return whether any regime dimension resolves to a non-unknown label."""
    state = normalize_regime_state(regime)
    return any(
        str(state.get(field) or UNKNOWN_REGIME) != UNKNOWN_REGIME
        for field in ("volatility_regime", "trend_regime", "liquidity_regime")
    )


def _classify_volatility(features: Mapping[str, Any]) -> str:
    vol_20 = abs(_safe_float(features.get("volatility_20"), 0.0))
    vol_60 = abs(_safe_float(features.get("volatility_60"), 0.0))
    atr_pct = abs(_safe_float(features.get("atr_pct_14"), 0.0))
    vol_ratio = (vol_20 / vol_60) if vol_60 > 1e-9 else (2.0 if vol_20 > 0.0 else 1.0)
    if vol_20 >= 0.035 or atr_pct >= 0.020 or vol_ratio >= 1.35:
        return "high"
    if (vol_20 > 0.0 and vol_20 <= 0.010 and atr_pct <= 0.008) or (vol_ratio <= 0.75 and vol_20 <= 0.020):
        return "low"
    return "normal"


def _classify_trend(features: Mapping[str, Any]) -> str:
    trend_strength = abs(_safe_float(features.get("trend_strength_20"), 0.0))
    momentum_1d = _safe_float(features.get("momentum_1d"), 0.0)
    momentum_1h = _safe_float(features.get("momentum_1h"), 0.0)
    rolling_return_1d = _safe_float(features.get("rolling_return_1d"), 0.0)
    directional_bias = (0.55 * momentum_1d) + (0.30 * momentum_1h) + (0.15 * rolling_return_1d)
    if trend_strength >= 1.0 and directional_bias >= 0.002:
        return "bullish"
    if trend_strength >= 1.0 and directional_bias <= -0.002:
        return "bearish"
    return "range"


def _classify_liquidity(features: Mapping[str, Any]) -> str:
    volume_rel_20 = _safe_float(features.get("volume_rel_20"), 0.0)
    dollar_volume_rel_20 = _safe_float(features.get("dollar_volume_rel_20"), 0.0)
    nonzero_share = _safe_float(features.get("volume_nonzero_share_20"), 0.0)
    dollar_volume_last = _safe_float(features.get("dollar_volume_last"), 0.0)
    if nonzero_share < 0.60 or volume_rel_20 < 0.60 or dollar_volume_rel_20 < 0.65 or dollar_volume_last < 1_000_000.0:
        return "thin"
    if nonzero_share >= 0.90 and volume_rel_20 >= 1.20 and dollar_volume_rel_20 >= 1.40 and dollar_volume_last >= 5_000_000.0:
        return "deep"
    return "normal"


def classify_regime_snapshot(
    symbol: Any,
    feature_snapshot: Mapping[str, Any] | None = None,
    *,
    ts_ms: Any = None,
    source: str | None = None,
) -> Dict[str, Any]:
    """Classify volatility, trend, and liquidity regimes from a feature snapshot."""
    snapshot = _normalize_snapshot(feature_snapshot or {})
    features = dict((snapshot or {}).get("features") or {})
    resolved_symbol = _normalize_symbol(symbol or (snapshot or {}).get("symbol"))
    resolved_ts_ms = int(
        _safe_int(
            ts_ms,
            _safe_int((snapshot or {}).get("ts_ms"), _now_ms()),
        )
    )
    if not features:
        return unknown_regime_state(symbol=resolved_symbol, ts_ms=resolved_ts_ms, source=source)
    state = {
        "time": int(resolved_ts_ms),
        "symbol": str(resolved_symbol),
        "volatility_regime": _classify_volatility(features),
        "trend_regime": _classify_trend(features),
        "liquidity_regime": _classify_liquidity(features),
    }
    state["regime_key"] = regime_signature(state)
    if source:
        state["source"] = str(source)
    return state


class RegimeDetector:
    """Asynchronously classify and cache the latest regime state per symbol."""

    def __init__(self) -> None:
        self._queue: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=int(REGIME_QUEUE_MAXSIZE))
        self._stop_event = threading.Event()
        self._start_lock = threading.Lock()
        self._started = False
        self._thread: threading.Thread | None = None
        self._cache_lock = threading.Lock()
        self._latest_cache: dict[str, dict[str, Any]] = {}
        self._flush_condition = threading.Condition()
        self._submitted_count = 0
        self._completed_count = 0
        self._inflight_count = 0

    def submit_refresh_nowait(
        self,
        symbol: Any,
        *,
        feature_snapshot: Mapping[str, Any] | None = None,
        ts_ms: Any = None,
        source: str | None = None,
    ) -> bool:
        """Queue a best-effort regime refresh without blocking the caller."""
        symbol_key = _normalize_symbol(symbol or (feature_snapshot or {}).get("symbol"))
        if not symbol_key:
            return False
        payload = {
            "symbol": str(symbol_key),
            "time": int(
                _safe_int(
                    ts_ms,
                    _safe_int((feature_snapshot or {}).get("ts_ms"), _now_ms()),
                )
            ),
            "feature_snapshot": _normalize_snapshot(feature_snapshot),
            "source": str(source or "background"),
        }
        self._ensure_started()
        try:
            self._queue.put_nowait(payload)
        except queue.Full as exc:
            _warn_nonfatal(
                "REGIME_DETECTOR_QUEUE_FULL",
                exc,
                symbol=str(symbol_key),
                queue_maxsize=int(self._queue.maxsize),
            )
            return False
        with self._flush_condition:
            self._submitted_count += 1
            self._flush_condition.notify_all()
        return True

    def resolve_latest(
        self,
        symbol: Any,
        *,
        target_time_ms: Any = None,
    ) -> Dict[str, Any]:
        """Return the freshest known regime, loading from cache or DB as needed."""
        symbol_key = _normalize_symbol(symbol)
        if not symbol_key:
            return unknown_regime_state()
        requested_time = int(_safe_int(target_time_ms, 0))
        cached = self._load_cached(symbol_key)
        if cached is not None and has_known_regime(cached):
            if requested_time <= 0:
                return dict(cached)
            cached_time = int(_safe_int(cached.get("time"), 0))
            if cached_time > 0:
                return dict(cached)
        loaded = self._load_from_db(symbol_key, requested_time_ms=requested_time)
        if loaded is not None:
            self._store_cached(loaded)
            return dict(loaded)
        if cached is not None:
            return dict(cached)
        return unknown_regime_state(symbol=symbol_key, ts_ms=requested_time)

    def flush(self, timeout_s: float | None = None) -> bool:
        """Wait for submitted refresh work to drain."""
        deadline = time.monotonic() + float(timeout_s if timeout_s is not None else 5.0)
        with self._flush_condition:
            while True:
                if self._completed_count >= self._submitted_count and self._inflight_count <= 0 and self._queue.empty():
                    return True
                remaining = deadline - time.monotonic()
                if remaining <= 0.0:
                    return False
                self._flush_condition.wait(timeout=remaining)

    def close(self, timeout_s: float | None = None) -> bool:
        """Stop the background detector thread after draining queued work."""
        self._stop_event.set()
        thread = self._thread
        if thread is None:
            self._started = False
            self._stop_event = threading.Event()
            return True
        thread.join(timeout=float(timeout_s if timeout_s is not None else 2.0))
        closed = not thread.is_alive()
        if closed:
            self._thread = None
            self._started = False
            self._stop_event = threading.Event()
        return closed

    def _ensure_started(self) -> None:
        if self._started and self._thread is not None and self._thread.is_alive() and not self._stop_event.is_set():
            return
        with self._start_lock:
            if self._started and self._thread is not None and self._thread.is_alive() and not self._stop_event.is_set():
                return
            self._stop_event = threading.Event()
            self._thread = threading.Thread(target=self._run, name="regime-detector", daemon=True)
            self._thread.start()
            self._started = True

    def _run(self) -> None:
        while True:
            if self._stop_event.is_set() and self._queue.empty():
                break
            try:
                payload = self._queue.get(timeout=0.10)
            except queue.Empty:
                continue
            self._mark_inflight(1)
            try:
                state = self._compute_payload(payload)
                self._persist_state(state)
                self._store_cached(state)
            except Exception as exc:
                _warn_nonfatal(
                    "REGIME_DETECTOR_COMPUTE_FAILED",
                    exc,
                    symbol=str(payload.get("symbol") or ""),
                    time=int(_safe_int(payload.get("time"), 0)),
                )
            finally:
                self._mark_completed(1)

    def _compute_payload(self, payload: Mapping[str, Any]) -> Dict[str, Any]:
        symbol_key = _normalize_symbol(payload.get("symbol"))
        snapshot = _normalize_snapshot(payload.get("feature_snapshot"))
        if snapshot is None or not dict(snapshot.get("features") or {}):
            snapshot = _normalize_snapshot(get_live_features(symbol_key))
        return classify_regime_snapshot(
            symbol_key,
            snapshot,
            ts_ms=int(_safe_int(payload.get("time"), _safe_int((snapshot or {}).get("ts_ms"), _now_ms()))),
            source=str(payload.get("source") or "background"),
        )

    def _persist_state(self, regime: Mapping[str, Any]) -> None:
        state = normalize_regime_state(regime)
        if not state["symbol"]:
            return
        init_db()

        def _write(con) -> None:
            con.execute(
                """
                INSERT INTO regime_state(time, symbol, volatility_regime, trend_regime, liquidity_regime)
                VALUES(?,?,?,?,?)
                ON CONFLICT(symbol, time) DO UPDATE SET
                  volatility_regime=excluded.volatility_regime,
                  trend_regime=excluded.trend_regime,
                  liquidity_regime=excluded.liquidity_regime
                """,
                (
                    int(state["time"]),
                    str(state["symbol"]),
                    str(state["volatility_regime"]),
                    str(state["trend_regime"]),
                    str(state["liquidity_regime"]),
                ),
            )
            con.execute(
                """
                UPDATE predictions
                SET volatility_regime=?, trend_regime=?, liquidity_regime=?
                WHERE symbol=? AND regime_time_ms=?
                """,
                (
                    str(state["volatility_regime"]),
                    str(state["trend_regime"]),
                    str(state["liquidity_regime"]),
                    str(state["symbol"]),
                    int(state["time"]),
                ),
            )
            con.execute(
                """
                UPDATE prediction_history
                SET volatility_regime=?, trend_regime=?, liquidity_regime=?
                WHERE symbol=? AND regime_time_ms=?
                """,
                (
                    str(state["volatility_regime"]),
                    str(state["trend_regime"]),
                    str(state["liquidity_regime"]),
                    str(state["symbol"]),
                    int(state["time"]),
                ),
            )

        run_write_txn(_write, table="regime_state", operation="regime_detector_persist")

    def _load_cached(self, symbol: str) -> dict[str, Any] | None:
        with self._cache_lock:
            cached = self._latest_cache.get(str(symbol))
            return dict(cached) if cached is not None else None

    def _store_cached(self, regime: Mapping[str, Any]) -> None:
        state = normalize_regime_state(regime)
        if not state["symbol"]:
            return
        with self._cache_lock:
            current = self._latest_cache.get(str(state["symbol"]))
            if current is None or int(_safe_int(current.get("time"), 0)) <= int(state["time"]):
                self._latest_cache[str(state["symbol"])] = dict(state)

    def _load_from_db(self, symbol: str, *, requested_time_ms: int) -> dict[str, Any] | None:
        con = None
        try:
            con = connect(readonly=True)
            if requested_time_ms > 0:
                row = con.execute(
                    """
                    SELECT time, symbol, volatility_regime, trend_regime, liquidity_regime
                    FROM regime_state
                    WHERE symbol=? AND time<=?
                    ORDER BY time DESC
                    LIMIT 1
                    """,
                    (str(symbol), int(requested_time_ms)),
                ).fetchone()
            else:
                row = con.execute(
                    """
                    SELECT time, symbol, volatility_regime, trend_regime, liquidity_regime
                    FROM regime_state
                    WHERE symbol=?
                    ORDER BY time DESC
                    LIMIT 1
                    """,
                    (str(symbol),),
                ).fetchone()
        except Exception:
            return None
        finally:
            if con is not None:
                try:
                    con.close()
                except Exception:
                    pass  # no-op-guard: allow best-effort cleanup
        if row is None:
            return None
        return normalize_regime_state(
            {
                "time": int(_safe_int(row[0], 0)),
                "symbol": str(row[1] or ""),
                "volatility_regime": row[2],
                "trend_regime": row[3],
                "liquidity_regime": row[4],
                "source": "db",
            }
        )

    def _mark_inflight(self, count: int) -> None:
        with self._flush_condition:
            self._inflight_count += int(max(0, count))
            self._flush_condition.notify_all()

    def _mark_completed(self, count: int) -> None:
        with self._flush_condition:
            resolved = int(max(0, count))
            self._inflight_count = max(0, self._inflight_count - resolved)
            self._completed_count += resolved
            self._flush_condition.notify_all()


DEFAULT_REGIME_DETECTOR = RegimeDetector()


def resolve_regime_snapshot(
    symbol: Any,
    *,
    feature_snapshot: Mapping[str, Any] | None = None,
    target_time_ms: Any = None,
    enqueue_refresh: bool = True,
    allow_inline_fallback: bool = False,
    source: str | None = None,
) -> Dict[str, Any]:
    """Resolve a regime snapshot and optionally enqueue a background refresh."""
    symbol_key = _normalize_symbol(symbol or (feature_snapshot or {}).get("symbol"))
    snapshot = _normalize_snapshot(feature_snapshot)
    requested_time = int(
        _safe_int(
            target_time_ms,
            _safe_int((snapshot or {}).get("ts_ms"), 0),
        )
    )
    if enqueue_refresh:
        DEFAULT_REGIME_DETECTOR.submit_refresh_nowait(
            symbol_key,
            feature_snapshot=snapshot,
            ts_ms=requested_time,
            source=source or "resolve_regime_snapshot",
        )
    cached = DEFAULT_REGIME_DETECTOR.resolve_latest(symbol_key, target_time_ms=requested_time)
    if has_known_regime(cached):
        return dict(cached)
    if allow_inline_fallback and snapshot is not None:
        return classify_regime_snapshot(
            symbol_key,
            snapshot,
            ts_ms=requested_time,
            source="inline_fallback",
        )
    return dict(cached)


def get_latest_regime_snapshot(symbol: Any, *, target_time_ms: Any = None) -> Dict[str, Any]:
    """Return the latest cached or persisted regime snapshot for a symbol."""
    return DEFAULT_REGIME_DETECTOR.resolve_latest(symbol, target_time_ms=target_time_ms)


def submit_regime_refresh_nowait(
    symbol: Any,
    *,
    feature_snapshot: Mapping[str, Any] | None = None,
    ts_ms: Any = None,
    source: str | None = None,
) -> bool:
    """Submit a best-effort refresh to the default detector."""
    return DEFAULT_REGIME_DETECTOR.submit_refresh_nowait(
        symbol,
        feature_snapshot=feature_snapshot,
        ts_ms=ts_ms,
        source=source,
    )


def flush_regime_detector(timeout_s: float | None = None) -> bool:
    """Wait for the default detector to drain pending work."""
    return DEFAULT_REGIME_DETECTOR.flush(timeout_s=timeout_s)


def shutdown_regime_detector(timeout_s: float | None = None) -> bool:
    """Stop the default detector instance."""
    return DEFAULT_REGIME_DETECTOR.close(timeout_s=timeout_s)


__all__ = [
    "DEFAULT_REGIME_DETECTOR",
    "RegimeDetector",
    "UNKNOWN_REGIME",
    "classify_regime_snapshot",
    "flush_regime_detector",
    "get_latest_regime_snapshot",
    "has_known_regime",
    "normalize_regime_state",
    "regime_signature",
    "resolve_regime_snapshot",
    "shutdown_regime_detector",
    "submit_regime_refresh_nowait",
    "unknown_regime_state",
]
