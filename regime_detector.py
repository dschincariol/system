"""Public shim for async market regime detection."""

from engine.regime_detector import (
    DEFAULT_REGIME_DETECTOR,
    RegimeDetector,
    classify_regime_snapshot,
    flush_regime_detector,
    get_latest_regime_snapshot,
    has_known_regime,
    normalize_regime_state,
    regime_signature,
    resolve_regime_snapshot,
    shutdown_regime_detector,
    submit_regime_refresh_nowait,
    unknown_regime_state,
)

__all__ = [
    "DEFAULT_REGIME_DETECTOR",
    "RegimeDetector",
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
