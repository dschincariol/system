"""
FILE: asset_map.py

Data subsystem module for `asset_map`.
"""

# dev_core/asset_map.py
"""
Asset-class mapping (A.4).
Used for training and inference fallback.

Default mapping is minimal and safe.
Override via env:
  ASSET_CLASS_MAP_JSON='{"SPY":"EQUITY","BTC":"CRYPTO","OIL":"COMMODITY"}'
"""

import json
import logging
import os

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger

_DEFAULT = {
    "SPY": "EQUITY",
    "BTC": "CRYPTO",
    "OIL": "COMMODITY",
}
LOG = get_logger("data.asset_map")


def _warn_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    log_failure(
        LOG,
        event="data_asset_map_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.data.asset_map",
        extra=dict(extra or {}) or None,
        persist=False,
    )

def _load_override():
    raw = os.environ.get("ASSET_CLASS_MAP_JSON", "").strip()
    if not raw:
        return {}
    try:
        d = json.loads(raw)
        if isinstance(d, dict):
            return {str(k).upper(): str(v).upper() for k, v in d.items()}
    except Exception as e:
        _warn_nonfatal(
            "DATA_ASSET_MAP_OVERRIDE_PARSE_FAILED",
            e,
            raw_preview=raw[:120],
        )
    return {}

_OVERRIDE = _load_override()

def asset_class_for_symbol(symbol: str) -> str:
    s = str(symbol or "").upper().strip()
    if not s:
        return "UNKNOWN"
    if s in _OVERRIDE:
        return _OVERRIDE[s]
    if s in _DEFAULT:
        return _DEFAULT[s]

    # Lightweight heuristics are a fallback only. If classification starts to
    # matter strategically, prefer explicit overrides or a richer registry.
    # lightweight heuristics (safe defaults)
    if s in ("QQQ", "DIA", "IWM", "VTI", "VOO"):
        return "EQUITY"
    if s in ("ETH", "SOL", "BNB", "XRP"):
        return "CRYPTO"
    if s in ("GC", "GOLD", "SI", "SILVER", "CL", "OIL", "NG"):
        return "COMMODITY"
    if s in ("DXY", "EURUSD", "USDJPY", "GBPUSD"):
        return "FX"
    if s in ("TLT", "IEF", "ZB", "ZN"):
        return "RATES"

    return "UNKNOWN"
