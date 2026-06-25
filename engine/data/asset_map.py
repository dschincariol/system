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
from typing import FrozenSet

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


try:
    from engine.data.fx_instrument import is_fx_symbol  # type: ignore
except Exception as e:
    _warn_nonfatal(
        "DATA_ASSET_MAP_FX_INSTRUMENT_IMPORT_FAILED",
        e,
    )

    def is_fx_symbol(symbol: object) -> bool:
        return False


try:
    from engine.data.futures_instrument import is_futures_symbol  # type: ignore
except Exception as e:
    _warn_nonfatal(
        "DATA_ASSET_MAP_FUTURES_INSTRUMENT_IMPORT_FAILED",
        e,
    )

    def is_futures_symbol(symbol: object) -> bool:
        return False


try:
    from engine.data.options_instrument import is_option_symbol  # type: ignore
except Exception as e:
    _warn_nonfatal(
        "DATA_ASSET_MAP_OPTIONS_INSTRUMENT_IMPORT_FAILED",
        e,
    )

    def is_option_symbol(symbol: object) -> bool:
        return False


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


_EQUITY_REGISTRY_EXCHANGES = {
    "NASDAQ",
    "NYSE",
    "NYSE ARCA",
    "NYSE AMERICAN",
    "AMEX",
    # Exchange-listed ETFs on CBOE use the equity rails; OTC/null venues stay UNKNOWN.
    "CBOE",
}


def _env_enabled(name: str, default: bool = False) -> bool:
    raw = os.environ.get(str(name))
    if raw is None or str(raw).strip() == "":
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "on", "y"}


def _load_equity_registry() -> FrozenSet[str]:
    if not _env_enabled("ASSET_MAP_USE_EQUITY_REGISTRY", True):
        return frozenset()

    try:
        from engine.data.default_symbols import _VALID_TICKER_RX, _sec_ticker_map_path
    except Exception as e:
        _warn_nonfatal(
            "DATA_ASSET_MAP_EQUITY_REGISTRY_IMPORT_FAILED",
            e,
        )
        return frozenset()

    try:
        path = _sec_ticker_map_path()
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        _warn_nonfatal(
            "DATA_ASSET_MAP_EQUITY_REGISTRY_LOAD_FAILED",
            e,
        )
        return frozenset()

    fields = list(payload.get("fields") or []) if isinstance(payload, dict) else []
    rows = list(payload.get("data") or []) if isinstance(payload, dict) else []
    if not fields or not rows:
        _warn_nonfatal(
            "DATA_ASSET_MAP_EQUITY_REGISTRY_EMPTY_PAYLOAD",
            RuntimeError("empty sec ticker payload"),
        )
        return frozenset()

    try:
        ticker_idx = fields.index("ticker")
        exchange_idx = fields.index("exchange")
    except ValueError as e:
        _warn_nonfatal(
            "DATA_ASSET_MAP_EQUITY_REGISTRY_MISSING_FIELD",
            e,
            fields=fields[:20],
        )
        return frozenset()

    out: set[str] = set()
    for row in rows:
        if not isinstance(row, list) or ticker_idx >= len(row) or exchange_idx >= len(row):
            continue
        ticker = str(row[ticker_idx] or "").strip().upper()
        if not ticker or not _VALID_TICKER_RX.match(ticker):
            continue
        exchange = str(row[exchange_idx] or "").strip().upper()
        if exchange not in _EQUITY_REGISTRY_EXCHANGES:
            continue
        out.add(ticker)
    return frozenset(out)


_OVERRIDE = _load_override()
_EQUITY_REGISTRY = _load_equity_registry()


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
    if is_futures_symbol(s):
        return "FUTURES"
    if is_option_symbol(s):
        return "OPTION"
    if is_fx_symbol(s):
        return "FX"
    if s in ("TLT", "IEF", "ZB", "ZN"):
        return "RATES"
    if s in _EQUITY_REGISTRY:
        return "EQUITY"

    return "UNKNOWN"
