from __future__ import annotations

import json
import logging
import os
import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger

_VALID_TICKER_RX = re.compile(r"^[A-Z][A-Z0-9]{0,5}$")
LOG = get_logger("data.default_symbols")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="data_default_symbols_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.data.default_symbols",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)

ETF_SEED_SYMBOLS = [
    "SPY", "QQQ", "IWM", "DIA", "VTI", "VOO", "IVV", "VEA", "VWO", "TLT",
    "IEF", "SHY", "HYG", "LQD", "XLF", "XLE", "XLK", "XLV", "XLY", "XLP",
    "XLI", "XLB", "XLU", "XLC", "XBI", "SMH", "SOXX", "ARKK", "GDX", "XOP",
    "KRE", "IYR", "GLD", "SLV", "USO", "UNG", "DBC", "AGG", "VNQ", "XHB",
]

CROSS_ASSET_SEED_SYMBOLS = [
    "BTC", "ETH", "SOL", "BNB", "XRP", "MSTR", "COIN", "IBIT", "GBTC", "OIL",
]

FX_MAJOR_SEED_SYMBOLS = [
    "EURUSD",
    "USDJPY",
    "GBPUSD",
    "USDCHF",
    "USDCAD",
    "AUDUSD",
    "NZDUSD",
]


def _env_enabled(name: str, default: bool = False) -> bool:
    raw = os.environ.get(str(name))
    if raw is None or str(raw).strip() == "":
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def oanda_instrument_to_fx_pair(instrument: object) -> str:
    pair = str(instrument or "").strip().upper().replace("_", "")
    if not re.fullmatch(r"[A-Z]{6}", pair):
        raise ValueError(f"invalid_oanda_fx_instrument:{instrument}")
    return pair


def fx_pair_to_oanda_instrument(symbol: object) -> str:
    pair = oanda_instrument_to_fx_pair(symbol)
    return f"{pair[:3]}_{pair[3:]}"


def is_fx_major_symbol(symbol: object) -> bool:
    return str(symbol or "").strip().upper() in set(FX_MAJOR_SEED_SYMBOLS)


def default_symbol_metadata(symbol: object) -> Dict[str, object]:
    sym = str(symbol or "").strip().upper()
    if not is_fx_major_symbol(sym):
        return {}
    return {
        "price_provider": "oanda",
        "oanda_instrument": fx_pair_to_oanda_instrument(sym),
        "source_fx_seed": True,
    }


def load_fx_seed_symbols() -> List[str]:
    override_candidates: List[str] = []
    for part in str(os.environ.get("OANDA_FX_PAIRS", "") or "").split(","):
        if not str(part or "").strip():
            continue
        try:
            override_candidates.append(oanda_instrument_to_fx_pair(part))
        except ValueError as e:
            _warn_nonfatal(
                "DEFAULT_SYMBOLS_INVALID_OANDA_FX_PAIR",
                e,
                once_key=f"invalid_oanda_fx_pair:{part}",
                value=repr(part)[:120],
            )
    override = _dedupe_symbols(override_candidates)
    if override:
        return [sym for sym in override if _VALID_TICKER_RX.match(sym)]
    if _env_enabled("FX_PAIRS_ENABLED", False):
        return list(FX_MAJOR_SEED_SYMBOLS)
    return []


def _safe_int(value: object, default: int) -> int:
    try:
        return int(float(str(value if value is not None else default).strip()))
    except Exception as e:
        _warn_nonfatal(
            "DEFAULT_SYMBOLS_SAFE_INT_FAILED",
            e,
            once_key="safe_int",
            value=repr(value)[:120],
            default=default,
        )
        return int(default)


def _dedupe_symbols(values: Iterable[object]) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values or []:
        symbol = str(value or "").strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        out.append(symbol)
    return out


def _sec_ticker_map_path() -> Path:
    raw = str(os.environ.get("SEC_TICKER_MAP_CACHE", "data/sec_company_tickers_exchange.json") or "").strip()
    path = Path(raw)
    if path.is_absolute():
        return path
    return (Path(__file__).resolve().parents[2] / path).resolve()


def parse_symbol_limit(value: object, default: Optional[int]) -> Optional[int]:
    if value is None or str(value).strip() == "":
        if default is None:
            return None
        value = default
    try:
        limit = int(float(str(value).strip()))
    except Exception as e:
        _warn_nonfatal(
            "DEFAULT_SYMBOLS_PARSE_SYMBOL_LIMIT_FAILED",
            e,
            once_key="parse_symbol_limit",
            value=repr(value)[:120],
            default=repr(default),
        )
        return int(default) if default is not None else None
    if limit <= 0:
        return None
    return int(limit)


def _read_symbols_file(path: Path) -> List[str]:
    try:
        raw = path.read_text(encoding="utf-8")
    except Exception as e:
        _warn_nonfatal(
            "DEFAULT_SYMBOLS_READ_SYMBOLS_FILE_FAILED",
            e,
            once_key=f"read_symbols_file:{path}",
            path=str(path),
        )
        return []

    try:
        payload = json.loads(raw)
    except Exception:
        payload = None

    if isinstance(payload, dict):
        for key in ("symbols", "tickers", "items"):
            if isinstance(payload.get(key), list):
                return _dedupe_symbols(payload.get(key) or [])
    if isinstance(payload, list):
        return _dedupe_symbols(payload)

    return _dedupe_symbols(part for part in raw.replace("\n", ",").split(","))


def load_sec_seed_symbols(top_n: Optional[int] = None) -> List[str]:
    raw_limit = top_n if top_n is not None else os.environ.get("DEFAULT_SYMBOLS_SEC_TOP_N")
    limit = parse_symbol_limit(
        raw_limit,
        400,
    )
    if limit is None:
        return []

    path = _sec_ticker_map_path()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        _warn_nonfatal(
            "DEFAULT_SYMBOLS_LOAD_SEC_SEED_SYMBOLS_FAILED",
            e,
            once_key=f"load_sec_seed_symbols:{path}",
            path=str(path),
        )
        return []

    fields = list(payload.get("fields") or []) if isinstance(payload, dict) else []
    rows = list(payload.get("data") or []) if isinstance(payload, dict) else []
    if not fields or not rows:
        _warn_nonfatal(
            "DEFAULT_SYMBOLS_LOAD_SEC_SEED_SYMBOLS_EMPTY_PAYLOAD",
            RuntimeError("empty sec ticker payload"),
            once_key=f"load_sec_seed_symbols_empty:{path}",
            path=str(path),
            fields=len(fields),
            rows=len(rows),
        )
        return []

    try:
        ticker_idx = fields.index("ticker")
    except ValueError as e:
        _warn_nonfatal(
            "DEFAULT_SYMBOLS_LOAD_SEC_SEED_SYMBOLS_MISSING_TICKER_FIELD",
            e,
            once_key=f"load_sec_seed_symbols_missing_ticker:{path}",
            path=str(path),
            fields=fields[:20],
        )
        return []

    exchange_idx = fields.index("exchange") if "exchange" in fields else -1
    allowed_exchanges = {"NASDAQ", "NYSE", "NYSE ARCA", "NYSE AMERICAN", "AMEX"}

    out: List[str] = []
    seen = set()
    for row in rows:
        if not isinstance(row, list) or ticker_idx >= len(row):
            continue
        ticker = str(row[ticker_idx] or "").strip().upper()
        if not ticker or ticker in seen or not _VALID_TICKER_RX.match(ticker):
            continue
        if exchange_idx >= 0 and exchange_idx < len(row):
            exchange = str(row[exchange_idx] or "").strip().upper()
            if exchange and exchange not in allowed_exchanges:
                continue
        seen.add(ticker)
        out.append(ticker)
        if len(out) >= int(limit):
            break
    return out


def load_default_symbols(*, extra: Optional[Iterable[object]] = None) -> List[str]:
    explicit = _dedupe_symbols(
        part for part in str(os.environ.get("DEFAULT_SYMBOLS", "") or "").split(",")
    )
    file_path = str(os.environ.get("DEFAULT_SYMBOLS_FILE", "") or "").strip()
    file_symbols = _read_symbols_file(Path(file_path)) if file_path else []
    include_seed_symbols = str(
        os.environ.get("DEFAULT_SYMBOLS_INCLUDE_SEEDS", "1") or "1"
    ).strip().lower() in ("1", "true", "yes", "on")
    sec_symbols = load_sec_seed_symbols() if include_seed_symbols else []
    etf_symbols = ETF_SEED_SYMBOLS if include_seed_symbols else []
    cross_asset_symbols = CROSS_ASSET_SEED_SYMBOLS if include_seed_symbols else []
    fx_symbols = load_fx_seed_symbols() if include_seed_symbols else []
    return _dedupe_symbols(
        [
            *explicit,
            *file_symbols,
            *etf_symbols,
            *cross_asset_symbols,
            *fx_symbols,
            *sec_symbols,
            *(extra or []),
        ]
    )
