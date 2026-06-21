"""Read-only Deribit public crypto derivatives market-data adapter.

Deribit is treated as a crypto derivatives signal source, not as a broker or
prediction-market feed.  This module only calls public market-data endpoints,
normalizes point-in-time snapshots, and computes shadow-only feature values for
crypto symbols.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

import requests

try:
    import websocket  # type: ignore
except Exception as exc:  # pragma: no cover - import depends on deployment profile
    websocket = None  # type: ignore
    _WEBSOCKET_IMPORT_ERROR: Exception | None = exc
else:  # pragma: no cover - exercised only when dependency is installed
    _WEBSOCKET_IMPORT_ERROR = None

DERIBIT_BASE_URL = "https://www.deribit.com"
DERIBIT_WS_BASE_URL = "wss://www.deribit.com/ws/api/v2"
DERIBIT_FEATURE_GROUP = "deribit_crypto_derivatives_v1"
DERIBIT_FEATURE_PREFIX = f"{DERIBIT_FEATURE_GROUP}."
DERIBIT_FEATURE_IDS = [
    f"{DERIBIT_FEATURE_PREFIX}iv_rank",
    f"{DERIBIT_FEATURE_PREFIX}short_dated_iv",
    f"{DERIBIT_FEATURE_PREFIX}skew_25d_proxy",
    f"{DERIBIT_FEATURE_PREFIX}term_structure_slope",
    f"{DERIBIT_FEATURE_PREFIX}put_call_open_interest_ratio",
    f"{DERIBIT_FEATURE_PREFIX}futures_basis",
    f"{DERIBIT_FEATURE_PREFIX}perp_basis",
    f"{DERIBIT_FEATURE_PREFIX}funding_pressure",
    f"{DERIBIT_FEATURE_PREFIX}volume_shock",
    f"{DERIBIT_FEATURE_PREFIX}vol_regime_high",
    f"{DERIBIT_FEATURE_PREFIX}vol_regime_low",
    f"{DERIBIT_FEATURE_PREFIX}available",
]

DERIBIT_FORBIDDEN_SETTING_KEYS = {
    "access_token",
    "account",
    "api_key",
    "api_secret",
    "auth",
    "client_id",
    "client_secret",
    "private_key",
    "refresh_token",
    "secret",
    "trade",
    "trading",
    "wallet",
}
DERIBIT_WEBSOCKET_MODES = {"ws", "wss", "websocket"}
DERIBIT_ALLOWED_MODES = {"http", "rest", "https"} | DERIBIT_WEBSOCKET_MODES
DERIBIT_ALLOWED_INSTRUMENT_TYPES = {"future", "futures", "perpetual", "perpetuals", "option", "options"}
DERIBIT_CRYPTO_EQUITY_MAP = {
    "COIN": ("BTC", "ETH", "SOL"),
    "MSTR": ("BTC",),
    "MARA": ("BTC",),
    "RIOT": ("BTC",),
    "CLSK": ("BTC",),
    "HUT": ("BTC",),
    "IBIT": ("BTC",),
    "GBTC": ("BTC",),
    "ETHE": ("ETH",),
}

_EXPIRY_RE = re.compile(r"^\d{1,2}[A-Z]{3}\d{2}$")


@dataclass(frozen=True)
class DeribitSettings:
    """Runtime settings for public Deribit polling."""

    enabled_assets: tuple[str, ...] = ("BTC", "ETH", "SOL")
    instrument_types: tuple[str, ...] = ("future", "option")
    expiry_days: int | None = 90
    min_open_interest: float = 0.0
    min_volume: float = 0.0
    poll_seconds: float = 900.0
    mode: str = "http"
    stale_threshold_ms: int = 30 * 60 * 1000
    base_url: str = DERIBIT_BASE_URL
    timeout_s: float = 10.0
    include_ticker: bool = True
    max_tickers: int = 50
    include_order_book: bool = False
    max_order_books: int = 12
    order_book_depth: int = 1
    max_spread_bps: float = 500.0
    max_instruments: int = 500


def utc_ms() -> int:
    return int(time.time() * 1000)


def _clean_symbol(value: Any) -> str:
    return str(value or "").upper().strip().replace("$", "")


def _safe_float(value: Any, default: float | None = None) -> float | None:
    if value is None:
        return default
    try:
        out = float(value)
    except Exception:
        return default
    if not math.isfinite(out):
        return default
    return float(out)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        out = int(float(value))
    except Exception:
        return int(default)
    return int(out)


def _bool_value(value: Any, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def parse_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value or "").strip()
    if not text:
        return []
    if text.startswith("["):
        try:
            parsed = json.loads(text)
        except Exception:
            parsed = None
        if isinstance(parsed, list):
            return [str(item).strip() for item in parsed if str(item).strip()]
    return [part.strip() for part in text.split(",") if part.strip()]


def _json_dumps(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True, default=str)


def _json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            loaded = json.loads(value)
        except Exception:
            return {}
        return dict(loaded) if isinstance(loaded, Mapping) else {}
    return {}


def _hash_id(*parts: Any) -> str:
    raw = "|".join(str(part or "") for part in parts)
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]
    return f"deribit:{digest}"


def _derive_deribit_ws_url(base_url: str | None = None) -> str:
    text = str(base_url or DERIBIT_BASE_URL).rstrip("/")
    if text.startswith(("ws://", "wss://")):
        if text.endswith("/ws/api/v2"):
            return text
        text = re.sub(r"/api/v2$", "", text)
        return f"{text}/ws/api/v2"
    if text.startswith("https://"):
        host = re.sub(r"/api/v2$", "", text[len("https://") :])
        return f"wss://{host}/ws/api/v2"
    if text.startswith("http://"):
        host = re.sub(r"/api/v2$", "", text[len("http://") :])
        return f"ws://{host}/ws/api/v2"
    return DERIBIT_WS_BASE_URL


def validate_deribit_settings(settings: Mapping[str, Any] | None = None) -> None:
    keys = {str(key or "").strip().lower() for key in dict(settings or {}).keys()}
    forbidden = sorted(key for key in keys if key in DERIBIT_FORBIDDEN_SETTING_KEYS)
    if forbidden:
        raise ValueError(f"deribit_authenticated_or_trading_settings_forbidden:{','.join(forbidden)}")


def load_deribit_settings(settings: Mapping[str, Any] | None = None) -> DeribitSettings:
    source = dict(settings or {})
    validate_deribit_settings(source)

    def _setting(name: str, env_name: str, default: Any) -> Any:
        if name in source and source.get(name) not in (None, ""):
            return source.get(name)
        return os.environ.get(env_name, default)

    assets = [_clean_symbol(asset) for asset in parse_list(_setting("enabled_assets", "DERIBIT_ENABLED_ASSETS", "BTC,ETH,SOL"))]
    instrument_types = [
        str(item or "").strip().lower()
        for item in parse_list(_setting("instrument_types", "DERIBIT_INSTRUMENT_TYPES", "future,option"))
    ]
    instrument_types = [item for item in instrument_types if item in DERIBIT_ALLOWED_INSTRUMENT_TYPES]
    expiry_raw = str(_setting("expiry_days", "DERIBIT_EXPIRY_DAYS", "90") or "").strip().lower()
    expiry_days = None if expiry_raw in {"", "all", "*", "none"} else max(1, _safe_int(expiry_raw, 90))
    mode = str(_setting("mode", "DERIBIT_MODE", "http") or "http").strip().lower()
    return DeribitSettings(
        enabled_assets=tuple(assets or ["BTC", "ETH", "SOL"]),
        instrument_types=tuple(instrument_types or ["future", "option"]),
        expiry_days=expiry_days,
        min_open_interest=max(0.0, float(_safe_float(_setting("min_open_interest", "DERIBIT_MIN_OPEN_INTEREST", 0.0), 0.0) or 0.0)),
        min_volume=max(0.0, float(_safe_float(_setting("min_volume", "DERIBIT_MIN_VOLUME", 0.0), 0.0) or 0.0)),
        poll_seconds=max(60.0, float(_safe_float(_setting("poll_seconds", "DERIBIT_POLL_SECONDS", 900.0), 900.0) or 900.0)),
        mode=mode,
        stale_threshold_ms=max(60_000, _safe_int(_setting("stale_threshold_ms", "DERIBIT_STALE_THRESHOLD_MS", 30 * 60 * 1000), 30 * 60 * 1000)),
        base_url=str(_setting("base_url", "DERIBIT_BASE_URL", DERIBIT_BASE_URL) or DERIBIT_BASE_URL).rstrip("/"),
        timeout_s=max(1.0, float(_safe_float(_setting("timeout_s", "DERIBIT_TIMEOUT_S", 10.0), 10.0) or 10.0)),
        include_ticker=_bool_value(_setting("include_ticker", "DERIBIT_INCLUDE_TICKER", True), True),
        max_tickers=max(0, _safe_int(_setting("max_tickers", "DERIBIT_MAX_TICKERS", 50), 50)),
        include_order_book=_bool_value(_setting("include_order_book", "DERIBIT_INCLUDE_ORDER_BOOK", False), False),
        max_order_books=max(0, _safe_int(_setting("max_order_books", "DERIBIT_MAX_ORDER_BOOKS", 12), 12)),
        order_book_depth=max(1, min(100, _safe_int(_setting("order_book_depth", "DERIBIT_ORDER_BOOK_DEPTH", 1), 1))),
        max_spread_bps=max(1.0, float(_safe_float(_setting("max_spread_bps", "DERIBIT_MAX_SPREAD_BPS", 500.0), 500.0) or 500.0)),
        max_instruments=max(1, _safe_int(_setting("max_instruments", "DERIBIT_MAX_INSTRUMENTS", 500), 500)),
    )


def _expiry_from_token(token: str) -> int | None:
    text = str(token or "").upper().strip()
    if not _EXPIRY_RE.match(text):
        return None
    try:
        dt = datetime.strptime(text, "%d%b%y").replace(tzinfo=timezone.utc, hour=8)
    except Exception:
        return None
    return int(dt.timestamp() * 1000)


def parse_deribit_instrument_name(name: str) -> dict[str, Any]:
    """Parse Deribit instrument names such as ``BTC-PERPETUAL`` or options."""

    text = str(name or "").upper().strip()
    parts = [part for part in text.split("-") if part]
    base = parts[0] if parts else ""
    result: dict[str, Any] = {
        "instrument_name": text,
        "base_asset": base,
        "expiry_ts_ms": None,
        "strike": None,
        "option_type": "",
        "instrument_type": "",
        "settlement_period": "",
    }
    if len(parts) >= 2 and parts[1] == "PERPETUAL":
        result["instrument_type"] = "perpetual"
        result["settlement_period"] = "perpetual"
        return result
    if len(parts) >= 2:
        result["expiry_ts_ms"] = _expiry_from_token(parts[1])
    if len(parts) >= 4 and parts[-1] in {"C", "P"}:
        result["instrument_type"] = "option"
        result["strike"] = _safe_float(parts[-2])
        result["option_type"] = "call" if parts[-1] == "C" else "put"
        return result
    if len(parts) >= 2:
        result["instrument_type"] = "future"
    return result


def normalize_deribit_instrument(payload: Mapping[str, Any], *, now_ms: int | None = None) -> dict[str, Any] | None:
    raw = dict(payload or {})
    name = str(raw.get("instrument_name") or "").upper().strip()
    if not name:
        return None
    parsed = parse_deribit_instrument_name(name)
    kind = str(raw.get("kind") or "").strip().lower()
    settlement_period = str(raw.get("settlement_period") or parsed.get("settlement_period") or "").strip().lower()
    instrument_type = str(parsed.get("instrument_type") or "").strip().lower()
    if kind == "option":
        instrument_type = "option"
    elif settlement_period == "perpetual" or name.endswith("-PERPETUAL"):
        instrument_type = "perpetual"
    elif kind == "future":
        instrument_type = "future"
    option_type = str(raw.get("option_type") or parsed.get("option_type") or "").strip().lower()
    if option_type in {"c", "call"}:
        option_type = "call"
    elif option_type in {"p", "put"}:
        option_type = "put"
    else:
        option_type = ""
    expiry = _safe_int(raw.get("expiration_timestamp"), 0) or parsed.get("expiry_ts_ms")
    observed = int(now_ms or utc_ms())
    return {
        "instrument_name": name,
        "base_asset": _clean_symbol(raw.get("base_currency") or parsed.get("base_asset")),
        "quote_currency": _clean_symbol(raw.get("quote_currency") or raw.get("counter_currency") or "USD"),
        "instrument_type": instrument_type,
        "kind": kind,
        "settlement_period": settlement_period,
        "expiry_ts_ms": int(expiry) if expiry else None,
        "strike": _safe_float(raw.get("strike") if raw.get("strike") is not None else parsed.get("strike")),
        "option_type": option_type,
        "is_active": bool(raw.get("is_active", True)) and str(raw.get("state") or "open").lower() not in {"inactive", "delivered", "archivized"},
        "source_ts_ms": observed,
        "availability_ts_ms": observed,
        "raw_json": raw,
    }


def _normalize_iv(value: Any) -> float | None:
    out = _safe_float(value)
    if out is None or out <= 0.0:
        return None
    return float(out / 100.0) if out > 3.0 else float(out)


def _first_number(*values: Any) -> float | None:
    for value in values:
        out = _safe_float(value)
        if out is not None:
            return float(out)
    return None


def _nested(payload: Mapping[str, Any], *keys: str) -> Any:
    obj: Any = payload
    for key in keys:
        if not isinstance(obj, Mapping):
            return None
        obj = obj.get(key)
    return obj


def _best_level(order_book: Mapping[str, Any] | None, side: str) -> tuple[float | None, float | None]:
    levels = list((order_book or {}).get(side) or [])
    if not levels:
        return None, None
    first = levels[0]
    if isinstance(first, (list, tuple)) and len(first) >= 2:
        return _safe_float(first[0]), _safe_float(first[1])
    return None, None


def _basis(mark_price: Any, index_price: Any) -> float | None:
    mark = _safe_float(mark_price)
    index = _safe_float(index_price)
    if mark is None or index is None or index <= 0.0:
        return None
    return float((mark - index) / index)


def _spread_bps(bid: Any, ask: Any, mark: Any = None) -> float | None:
    bid_f = _safe_float(bid)
    ask_f = _safe_float(ask)
    mark_f = _safe_float(mark)
    if bid_f is None or ask_f is None or bid_f <= 0.0 or ask_f <= 0.0 or ask_f < bid_f:
        return None
    mid = mark_f if mark_f and mark_f > 0.0 else (bid_f + ask_f) / 2.0
    if mid <= 0.0:
        return None
    return float((ask_f - bid_f) / mid * 10_000.0)


def normalize_deribit_snapshot(
    summary: Mapping[str, Any],
    *,
    instrument: Mapping[str, Any] | None = None,
    ticker: Mapping[str, Any] | None = None,
    order_book: Mapping[str, Any] | None = None,
    now_ms: int | None = None,
) -> dict[str, Any] | None:
    """Normalize public Deribit market-data snapshots into a PIT row."""

    base_summary = dict(summary or {})
    base_ticker = dict(ticker or {})
    base_book = dict(order_book or {})
    inst = dict(instrument or {})
    name = str(
        base_summary.get("instrument_name")
        or base_ticker.get("instrument_name")
        or base_book.get("instrument_name")
        or inst.get("instrument_name")
        or ""
    ).upper().strip()
    if not name:
        return None
    if not inst:
        inst = normalize_deribit_instrument({"instrument_name": name}, now_ms=now_ms) or {}
    observed = int(now_ms or utc_ms())
    source_ts = max(
        [
            _safe_int(base_summary.get("timestamp") or base_summary.get("creation_timestamp"), 0),
            _safe_int(base_ticker.get("timestamp"), 0),
            _safe_int(base_book.get("timestamp"), 0),
            observed,
        ]
    )
    stats = dict(base_ticker.get("stats") or {})
    if not stats:
        stats = dict(base_book.get("stats") or {})
    book_bid, book_bid_amount = _best_level(base_book, "bids")
    book_ask, book_ask_amount = _best_level(base_book, "asks")
    bid = _first_number(
        base_ticker.get("best_bid_price"),
        base_book.get("best_bid_price"),
        book_bid,
        base_summary.get("bid_price"),
    )
    ask = _first_number(
        base_ticker.get("best_ask_price"),
        base_book.get("best_ask_price"),
        book_ask,
        base_summary.get("ask_price"),
    )
    mark = _first_number(base_ticker.get("mark_price"), base_book.get("mark_price"), base_summary.get("mark_price"))
    index = _first_number(base_ticker.get("index_price"), base_book.get("index_price"), base_summary.get("index_price"))
    instrument_type = str(inst.get("instrument_type") or "").strip().lower()
    basis = _basis(mark, index)
    bid_iv = _normalize_iv(_first_number(base_ticker.get("bid_iv"), base_summary.get("bid_iv")))
    ask_iv = _normalize_iv(_first_number(base_ticker.get("ask_iv"), base_summary.get("ask_iv")))
    mark_iv = _normalize_iv(_first_number(base_ticker.get("mark_iv"), base_book.get("mark_iv"), base_summary.get("mark_iv")))
    greeks = dict(base_ticker.get("greeks") or base_book.get("greeks") or {})
    spread = _spread_bps(bid, ask, mark)
    volume = _first_number(base_summary.get("volume"), stats.get("volume"))
    volume_usd = _first_number(base_summary.get("volume_usd"), stats.get("volume_usd"))
    row = {
        "source_record_id": _hash_id(name, source_ts, mark, bid, ask, mark_iv, volume),
        "instrument_name": name,
        "base_asset": _clean_symbol(inst.get("base_asset") or base_summary.get("base_currency")),
        "quote_currency": _clean_symbol(inst.get("quote_currency") or base_summary.get("quote_currency") or "USD"),
        "instrument_type": instrument_type,
        "kind": str(inst.get("kind") or "").lower(),
        "expiry_ts_ms": inst.get("expiry_ts_ms"),
        "strike": _safe_float(inst.get("strike")),
        "option_type": str(inst.get("option_type") or "").lower(),
        "mark_price": mark,
        "index_price": index,
        "bid_price": bid,
        "ask_price": ask,
        "mid_price": _first_number(base_summary.get("mid_price"), ((bid + ask) / 2.0 if bid and ask else None)),
        "last_price": _first_number(base_ticker.get("last_price"), base_book.get("last_price"), base_summary.get("last")),
        "underlying_price": _first_number(base_ticker.get("underlying_price"), base_summary.get("underlying_price")),
        "bid_iv": bid_iv,
        "ask_iv": ask_iv,
        "mark_iv": mark_iv,
        "delta": _safe_float(greeks.get("delta")),
        "gamma": _safe_float(greeks.get("gamma")),
        "theta": _safe_float(greeks.get("theta")),
        "vega": _safe_float(greeks.get("vega")),
        "open_interest": _first_number(base_ticker.get("open_interest"), base_book.get("open_interest"), base_summary.get("open_interest")),
        "volume": volume,
        "volume_usd": volume_usd,
        "current_funding": _first_number(base_ticker.get("current_funding"), base_book.get("current_funding"), base_summary.get("current_funding")),
        "funding_8h": _first_number(base_ticker.get("funding_8h"), base_book.get("funding_8h"), base_summary.get("funding_8h")),
        "futures_basis": basis if instrument_type == "future" else None,
        "perp_basis": basis if instrument_type == "perpetual" else None,
        "best_bid_amount": _first_number(base_ticker.get("best_bid_amount"), base_book.get("best_bid_amount"), book_bid_amount),
        "best_ask_amount": _first_number(base_ticker.get("best_ask_amount"), base_book.get("best_ask_amount"), book_ask_amount),
        "spread_bps": spread,
        "source_ts_ms": int(source_ts),
        "availability_ts_ms": int(observed),
        "ingested_ts_ms": int(observed),
        "raw_json": {
            "summary": base_summary,
            "ticker": base_ticker,
            "order_book": base_book,
            "instrument": inst,
        },
        "diagnostics_json": {
            "missing_iv": bool(instrument_type == "option" and mark_iv is None and bid_iv is None and ask_iv is None),
            "spread_bps": spread,
            "has_order_book": bool(base_book),
            "public_market_data_only": True,
            "direct_trading_authority": False,
        },
    }
    return row


def _instrument_type_allowed(instrument: Mapping[str, Any], settings: DeribitSettings) -> bool:
    requested = {str(item).lower() for item in settings.instrument_types}
    inst_type = str(instrument.get("instrument_type") or "").lower()
    if inst_type == "perpetual":
        return "perpetual" in requested or "perpetuals" in requested or "future" in requested or "futures" in requested
    if inst_type == "future":
        return "future" in requested or "futures" in requested
    if inst_type == "option":
        return "option" in requested or "options" in requested
    return False


def _expiry_allowed(instrument: Mapping[str, Any], settings: DeribitSettings, *, now_ms: int) -> bool:
    if settings.expiry_days is None:
        return True
    expiry = _safe_int(instrument.get("expiry_ts_ms"), 0)
    if expiry <= 0:
        return True
    max_ts = int(now_ms) + int(settings.expiry_days * 86_400_000)
    return int(expiry) <= int(max_ts)


def _liquidity_ok(snapshot: Mapping[str, Any], settings: DeribitSettings) -> bool:
    oi = _safe_float(snapshot.get("open_interest"), 0.0) or 0.0
    volume = _safe_float(snapshot.get("volume"), 0.0) or 0.0
    return bool(oi >= float(settings.min_open_interest) and volume >= float(settings.min_volume))


class DeribitPublicClient:
    """Small HTTP client for Deribit public JSON-RPC market-data methods."""

    def __init__(self, *, base_url: str = DERIBIT_BASE_URL, timeout_s: float = 10.0, session: Any = None) -> None:
        self.base_url = str(base_url or DERIBIT_BASE_URL).rstrip("/")
        self.timeout_s = float(timeout_s)
        self.session = session or requests.Session()

    def public_get(self, method: str, params: Mapping[str, Any] | None = None) -> Any:
        method_path = str(method or "").strip().lstrip("/")
        if not method_path.startswith("public/"):
            raise ValueError(f"deribit_public_method_required:{method}")
        url = f"{self.base_url}/api/v2/{method_path}"
        response = self.session.get(url, params=dict(params or {}), timeout=float(self.timeout_s))
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, Mapping) and payload.get("error"):
            raise RuntimeError(f"deribit_api_error:{payload.get('error')}")
        return payload.get("result") if isinstance(payload, Mapping) and "result" in payload else payload

    def get_instruments(self, *, currency: str, kind: str) -> list[dict[str, Any]]:
        result = self.public_get(
            "public/get_instruments",
            {"currency": _clean_symbol(currency), "kind": str(kind), "expired": "false"},
        )
        return [dict(item or {}) for item in list(result or []) if isinstance(item, Mapping)]

    def get_book_summary_by_currency(self, *, currency: str, kind: str) -> list[dict[str, Any]]:
        result = self.public_get(
            "public/get_book_summary_by_currency",
            {"currency": _clean_symbol(currency), "kind": str(kind)},
        )
        return [dict(item or {}) for item in list(result or []) if isinstance(item, Mapping)]

    def get_order_book(self, *, instrument_name: str, depth: int = 1) -> dict[str, Any]:
        result = self.public_get(
            "public/get_order_book",
            {"instrument_name": str(instrument_name), "depth": int(depth)},
        )
        return dict(result or {}) if isinstance(result, Mapping) else {}

    def ticker(self, *, instrument_name: str) -> dict[str, Any]:
        result = self.public_get("public/ticker", {"instrument_name": str(instrument_name)})
        return dict(result or {}) if isinstance(result, Mapping) else {}


class DeribitPublicWebSocketClient(DeribitPublicClient):
    """Public JSON-RPC-over-WebSocket Deribit client.

    The client intentionally exposes the same read-only public method surface as
    the HTTP client and opens short-lived request/response connections for the
    supervised poller. Streaming subscriptions can be layered on later without
    changing the normalized storage contract.
    """

    def __init__(
        self,
        *,
        base_url: str = DERIBIT_BASE_URL,
        timeout_s: float = 10.0,
        connection_factory: Any = None,
    ) -> None:
        self.base_url = str(base_url or DERIBIT_BASE_URL).rstrip("/")
        self.ws_url = _derive_deribit_ws_url(self.base_url)
        self.timeout_s = float(timeout_s)
        self.connection_factory = connection_factory
        self._request_id = 0
        self.last_reconnect_state = "never_connected"
        self.last_error = ""

    def public_get(self, method: str, params: Mapping[str, Any] | None = None) -> Any:
        method_path = str(method or "").strip().lstrip("/")
        if not method_path.startswith("public/"):
            raise ValueError(f"deribit_public_method_required:{method}")
        if self.connection_factory is None and websocket is None:
            raise RuntimeError(f"websocket_client_unavailable:{_WEBSOCKET_IMPORT_ERROR}")
        self._request_id += 1
        request = {
            "jsonrpc": "2.0",
            "id": self._request_id,
            "method": method_path,
            "params": dict(params or {}),
        }
        conn = None
        try:
            if self.connection_factory is not None:
                conn = self.connection_factory(self.ws_url, timeout=float(self.timeout_s))
            else:
                conn = websocket.create_connection(self.ws_url, timeout=float(self.timeout_s))  # type: ignore[union-attr]
            self.last_reconnect_state = "connected"
            conn.send(_json_dumps(request))
            payload = json.loads(conn.recv())
            if isinstance(payload, Mapping) and payload.get("error"):
                raise RuntimeError(f"deribit_api_error:{payload.get('error')}")
            return payload.get("result") if isinstance(payload, Mapping) and "result" in payload else payload
        except Exception as exc:
            self.last_reconnect_state = "error"
            self.last_error = str(exc)
            raise
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass


def fetch_deribit_public_batch(
    settings: Mapping[str, Any] | DeribitSettings | None = None,
    *,
    client: DeribitPublicClient | None = None,
    now_ms: int | None = None,
) -> dict[str, Any]:
    """Fetch and normalize a public Deribit instrument/snapshot batch."""

    config = settings if isinstance(settings, DeribitSettings) else load_deribit_settings(settings)
    mode = str(config.mode).lower()
    if mode not in DERIBIT_ALLOWED_MODES:
        raise ValueError(f"deribit_mode_not_supported_for_read_only_public_poller:{config.mode}")
    observed = int(now_ms or utc_ms())
    api = client or (
        DeribitPublicWebSocketClient(base_url=config.base_url, timeout_s=config.timeout_s)
        if mode in DERIBIT_WEBSOCKET_MODES
        else DeribitPublicClient(base_url=config.base_url, timeout_s=config.timeout_s)
    )
    instruments: list[dict[str, Any]] = []
    snapshots: list[dict[str, Any]] = []
    errors: list[str] = []
    tickers_used = 0
    order_books_used = 0
    kinds = []
    requested_types = {str(item).lower() for item in config.instrument_types}
    if requested_types & {"future", "futures", "perpetual", "perpetuals"}:
        kinds.append("future")
    if requested_types & {"option", "options"}:
        kinds.append("option")

    for asset in config.enabled_assets:
        for kind in kinds:
            try:
                raw_instruments = api.get_instruments(currency=asset, kind=kind)
            except Exception as exc:
                errors.append(f"{asset}:{kind}:instruments:{exc}")
                continue
            normalized_instruments = [
                row
                for payload in raw_instruments
                if (row := normalize_deribit_instrument(payload, now_ms=observed)) is not None
                and _instrument_type_allowed(row, config)
                and _expiry_allowed(row, config, now_ms=observed)
            ]
            instruments.extend(normalized_instruments)
            instrument_map = {str(row.get("instrument_name")): row for row in normalized_instruments}
            try:
                summaries = api.get_book_summary_by_currency(currency=asset, kind=kind)
            except Exception as exc:
                errors.append(f"{asset}:{kind}:summary:{exc}")
                continue
            for summary in summaries[: int(config.max_instruments)]:
                name = str(summary.get("instrument_name") or "").upper().strip()
                inst = instrument_map.get(name)
                if not inst:
                    continue
                ticker: dict[str, Any] = {}
                if bool(config.include_ticker) and tickers_used < int(config.max_tickers):
                    try:
                        ticker = api.ticker(instrument_name=name)
                        tickers_used += 1
                    except Exception as exc:
                        errors.append(f"{name}:ticker:{exc}")
                order_book: dict[str, Any] = {}
                if bool(config.include_order_book) and order_books_used < int(config.max_order_books):
                    try:
                        order_book = api.get_order_book(
                            instrument_name=name,
                            depth=int(config.order_book_depth),
                        )
                        order_books_used += 1
                    except Exception as exc:
                        errors.append(f"{name}:order_book:{exc}")
                row = normalize_deribit_snapshot(
                    summary,
                    instrument=inst,
                    ticker=ticker,
                    order_book=order_book,
                    now_ms=observed,
                )
                if row and _liquidity_ok(row, config):
                    snapshots.append(row)

    readiness = build_deribit_provider_readiness(
        instruments,
        snapshots,
        settings=config,
        now_ms=observed,
        errors=errors,
        ws_reconnect_state=(
            str(getattr(api, "last_reconnect_state", "") or "unknown")
            if mode in DERIBIT_WEBSOCKET_MODES
            else "not_used_http_mode"
        ),
    )
    return {
        "instruments": instruments,
        "snapshots": snapshots,
        "readiness": readiness,
        "errors": errors,
    }


def ensure_deribit_schema(con) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS deribit_instruments (
          instrument_name TEXT PRIMARY KEY,
          base_asset TEXT,
          quote_currency TEXT,
          instrument_type TEXT,
          kind TEXT,
          settlement_period TEXT,
          expiry_ts_ms BIGINT,
          strike DOUBLE PRECISION,
          option_type TEXT,
          is_active BOOLEAN,
          source_ts_ms BIGINT,
          availability_ts_ms BIGINT,
          raw_json TEXT
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS deribit_market_snapshots (
          source_record_id TEXT PRIMARY KEY,
          instrument_name TEXT,
          base_asset TEXT,
          quote_currency TEXT,
          instrument_type TEXT,
          kind TEXT,
          expiry_ts_ms BIGINT,
          strike DOUBLE PRECISION,
          option_type TEXT,
          mark_price DOUBLE PRECISION,
          index_price DOUBLE PRECISION,
          bid_price DOUBLE PRECISION,
          ask_price DOUBLE PRECISION,
          mid_price DOUBLE PRECISION,
          last_price DOUBLE PRECISION,
          underlying_price DOUBLE PRECISION,
          bid_iv DOUBLE PRECISION,
          ask_iv DOUBLE PRECISION,
          mark_iv DOUBLE PRECISION,
          delta DOUBLE PRECISION,
          gamma DOUBLE PRECISION,
          theta DOUBLE PRECISION,
          vega DOUBLE PRECISION,
          open_interest DOUBLE PRECISION,
          volume DOUBLE PRECISION,
          volume_usd DOUBLE PRECISION,
          current_funding DOUBLE PRECISION,
          funding_8h DOUBLE PRECISION,
          futures_basis DOUBLE PRECISION,
          perp_basis DOUBLE PRECISION,
          best_bid_amount DOUBLE PRECISION,
          best_ask_amount DOUBLE PRECISION,
          spread_bps DOUBLE PRECISION,
          source_ts_ms BIGINT,
          availability_ts_ms BIGINT,
          ingested_ts_ms BIGINT,
          raw_json TEXT,
          diagnostics_json TEXT
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS deribit_provider_state (
          source_key TEXT PRIMARY KEY,
          ts_ms BIGINT,
          readiness_json TEXT
        )
        """
    )
    for statement in (
        "CREATE INDEX IF NOT EXISTS idx_deribit_snapshots_asset_avail ON deribit_market_snapshots(base_asset, availability_ts_ms DESC)",
        "CREATE INDEX IF NOT EXISTS idx_deribit_snapshots_instrument_avail ON deribit_market_snapshots(instrument_name, availability_ts_ms DESC)",
        "CREATE INDEX IF NOT EXISTS idx_deribit_instruments_asset_type ON deribit_instruments(base_asset, instrument_type, expiry_ts_ms)",
    ):
        con.execute(statement)


def _db_value(value: Any) -> Any:
    if isinstance(value, (dict, list, tuple)):
        return _json_dumps(value)
    return value


def put_deribit_batch(
    con,
    *,
    instruments: Sequence[Mapping[str, Any]] = (),
    snapshots: Sequence[Mapping[str, Any]] = (),
    readiness: Mapping[str, Any] | None = None,
    source_key: str = "deribit_crypto_derivatives",
    now_ms: int | None = None,
) -> dict[str, int]:
    ensure_deribit_schema(con)
    instrument_cols = (
        "instrument_name",
        "base_asset",
        "quote_currency",
        "instrument_type",
        "kind",
        "settlement_period",
        "expiry_ts_ms",
        "strike",
        "option_type",
        "is_active",
        "source_ts_ms",
        "availability_ts_ms",
        "raw_json",
    )
    snapshot_cols = (
        "source_record_id",
        "instrument_name",
        "base_asset",
        "quote_currency",
        "instrument_type",
        "kind",
        "expiry_ts_ms",
        "strike",
        "option_type",
        "mark_price",
        "index_price",
        "bid_price",
        "ask_price",
        "mid_price",
        "last_price",
        "underlying_price",
        "bid_iv",
        "ask_iv",
        "mark_iv",
        "delta",
        "gamma",
        "theta",
        "vega",
        "open_interest",
        "volume",
        "volume_usd",
        "current_funding",
        "funding_8h",
        "futures_basis",
        "perp_basis",
        "best_bid_amount",
        "best_ask_amount",
        "spread_bps",
        "source_ts_ms",
        "availability_ts_ms",
        "ingested_ts_ms",
        "raw_json",
        "diagnostics_json",
    )
    instrument_count = 0
    for row in instruments or []:
        values = [_db_value(dict(row).get(col)) for col in instrument_cols]
        con.execute(
            f"""
            INSERT INTO deribit_instruments({','.join(instrument_cols)})
            VALUES ({','.join(['?'] * len(instrument_cols))})
            ON CONFLICT(instrument_name) DO UPDATE SET
              base_asset=excluded.base_asset,
              quote_currency=excluded.quote_currency,
              instrument_type=excluded.instrument_type,
              kind=excluded.kind,
              settlement_period=excluded.settlement_period,
              expiry_ts_ms=excluded.expiry_ts_ms,
              strike=excluded.strike,
              option_type=excluded.option_type,
              is_active=excluded.is_active,
              source_ts_ms=excluded.source_ts_ms,
              availability_ts_ms=excluded.availability_ts_ms,
              raw_json=excluded.raw_json
            """,
            tuple(values),
        )
        instrument_count += 1
    snapshot_count = 0
    for row in snapshots or []:
        values = [_db_value(dict(row).get(col)) for col in snapshot_cols]
        con.execute(
            f"""
            INSERT INTO deribit_market_snapshots({','.join(snapshot_cols)})
            VALUES ({','.join(['?'] * len(snapshot_cols))})
            ON CONFLICT(source_record_id) DO UPDATE SET
              instrument_name=excluded.instrument_name,
              base_asset=excluded.base_asset,
              quote_currency=excluded.quote_currency,
              instrument_type=excluded.instrument_type,
              kind=excluded.kind,
              expiry_ts_ms=excluded.expiry_ts_ms,
              strike=excluded.strike,
              option_type=excluded.option_type,
              mark_price=excluded.mark_price,
              index_price=excluded.index_price,
              bid_price=excluded.bid_price,
              ask_price=excluded.ask_price,
              mid_price=excluded.mid_price,
              last_price=excluded.last_price,
              underlying_price=excluded.underlying_price,
              bid_iv=excluded.bid_iv,
              ask_iv=excluded.ask_iv,
              mark_iv=excluded.mark_iv,
              delta=excluded.delta,
              gamma=excluded.gamma,
              theta=excluded.theta,
              vega=excluded.vega,
              open_interest=excluded.open_interest,
              volume=excluded.volume,
              volume_usd=excluded.volume_usd,
              current_funding=excluded.current_funding,
              funding_8h=excluded.funding_8h,
              futures_basis=excluded.futures_basis,
              perp_basis=excluded.perp_basis,
              best_bid_amount=excluded.best_bid_amount,
              best_ask_amount=excluded.best_ask_amount,
              spread_bps=excluded.spread_bps,
              source_ts_ms=excluded.source_ts_ms,
              availability_ts_ms=excluded.availability_ts_ms,
              ingested_ts_ms=excluded.ingested_ts_ms,
              raw_json=excluded.raw_json,
              diagnostics_json=excluded.diagnostics_json
            """,
            tuple(values),
        )
        snapshot_count += 1
    if readiness is not None:
        con.execute(
            """
            INSERT INTO deribit_provider_state(source_key, ts_ms, readiness_json)
            VALUES(?,?,?)
            ON CONFLICT(source_key) DO UPDATE SET
              ts_ms=excluded.ts_ms,
              readiness_json=excluded.readiness_json
            """,
            (
                str(source_key),
                int(now_ms or utc_ms()),
                _json_dumps(dict(readiness or {})),
            ),
        )
    return {"instruments": int(instrument_count), "snapshots": int(snapshot_count), "provider_state": 1 if readiness is not None else 0}


def deribit_snapshots_to_crypto_funding_rows(snapshots: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for snapshot in snapshots or []:
        snap = dict(snapshot or {})
        if str(snap.get("instrument_type") or "").lower() != "perpetual":
            continue
        rate = _first_number(snap.get("funding_8h"), snap.get("current_funding"))
        if rate is None:
            continue
        source_ts = _safe_int(snap.get("source_ts_ms") or snap.get("availability_ts_ms"), utc_ms())
        base = _clean_symbol(snap.get("base_asset"))
        quote = _clean_symbol(snap.get("quote_currency") or "USD")
        rows.append(
            {
                "ts_ms": int(source_ts),
                "symbol": base,
                "exchange": "deribit",
                "perp_market": str(snap.get("instrument_name") or ""),
                "spot_market": f"{base}/{quote}" if base and quote else base,
                "funding_ts_ms": int(source_ts),
                "availability_ts_ms": _safe_int(snap.get("availability_ts_ms"), source_ts),
                "funding_rate": float(rate),
                "mark_price": snap.get("mark_price"),
                "index_price": snap.get("index_price"),
                "spot_price": snap.get("index_price"),
                "spot_ts_ms": snap.get("source_ts_ms"),
                "perp_ts_ms": snap.get("source_ts_ms"),
                "perp_basis_pct": snap.get("perp_basis"),
                "source_record_id": _hash_id("crypto_funding", snap.get("source_record_id")),
                "ingested_ts_ms": snap.get("ingested_ts_ms") or utc_ms(),
                "is_live": True,
                "payload_json": snap.get("raw_json") if isinstance(snap.get("raw_json"), Mapping) else snap,
                "diagnostics_json": {
                    "source": "deribit_public_market_data",
                    "funding_field": "funding_8h" if snap.get("funding_8h") is not None else "current_funding",
                    "direct_trading_authority": False,
                },
            }
        )
    return rows


def build_deribit_provider_readiness(
    instruments: Sequence[Mapping[str, Any]],
    snapshots: Sequence[Mapping[str, Any]],
    *,
    settings: Mapping[str, Any] | DeribitSettings | None = None,
    now_ms: int | None = None,
    errors: Sequence[str] = (),
    ws_reconnect_state: str = "not_used_http_mode",
) -> dict[str, Any]:
    config = settings if isinstance(settings, DeribitSettings) else load_deribit_settings(settings)
    observed = int(now_ms or utc_ms())
    active = [dict(item) for item in instruments or [] if bool(dict(item).get("is_active", True))]
    snapshot_rows = [dict(row) for row in snapshots or []]
    latest_availability = max([_safe_int(row.get("availability_ts_ms"), 0) for row in snapshot_rows] or [0])
    latest_age_ms = int(observed - latest_availability) if latest_availability > 0 else None
    stale_instruments = [
        str(row.get("instrument_name") or "")
        for row in snapshot_rows
        if latest_availability <= 0
        or (observed - _safe_int(row.get("availability_ts_ms"), 0)) > int(config.stale_threshold_ms)
    ]
    snap_names = {str(row.get("instrument_name") or "") for row in snapshot_rows}
    no_snapshot = [str(row.get("instrument_name") or "") for row in active if str(row.get("instrument_name") or "") not in snap_names]
    option_rows = [row for row in snapshot_rows if str(row.get("instrument_type") or "").lower() == "option"]
    missing_iv = [
        str(row.get("instrument_name") or "")
        for row in option_rows
        if row.get("mark_iv") is None and row.get("bid_iv") is None and row.get("ask_iv") is None
    ]
    spreads = [
        float(row.get("spread_bps"))
        for row in snapshot_rows
        if _safe_float(row.get("spread_bps")) is not None
    ]
    wide = [value for value in spreads if value > float(config.max_spread_bps)]
    ok = bool(snapshot_rows) and not errors and (latest_age_ms is not None and latest_age_ms <= int(config.stale_threshold_ms))
    return {
        "ok": bool(ok),
        "provider_name": "deribit",
        "source_key": "deribit_crypto_derivatives",
        "stage": "shadow",
        "direct_trading_authority": False,
        "public_market_data_only": True,
        "mode": str(config.mode),
        "enabled_assets": list(config.enabled_assets),
        "instrument_types": list(config.instrument_types),
        "active_instruments": int(len(active)),
        "snapshot_instruments": int(len(snapshot_rows)),
        "stale_instruments": int(len(stale_instruments) + len(no_snapshot)),
        "stale_instrument_names": [name for name in stale_instruments[:20] if name],
        "instruments_without_snapshot": [name for name in no_snapshot[:20] if name],
        "missing_iv_fields": int(len(missing_iv)),
        "missing_iv_instruments": [name for name in missing_iv[:20] if name],
        "iv_signal_available": bool(option_rows and len(missing_iv) < len(option_rows)),
        "order_book_spread_quality": {
            "samples": int(len(spreads)),
            "wide_spread_count": int(len(wide)),
            "max_spread_bps": float(max(spreads) if spreads else 0.0),
            "threshold_bps": float(config.max_spread_bps),
            "ok": bool(not wide) if spreads else False,
        },
        "websocket_reconnect_state": str(ws_reconnect_state),
        "latest_snapshot_age_ms": latest_age_ms,
        "latest_availability_ts_ms": int(latest_availability) if latest_availability > 0 else None,
        "stale_threshold_ms": int(config.stale_threshold_ms),
        "errors": list(errors or [])[:20],
    }


def _row_dict(cursor, row: Any) -> dict[str, Any]:
    names = [str(desc[0]) for desc in cursor.description or []]
    return {names[idx]: row[idx] for idx in range(min(len(names), len(row)))}


def _clip(value: float, lo: float, hi: float) -> float:
    return float(max(float(lo), min(float(hi), float(value))))


def _rank(current: float, history: Sequence[float]) -> float:
    values = [float(item) for item in history if math.isfinite(float(item))]
    if len(values) < 2:
        return 0.0
    low = min(values)
    high = max(values)
    if high - low <= 1e-12:
        return 0.0
    return _clip((float(current) - low) / (high - low), 0.0, 1.0)


def _latest_by_instrument(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for row in rows or []:
        item = dict(row or {})
        name = str(item.get("instrument_name") or "")
        if not name:
            continue
        prev = latest.get(name)
        if prev is None or _safe_int(item.get("availability_ts_ms"), 0) >= _safe_int(prev.get("availability_ts_ms"), 0):
            latest[name] = item
    return list(latest.values())


def _weighted_average(values: Sequence[tuple[float, float]]) -> float:
    clean = [(float(value), max(0.0, float(weight))) for value, weight in values if math.isfinite(float(value))]
    if not clean:
        return 0.0
    weight_sum = sum(weight for _value, weight in clean)
    if weight_sum <= 0.0:
        return float(sum(value for value, _weight in clean) / len(clean))
    return float(sum(value * weight for value, weight in clean) / weight_sum)


def _option_iv(row: Mapping[str, Any]) -> float | None:
    return _normalize_iv(_first_number(row.get("mark_iv"), row.get("mid_iv"), row.get("bid_iv"), row.get("ask_iv")))


def _expiry_iv_groups(option_rows: Sequence[Mapping[str, Any]], *, asof_ts_ms: int) -> list[tuple[int, float, list[dict[str, Any]]]]:
    grouped: dict[int, list[dict[str, Any]]] = {}
    for row in option_rows or []:
        expiry = _safe_int(row.get("expiry_ts_ms"), 0)
        iv = _option_iv(row)
        if expiry <= int(asof_ts_ms) or iv is None:
            continue
        grouped.setdefault(int(expiry), []).append(dict(row))
    out: list[tuple[int, float, list[dict[str, Any]]]] = []
    for expiry, rows in sorted(grouped.items()):
        weighted = []
        for row in rows:
            iv = _option_iv(row)
            if iv is None:
                continue
            weight = max(1.0, _safe_float(row.get("open_interest"), 0.0) or 0.0, _safe_float(row.get("volume"), 0.0) or 0.0)
            weighted.append((float(iv), float(weight)))
        if weighted:
            out.append((int(expiry), _weighted_average(weighted), rows))
    return out


def _skew_proxy(rows: Sequence[Mapping[str, Any]]) -> tuple[float, bool]:
    calls = []
    puts = []
    for row in rows or []:
        iv = _option_iv(row)
        if iv is None:
            continue
        option_type = str(row.get("option_type") or "").lower()
        delta = _safe_float(row.get("delta"))
        strike = _safe_float(row.get("strike"))
        underlying = _first_number(row.get("underlying_price"), row.get("index_price"), row.get("mark_price"))
        if delta is not None and abs(delta) > 0.0:
            distance = abs(abs(float(delta)) - 0.25)
        elif strike is not None and underlying is not None and underlying > 0.0:
            if option_type == "call" and strike < underlying:
                continue
            if option_type == "put" and strike > underlying:
                continue
            distance = abs(math.log(max(strike, 1e-9) / max(underlying, 1e-9)))
        else:
            distance = 1e9
        if option_type == "call":
            calls.append((distance, float(iv)))
        elif option_type == "put":
            puts.append((distance, float(iv)))
    if not calls or not puts:
        return 0.0, False
    call_iv = sorted(calls, key=lambda item: item[0])[0][1]
    put_iv = sorted(puts, key=lambda item: item[0])[0][1]
    return float(put_iv - call_iv), True


def _volume_shock(rows: Sequence[Mapping[str, Any]]) -> float:
    by_ts: dict[int, float] = {}
    for row in rows or []:
        ts = _safe_int(row.get("source_ts_ms") or row.get("availability_ts_ms"), 0)
        volume = _safe_float(row.get("volume"), 0.0) or 0.0
        if ts <= 0 or volume <= 0.0:
            continue
        bucket = int(ts // 3_600_000)
        by_ts[bucket] = by_ts.get(bucket, 0.0) + float(volume)
    if len(by_ts) < 2:
        return 0.0
    keys = sorted(by_ts)
    latest = by_ts[keys[-1]]
    hist = [by_ts[key] for key in keys[:-1]]
    avg = sum(hist) / max(1, len(hist))
    if avg <= 0.0:
        return 0.0
    return _clip((latest / avg) - 1.0, -10.0, 10.0)


def compute_deribit_crypto_derivative_features(
    rows: Sequence[Mapping[str, Any]],
    *,
    asof_ts_ms: int,
) -> tuple[dict[str, float], dict[str, Any], bool]:
    features = {fid: 0.0 for fid in DERIBIT_FEATURE_IDS}
    eligible = [
        dict(row or {})
        for row in rows or []
        if _safe_int((row or {}).get("availability_ts_ms"), 0) <= int(asof_ts_ms)
    ]
    if not eligible:
        return features, {"latest_source_ts_ms": None, "latest_availability_ts_ms": None}, False
    latest_rows = _latest_by_instrument(eligible)
    option_rows = [row for row in latest_rows if str(row.get("instrument_type") or "").lower() == "option"]
    expiry_groups = _expiry_iv_groups(option_rows, asof_ts_ms=int(asof_ts_ms))
    short_iv = expiry_groups[0][1] if expiry_groups else 0.0
    iv_history = [
        float(iv)
        for row in eligible
        if (iv := _option_iv(row)) is not None
    ]
    features[f"{DERIBIT_FEATURE_PREFIX}short_dated_iv"] = float(short_iv)
    features[f"{DERIBIT_FEATURE_PREFIX}iv_rank"] = _rank(float(short_iv), iv_history)
    if len(expiry_groups) >= 2:
        features[f"{DERIBIT_FEATURE_PREFIX}term_structure_slope"] = float(expiry_groups[1][1] - expiry_groups[0][1])
    skew_available = False
    if expiry_groups:
        skew, skew_available = _skew_proxy(expiry_groups[0][2])
        features[f"{DERIBIT_FEATURE_PREFIX}skew_25d_proxy"] = float(skew)

    call_oi = 0.0
    put_oi = 0.0
    for row in option_rows:
        oi = _safe_float(row.get("open_interest"), 0.0) or 0.0
        if str(row.get("option_type") or "").lower() == "call":
            call_oi += float(oi)
        elif str(row.get("option_type") or "").lower() == "put":
            put_oi += float(oi)
    if call_oi > 0.0 or put_oi > 0.0:
        features[f"{DERIBIT_FEATURE_PREFIX}put_call_open_interest_ratio"] = float((put_oi + 1.0) / (call_oi + 1.0))

    future_rows = [
        row
        for row in latest_rows
        if str(row.get("instrument_type") or "").lower() == "future"
        and _safe_float(row.get("futures_basis")) is not None
    ]
    if future_rows:
        nearest_future = sorted(future_rows, key=lambda row: _safe_int(row.get("expiry_ts_ms"), 9_999_999_999_999))[0]
        features[f"{DERIBIT_FEATURE_PREFIX}futures_basis"] = float(_safe_float(nearest_future.get("futures_basis"), 0.0) or 0.0)

    perp_rows = [
        row
        for row in latest_rows
        if str(row.get("instrument_type") or "").lower() == "perpetual"
    ]
    if perp_rows:
        latest_perp = sorted(perp_rows, key=lambda row: _safe_int(row.get("availability_ts_ms"), 0))[-1]
        features[f"{DERIBIT_FEATURE_PREFIX}perp_basis"] = float(_safe_float(latest_perp.get("perp_basis"), 0.0) or 0.0)
        features[f"{DERIBIT_FEATURE_PREFIX}funding_pressure"] = float(
            _safe_float(latest_perp.get("funding_8h"), _safe_float(latest_perp.get("current_funding"), 0.0)) or 0.0
        )

    features[f"{DERIBIT_FEATURE_PREFIX}volume_shock"] = _volume_shock(eligible)
    iv_rank = float(features[f"{DERIBIT_FEATURE_PREFIX}iv_rank"])
    features[f"{DERIBIT_FEATURE_PREFIX}vol_regime_high"] = 1.0 if short_iv > 0.0 and iv_rank >= 0.80 else 0.0
    features[f"{DERIBIT_FEATURE_PREFIX}vol_regime_low"] = 1.0 if short_iv > 0.0 and iv_rank <= 0.20 else 0.0
    derivative_signal_available = bool(short_iv > 0.0 or future_rows or perp_rows)
    features[f"{DERIBIT_FEATURE_PREFIX}available"] = 1.0 if derivative_signal_available else 0.0
    latest_source = max([_safe_int(row.get("source_ts_ms"), 0) for row in eligible] or [0])
    latest_availability = max([_safe_int(row.get("availability_ts_ms"), 0) for row in eligible] or [0])
    meta = {
        "latest_source_ts_ms": int(latest_source) if latest_source > 0 else None,
        "latest_availability_ts_ms": int(latest_availability) if latest_availability > 0 else None,
        "rows": int(len(eligible)),
        "latest_instruments": int(len(latest_rows)),
        "option_instruments": int(len(option_rows)),
        "iv_signal_available": bool(short_iv > 0.0),
        "skew_available": bool(skew_available),
        "futures_basis_available": bool(future_rows),
        "perp_basis_available": bool(perp_rows),
        "stage": "shadow",
        "direct_trading_authority": False,
        "provider": "deribit",
    }
    return features, meta, derivative_signal_available


def deribit_base_assets_for_symbol(symbol: str, *, include_crypto_equity_mappings: bool = False) -> list[str]:
    symbol_key = _clean_symbol(symbol)
    if not symbol_key:
        return []
    if include_crypto_equity_mappings and symbol_key in DERIBIT_CRYPTO_EQUITY_MAP:
        return list(DERIBIT_CRYPTO_EQUITY_MAP[symbol_key])
    if "-" in symbol_key or "/" in symbol_key:
        head = re.split(r"[-/]", symbol_key, maxsplit=1)[0]
        if head:
            symbol_key = head
    try:
        from engine.data.asset_map import asset_class_for_symbol

        asset_class = str(asset_class_for_symbol(symbol_key) or "").upper().strip()
    except Exception:
        asset_class = "UNKNOWN"
    if asset_class != "CRYPTO":
        return []
    return [symbol_key]


def resolve_deribit_crypto_derivatives_snapshot(
    con,
    *,
    symbol: str,
    ts_ms: int,
    include_crypto_equity_mappings: bool = False,
) -> tuple[dict[str, float], dict[str, Any], bool]:
    features = {fid: 0.0 for fid in DERIBIT_FEATURE_IDS}
    source_meta: dict[str, Any] = {
        "latest_source_ts_ms": None,
        "latest_availability_ts_ms": None,
        "stage": "shadow",
        "direct_trading_authority": False,
        "provider": "deribit",
    }
    bases = deribit_base_assets_for_symbol(symbol, include_crypto_equity_mappings=include_crypto_equity_mappings)
    if not bases:
        return features, source_meta, False
    window_start = int(ts_ms) - int(30 * 24 * 3_600_000)
    try:
        cursor = con.execute(
            """
            SELECT
              instrument_name,
              base_asset,
              quote_currency,
              instrument_type,
              kind,
              expiry_ts_ms,
              strike,
              option_type,
              mark_price,
              index_price,
              bid_price,
              ask_price,
              mid_price,
              last_price,
              underlying_price,
              bid_iv,
              ask_iv,
              mark_iv,
              delta,
              gamma,
              theta,
              vega,
              open_interest,
              volume,
              volume_usd,
              current_funding,
              funding_8h,
              futures_basis,
              perp_basis,
              best_bid_amount,
              best_ask_amount,
              spread_bps,
              source_ts_ms,
              availability_ts_ms,
              ingested_ts_ms,
              diagnostics_json
            FROM deribit_market_snapshots
            WHERE base_asset IN ({})
              AND availability_ts_ms <= ?
              AND availability_ts_ms >= ?
            ORDER BY availability_ts_ms ASC, source_ts_ms ASC
            """.format(",".join(["?"] * len(bases))),
            tuple(list(bases) + [int(ts_ms), int(window_start)]),
        )
        rows = [_row_dict(cursor, row) for row in cursor.fetchall() or []]
    except Exception:
        return features, source_meta, False
    computed, meta, available = compute_deribit_crypto_derivative_features(rows, asof_ts_ms=int(ts_ms))
    for fid in DERIBIT_FEATURE_IDS:
        features[fid] = float(_safe_float(computed.get(fid), 0.0) or 0.0)
    source_meta.update(meta)
    source_meta["base_assets"] = list(bases)
    return features, source_meta, bool(available)


__all__ = [
    "DERIBIT_FEATURE_GROUP",
    "DERIBIT_FEATURE_IDS",
    "DERIBIT_FEATURE_PREFIX",
    "DERIBIT_FORBIDDEN_SETTING_KEYS",
    "DeribitPublicClient",
    "DeribitPublicWebSocketClient",
    "DeribitSettings",
    "build_deribit_provider_readiness",
    "compute_deribit_crypto_derivative_features",
    "deribit_base_assets_for_symbol",
    "deribit_snapshots_to_crypto_funding_rows",
    "ensure_deribit_schema",
    "fetch_deribit_public_batch",
    "load_deribit_settings",
    "normalize_deribit_instrument",
    "normalize_deribit_snapshot",
    "parse_deribit_instrument_name",
    "put_deribit_batch",
    "resolve_deribit_crypto_derivatives_snapshot",
    "validate_deribit_settings",
]
