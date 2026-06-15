"""Crypto perpetual funding and basis helpers.

README:
- Source: existing CCXT exchange plumbing for perpetual funding rates and
  spot/perp ticker snapshots.
- Cadence: hourly polling is enough because most venues settle funding every
  eight hours; the job may also backfill recent funding history.
- Availability lag: a funding event is available at its exchange funding
  timestamp, and point-in-time features only use events with
  ``availability_ts_ms <= ts_ms``.
- Caveats: exchange endpoint coverage varies. Missing funding endpoints are
  skipped gracefully, and basis is stored only when perp and spot prices are
  observed together.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Sequence, Tuple

try:
    import ccxt  # type: ignore

    _CCXT_IMPORT_ERROR = None
except Exception as _ccxt_import_error:  # pragma: no cover - import environment dependent
    ccxt = None  # type: ignore
    _CCXT_IMPORT_ERROR = _ccxt_import_error


@dataclass(frozen=True)
class CryptoPerpMarket:
    symbol: str
    exchange_id: str
    perp_market: str
    spot_market: str


CRYPTO_FUNDING_FEATURE_IDS = [
    "funding_rate_now",
    "funding_z_30d",
    "funding_extreme_flag",
    "funding_cum_3d",
    "perp_basis_pct",
    "basis_z_30d",
]


def utc_now_ms() -> int:
    return int(time.time() * 1000)


def _clean_symbol(value: Any) -> str:
    return str(value or "").upper().strip()


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except Exception:
        return None
    if not math.isfinite(out):
        return None
    return float(out)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return int(default)


def _json_obj(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            loaded = json.loads(value)
            return dict(loaded) if isinstance(loaded, dict) else {}
        except Exception:
            return {}
    return {}


def _source_record_id(*parts: Any) -> str:
    raw = "|".join(str(part or "") for part in parts)
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]
    return f"crypto_funding:{digest}"


def derive_perp_market(spot_market: str, exchange_id: str = "binance") -> str:
    market = str(spot_market or "").strip()
    if not market:
        return ""
    if ":" in market:
        return market
    exchange = str(exchange_id or "").strip().lower()
    if exchange in {"binance", "bybit", "okx"} and "/" in market:
        base, quote = market.split("/", 1)
        settle = "USDT" if quote.upper().startswith("USDT") else quote.split(":", 1)[0].upper()
        return f"{base.upper()}/{quote.upper()}:{settle}"
    return market


def _spot_from_perp(perp_market: str) -> str:
    market = str(perp_market or "").strip()
    if ":" in market:
        return market.split(":", 1)[0]
    return market


def parse_env_market_map(raw: str | None = None) -> List[CryptoPerpMarket]:
    text = str(raw if raw is not None else os.environ.get("CRYPTO_PERP_MARKETS", "") or "").strip()
    if not text:
        return []
    default_exchange = str(os.environ.get("CCXT_FUNDING_EXCHANGE_ID", os.environ.get("CCXT_EXCHANGE_ID", "binance"))).strip() or "binance"
    out: List[CryptoPerpMarket] = []
    for item in [part.strip() for part in text.split(",") if part.strip()]:
        parts = [part.strip() for part in item.split("|")]
        if len(parts) == 1:
            symbol = _clean_symbol(parts[0].split("/", 1)[0])
            perp = derive_perp_market(parts[0], default_exchange)
            spot = _spot_from_perp(perp)
            exchange = default_exchange
        elif len(parts) == 2:
            symbol = _clean_symbol(parts[0])
            perp = derive_perp_market(parts[1], default_exchange)
            spot = _spot_from_perp(perp)
            exchange = default_exchange
        elif len(parts) == 3:
            symbol = _clean_symbol(parts[0])
            perp = derive_perp_market(parts[1], default_exchange)
            spot = parts[2]
            exchange = default_exchange
        else:
            symbol = _clean_symbol(parts[0])
            exchange = parts[1] or default_exchange
            perp = derive_perp_market(parts[2], exchange)
            spot = parts[3] or _spot_from_perp(perp)
        if symbol and perp and spot:
            out.append(CryptoPerpMarket(symbol=symbol, exchange_id=str(exchange), perp_market=perp, spot_market=str(spot)))
    return out


def load_crypto_perp_markets(con=None) -> List[CryptoPerpMarket]:
    env_rows = parse_env_market_map()
    if env_rows:
        return env_rows

    default_exchange = str(os.environ.get("CCXT_FUNDING_EXCHANGE_ID", os.environ.get("CCXT_EXCHANGE_ID", "binance"))).strip() or "binance"
    owns = False
    if con is None:
        try:
            from engine.runtime.storage import connect

            con = connect(readonly=True)
            owns = True
        except Exception:
            con = None
    rows = []
    if con is not None:
        try:
            rows = con.execute(
                """
                SELECT symbol, asset_class, meta_json
                FROM symbols
                WHERE status IN ('ACTIVE','WATCH')
                ORDER BY updated_ts_ms DESC, created_ts_ms DESC, symbol
                LIMIT 500
                """
            ).fetchall()
        except Exception:
            rows = []
        finally:
            if owns:
                try:
                    con.close()
                # system-audit: ignore[silent_except] connection close is best-effort cleanup.
                except Exception:
                    pass

    out: List[CryptoPerpMarket] = []
    for row in rows or []:
        symbol = _clean_symbol(row[0] if not hasattr(row, "keys") else row["symbol"])
        asset_class = str(row[1] if not hasattr(row, "keys") else row["asset_class"] or "").upper().strip()
        meta = _json_obj(row[2] if not hasattr(row, "keys") else row["meta_json"])
        provider = str(meta.get("price_provider") or "").lower().strip()
        if asset_class != "CRYPTO" and provider != "ccxt":
            continue
        exchange = str(meta.get("ccxt_exchange") or default_exchange).strip() or default_exchange
        spot = str(meta.get("ccxt_spot_market") or meta.get("ccxt_market") or "").strip()
        perp = str(meta.get("ccxt_perp_market") or "").strip()
        if not perp and spot:
            perp = derive_perp_market(spot, exchange)
        if not spot and perp:
            spot = _spot_from_perp(perp)
        if symbol and perp and spot:
            out.append(CryptoPerpMarket(symbol=symbol, exchange_id=exchange, perp_market=perp, spot_market=spot))

    seen: set[tuple[str, str, str]] = set()
    deduped: List[CryptoPerpMarket] = []
    for item in out:
        key = (item.exchange_id.lower(), item.symbol, item.perp_market)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def build_ccxt_exchange(exchange_id: str):
    if ccxt is None:
        raise RuntimeError(f"ccxt_unavailable:{type(_CCXT_IMPORT_ERROR).__name__ if _CCXT_IMPORT_ERROR else 'unknown'}")
    ex_class = getattr(ccxt, str(exchange_id or "").strip(), None)
    if ex_class is None:
        raise RuntimeError(f"ccxt_exchange_not_found:{exchange_id}")
    return ex_class({"enableRateLimit": True, "options": {"defaultType": "swap"}})


def _has_endpoint(exchange: Any, endpoint: str) -> bool:
    has = getattr(exchange, "has", None)
    if isinstance(has, dict):
        value = has.get(endpoint) or has.get(endpoint[0].lower() + endpoint[1:])
        if value is False:
            return False
    snake = endpoint[0].lower() + "".join([f"_{ch.lower()}" if ch.isupper() else ch for ch in endpoint[1:]])
    return callable(getattr(exchange, endpoint, None)) or callable(getattr(exchange, snake, None))


def _call_endpoint(exchange: Any, endpoint: str, *args: Any, **kwargs: Any) -> Any:
    snake = endpoint[0].lower() + "".join([f"_{ch.lower()}" if ch.isupper() else ch for ch in endpoint[1:]])
    fn = getattr(exchange, endpoint, None) or getattr(exchange, snake, None)
    if not callable(fn):
        raise AttributeError(endpoint)
    return fn(*args, **kwargs)


def _ticker_price(ticker: Any) -> tuple[float | None, int | None]:
    data = dict(ticker or {}) if isinstance(ticker, dict) else {}
    for key in ("mark", "last", "close", "bid", "ask"):
        value = _safe_float(data.get(key))
        if value is not None:
            return value, (_safe_int(data.get("timestamp"), 0) or None)
    return None, (_safe_int(data.get("timestamp"), 0) or None)


def basis_pct(perp_price: Any, spot_price: Any) -> float | None:
    perp = _safe_float(perp_price)
    spot = _safe_float(spot_price)
    if perp is None or spot is None or spot <= 0.0:
        return None
    return float((perp - spot) / spot)


def normalize_funding_record(
    *,
    exchange_id: str,
    symbol: str,
    perp_market: str,
    spot_market: str,
    record: Dict[str, Any],
    live: bool = False,
    ingested_ts_ms: int | None = None,
    spot_price: float | None = None,
    spot_ts_ms: int | None = None,
    perp_price: float | None = None,
    perp_ts_ms: int | None = None,
) -> Dict[str, Any] | None:
    payload = dict(record or {})
    rate = _safe_float(payload.get("fundingRate") if payload.get("fundingRate") is not None else payload.get("rate"))
    if rate is None:
        return None
    funding_ts = _safe_int(payload.get("timestamp") or payload.get("fundingTimestamp") or payload.get("fundingTime"), 0)
    if funding_ts <= 0:
        funding_ts = int(ingested_ts_ms or utc_now_ms())
    mark_price = _safe_float(payload.get("markPrice")) or _safe_float(payload.get("mark")) or _safe_float(perp_price)
    index_price = _safe_float(payload.get("indexPrice")) or _safe_float(payload.get("index"))
    basis = basis_pct(mark_price, spot_price)
    row = {
        "ts_ms": int(funding_ts),
        "symbol": _clean_symbol(symbol),
        "exchange": str(exchange_id or "").strip().lower(),
        "perp_market": str(perp_market),
        "spot_market": str(spot_market),
        "funding_ts_ms": int(funding_ts),
        "availability_ts_ms": int(funding_ts),
        "funding_rate": float(rate),
        "mark_price": mark_price,
        "index_price": index_price,
        "spot_price": _safe_float(spot_price),
        "spot_ts_ms": int(spot_ts_ms) if spot_ts_ms else None,
        "perp_ts_ms": int(perp_ts_ms) if perp_ts_ms else None,
        "perp_basis_pct": basis,
        "source_record_id": _source_record_id(exchange_id, symbol, perp_market, funding_ts, rate),
        "ingested_ts_ms": int(ingested_ts_ms or utc_now_ms()),
        "is_live": bool(live),
        "payload_json": payload,
        "diagnostics_json": {"availability_rule": "funding_timestamp", "basis_matched_live_tickers": basis is not None},
    }
    return row


def poll_exchange_funding(
    exchange: Any,
    markets: Sequence[CryptoPerpMarket],
    *,
    since_ms: int | None = None,
    history_limit: int = 32,
    include_live: bool = True,
    now_ms: int | None = None,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    rows: List[Dict[str, Any]] = []
    errors: List[str] = []
    ingested = int(now_ms or utc_now_ms())
    for market in markets or []:
        spot_price = None
        spot_ts_ms = None
        perp_price = None
        perp_ts_ms = None
        try:
            if _has_endpoint(exchange, "fetchTicker"):
                perp_ticker = _call_endpoint(exchange, "fetchTicker", market.perp_market)
                perp_price, perp_ts_ms = _ticker_price(perp_ticker)
                spot_ticker = _call_endpoint(exchange, "fetchTicker", market.spot_market)
                spot_price, spot_ts_ms = _ticker_price(spot_ticker)
        except Exception as exc:
            errors.append(f"{market.symbol}:ticker:{exc}")

        if _has_endpoint(exchange, "fetchFundingRateHistory"):
            try:
                history = _call_endpoint(
                    exchange,
                    "fetchFundingRateHistory",
                    market.perp_market,
                    since_ms,
                    int(history_limit),
                )
                for record in list(history or []):
                    row = normalize_funding_record(
                        exchange_id=market.exchange_id,
                        symbol=market.symbol,
                        perp_market=market.perp_market,
                        spot_market=market.spot_market,
                        record=dict(record or {}),
                        live=False,
                        ingested_ts_ms=ingested,
                    )
                    if row:
                        rows.append(row)
            except Exception as exc:
                errors.append(f"{market.symbol}:history:{exc}")
        else:
            errors.append(f"{market.symbol}:history_endpoint_unavailable")

        if include_live:
            if not _has_endpoint(exchange, "fetchFundingRate"):
                errors.append(f"{market.symbol}:live_endpoint_unavailable")
                continue
            try:
                record = _call_endpoint(exchange, "fetchFundingRate", market.perp_market)
                row = normalize_funding_record(
                    exchange_id=market.exchange_id,
                    symbol=market.symbol,
                    perp_market=market.perp_market,
                    spot_market=market.spot_market,
                    record=dict(record or {}),
                    live=True,
                    ingested_ts_ms=ingested,
                    spot_price=spot_price,
                    spot_ts_ms=spot_ts_ms,
                    perp_price=perp_price,
                    perp_ts_ms=perp_ts_ms,
                )
                if row:
                    rows.append(row)
            except Exception as exc:
                errors.append(f"{market.symbol}:live:{exc}")
    return rows, errors


def trailing_z(values: Sequence[float]) -> float:
    clean = [float(value) for value in values if math.isfinite(float(value))]
    if len(clean) < 3:
        return 0.0
    latest = clean[-1]
    hist = clean[:-1]
    mean = sum(hist) / len(hist)
    if len(hist) < 2:
        return 0.0
    variance = sum((value - mean) ** 2 for value in hist) / max(1, len(hist) - 1)
    std = math.sqrt(max(0.0, variance))
    if std <= 0.0:
        return 0.0
    return float(max(-10.0, min(10.0, (latest - mean) / std)))


def compute_positioning_features(rows: Sequence[Dict[str, Any]], *, asof_ts_ms: int) -> Dict[str, float]:
    ordered = sorted(
        [dict(row) for row in rows if _safe_int(row.get("availability_ts_ms"), 0) <= int(asof_ts_ms)],
        key=lambda row: (_safe_int(row.get("funding_ts_ms"), 0), _safe_int(row.get("availability_ts_ms"), 0)),
    )
    features = {fid: 0.0 for fid in CRYPTO_FUNDING_FEATURE_IDS}
    if not ordered:
        return features
    latest = ordered[-1]
    rates = [float(row.get("funding_rate") or 0.0) for row in ordered if _safe_float(row.get("funding_rate")) is not None]
    z = trailing_z(rates)
    features["funding_rate_now"] = float(latest.get("funding_rate") or 0.0)
    features["funding_z_30d"] = float(z)
    features["funding_extreme_flag"] = 1.0 if abs(float(z)) > 2.0 else 0.0
    cutoff_3d = int(asof_ts_ms) - int(3 * 24 * 3600 * 1000)
    features["funding_cum_3d"] = float(
        sum(
            float(row.get("funding_rate") or 0.0)
            for row in ordered
            if _safe_int(row.get("funding_ts_ms") or row.get("availability_ts_ms"), 0) >= cutoff_3d
        )
    )
    basis_rows = [row for row in ordered if _safe_float(row.get("perp_basis_pct")) is not None]
    if basis_rows:
        basis_values = [float(row.get("perp_basis_pct") or 0.0) for row in basis_rows]
        features["perp_basis_pct"] = float(basis_values[-1])
        features["basis_z_30d"] = float(trailing_z(basis_values))
    return features
