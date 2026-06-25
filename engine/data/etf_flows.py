"""ETF shares-outstanding ingestion and unexpected-flow feature helpers.

README:
- Source: daily ETF shares outstanding from Polygon ticker details
  (``share_class_shares_outstanding``) with FMP profile
  (``sharesOutstanding``) as a fallback when Polygon is unavailable or the
  subscription does not expose the field.
- Cadence: the supervised ingestion job runs once per day by default.
- Availability lag: issuer/vendor shares updates are treated as available the
  next morning at the U.S. cash-market open; feature joins use
  ``availability_ts_ms`` and never the share count's as-of date alone.
- Caveats: raw share counts are split-sensitive. Flow math detects reciprocal
  price/share jumps and treats them as split adjustments, not creations or
  redemptions. NAV/premium features are left as a follow-up because no reliable
  ETF NAV source is already configured in the repo.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timedelta, timezone
from typing import Any, Dict, Iterable, List, Mapping, Sequence, Tuple
from zoneinfo import ZoneInfo

import requests

from engine.data._credentials import get_data_credential
from engine.data.default_symbols import ETF_SEED_SYMBOLS, parse_symbol_limit
from engine.runtime.storage import connect, run_write_txn

ETF_FLOW_FEATURE_IDS = [
    "etf_unexpected_flow_z",
    "etf_flow_3d_sum_z",
    "etf_flow_reversal_flag",
]

POLYGON_TICKER_DETAILS_URL = "https://api.polygon.io/v3/reference/tickers/{symbol}"
FMP_PROFILE_URL = "https://financialmodelingprep.com/api/v3/profile/{symbol}"
REQUEST_TIMEOUT_S = float(os.environ.get("ETF_FLOW_REQUEST_TIMEOUT_S", "20"))
FEATURE_LOOKBACK_READINGS = max(24, int(os.environ.get("ETF_FLOW_FEATURE_LOOKBACK_READINGS", "64") or "64"))
EWMA_ALPHA_20 = 2.0 / 21.0
ETF_FLOW_SUPPRESS_EX_DIVIDEND = os.environ.get("ETF_FLOW_SUPPRESS_EX_DIVIDEND", "1").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

_EASTERN = ZoneInfo("America/New_York")
_UTC = timezone.utc
_ETF_SYMBOLS = {str(symbol).upper().strip() for symbol in ETF_SEED_SYMBOLS}


@dataclass(frozen=True)
class EtfShareReading:
    symbol: str
    asof_date: str
    asof_ts_ms: int
    availability_ts_ms: int
    shares_outstanding: float
    source: str
    price: float | None = None
    nav: float | None = None
    premium_pct: float | None = None
    source_record_id: str | None = None
    payload_json: Dict[str, Any] | None = None
    diagnostics_json: Dict[str, Any] | None = None


def utc_now_ms() -> int:
    return int(time.time() * 1000)


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(str(value).replace(",", "").strip())
    except Exception:
        return None
    if not math.isfinite(out):
        return None
    return float(out)


def _clean_symbol(value: Any) -> str:
    return str(value or "").upper().strip()


def _is_sqlite_connection(con: Any) -> bool:
    return "sqlite" in str(type(con).__module__).lower()


def _json_param(con: Any, value: Any) -> Any:
    if _is_sqlite_connection(con) and isinstance(value, (dict, list)):
        return json.dumps(value, separators=(",", ":"), sort_keys=True, default=str)
    return value


def parse_date(value: Any) -> date:
    text = str(value or "").strip()
    if not text:
        raise ValueError("empty date")
    if len(text) == 8 and text.isdigit():
        return datetime.strptime(text, "%Y%m%d").date()
    return datetime.fromisoformat(text.replace("Z", "+00:00")).date()


def date_to_ms(day: date | str) -> int:
    parsed = parse_date(day) if not isinstance(day, date) else day
    return int(datetime.combine(parsed, dt_time.min, tzinfo=_UTC).timestamp() * 1000)


def asof_date_for_now(now_ms: int | None = None) -> date:
    """Return the share-count date expected to be available by ``now_ms``."""

    now_dt = datetime.fromtimestamp(int(now_ms or utc_now_ms()) / 1000.0, tz=_UTC).astimezone(_EASTERN)
    return now_dt.date() - timedelta(days=1)


def availability_ts_ms_for_date(day: date | str) -> int:
    parsed = parse_date(day) if not isinstance(day, date) else day
    next_morning = datetime.combine(parsed + timedelta(days=1), dt_time(hour=9, minute=30), tzinfo=_EASTERN)
    return int(next_morning.astimezone(_UTC).timestamp() * 1000)


def source_record_id(symbol: str, asof_day: date | str) -> str:
    day = parse_date(asof_day).isoformat() if not isinstance(asof_day, date) else asof_day.isoformat()
    digest = hashlib.sha256(f"etf_so|{_clean_symbol(symbol)}|{day}".encode("utf-8")).hexdigest()[:20]
    return f"etf_so:{digest}"


def is_etf_symbol(symbol: str) -> bool:
    return _clean_symbol(symbol) in _ETF_SYMBOLS


def configured_etf_symbols(con=None, *, limit: int | None = None) -> List[str]:
    symbols: List[str] = list(ETF_SEED_SYMBOLS)
    if con is not None:
        for table in ("symbols", "universe_symbols"):
            try:
                rows = con.execute(f"SELECT symbol FROM {table}").fetchall()
            except Exception:
                rows = []
            for row in rows or []:
                sym = _clean_symbol(row[0] if not hasattr(row, "get") else row.get("symbol"))
                if sym and is_etf_symbol(sym):
                    symbols.append(sym)
    out: List[str] = []
    seen = set()
    for sym in symbols:
        key = _clean_symbol(sym)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
        if limit is not None and len(out) >= int(limit):
            break
    return out


def ensure_etf_flow_tables(con) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS etf_shares_outstanding (
            id BIGSERIAL PRIMARY KEY,
            ts_ms BIGINT,
            symbol TEXT NOT NULL,
            asof_date TEXT NOT NULL,
            asof_ts_ms BIGINT NOT NULL,
            availability_ts_ms BIGINT NOT NULL,
            shares_outstanding DOUBLE PRECISION NOT NULL,
            source TEXT NOT NULL,
            source_record_id TEXT NOT NULL,
            price DOUBLE PRECISION,
            nav DOUBLE PRECISION,
            premium_pct DOUBLE PRECISION,
            ingested_ts_ms BIGINT,
            payload_json JSONB,
            diagnostics_json JSONB
        )
        """
    )
    con.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_etf_shares_outstanding_source_record_id
          ON etf_shares_outstanding(source_record_id)
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_etf_shares_outstanding_symbol_availability
          ON etf_shares_outstanding(symbol, availability_ts_ms DESC)
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS etf_flow_features (
            symbol TEXT NOT NULL,
            asof_ts_ms BIGINT NOT NULL,
            bucket_ts_ms BIGINT NOT NULL,
            etf_unexpected_flow_z DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            etf_flow_3d_sum_z DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            etf_flow_reversal_flag DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            latest_shares_outstanding DOUBLE PRECISION,
            latest_flow_dollars DOUBLE PRECISION,
            latest_unexpected_flow DOUBLE PRECISION,
            latest_aum DOUBLE PRECISION,
            source_max_availability_ts_ms BIGINT,
            created_ts_ms BIGINT,
            meta_json JSONB,
            PRIMARY KEY(symbol, asof_ts_ms)
        )
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_etf_flow_features_symbol_asof
          ON etf_flow_features(symbol, asof_ts_ms DESC)
        """
    )


def fetch_polygon_shares_outstanding(symbol: str) -> Tuple[float | None, Dict[str, Any]]:
    api_key = get_data_credential("POLYGON_API_KEY")
    if not api_key:
        return None, {"error": "missing_polygon_api_key"}
    response = requests.get(
        POLYGON_TICKER_DETAILS_URL.format(symbol=_clean_symbol(symbol)),
        params={"apiKey": api_key},
        timeout=float(REQUEST_TIMEOUT_S),
    )
    response.raise_for_status()
    payload = response.json() or {}
    results = payload.get("results") if isinstance(payload, dict) else {}
    if not isinstance(results, dict):
        results = {}
    shares = _safe_float(results.get("share_class_shares_outstanding"))
    if shares is None:
        shares = _safe_float(results.get("weighted_shares_outstanding"))
    return shares, dict(payload) if isinstance(payload, dict) else {"payload": payload}


def fetch_fmp_profile_shares(symbol: str) -> Tuple[float | None, Dict[str, Any]]:
    api_key = get_data_credential("FMP_API_KEY")
    if not api_key:
        return None, {"error": "missing_fmp_api_key"}
    response = requests.get(
        FMP_PROFILE_URL.format(symbol=_clean_symbol(symbol)),
        params={"apikey": api_key},
        timeout=float(REQUEST_TIMEOUT_S),
    )
    response.raise_for_status()
    payload = response.json()
    row = payload[0] if isinstance(payload, list) and payload else payload
    if not isinstance(row, dict):
        row = {}
    shares = _safe_float(row.get("sharesOutstanding"))
    return shares, {"payload": payload}


def fetch_shares_outstanding(symbol: str, *, provider_order: Sequence[str] | None = None) -> Tuple[float | None, str | None, Dict[str, Any]]:
    errors: List[str] = []
    for provider in provider_order or tuple(os.environ.get("ETF_FLOW_PROVIDER_ORDER", "polygon,fmp").split(",")):
        name = str(provider or "").strip().lower()
        if not name:
            continue
        try:
            if name == "polygon":
                shares, payload = fetch_polygon_shares_outstanding(symbol)
            elif name == "fmp":
                shares, payload = fetch_fmp_profile_shares(symbol)
            else:
                errors.append(f"{name}: unsupported_provider")
                continue
            if shares is not None and shares > 0.0:
                return float(shares), name, dict(payload or {})
            errors.append(f"{name}: missing_shares_outstanding")
        except Exception as exc:
            errors.append(f"{name}: {exc}")
    return None, None, {"errors": list(errors)}


def closing_price_asof(con, *, symbol: str, asof_ts_ms: int) -> float | None:
    symbol_key = _clean_symbol(symbol)
    queries = (
        (
            """
            SELECT close
            FROM price_bars
            WHERE symbol = ?
              AND ts_ms <= ?
              AND close IS NOT NULL
            ORDER BY ts_ms DESC
            LIMIT 1
            """,
            (symbol_key, int(asof_ts_ms) + 86_399_999),
        ),
        (
            """
            SELECT COALESCE(price, px)
            FROM prices
            WHERE symbol = ?
              AND ts_ms <= ?
              AND COALESCE(price, px) IS NOT NULL
            ORDER BY ts_ms DESC
            LIMIT 1
            """,
            (symbol_key, int(asof_ts_ms) + 86_399_999),
        ),
        (
            """
            SELECT last
            FROM price_quotes
            WHERE symbol = ?
              AND ts_ms <= ?
              AND last IS NOT NULL
            ORDER BY ts_ms DESC
            LIMIT 1
            """,
            (symbol_key, int(asof_ts_ms) + 86_399_999),
        ),
    )
    for sql, params in queries:
        try:
            row = con.execute(sql, params).fetchone()
        except Exception:
            row = None
        if row and _safe_float(row[0]) is not None and float(row[0]) > 0.0:
            return float(row[0])
    return None


def normalize_share_reading(
    *,
    symbol: str,
    asof_day: date | str,
    shares_outstanding: float,
    source: str,
    price: float | None = None,
    nav: float | None = None,
    payload_json: Dict[str, Any] | None = None,
    diagnostics_json: Dict[str, Any] | None = None,
    ingested_ts_ms: int | None = None,
) -> Dict[str, Any]:
    symbol_key = _clean_symbol(symbol)
    parsed_day = parse_date(asof_day) if not isinstance(asof_day, date) else asof_day
    price_value = _safe_float(price)
    nav_value = _safe_float(nav)
    premium_pct = None
    if price_value is not None and nav_value is not None and nav_value > 0.0:
        premium_pct = float((price_value / nav_value) - 1.0)
    row = {
        "ts_ms": int(availability_ts_ms_for_date(parsed_day)),
        "symbol": symbol_key,
        "asof_date": parsed_day.isoformat(),
        "asof_ts_ms": int(date_to_ms(parsed_day)),
        "availability_ts_ms": int(availability_ts_ms_for_date(parsed_day)),
        "shares_outstanding": float(shares_outstanding),
        "source": str(source or "unknown"),
        "source_record_id": source_record_id(symbol_key, parsed_day),
        "price": price_value,
        "nav": nav_value,
        "premium_pct": premium_pct,
        "ingested_ts_ms": int(ingested_ts_ms or utc_now_ms()),
        "payload_json": dict(payload_json or {}),
        "diagnostics_json": dict(diagnostics_json or {}),
    }
    return row


def put_etf_shares_outstanding(row: Dict[str, Any], *, con) -> int:
    clean = dict(row or {})
    columns = [
        "ts_ms",
        "symbol",
        "asof_date",
        "asof_ts_ms",
        "availability_ts_ms",
        "shares_outstanding",
        "source",
        "source_record_id",
        "price",
        "nav",
        "premium_pct",
        "ingested_ts_ms",
        "payload_json",
        "diagnostics_json",
    ]
    values = [
        _json_param(con, clean.get(column)) if column in {"payload_json", "diagnostics_json"} else clean.get(column)
        for column in columns
    ]
    cur = con.execute(
        f"""
        INSERT INTO etf_shares_outstanding({", ".join(columns)})
        VALUES ({", ".join(["?"] * len(columns))})
        ON CONFLICT(source_record_id) DO UPDATE SET
          ts_ms = excluded.ts_ms,
          symbol = excluded.symbol,
          asof_date = excluded.asof_date,
          asof_ts_ms = excluded.asof_ts_ms,
          availability_ts_ms = excluded.availability_ts_ms,
          shares_outstanding = excluded.shares_outstanding,
          source = excluded.source,
          price = COALESCE(excluded.price, etf_shares_outstanding.price),
          nav = COALESCE(excluded.nav, etf_shares_outstanding.nav),
          premium_pct = COALESCE(excluded.premium_pct, etf_shares_outstanding.premium_pct),
          ingested_ts_ms = excluded.ingested_ts_ms,
          payload_json = excluded.payload_json,
          diagnostics_json = excluded.diagnostics_json
        """,
        tuple(values),
    )
    return int(getattr(cur, "rowcount", 0) or 0)


def _mean(values: Sequence[float]) -> float:
    return float(sum(values) / max(1, len(values)))


def _sample_std(values: Sequence[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = _mean(values)
    return float(math.sqrt(max(0.0, sum((x - mean) ** 2 for x in values) / max(1, len(values) - 1))))


def _zscore(value: float, history: Sequence[float]) -> float:
    std = _sample_std(list(history))
    if std <= 1e-12:
        return 0.0
    return float(max(-10.0, min(10.0, (float(value) - _mean(history)) / std)))


def is_split_adjustment(
    *,
    prev_shares: float,
    curr_shares: float,
    prev_price: float,
    curr_price: float,
) -> bool:
    if min(prev_shares, curr_shares, prev_price, curr_price) <= 0.0:
        return False
    share_ratio = float(curr_shares) / float(prev_shares)
    price_ratio = float(curr_price) / float(prev_price)
    reciprocal = abs((share_ratio * price_ratio) - 1.0)
    large_joint_move = share_ratio >= 1.5 or share_ratio <= (1.0 / 1.5) or price_ratio >= 1.5 or price_ratio <= (1.0 / 1.5)
    return bool(large_joint_move and reciprocal <= 0.15)


def split_adjusted_share_delta(
    *,
    prev_shares: float,
    curr_shares: float,
    prev_price: float | None,
    curr_price: float | None,
) -> Tuple[float, bool]:
    if prev_price is not None and curr_price is not None and is_split_adjustment(
        prev_shares=float(prev_shares),
        curr_shares=float(curr_shares),
        prev_price=float(prev_price),
        curr_price=float(curr_price),
    ):
        return 0.0, True
    return float(curr_shares) - float(prev_shares), False


def _crosses_ex_dividend(con: Any | None, *, previous: Mapping[str, Any], current: Mapping[str, Any]) -> bool:
    if con is None or not ETF_FLOW_SUPPRESS_EX_DIVIDEND:
        return False
    symbol = _clean_symbol(current.get("symbol") or previous.get("symbol"))
    if not symbol:
        return False
    start = int(previous.get("asof_ts_ms") or previous.get("availability_ts_ms") or 0)
    end = int(current.get("asof_ts_ms") or current.get("availability_ts_ms") or 0)
    if start <= 0 or end <= start:
        return False
    try:
        from engine.data.corporate_actions import corporate_action_ex_dates

        return bool(
            corporate_action_ex_dates(
                con,
                symbol=symbol,
                action_type="dividend",
                start_ts_ms=int(start),
                end_ts_ms=int(end),
            )
        )
    except Exception:
        return False


def compute_flow_features(
    readings: Sequence[Dict[str, Any]],
    *,
    asof_ts_ms: int,
    con: Any | None = None,
) -> Tuple[Dict[str, float], Dict[str, Any], bool]:
    features = {fid: 0.0 for fid in ETF_FLOW_FEATURE_IDS}
    rows = [
        dict(row)
        for row in readings or []
        if _safe_float((row or {}).get("shares_outstanding")) is not None
        and int((row or {}).get("availability_ts_ms") or 0) <= int(asof_ts_ms)
    ]
    rows.sort(key=lambda row: (int(row.get("availability_ts_ms") or 0), int(row.get("asof_ts_ms") or 0)))
    meta: Dict[str, Any] = {
        "latest_availability_ts_ms": None,
        "latest_asof_date": None,
        "latest_shares_outstanding": None,
        "latest_flow_dollars": 0.0,
        "latest_unexpected_flow": 0.0,
        "latest_aum": 0.0,
        "split_adjustments": 0,
        "rows": int(len(rows)),
        "nav_available": 1 if any(_safe_float(row.get("nav")) is not None for row in rows) else 0,
        "premium_follow_up": "ETF NAV/premium source not configured",
    }
    if not rows:
        return features, meta, False

    flows_scaled: List[float] = []
    unexpected_scaled: List[float] = []
    flow3_values: List[float] = []
    ewma: float | None = None
    latest_flow = 0.0
    latest_unexpected = 0.0
    latest_aum = 0.0
    split_count = 0
    ex_dividend_suppressions = 0
    previous: Dict[str, Any] | None = None
    for row in rows:
        shares = float(_safe_float(row.get("shares_outstanding")) or 0.0)
        price = _safe_float(row.get("price"))
        aum = float(shares * price) if price is not None and price > 0.0 else 0.0
        flow = 0.0
        if previous is not None:
            prev_shares = float(_safe_float(previous.get("shares_outstanding")) or 0.0)
            prev_price = _safe_float(previous.get("price"))
            delta, is_split = split_adjusted_share_delta(
                prev_shares=prev_shares,
                curr_shares=shares,
                prev_price=prev_price,
                curr_price=price,
            )
            split_count += 1 if is_split else 0
            flow = float(delta * (price or 0.0)) if price is not None and price > 0.0 else 0.0
            expected = float(ewma or 0.0)
            if _crosses_ex_dividend(con, previous=previous, current=row):
                flow = float(expected)
                ex_dividend_suppressions += 1
            unexpected = float(flow - expected)
            scaled_flow = float(flow / max(abs(aum), 1.0))
            scaled_unexpected = float(unexpected / max(abs(aum), 1.0))
            flows_scaled.append(scaled_flow)
            unexpected_scaled.append(scaled_unexpected)
            if len(flows_scaled) >= 3:
                flow3_values.append(float(sum(flows_scaled[-3:])))
            latest_flow = float(flow)
            latest_unexpected = float(unexpected)
            latest_aum = float(aum)
            ewma = float(flow if ewma is None else (EWMA_ALPHA_20 * flow + (1.0 - EWMA_ALPHA_20) * ewma))
        previous = row

    if unexpected_scaled:
        current_unexpected_scaled = float(unexpected_scaled[-1])
        features["etf_unexpected_flow_z"] = _zscore(current_unexpected_scaled, unexpected_scaled[:-1])
        if features["etf_unexpected_flow_z"] == 0.0 and abs(current_unexpected_scaled) > 0.0:
            features["etf_unexpected_flow_z"] = current_unexpected_scaled

    if flow3_values:
        current_flow3 = float(flow3_values[-1])
        features["etf_flow_3d_sum_z"] = _zscore(current_flow3, flow3_values[:-1])
        if features["etf_flow_3d_sum_z"] == 0.0 and abs(current_flow3) > 0.0:
            features["etf_flow_3d_sum_z"] = current_flow3

    history_std = _sample_std(flows_scaled[:-1])
    threshold = max(0.01, 2.0 * history_std)
    lagged = [value for value in flows_scaled[-6:-3] if value is not None]
    features["etf_flow_reversal_flag"] = 1.0 if any(float(value) >= float(threshold) for value in lagged) else 0.0

    latest = rows[-1]
    meta.update(
        {
            "latest_availability_ts_ms": int(latest.get("availability_ts_ms") or 0) or None,
            "latest_asof_date": str(latest.get("asof_date") or ""),
            "latest_shares_outstanding": float(_safe_float(latest.get("shares_outstanding")) or 0.0),
            "latest_flow_dollars": float(latest_flow),
            "latest_unexpected_flow": float(latest_unexpected),
            "latest_aum": float(latest_aum),
            "split_adjustments": int(split_count),
        }
    )
    if ex_dividend_suppressions > 0:
        meta["ex_dividend_suppressions"] = int(ex_dividend_suppressions)
    return {str(k): float(v or 0.0) for k, v in features.items()}, meta, len(rows) >= 2


def _row_to_dict(row: Any) -> Dict[str, Any]:
    if hasattr(row, "keys"):
        return {str(key): row[key] for key in row.keys()}
    return {}


def _load_share_rows(con, *, symbol: str, ts_ms: int, limit: int = FEATURE_LOOKBACK_READINGS) -> List[Dict[str, Any]]:
    rows = con.execute(
        """
        SELECT
          symbol,
          asof_date,
          asof_ts_ms,
          availability_ts_ms,
          shares_outstanding,
          source,
          price,
          nav,
          premium_pct
        FROM etf_shares_outstanding
        WHERE symbol = ?
          AND availability_ts_ms <= ?
        ORDER BY availability_ts_ms DESC, asof_ts_ms DESC
        LIMIT ?
        """,
        (_clean_symbol(symbol), int(ts_ms), int(limit)),
    ).fetchall()
    out = [row_dict for row in rows or [] if (row_dict := _row_to_dict(row))]
    if len(out) != len(rows or []):
        out = [
            {
                "symbol": row[0],
                "asof_date": row[1],
                "asof_ts_ms": row[2],
                "availability_ts_ms": row[3],
                "shares_outstanding": row[4],
                "source": row[5],
                "price": row[6],
                "nav": row[7],
                "premium_pct": row[8],
            }
            for row in rows
        ]
    return list(reversed(out))


def resolve_etf_flow_features(con, *, symbol: str, ts_ms: int) -> Tuple[Dict[str, float], Dict[str, Any], bool]:
    features = {fid: 0.0 for fid in ETF_FLOW_FEATURE_IDS}
    meta: Dict[str, Any] = {"latest_availability_ts_ms": None, "latest_asof_date": None, "rows": 0}
    if not is_etf_symbol(symbol):
        return features, meta, False
    try:
        rows = _load_share_rows(con, symbol=symbol, ts_ms=int(ts_ms))
    except Exception:
        return features, meta, False
    resolved, resolved_meta, available = compute_flow_features(rows, asof_ts_ms=int(ts_ms), con=con)
    for fid in ETF_FLOW_FEATURE_IDS:
        features[fid] = float(resolved.get(fid, 0.0) or 0.0)
    return features, dict(resolved_meta or {}), bool(available)


def materialize_etf_flow_features(con, *, symbol: str, ts_ms: int) -> Dict[str, Any]:
    ensure_etf_flow_tables(con)
    features, meta, available = resolve_etf_flow_features(con, symbol=symbol, ts_ms=int(ts_ms))
    now_ms = utc_now_ms()
    con.execute(
        """
        INSERT INTO etf_flow_features(
          symbol, asof_ts_ms, bucket_ts_ms,
          etf_unexpected_flow_z, etf_flow_3d_sum_z, etf_flow_reversal_flag,
          latest_shares_outstanding, latest_flow_dollars, latest_unexpected_flow,
          latest_aum, source_max_availability_ts_ms, created_ts_ms, meta_json
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(symbol, asof_ts_ms) DO UPDATE SET
          bucket_ts_ms = excluded.bucket_ts_ms,
          etf_unexpected_flow_z = excluded.etf_unexpected_flow_z,
          etf_flow_3d_sum_z = excluded.etf_flow_3d_sum_z,
          etf_flow_reversal_flag = excluded.etf_flow_reversal_flag,
          latest_shares_outstanding = excluded.latest_shares_outstanding,
          latest_flow_dollars = excluded.latest_flow_dollars,
          latest_unexpected_flow = excluded.latest_unexpected_flow,
          latest_aum = excluded.latest_aum,
          source_max_availability_ts_ms = excluded.source_max_availability_ts_ms,
          created_ts_ms = excluded.created_ts_ms,
          meta_json = excluded.meta_json
        """,
        (
            _clean_symbol(symbol),
            int(ts_ms),
            int(date_to_ms(datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=_UTC).date())),
            float(features["etf_unexpected_flow_z"]),
            float(features["etf_flow_3d_sum_z"]),
            float(features["etf_flow_reversal_flag"]),
            meta.get("latest_shares_outstanding"),
            float(meta.get("latest_flow_dollars") or 0.0),
            float(meta.get("latest_unexpected_flow") or 0.0),
            float(meta.get("latest_aum") or 0.0),
            meta.get("latest_availability_ts_ms"),
            int(now_ms),
            _json_param(con, dict(meta)),
        ),
    )
    return {"symbol": _clean_symbol(symbol), "available": bool(available), "features": features, "meta": meta}


def ingest_etf_shares_batch(
    *,
    symbols: Iterable[str] | None = None,
    now_ms: int | None = None,
    symbol_limit: int | None = None,
) -> Dict[str, Any]:
    anchor_ms = int(now_ms or utc_now_ms())
    limit = parse_symbol_limit(symbol_limit if symbol_limit is not None else os.environ.get("ETF_FLOW_SYMBOL_LIMIT"), 50)
    owns = True
    con = connect()
    try:
        ensure_etf_flow_tables(con)
        target_symbols = list(symbols or configured_etf_symbols(con, limit=limit))
        asof_day = asof_date_for_now(anchor_ms)
        rows: List[Dict[str, Any]] = []
        errors: List[str] = []
        for symbol in target_symbols:
            symbol_key = _clean_symbol(symbol)
            if not symbol_key or not is_etf_symbol(symbol_key):
                continue
            shares, source, payload = fetch_shares_outstanding(symbol_key)
            if shares is None or source is None:
                errors.append(f"{symbol_key}: {(payload or {}).get('errors') or 'missing_shares_outstanding'}")
                continue
            price = closing_price_asof(con, symbol=symbol_key, asof_ts_ms=date_to_ms(asof_day))
            rows.append(
                normalize_share_reading(
                    symbol=symbol_key,
                    asof_day=asof_day,
                    shares_outstanding=float(shares),
                    source=str(source),
                    price=price,
                    payload_json=dict(payload or {}),
                    diagnostics_json={
                        "availability_rule": "next_morning_09_30_et",
                        "source_priority": os.environ.get("ETF_FLOW_PROVIDER_ORDER", "polygon,fmp"),
                    },
                    ingested_ts_ms=anchor_ms,
                )
            )

        def _write(conw) -> int:
            ensure_etf_flow_tables(conw)
            written = 0
            for row in rows:
                written += int(put_etf_shares_outstanding(row, con=conw) or 0)
                materialize_etf_flow_features(conw, symbol=str(row.get("symbol")), ts_ms=int(row.get("availability_ts_ms") or anchor_ms))
            return int(written)

        written = int(run_write_txn(_write, table="etf_shares_outstanding", operation="ingest_etf_flows") or 0) if rows else 0
        return {
            "ok": not bool(errors),
            "symbols": int(len(target_symbols)),
            "rows": int(len(rows)),
            "written": int(written),
            "errors": list(errors),
            "asof_date": asof_day.isoformat(),
            "availability_ts_ms": int(availability_ts_ms_for_date(asof_day)),
        }
    finally:
        if owns:
            try:
                con.close()
            # system-audit: ignore[silent_except] connection close is best-effort cleanup.
            except Exception:
                pass
