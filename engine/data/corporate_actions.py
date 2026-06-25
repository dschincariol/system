"""Point-in-time corporate-action calendar for equity labels and hygiene.

Rows are append-only and keyed by source_record_id. The availability timestamp
is the PIT guard: consumers must use only rows available at their decision or
evaluation anchor, never the pay date.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import time
from datetime import date, datetime, time as dt_time, timezone
from typing import Any, Iterable, Mapping, Sequence

import requests

from engine.data._credentials import get_data_credential
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import run_write_txn

LOG = get_logger("engine.data.corporate_actions")

POLYGON_BASE = "https://api.polygon.io"
FMP_BASE = "https://financialmodelingprep.com/api/v3"
REQUEST_TIMEOUT_S = float(os.environ.get("CORPORATE_ACTIONS_REQUEST_TIMEOUT_S", "20"))

_UTC = timezone.utc
_ACTION_TYPES = {"dividend", "split"}
_CORPORATE_ACTION_COLUMNS = (
    "symbol",
    "action_type",
    "ex_date",
    "ex_ts_ms",
    "pay_date",
    "pay_ts_ms",
    "record_date",
    "cash_amount",
    "split_from",
    "split_to",
    "currency",
    "availability_ts_ms",
    "source",
    "source_record_id",
    "ingested_ts_ms",
    "payload_json",
    "diagnostics_json",
)


def _warn_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    log_failure(
        LOG,
        event="corporate_actions_nonfatal",
        code=str(code),
        message=str(code),
        error=error,
        level=logging.WARNING,
        component="engine.data.corporate_actions",
        extra=dict(extra or {}) or None,
        persist=False,
    )


def utc_now_ms() -> int:
    return int(time.time() * 1000)


def _clean_symbol(value: Any) -> str:
    return str(value or "").upper().strip().replace(".", "-")


def _safe_float(value: Any, default: float | None = None) -> float | None:
    if value in (None, ""):
        return default
    try:
        out = float(str(value).replace(",", "").strip())
    except Exception:
        return default
    return float(out) if math.isfinite(out) else default


def _field(record: Mapping[str, Any], aliases: Sequence[str]) -> Any:
    normalized = {str(k).lower().replace(" ", "").replace("_", ""): v for k, v in dict(record or {}).items()}
    for alias in aliases:
        key = str(alias).lower().replace(" ", "").replace("_", "")
        if key in normalized and normalized[key] not in (None, ""):
            return normalized[key]
    return None


def parse_date(value: Any) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    text = str(value or "").strip()
    if not text:
        raise ValueError("empty date")
    if len(text) == 8 and text.isdigit():
        return datetime.strptime(text, "%Y%m%d").date()
    if "/" in text:
        return datetime.strptime(text[:10], "%m/%d/%Y").date()
    return datetime.fromisoformat(text[:10].replace("Z", "+00:00")).date()


def parse_ts_ms(value: Any) -> int | None:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        raw = int(value)
        return raw if raw > 10_000_000_000 else raw * 1000
    text = str(value or "").strip()
    if not text:
        return None
    try:
        if "T" not in text and len(text) <= 10:
            return date_to_ms(parse_date(text))
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=_UTC)
        return int(parsed.astimezone(_UTC).timestamp() * 1000)
    except Exception:
        return None


def date_to_ms(day: date | str) -> int:
    parsed = parse_date(day) if not isinstance(day, date) else day
    return int(datetime.combine(parsed, dt_time.min, tzinfo=_UTC).timestamp() * 1000)


def _date_text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    try:
        return parse_date(value).isoformat()
    except Exception:
        return None


def _source_record_id(symbol: str, action_type: str, ex_date: Any, pay_date: Any, source: str) -> str:
    parts = (
        _clean_symbol(symbol),
        str(action_type or "").lower().strip(),
        str(ex_date or ""),
        str(pay_date or ""),
        str(source or "").lower().strip(),
    )
    digest = hashlib.sha256("|".join(parts).encode("utf-8", "ignore")).hexdigest()[:24]
    return f"corp_action:{digest}"


def _is_sqlite_connection(con: Any) -> bool:
    return "sqlite" in str(type(con).__module__).lower()


def _json_param(con: Any, value: Any) -> Any:
    if _is_sqlite_connection(con) and isinstance(value, (dict, list)):
        return json.dumps(value, separators=(",", ":"), sort_keys=True, default=str)
    return value


def ensure_corporate_actions_tables(con: Any) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS corporate_actions (
            id BIGSERIAL PRIMARY KEY,
            symbol TEXT NOT NULL,
            action_type TEXT NOT NULL,
            ex_date TEXT,
            ex_ts_ms BIGINT,
            pay_date TEXT,
            pay_ts_ms BIGINT,
            record_date TEXT,
            cash_amount DOUBLE PRECISION,
            split_from DOUBLE PRECISION,
            split_to DOUBLE PRECISION,
            currency TEXT,
            availability_ts_ms BIGINT NOT NULL,
            source TEXT NOT NULL,
            source_record_id TEXT NOT NULL,
            ingested_ts_ms BIGINT,
            payload_json JSONB,
            diagnostics_json JSONB
        )
        """
    )
    con.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_corporate_actions_source_record_id ON corporate_actions(source_record_id)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_corporate_actions_symbol_type_ex ON corporate_actions(symbol, action_type, ex_ts_ms DESC)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_corporate_actions_symbol_availability ON corporate_actions(symbol, availability_ts_ms)")


def _infer_action_type(record: Mapping[str, Any], source: str) -> str | None:
    explicit = str(_field(record, ("action_type", "type", "event_type")) or "").lower().strip()
    if explicit in _ACTION_TYPES:
        return explicit
    source_l = str(source or "").lower()
    if "split" in source_l or _field(record, ("split_from", "split_to", "splitFrom", "splitTo", "numerator", "denominator")) is not None:
        return "split"
    if "dividend" in source_l or _field(record, ("cash_amount", "cashAmount", "dividend", "adjDividend")) is not None:
        return "dividend"
    return None


def _availability_ts_ms(record: Mapping[str, Any], *, ex_ts_ms: int | None, ingested_ts_ms: int) -> int:
    explicit = parse_ts_ms(_field(record, ("availability_ts_ms", "available_ts_ms", "available_at", "availableDate")))
    if explicit is not None:
        return int(explicit)
    announced = parse_ts_ms(
        _field(
            record,
            (
                "declaration_date",
                "declarationDate",
                "declared_date",
                "declaredDate",
                "announcement_date",
                "announcementDate",
                "published_date",
                "publishedDate",
            ),
        )
    )
    if announced is not None:
        return int(announced)
    # Historical vendor rows often lack announcement timestamps. Ex-date is a
    # conservative PIT fallback; pay-date is never used as availability.
    return int(ex_ts_ms if ex_ts_ms is not None else ingested_ts_ms)


def normalize_corporate_action(record: Mapping[str, Any], *, source: str, ingested_ts_ms: int | None = None) -> list[dict[str, Any]]:
    action_type = _infer_action_type(record, source)
    if action_type not in _ACTION_TYPES:
        return []
    ingested = int(ingested_ts_ms or utc_now_ms())
    symbol = _clean_symbol(_field(record, ("ticker", "symbol", "underlying", "Symbol")) or "")
    if not symbol:
        return []

    if action_type == "split":
        ex_raw = _field(record, ("execution_date", "executionDate", "ex_date", "exDate", "date"))
        split_from = _safe_float(_field(record, ("split_from", "splitFrom", "fromFactor", "denominator")))
        split_to = _safe_float(_field(record, ("split_to", "splitTo", "toFactor", "numerator")))
        cash_amount = None
        pay_raw = None
    else:
        ex_raw = _field(record, ("ex_dividend_date", "exDividendDate", "ex_date", "exDate", "date"))
        cash_amount = _safe_float(_field(record, ("cash_amount", "cashAmount", "dividend", "adjDividend")))
        pay_raw = _field(record, ("pay_date", "payDate", "payment_date", "paymentDate"))
        split_from = None
        split_to = None

    ex_date = _date_text(ex_raw)
    if not ex_date:
        return []
    ex_ts_ms = date_to_ms(ex_date)
    pay_date = _date_text(pay_raw)
    record_date = _date_text(_field(record, ("record_date", "recordDate")))
    source_key = str(source or "").lower().strip()
    row = {
        "symbol": symbol,
        "action_type": action_type,
        "ex_date": ex_date,
        "ex_ts_ms": int(ex_ts_ms),
        "pay_date": pay_date,
        "pay_ts_ms": date_to_ms(pay_date) if pay_date else None,
        "record_date": record_date,
        "cash_amount": cash_amount,
        "split_from": split_from,
        "split_to": split_to,
        "currency": str(_field(record, ("currency", "currency_name", "currencyCode")) or "").upper().strip() or None,
        "availability_ts_ms": _availability_ts_ms(record, ex_ts_ms=int(ex_ts_ms), ingested_ts_ms=ingested),
        "source": source_key,
        "source_record_id": _source_record_id(symbol, action_type, ex_date, pay_date, source_key),
        "ingested_ts_ms": int(ingested),
        "payload_json": dict(record or {}),
        "diagnostics_json": {"normalizer": "corporate_actions_v1"},
    }
    return [row]


def _json_response(url: str, *, params: Mapping[str, Any]) -> tuple[Any, dict[str, Any]]:
    try:
        response = requests.get(url, params=dict(params), timeout=float(REQUEST_TIMEOUT_S))
        if hasattr(response, "raise_for_status"):
            response.raise_for_status()
        payload = response.json() if hasattr(response, "json") else {}
        return payload, {"ok": True}
    except Exception as exc:
        return {}, {"ok": False, "error": str(exc)}


def _results_list(payload: Any) -> list[Mapping[str, Any]]:
    if isinstance(payload, dict):
        for key in ("results", "historical", "data"):
            value = payload.get(key)
            if isinstance(value, list):
                return [dict(row) for row in value if isinstance(row, Mapping)]
    if isinstance(payload, list):
        return [dict(row) for row in payload if isinstance(row, Mapping)]
    return []


def fetch_polygon_corporate_actions(symbol: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    key = get_data_credential("POLYGON_API_KEY")
    symbol_key = _clean_symbol(symbol)
    if not key:
        return [], {"provider": "polygon", "error": "missing_polygon_api_key"}
    rows: list[dict[str, Any]] = []
    payload_meta: dict[str, Any] = {"provider": "polygon", "errors": []}
    endpoints = (
        ("polygon_splits", f"{POLYGON_BASE}/v3/reference/splits", {"ticker": symbol_key, "limit": 1000}),
        ("polygon_dividends", f"{POLYGON_BASE}/v3/reference/dividends", {"ticker": symbol_key, "limit": 1000}),
    )
    now_ms = utc_now_ms()
    for source, url, params in endpoints:
        payload, meta = _json_response(url, params={**params, "apiKey": key})
        if not meta.get("ok"):
            payload_meta["errors"].append({source: meta.get("error")})
            continue
        results = _results_list(payload)
        payload_meta[source] = int(len(results))
        for record in results:
            record = {**dict(record), "ticker": dict(record).get("ticker") or symbol_key}
            rows.extend(normalize_corporate_action(record, source=source, ingested_ts_ms=now_ms))
    return rows, payload_meta


def fetch_fmp_corporate_actions(symbol: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    key = get_data_credential("FMP_API_KEY")
    symbol_key = _clean_symbol(symbol)
    if not key:
        return [], {"provider": "fmp", "error": "missing_fmp_api_key"}
    rows: list[dict[str, Any]] = []
    payload_meta: dict[str, Any] = {"provider": "fmp", "errors": []}
    endpoints = (
        ("fmp_dividend", f"{FMP_BASE}/historical-price-full/stock_dividend/{symbol_key}"),
        ("fmp_split", f"{FMP_BASE}/historical-price-full/stock_split/{symbol_key}"),
    )
    now_ms = utc_now_ms()
    for source, url in endpoints:
        payload, meta = _json_response(url, params={"apikey": key})
        if not meta.get("ok"):
            payload_meta["errors"].append({source: meta.get("error")})
            continue
        results = _results_list(payload)
        payload_meta[source] = int(len(results))
        for record in results:
            record = {**dict(record), "symbol": dict(record).get("symbol") or symbol_key}
            rows.extend(normalize_corporate_action(record, source=source, ingested_ts_ms=now_ms))
    return rows, payload_meta


def put_corporate_action_row(row: Mapping[str, Any], *, con: Any) -> int:
    ensure_corporate_actions_tables(con)
    values = [
        _json_param(con, row.get(column)) if column in {"payload_json", "diagnostics_json"} else row.get(column)
        for column in _CORPORATE_ACTION_COLUMNS
    ]
    cur = con.execute(
        f"""
        INSERT INTO corporate_actions({", ".join(_CORPORATE_ACTION_COLUMNS)})
        VALUES ({", ".join(["?"] * len(_CORPORATE_ACTION_COLUMNS))})
        ON CONFLICT(source_record_id) DO NOTHING
        """,
        tuple(values),
    )
    return int(getattr(cur, "rowcount", 0) or 0)


def _fetch_action_rows(
    con: Any,
    *,
    symbol: str,
    start_ts_ms: int,
    end_ts_ms: int,
    action_type: str | None = None,
    availability_lte_ts_ms: int | None = None,
) -> list[dict[str, Any]]:
    symbol_key = _clean_symbol(symbol)
    if not symbol_key:
        return []
    clauses = ["symbol = ?", "ex_ts_ms > ?", "ex_ts_ms <= ?"]
    params: list[Any] = [symbol_key, int(start_ts_ms), int(end_ts_ms)]
    if action_type:
        clauses.append("action_type = ?")
        params.append(str(action_type).lower().strip())
    if availability_lte_ts_ms is not None:
        clauses.append("availability_ts_ms <= ?")
        params.append(int(availability_lte_ts_ms))
    sql = f"""
        SELECT action_type, ex_ts_ms, ex_date, pay_date, cash_amount, split_from,
               split_to, source, source_record_id, availability_ts_ms
        FROM corporate_actions
        WHERE {" AND ".join(clauses)}
        ORDER BY ex_ts_ms ASC, id ASC
    """
    try:
        rows = con.execute(sql, tuple(params)).fetchall()
    except Exception:
        return []
    out: list[dict[str, Any]] = []
    for row in rows or []:
        out.append(
            {
                "action_type": str(row[0] or "").lower().strip(),
                "ex_ts_ms": int(row[1] or 0),
                "ex_date": row[2],
                "pay_date": row[3],
                "cash_amount": _safe_float(row[4], 0.0),
                "split_from": _safe_float(row[5]),
                "split_to": _safe_float(row[6]),
                "source": row[7],
                "source_record_id": row[8],
                "availability_ts_ms": int(row[9] or 0),
            }
        )
    return out


def corporate_action_total_return_factor(
    con: Any,
    *,
    symbol: str,
    start_ts_ms: int,
    end_ts_ms: int,
    entry_px: float,
) -> tuple[float, dict[str, Any]]:
    entry = _safe_float(entry_px)
    if entry is None or entry <= 0.0:
        return 1.0, {"reason": "invalid_entry_price"}
    rows = _fetch_action_rows(
        con,
        symbol=symbol,
        start_ts_ms=int(start_ts_ms),
        end_ts_ms=int(end_ts_ms),
        availability_lte_ts_ms=int(start_ts_ms),
    )
    if not rows:
        return 1.0, {"reason": "no_corporate_action"}

    split_factor = 1.0
    dividend_cash = 0.0
    dividends = 0
    splits = 0
    source_ids: list[str] = []
    for row in rows:
        source_id = str(row.get("source_record_id") or "")
        if source_id:
            source_ids.append(source_id)
        action_type = str(row.get("action_type") or "").lower().strip()
        if action_type == "dividend":
            cash = _safe_float(row.get("cash_amount"), 0.0) or 0.0
            if cash > 0.0:
                dividend_cash += float(cash)
                dividends += 1
        elif action_type == "split":
            split_from = _safe_float(row.get("split_from"))
            split_to = _safe_float(row.get("split_to"))
            if split_from is None or split_to is None or split_from <= 0.0 or split_to <= 0.0:
                err = RuntimeError("unparseable split corporate action")
                _warn_nonfatal(
                    "CORPORATE_ACTION_SPLIT_UNPARSEABLE",
                    err,
                    symbol=_clean_symbol(symbol),
                    source_record_id=source_id,
                )
                return 1.0, {
                    "reason": "corp_action_unparseable",
                    "corp_action_adjusted": False,
                    "source_record_id": source_id,
                }
            split_factor *= float(split_to) / float(split_from)
            splits += 1

    dividend_return = float(dividend_cash) / float(entry)
    adjusted = bool(abs(split_factor - 1.0) > 1e-12 or dividend_cash > 0.0)
    return float(split_factor), {
        "reason": "corporate_action_adjusted" if adjusted else "corporate_action_neutral",
        "corp_action_adjusted": bool(adjusted),
        "split_factor": float(split_factor),
        "dividend_cash": float(dividend_cash),
        "dividend_return": float(dividend_return),
        "dividend_count": int(dividends),
        "split_count": int(splits),
        "source_record_ids": source_ids[:20],
    }


def corporate_action_ex_dates(
    con: Any,
    *,
    symbol: str,
    action_type: str,
    start_ts_ms: int,
    end_ts_ms: int,
) -> list[int]:
    rows = _fetch_action_rows(
        con,
        symbol=symbol,
        action_type=str(action_type).lower().strip(),
        start_ts_ms=int(start_ts_ms),
        end_ts_ms=int(end_ts_ms),
        availability_lte_ts_ms=int(end_ts_ms),
    )
    out: list[int] = []
    for row in rows:
        if str(action_type).lower().strip() == "split":
            split_from = _safe_float(row.get("split_from"))
            split_to = _safe_float(row.get("split_to"))
            if split_from is None or split_to is None or split_from <= 0.0 or split_to <= 0.0:
                continue
        ex_ts = int(row.get("ex_ts_ms") or 0)
        if ex_ts > 0:
            out.append(int(ex_ts))
    return sorted(set(out))


def ingest_corporate_actions_batch(
    *,
    symbols: Iterable[str] | None = None,
    provider_order: Sequence[str] = ("polygon", "fmp"),
) -> dict[str, Any]:
    configured = [
        _clean_symbol(item)
        for item in (
            symbols
            if symbols is not None
            else str(os.environ.get("CORPORATE_ACTION_SYMBOLS", os.environ.get("DEFAULT_SYMBOLS", "SPY,QQQ,IWM,DIA"))).split(",")
        )
        if _clean_symbol(item)
    ]
    if not configured:
        return {"ok": True, "blocked": True, "blocker": "no_corporate_action_symbols", "rows": 0, "written": 0, "errors": []}

    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    enabled_provider = False
    for symbol in configured:
        for provider in provider_order:
            provider_key = str(provider or "").lower().strip()
            if provider_key == "polygon":
                fetched, meta = fetch_polygon_corporate_actions(symbol)
            elif provider_key == "fmp":
                fetched, meta = fetch_fmp_corporate_actions(symbol)
            else:
                continue
            if "missing_" not in str(meta.get("error") or ""):
                enabled_provider = True
            if meta.get("error"):
                errors.append(f"{provider_key}:{symbol}:{meta.get('error')}")
            rows.extend(fetched)
    if not enabled_provider:
        return {
            "ok": True,
            "blocked": True,
            "blocker": "missing_polygon_or_fmp_api_key",
            "rows": 0,
            "written": 0,
            "errors": ["POLYGON_API_KEY or FMP_API_KEY is required"],
            "last_ingested_ts_ms": int(utc_now_ms()),
        }

    def _write(con: Any) -> int:
        ensure_corporate_actions_tables(con)
        written = 0
        for row in rows:
            written += put_corporate_action_row(row, con=con)
        return int(written)

    written = int(run_write_txn(_write, table="corporate_actions", operation="ingest_corporate_actions_batch") or 0)
    latest = max([int(row.get("ingested_ts_ms") or 0) for row in rows] or [utc_now_ms()])
    return {
        "ok": not bool(errors),
        "blocked": False,
        "rows": int(len(rows)),
        "written": int(written),
        "errors": errors,
        "last_ingested_ts_ms": int(latest),
    }


__all__ = [
    "corporate_action_ex_dates",
    "corporate_action_total_return_factor",
    "date_to_ms",
    "ensure_corporate_actions_tables",
    "fetch_fmp_corporate_actions",
    "fetch_polygon_corporate_actions",
    "ingest_corporate_actions_batch",
    "normalize_corporate_action",
    "parse_ts_ms",
    "put_corporate_action_row",
]
