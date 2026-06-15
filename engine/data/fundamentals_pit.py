"""Point-in-time fundamentals ingestion and feature joins.

README:
- Source: vendor fundamentals files with explicit publish/filing dates. SimFin
  is the first adapter because its bulk exports include publish-date fields;
  Sharadar is present as the same-interface optional adapter and is enabled
  only when ``SHARADAR_API_KEY`` is configured.
- Cadence: the supervised ingestion job polls daily by default via
  ``FUNDAMENTALS_PIT_POLL_SECONDS``. Bulk backfills are resumable through
  ``fundamentals_pit_backfill_state`` and idempotent by source record id.
- Availability lag: feature values join on ``publish_ts_ms`` only. Fiscal
  period end dates are stored for context but are never used as availability.
- Caveats: one row represents one original vendor publication for one metric.
  Restatements or amended filings must insert new rows with their own
  ``publish_ts_ms``; they must not update older rows backward in time.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import math
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timezone
from typing import Any, Dict, Iterable, List, Mapping, Protocol, Sequence, Tuple

import requests

from engine.data._credentials import get_data_credential
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect, run_write_txn

LOG = get_logger("engine.data.fundamentals_pit")

FUNDAMENTALS_FEATURE_IDS = [
    "fund_revenue",
    "fund_eps",
    "fund_gross_margin",
    "fund_net_margin",
    "fund_shares",
    "fund_book_value",
    "fund_fcf",
]

METRIC_NAMES = {
    "revenue",
    "eps",
    "gross_margin",
    "net_margin",
    "shares",
    "book_value",
    "fcf",
}

FEATURE_TO_METRIC = {
    "fund_revenue": "revenue",
    "fund_eps": "eps",
    "fund_gross_margin": "gross_margin",
    "fund_net_margin": "net_margin",
    "fund_shares": "shares",
    "fund_book_value": "book_value",
    "fund_fcf": "fcf",
}

SIMFIN_BULK_URL = os.environ.get("SIMFIN_BULK_URL", "https://simfin.com/api/bulk/fundamentals")
SHARADAR_BULK_URL = os.environ.get("SHARADAR_BULK_URL", "https://data.nasdaq.com/api/v3/datatables/SHARADAR/SF1")
FUNDAMENTALS_PIT_MODE = str(os.environ.get("FUNDAMENTALS_PIT_MODE", "auto") or "auto").strip().lower()
REQUEST_TIMEOUT_S = float(os.environ.get("FUNDAMENTALS_PIT_REQUEST_TIMEOUT_S", "30"))
RATE_LIMIT_SLEEP_S = max(0.0, float(os.environ.get("FUNDAMENTALS_PIT_RATE_LIMIT_SLEEP_S", "0.25")))

_UTC = timezone.utc


class FundamentalsAdapter(Protocol):
    vendor: str

    @property
    def enabled(self) -> bool: ...

    def fetch_publications(self, *, symbols: Sequence[str] | None = None) -> List[Dict[str, Any]]: ...


@dataclass
class AdapterResult:
    rows: List[Dict[str, Any]]
    errors: List[str]


def utc_now_ms() -> int:
    return int(time.time() * 1000)


def _clean_symbol(value: Any) -> str:
    return str(value or "").upper().strip().replace(".", "-")


def _clean_metric(value: Any) -> str:
    return str(value or "").lower().strip().replace(" ", "_").replace("-", "_")


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).replace(",", "").strip()
    if not text:
        return None
    try:
        out = float(text)
    except Exception:
        return None
    if not math.isfinite(out):
        return None
    return float(out)


def _json_param(con: Any, value: Any) -> Any:
    if isinstance(value, (dict, list)) and "sqlite" in str(type(con).__module__).lower():
        return json.dumps(value, separators=(",", ":"), sort_keys=True, default=str)
    return value


def parse_date(value: Any) -> date:
    text = str(value or "").strip()
    if not text:
        raise ValueError("empty date")
    if len(text) == 8 and text.isdigit():
        return datetime.strptime(text, "%Y%m%d").date()
    if "/" in text:
        return datetime.strptime(text[:10], "%m/%d/%Y").date()
    return datetime.fromisoformat(text[:10]).date()


def parse_ts_ms(value: Any) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        if "T" not in text and len(text) <= 10:
            return int(datetime.combine(parse_date(text), dt_time.min, tzinfo=_UTC).timestamp() * 1000)
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=_UTC)
        return int(parsed.astimezone(_UTC).timestamp() * 1000)
    except Exception:
        return None


def _field(record: Mapping[str, Any], aliases: Sequence[str]) -> Any:
    normalized = {str(k).lower().replace(" ", "").replace("_", ""): v for k, v in dict(record or {}).items()}
    for alias in aliases:
        key = str(alias).lower().replace(" ", "").replace("_", "")
        if key in normalized and normalized[key] not in (None, ""):
            return normalized[key]
    return None


def _source_record_id(*parts: Any) -> str:
    digest = hashlib.sha256("|".join(str(part or "") for part in parts).encode("utf-8", "ignore")).hexdigest()[:24]
    return f"fund_pit:{digest}"


def ensure_fundamentals_tables(con) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS fundamentals_pit (
            id BIGSERIAL PRIMARY KEY,
            ts_ms BIGINT,
            symbol TEXT NOT NULL,
            fiscal_period TEXT NOT NULL,
            metric TEXT NOT NULL,
            value DOUBLE PRECISION,
            publish_ts_ms BIGINT NOT NULL,
            publish_date TEXT,
            vendor TEXT NOT NULL,
            source_record_id TEXT NOT NULL,
            fiscal_year BIGINT,
            fiscal_quarter BIGINT,
            statement_type TEXT,
            ingested_ts_ms BIGINT,
            payload_json JSONB,
            diagnostics_json JSONB
        )
        """
    )
    con.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_fundamentals_pit_source_record_id ON fundamentals_pit(source_record_id)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_fundamentals_pit_symbol_metric_publish ON fundamentals_pit(symbol, metric, publish_ts_ms DESC)")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS fundamentals_pit_backfill_state (
            vendor TEXT NOT NULL,
            state_key TEXT NOT NULL,
            cursor TEXT,
            completed BIGINT NOT NULL DEFAULT 0,
            updated_ts_ms BIGINT,
            meta_json JSONB,
            PRIMARY KEY(vendor, state_key)
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS fundamentals_pit_symbol_features (
            symbol TEXT NOT NULL,
            asof_ts_ms BIGINT NOT NULL,
            fund_revenue DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            fund_eps DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            fund_gross_margin DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            fund_net_margin DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            fund_shares DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            fund_book_value DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            fund_fcf DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            source_max_publish_ts_ms BIGINT,
            created_ts_ms BIGINT,
            meta_json JSONB,
            PRIMARY KEY(symbol, asof_ts_ms)
        )
        """
    )


def _metric_values(record: Mapping[str, Any]) -> Dict[str, float]:
    revenue = _safe_float(_field(record, ("revenue", "Revenue", "revenue_usd", "RevenueUSD", "sales")))
    gross_profit = _safe_float(_field(record, ("gross_profit", "GrossProfit", "grossProfit")))
    net_income = _safe_float(_field(record, ("net_income", "NetIncome", "netIncome")))
    shares = _safe_float(_field(record, ("shares", "shares_basic", "SharesBasic", "weightedAverageShares", "sharesbas")))
    eps = _safe_float(_field(record, ("eps", "EPS", "eps_basic", "EPSBasic", "epsdil")))
    fcf = _safe_float(_field(record, ("fcf", "free_cash_flow", "FreeCashFlow", "freeCashFlow", "ncfo_capex")))
    book_value = _safe_float(_field(record, ("book_value", "BookValue", "total_equity", "TotalEquity", "equity")))
    gross_margin = _safe_float(_field(record, ("gross_margin", "GrossMargin", "grossMargin")))
    net_margin = _safe_float(_field(record, ("net_margin", "NetMargin", "netMargin")))
    if gross_margin is None and gross_profit is not None and revenue and revenue != 0.0:
        gross_margin = float(gross_profit / revenue)
    if net_margin is None and net_income is not None and revenue and revenue != 0.0:
        net_margin = float(net_income / revenue)
    if eps is None and net_income is not None and shares and shares > 0.0:
        eps = float(net_income / shares)
    values = {
        "revenue": revenue,
        "eps": eps,
        "gross_margin": gross_margin,
        "net_margin": net_margin,
        "shares": shares,
        "book_value": book_value,
        "fcf": fcf,
    }
    return {key: float(value) for key, value in values.items() if value is not None}


def normalize_fundamental_publication(record: Mapping[str, Any], *, vendor: str, ingested_ts_ms: int | None = None) -> List[Dict[str, Any]]:
    raw = dict(record or {})
    symbol = _clean_symbol(_field(raw, ("symbol", "ticker", "Ticker", "SimFinTicker")))
    publish_raw = _field(raw, ("publish_date", "PublishDate", "publishdate", "filing_date", "FilingDate", "datekey", "reportdate"))
    publish_ts = parse_ts_ms(publish_raw)
    fiscal_period = str(
        _field(raw, ("fiscal_period", "FiscalPeriod", "period", "fiscalPeriod", "calendardate", "ReportDate"))
        or ""
    ).strip()
    fiscal_year = _safe_float(_field(raw, ("fiscal_year", "FiscalYear", "fy")))
    fiscal_quarter = _safe_float(_field(raw, ("fiscal_quarter", "FiscalQuarter", "fq")))
    if not fiscal_period:
        year = int(fiscal_year or 0)
        quarter = int(fiscal_quarter or 0)
        fiscal_period = f"{year}Q{quarter}" if year and quarter else str(_field(raw, ("report_date", "ReportDate", "calendardate")) or "")
    if not symbol or publish_ts is None or not fiscal_period:
        return []
    now_ms = int(ingested_ts_ms or utc_now_ms())
    out: List[Dict[str, Any]] = []
    for metric, value in _metric_values(raw).items():
        source_id = _source_record_id(str(vendor), symbol, fiscal_period, metric, publish_ts, value)
        out.append(
            {
                "ts_ms": int(publish_ts),
                "symbol": symbol,
                "fiscal_period": fiscal_period,
                "metric": metric,
                "value": float(value),
                "publish_ts_ms": int(publish_ts),
                "publish_date": str(publish_raw or ""),
                "vendor": str(vendor),
                "source_record_id": source_id,
                "fiscal_year": int(fiscal_year) if fiscal_year is not None else None,
                "fiscal_quarter": int(fiscal_quarter) if fiscal_quarter is not None else None,
                "statement_type": str(_field(raw, ("statement_type", "StatementType", "dimension")) or ""),
                "ingested_ts_ms": int(now_ms),
                "payload_json": raw,
                "diagnostics_json": {"availability_rule": "publish_ts_ms"},
            }
        )
    return out


class SimFinAdapter:
    vendor = "simfin"

    def __init__(self, *, api_key: str | None = None, session: requests.Session | None = None, bulk_url: str | None = None) -> None:
        self.api_key = str(api_key if api_key is not None else get_data_credential("SIMFIN_API_KEY") or "").strip()
        self.session = session or requests.Session()
        self.bulk_url = str(bulk_url or SIMFIN_BULK_URL)

    @property
    def enabled(self) -> bool:
        return bool(self.api_key)

    def fetch_publications(self, *, symbols: Sequence[str] | None = None) -> List[Dict[str, Any]]:
        if not self.enabled:
            return []
        params: Dict[str, Any] = {"api-key": self.api_key}
        if symbols:
            params["ticker"] = ",".join(_clean_symbol(sym) for sym in symbols if _clean_symbol(sym))
        response = self.session.get(self.bulk_url, params=params, timeout=float(REQUEST_TIMEOUT_S))
        response.raise_for_status()
        payload = _decode_payload(response)
        return _filter_symbols(payload, symbols)


class SharadarAdapter:
    vendor = "sharadar"

    def __init__(self, *, api_key: str | None = None, session: requests.Session | None = None, bulk_url: str | None = None) -> None:
        self.api_key = str(api_key if api_key is not None else get_data_credential("SHARADAR_API_KEY") or "").strip()
        self.session = session or requests.Session()
        self.bulk_url = str(bulk_url or SHARADAR_BULK_URL)

    @property
    def enabled(self) -> bool:
        return bool(self.api_key)

    def fetch_publications(self, *, symbols: Sequence[str] | None = None) -> List[Dict[str, Any]]:
        if not self.enabled:
            return []
        params: Dict[str, Any] = {"api_key": self.api_key}
        if symbols:
            params["ticker"] = ",".join(_clean_symbol(sym) for sym in symbols if _clean_symbol(sym))
        response = self.session.get(self.bulk_url, params=params, timeout=float(REQUEST_TIMEOUT_S))
        response.raise_for_status()
        payload = _decode_payload(response)
        if isinstance(payload, dict) and "datatable" in payload:
            datatable = payload.get("datatable") or {}
            columns = [str((col or {}).get("name") or "") for col in (datatable.get("columns") or [])]
            payload = [dict(zip(columns, row)) for row in (datatable.get("data") or [])]
        return _filter_symbols(payload, symbols)


def _decode_payload(response: Any) -> List[Dict[str, Any]] | Dict[str, Any]:
    try:
        payload = response.json()
        if isinstance(payload, (list, dict)):
            return payload
    # system-audit: ignore[silent_except] non-JSON responses fall back to text decoding below.
    except Exception:
        pass
    text = str(getattr(response, "text", "") or "")
    if not text:
        try:
            text = bytes(getattr(response, "content", b"") or b"").decode("utf-8", "ignore")
        except Exception:
            text = ""
    if not text:
        return []
    rows = list(csv.DictReader(io.StringIO(text)))
    return [dict(row) for row in rows]


def _filter_symbols(payload: List[Dict[str, Any]] | Dict[str, Any], symbols: Sequence[str] | None) -> List[Dict[str, Any]]:
    if isinstance(payload, dict):
        rows = payload.get("data") or payload.get("results") or payload.get("rows") or []
    else:
        rows = payload
    out = [dict(row) for row in rows or [] if isinstance(row, dict)]
    wanted = {_clean_symbol(sym) for sym in symbols or [] if _clean_symbol(sym)}
    if not wanted:
        return out
    return [row for row in out if _clean_symbol(_field(row, ("symbol", "ticker", "Ticker", "SimFinTicker"))) in wanted]


_FUNDAMENTALS_COLUMNS = (
    "ts_ms",
    "symbol",
    "fiscal_period",
    "metric",
    "value",
    "publish_ts_ms",
    "publish_date",
    "vendor",
    "source_record_id",
    "fiscal_year",
    "fiscal_quarter",
    "statement_type",
    "ingested_ts_ms",
    "payload_json",
    "diagnostics_json",
)


def put_fundamental_pit_row(row: Mapping[str, Any], *, con) -> int:
    ensure_fundamentals_tables(con)
    values = [
        _json_param(con, row.get(column)) if column in {"payload_json", "diagnostics_json"} else row.get(column)
        for column in _FUNDAMENTALS_COLUMNS
    ]
    cur = con.execute(
        f"""
        INSERT INTO fundamentals_pit({", ".join(_FUNDAMENTALS_COLUMNS)})
        VALUES ({", ".join(["?"] * len(_FUNDAMENTALS_COLUMNS))})
        ON CONFLICT(source_record_id) DO NOTHING
        """,
        tuple(values),
    )
    return int(getattr(cur, "rowcount", 0) or 0)


def update_backfill_state(con, *, vendor: str, state_key: str, cursor: str = "", completed: bool = False, meta: Mapping[str, Any] | None = None) -> None:
    ensure_fundamentals_tables(con)
    con.execute(
        """
        INSERT INTO fundamentals_pit_backfill_state(vendor, state_key, cursor, completed, updated_ts_ms, meta_json)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(vendor, state_key) DO UPDATE SET
          cursor = excluded.cursor,
          completed = excluded.completed,
          updated_ts_ms = excluded.updated_ts_ms,
          meta_json = excluded.meta_json
        """,
        (str(vendor), str(state_key), str(cursor or ""), 1 if completed else 0, int(utc_now_ms()), _json_param(con, dict(meta or {}))),
    )


def latest_metric_rows(con, *, symbol: str, ts_ms: int) -> Dict[str, Dict[str, Any]]:
    ensure_fundamentals_tables(con)
    rows = con.execute(
        """
        SELECT symbol, fiscal_period, metric, value, publish_ts_ms, vendor, source_record_id
        FROM fundamentals_pit
        WHERE symbol = ?
          AND publish_ts_ms <= ?
        ORDER BY publish_ts_ms DESC, id DESC
        """,
        (_clean_symbol(symbol), int(ts_ms)),
    ).fetchall()
    latest: Dict[str, Dict[str, Any]] = {}
    columns = ["symbol", "fiscal_period", "metric", "value", "publish_ts_ms", "vendor", "source_record_id"]
    for raw in rows or []:
        row = {str(key): raw[key] for key in raw.keys()} if hasattr(raw, "keys") else dict(zip(columns, raw))
        metric = _clean_metric(row.get("metric"))
        if metric in METRIC_NAMES and metric not in latest:
            latest[metric] = row
    return latest


def _legacy_fundamentals_features(con, *, symbol: str, ts_ms: int) -> Tuple[Dict[str, float], Dict[str, Any], bool]:
    features = {fid: 0.0 for fid in FUNDAMENTALS_FEATURE_IDS}
    try:
        row = con.execute(
            """
            SELECT eps_act, revenue_act, updated_ts_ms
            FROM earnings_calendar
            WHERE symbol = ?
              AND COALESCE(updated_ts_ms, 0) <= ?
            ORDER BY updated_ts_ms DESC, earnings_date DESC
            LIMIT 1
            """,
            (_clean_symbol(symbol), int(ts_ms)),
        ).fetchone()
    except Exception:
        row = None
    if not row:
        return features, {"latest_publish_ts_ms": None, "mode": "legacy"}, False
    eps = _safe_float(row[0])
    revenue = _safe_float(row[1])
    if eps is not None:
        features["fund_eps"] = float(eps)
    if revenue is not None:
        features["fund_revenue"] = float(revenue)
    return features, {"latest_publish_ts_ms": int(row[2] or 0) or None, "mode": "legacy"}, True


def resolve_fundamentals_features(con, *, symbol: str, ts_ms: int, mode: str | None = None) -> Tuple[Dict[str, float], Dict[str, Any], bool]:
    selected_mode = str(mode or FUNDAMENTALS_PIT_MODE or "auto").strip().lower()
    features = {fid: 0.0 for fid in FUNDAMENTALS_FEATURE_IDS}
    pit_rows = latest_metric_rows(con, symbol=symbol, ts_ms=int(ts_ms)) if selected_mode not in {"legacy", "off", "0"} else {}
    if pit_rows:
        latest_publish = max([int(row.get("publish_ts_ms") or 0) for row in pit_rows.values()] or [0])
        vendors = sorted({str(row.get("vendor") or "") for row in pit_rows.values() if str(row.get("vendor") or "")})
        for fid, metric in FEATURE_TO_METRIC.items():
            row = pit_rows.get(metric)
            if row is not None:
                features[fid] = float(_safe_float(row.get("value")) or 0.0)
        return (
            features,
            {
                "latest_publish_ts_ms": int(latest_publish) if latest_publish > 0 else None,
                "mode": "pit",
                "vendors": vendors,
                "metrics": sorted(pit_rows.keys()),
            },
            True,
        )
    if selected_mode in {"pit", "on", "1", "true"}:
        return features, {"latest_publish_ts_ms": None, "mode": "pit"}, False
    return _legacy_fundamentals_features(con, symbol=symbol, ts_ms=int(ts_ms))


def materialize_fundamentals_symbol_features(con, *, symbol: str, ts_ms: int) -> Dict[str, Any]:
    ensure_fundamentals_tables(con)
    features, meta, available = resolve_fundamentals_features(con, symbol=symbol, ts_ms=int(ts_ms))
    con.execute(
        """
        INSERT INTO fundamentals_pit_symbol_features(
          symbol, asof_ts_ms, fund_revenue, fund_eps, fund_gross_margin,
          fund_net_margin, fund_shares, fund_book_value, fund_fcf,
          source_max_publish_ts_ms, created_ts_ms, meta_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(symbol, asof_ts_ms) DO UPDATE SET
          fund_revenue = excluded.fund_revenue,
          fund_eps = excluded.fund_eps,
          fund_gross_margin = excluded.fund_gross_margin,
          fund_net_margin = excluded.fund_net_margin,
          fund_shares = excluded.fund_shares,
          fund_book_value = excluded.fund_book_value,
          fund_fcf = excluded.fund_fcf,
          source_max_publish_ts_ms = excluded.source_max_publish_ts_ms,
          created_ts_ms = excluded.created_ts_ms,
          meta_json = excluded.meta_json
        """,
        (
            _clean_symbol(symbol),
            int(ts_ms),
            float(features["fund_revenue"]),
            float(features["fund_eps"]),
            float(features["fund_gross_margin"]),
            float(features["fund_net_margin"]),
            float(features["fund_shares"]),
            float(features["fund_book_value"]),
            float(features["fund_fcf"]),
            meta.get("latest_publish_ts_ms"),
            int(utc_now_ms()),
            _json_param(con, dict(meta or {})),
        ),
    )
    return {"features": dict(features), "meta": dict(meta), "available": bool(available)}


def compare_pit_vs_legacy(con, *, symbol: str, ts_values: Sequence[int]) -> Dict[str, Any]:
    changed = 0
    compared = 0
    examples: List[Dict[str, Any]] = []
    for ts_ms in ts_values or []:
        pit, _pit_meta, pit_available = resolve_fundamentals_features(con, symbol=symbol, ts_ms=int(ts_ms), mode="pit")
        legacy, _legacy_meta, legacy_available = resolve_fundamentals_features(con, symbol=symbol, ts_ms=int(ts_ms), mode="legacy")
        if not (pit_available or legacy_available):
            continue
        compared += 1
        diffs = {fid: (float(pit.get(fid, 0.0)), float(legacy.get(fid, 0.0))) for fid in FUNDAMENTALS_FEATURE_IDS if abs(float(pit.get(fid, 0.0)) - float(legacy.get(fid, 0.0))) > 1e-12}
        if diffs:
            changed += 1
            if len(examples) < 5:
                examples.append({"ts_ms": int(ts_ms), "diffs": diffs})
    return {"symbol": _clean_symbol(symbol), "compared": int(compared), "changed": int(changed), "examples": examples}


def ingest_fundamentals_pit_batch(*, adapters: Sequence[FundamentalsAdapter] | None = None, symbols: Sequence[str] | None = None) -> Dict[str, Any]:
    active_adapters = list([SimFinAdapter(), SharadarAdapter()] if adapters is None else adapters)
    now_ms = utc_now_ms()
    errors: List[str] = []
    normalized_rows: List[Dict[str, Any]] = []
    enabled_count = 0
    for adapter in active_adapters:
        if not adapter.enabled:
            continue
        enabled_count += 1
        try:
            raw_rows = adapter.fetch_publications(symbols=symbols)
            for raw in raw_rows:
                normalized_rows.extend(normalize_fundamental_publication(raw, vendor=adapter.vendor, ingested_ts_ms=now_ms))
        except Exception as exc:
            errors.append(f"{adapter.vendor}:{exc}")
        if RATE_LIMIT_SLEEP_S > 0:
            time.sleep(float(RATE_LIMIT_SLEEP_S))
    if enabled_count == 0:
        return {
            "ok": True,
            "blocked": True,
            "blocker": "missing_simfin_or_sharadar_api_key",
            "rows": 0,
            "written": 0,
            "errors": ["SIMFIN_API_KEY or SHARADAR_API_KEY is required"],
            "last_ingested_ts_ms": int(now_ms),
        }

    def _write(con) -> int:
        ensure_fundamentals_tables(con)
        written = 0
        for row in normalized_rows:
            written += put_fundamental_pit_row(row, con=con)
        for adapter in active_adapters:
            if adapter.enabled:
                update_backfill_state(con, vendor=adapter.vendor, state_key="bulk", cursor=str(now_ms), completed=True, meta={"rows": int(len(normalized_rows))})
        return int(written)

    written = int(run_write_txn(_write, table="fundamentals_pit", operation="ingest_fundamentals_pit_batch") or 0)
    latest = max([int(row.get("publish_ts_ms") or 0) for row in normalized_rows] or [now_ms])
    return {
        "ok": not bool(errors),
        "blocked": False,
        "rows": int(len(normalized_rows)),
        "written": int(written),
        "errors": errors,
        "last_ingested_ts_ms": int(latest or now_ms),
    }


__all__ = [
    "FUNDAMENTALS_FEATURE_IDS",
    "METRIC_NAMES",
    "SharadarAdapter",
    "SimFinAdapter",
    "compare_pit_vs_legacy",
    "ensure_fundamentals_tables",
    "ingest_fundamentals_pit_batch",
    "latest_metric_rows",
    "normalize_fundamental_publication",
    "parse_ts_ms",
    "put_fundamental_pit_row",
    "resolve_fundamentals_features",
]
