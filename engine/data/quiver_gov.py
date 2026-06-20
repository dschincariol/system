"""Quiver government-flow ingestion and point-in-time features.

README:
- Source: Quiver Quantitative Tier-1 API for congressional trading, lobbying
  spend, and government contract awards. Existing free STOCK Act congressional
  feeds remain usable as a fallback source.
- Cadence: the supervised ingestion job polls daily by default via
  ``QUIVER_GOV_POLL_SECONDS``. Quiver endpoints are low-frequency disclosure
  feeds, so extra polls are idempotent.
- Availability lag: congressional trade features join on disclosure/filing
  timestamp only, never transaction date. Lobbying and contracts join on the
  provider disclosure/publish date stored as ``availability_ts_ms``.
- Caveats: unconditional congressional-trade alpha is weak. These features are
  conditional slices only: committee relevance, leadership trades, sales, and
  policy-linked lobbying/contract flow. Committee, sector, and leadership maps
  are static config tables and must be refreshed as Congress changes.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import time
from datetime import date, datetime, time as dt_time, timezone
from typing import Any, Callable, Dict, List, Mapping, Sequence, Tuple
from urllib.parse import urljoin

import requests

from engine.data._credentials import get_data_credential
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import run_write_txn

GOV_FEATURE_IDS = [
    "congress_committee_buy_30d",
    "congress_leadership_trade_flag",
    "congress_sale_signal_30d",
    "lobbying_spend_z_yoy",
    "gov_contract_award_z",
]

QUIVER_BASE_URL = os.environ.get("QUIVER_BASE_URL", "https://api.quiverquant.com/beta").rstrip("/")
QUIVER_AUTH_SCHEME = os.environ.get("QUIVER_AUTH_SCHEME", "Bearer").strip()
QUIVER_CONGRESS_ENDPOINT = os.environ.get("QUIVER_CONGRESS_ENDPOINT", "/live/congresstrading")
QUIVER_LOBBYING_ENDPOINT = os.environ.get("QUIVER_LOBBYING_ENDPOINT", "/live/lobbying")
QUIVER_CONTRACTS_ENDPOINT = os.environ.get("QUIVER_CONTRACTS_ENDPOINT", "/live/govcontractsall")
QUIVER_TIMEOUT_S = float(os.environ.get("QUIVER_TIMEOUT_S", "20"))
QUIVER_MAX_RETRIES = max(1, int(os.environ.get("QUIVER_MAX_RETRIES", "3")))
QUIVER_MAX_PAGES = max(1, int(os.environ.get("QUIVER_MAX_PAGES", "50")))
GOV_FEATURE_LOOKBACK_DAYS = max(30, int(os.environ.get("GOV_FEATURE_LOOKBACK_DAYS", "730")))

_UTC = timezone.utc
_KEY_RE = re.compile(r"[^a-z0-9]+", re.IGNORECASE)
_AMOUNT_RANGE_RX = re.compile(r"(?P<lo>\d[\d,]*(?:\.\d+)?)\s*(?:-|to)\s*(?P<hi>\d[\d,]*(?:\.\d+)?)", re.IGNORECASE)
_AMOUNT_SINGLE_RX = re.compile(r"(?P<value>\d[\d,]*(?:\.\d+)?)")
LOG = get_logger("engine.data.quiver_gov")


def _warn_nonfatal(code: str, error: BaseException | None = None, **extra: Any) -> None:
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error or code),
        error=error,
        level=30,
        component="engine.data.quiver_gov",
        extra=extra or None,
        persist=False,
    )


DEFAULT_COMMITTEE_SECTOR_MAP: Tuple[Tuple[str, str, float], ...] = (
    ("Agriculture", "consumer_staples", 1.0),
    ("Armed Services", "defense", 1.0),
    ("Banking", "financials", 1.0),
    ("Commerce", "technology", 1.0),
    ("Energy and Commerce", "healthcare", 1.0),
    ("Energy and Commerce", "energy", 1.0),
    ("Energy and Natural Resources", "energy", 1.0),
    ("Financial Services", "financials", 1.0),
    ("Health, Education, Labor, and Pensions", "healthcare", 1.0),
    ("Homeland Security", "defense", 0.7),
    ("Judiciary", "technology", 0.5),
    ("Transportation and Infrastructure", "industrials", 1.0),
    ("Ways and Means", "healthcare", 0.5),
    ("Ways and Means", "consumer_discretionary", 0.5),
)


def utc_now_ms() -> int:
    return int(time.time() * 1000)


def _clean_symbol(value: Any) -> str:
    return str(value or "").upper().strip().replace("$", "").replace(".", "-")


def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _norm_key(value: Any) -> str:
    return _KEY_RE.sub("", str(value or "").strip().lower())


def _field(record: Mapping[str, Any], aliases: Sequence[str], default: Any = None) -> Any:
    normalized = {_norm_key(key): value for key, value in dict(record or {}).items()}
    for alias in aliases:
        key = _norm_key(alias)
        if key in normalized and normalized[key] not in (None, ""):
            return normalized[key]
    return default


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).replace("$", "").replace(",", "").strip()
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
    if re.fullmatch(r"\d{8}", text):
        return datetime.strptime(text, "%Y%m%d").date()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return datetime.strptime(text, "%Y-%m-%d").date()
    if re.fullmatch(r"\d{1,2}/\d{1,2}/\d{4}", text):
        return datetime.strptime(text, "%m/%d/%Y").date()
    return datetime.fromisoformat(text.replace("Z", "+00:00")).date()


def parse_ts_ms(value: Any) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text) or re.fullmatch(r"\d{8}", text) or re.fullmatch(r"\d{1,2}/\d{1,2}/\d{4}", text):
        return int(datetime.combine(parse_date(text), dt_time.min, tzinfo=_UTC).timestamp() * 1000)
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=_UTC)
        return int(parsed.astimezone(_UTC).timestamp() * 1000)
    except Exception:
        return None


def _date_key(value: Any, ts_ms: int | None = None) -> str:
    try:
        return parse_date(value).isoformat()
    # system-audit: ignore[silent_except] unparsable dates fall back to the event timestamp.
    except Exception:
        pass
    if ts_ms:
        return datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=_UTC).date().isoformat()
    return ""


def _source_record_id(prefix: str, *parts: Any) -> str:
    digest = hashlib.sha256("|".join(str(part or "") for part in parts).encode("utf-8", "ignore")).hexdigest()[:24]
    return f"{prefix}:{digest}"


def _parse_amount_range(value: Any) -> tuple[float | None, float | None, float | None]:
    raw = str(value or "").strip()
    if not raw:
        return None, None, None
    cleaned = raw.replace("$", "").replace(">", "").replace("<", "").strip()
    match = _AMOUNT_RANGE_RX.search(cleaned)
    if match:
        lo = _safe_float(match.group("lo"))
        hi = _safe_float(match.group("hi"))
        if lo is not None and hi is not None:
            return float(lo), float(hi), float((lo + hi) / 2.0)
    match = _AMOUNT_SINGLE_RX.search(cleaned)
    if match:
        num = _safe_float(match.group("value"))
        if num is not None:
            return float(num), float(num), float(num)
    return None, None, None


def _normalize_direction(raw_type: Any) -> tuple[str, str]:
    text = str(raw_type or "").strip()
    lowered = text.lower()
    if any(token in lowered for token in ("purchase", "buy", "purch", "acquired", "acquisition")):
        return "purchase", "buy"
    if any(token in lowered for token in ("sale", "sell", "sold", "disposed")):
        return "sale", "sell"
    if "exchange" in lowered:
        return "exchange", "neutral"
    return "other", "neutral"


def congressional_dedupe_key(*, member_name: Any, symbol: Any, transaction_date: Any = "", transaction_ts_ms: Any = None) -> str:
    member = re.sub(r"\s+", " ", str(member_name or "").strip().lower())
    sym = _clean_symbol(symbol)
    date_part = _date_key(transaction_date, int(transaction_ts_ms or 0) if transaction_ts_ms else None)
    return "|".join((member, sym, date_part))


class QuiverClient:
    """Small Quiver API client with auth, pagination, and retry/backoff."""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        base_url: str | None = None,
        session: requests.Session | None = None,
        sleep_fn: Callable[[float], None] | None = None,
        max_retries: int | None = None,
    ) -> None:
        self.api_key = str(api_key if api_key is not None else get_data_credential("QUIVER_API_KEY") or "").strip()
        self.base_url = str(base_url or QUIVER_BASE_URL).rstrip("/")
        self.session = session or requests.Session()
        self.sleep_fn = sleep_fn or time.sleep
        self.max_retries = max(1, int(max_retries or QUIVER_MAX_RETRIES))

    @property
    def enabled(self) -> bool:
        return bool(self.api_key)

    def _headers(self) -> Dict[str, str]:
        auth_value = f"{QUIVER_AUTH_SCHEME} {self.api_key}".strip() if QUIVER_AUTH_SCHEME else self.api_key
        return {"Accept": "application/json", "Authorization": auth_value}

    def _url(self, path: str) -> str:
        text = str(path or "")
        if text.startswith("http://") or text.startswith("https://"):
            return text
        return urljoin(f"{self.base_url}/", text.lstrip("/"))

    def _request_json(self, path: str, *, params: Mapping[str, Any] | None = None) -> Any:
        if not self.enabled:
            raise RuntimeError("missing_quiver_api_key")
        last_error: Exception | None = None
        for attempt in range(self.max_retries):
            try:
                response = self.session.get(
                    self._url(path),
                    params=dict(params or {}),
                    headers=self._headers(),
                    timeout=float(QUIVER_TIMEOUT_S),
                )
                status_code = int(getattr(response, "status_code", 200) or 200)
                if status_code in {429, 500, 502, 503, 504} and attempt < self.max_retries - 1:
                    retry_after = 0.0
                    try:
                        retry_after = float((getattr(response, "headers", {}) or {}).get("Retry-After") or 0.0)
                    except Exception:
                        retry_after = 0.0
                    self.sleep_fn(max(float(retry_after), min(30.0, 2.0 ** attempt)))
                    continue
                if hasattr(response, "raise_for_status"):
                    response.raise_for_status()
                if hasattr(response, "json"):
                    return response.json()
                return json.loads(str(getattr(response, "text", "") or "{}"))
            except Exception as exc:
                last_error = exc
                if attempt < self.max_retries - 1:
                    self.sleep_fn(min(30.0, 2.0 ** attempt))
                    continue
                raise
        if last_error is not None:
            raise last_error
        return []

    def get_paginated(self, path: str, *, params: Mapping[str, Any] | None = None) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        next_path = str(path)
        next_params: Dict[str, Any] = dict(params or {})
        page = int(next_params.get("page") or 1)

        for _idx in range(QUIVER_MAX_PAGES):
            payload = self._request_json(next_path, params=next_params)
            if isinstance(payload, list):
                rows.extend(dict(item) for item in payload if isinstance(item, dict))
                break
            if not isinstance(payload, dict):
                break
            batch = payload.get("data") or payload.get("results") or payload.get("items") or payload.get("transactions") or []
            if isinstance(batch, list):
                rows.extend(dict(item) for item in batch if isinstance(item, dict))
            next_value = payload.get("next") or payload.get("next_page") or (payload.get("pagination") or {}).get("next")
            if not next_value:
                break
            if isinstance(next_value, str) and next_value.startswith(("http://", "https://", "/")):
                next_path = str(next_value)
                next_params = {}
            else:
                page = int(next_value if str(next_value).isdigit() else page + 1)
                next_params = dict(params or {})
                next_params["page"] = int(page)
        return rows

    def fetch_congressional_trading(self) -> List[Dict[str, Any]]:
        return self.get_paginated(QUIVER_CONGRESS_ENDPOINT)

    def fetch_lobbying(self) -> List[Dict[str, Any]]:
        return self.get_paginated(QUIVER_LOBBYING_ENDPOINT)

    def fetch_gov_contracts(self) -> List[Dict[str, Any]]:
        return self.get_paginated(QUIVER_CONTRACTS_ENDPOINT)


def ensure_gov_tables(con) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS quiver_congressional_trades (
            id BIGSERIAL PRIMARY KEY,
            ts_ms BIGINT,
            symbol TEXT,
            source_record_id TEXT NOT NULL,
            dedupe_key TEXT NOT NULL,
            member_name TEXT,
            chamber TEXT,
            party TEXT,
            district TEXT,
            transaction_type_raw TEXT,
            transaction_type TEXT,
            direction TEXT,
            amount_range TEXT,
            amount_low DOUBLE PRECISION,
            amount_high DOUBLE PRECISION,
            amount_mid DOUBLE PRECISION,
            transaction_date TEXT,
            transaction_ts_ms BIGINT,
            disclosure_date TEXT,
            disclosure_ts_ms BIGINT,
            availability_ts_ms BIGINT NOT NULL,
            source_url TEXT,
            ingested_ts_ms BIGINT,
            payload_json JSONB,
            diagnostics_json JSONB
        )
        """
    )
    con.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_quiver_congressional_trades_source_record_id ON quiver_congressional_trades(source_record_id)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_quiver_congressional_trades_symbol_avail ON quiver_congressional_trades(symbol, availability_ts_ms DESC)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_quiver_congressional_trades_dedupe ON quiver_congressional_trades(dedupe_key)")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS quiver_lobbying_filings (
            id BIGSERIAL PRIMARY KEY,
            ts_ms BIGINT,
            symbol TEXT,
            sector TEXT,
            source_record_id TEXT NOT NULL,
            client_name TEXT,
            registrant_name TEXT,
            issue_area TEXT,
            filing_date TEXT,
            filing_ts_ms BIGINT,
            disclosure_date TEXT,
            disclosure_ts_ms BIGINT,
            availability_ts_ms BIGINT NOT NULL,
            amount_usd DOUBLE PRECISION,
            source_url TEXT,
            ingested_ts_ms BIGINT,
            payload_json JSONB,
            diagnostics_json JSONB
        )
        """
    )
    con.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_quiver_lobbying_filings_source_record_id ON quiver_lobbying_filings(source_record_id)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_quiver_lobbying_filings_symbol_avail ON quiver_lobbying_filings(symbol, availability_ts_ms DESC)")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS quiver_gov_contracts (
            id BIGSERIAL PRIMARY KEY,
            ts_ms BIGINT,
            symbol TEXT,
            sector TEXT,
            source_record_id TEXT NOT NULL,
            recipient_name TEXT,
            agency TEXT,
            contract_id TEXT,
            description TEXT,
            award_date TEXT,
            award_ts_ms BIGINT,
            disclosure_date TEXT,
            disclosure_ts_ms BIGINT,
            availability_ts_ms BIGINT NOT NULL,
            amount_usd DOUBLE PRECISION,
            source_url TEXT,
            ingested_ts_ms BIGINT,
            payload_json JSONB,
            diagnostics_json JSONB
        )
        """
    )
    con.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_quiver_gov_contracts_source_record_id ON quiver_gov_contracts(source_record_id)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_quiver_gov_contracts_symbol_avail ON quiver_gov_contracts(symbol, availability_ts_ms DESC)")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS gov_member_committee_map (
            member_name TEXT NOT NULL,
            committee TEXT NOT NULL,
            active BIGINT NOT NULL DEFAULT 1,
            updated_ts_ms BIGINT,
            meta_json JSONB,
            PRIMARY KEY(member_name, committee)
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS gov_committee_sector_map (
            committee TEXT NOT NULL,
            sector TEXT NOT NULL,
            weight DOUBLE PRECISION NOT NULL DEFAULT 1.0,
            active BIGINT NOT NULL DEFAULT 1,
            updated_ts_ms BIGINT,
            meta_json JSONB,
            PRIMARY KEY(committee, sector)
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS gov_member_leadership_map (
            member_name TEXT PRIMARY KEY,
            leadership_role TEXT,
            active BIGINT NOT NULL DEFAULT 1,
            updated_ts_ms BIGINT,
            meta_json JSONB
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS gov_symbol_sector_map (
            symbol TEXT PRIMARY KEY,
            sector TEXT,
            source TEXT,
            updated_ts_ms BIGINT,
            meta_json JSONB
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS gov_symbol_features (
            symbol TEXT NOT NULL,
            asof_ts_ms BIGINT NOT NULL,
            congress_committee_buy_30d DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            congress_leadership_trade_flag DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            congress_sale_signal_30d DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            lobbying_spend_z_yoy DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            gov_contract_award_z DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            source_max_availability_ts_ms BIGINT,
            created_ts_ms BIGINT,
            meta_json JSONB,
            PRIMARY KEY(symbol, asof_ts_ms)
        )
        """
    )


def _parse_json_env(name: str) -> Any:
    raw = str(os.environ.get(name) or "").strip()
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception as e:
        _warn_nonfatal("QUIVER_GOV_INVALID_JSON_ENV", e, name=str(name))
        return None


def seed_gov_conditioning_tables(con) -> Dict[str, int]:
    ensure_gov_tables(con)
    now_ms = utc_now_ms()
    counts = {"committee_sectors": 0, "member_committees": 0, "leadership": 0, "symbol_sectors": 0}
    for committee, sector, weight in DEFAULT_COMMITTEE_SECTOR_MAP:
        cur = con.execute(
            """
            INSERT INTO gov_committee_sector_map(committee, sector, weight, active, updated_ts_ms, meta_json)
            VALUES (?, ?, ?, 1, ?, ?)
            ON CONFLICT(committee, sector) DO NOTHING
            """,
            (committee, sector, float(weight), int(now_ms), _json_param(con, {"source": "default_committee_sector_map"})),
        )
        counts["committee_sectors"] += int(getattr(cur, "rowcount", 0) or 0)

    for item in _iter_mapping_items(_parse_json_env("GOV_MEMBER_COMMITTEES_JSON")):
        member = _clean_text(item.get("member") or item.get("member_name") or item.get("name"))
        committees = item.get("committees") or item.get("committee") or []
        if isinstance(committees, str):
            committees = [committees]
        for committee in committees:
            committee_name = _clean_text(committee)
            if not member or not committee_name:
                continue
            cur = con.execute(
                """
                INSERT INTO gov_member_committee_map(member_name, committee, active, updated_ts_ms, meta_json)
                VALUES (?, ?, 1, ?, ?)
                ON CONFLICT(member_name, committee) DO UPDATE SET
                  active = excluded.active,
                  updated_ts_ms = excluded.updated_ts_ms,
                  meta_json = excluded.meta_json
                """,
                (member, committee_name, int(now_ms), _json_param(con, {"source": "GOV_MEMBER_COMMITTEES_JSON"})),
            )
            counts["member_committees"] += int(getattr(cur, "rowcount", 0) or 0)

    leadership_payload = _parse_json_env("GOV_LEADERSHIP_MEMBERS_JSON")
    leadership_items = leadership_payload if isinstance(leadership_payload, list) else []
    for item in leadership_items:
        if isinstance(item, str):
            member = _clean_text(item)
            role = "leadership"
        elif isinstance(item, dict):
            member = _clean_text(item.get("member") or item.get("member_name") or item.get("name"))
            role = _clean_text(item.get("role") or item.get("leadership_role") or "leadership")
        else:
            continue
        if not member:
            continue
        cur = con.execute(
            """
            INSERT INTO gov_member_leadership_map(member_name, leadership_role, active, updated_ts_ms, meta_json)
            VALUES (?, ?, 1, ?, ?)
            ON CONFLICT(member_name) DO UPDATE SET
              leadership_role = excluded.leadership_role,
              active = excluded.active,
              updated_ts_ms = excluded.updated_ts_ms,
              meta_json = excluded.meta_json
            """,
            (member, role or "leadership", int(now_ms), _json_param(con, {"source": "GOV_LEADERSHIP_MEMBERS_JSON"})),
        )
        counts["leadership"] += int(getattr(cur, "rowcount", 0) or 0)

    sector_payload = _parse_json_env("GOV_SYMBOL_SECTORS_JSON")
    if isinstance(sector_payload, dict):
        sector_items = [{"symbol": key, "sector": value} for key, value in sector_payload.items()]
    else:
        sector_items = sector_payload if isinstance(sector_payload, list) else []
    for item in sector_items:
        if not isinstance(item, dict):
            continue
        symbol = _clean_symbol(item.get("symbol") or item.get("ticker"))
        sector = _clean_text(item.get("sector"))
        if not symbol or not sector:
            continue
        cur = con.execute(
            """
            INSERT INTO gov_symbol_sector_map(symbol, sector, source, updated_ts_ms, meta_json)
            VALUES (?, ?, 'env', ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET
              sector = excluded.sector,
              source = excluded.source,
              updated_ts_ms = excluded.updated_ts_ms,
              meta_json = excluded.meta_json
            """,
            (symbol, sector, int(now_ms), _json_param(con, {"source": "GOV_SYMBOL_SECTORS_JSON"})),
        )
        counts["symbol_sectors"] += int(getattr(cur, "rowcount", 0) or 0)
    return counts


def _iter_mapping_items(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [dict(item) for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        out: List[Dict[str, Any]] = []
        for key, value in payload.items():
            if isinstance(value, dict):
                item = dict(value)
                item.setdefault("member", key)
            else:
                item = {"member": key, "committees": value}
            out.append(item)
        return out
    return []


def normalize_quiver_congressional_record(raw: Mapping[str, Any], *, ingested_ts_ms: int | None = None) -> Dict[str, Any]:
    record = dict(raw or {})
    now_ms = int(ingested_ts_ms or utc_now_ms())
    member = _clean_text(_field(record, ["member", "member_name", "representative", "senator", "politician", "name", "Representative"]))
    symbol = _clean_symbol(_field(record, ["ticker", "symbol", "asset_ticker", "Ticker"]))
    transaction_raw = _field(record, ["transaction", "transaction_type", "type", "Transaction", "TransactionType"])
    transaction_type, direction = _normalize_direction(transaction_raw)
    amount_range = _clean_text(_field(record, ["amount", "amount_range", "range", "value", "Amount"]))
    amount_low, amount_high, amount_mid = _parse_amount_range(amount_range)
    transaction_date = _clean_text(_field(record, ["transaction_date", "transactionDate", "TransactionDate", "trade_date", "date"]))
    disclosure_date = _clean_text(
        _field(record, ["disclosure_date", "disclosureDate", "ReportDate", "report_date", "filed_date", "date_received"])
    )
    transaction_ts = parse_ts_ms(transaction_date)
    disclosure_ts = parse_ts_ms(disclosure_date)
    source_id = _clean_text(_field(record, ["id", "source_record_id", "transaction_id", "TransactionID", "ReportID"]))
    source_record_id = source_id or _source_record_id("quiver_congress", member, symbol, transaction_raw, transaction_date, disclosure_date, amount_range)
    dedupe_key = congressional_dedupe_key(
        member_name=member,
        symbol=symbol,
        transaction_date=transaction_date,
        transaction_ts_ms=transaction_ts,
    )
    availability_ts = int(disclosure_ts or now_ms)
    return {
        "ts_ms": int(availability_ts),
        "symbol": symbol or None,
        "source_record_id": str(source_record_id),
        "dedupe_key": dedupe_key,
        "member_name": member or None,
        "chamber": _clean_text(_field(record, ["chamber", "Chamber", "house", "body"])) or None,
        "party": _clean_text(_field(record, ["party", "Party"])) or None,
        "district": _clean_text(_field(record, ["district", "District", "state"])) or None,
        "transaction_type_raw": str(transaction_raw or "").strip() or None,
        "transaction_type": transaction_type,
        "direction": direction,
        "amount_range": amount_range or None,
        "amount_low": amount_low,
        "amount_high": amount_high,
        "amount_mid": amount_mid,
        "transaction_date": transaction_date or None,
        "transaction_ts_ms": transaction_ts,
        "disclosure_date": disclosure_date or None,
        "disclosure_ts_ms": disclosure_ts,
        "availability_ts_ms": int(availability_ts),
        "source_url": _clean_text(_field(record, ["url", "source_url", "URL"])) or None,
        "ingested_ts_ms": int(now_ms),
        "payload_json": record,
        "diagnostics_json": {
            "availability_rule": "disclosure_ts_ms_only_for_features",
            "availability_fallback": "ingested_ts_ms" if disclosure_ts is None else "",
        },
    }


def normalize_quiver_lobbying_record(raw: Mapping[str, Any], *, ingested_ts_ms: int | None = None) -> Dict[str, Any]:
    record = dict(raw or {})
    now_ms = int(ingested_ts_ms or utc_now_ms())
    symbol = _clean_symbol(_field(record, ["ticker", "symbol", "Ticker"]))
    sector = _clean_text(_field(record, ["sector", "Sector"]))
    disclosure_date = _clean_text(_field(record, ["disclosure_date", "filing_date", "report_date", "date", "Date", "FilingDate"]))
    disclosure_ts = parse_ts_ms(disclosure_date)
    amount = _safe_float(_field(record, ["amount", "amount_usd", "income", "spend", "Amount"]))
    client = _clean_text(_field(record, ["client", "client_name", "Client"]))
    registrant = _clean_text(_field(record, ["registrant", "registrant_name", "Registrant"]))
    issue_area = _clean_text(_field(record, ["issue", "issue_area", "Issue", "SpecificIssue"]))
    source_id = _clean_text(_field(record, ["id", "source_record_id", "filing_id", "FilingID"]))
    source_record_id = source_id or _source_record_id("quiver_lobbying", symbol, client, registrant, disclosure_date, amount)
    availability_ts = int(disclosure_ts or now_ms)
    return {
        "ts_ms": int(availability_ts),
        "symbol": symbol or None,
        "sector": sector or None,
        "source_record_id": str(source_record_id),
        "client_name": client or None,
        "registrant_name": registrant or None,
        "issue_area": issue_area or None,
        "filing_date": disclosure_date or None,
        "filing_ts_ms": disclosure_ts,
        "disclosure_date": disclosure_date or None,
        "disclosure_ts_ms": disclosure_ts,
        "availability_ts_ms": int(availability_ts),
        "amount_usd": amount,
        "source_url": _clean_text(_field(record, ["url", "source_url", "URL"])) or None,
        "ingested_ts_ms": int(now_ms),
        "payload_json": record,
        "diagnostics_json": {"availability_rule": "quiver_disclosure_or_publish_date"},
    }


def normalize_quiver_contract_record(raw: Mapping[str, Any], *, ingested_ts_ms: int | None = None) -> Dict[str, Any]:
    record = dict(raw or {})
    now_ms = int(ingested_ts_ms or utc_now_ms())
    symbol = _clean_symbol(_field(record, ["ticker", "symbol", "Ticker"]))
    sector = _clean_text(_field(record, ["sector", "Sector"]))
    award_date = _clean_text(_field(record, ["award_date", "date", "Date", "ActionDate"]))
    disclosure_date = _clean_text(_field(record, ["disclosure_date", "publish_date", "posted_date", "Date", "date"])) or award_date
    award_ts = parse_ts_ms(award_date)
    disclosure_ts = parse_ts_ms(disclosure_date)
    amount = _safe_float(_field(record, ["amount", "amount_usd", "obligated_amount", "AwardAmount", "value"]))
    recipient = _clean_text(_field(record, ["recipient", "recipient_name", "vendor", "RecipientName"]))
    agency = _clean_text(_field(record, ["agency", "department", "Agency"]))
    contract_id = _clean_text(_field(record, ["contract_id", "award_id", "AwardID", "id"]))
    source_record_id = contract_id or _source_record_id("quiver_contract", symbol, recipient, agency, award_date, amount)
    availability_ts = int(disclosure_ts or award_ts or now_ms)
    return {
        "ts_ms": int(availability_ts),
        "symbol": symbol or None,
        "sector": sector or None,
        "source_record_id": str(source_record_id),
        "recipient_name": recipient or None,
        "agency": agency or None,
        "contract_id": contract_id or None,
        "description": _clean_text(_field(record, ["description", "Description"])) or None,
        "award_date": award_date or None,
        "award_ts_ms": award_ts,
        "disclosure_date": disclosure_date or None,
        "disclosure_ts_ms": disclosure_ts,
        "availability_ts_ms": int(availability_ts),
        "amount_usd": amount,
        "source_url": _clean_text(_field(record, ["url", "source_url", "URL"])) or None,
        "ingested_ts_ms": int(now_ms),
        "payload_json": record,
        "diagnostics_json": {"availability_rule": "quiver_publish_date_fallback_to_award_date"},
    }


_CONGRESS_COLUMNS = (
    "ts_ms",
    "symbol",
    "source_record_id",
    "dedupe_key",
    "member_name",
    "chamber",
    "party",
    "district",
    "transaction_type_raw",
    "transaction_type",
    "direction",
    "amount_range",
    "amount_low",
    "amount_high",
    "amount_mid",
    "transaction_date",
    "transaction_ts_ms",
    "disclosure_date",
    "disclosure_ts_ms",
    "availability_ts_ms",
    "source_url",
    "ingested_ts_ms",
    "payload_json",
    "diagnostics_json",
)
_LOBBYING_COLUMNS = (
    "ts_ms",
    "symbol",
    "sector",
    "source_record_id",
    "client_name",
    "registrant_name",
    "issue_area",
    "filing_date",
    "filing_ts_ms",
    "disclosure_date",
    "disclosure_ts_ms",
    "availability_ts_ms",
    "amount_usd",
    "source_url",
    "ingested_ts_ms",
    "payload_json",
    "diagnostics_json",
)
_CONTRACT_COLUMNS = (
    "ts_ms",
    "symbol",
    "sector",
    "source_record_id",
    "recipient_name",
    "agency",
    "contract_id",
    "description",
    "award_date",
    "award_ts_ms",
    "disclosure_date",
    "disclosure_ts_ms",
    "availability_ts_ms",
    "amount_usd",
    "source_url",
    "ingested_ts_ms",
    "payload_json",
    "diagnostics_json",
)


def _upsert_row(con, table: str, row: Mapping[str, Any], columns: Sequence[str], conflict_column: str) -> int:
    values = [_json_param(con, row.get(column)) if column in {"payload_json", "diagnostics_json", "meta_json"} else row.get(column) for column in columns]
    assignments = ",\n          ".join(f"{column} = excluded.{column}" for column in columns if column != conflict_column)
    cur = con.execute(
        f"""
        INSERT INTO {table}({", ".join(columns)})
        VALUES ({", ".join(["?"] * len(columns))})
        ON CONFLICT({conflict_column}) DO UPDATE SET
          {assignments}
        """,
        tuple(values),
    )
    return int(getattr(cur, "rowcount", 0) or 0)


def put_quiver_congress_trade(row: Mapping[str, Any], *, con) -> int:
    ensure_gov_tables(con)
    return _upsert_row(con, "quiver_congressional_trades", row, _CONGRESS_COLUMNS, "source_record_id")


def put_quiver_lobbying_filing(row: Mapping[str, Any], *, con) -> int:
    ensure_gov_tables(con)
    return _upsert_row(con, "quiver_lobbying_filings", row, _LOBBYING_COLUMNS, "source_record_id")


def put_quiver_gov_contract(row: Mapping[str, Any], *, con) -> int:
    ensure_gov_tables(con)
    return _upsert_row(con, "quiver_gov_contracts", row, _CONTRACT_COLUMNS, "source_record_id")


def _rows_as_dicts(cur) -> List[Dict[str, Any]]:
    rows = cur.fetchall() or []
    if not rows:
        return []
    if hasattr(rows[0], "keys"):
        return [{str(key): row[key] for key in row.keys()} for row in rows]
    columns = [str(col[0]) for col in (cur.description or [])]
    return [dict(zip(columns, row)) for row in rows]


def existing_congressional_dedupe_keys(con, *, symbol: str, ts_ms: int, lookback_ms: int) -> set[str]:
    try:
        cur = con.execute(
            """
            SELECT politician_name, symbol, transaction_date, transaction_ts_ms
            FROM congressional_trades
            WHERE symbol = ?
              AND disclosure_ts_ms IS NOT NULL
              AND disclosure_ts_ms > 0
              AND disclosure_ts_ms <= ?
              AND disclosure_ts_ms >= ?
            """,
            (_clean_symbol(symbol), int(ts_ms), int(ts_ms) - int(lookback_ms)),
        )
    except Exception:
        return set()
    keys = set()
    for row in _rows_as_dicts(cur):
        key = congressional_dedupe_key(
            member_name=row.get("politician_name"),
            symbol=row.get("symbol"),
            transaction_date=row.get("transaction_date"),
            transaction_ts_ms=row.get("transaction_ts_ms"),
        )
        if key:
            keys.add(key)
    return keys


def is_duplicate_existing_congressional_trade(con, row: Mapping[str, Any]) -> bool:
    symbol = _clean_symbol((row or {}).get("symbol"))
    key = str((row or {}).get("dedupe_key") or congressional_dedupe_key(
        member_name=(row or {}).get("member_name"),
        symbol=symbol,
        transaction_date=(row or {}).get("transaction_date"),
        transaction_ts_ms=(row or {}).get("transaction_ts_ms"),
    ))
    if not symbol or not key:
        return False
    tx_ts = int((row or {}).get("transaction_ts_ms") or (row or {}).get("disclosure_ts_ms") or utc_now_ms())
    keys = existing_congressional_dedupe_keys(con, symbol=symbol, ts_ms=tx_ts + 366 * 24 * 3600 * 1000, lookback_ms=732 * 24 * 3600 * 1000)
    return key in keys


def _symbol_sector_from_reference(con, symbol: str) -> str:
    symbol_key = _clean_symbol(symbol)
    for table in ("security_master", "securities", "symbols"):
        try:
            row = con.execute(f"SELECT sector FROM {table} WHERE symbol = ? LIMIT 1", (symbol_key,)).fetchone()
        except Exception:
            row = None
        if row and _clean_text(row[0]):
            return _clean_text(row[0])
    return ""


def sector_for_symbol(con, symbol: str) -> str:
    ensure_gov_tables(con)
    symbol_key = _clean_symbol(symbol)
    if not symbol_key:
        return ""
    try:
        row = con.execute("SELECT sector FROM gov_symbol_sector_map WHERE symbol = ?", (symbol_key,)).fetchone()
    except Exception:
        row = None
    if row and _clean_text(row[0]):
        return _clean_text(row[0])
    return _symbol_sector_from_reference(con, symbol_key)


def member_committees(con, member_name: str) -> List[str]:
    ensure_gov_tables(con)
    member = _clean_text(member_name)
    if not member:
        return []
    try:
        rows = con.execute(
            """
            SELECT committee
            FROM gov_member_committee_map
            WHERE lower(member_name) = lower(?)
              AND active = 1
            ORDER BY committee ASC
            """,
            (member,),
        ).fetchall()
    except Exception:
        rows = []
    return [_clean_text(row[0]) for row in rows or [] if _clean_text(row[0])]


def is_leadership_member(con, member_name: str) -> bool:
    ensure_gov_tables(con)
    member = _clean_text(member_name)
    if not member:
        return False
    try:
        row = con.execute(
            """
            SELECT 1
            FROM gov_member_leadership_map
            WHERE lower(member_name) = lower(?)
              AND active = 1
            LIMIT 1
            """,
            (member,),
        ).fetchone()
    except Exception:
        row = None
    return bool(row)


def member_is_committee_relevant(con, *, member_name: str, symbol: str) -> bool:
    sector = _clean_text(sector_for_symbol(con, symbol)).lower()
    if not sector:
        return False
    committees = member_committees(con, member_name)
    if not committees:
        return False
    for committee in committees:
        try:
            row = con.execute(
                """
                SELECT 1
                FROM gov_committee_sector_map
                WHERE lower(committee) = lower(?)
                  AND lower(sector) = lower(?)
                  AND active = 1
                  AND weight > 0
                LIMIT 1
                """,
                (committee, sector),
            ).fetchone()
        except Exception:
            row = None
        if row:
            return True
    return False


def _load_congress_events(con, *, symbol: str, ts_ms: int) -> List[Dict[str, Any]]:
    symbol_key = _clean_symbol(symbol)
    window_start = int(ts_ms) - int(GOV_FEATURE_LOOKBACK_DAYS * 24 * 3600 * 1000)
    events: Dict[str, Dict[str, Any]] = {}

    try:
        cur = con.execute(
            """
            SELECT
              politician_name AS member_name,
              symbol,
              direction,
              transaction_type,
              amount_mid,
              transaction_date,
              transaction_ts_ms,
              disclosure_date,
              disclosure_ts_ms,
              source_trade_id AS source_record_id
            FROM congressional_trades
            WHERE symbol = ?
              AND disclosure_ts_ms IS NOT NULL
              AND disclosure_ts_ms > 0
              AND disclosure_ts_ms <= ?
              AND disclosure_ts_ms >= ?
            ORDER BY disclosure_ts_ms ASC
            """,
            (symbol_key, int(ts_ms), int(window_start)),
        )
        for row in _rows_as_dicts(cur):
            key = congressional_dedupe_key(
                member_name=row.get("member_name"),
                symbol=row.get("symbol"),
                transaction_date=row.get("transaction_date"),
                transaction_ts_ms=row.get("transaction_ts_ms"),
            )
            row["dedupe_key"] = key
            row["source"] = "fallback_congressional_trades"
            events[key] = row
    # system-audit: ignore[silent_except] legacy congressional table may be absent in newer deployments.
    except Exception:
        pass

    try:
        cur = con.execute(
            """
            SELECT
              member_name,
              symbol,
              direction,
              transaction_type,
              amount_mid,
              transaction_date,
              transaction_ts_ms,
              disclosure_date,
              disclosure_ts_ms,
              source_record_id,
              dedupe_key
            FROM quiver_congressional_trades
            WHERE symbol = ?
              AND disclosure_ts_ms IS NOT NULL
              AND disclosure_ts_ms > 0
              AND disclosure_ts_ms <= ?
              AND disclosure_ts_ms >= ?
            ORDER BY disclosure_ts_ms ASC
            """,
            (symbol_key, int(ts_ms), int(window_start)),
        )
        for row in _rows_as_dicts(cur):
            key = str(row.get("dedupe_key") or congressional_dedupe_key(
                member_name=row.get("member_name"),
                symbol=row.get("symbol"),
                transaction_date=row.get("transaction_date"),
                transaction_ts_ms=row.get("transaction_ts_ms"),
            ))
            row["dedupe_key"] = key
            row["source"] = "quiver"
            events[key] = row
    # system-audit: ignore[silent_except] quiver congressional table is optional during migration.
    except Exception:
        pass

    out = list(events.values())
    out.sort(key=lambda row: (int(row.get("disclosure_ts_ms") or 0), str(row.get("dedupe_key") or "")))
    return out


def _amount_scale(amount_mid: Any) -> float:
    amount = _safe_float(amount_mid)
    if amount is None or amount <= 0.0:
        return 1.0
    return float(max(0.01, min(100.0, amount / 100_000.0)))


def _clip(value: float, lo: float = -10.0, hi: float = 10.0) -> float:
    return float(max(float(lo), min(float(hi), float(value))))


def _yoy_change_z(rows: Sequence[Mapping[str, Any]], *, ts_ms: int) -> Tuple[float, int | None]:
    one_year = 365 * 24 * 3600 * 1000
    current_start = int(ts_ms) - one_year
    previous_start = int(ts_ms) - 2 * one_year
    cur_total = 0.0
    prev_total = 0.0
    latest = 0
    for row in rows or []:
        avail = int((row or {}).get("availability_ts_ms") or 0)
        if avail <= 0 or avail > int(ts_ms):
            continue
        latest = max(latest, avail)
        amount = float(_safe_float((row or {}).get("amount_usd")) or 0.0)
        if avail >= current_start:
            cur_total += amount
        elif avail >= previous_start:
            prev_total += amount
    denom = max(1.0, abs(prev_total) * 0.25, math.sqrt(max(cur_total + prev_total, 0.0)))
    return _clip((cur_total - prev_total) / denom), (int(latest) if latest > 0 else None)


def _load_amount_rows(con, *, table: str, symbol: str, sector: str, ts_ms: int) -> List[Dict[str, Any]]:
    symbol_key = _clean_symbol(symbol)
    sector_key = _clean_text(sector).lower()
    start = int(ts_ms) - int(2 * 365 * 24 * 3600 * 1000)
    try:
        cur = con.execute(
            f"""
            SELECT symbol, sector, amount_usd, availability_ts_ms
            FROM {table}
            WHERE availability_ts_ms <= ?
              AND availability_ts_ms >= ?
              AND (
                symbol = ?
                OR (? <> '' AND lower(COALESCE(sector, '')) = ?)
              )
            ORDER BY availability_ts_ms ASC
            """,
            (int(ts_ms), int(start), symbol_key, sector_key, sector_key),
        )
    except Exception:
        return []
    return _rows_as_dicts(cur)


def resolve_gov_features(con, *, symbol: str, ts_ms: int) -> Tuple[Dict[str, float], Dict[str, Any], bool]:
    ensure_gov_tables(con)
    symbol_key = _clean_symbol(symbol)
    features = {fid: 0.0 for fid in GOV_FEATURE_IDS}
    if not symbol_key:
        return features, {"latest_availability_ts_ms": None, "sector": ""}, False

    window_start = int(ts_ms) - int(30 * 24 * 3600 * 1000)
    events = _load_congress_events(con, symbol=symbol_key, ts_ms=int(ts_ms))
    latest_availability = max([int(row.get("disclosure_ts_ms") or 0) for row in events] or [0])

    for row in events:
        disclosure_ts = int(row.get("disclosure_ts_ms") or 0)
        if disclosure_ts < window_start:
            continue
        direction = str(row.get("direction") or "").strip().lower()
        member = _clean_text(row.get("member_name"))
        amount_scale = _amount_scale(row.get("amount_mid"))
        if direction == "buy" and member_is_committee_relevant(con, member_name=member, symbol=symbol_key):
            features["congress_committee_buy_30d"] += float(amount_scale)
        if direction == "sell":
            features["congress_sale_signal_30d"] += float(amount_scale)
        if is_leadership_member(con, member):
            features["congress_leadership_trade_flag"] = 1.0

    sector = sector_for_symbol(con, symbol_key)
    lobbying_rows = _load_amount_rows(con, table="quiver_lobbying_filings", symbol=symbol_key, sector=sector, ts_ms=int(ts_ms))
    lobbying_z, lobbying_latest = _yoy_change_z(lobbying_rows, ts_ms=int(ts_ms))
    contract_rows = _load_amount_rows(con, table="quiver_gov_contracts", symbol=symbol_key, sector=sector, ts_ms=int(ts_ms))
    contract_z, contract_latest = _yoy_change_z(contract_rows, ts_ms=int(ts_ms))
    features["lobbying_spend_z_yoy"] = float(lobbying_z)
    features["gov_contract_award_z"] = float(contract_z)
    latest_values = [latest_availability, int(lobbying_latest or 0), int(contract_latest or 0)]
    latest = max(latest_values or [0])
    return (
        {key: float(value or 0.0) for key, value in features.items()},
        {
            "latest_availability_ts_ms": int(latest) if latest > 0 else None,
            "latest_disclosure_ts_ms": int(latest_availability) if latest_availability > 0 else None,
            "sector": str(sector or ""),
            "congress_event_count": int(len(events)),
            "lobbying_rows": int(len(lobbying_rows)),
            "contract_rows": int(len(contract_rows)),
        },
        bool(latest > 0),
    )


def materialize_gov_symbol_features(con, *, symbol: str, ts_ms: int) -> Dict[str, Any]:
    ensure_gov_tables(con)
    features, meta, available = resolve_gov_features(con, symbol=symbol, ts_ms=int(ts_ms))
    con.execute(
        """
        INSERT INTO gov_symbol_features(
          symbol, asof_ts_ms,
          congress_committee_buy_30d,
          congress_leadership_trade_flag,
          congress_sale_signal_30d,
          lobbying_spend_z_yoy,
          gov_contract_award_z,
          source_max_availability_ts_ms,
          created_ts_ms,
          meta_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(symbol, asof_ts_ms) DO UPDATE SET
          congress_committee_buy_30d = excluded.congress_committee_buy_30d,
          congress_leadership_trade_flag = excluded.congress_leadership_trade_flag,
          congress_sale_signal_30d = excluded.congress_sale_signal_30d,
          lobbying_spend_z_yoy = excluded.lobbying_spend_z_yoy,
          gov_contract_award_z = excluded.gov_contract_award_z,
          source_max_availability_ts_ms = excluded.source_max_availability_ts_ms,
          created_ts_ms = excluded.created_ts_ms,
          meta_json = excluded.meta_json
        """,
        (
            _clean_symbol(symbol),
            int(ts_ms),
            float(features["congress_committee_buy_30d"]),
            float(features["congress_leadership_trade_flag"]),
            float(features["congress_sale_signal_30d"]),
            float(features["lobbying_spend_z_yoy"]),
            float(features["gov_contract_award_z"]),
            meta.get("latest_availability_ts_ms"),
            int(utc_now_ms()),
            _json_param(con, dict(meta or {})),
        ),
    )
    return {"features": dict(features), "meta": dict(meta), "available": bool(available)}


def ingest_quiver_gov_batch(*, client: QuiverClient | None = None) -> Dict[str, Any]:
    quiver = client or QuiverClient()
    if not quiver.enabled:
        return {
            "ok": True,
            "blocked": True,
            "blocker": "missing_quiver_api_key",
            "errors": ["QUIVER_API_KEY is not configured"],
            "congress_rows": 0,
            "lobbying_rows": 0,
            "contract_rows": 0,
            "written": 0,
            "last_ingested_ts_ms": int(utc_now_ms()),
        }

    errors: List[str] = []
    now_ms = utc_now_ms()
    congress_raw: List[Dict[str, Any]] = []
    lobbying_raw: List[Dict[str, Any]] = []
    contract_raw: List[Dict[str, Any]] = []
    for label, fetcher in (
        ("congressional", quiver.fetch_congressional_trading),
        ("lobbying", quiver.fetch_lobbying),
        ("contracts", quiver.fetch_gov_contracts),
    ):
        try:
            rows = fetcher()
            if label == "congressional":
                congress_raw = rows
            elif label == "lobbying":
                lobbying_raw = rows
            else:
                contract_raw = rows
        except Exception as exc:
            errors.append(f"{label}:{exc}")
            _warn_nonfatal("QUIVER_GOV_FETCH_FAILED", exc, label=str(label))

    congress_rows = [normalize_quiver_congressional_record(row, ingested_ts_ms=now_ms) for row in congress_raw]
    lobbying_rows = [normalize_quiver_lobbying_record(row, ingested_ts_ms=now_ms) for row in lobbying_raw]
    contract_rows = [normalize_quiver_contract_record(row, ingested_ts_ms=now_ms) for row in contract_raw]

    def _write(con) -> int:
        ensure_gov_tables(con)
        seed_gov_conditioning_tables(con)
        written = 0
        for row in congress_rows:
            written += put_quiver_congress_trade(row, con=con)
        for row in lobbying_rows:
            written += put_quiver_lobbying_filing(row, con=con)
        for row in contract_rows:
            written += put_quiver_gov_contract(row, con=con)
        return int(written)

    written = int(run_write_txn(_write, table="quiver_congressional_trades", operation="ingest_quiver_gov_batch") or 0)
    latest = max(
        [int(row.get("availability_ts_ms") or 0) for row in list(congress_rows) + list(lobbying_rows) + list(contract_rows)] or [now_ms]
    )
    return {
        "ok": not bool(errors),
        "blocked": False,
        "errors": errors,
        "congress_rows": int(len(congress_rows)),
        "lobbying_rows": int(len(lobbying_rows)),
        "contract_rows": int(len(contract_rows)),
        "written": int(written),
        "last_ingested_ts_ms": int(latest or now_ms),
    }


__all__ = [
    "GOV_FEATURE_IDS",
    "QuiverClient",
    "congressional_dedupe_key",
    "ensure_gov_tables",
    "existing_congressional_dedupe_keys",
    "ingest_quiver_gov_batch",
    "is_duplicate_existing_congressional_trade",
    "is_leadership_member",
    "member_is_committee_relevant",
    "normalize_quiver_congressional_record",
    "normalize_quiver_contract_record",
    "normalize_quiver_lobbying_record",
    "parse_ts_ms",
    "put_quiver_congress_trade",
    "put_quiver_gov_contract",
    "put_quiver_lobbying_filing",
    "resolve_gov_features",
    "seed_gov_conditioning_tables",
]
