"""SEC 13F institutional holdings ingestion and point-in-time overlay features.

README:
- Source: SEC EDGAR 13F-HR / 13F-HR/A filings for configured investment
  managers. The ingestion fetches the EDGAR submissions JSON and filing
  archive index, then parses the information-table XML.
- Cadence: the source is quarterly; the supervised job polls once per day by
  default during the filing window via ``INST_13F_POLL_SECONDS`` so filings that
  trickle in before the 45-day deadline become available promptly.
- Availability lag: features join on each filing's EDGAR acceptance timestamp
  (``availability_ts_ms``). Quarter-end/report dates are never used as the
  availability time.
- Caveats: CUSIP-to-symbol mapping depends on local/security-master mappings
  when present, with Polygon/FMP reference lookups as optional fallbacks. Rows
  that cannot be mapped are still stored with ``mapping_status='unmapped'`` for
  review. This is a low-frequency manager overlay, not standalone alpha.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timezone
from typing import Any, Dict, Iterable, List, Mapping, Sequence, Tuple
from xml.etree import ElementTree as ET

import requests

from engine.data._credentials import get_data_credential
from engine.data.sec import edgar_live
from engine.runtime.storage import connect, run_write_txn

INST_13F_FEATURE_IDS = [
    "13f_consensus_holders",
    "13f_conviction_max",
    "13f_new_position_flag",
    "13f_add_flag",
]

SEC_ARCHIVES_URL = "https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession_nodash}/{document}"
SEC_ARCHIVE_INDEX_URL = "https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession_nodash}/index.json"
POLYGON_CUSIP_URL = "https://api.polygon.io/v3/reference/tickers"
FMP_CUSIP_URL = "https://financialmodelingprep.com/stable/cusip-mapper"
REQUEST_TIMEOUT_S = float(os.environ.get("INST_13F_REQUEST_TIMEOUT_S", "20"))
TURNOVER_THRESHOLD = float(os.environ.get("INST_13F_TURNOVER_THRESHOLD", "0.25"))
FILING_LIMIT = max(1, int(os.environ.get("INST_13F_FILING_LIMIT", "12")))

_UTC = timezone.utc


@dataclass(frozen=True)
class Manager13F:
    cik: str
    name: str


DEFAULT_13F_MANAGERS: Tuple[Manager13F, ...] = (
    Manager13F("0001067983", "Berkshire Hathaway Inc"),
    Manager13F("0001336528", "Pershing Square Capital Management"),
    Manager13F("0001061768", "Baupost Group"),
    Manager13F("0001079114", "Greenlight Capital"),
    Manager13F("0001006438", "Appaloosa"),
    Manager13F("0001061165", "Lone Pine Capital"),
    Manager13F("0001167483", "Tiger Global Management"),
    Manager13F("0001350694", "Bridgewater Associates"),
    Manager13F("0001037389", "Renaissance Technologies"),
    Manager13F("0001009207", "D. E. Shaw"),
    Manager13F("0001647251", "TCI Fund Management"),
    Manager13F("0001569205", "Fundsmith"),
    Manager13F("0001112520", "Akre Capital Management"),
    Manager13F("0001056594", "Ruane, Cunniff & Goldfarb"),
    Manager13F("0001088439", "Gardner Russo & Quinn"),
    Manager13F("0001035312", "Polen Capital Management"),
    Manager13F("0001081019", "Tweedy, Browne"),
    Manager13F("0001422849", "Capital World Investors"),
    Manager13F("000102909", "Sands Capital Management"),
    Manager13F("0001179392", "Maverick Capital"),
)


def utc_now_ms() -> int:
    return int(time.time() * 1000)


def _clean_symbol(value: Any) -> str:
    return str(value or "").upper().strip().replace(".", "-")


def _clean_cik(value: Any) -> str:
    text = re.sub(r"\D+", "", str(value or ""))
    if not text:
        return ""
    return text.zfill(10)


def _clean_cusip(value: Any) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "", str(value or "")).upper().strip()


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
    return datetime.fromisoformat(text.replace("Z", "+00:00")).date()


def parse_ts_ms(value: Any) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return int(datetime.combine(parse_date(text), dt_time.min, tzinfo=_UTC).timestamp() * 1000)
    normalized = text.replace("Z", "+00:00")
    if re.fullmatch(r"\d{8}", text):
        return int(datetime.strptime(text, "%Y%m%d").replace(tzinfo=_UTC).timestamp() * 1000)
    try:
        return int(datetime.fromisoformat(normalized).astimezone(_UTC).timestamp() * 1000)
    except Exception:
        return None


def date_to_ms(day: date | str) -> int:
    parsed = parse_date(day) if not isinstance(day, date) else day
    return int(datetime.combine(parsed, dt_time.min, tzinfo=_UTC).timestamp() * 1000)


def _source_record_id(*parts: Any) -> str:
    digest = hashlib.sha256("|".join(str(part or "") for part in parts).encode("utf-8", "ignore")).hexdigest()[:24]
    return f"13f:{digest}"


def _local_name(tag: Any) -> str:
    return str(tag or "").split("}", 1)[-1]


def _child(node: ET.Element | None, name: str) -> ET.Element | None:
    if node is None:
        return None
    for child in list(node):
        if _local_name(child.tag).lower() == str(name).lower():
            return child
    return None


def _children(node: ET.Element | None, name: str) -> List[ET.Element]:
    if node is None:
        return []
    return [child for child in list(node) if _local_name(child.tag).lower() == str(name).lower()]


def _text(node: ET.Element | None, *path: str) -> str:
    cur = node
    for part in path:
        cur = _child(cur, part)
        if cur is None:
            return ""
    return str(cur.text or "").strip()


def _manager_specs_from_env() -> List[Manager13F]:
    raw = str(os.environ.get("INST_13F_MANAGERS_JSON") or "").strip()
    if not raw:
        return []
    try:
        payload = json.loads(raw)
    except Exception:
        return []
    out: List[Manager13F] = []
    items = payload if isinstance(payload, list) else []
    for item in items:
        if not isinstance(item, dict):
            continue
        cik = _clean_cik(item.get("cik"))
        name = str(item.get("name") or item.get("manager_name") or cik).strip()
        if cik:
            out.append(Manager13F(cik, name or cik))
    return out


def load_13f_managers() -> List[Manager13F]:
    return list(_manager_specs_from_env() or DEFAULT_13F_MANAGERS)


def ensure_13f_tables(con) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS inst_13f_manager_universe (
            manager_cik TEXT PRIMARY KEY,
            manager_name TEXT,
            active BIGINT NOT NULL DEFAULT 1,
            turnover_threshold DOUBLE PRECISION,
            source TEXT,
            updated_ts_ms BIGINT,
            meta_json JSONB
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS inst_13f_filings (
            id BIGSERIAL PRIMARY KEY,
            ts_ms BIGINT,
            manager_cik TEXT NOT NULL,
            manager_name TEXT,
            accession TEXT NOT NULL,
            form TEXT,
            filing_date TEXT,
            report_date TEXT,
            report_ts_ms BIGINT,
            acceptance_datetime TEXT,
            acceptance_ts_ms BIGINT NOT NULL,
            availability_ts_ms BIGINT NOT NULL,
            primary_doc_url TEXT,
            info_table_url TEXT,
            total_value_usd DOUBLE PRECISION,
            holdings_count BIGINT,
            source_record_id TEXT NOT NULL,
            ingested_ts_ms BIGINT,
            payload_json JSONB,
            diagnostics_json JSONB
        )
        """
    )
    con.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_inst_13f_filings_source_record_id
          ON inst_13f_filings(source_record_id)
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_inst_13f_filings_manager_avail
          ON inst_13f_filings(manager_cik, availability_ts_ms DESC)
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS inst_13f_holdings (
            id BIGSERIAL PRIMARY KEY,
            ts_ms BIGINT,
            manager_cik TEXT NOT NULL,
            manager_name TEXT,
            accession TEXT NOT NULL,
            report_date TEXT,
            report_ts_ms BIGINT,
            availability_ts_ms BIGINT NOT NULL,
            issuer_name TEXT,
            title_of_class TEXT,
            cusip TEXT,
            value_usd DOUBLE PRECISION,
            value_thousands DOUBLE PRECISION,
            shares DOUBLE PRECISION,
            share_type TEXT,
            put_call TEXT,
            investment_discretion TEXT,
            voting_sole DOUBLE PRECISION,
            voting_shared DOUBLE PRECISION,
            voting_none DOUBLE PRECISION,
            symbol TEXT,
            mapping_status TEXT,
            source_record_id TEXT NOT NULL,
            ingested_ts_ms BIGINT,
            payload_json JSONB,
            diagnostics_json JSONB
        )
        """
    )
    con.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_inst_13f_holdings_source_record_id
          ON inst_13f_holdings(source_record_id)
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_inst_13f_holdings_symbol_avail
          ON inst_13f_holdings(symbol, availability_ts_ms DESC)
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_inst_13f_holdings_manager_report
          ON inst_13f_holdings(manager_cik, report_ts_ms DESC)
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS inst_13f_cusip_symbol_map (
            cusip TEXT PRIMARY KEY,
            symbol TEXT,
            source TEXT,
            confidence DOUBLE PRECISION,
            updated_ts_ms BIGINT,
            payload_json JSONB
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS inst_13f_symbol_features (
            symbol TEXT NOT NULL,
            asof_ts_ms BIGINT NOT NULL,
            "13f_consensus_holders" DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            "13f_conviction_max" DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            "13f_new_position_flag" DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            "13f_add_flag" DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            source_max_availability_ts_ms BIGINT,
            created_ts_ms BIGINT,
            meta_json JSONB,
            PRIMARY KEY(symbol, asof_ts_ms)
        )
        """
    )


def seed_default_13f_managers(con) -> int:
    ensure_13f_tables(con)
    now_ms = utc_now_ms()
    written = 0
    for manager in load_13f_managers():
        cur = con.execute(
            """
            INSERT INTO inst_13f_manager_universe(
              manager_cik, manager_name, active, turnover_threshold, source, updated_ts_ms, meta_json
            ) VALUES (?, ?, 1, ?, 'default', ?, ?)
            ON CONFLICT(manager_cik) DO NOTHING
            """,
            (
                _clean_cik(manager.cik),
                str(manager.name),
                float(TURNOVER_THRESHOLD),
                int(now_ms),
                _json_param(con, {"seed": "DEFAULT_13F_MANAGERS"}),
            ),
        )
        written += int(getattr(cur, "rowcount", 0) or 0)
    return int(written)


def configured_13f_managers(con=None) -> List[Manager13F]:
    if con is None:
        return load_13f_managers()
    try:
        ensure_13f_tables(con)
        seed_default_13f_managers(con)
        rows = con.execute(
            """
            SELECT manager_cik, manager_name
            FROM inst_13f_manager_universe
            WHERE active = 1
            ORDER BY manager_cik ASC
            """
        ).fetchall()
    except Exception:
        rows = []
    out = [Manager13F(_clean_cik(row[0]), str(row[1] or row[0])) for row in rows or [] if _clean_cik(row[0])]
    return out or load_13f_managers()


def parse_13f_information_table(xml_text: str) -> List[Dict[str, Any]]:
    root = ET.fromstring(str(xml_text or "").encode("utf-8"))
    info_nodes = [node for node in root.iter() if _local_name(node.tag).lower() == "infotable"]
    rows: List[Dict[str, Any]] = []
    for idx, node in enumerate(info_nodes):
        value_thousands = _safe_float(_text(node, "value"))
        shares = _safe_float(_text(node, "shrsOrPrnAmt", "sshPrnamt"))
        row = {
            "issuer_name": _text(node, "nameOfIssuer"),
            "title_of_class": _text(node, "titleOfClass"),
            "cusip": _clean_cusip(_text(node, "cusip")),
            "value_thousands": value_thousands,
            "value_usd": (float(value_thousands) * 1000.0) if value_thousands is not None else None,
            "shares": shares,
            "share_type": _text(node, "shrsOrPrnAmt", "sshPrnamtType"),
            "put_call": _text(node, "putCall"),
            "investment_discretion": _text(node, "investmentDiscretion"),
            "voting_sole": _safe_float(_text(node, "votingAuthority", "Sole")),
            "voting_shared": _safe_float(_text(node, "votingAuthority", "Shared")),
            "voting_none": _safe_float(_text(node, "votingAuthority", "None")),
            "row_number": int(idx),
        }
        if row["cusip"] or row["issuer_name"]:
            rows.append(row)
    return rows


def _archive_url(cik: str, accession: str, document: str) -> str:
    return SEC_ARCHIVES_URL.format(
        cik_int=int(_clean_cik(cik) or "0"),
        accession_nodash=str(accession or "").replace("-", ""),
        document=str(document or ""),
    )


def fetch_manager_13f_filings(manager: Manager13F, *, limit: int | None = None) -> Tuple[List[Dict[str, Any]], List[str]]:
    cik = _clean_cik(manager.cik)
    if not cik:
        return [], ["missing_cik"]
    errors: List[str] = []
    try:
        response = requests.get(edgar_live.SUBMISSIONS_URL.format(cik=cik), headers=edgar_live.HEADERS, timeout=REQUEST_TIMEOUT_S)
        response.raise_for_status()
        payload = response.json() or {}
    except Exception as exc:
        return [], [f"{cik}:submissions:{exc}"]

    recent = (payload.get("filings") or {}).get("recent") or {}
    forms = list(recent.get("form") or [])
    accessions = list(recent.get("accessionNumber") or [])
    filing_dates = list(recent.get("filingDate") or [])
    report_dates = list(recent.get("reportDate") or [])
    acceptances = list(recent.get("acceptanceDateTime") or [])
    primary_docs = list(recent.get("primaryDocument") or [])
    company_name = str(payload.get("name") or manager.name or "")

    out: List[Dict[str, Any]] = []
    max_len = min(len(forms), len(accessions), len(primary_docs))
    for idx in range(max_len):
        form = str(forms[idx] or "").upper().strip()
        if form not in {"13F-HR", "13F-HR/A"}:
            continue
        accession = str(accessions[idx] or "").strip()
        acceptance_raw = str(acceptances[idx] or "").strip() if idx < len(acceptances) else ""
        acceptance_ts = parse_ts_ms(acceptance_raw)
        filing_date = str(filing_dates[idx] or "").strip() if idx < len(filing_dates) else ""
        if acceptance_ts is None:
            acceptance_ts = parse_ts_ms(filing_date) or utc_now_ms()
        primary = str(primary_docs[idx] or "").strip()
        report_date = str(report_dates[idx] or "").strip() if idx < len(report_dates) else ""
        out.append(
            {
                "manager_cik": cik,
                "manager_name": company_name,
                "accession": accession,
                "form": form,
                "filing_date": filing_date,
                "report_date": report_date,
                "report_ts_ms": date_to_ms(report_date) if report_date else None,
                "acceptance_datetime": acceptance_raw,
                "acceptance_ts_ms": int(acceptance_ts),
                "availability_ts_ms": int(acceptance_ts),
                "primary_doc_url": _archive_url(cik, accession, primary) if primary else "",
                "primary_document": primary,
                "source_record_id": _source_record_id("filing", cik, accession),
                "payload_json": {"submissions_name": company_name, "recent_index": idx},
            }
        )
        if len(out) >= int(limit or FILING_LIMIT):
            break
    return out, errors


def fetch_filing_index_documents(cik: str, accession: str) -> Tuple[List[Dict[str, Any]], List[str]]:
    url = SEC_ARCHIVE_INDEX_URL.format(cik_int=int(_clean_cik(cik) or "0"), accession_nodash=str(accession or "").replace("-", ""))
    try:
        response = requests.get(url, headers=edgar_live.HEADERS, timeout=REQUEST_TIMEOUT_S)
        response.raise_for_status()
        payload = response.json() or {}
    except Exception as exc:
        return [], [f"index:{accession}:{exc}"]
    items = (((payload.get("directory") or {}).get("item")) or []) if isinstance(payload, dict) else []
    rows = [dict(item) for item in items if isinstance(item, dict)]
    return rows, []


def choose_information_table_document(index_docs: Sequence[Mapping[str, Any]], *, primary_document: str = "") -> str:
    xml_docs: List[str] = []
    for item in index_docs or []:
        name = str((item or {}).get("name") or "").strip()
        if not name:
            continue
        lowered = name.lower()
        if lowered.endswith(".xml"):
            xml_docs.append(name)
        if "infotable" in lowered or "informationtable" in lowered:
            return name
    for name in xml_docs:
        lowered = name.lower()
        if not lowered.startswith("primary"):
            return name
    return str(primary_document or (xml_docs[0] if xml_docs else "")).strip()


def fetch_information_table_xml(filing: Mapping[str, Any]) -> Tuple[str, str, List[str]]:
    cik = str((filing or {}).get("manager_cik") or "")
    accession = str((filing or {}).get("accession") or "")
    docs, errors = fetch_filing_index_documents(cik, accession)
    document = choose_information_table_document(docs, primary_document=str((filing or {}).get("primary_document") or ""))
    if not document:
        return "", "", errors + [f"{accession}:missing_information_table_document"]
    url = _archive_url(cik, accession, document)
    try:
        response = requests.get(url, headers=edgar_live.HEADERS, timeout=REQUEST_TIMEOUT_S)
        response.raise_for_status()
        return response.text or "", url, errors
    except Exception as exc:
        return "", url, errors + [f"{accession}:information_table:{exc}"]


def _row_dict(row: Any, columns: Sequence[str] | None = None) -> Dict[str, Any]:
    if hasattr(row, "keys"):
        return {str(key): row[key] for key in row.keys()}
    if columns:
        return {str(col): row[idx] for idx, col in enumerate(columns) if idx < len(row)}
    return {}


def _lookup_local_cusip_map(con, cusip: str) -> str:
    key = _clean_cusip(cusip)
    if not key:
        return ""
    try:
        row = con.execute("SELECT symbol FROM inst_13f_cusip_symbol_map WHERE cusip = ?", (key,)).fetchone()
        if row and _clean_symbol(row[0]):
            return _clean_symbol(row[0])
    # system-audit: ignore[silent_except] missing optional CUSIP map falls back to legacy symbol tables.
    except Exception:
        pass
    for table_name in ("security_master", "securities", "symbols"):
        try:
            row = con.execute(f"SELECT symbol FROM {table_name} WHERE cusip = ? LIMIT 1", (key,)).fetchone()
        except Exception:
            row = None
        if row and _clean_symbol(row[0]):
            return _clean_symbol(row[0])
    return ""


def _put_cusip_map(con, *, cusip: str, symbol: str, source: str, confidence: float, payload: Any = None) -> None:
    key = _clean_cusip(cusip)
    sym = _clean_symbol(symbol)
    if not key or not sym:
        return
    con.execute(
        """
        INSERT INTO inst_13f_cusip_symbol_map(cusip, symbol, source, confidence, updated_ts_ms, payload_json)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(cusip) DO UPDATE SET
          symbol = excluded.symbol,
          source = excluded.source,
          confidence = excluded.confidence,
          updated_ts_ms = excluded.updated_ts_ms,
          payload_json = excluded.payload_json
        """,
        (key, sym, str(source), float(confidence), int(utc_now_ms()), _json_param(con, payload or {})),
    )


def fetch_polygon_symbol_for_cusip(cusip: str) -> Tuple[str, Dict[str, Any]]:
    api_key = get_data_credential("POLYGON_API_KEY")
    if not api_key:
        return "", {"error": "missing_polygon_api_key"}
    response = requests.get(
        POLYGON_CUSIP_URL,
        params={"cusip": _clean_cusip(cusip), "market": "stocks", "active": "true", "apiKey": api_key},
        timeout=float(REQUEST_TIMEOUT_S),
    )
    response.raise_for_status()
    payload = response.json() or {}
    results = payload.get("results") if isinstance(payload, dict) else []
    row = results[0] if isinstance(results, list) and results else {}
    return _clean_symbol((row or {}).get("ticker")), dict(payload) if isinstance(payload, dict) else {"payload": payload}


def fetch_fmp_symbol_for_cusip(cusip: str) -> Tuple[str, Dict[str, Any]]:
    api_key = get_data_credential("FMP_API_KEY")
    if not api_key:
        return "", {"error": "missing_fmp_api_key"}
    response = requests.get(
        FMP_CUSIP_URL,
        params={"cusip": _clean_cusip(cusip), "apikey": api_key},
        timeout=float(REQUEST_TIMEOUT_S),
    )
    response.raise_for_status()
    payload = response.json()
    row = payload[0] if isinstance(payload, list) and payload else payload
    if not isinstance(row, dict):
        row = {}
    return _clean_symbol(row.get("symbol") or row.get("ticker")), {"payload": payload}


def resolve_cusip_symbol(con, cusip: str) -> Tuple[str, str, Dict[str, Any]]:
    local = _lookup_local_cusip_map(con, cusip)
    if local:
        return local, "mapped_local", {"source": "local"}
    errors: List[str] = []
    for source, fn in (("polygon", fetch_polygon_symbol_for_cusip), ("fmp", fetch_fmp_symbol_for_cusip)):
        try:
            symbol, payload = fn(cusip)
        except Exception as exc:
            errors.append(f"{source}:{exc}")
            continue
        if symbol:
            _put_cusip_map(con, cusip=cusip, symbol=symbol, source=source, confidence=0.8, payload=payload)
            return symbol, f"mapped_{source}", {"source": source, "payload": payload}
        if payload:
            errors.append(f"{source}:{payload.get('error') or 'unmapped'}")
    return "", "unmapped", {"errors": errors}


def normalize_filing_with_holdings(
    filing: Mapping[str, Any],
    holdings: Sequence[Mapping[str, Any]],
    *,
    con=None,
    ingested_ts_ms: int | None = None,
    info_table_url: str = "",
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    manager_cik = _clean_cik((filing or {}).get("manager_cik"))
    accession = str((filing or {}).get("accession") or "").strip()
    availability_ts_ms = int((filing or {}).get("availability_ts_ms") or (filing or {}).get("acceptance_ts_ms") or utc_now_ms())
    report_date = str((filing or {}).get("report_date") or "").strip()
    report_ts_ms = int((filing or {}).get("report_ts_ms") or (date_to_ms(report_date) if report_date else 0) or 0) or None
    now_ms = int(ingested_ts_ms or utc_now_ms())
    normalized_holdings: List[Dict[str, Any]] = []
    total_value_usd = 0.0
    for idx, raw in enumerate(holdings or []):
        cusip = _clean_cusip((raw or {}).get("cusip"))
        symbol = _clean_symbol((raw or {}).get("symbol"))
        mapping_status = str((raw or {}).get("mapping_status") or "").strip()
        mapping_meta: Dict[str, Any] = {}
        if not symbol and con is not None:
            symbol, mapping_status, mapping_meta = resolve_cusip_symbol(con, cusip)
        if not mapping_status:
            mapping_status = "mapped" if symbol else "unmapped"
        value_usd = _safe_float((raw or {}).get("value_usd"))
        if value_usd is not None:
            total_value_usd += float(value_usd)
        source_id = _source_record_id("holding", manager_cik, accession, cusip, idx)
        normalized_holdings.append(
            {
                "ts_ms": int(availability_ts_ms),
                "manager_cik": manager_cik,
                "manager_name": str((filing or {}).get("manager_name") or ""),
                "accession": accession,
                "report_date": report_date,
                "report_ts_ms": report_ts_ms,
                "availability_ts_ms": int(availability_ts_ms),
                "issuer_name": str((raw or {}).get("issuer_name") or ""),
                "title_of_class": str((raw or {}).get("title_of_class") or ""),
                "cusip": cusip,
                "value_usd": value_usd,
                "value_thousands": _safe_float((raw or {}).get("value_thousands")),
                "shares": _safe_float((raw or {}).get("shares")),
                "share_type": str((raw or {}).get("share_type") or ""),
                "put_call": str((raw or {}).get("put_call") or ""),
                "investment_discretion": str((raw or {}).get("investment_discretion") or ""),
                "voting_sole": _safe_float((raw or {}).get("voting_sole")),
                "voting_shared": _safe_float((raw or {}).get("voting_shared")),
                "voting_none": _safe_float((raw or {}).get("voting_none")),
                "symbol": symbol or None,
                "mapping_status": mapping_status,
                "source_record_id": source_id,
                "ingested_ts_ms": int(now_ms),
                "payload_json": dict(raw or {}),
                "diagnostics_json": dict(mapping_meta or {}),
            }
        )
    filing_row = {
        "ts_ms": int(availability_ts_ms),
        "manager_cik": manager_cik,
        "manager_name": str((filing or {}).get("manager_name") or ""),
        "accession": accession,
        "form": str((filing or {}).get("form") or "13F-HR"),
        "filing_date": str((filing or {}).get("filing_date") or ""),
        "report_date": report_date,
        "report_ts_ms": report_ts_ms,
        "acceptance_datetime": str((filing or {}).get("acceptance_datetime") or ""),
        "acceptance_ts_ms": int(availability_ts_ms),
        "availability_ts_ms": int(availability_ts_ms),
        "primary_doc_url": str((filing or {}).get("primary_doc_url") or ""),
        "info_table_url": str(info_table_url or (filing or {}).get("info_table_url") or ""),
        "total_value_usd": float(total_value_usd),
        "holdings_count": int(len(normalized_holdings)),
        "source_record_id": str((filing or {}).get("source_record_id") or _source_record_id("filing", manager_cik, accession)),
        "ingested_ts_ms": int(now_ms),
        "payload_json": dict((filing or {}).get("payload_json") or {}),
        "diagnostics_json": {"availability_rule": "edgar_acceptance_timestamp"},
    }
    return filing_row, normalized_holdings


_FILING_COLUMNS = (
    "ts_ms",
    "manager_cik",
    "manager_name",
    "accession",
    "form",
    "filing_date",
    "report_date",
    "report_ts_ms",
    "acceptance_datetime",
    "acceptance_ts_ms",
    "availability_ts_ms",
    "primary_doc_url",
    "info_table_url",
    "total_value_usd",
    "holdings_count",
    "source_record_id",
    "ingested_ts_ms",
    "payload_json",
    "diagnostics_json",
)

_HOLDING_COLUMNS = (
    "ts_ms",
    "manager_cik",
    "manager_name",
    "accession",
    "report_date",
    "report_ts_ms",
    "availability_ts_ms",
    "issuer_name",
    "title_of_class",
    "cusip",
    "value_usd",
    "value_thousands",
    "shares",
    "share_type",
    "put_call",
    "investment_discretion",
    "voting_sole",
    "voting_shared",
    "voting_none",
    "symbol",
    "mapping_status",
    "source_record_id",
    "ingested_ts_ms",
    "payload_json",
    "diagnostics_json",
)


def put_13f_filing(row: Mapping[str, Any], *, con) -> int:
    values = [_json_param(con, row.get(col)) if col in {"payload_json", "diagnostics_json"} else row.get(col) for col in _FILING_COLUMNS]
    cur = con.execute(
        f"""
        INSERT INTO inst_13f_filings({", ".join(_FILING_COLUMNS)})
        VALUES ({", ".join(["?"] * len(_FILING_COLUMNS))})
        ON CONFLICT(source_record_id) DO UPDATE SET
          ts_ms = excluded.ts_ms,
          manager_name = excluded.manager_name,
          form = excluded.form,
          filing_date = excluded.filing_date,
          report_date = excluded.report_date,
          report_ts_ms = excluded.report_ts_ms,
          acceptance_datetime = excluded.acceptance_datetime,
          acceptance_ts_ms = excluded.acceptance_ts_ms,
          availability_ts_ms = excluded.availability_ts_ms,
          primary_doc_url = excluded.primary_doc_url,
          info_table_url = excluded.info_table_url,
          total_value_usd = excluded.total_value_usd,
          holdings_count = excluded.holdings_count,
          ingested_ts_ms = excluded.ingested_ts_ms,
          payload_json = excluded.payload_json,
          diagnostics_json = excluded.diagnostics_json
        """,
        tuple(values),
    )
    return int(getattr(cur, "rowcount", 0) or 0)


def put_13f_holding(row: Mapping[str, Any], *, con) -> int:
    values = [_json_param(con, row.get(col)) if col in {"payload_json", "diagnostics_json"} else row.get(col) for col in _HOLDING_COLUMNS]
    cur = con.execute(
        f"""
        INSERT INTO inst_13f_holdings({", ".join(_HOLDING_COLUMNS)})
        VALUES ({", ".join(["?"] * len(_HOLDING_COLUMNS))})
        ON CONFLICT(source_record_id) DO UPDATE SET
          ts_ms = excluded.ts_ms,
          manager_name = excluded.manager_name,
          report_date = excluded.report_date,
          report_ts_ms = excluded.report_ts_ms,
          availability_ts_ms = excluded.availability_ts_ms,
          issuer_name = excluded.issuer_name,
          title_of_class = excluded.title_of_class,
          cusip = excluded.cusip,
          value_usd = excluded.value_usd,
          value_thousands = excluded.value_thousands,
          shares = excluded.shares,
          share_type = excluded.share_type,
          put_call = excluded.put_call,
          investment_discretion = excluded.investment_discretion,
          voting_sole = excluded.voting_sole,
          voting_shared = excluded.voting_shared,
          voting_none = excluded.voting_none,
          symbol = excluded.symbol,
          mapping_status = excluded.mapping_status,
          ingested_ts_ms = excluded.ingested_ts_ms,
          payload_json = excluded.payload_json,
          diagnostics_json = excluded.diagnostics_json
        """,
        tuple(values),
    )
    return int(getattr(cur, "rowcount", 0) or 0)


def compute_turnover(previous: Mapping[str, float], current: Mapping[str, float]) -> float:
    prev_total = sum(max(0.0, float(v or 0.0)) for v in (previous or {}).values())
    curr_total = sum(max(0.0, float(v or 0.0)) for v in (current or {}).values())
    if prev_total <= 0.0 or curr_total <= 0.0:
        return 1.0
    keys = set(previous or {}) | set(current or {})
    prev_w = {key: max(0.0, float((previous or {}).get(key) or 0.0)) / prev_total for key in keys}
    curr_w = {key: max(0.0, float((current or {}).get(key) or 0.0)) / curr_total for key in keys}
    return float(0.5 * sum(abs(curr_w.get(key, 0.0) - prev_w.get(key, 0.0)) for key in keys))


def turnover_screen_passed(turnover: float | None, *, threshold: float | None = None) -> bool:
    if turnover is None:
        return False
    return float(turnover) <= float(TURNOVER_THRESHOLD if threshold is None else threshold)


def _latest_manager_reports(con, *, asof_ts_ms: int) -> Dict[str, List[Dict[str, Any]]]:
    try:
        rows = con.execute(
            """
            SELECT manager_cik, report_ts_ms, availability_ts_ms
            FROM inst_13f_filings
            WHERE availability_ts_ms <= ?
            ORDER BY manager_cik ASC, report_ts_ms DESC, availability_ts_ms DESC
            """,
            (int(asof_ts_ms),),
        ).fetchall()
    except Exception:
        return {}
    reports: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows or []:
        manager = _clean_cik(row[0])
        report_ts = int(row[1] or 0)
        availability_ts = int(row[2] or 0)
        if not manager or report_ts <= 0:
            continue
        seen = {int(item["report_ts_ms"]) for item in reports.get(manager, [])}
        if report_ts in seen:
            continue
        reports.setdefault(manager, []).append({"report_ts_ms": report_ts, "availability_ts_ms": availability_ts})
        reports[manager] = reports[manager][:2]
    return {key: vals for key, vals in reports.items() if vals}


def _holdings_for_manager_report(con, *, manager_cik: str, report_ts_ms: int, asof_ts_ms: int) -> List[Dict[str, Any]]:
    cur = con.execute(
        """
        SELECT symbol, cusip, value_usd, shares, availability_ts_ms, report_date, manager_name
        FROM inst_13f_holdings
        WHERE manager_cik = ?
          AND report_ts_ms = ?
          AND availability_ts_ms <= ?
          AND COALESCE(symbol, '') != ''
          AND COALESCE(put_call, '') = ''
        """,
        (_clean_cik(manager_cik), int(report_ts_ms), int(asof_ts_ms)),
    )
    cols = [str(col[0]) for col in (cur.description or [])]
    return [_row_dict(row, cols) for row in cur.fetchall() or []]


def resolve_13f_features(con, *, symbol: str, ts_ms: int) -> Tuple[Dict[str, float], Dict[str, Any], bool]:
    symbol_key = _clean_symbol(symbol)
    features = {fid: 0.0 for fid in INST_13F_FEATURE_IDS}
    if not symbol_key:
        return features, {"latest_availability_ts_ms": None, "managers": []}, False
    ensure_13f_tables(con)
    reports_by_manager = _latest_manager_reports(con, asof_ts_ms=int(ts_ms))
    screened_managers: List[str] = []
    holding_managers: List[str] = []
    latest_availability = 0
    conviction_max = 0.0
    new_flag = 0.0
    add_flag = 0.0

    for manager_cik, reports in reports_by_manager.items():
        if len(reports) < 2:
            continue
        current_report = reports[0]
        previous_report = reports[1]
        current_rows = _holdings_for_manager_report(
            con,
            manager_cik=manager_cik,
            report_ts_ms=int(current_report["report_ts_ms"]),
            asof_ts_ms=int(ts_ms),
        )
        previous_rows = _holdings_for_manager_report(
            con,
            manager_cik=manager_cik,
            report_ts_ms=int(previous_report["report_ts_ms"]),
            asof_ts_ms=int(ts_ms),
        )
        current_by_symbol: Dict[str, float] = {}
        previous_by_symbol: Dict[str, float] = {}
        for row in current_rows:
            sym = _clean_symbol(row.get("symbol"))
            current_by_symbol[sym] = current_by_symbol.get(sym, 0.0) + float(_safe_float(row.get("value_usd")) or 0.0)
        for row in previous_rows:
            sym = _clean_symbol(row.get("symbol"))
            previous_by_symbol[sym] = previous_by_symbol.get(sym, 0.0) + float(_safe_float(row.get("value_usd")) or 0.0)
        turnover = compute_turnover(previous_by_symbol, current_by_symbol)
        if not turnover_screen_passed(turnover):
            continue
        screened_managers.append(manager_cik)
        total_current = sum(max(0.0, value) for value in current_by_symbol.values())
        current_value = float(current_by_symbol.get(symbol_key, 0.0))
        previous_value = float(previous_by_symbol.get(symbol_key, 0.0))
        if current_value <= 0.0:
            continue
        holding_managers.append(manager_cik)
        latest_availability = max(latest_availability, int(current_report.get("availability_ts_ms") or 0))
        if total_current > 0.0:
            conviction_max = max(conviction_max, current_value / total_current)
        if previous_value <= 0.0:
            new_flag = 1.0
        if current_value > previous_value:
            add_flag = 1.0

    features["13f_consensus_holders"] = float(len(holding_managers))
    features["13f_conviction_max"] = float(conviction_max)
    features["13f_new_position_flag"] = float(new_flag)
    features["13f_add_flag"] = float(add_flag)
    return (
        features,
        {
            "latest_availability_ts_ms": int(latest_availability) if latest_availability > 0 else None,
            "screened_managers": list(screened_managers),
            "holding_managers": list(holding_managers),
            "screened_manager_count": int(len(screened_managers)),
        },
        bool(holding_managers),
    )


def materialize_13f_symbol_features(con, *, symbol: str, ts_ms: int) -> Dict[str, Any]:
    ensure_13f_tables(con)
    features, meta, available = resolve_13f_features(con, symbol=symbol, ts_ms=int(ts_ms))
    con.execute(
        """
        INSERT INTO inst_13f_symbol_features(
          symbol, asof_ts_ms,
          "13f_consensus_holders", "13f_conviction_max", "13f_new_position_flag", "13f_add_flag",
          source_max_availability_ts_ms, created_ts_ms, meta_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(symbol, asof_ts_ms) DO UPDATE SET
          "13f_consensus_holders" = excluded."13f_consensus_holders",
          "13f_conviction_max" = excluded."13f_conviction_max",
          "13f_new_position_flag" = excluded."13f_new_position_flag",
          "13f_add_flag" = excluded."13f_add_flag",
          source_max_availability_ts_ms = excluded.source_max_availability_ts_ms,
          created_ts_ms = excluded.created_ts_ms,
          meta_json = excluded.meta_json
        """,
        (
            _clean_symbol(symbol),
            int(ts_ms),
            float(features["13f_consensus_holders"]),
            float(features["13f_conviction_max"]),
            float(features["13f_new_position_flag"]),
            float(features["13f_add_flag"]),
            meta.get("latest_availability_ts_ms"),
            int(utc_now_ms()),
            _json_param(con, dict(meta)),
        ),
    )
    return {"symbol": _clean_symbol(symbol), "available": bool(available), "features": features, "meta": meta}


def ingest_13f_batch(*, managers: Sequence[Manager13F] | None = None, now_ms: int | None = None) -> Dict[str, Any]:
    anchor_ms = int(now_ms or utc_now_ms())
    con = connect()
    rows: List[Tuple[Dict[str, Any], List[Dict[str, Any]]]] = []
    errors: List[str] = []
    try:
        ensure_13f_tables(con)
        seed_default_13f_managers(con)
        manager_list = list(managers or configured_13f_managers(con))
        for manager in manager_list:
            filings, filing_errors = fetch_manager_13f_filings(manager, limit=FILING_LIMIT)
            errors.extend(filing_errors)
            for filing in filings:
                xml_text, info_url, xml_errors = fetch_information_table_xml(filing)
                errors.extend(xml_errors)
                if not xml_text:
                    continue
                try:
                    holdings = parse_13f_information_table(xml_text)
                    filing_row, holding_rows = normalize_filing_with_holdings(
                        filing,
                        holdings,
                        con=con,
                        ingested_ts_ms=anchor_ms,
                        info_table_url=info_url,
                    )
                    rows.append((filing_row, holding_rows))
                except Exception as exc:
                    errors.append(f"{manager.cik}:{filing.get('accession')}:{exc}")

        def _write(conw) -> Tuple[int, int]:
            ensure_13f_tables(conw)
            seed_default_13f_managers(conw)
            filing_count = 0
            holding_count = 0
            for filing_row, holding_rows in rows:
                filing_count += int(put_13f_filing(filing_row, con=conw) or 0)
                for holding in holding_rows:
                    holding_count += int(put_13f_holding(holding, con=conw) or 0)
            return filing_count, holding_count

        written_filings, written_holdings = run_write_txn(
            _write,
            table="inst_13f_filings",
            operation="ingest_inst_13f",
            context={"managers": int(len(managers or [])), "filings": int(len(rows))},
        ) or (0, 0)
        latest_ts = max([int(row[0].get("availability_ts_ms") or 0) for row in rows] or [anchor_ms])
        return {
            "ok": not bool(errors),
            "managers": int(len(manager_list)),
            "filings": int(len(rows)),
            "holdings": int(sum(len(holding_rows) for _filing, holding_rows in rows)),
            "written_filings": int(written_filings),
            "written_holdings": int(written_holdings),
            "errors": list(errors),
            "last_ingested_ts_ms": int(latest_ts),
        }
    finally:
        try:
            con.close()
        # system-audit: ignore[silent_except] connection close is best-effort cleanup.
        except Exception:
            pass
