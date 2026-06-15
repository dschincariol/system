"""
Additive SEC Form 4 ingestion helpers built on top of the existing EDGAR path.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional, Sequence
from xml.etree import ElementTree as ET

import requests

from engine.artifacts.store import LocalArtifactStore
from engine.data.ingest.news_enrichment import infer_symbols
from engine.data.sec import edgar_live
from engine.data.sec.form4_classifier import parse_ts_ms as _parse_form4_ts_ms
from engine.runtime.config import FORM4_BACKFILL_DAYS as CONFIG_FORM4_BACKFILL_DAYS
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger

FORM4_BACKFILL_DAYS = max(1, int(CONFIG_FORM4_BACKFILL_DAYS))
FORM4_FILING_LIMIT = max(10, int(os.environ.get("FORM4_FILING_LIMIT", "60")))
FORM4_TIMEOUT_S = max(5.0, float(os.environ.get("FORM4_TIMEOUT_S", "20")))
FORM4_FORMS = {"4", "4/A"}

LOG = get_logger("engine.data.sec.form4_live")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.data.sec.form4_live",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _store_filing_body(*, symbol: str, filing: Dict[str, Any], body: str, url: str) -> Dict[str, Any]:
    if not body:
        return {}
    accession = str((filing or {}).get("accession") or hashlib.sha1(url.encode("utf-8", errors="ignore")).hexdigest()).strip()
    alias = f"filing:sec:{accession}"
    try:
        ref = LocalArtifactStore().put(
            body.encode("utf-8", errors="replace"),
            content_type="application/xml; charset=utf-8",
            kind="filing",
            alias=alias,
            metadata={
                "symbol": str(symbol),
                "accession": accession,
                "form": str((filing or {}).get("form") or ""),
                "filed_date": str((filing or {}).get("filed_date") or ""),
                "url": str(url),
                "provider": "sec",
            },
        )
        return {
            "artifact_alias": alias,
            "artifact_sha256": ref.sha256,
            "artifact_size_bytes": int(ref.size),
            "content_type": ref.content_type,
        }
    except Exception as exc:
        _warn_nonfatal(
            "FORM4_LIVE_ARTIFACT_STORE_FAILED",
            exc,
            once_key=f"form4_artifact_store:{accession}",
            symbol=symbol,
            filing_url=url,
        )
        return {}


def _normalize_symbol(value: Any) -> str:
    text = str(value or "").strip().upper().replace("$", "")
    return text


def _entity_id(*, symbol: Any = "", issuer_cik: Any = "") -> Optional[str]:
    cik = str(issuer_cik or "").strip()
    if cik:
        return f"cik:{cik.lstrip('0') or '0'}"
    symbol_key = _normalize_symbol(symbol)
    if symbol_key:
        return f"symbol:{symbol_key}"
    return None


def _resolution_status(symbol: Any, entity_id: Any) -> str:
    if _normalize_symbol(symbol):
        return "resolved"
    if str(entity_id or "").strip():
        return "entity_resolved"
    return "unresolved"


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except Exception:
        return None


def _local_name(tag: Any) -> str:
    return str(tag or "").split("}", 1)[-1]


def _child(node: ET.Element | None, name: str) -> ET.Element | None:
    if node is None:
        return None
    for child in list(node):
        if _local_name(child.tag) == str(name):
            return child
    return None


def _children(node: ET.Element | None, name: str) -> List[ET.Element]:
    if node is None:
        return []
    return [child for child in list(node) if _local_name(child.tag) == str(name)]


def _text(node: ET.Element | None, *path: str) -> Optional[str]:
    cur = node
    for part in path:
        cur = _child(cur, str(part))
        if cur is None:
            return None
    text = str(cur.text or "").strip()
    return text or None


def _bool_text(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "y", "yes"}


def _parse_ts_ms(value: Any) -> Optional[int]:
    return _parse_form4_ts_ms(value)


def _transaction_kind(code: str, acquired_disposed: str) -> tuple[str, str]:
    code_key = str(code or "").strip().upper()
    acquired_key = str(acquired_disposed or "").strip().upper()
    tx_type = {
        "P": "purchase",
        "S": "sale",
        "F": "tax_withholding",
        "D": "issuer_disposition",
        "A": "grant",
        "M": "option_exercise",
        "X": "option_exercise",
        "C": "conversion",
        "G": "gift",
        "L": "small_acquisition",
        "W": "inheritance",
    }.get(code_key, "other")
    if code_key in {"P", "L"}:
        return tx_type, "buy"
    if code_key in {"S", "F", "D"}:
        return tx_type, "sell"
    if not code_key and acquired_key == "A":
        return tx_type, "buy"
    if not code_key and acquired_key == "D":
        return tx_type, "sell"
    return tx_type, "neutral"


def _stable_id(parts: Sequence[Any]) -> str:
    payload = "|".join(str(part or "").strip() for part in parts)
    return hashlib.sha1(payload.encode("utf-8", "ignore")).hexdigest()


def _node_text_blob(node: ET.Element | None) -> str:
    if node is None:
        return ""
    try:
        return " ".join(str(part or "").strip() for part in node.itertext() if str(part or "").strip())
    except Exception:
        return ""


def _detect_10b5_1_plan(*values: Any) -> bool:
    text = " ".join(str(value or "") for value in values if value is not None)
    normalized = text.lower().replace("\u2011", "-").replace("\u2010", "-").replace("\u2013", "-").replace("\u2014", "-")
    compact = "".join(ch for ch in normalized if ch.isalnum())
    return "10b51" in compact or "10b5-1" in normalized or "10b5 1" in normalized


def _cik_to_ticker(value: Any) -> str:
    raw_cik = str(value or "").strip()
    if not raw_cik:
        return ""
    normalized_cik = raw_cik.lstrip("0") or "0"
    try:
        payload = edgar_live._load_ticker_map()
        rows = edgar_live._ticker_map_rows(payload)
    except Exception as exc:
        _warn_nonfatal(
            "FORM4_LIVE_CIK_TICKER_MAP_FAILED",
            exc,
            once_key="form4_live_cik_ticker_map",
        )
        return ""
    for row in rows:
        cik = str(row.get("cik") or row.get("cik_str") or "").strip()
        if not cik:
            continue
        if (cik.lstrip("0") or "0") == normalized_cik:
            return _normalize_symbol(row.get("ticker"))
    return ""


def _resolve_symbol(
    *,
    explicit_symbol: str = "",
    issuer_name: str = "",
    issuer_cik: str = "",
    filing_symbol: str = "",
    allowed_symbols: Optional[Sequence[str]] = None,
) -> tuple[Optional[str], Dict[str, Any]]:
    diagnostics: Dict[str, Any] = {
        "allowed_symbol_count": int(len(list(allowed_symbols or []))),
        "issuer_cik": str(issuer_cik or ""),
    }
    for method, candidate in (
        ("issuer_trading_symbol", explicit_symbol),
        ("issuer_cik", _cik_to_ticker(issuer_cik)),
        ("filing_symbol", filing_symbol),
    ):
        symbol = _normalize_symbol(candidate)
        if symbol:
            diagnostics["symbol_match_method"] = method
            diagnostics["symbol_match_confidence"] = 1.0
            return symbol, diagnostics

    payload = {
        "title": str(issuer_name or ""),
        "body": str(issuer_name or ""),
        "summary": str(issuer_cik or ""),
    }
    try:
        inferred = infer_symbols(payload, allowed_symbols=allowed_symbols)
        symbols = list((inferred or {}).get("symbols") or [])
        if symbols:
            symbol = _normalize_symbol(symbols[0])
            diagnostics["symbol_match_method"] = str(((inferred or {}).get("match_method") or {}).get(symbol, "company_name"))
            diagnostics["symbol_match_confidence"] = float(((inferred or {}).get("match_confidence") or {}).get(symbol, 0.0))
            return symbol, diagnostics
        inferred = infer_symbols(payload, allowed_symbols=None)
        symbols = list((inferred or {}).get("symbols") or [])
        if symbols:
            symbol = _normalize_symbol(symbols[0])
            diagnostics["symbol_match_method"] = str(((inferred or {}).get("match_method") or {}).get(symbol, "company_name"))
            diagnostics["symbol_match_confidence"] = float(((inferred or {}).get("match_confidence") or {}).get(symbol, 0.0))
            return symbol, diagnostics
    except Exception as exc:
        _warn_nonfatal(
            "FORM4_LIVE_SYMBOL_RESOLUTION_FAILED",
            exc,
            once_key=f"form4_symbol_resolution:{issuer_name}:{issuer_cik}",
            issuer_name=str(issuer_name or ""),
            issuer_cik=str(issuer_cik or ""),
        )
    diagnostics["symbol_match_method"] = "unresolved"
    diagnostics["symbol_match_confidence"] = 0.0
    return None, diagnostics


def _owner_roles(owner: Dict[str, Any]) -> tuple[Optional[str], Optional[str]]:
    labels: List[str] = []
    if owner.get("is_director"):
        labels.append("director")
    if owner.get("is_officer"):
        labels.append("officer")
    if owner.get("is_ten_percent_owner"):
        labels.append("ten_percent_owner")
    if owner.get("is_other"):
        labels.append("other")
    role = ",".join(labels) if labels else None
    title = str(owner.get("officer_title") or owner.get("other_text") or "").strip() or None
    return role, title


def _merge_diagnostics(existing: Any, updates: Dict[str, Any]) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    if isinstance(existing, dict):
        merged.update(existing)
    elif isinstance(existing, str):
        try:
            parsed = json.loads(existing)
        except Exception:
            parsed = None
        if isinstance(parsed, dict):
            merged.update(parsed)
    merged.update(dict(updates or {}))
    return merged


def apply_form4_resolution(
    row: Dict[str, Any],
    *,
    resolution_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Apply symbol/entity resolution updates to one normalized Form 4 row."""
    updated = dict(row or {})
    issuer_cik = str(updated.get("issuer_cik") or "").strip()
    symbol = _normalize_symbol(updated.get("symbol"))
    entity_id = _entity_id(symbol=symbol, issuer_cik=issuer_cik)
    merged_meta = _merge_diagnostics(updated.get("diagnostics_json"), dict(resolution_meta or {}))
    updated["symbol"] = symbol or None
    updated["entity_id"] = entity_id
    updated["resolution_status"] = _resolution_status(symbol, entity_id)
    updated["resolution_method"] = (
        str((resolution_meta or {}).get("symbol_match_method") or ("issuer_cik" if issuer_cik else "unresolved")).strip()
        or None
    )
    updated["diagnostics_json"] = merged_meta
    return updated


def refresh_form4_transaction_resolution(
    row: Dict[str, Any],
    *,
    allowed_symbols: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """Recompute resolution state from the row's current symbol and issuer fields."""
    updated = dict(row or {})
    resolved_symbol, resolution_meta = _resolve_symbol(
        explicit_symbol=str(updated.get("symbol") or ""),
        issuer_name=str(updated.get("issuer_name") or ""),
        issuer_cik=str(updated.get("issuer_cik") or ""),
        filing_symbol=str(updated.get("symbol") or ""),
        allowed_symbols=allowed_symbols,
    )
    if resolved_symbol:
        updated["symbol"] = resolved_symbol
    return apply_form4_resolution(updated, resolution_meta=resolution_meta)


def parse_form4_xml(
    payload_xml: str,
    *,
    filing: Optional[Dict[str, Any]] = None,
    filing_symbol: str = "",
    allowed_symbols: Optional[Sequence[str]] = None,
    ingested_ts_ms: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Parse one Form 4 XML document into normalized transaction rows."""
    if not str(payload_xml or "").strip():
        return []
    try:
        root = ET.fromstring(payload_xml)
    except Exception as exc:
        _warn_nonfatal(
            "FORM4_LIVE_XML_PARSE_FAILED",
            exc,
            once_key=f"form4_xml_parse:{hash(str(payload_xml)[:200])}",
        )
        return []

    issuer = _child(root, "issuer")
    issuer_name = str(_text(issuer, "issuerName") or "").strip()
    issuer_cik = str(_text(issuer, "issuerCik") or "").strip()
    explicit_symbol = _normalize_symbol(_text(issuer, "issuerTradingSymbol"))
    filing_accession = str((filing or {}).get("accession") or "").strip()
    filing_date = str((filing or {}).get("filed_date") or _text(root, "periodOfReport") or "").strip() or None
    filing_accepted_at = (
        str(
            (filing or {}).get("acceptance_datetime")
            or (filing or {}).get("acceptanceDateTime")
            or (filing or {}).get("filing_accepted_at")
            or ""
        ).strip()
        or None
    )
    filing_ts_ms = _parse_ts_ms(filing_accepted_at) or _parse_ts_ms(filing_date)
    ingested_ts = int(ingested_ts_ms or int(time.time() * 1000))
    document_plan_flag = _detect_10b5_1_plan(_node_text_blob(root))

    owners: List[Dict[str, Any]] = []
    for owner_node in _children(root, "reportingOwner"):
        owner: Dict[str, Any] = {
            "name": _text(owner_node, "reportingOwnerId", "rptOwnerName"),
            "cik": _text(owner_node, "reportingOwnerId", "rptOwnerCik"),
            "is_director": _bool_text(_text(owner_node, "reportingOwnerRelationship", "isDirector")),
            "is_officer": _bool_text(_text(owner_node, "reportingOwnerRelationship", "isOfficer")),
            "is_ten_percent_owner": _bool_text(_text(owner_node, "reportingOwnerRelationship", "isTenPercentOwner")),
            "is_other": _bool_text(_text(owner_node, "reportingOwnerRelationship", "isOther")),
            "officer_title": _text(owner_node, "reportingOwnerRelationship", "officerTitle"),
            "other_text": _text(owner_node, "reportingOwnerRelationship", "otherText"),
        }
        owners.append(owner)
    primary_owner = owners[0] if owners else {}
    insider_role, insider_title = _owner_roles(primary_owner)
    resolved_symbol, resolution_meta = _resolve_symbol(
        explicit_symbol=explicit_symbol,
        issuer_name=issuer_name,
        issuer_cik=issuer_cik,
        filing_symbol=str(filing_symbol or ""),
        allowed_symbols=allowed_symbols,
    )

    rows: List[Dict[str, Any]] = []
    transaction_sets = (
        ("non_derivative", _child(root, "nonDerivativeTable"), "nonDerivativeTransaction"),
        ("derivative", _child(root, "derivativeTable"), "derivativeTransaction"),
    )
    for security_type, table_node, transaction_name in transaction_sets:
        for index, tx_node in enumerate(_children(table_node, transaction_name)):
            transaction_date = str(_text(tx_node, "transactionDate", "value") or filing_date or "").strip() or None
            transaction_ts_ms = _parse_ts_ms(transaction_date)
            transaction_code = str(_text(tx_node, "transactionCoding", "transactionCode") or "").strip().upper()
            acquired_disposed = str(
                _text(tx_node, "transactionAmounts", "transactionAcquiredDisposedCode", "value") or ""
            ).strip().upper()
            transaction_type, direction = _transaction_kind(transaction_code, acquired_disposed)
            shares = _safe_float(_text(tx_node, "transactionAmounts", "transactionShares", "value"))
            price = _safe_float(_text(tx_node, "transactionAmounts", "transactionPricePerShare", "value"))
            value = _safe_float(_text(tx_node, "transactionAmounts", "transactionTotalValue", "value"))
            if value is None and shares is not None and price is not None:
                value = float(shares * price)
            ownership_nature = str(
                _text(tx_node, "ownershipNature", "directOrIndirectOwnership", "value")
                or _text(tx_node, "ownershipNature", "natureOfOwnership", "value")
                or ""
            ).strip() or None
            is_10b5_1_plan = bool(document_plan_flag or _detect_10b5_1_plan(_node_text_blob(tx_node)))
            source_transaction_id = _stable_id(
                (
                    filing_accession,
                    index,
                    str(primary_owner.get("cik") or primary_owner.get("name") or ""),
                    transaction_date,
                    transaction_code,
                    shares,
                    price,
                    security_type,
                )
            )
            diagnostics_json = dict(resolution_meta)
            diagnostics_json["reporting_owner_count"] = int(len(owners))
            diagnostics_json["acquired_disposed_code"] = acquired_disposed or None
            diagnostics_json["is_10b5_1_plan"] = bool(is_10b5_1_plan)
            row = {
                "source_transaction_id": str(source_transaction_id),
                "created_ts_ms": int(ingested_ts),
                "ingested_ts_ms": int(ingested_ts),
                "source": "sec_form4",
                "filing_accession": filing_accession or None,
                "filing_identifier": filing_accession or None,
                "filing_url": (filing or {}).get("primary_doc_url"),
                "filing_ts_ms": filing_ts_ms,
                "availability_ts_ms": filing_ts_ms,
                "filing_date": filing_date,
                "filing_accepted_at": filing_accepted_at,
                "transaction_ts_ms": transaction_ts_ms,
                "transaction_date": transaction_date,
                "symbol": resolved_symbol,
                "issuer_name": issuer_name or None,
                "issuer_cik": issuer_cik or None,
                "insider_name": str(primary_owner.get("name") or "").strip() or None,
                "insider_cik": str(primary_owner.get("cik") or "").strip() or None,
                "insider_role": insider_role,
                "insider_title": insider_title,
                "transaction_code": transaction_code or None,
                "transaction_type": transaction_type,
                "direction": direction,
                "security_type": security_type,
                "shares": shares,
                "price": price,
                "value": value,
                "ownership_nature": ownership_nature,
                "is_10b5_1_plan": bool(is_10b5_1_plan),
                "payload_json": {
                    "filing": dict(filing or {}),
                    "issuer_name": issuer_name or None,
                    "issuer_cik": issuer_cik or None,
                    "issuer_trading_symbol": explicit_symbol or None,
                    "period_of_report": _text(root, "periodOfReport"),
                    "document_type": _text(root, "documentType"),
                    "security_type": security_type,
                    "filing_accepted_at": filing_accepted_at,
                    "availability_ts_ms": filing_ts_ms,
                    "is_10b5_1_plan": bool(is_10b5_1_plan),
                    "reporting_owners": owners,
                },
                "diagnostics_json": diagnostics_json,
            }
            rows.append(apply_form4_resolution(row, resolution_meta=resolution_meta))
    return rows


def fetch_form4_transactions(
    symbol: str,
    *,
    filing_limit: Optional[int] = None,
    backfill_days: Optional[int] = None,
    allowed_symbols: Optional[Sequence[str]] = None,
    session: Optional[requests.Session] = None,
) -> List[Dict[str, Any]]:
    """Fetch recent Form 4 filings for one symbol and normalize the transactions."""
    symbol_key = _normalize_symbol(symbol)
    if not symbol_key:
        return []
    limit = max(1, int(filing_limit or FORM4_FILING_LIMIT))
    lookback_days = max(1, int(backfill_days or FORM4_BACKFILL_DAYS))
    cutoff_ms = int(time.time() * 1000) - int(lookback_days * 24 * 3600 * 1000)
    filings = edgar_live.fetch_recent_filings(symbol_key, limit=limit)
    session_obj = session or requests.Session()

    rows: List[Dict[str, Any]] = []
    for filing in filings or []:
        form = str((filing or {}).get("form") or "").strip().upper()
        if form not in FORM4_FORMS:
            continue
        filed_date = str((filing or {}).get("filed_date") or "").strip()
        filed_ts_ms = _parse_ts_ms(filed_date)
        if filed_ts_ms is not None and filed_ts_ms < int(cutoff_ms):
            continue
        filing_url = str((filing or {}).get("primary_doc_url") or "").strip()
        if not filing_url:
            continue
        try:
            response = session_obj.get(filing_url, headers=edgar_live.HEADERS, timeout=FORM4_TIMEOUT_S)
            response.raise_for_status()
        except Exception as exc:
            _warn_nonfatal(
                "FORM4_LIVE_FETCH_FAILED",
                exc,
                once_key=f"form4_fetch:{filing_url}",
                symbol=symbol_key,
                filing_url=filing_url,
            )
            continue
        filing_meta = _store_filing_body(
            symbol=symbol_key,
            filing=dict(filing or {}),
            body=response.text,
            url=filing_url,
        )
        parsed_rows = parse_form4_xml(
            response.text,
            filing=dict(filing or {}),
            filing_symbol=symbol_key,
            allowed_symbols=allowed_symbols,
        )
        if filing_meta:
            for row in parsed_rows:
                meta = dict(row.get("meta_json") or {}) if isinstance(row.get("meta_json"), dict) else {}
                meta.update(filing_meta)
                row["meta_json"] = meta
        rows.extend(parsed_rows)
    return rows
