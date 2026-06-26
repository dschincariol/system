"""Structured document event extraction and PIT feature resolution.

The extractor is intentionally deterministic: it converts filings,
transcripts, and news text into auditable event rows with source document
identity, event time, availability time, confidence, and PIT metadata.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Sequence, Tuple


EXTRACTOR_NAME = "structured_document_events"
EXTRACTOR_VERSION = "structured_document_events_v1"
LOOKBACK_DAYS_DEFAULT = 90
FRESH_COUNT_DAYS = 30
MS_DAY = 24 * 60 * 60 * 1000


STRUCTURED_DOCUMENT_EVENT_FEATURE_IDS = [
    "structured_doc_events_v1.guidance_raise_confidence",
    "structured_doc_events_v1.guidance_cut_confidence",
    "structured_doc_events_v1.margin_pressure_confidence",
    "structured_doc_events_v1.liquidity_stress_confidence",
    "structured_doc_events_v1.capex_increase_confidence",
    "structured_doc_events_v1.capex_cut_confidence",
    "structured_doc_events_v1.debt_refinancing_risk_confidence",
    "structured_doc_events_v1.regulatory_litigation_risk_confidence",
    "structured_doc_events_v1.supply_chain_exposure_confidence",
    "structured_doc_events_v1.capital_allocation_positive_confidence",
    "structured_doc_events_v1.capital_allocation_negative_confidence",
    "structured_doc_events_v1.buyback_increase_confidence",
    "structured_doc_events_v1.buyback_cut_confidence",
    "structured_doc_events_v1.dividend_increase_confidence",
    "structured_doc_events_v1.dividend_cut_confidence",
    "structured_doc_events_v1.customer_concentration_confidence",
    "structured_doc_events_v1.management_tone_positive_confidence",
    "structured_doc_events_v1.management_tone_negative_confidence",
    "structured_doc_events_v1.management_uncertainty_confidence",
    "structured_doc_events_v1.macro_positive_surprise_confidence",
    "structured_doc_events_v1.macro_negative_surprise_confidence",
    "structured_doc_events_v1.event_count_30d",
    "structured_doc_events_v1.latest_event_age_days",
]


EVENT_FEATURE_ID = {
    "guidance_raise": "structured_doc_events_v1.guidance_raise_confidence",
    "guidance_cut": "structured_doc_events_v1.guidance_cut_confidence",
    "margin_pressure": "structured_doc_events_v1.margin_pressure_confidence",
    "liquidity_stress": "structured_doc_events_v1.liquidity_stress_confidence",
    "capex_increase": "structured_doc_events_v1.capex_increase_confidence",
    "capex_cut": "structured_doc_events_v1.capex_cut_confidence",
    "debt_refinancing_risk": "structured_doc_events_v1.debt_refinancing_risk_confidence",
    "regulatory_litigation_risk": "structured_doc_events_v1.regulatory_litigation_risk_confidence",
    "supply_chain_exposure": "structured_doc_events_v1.supply_chain_exposure_confidence",
    "capital_allocation_positive": "structured_doc_events_v1.capital_allocation_positive_confidence",
    "capital_allocation_negative": "structured_doc_events_v1.capital_allocation_negative_confidence",
    "buyback_increase": "structured_doc_events_v1.buyback_increase_confidence",
    "buyback_cut": "structured_doc_events_v1.buyback_cut_confidence",
    "dividend_increase": "structured_doc_events_v1.dividend_increase_confidence",
    "dividend_cut": "structured_doc_events_v1.dividend_cut_confidence",
    "customer_concentration": "structured_doc_events_v1.customer_concentration_confidence",
    "management_tone_positive": "structured_doc_events_v1.management_tone_positive_confidence",
    "management_tone_negative": "structured_doc_events_v1.management_tone_negative_confidence",
    "management_uncertainty": "structured_doc_events_v1.management_uncertainty_confidence",
    "macro_positive_surprise": "structured_doc_events_v1.macro_positive_surprise_confidence",
    "macro_negative_surprise": "structured_doc_events_v1.macro_negative_surprise_confidence",
}


@dataclass(frozen=True)
class EventSpec:
    event_type: str
    polarity: float
    patterns: Tuple[str, ...]


EVENT_SPECS: Tuple[EventSpec, ...] = (
    EventSpec(
        "guidance_raise",
        1.0,
        (
            r"\b(?:raise|raises|raised|raising|increase|increases|increased|lift|lifts|lifted)\b.{0,80}\b(?:guidance|outlook|forecast|revenue|eps|earnings|ebitda)\b",
            r"\b(?:guidance|outlook|forecast)\b.{0,80}\b(?:above|higher|better|increased|raised|upward)\b",
        ),
    ),
    EventSpec(
        "guidance_cut",
        -1.0,
        (
            r"\b(?:cut|cuts|cutting|lower|lowers|lowered|reduce|reduces|reduced|trim|trimmed)\b.{0,80}\b(?:guidance|outlook|forecast|revenue|eps|earnings|ebitda)\b",
            r"\b(?:guidance|outlook|forecast)\b.{0,80}\b(?:below|weaker|reduced|lowered|downward)\b",
            r"\b(?:warn|warns|warning|warned)\b.{0,80}\b(?:guidance|outlook|forecast|demand|sales|revenue)\b",
        ),
    ),
    EventSpec(
        "margin_pressure",
        -1.0,
        (
            r"\b(?:margin|margins)\b.{0,80}\b(?:pressure|compression|compressed|decline|declined|down|weaker|headwind|erosion)\b",
            r"\b(?:cost inflation|higher costs|input costs|freight costs|pricing pressure|gross margin decline)\b",
        ),
    ),
    EventSpec(
        "liquidity_stress",
        -1.0,
        (
            r"\b(?:going concern|substantial doubt|liquidity crunch|liquidity stress|cash runway|working capital deficit)\b",
            r"\b(?:liquidity|cash|capital resources)\b.{0,90}\b(?:insufficient|constrained|limited|deteriorated|risk|shortfall)\b",
        ),
    ),
    EventSpec(
        "capex_increase",
        -0.25,
        (
            r"\b(?:capex|capital expenditure|capital expenditures|capital spending)\b.{0,80}\b(?:increase|increases|increased|raising|raised|higher|accelerate|accelerated)\b",
            r"\b(?:increase|increases|increased|raising|raised|higher)\b.{0,80}\b(?:capex|capital expenditure|capital expenditures|capital spending)\b",
        ),
    ),
    EventSpec(
        "capex_cut",
        0.25,
        (
            r"\b(?:capex|capital expenditure|capital expenditures|capital spending)\b.{0,80}\b(?:cut|cuts|cutting|reduce|reduced|lower|lowered|defer|deferred)\b",
            r"\b(?:cut|cuts|cutting|reduce|reduced|lower|lowered|defer|deferred)\b.{0,80}\b(?:capex|capital expenditure|capital expenditures|capital spending)\b",
        ),
    ),
    EventSpec(
        "debt_refinancing_risk",
        -1.0,
        (
            r"\b(?:refinancing risk|debt maturity|debt maturities|maturity wall|covenant breach|default|going concern)\b",
            r"\b(?:debt|notes|credit facility|term loan|revolver)\b.{0,90}\b(?:mature|matures|maturity|refinance|refinancing|covenant|waiver|default)\b",
        ),
    ),
    EventSpec(
        "regulatory_litigation_risk",
        -1.0,
        (
            r"\b(?:lawsuit|litigation|class action|settlement|subpoena|investigation|probe|antitrust|regulatory action)\b",
            r"\b(?:sec|doj|ftc|fda|epa|cftc|finra|department of justice)\b.{0,90}\b(?:investigation|probe|lawsuit|inquiry|action|complaint)\b",
        ),
    ),
    EventSpec(
        "supply_chain_exposure",
        -0.75,
        (
            r"\b(?:supply chain|supplier|suppliers|component|components|logistics|freight|shipping)\b.{0,90}\b(?:disruption|constraint|shortage|delay|delays|exposure|risk|headwind)\b",
            r"\b(?:shortage|shortages|delays|disruptions)\b.{0,90}\b(?:supplier|supply chain|component|components|logistics|freight|shipping)\b",
        ),
    ),
    EventSpec(
        "capital_allocation_positive",
        0.35,
        (
            r"\b(?:capital allocation|cash deployment|free cash flow)\b.{0,90}\b(?:disciplined|improved|shareholder return|returning capital|accretive)\b",
            r"\b(?:asset sale|divestiture|cost discipline)\b.{0,90}\b(?:strengthen|improve|improves|improved|balance sheet|returns)\b",
        ),
    ),
    EventSpec(
        "capital_allocation_negative",
        -0.35,
        (
            r"\b(?:capital allocation|cash deployment|free cash flow)\b.{0,90}\b(?:deteriorated|strained|undisciplined|dilutive|pressure)\b",
            r"\b(?:dilutive acquisition|equity issuance|cash burn)\b.{0,90}\b(?:pressure|risk|concern|negative)\b",
        ),
    ),
    EventSpec(
        "buyback_increase",
        0.5,
        (
            r"\b(?:authorized|increased|expanded|raised|resumed)\b.{0,90}\b(?:share repurchase|stock repurchase|buyback|buy-back)\b",
            r"\b(?:share repurchase|stock repurchase|buyback|buy-back)\b.{0,90}\b(?:authorized|increase|increased|expanded|raised|resumed)\b",
        ),
    ),
    EventSpec(
        "buyback_cut",
        -0.5,
        (
            r"\b(?:suspend|suspended|pause|paused|reduce|reduced|cut|cancel|cancelled)\b.{0,90}\b(?:share repurchase|stock repurchase|buyback|buy-back)\b",
            r"\b(?:share repurchase|stock repurchase|buyback|buy-back)\b.{0,90}\b(?:suspend|suspended|pause|paused|reduce|reduced|cut|cancel|cancelled)\b",
        ),
    ),
    EventSpec(
        "dividend_increase",
        0.4,
        (
            r"\b(?:increase|increased|raises|raised|hike|hiked)\b.{0,90}\b(?:dividend|quarterly dividend|cash dividend)\b",
            r"\b(?:dividend|quarterly dividend|cash dividend)\b.{0,90}\b(?:increase|increased|raises|raised|hike|hiked)\b",
        ),
    ),
    EventSpec(
        "dividend_cut",
        -0.75,
        (
            r"\b(?:cut|cuts|reduced|reduce|suspend|suspended|eliminate|eliminated)\b.{0,90}\b(?:dividend|quarterly dividend|cash dividend)\b",
            r"\b(?:dividend|quarterly dividend|cash dividend)\b.{0,90}\b(?:cut|cuts|reduced|reduce|suspend|suspended|eliminate|eliminated)\b",
        ),
    ),
    EventSpec(
        "customer_concentration",
        -0.75,
        (
            r"\b(?:customer concentration|major customer|largest customer|top customer|key customer|customer accounted for)\b",
            r"\b(?:customer|customers)\b.{0,70}\b(?:accounted for|represent|represents|represented)\b.{0,40}\b(?:revenue|sales)\b",
            r"\b(?:lost|loss of)\b.{0,60}\b(?:major|largest|top|key)\b.{0,30}\bcustomer\b",
        ),
    ),
    EventSpec(
        "management_tone_positive",
        0.35,
        (
            r"\b(?:management|executives?|ceo|cfo)\b.{0,90}\b(?:confident|confidence|encouraged|optimistic|strong demand|improving visibility)\b",
            r"\b(?:confident|optimistic|encouraged)\b.{0,90}\b(?:outlook|demand|visibility|management)\b",
        ),
    ),
    EventSpec(
        "management_tone_negative",
        -0.5,
        (
            r"\b(?:management|executives?|ceo|cfo)\b.{0,90}\b(?:cautious|uncertain|concerned|pressure|weak demand|limited visibility)\b",
            r"\b(?:cautious|concerned|uncertain)\b.{0,90}\b(?:outlook|demand|visibility|management)\b",
        ),
    ),
    EventSpec(
        "management_uncertainty",
        -0.5,
        (
            r"\b(?:limited visibility|low visibility|uncertain outlook|uncertainty remains|cannot provide guidance|suspend guidance|suspended guidance)\b",
            r"\b(?:management|executives?|ceo|cfo)\b.{0,90}\b(?:uncertain|uncertainty|resign|resigned|departure|turnover|transition)\b",
        ),
    ),
    EventSpec(
        "macro_positive_surprise",
        0.4,
        (
            r"\b(?:above expectations|better than expected|positive surprise|stronger than expected)\b.{0,90}\b(?:inflation|jobs|payrolls|gdp|sales|pmi|macro|economic)\b",
            r"\b(?:inflation|jobs|payrolls|gdp|sales|pmi|macro|economic)\b.{0,90}\b(?:above expectations|better than expected|positive surprise|stronger than expected)\b",
        ),
    ),
    EventSpec(
        "macro_negative_surprise",
        -0.4,
        (
            r"\b(?:below expectations|worse than expected|negative surprise|weaker than expected)\b.{0,90}\b(?:inflation|jobs|payrolls|gdp|sales|pmi|macro|economic)\b",
            r"\b(?:inflation|jobs|payrolls|gdp|sales|pmi|macro|economic)\b.{0,90}\b(?:below expectations|worse than expected|negative surprise|weaker than expected)\b",
        ),
    ),
)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return int(default)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    if not math.isfinite(out):
        return float(default)
    return float(out)


def _json_loads(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except Exception:
        return {}
    return dict(parsed) if isinstance(parsed, dict) else {}


def _json_dumps(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True, default=str)


def _row_get(row: Any, key: str, idx: int, default: Any = None) -> Any:
    try:
        if hasattr(row, "keys"):
            return row[key]
    except Exception:
        pass
    try:
        return row[idx]
    except Exception:
        return default


def _norm_symbol(value: Any) -> str | None:
    text = str(value or "").upper().strip()
    return text or None


def _clean_text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _stable_id(parts: Sequence[Any]) -> str:
    raw = "|".join(str(part or "") for part in parts)
    return hashlib.sha1(raw.encode("utf-8", errors="ignore")).hexdigest()


def _document_type(payload: Mapping[str, Any], meta: Mapping[str, Any]) -> str:
    event_type = str(payload.get("event_type") or payload.get("type") or "").strip().lower()
    source = str(payload.get("source") or "").strip().lower()
    if event_type == "filing" or source.startswith("sec"):
        return "filing"
    if bool(meta.get("transcript")) or source == "fmp_transcript" or "transcript" in source:
        return "transcript"
    return "news"


def _source_document_id(payload: Mapping[str, Any], meta: Mapping[str, Any], document_type: str) -> str:
    for key in ("source_id", "event_key", "dedupe_hash", "url"):
        value = str(payload.get(key) or "").strip()
        if value:
            return value
    for key in ("artifact_sha256", "artifact_alias", "source_id"):
        value = str(meta.get(key) or "").strip()
        if value:
            return value
    return f"{document_type}:{_stable_id([payload.get('symbol'), payload.get('source'), payload.get('title'), payload.get('body')])}"


def _evidence_window(text: str, start: int, end: int, width: int = 120) -> str:
    left = max(0, int(start) - int(width))
    right = min(len(text), int(end) + int(width))
    return _clean_text(text[left:right])[:500]


def _confidence_for_matches(matches: Sequence[re.Match[str]], *, document_type: str) -> float:
    base = 0.48
    if document_type == "filing":
        base += 0.08
    elif document_type == "transcript":
        base += 0.06
    score = base + min(0.34, 0.11 * len(list(matches or [])))
    longest = max((len(match.group(0)) for match in matches or []), default=0)
    if longest >= 40:
        score += 0.06
    return float(max(0.0, min(0.99, score)))


def extract_structured_document_events(payload: Mapping[str, Any]) -> List[Dict[str, Any]]:
    """Extract structured event rows from a normalized document/event payload."""

    source_payload = dict(payload or {})
    meta = _json_loads(source_payload.get("meta_json"))
    derived = _json_loads(source_payload.get("derived_features"))
    if isinstance((meta.get("raw_payload") or {}), dict):
        raw_payload = dict(meta.get("raw_payload") or {})
    else:
        raw_payload = _json_loads(source_payload.get("raw_payload"))
    document_type = _document_type(source_payload, meta)
    if document_type not in {"filing", "transcript", "news"}:
        return []

    title = str(source_payload.get("title") or raw_payload.get("title") or "")
    body = str(source_payload.get("body") or raw_payload.get("body") or raw_payload.get("summary") or "")
    text = "\n".join(part for part in (title, body) if str(part or "").strip()).strip()
    if not text:
        return []

    symbol = _norm_symbol(source_payload.get("symbol") or raw_payload.get("symbol") or meta.get("matched_symbol"))
    event_ts_ms = _safe_int(
        source_payload.get("ts_ms")
        or source_payload.get("timestamp")
        or raw_payload.get("ts_ms")
        or raw_payload.get("timestamp")
        or raw_payload.get("filing_ts_ms"),
        _now_ms(),
    )
    availability_ts_ms = _safe_int(
        source_payload.get("availability_ts_ms")
        or raw_payload.get("availability_ts_ms")
        or raw_payload.get("filing_ts_ms")
        or (meta.get("pipeline_timing") or {}).get("db_observed_ts_ms")
        or event_ts_ms,
        event_ts_ms,
    )
    source_document_id = _source_document_id(source_payload, meta, document_type)
    source_event_id = source_payload.get("event_id") or source_payload.get("id")
    created_ts_ms = _now_ms()
    lower_text = text.lower()

    rows: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for spec in EVENT_SPECS:
        matches: List[re.Match[str]] = []
        for pattern in spec.patterns:
            matches.extend(list(re.finditer(pattern, lower_text, flags=re.IGNORECASE | re.DOTALL)))
        if not matches:
            continue
        feature_id = EVENT_FEATURE_ID[spec.event_type]
        key = f"{source_document_id}|{symbol or ''}|{spec.event_type}|{event_ts_ms}|{EXTRACTOR_VERSION}"
        if key in seen:
            continue
        seen.add(key)
        confidence = _confidence_for_matches(matches, document_type=document_type)
        first = matches[0]
        pit_metadata = {
            "pit_eligible": True,
            "source_timestamp_field": "event_ts_ms",
            "availability_timestamp_field": "availability_ts_ms",
            "source_event_ts_ms": int(event_ts_ms),
            "availability_ts_ms": int(availability_ts_ms),
            "extraction_created_ts_ms": int(created_ts_ms),
            "lag_policy": "document_availability_timestamp",
            "direct_trading_authority": False,
            "extractor_name": EXTRACTOR_NAME,
            "extractor_version": EXTRACTOR_VERSION,
        }
        rows.append(
            {
                "source_document_id": source_document_id,
                "source_event_id": int(source_event_id) if source_event_id is not None and str(source_event_id).strip() else None,
                "symbol": symbol,
                "document_type": document_type,
                "source": str(source_payload.get("source") or raw_payload.get("source") or "unknown"),
                "event_type": spec.event_type,
                "event_ts_ms": int(event_ts_ms),
                "availability_ts_ms": int(max(event_ts_ms, availability_ts_ms)),
                "extraction_confidence": float(confidence),
                "polarity": float(spec.polarity),
                "feature_id": feature_id,
                "evidence": _evidence_window(text, first.start(), first.end()),
                "extractor_name": EXTRACTOR_NAME,
                "extractor_version": EXTRACTOR_VERSION,
                "created_ts_ms": int(created_ts_ms),
                "payload_json": {
                    "title": title[:500],
                    "matched_text": _clean_text(first.group(0))[:300],
                    "match_count": int(len(matches)),
                    "document_type": document_type,
                    "derived_feature_keys": sorted(str(key) for key in derived.keys())[:50],
                },
                "pit_metadata_json": pit_metadata,
            }
        )
    return rows


def ensure_structured_document_event_schema(con) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS structured_document_events (
            id BIGSERIAL PRIMARY KEY,
            source_document_id TEXT NOT NULL,
            source_event_id BIGINT,
            symbol TEXT NOT NULL DEFAULT '',
            document_type TEXT NOT NULL,
            source TEXT,
            event_type TEXT NOT NULL,
            event_ts_ms BIGINT NOT NULL,
            availability_ts_ms BIGINT NOT NULL,
            extraction_confidence DOUBLE PRECISION NOT NULL,
            polarity DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            feature_id TEXT NOT NULL,
            evidence TEXT,
            extractor_name TEXT NOT NULL,
            extractor_version TEXT NOT NULL,
            created_ts_ms BIGINT NOT NULL,
            payload_json JSONB,
            pit_metadata_json JSONB
        )
        """
    )
    con.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_structured_document_events_doc_type_ts
          ON structured_document_events(source_document_id, symbol, event_type, event_ts_ms, extractor_version)
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_structured_document_events_symbol_avail
          ON structured_document_events(symbol, availability_ts_ms DESC)
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_structured_document_events_source_event
          ON structured_document_events(source_event_id)
        """
    )


def put_structured_document_events(con, rows: Sequence[Mapping[str, Any]]) -> int:
    """Upsert extracted structured document events into ``structured_document_events``."""

    clean_rows = [dict(row or {}) for row in list(rows or []) if row]
    if not clean_rows:
        return 0
    ensure_structured_document_event_schema(con)
    written = 0
    for row in clean_rows:
        con.execute(
            """
            INSERT INTO structured_document_events(
              source_document_id, source_event_id, symbol, document_type, source,
              event_type, event_ts_ms, availability_ts_ms, extraction_confidence,
              polarity, feature_id, evidence, extractor_name, extractor_version,
              created_ts_ms, payload_json, pit_metadata_json
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(source_document_id, symbol, event_type, event_ts_ms, extractor_version)
            DO UPDATE SET
              source_event_id=COALESCE(excluded.source_event_id, structured_document_events.source_event_id),
              document_type=excluded.document_type,
              source=excluded.source,
              availability_ts_ms=excluded.availability_ts_ms,
              extraction_confidence=excluded.extraction_confidence,
              polarity=excluded.polarity,
              feature_id=excluded.feature_id,
              evidence=excluded.evidence,
              created_ts_ms=excluded.created_ts_ms,
              payload_json=excluded.payload_json,
              pit_metadata_json=excluded.pit_metadata_json
            """,
            (
                str(row.get("source_document_id") or ""),
                row.get("source_event_id"),
                _norm_symbol(row.get("symbol")) or "",
                str(row.get("document_type") or "news"),
                str(row.get("source") or "unknown"),
                str(row.get("event_type") or ""),
                int(row.get("event_ts_ms") or 0),
                int(row.get("availability_ts_ms") or row.get("event_ts_ms") or 0),
                float(row.get("extraction_confidence") or 0.0),
                float(row.get("polarity") or 0.0),
                str(row.get("feature_id") or EVENT_FEATURE_ID.get(str(row.get("event_type") or ""), "")),
                str(row.get("evidence") or ""),
                str(row.get("extractor_name") or EXTRACTOR_NAME),
                str(row.get("extractor_version") or EXTRACTOR_VERSION),
                int(row.get("created_ts_ms") or _now_ms()),
                _json_dumps(row.get("payload_json") or {}),
                _json_dumps(row.get("pit_metadata_json") or {}),
            ),
        )
        written += 1
    return int(written)


def resolve_structured_document_event_features(
    con,
    *,
    symbol: str,
    ts_ms: int,
    lookback_days: int = LOOKBACK_DAYS_DEFAULT,
) -> Tuple[Dict[str, float], Dict[str, Any], bool]:
    """Resolve PIT-safe structured document event features as of ``ts_ms``."""

    features = {fid: 0.0 for fid in STRUCTURED_DOCUMENT_EVENT_FEATURE_IDS}
    sym = _norm_symbol(symbol)
    if not sym:
        return features, {"latest_availability_ts_ms": None, "latest_event_ts_ms": None}, False

    try:
        ensure_structured_document_event_schema(con)
    except Exception:
        return features, {"latest_availability_ts_ms": None, "latest_event_ts_ms": None}, False

    anchor = int(ts_ms)
    window_start = anchor - max(1, int(lookback_days)) * MS_DAY
    rows = con.execute(
        """
        SELECT event_type, feature_id, event_ts_ms, availability_ts_ms,
               extraction_confidence, source_document_id, document_type
        FROM structured_document_events
        WHERE symbol = ?
          AND availability_ts_ms <= ?
          AND availability_ts_ms >= ?
        ORDER BY availability_ts_ms DESC, id DESC
        """,
        (sym, int(anchor), int(window_start)),
    ).fetchall()
    if not rows:
        return (
            features,
            {
                "latest_availability_ts_ms": None,
                "latest_event_ts_ms": None,
                "window_start_ts_ms": int(window_start),
                "lookback_days": int(lookback_days),
                "event_count": 0,
            },
            False,
        )

    latest_availability = 0
    latest_event = 0
    fresh_count = 0
    document_ids: List[str] = []
    event_types: set[str] = set()
    cutoff_30d = anchor - FRESH_COUNT_DAYS * MS_DAY
    for row in rows:
        event_type = str(_row_get(row, "event_type", 0) or "")
        feature_id = str(_row_get(row, "feature_id", 1) or EVENT_FEATURE_ID.get(event_type, ""))
        event_ts = _safe_int(_row_get(row, "event_ts_ms", 2), 0)
        availability_ts = _safe_int(_row_get(row, "availability_ts_ms", 3), 0)
        confidence = _safe_float(_row_get(row, "extraction_confidence", 4), 0.0)
        source_doc = str(_row_get(row, "source_document_id", 5) or "")
        if feature_id in features:
            features[feature_id] = max(float(features.get(feature_id, 0.0)), float(confidence))
        latest_availability = max(int(latest_availability), int(availability_ts))
        latest_event = max(int(latest_event), int(event_ts))
        if availability_ts >= cutoff_30d:
            fresh_count += 1
        if source_doc and source_doc not in document_ids and len(document_ids) < 12:
            document_ids.append(source_doc)
        if event_type:
            event_types.add(event_type)

    features["structured_doc_events_v1.event_count_30d"] = float(fresh_count)
    features["structured_doc_events_v1.latest_event_age_days"] = (
        float(max(0, anchor - latest_availability) / MS_DAY) if latest_availability > 0 else 0.0
    )
    return (
        {str(k): float(v or 0.0) for k, v in features.items()},
        {
            "latest_availability_ts_ms": int(latest_availability) if latest_availability > 0 else None,
            "latest_event_ts_ms": int(latest_event) if latest_event > 0 else None,
            "window_start_ts_ms": int(window_start),
            "lookback_days": int(lookback_days),
            "event_count": int(len(rows)),
            "event_count_30d": int(fresh_count),
            "source_document_ids": list(document_ids),
            "event_types": sorted(event_types),
            "extractor_name": EXTRACTOR_NAME,
            "extractor_version": EXTRACTOR_VERSION,
            "direct_trading_authority": False,
        },
        True,
    )


__all__ = [
    "EVENT_FEATURE_ID",
    "EXTRACTOR_NAME",
    "EXTRACTOR_VERSION",
    "STRUCTURED_DOCUMENT_EVENT_FEATURE_IDS",
    "ensure_structured_document_event_schema",
    "extract_structured_document_events",
    "put_structured_document_events",
    "resolve_structured_document_event_features",
]
