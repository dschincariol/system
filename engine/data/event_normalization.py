"""
Shared event normalization helpers for non-price ingestion pipelines.
"""

import hashlib
import json
import logging
import math
import time
from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger

LOG = get_logger("data.event_normalization")


def _warn_nonfatal(code: str, error: BaseException, **extra: Any) -> None:
    log_failure(
        LOG,
        event="data_event_normalization_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.data.event_normalization",
        extra=dict(extra or {}) or None,
        persist=False,
    )


def _now_ms() -> int:
    return int(time.time() * 1000)


def _json_loads(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    if not raw:
        return {}
    try:
        obj = json.loads(str(raw))
        if isinstance(obj, dict):
            return obj
        _warn_nonfatal(
            "EVENT_NORMALIZATION_JSON_LOADS_INVALID_TYPE",
            RuntimeError(f"json_root_type_invalid:{type(obj).__name__}"),
            value=repr(raw)[:120],
            json_type=type(obj).__name__,
        )
        return {"_parse_error": True}
    except Exception as e:
        _warn_nonfatal(
            "EVENT_NORMALIZATION_JSON_LOADS_FAILED",
            e,
            value=repr(raw)[:120],
        )
        return {"_parse_error": True}


def _json_dumps(raw: Any) -> Optional[str]:
    if raw is None:
        return None
    return json.dumps(raw, separators=(",", ":"), sort_keys=True)


def _norm_symbol(value: Any) -> Optional[str]:
    text = str(value or "").strip().upper()
    return text or None


def _sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()


def _source_reliability(source: str) -> float:
    src = str(source or "").strip().lower()
    if src.startswith("rss:reuters") or src.startswith("rss:bloomberg"):
        return 0.95
    if src in {"sec_form4", "form4"} or src.startswith("sec_form4"):
        return 0.94
    if src in {"gdelt", "macro"} or src.startswith("macro") or src.startswith("rss:"):
        return 0.80
    if src in {"sec", "earnings_calendar", "weather_forecast", "weather_alert"}:
        return 0.92
    if src in {"congressional_trade", "congressional_trades"} or src.startswith("congressional"):
        return 0.72
    if src.endswith("stock_watcher") or "stock_watcher" in src:
        return 0.72
    if src.startswith("social_stocktwits"):
        return 0.52
    if src.startswith("social_reddit"):
        return 0.48
    if src.startswith("social_"):
        return 0.50
    return 0.60


def _recency_score(timestamp_ms: int, now_ms: Optional[int] = None) -> float:
    now_ms = int(now_ms or _now_ms())
    age_ms = max(0, now_ms - int(timestamp_ms or now_ms))
    age_hours = age_ms / 3600000.0
    return float(math.exp(-age_hours / 24.0))


def _score_importance(*, recency: float, reliability: float, novelty: float, base: float = 0.5) -> float:
    score = (0.45 * float(recency)) + (0.30 * float(reliability)) + (0.25 * float(novelty))
    score = max(float(base), score)
    return float(max(0.0, min(1.0, score)))


@dataclass
class NormalizedEvent:
    event_type: str
    symbol: Optional[str]
    source: str
    timestamp: int
    importance_score: float
    raw_payload: Dict[str, Any]
    derived_features: Dict[str, Any]
    title: str
    body: Optional[str] = None
    url: Optional[str] = None
    source_id: Optional[str] = None
    dedupe_hash: Optional[str] = None
    event_key: Optional[str] = None
    meta_json: Optional[str] = None

    def to_record(self) -> Dict[str, Any]:
        out = asdict(self)
        out["ts_ms"] = int(self.timestamp)
        return out


def _finalize_normalized_event(
    *,
    event_type: str,
    symbol: Optional[str],
    source: str,
    timestamp: Any,
    raw_payload: Dict[str, Any],
    derived_features: Dict[str, Any],
    title: str,
    body: Optional[str] = None,
    url: Optional[str] = None,
    source_id: Optional[str] = None,
    dedupe_hash: Optional[str] = None,
    event_key: Optional[str] = None,
) -> Dict[str, Any]:
    ts_ms = int(timestamp or _now_ms())
    src = str(source or "unknown")
    payload = dict(raw_payload or {})
    features = dict(derived_features or {})
    reliability = float(features.get("source_reliability", _source_reliability(src)))
    recency = float(features.get("recency_score", _recency_score(ts_ms)))
    novelty = float(features.get("novelty", 1.0))
    importance = _score_importance(
        recency=recency,
        reliability=reliability,
        novelty=novelty,
        base=float(features.get("importance_floor", 0.0) or 0.0),
    )
    source_id_text = str(source_id).strip() if source_id is not None and str(source_id).strip() else None
    dedupe_text = dedupe_hash or _sha1(
        "|".join(
            [
                str(event_type or ""),
                str(src),
                str(_norm_symbol(symbol) or ""),
                str(source_id_text or ""),
                str(url or ""),
                str(title or ""),
                str(body or ""),
            ]
        )
    )
    event_key_text = event_key or (f"{src}:{source_id_text}" if source_id_text else dedupe_text)
    features.setdefault("source_reliability", reliability)
    features.setdefault("recency_score", recency)
    features.setdefault("novelty", novelty)
    features.setdefault("importance_components", {"recency": recency, "source_reliability": reliability, "novelty": novelty})
    meta = {
        "event_type": str(event_type),
        "symbol": _norm_symbol(symbol),
        "source": src,
        "source_id": source_id_text,
        "importance_score": importance,
        "raw_payload": payload,
        "derived_features": features,
    }
    normalized = NormalizedEvent(
        event_type=str(event_type),
        symbol=_norm_symbol(symbol),
        source=src,
        timestamp=ts_ms,
        importance_score=importance,
        raw_payload=payload,
        derived_features=features,
        title=str(title or src),
        body=body,
        url=url,
        source_id=source_id_text,
        dedupe_hash=str(dedupe_text),
        event_key=str(event_key_text),
        meta_json=_json_dumps(meta),
    )
    return normalized.to_record()


def normalize_news_event(raw: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(raw or {})
    meta = _json_loads(payload.get("meta_json"))
    existing_features = _json_loads(payload.get("derived_features"))
    symbol = payload.get("symbol") or meta.get("symbol") or meta.get("matched_symbol")
    features = {
        "taxonomy": meta.get("taxonomy") or [],
        "entities": meta.get("entities") or [],
        "matched_symbol": meta.get("matched_symbol"),
        "matched_symbols": meta.get("matched_symbols") or existing_features.get("matched_symbols") or [],
        "provider": meta.get("provider"),
        "cluster_key": meta.get("cluster_key") or existing_features.get("cluster_key"),
        "headline_key": meta.get("headline_key") or existing_features.get("headline_key"),
        "sentiment_score": meta.get("sentiment_score", existing_features.get("sentiment_score")),
        "headline_similarity": existing_features.get("headline_similarity"),
        "duplicate_count": existing_features.get("duplicate_count"),
        "is_duplicate": existing_features.get("is_duplicate"),
        "symbol_match_method": meta.get("symbol_match_method") or existing_features.get("symbol_match_method"),
        "symbol_match_confidence": meta.get("symbol_match_confidence", existing_features.get("symbol_match_confidence")),
        "transcript": existing_features.get("transcript") or bool(meta.get("transcript_meta")),
        "transcript_speaker_count": existing_features.get("transcript_speaker_count"),
        "transcript_has_qa": existing_features.get("transcript_has_qa"),
        "transcript_meta": meta.get("transcript_meta"),
        "source_reliability": _source_reliability(str(payload.get("source") or "news")),
        "novelty": meta.get("novelty", existing_features.get("novelty", 1.0)),
    }
    return _finalize_normalized_event(
        event_type=str(payload.get("event_type") or "news"),
        symbol=symbol,
        source=str(payload.get("source") or "news"),
        timestamp=payload.get("ts_ms") or payload.get("timestamp"),
        raw_payload=payload,
        derived_features=features,
        title=str(payload.get("title") or "News event"),
        body=payload.get("body"),
        url=payload.get("url"),
        source_id=payload.get("source_id") or payload.get("event_key") or payload.get("url"),
        dedupe_hash=payload.get("dedupe_hash"),
        event_key=payload.get("event_key"),
    )


def normalize_social_event(raw: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(raw or {})
    symbol = _norm_symbol(payload.get("symbol"))
    source = f"social_{str(payload.get('platform') or payload.get('source') or 'unknown')}"
    attention = float(payload.get("engagement_score") or 0.0)
    if not attention:
        attention = float(
            (payload.get("like_count") or 0)
            + (payload.get("reply_count") or 0)
            + (payload.get("repost_count") or 0)
            + (payload.get("quote_count") or 0)
        )
    return _finalize_normalized_event(
        event_type="social",
        symbol=symbol,
        source=source,
        timestamp=payload.get("ts_ms") or payload.get("timestamp"),
        raw_payload=payload,
        derived_features={
            "platform": payload.get("platform"),
            "subreddit": payload.get("subreddit"),
            "engagement_score": attention,
            "author_id_hash": payload.get("author_id_hash"),
            "source_reliability": _source_reliability(source),
            "importance_floor": min(0.85, 0.35 + (min(attention, 100.0) / 200.0)),
        },
        title=str(payload.get("title") or f"{symbol or 'market'} social signal"),
        body=payload.get("body") or payload.get("text"),
        url=payload.get("url"),
        source_id=payload.get("post_id"),
        dedupe_hash=payload.get("dedupe_hash"),
        event_key=payload.get("event_key"),
    )


def normalize_filings_event(raw: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(raw or {})
    form = str(payload.get("form") or "").upper()
    return _finalize_normalized_event(
        event_type="filing",
        symbol=_norm_symbol(payload.get("symbol")),
        source=str(payload.get("source") or "sec"),
        timestamp=payload.get("ts_ms") or payload.get("timestamp"),
        raw_payload=payload,
        derived_features={
            "form": form,
            "filed_date": payload.get("filed_date"),
            "report_date": payload.get("report_date"),
            "company_name": payload.get("company_name"),
            "source_reliability": _source_reliability(str(payload.get("source") or "sec")),
            "importance_floor": 0.70 if form in {"8-K", "10-Q", "10-K", "6-K"} else 0.58,
        },
        title=str(payload.get("title") or f"{payload.get('symbol') or ''} {form} filing").strip(),
        body=payload.get("body") or payload.get("company_name"),
        url=payload.get("url") or payload.get("primary_doc_url"),
        source_id=payload.get("source_id") or payload.get("accession"),
        dedupe_hash=payload.get("dedupe_hash"),
        event_key=payload.get("event_key"),
    )


def normalize_earnings_event(raw: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(raw or {})
    return _finalize_normalized_event(
        event_type="earnings",
        symbol=_norm_symbol(payload.get("symbol")),
        source=str(payload.get("source") or "earnings_calendar"),
        timestamp=payload.get("ts_ms") or payload.get("timestamp"),
        raw_payload=payload,
        derived_features={
            "earnings_date": payload.get("earnings_date") or payload.get("date"),
            "time_of_day": payload.get("time_of_day") or payload.get("time"),
            "eps_est": payload.get("eps_est") or payload.get("epsEstimated"),
            "eps_act": payload.get("eps_act") or payload.get("eps"),
            "revenue_est": payload.get("revenue_est") or payload.get("revenueEstimated"),
            "revenue_act": payload.get("revenue_act") or payload.get("revenue"),
            "source_reliability": _source_reliability(str(payload.get("source") or "earnings_calendar")),
            "importance_floor": 0.72,
        },
        title=str(payload.get("title") or f"{payload.get('symbol') or ''} earnings scheduled").strip(),
        body=payload.get("body"),
        url=payload.get("url"),
        source_id=payload.get("source_id") or f"{payload.get('symbol') or ''}:{payload.get('earnings_date') or payload.get('date') or ''}",
        dedupe_hash=payload.get("dedupe_hash"),
        event_key=payload.get("event_key"),
    )


def normalize_insider_event(raw: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(raw or {})
    diagnostics = _json_loads(payload.get("diagnostics_json"))
    direction = str(payload.get("direction") or "").strip().lower() or "neutral"
    target = str(payload.get("symbol") or payload.get("issuer_name") or "issuer").strip()
    action = "buy" if direction == "buy" else ("sell" if direction == "sell" else "trade")
    value = payload.get("value")
    shares = payload.get("shares")
    body_parts = [str(payload.get("insider_name") or "").strip(), str(payload.get("insider_role") or "").strip()]
    if shares is not None:
        body_parts.append(f"shares={shares}")
    if value is not None:
        body_parts.append(f"value={value}")
    title = str(payload.get("title") or f"{target} insider {action}").strip()
    return _finalize_normalized_event(
        event_type="insider",
        symbol=_norm_symbol(payload.get("symbol")),
        source=str(payload.get("source") or "sec_form4"),
        timestamp=payload.get("transaction_ts_ms") or payload.get("filing_ts_ms") or payload.get("ingested_ts_ms"),
        raw_payload=payload,
        derived_features={
            "entity_id": payload.get("entity_id"),
            "issuer_name": payload.get("issuer_name"),
            "issuer_cik": payload.get("issuer_cik"),
            "insider_name": payload.get("insider_name"),
            "insider_role": payload.get("insider_role"),
            "insider_title": payload.get("insider_title"),
            "transaction_code": payload.get("transaction_code"),
            "transaction_type": payload.get("transaction_type"),
            "direction": direction,
            "shares": payload.get("shares"),
            "price": payload.get("price"),
            "value": payload.get("value"),
            "security_type": payload.get("security_type"),
            "ownership_nature": payload.get("ownership_nature"),
            "filing_accession": payload.get("filing_accession"),
            "filing_date": payload.get("filing_date"),
            "transaction_date": payload.get("transaction_date"),
            "resolution_status": payload.get("resolution_status"),
            "resolution_method": payload.get("resolution_method"),
            "symbol_match_method": diagnostics.get("symbol_match_method"),
            "symbol_match_confidence": diagnostics.get("symbol_match_confidence"),
            "source_reliability": _source_reliability(str(payload.get("source") or "sec_form4")),
            "importance_floor": 0.74 if direction == "buy" else (0.71 if direction == "sell" else 0.64),
        },
        title=title,
        body=payload.get("body") or ", ".join(part for part in body_parts if part),
        url=payload.get("url") or payload.get("filing_url"),
        source_id=payload.get("source_transaction_id"),
        dedupe_hash=payload.get("dedupe_hash"),
        event_key=payload.get("event_key") or (f"form4:{payload.get('source_transaction_id')}" if payload.get("source_transaction_id") else None),
    )


def normalize_congressional_event(raw: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(raw or {})
    diagnostics = _json_loads(payload.get("diagnostics_json"))
    direction = str(payload.get("direction") or "").strip().lower() or "neutral"
    actor = str(payload.get("politician_name") or "Congressional trade").strip()
    target = str(payload.get("symbol") or payload.get("issuer_name") or "issuer").strip()
    action = "buy" if direction == "buy" else ("sell" if direction == "sell" else "trade")
    amount_mid = payload.get("amount_mid")
    body_parts = [actor]
    if payload.get("chamber"):
        body_parts.append(str(payload.get("chamber")))
    if amount_mid is not None:
        body_parts.append(f"amount_mid={amount_mid}")
    title = str(payload.get("title") or f"{actor} {action} disclosure for {target}").strip()
    return _finalize_normalized_event(
        event_type="congressional",
        symbol=_norm_symbol(payload.get("symbol")),
        source=str(payload.get("source") or "congressional_trade"),
        timestamp=payload.get("transaction_ts_ms") or payload.get("disclosure_ts_ms") or payload.get("ingested_ts_ms"),
        raw_payload=payload,
        derived_features={
            "entity_id": payload.get("entity_id"),
            "politician_name": payload.get("politician_name"),
            "chamber": payload.get("chamber"),
            "office": payload.get("office"),
            "issuer_name": payload.get("issuer_name"),
            "transaction_type_raw": payload.get("transaction_type_raw"),
            "transaction_type": payload.get("transaction_type"),
            "direction": direction,
            "amount_range": payload.get("amount_range"),
            "amount_low": payload.get("amount_low"),
            "amount_high": payload.get("amount_high"),
            "amount_mid": payload.get("amount_mid"),
            "transaction_date": payload.get("transaction_date"),
            "disclosure_date": payload.get("disclosure_date"),
            "resolution_status": payload.get("resolution_status"),
            "resolution_method": payload.get("resolution_method"),
            "symbol_match_method": diagnostics.get("symbol_match_method"),
            "symbol_match_confidence": diagnostics.get("symbol_match_confidence"),
            "source_reliability": _source_reliability(str(payload.get("source") or "congressional_trade")),
            "importance_floor": 0.66 if direction in {"buy", "sell"} else 0.58,
        },
        title=title,
        body=payload.get("body") or ", ".join(part for part in body_parts if part),
        url=payload.get("url") or payload.get("source_url"),
        source_id=payload.get("source_trade_id"),
        dedupe_hash=payload.get("dedupe_hash"),
        event_key=payload.get("event_key") or (
            f"congress:{payload.get('source_trade_id')}" if payload.get("source_trade_id") else None
        ),
    )


def normalize_weather_event(raw: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(raw or {})
    kind = str(payload.get("weather_kind") or payload.get("event_type") or "weather").strip().lower()
    event_type = "weather_alert" if "alert" in kind else "weather_forecast"
    symbol = _norm_symbol(payload.get("symbol"))
    if not symbol:
        regions = payload.get("affected_regions") or payload.get("regions") or []
        if isinstance(regions, list) and len(regions) == 1:
            symbol = None
    floor = 0.76 if event_type == "weather_alert" else 0.60
    return _finalize_normalized_event(
        event_type=event_type,
        symbol=symbol,
        source=str(payload.get("source") or event_type),
        timestamp=payload.get("ts_ms") or payload.get("timestamp") or payload.get("run_ts") or payload.get("issued_ts"),
        raw_payload=payload,
        derived_features={
            "provider": payload.get("provider"),
            "region_id": payload.get("region_id"),
            "affected_regions": payload.get("affected_regions"),
            "impact_channels": payload.get("impact_channels") or payload.get("channels") or [],
            "impact_weight": payload.get("impact_weight"),
            "affected_symbols": payload.get("affected_symbols") or [],
            "anomaly_score": payload.get("anomaly_score"),
            "extreme_event_score": payload.get("extreme_event_score"),
            "alert_severity": payload.get("alert_severity"),
            "severity": payload.get("severity"),
            "urgency": payload.get("urgency"),
            "certainty": payload.get("certainty"),
            "run_ts": payload.get("run_ts"),
            "day_ts": payload.get("day_ts"),
            "expires_ts": payload.get("expires_ts"),
            "source_reliability": _source_reliability(str(payload.get("source") or event_type)),
            "importance_floor": floor,
        },
        title=str(payload.get("title") or payload.get("headline") or "Weather event"),
        body=payload.get("body") or payload.get("description"),
        url=payload.get("url") or payload.get("source_uri"),
        source_id=payload.get("source_id") or payload.get("alert_id") or payload.get("forecast_id"),
        dedupe_hash=payload.get("dedupe_hash"),
        event_key=payload.get("event_key"),
    )


def normalize_macro_event(raw: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(raw or {})
    event_type = str(payload.get("event_type") or "macro").strip() or "macro"
    return _finalize_normalized_event(
        event_type=event_type,
        symbol=_norm_symbol(payload.get("symbol")),
        source=str(payload.get("source") or "macro"),
        timestamp=payload.get("ts_ms") or payload.get("timestamp"),
        raw_payload=payload,
        derived_features={
            "provider": payload.get("provider"),
            "topic": payload.get("topic"),
            "factor_id": payload.get("factor_id"),
            "release_date": payload.get("release_date"),
            "observation_date": payload.get("observation_date"),
            "mapped_symbols": payload.get("mapped_symbols") or [],
            "source_reliability": _source_reliability(str(payload.get("source") or "macro")),
            "importance_floor": 0.68,
        },
        title=str(payload.get("title") or "Macro event"),
        body=payload.get("body"),
        url=payload.get("url"),
        source_id=payload.get("source_id") or payload.get("event_key") or payload.get("url"),
        dedupe_hash=payload.get("dedupe_hash"),
        event_key=payload.get("event_key"),
    )


def normalize_legacy_event(
    *,
    ts_ms: Any,
    source: str,
    title: str,
    body: Optional[str],
    url: Optional[str],
    event_key: Optional[str],
    meta_json: Optional[str] = None,
) -> Dict[str, Any]:
    payload = {
        "ts_ms": ts_ms,
        "source": source,
        "title": title,
        "body": body,
        "url": url,
        "event_key": event_key,
        "meta_json": meta_json,
    }
    src = str(source or "").lower()
    if src.startswith("social_"):
        meta = _json_loads(meta_json)
        payload.update(meta)
        payload.setdefault("platform", meta.get("platform") or src.replace("social_", "", 1))
        payload.setdefault("text", body)
        return normalize_social_event(payload)
    if src in {"sec", "filings"}:
        payload.update(_json_loads(meta_json))
        return normalize_filings_event(payload)
    if src in {"earnings_calendar", "earnings"}:
        payload.update(_json_loads(meta_json))
        return normalize_earnings_event(payload)
    if src.startswith("weather_"):
        payload.update(_json_loads(meta_json))
        payload.setdefault("weather_kind", src)
        return normalize_weather_event(payload)
    if src in {"macro"}:
        payload.update(_json_loads(meta_json))
        return normalize_macro_event(payload)
    return normalize_news_event(payload)
