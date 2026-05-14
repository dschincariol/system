import hashlib
import json
import logging
import math
import os
import re
import time
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from engine.artifacts.store import LocalArtifactStore
from engine.data.universe import extract_symbol_candidates
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger

LOG = get_logger("data.ingest.news_enrichment")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="data_news_enrichment_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.data.ingest.news_enrichment",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _sec_ticker_map_path() -> Path:
    raw = str(os.environ.get("SEC_TICKER_MAP_CACHE", "data/sec_company_tickers_exchange.json") or "").strip()
    path = Path(raw)
    if path.is_absolute():
        return path
    return (Path(__file__).resolve().parents[3] / path).resolve()

_CORP_SUFFIXES = {
    "inc",
    "incorporated",
    "corp",
    "corporation",
    "co",
    "company",
    "plc",
    "limited",
    "ltd",
    "holdings",
    "holding",
    "group",
    "sa",
    "nv",
    "ag",
    "se",
    "spa",
    "lp",
    "llc",
}

_POSITIVE_WORDS = {
    "beat",
    "beats",
    "surge",
    "surges",
    "growth",
    "raise",
    "raises",
    "raised",
    "strong",
    "stronger",
    "profit",
    "profits",
    "upside",
    "record",
    "win",
    "wins",
    "bullish",
    "rebound",
    "expand",
    "expands",
    "expanded",
    "approval",
}

_NEGATIVE_WORDS = {
    "miss",
    "misses",
    "cut",
    "cuts",
    "warning",
    "warns",
    "warned",
    "weak",
    "weaker",
    "drop",
    "drops",
    "fall",
    "falls",
    "lawsuit",
    "probe",
    "fraud",
    "downgrade",
    "layoff",
    "layoffs",
    "delay",
    "delays",
    "decline",
    "declines",
}

_TOKEN_RX = re.compile(r"[a-z0-9]+")
_WHITESPACE_RX = re.compile(r"\s+")
_TRANSCRIPT_SPEAKER_RX = re.compile(r"^(?:[A-Z][A-Za-z .'-]{1,60}|Operator|Analyst|Unknown Speaker)\s*:\s*(.+)$")

_NAMED_ASSET_ALIASES = {
    "BTC": [r"\bbitcoin\b", r"\bbtc\b"],
    "ETH": [r"\bethereum\b", r"\bether\b", r"\beth\b"],
    "SOL": [r"\bsolana\b", r"\bsol\b"],
    "BNB": [r"\bbnb\b", r"\bbinance coin\b"],
    "XRP": [r"\bxrp\b", r"\bripple\b"],
    "AAPL": [r"\bapple\b", r"\bapple inc\.?\b"],
    "TSLA": [r"\btesla\b", r"\btesla inc\.?\b"],
    "NVDA": [r"\bnvidia\b", r"\bnvidia corp\.?\b"],
    "MSFT": [r"\bmicrosoft\b", r"\bmicrosoft corp\.?\b"],
    "AMZN": [r"\bamazon\b", r"\bamazon\.com\b"],
    "META": [r"\bmeta\b", r"\bmeta platforms\b", r"\bfacebook\b"],
    "GOOGL": [r"\balphabet\b", r"\bgoogle\b"],
    "NFLX": [r"\bnetflix\b"],
}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _clean_text(text: Any) -> str:
    return str(text or "").strip()


def _normalize_name(text: Any) -> str:
    raw = str(text or "").lower()
    raw = raw.replace("&", " and ")
    raw = re.sub(r"[^a-z0-9 ]+", " ", raw)
    raw = _WHITESPACE_RX.sub(" ", raw).strip()
    return raw


def _strip_company_suffixes(name: str) -> str:
    parts = [p for p in _normalize_name(name).split() if p]
    while parts and parts[-1] in _CORP_SUFFIXES:
        parts.pop()
    return " ".join(parts).strip()


def _headline_key(text: str) -> str:
    toks = _TOKEN_RX.findall(_normalize_name(text))
    return " ".join(toks[:20]).strip()


def _token_set(text: str) -> Set[str]:
    return set(_TOKEN_RX.findall(_normalize_name(text)))


def _token_counts(text: str) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for tok in _TOKEN_RX.findall(_normalize_name(text)):
        out[tok] = float(out.get(tok, 0.0) + 1.0)
    return out


def _jaccard(a: Set[str], b: Set[str]) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    if union <= 0:
        return 0.0
    return float(inter / union)


def _weighted_cosine(a: Dict[str, float], b: Dict[str, float]) -> float:
    if not a or not b:
        return 0.0
    dot = 0.0
    an = 0.0
    bn = 0.0
    for key, val in a.items():
        an += float(val) * float(val)
        dot += float(val) * float(b.get(key, 0.0))
    for val in b.values():
        bn += float(val) * float(val)
    if an <= 0.0 or bn <= 0.0:
        return 0.0
    return float(dot / math.sqrt(an * bn))


def _stable_hash(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()[:16]


def _store_text_body_artifact(payload: Dict[str, Any], *, title: str, body: str, meta: Dict[str, Any]) -> Dict[str, Any]:
    if not body or str(meta.get("artifact_sha256") or "").strip():
        return {}
    source = str(payload.get("source") or meta.get("provider") or "news").strip() or "news"
    source_id = str(payload.get("source_id") or payload.get("event_key") or payload.get("url") or title or body[:120])
    kind = "transcript" if bool(meta.get("transcript") or source == "fmp_transcript") else "news"
    alias = f"{kind}:{source}:{_stable_hash(source_id)}"
    try:
        ref = LocalArtifactStore().put(
            body.encode("utf-8"),
            content_type="text/plain; charset=utf-8",
            kind=kind,
            alias=alias,
            metadata={
                "source": source,
                "source_id": source_id,
                "title": str(title or ""),
                "url": str(payload.get("url") or ""),
                "body_len": len(body),
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
            "NEWS_ENRICHMENT_ARTIFACT_STORE_FAILED",
            exc,
            once_key=f"news_body_artifact_store_failed:{alias}",
            source=source,
            source_id=source_id[:160],
        )
        return {}


def _safe_json_loads(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except Exception as e:
        _warn_nonfatal(
            "NEWS_ENRICHMENT_SAFE_JSON_LOAD_FAILED",
            e,
            once_key="safe_json_load",
            value=repr(value)[:120],
        )
        return {}
    return parsed if isinstance(parsed, dict) else {}


def parse_transcript_metadata(text: str) -> Dict[str, Any]:
    body = _clean_text(text)
    if not body:
        return {}
    speakers: Set[str] = set()
    excerpt_lines: List[str] = []
    qa_markers = 0
    prepared_lines = [line.strip() for line in body.splitlines() if line.strip()]
    for line in prepared_lines[:250]:
        match = _TRANSCRIPT_SPEAKER_RX.match(line)
        if match:
            speaker = line.split(":", 1)[0].strip()
            if speaker:
                speakers.add(speaker)
            payload = match.group(1).strip()
            if payload and len(excerpt_lines) < 6:
                excerpt_lines.append(payload[:240])
        lower = line.lower()
        if "question-and-answer" in lower or "q&a" in lower or "question and answer" in lower:
            qa_markers += 1
    return {
        "speaker_count": int(len(speakers)),
        "speakers": sorted(speakers)[:12],
        "qa_markers": int(qa_markers),
        "has_qa": bool(qa_markers > 0),
        "excerpt": " ".join(excerpt_lines[:3]).strip(),
    }


@lru_cache(maxsize=1)
def _load_company_map() -> Dict[str, str]:
    path = _sec_ticker_map_path()
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        _warn_nonfatal(
            "NEWS_ENRICHMENT_COMPANY_MAP_LOAD_FAILED",
            e,
            once_key="company_map_load",
            path=str(path),
        )
        return {}
    fields = list(payload.get("fields") or [])
    data = list(payload.get("data") or [])
    try:
        idx_name = fields.index("name")
        idx_ticker = fields.index("ticker")
    except ValueError as e:
        _warn_nonfatal(
            "NEWS_ENRICHMENT_COMPANY_MAP_FIELDS_MISSING",
            e,
            once_key="company_map_fields",
            fields=fields[:20],
        )
        return {}

    out: Dict[str, str] = {}
    for row in data:
        if not isinstance(row, list):
            continue
        try:
            name = str(row[idx_name] or "").strip()
            ticker = str(row[idx_ticker] or "").strip().upper()
        except Exception as e:
            _warn_nonfatal(
                "NEWS_ENRICHMENT_COMPANY_MAP_ROW_PARSE_FAILED",
                e,
                once_key="company_map_row_parse",
            )
            continue
        if not name or not ticker:
            continue
        for alias in {_normalize_name(name), _strip_company_suffixes(name)}:
            if alias and len(alias) >= 4 and alias not in out:
                out[alias] = ticker
    return out


def _build_alias_map(allowed_symbols: Optional[Sequence[str]]) -> Dict[str, str]:
    base = _load_company_map()
    if not allowed_symbols:
        return base
    wanted = {str(sym or "").upper().strip() for sym in allowed_symbols if str(sym or "").strip()}
    return {alias: sym for alias, sym in base.items() if sym in wanted}


def _extract_company_matches(text: str, allowed_symbols: Optional[Sequence[str]]) -> List[Tuple[str, str, float]]:
    alias_map = _build_alias_map(allowed_symbols)
    if not alias_map:
        return []
    normalized = _normalize_name(text)
    matches: List[Tuple[str, str, float]] = []
    for alias, sym in alias_map.items():
        alias_norm = _normalize_name(alias)
        if len(alias_norm) < 4:
            continue
        if not allowed_symbols and len(alias_norm.split()) < 2:
            continue
        pattern = rf"(?<![a-z0-9]){re.escape(alias_norm)}(?![a-z0-9])"
        if not re.search(pattern, normalized, flags=re.IGNORECASE):
            continue
        conf = 0.93 if alias == _strip_company_suffixes(alias) else 0.86
        matches.append((sym, alias_norm, conf))
    matches.sort(key=lambda item: (-len(item[1]), item[0]))
    seen: Set[str] = set()
    deduped: List[Tuple[str, str, float]] = []
    for sym, alias, conf in matches:
        if sym in seen:
            continue
        seen.add(sym)
        deduped.append((sym, alias, conf))
    return deduped


def _extract_named_asset_matches(text: str, allowed_symbols: Optional[Sequence[str]]) -> List[Tuple[str, str, float]]:
    normalized = _normalize_name(text)
    if not normalized:
        return []
    out: List[Tuple[str, str, float]] = []
    for sym, patterns in _NAMED_ASSET_ALIASES.items():
        for pattern in patterns:
            try:
                if re.search(pattern, normalized, flags=re.IGNORECASE):
                    out.append((sym, pattern, 0.92))
                    break
            except re.error as e:
                _warn_nonfatal(
                    "NEWS_ENRICHMENT_NAMED_ASSET_PATTERN_FAILED",
                    e,
                    once_key=f"named_asset_pattern:{sym}",
                    symbol=sym,
                    pattern=pattern,
                )
                continue
    return out


def infer_symbols(raw_item: Dict[str, Any], allowed_symbols: Optional[Sequence[str]]) -> Dict[str, Any]:
    text = "\n".join(
        [
            _clean_text(raw_item.get("title")),
            _clean_text(raw_item.get("body")),
            _clean_text(raw_item.get("summary")),
        ]
    )
    allowed = {str(sym or "").upper().strip() for sym in (allowed_symbols or []) if str(sym or "").strip()}
    explicit: List[str] = []
    for key in ("symbol", "ticker"):
        value = str(raw_item.get(key) or "").strip().upper()
        if value:
            explicit.append(value)
    for sym in raw_item.get("symbols") or []:
        value = str(sym or "").strip().upper()
        if value:
            explicit.append(value)

    ticker_hits = extract_symbol_candidates(text)
    company_hits = _extract_company_matches(text, allowed_symbols)
    asset_hits = _extract_named_asset_matches(text, allowed_symbols)

    ordered: List[str] = []
    methods: Dict[str, str] = {}
    confidence: Dict[str, float] = {}

    def _add(sym: str, method: str, conf: float, *, require_allowed: bool) -> None:
        symbol = str(sym or "").strip().upper()
        if not symbol:
            return
        if require_allowed:
            if not allowed or symbol not in allowed:
                return
        if symbol not in ordered:
            ordered.append(symbol)
        methods[symbol] = method if methods.get(symbol) != "explicit" else methods[symbol]
        confidence[symbol] = max(float(conf), float(confidence.get(symbol) or 0.0))

    for sym in explicit:
        _add(sym, "explicit", 1.0, require_allowed=False)
    for sym in ticker_hits:
        _add(sym, "ticker", 0.74, require_allowed=True)
    for sym, alias, conf in company_hits:
        _add(sym, f"company_name:{alias}", conf, require_allowed=False)
    for sym, alias, conf in asset_hits:
        _add(sym, f"asset_name:{alias}", conf, require_allowed=False)

    return {
        "symbols": ordered,
        "match_method": methods,
        "match_confidence": confidence,
    }


def score_sentiment(title: str, body: str) -> float:
    text = f"{_clean_text(title)} {_clean_text(body)}".lower()
    if not text:
        return 0.0
    pos = sum(1 for w in _POSITIVE_WORDS if re.search(rf"\b{re.escape(w)}\b", text))
    neg = sum(1 for w in _NEGATIVE_WORDS if re.search(rf"\b{re.escape(w)}\b", text))
    total = pos + neg
    if total <= 0:
        return 0.0
    return float(max(-1.0, min(1.0, (pos - neg) / total)))


def _fetch_recent_candidates(con, symbol: Optional[str], ts_ms: int, lookback_hours: int = 36) -> List[Dict[str, Any]]:
    cutoff = int(ts_ms) - int(lookback_hours) * 3600_000
    if symbol:
        rows = con.execute(
            """
            SELECT e.id, e.ts_ms, e.source, e.title, e.body, e.url, nef.cluster_key, nef.headline_key
            FROM events e
            LEFT JOIN news_event_features nef ON nef.event_id = e.id
            WHERE e.ts_ms >= ?
              AND e.event_type = 'news'
              AND e.symbol = ?
            ORDER BY e.ts_ms DESC
            LIMIT 250
            """,
            (int(cutoff), str(symbol)),
        ).fetchall()
    else:
        rows = con.execute(
            """
            SELECT e.id, e.ts_ms, e.source, e.title, e.body, e.url, nef.cluster_key, nef.headline_key
            FROM events e
            LEFT JOIN news_event_features nef ON nef.event_id = e.id
            WHERE e.ts_ms >= ?
              AND e.event_type = 'news'
              AND e.symbol IS NULL
            ORDER BY e.ts_ms DESC
            LIMIT 150
            """,
            (int(cutoff),),
        ).fetchall()
    out: List[Dict[str, Any]] = []
    for row in rows or []:
        title = _clean_text(row[3])
        out.append(
            {
                "event_id": int(row[0]),
                "ts_ms": int(row[1]),
                "source": _clean_text(row[2]),
                "title": title,
                "body": _clean_text(row[4]),
                "url": _clean_text(row[5]),
                "cluster_key": _clean_text(row[6]),
                "headline_key": _clean_text(row[7]) or _headline_key(title),
                "token_set": _token_set(title),
                "semantic_counts": _token_counts(f"{title}\n{_clean_text(row[4])[:1000]}"),
            }
        )
    return out


def _cluster_event(
    con,
    *,
    symbol: Optional[str],
    title: str,
    body: str,
    ts_ms: int,
) -> Dict[str, Any]:
    headline_key = _headline_key(title)
    token_set = _token_set(title)
    semantic_counts = _token_counts(f"{title}\n{body[:1000]}")
    recent = _fetch_recent_candidates(con, symbol, ts_ms)
    best_sim = 0.0
    best_match: Optional[Dict[str, Any]] = None
    duplicate_count = 0
    source_set: Set[str] = set()
    for item in recent:
        title_sim = 1.0 if item["headline_key"] == headline_key else _jaccard(token_set, item["token_set"])
        semantic_sim = _weighted_cosine(semantic_counts, item["semantic_counts"])
        sim = max(float(title_sim), float((0.55 * title_sim) + (0.45 * semantic_sim)))
        if sim > best_sim:
            best_sim = float(sim)
            best_match = item
        if sim >= 0.65:
            duplicate_count += 1
            if item.get("source"):
                source_set.add(str(item["source"]))

    is_duplicate = bool(best_match and (best_sim >= 0.86 or best_match["headline_key"] == headline_key))
    if best_match and best_sim >= 0.65:
        cluster_key = _clean_text(best_match.get("cluster_key")) or _clean_text(best_match.get("headline_key")) or headline_key
    else:
        cluster_key = _stable_hash(f"{symbol or 'market'}|{headline_key}")
    novelty = float(max(0.0, min(1.0, 1.0 - best_sim)))
    return {
        "cluster_key": cluster_key,
        "headline_key": headline_key,
        "similarity": float(best_sim),
        "novelty": novelty,
        "is_duplicate": is_duplicate,
        "duplicate_count": int(duplicate_count),
        "source_count": int(len(source_set)),
    }


def build_enriched_news_records(
    con,
    raw_item: Dict[str, Any],
    allowed_symbols: Optional[Sequence[str]] = None,
) -> List[Dict[str, Any]]:
    payload = dict(raw_item or {})
    title = _clean_text(payload.get("title"))
    body = _clean_text(payload.get("body") or payload.get("summary"))
    if not title and not body:
        return []
    ts_ms = int(payload.get("ts_ms") or _now_ms())
    symbol_info = infer_symbols(payload, allowed_symbols)
    matched_symbols = list(symbol_info["symbols"] or [])
    sentiment = score_sentiment(title, body)
    symbols: List[Optional[str]] = matched_symbols or [None]
    meta = _safe_json_loads(payload.get("meta_json"))
    artifact_meta = _store_text_body_artifact(payload, title=title, body=body, meta=meta)
    if artifact_meta:
        meta.update(artifact_meta)
    transcript_meta = parse_transcript_metadata(body) if bool(meta.get("transcript") or payload.get("source") == "fmp_transcript") else {}
    text_blob = f"{title}\n{body}"
    output: List[Dict[str, Any]] = []

    for symbol in symbols:
        cluster = _cluster_event(con, symbol=symbol, title=title, body=body, ts_ms=ts_ms)
        base_event_key = _clean_text(payload.get("event_key")) or _headline_key(title)
        event_key = (
            f"news:{symbol}:{cluster['cluster_key']}"
            if cluster["is_duplicate"] and symbol
            else f"{base_event_key}:{symbol or 'market'}"
        )
        method = symbol_info["match_method"].get(symbol or "", "none") if symbol else "none"
        conf = float(symbol_info["match_confidence"].get(symbol or "", 0.0)) if symbol else 0.0
        derived = dict(_safe_json_loads(payload.get("derived_features")))
        derived.update(
            {
                "headline_key": cluster["headline_key"],
                "cluster_key": cluster["cluster_key"],
                "novelty": float(cluster["novelty"]),
                "sentiment_score": float(sentiment),
                "headline_similarity": float(cluster["similarity"]),
                "duplicate_count": int(cluster["duplicate_count"]),
                "is_duplicate": bool(cluster["is_duplicate"]),
                "matched_symbols": matched_symbols,
                "symbol_match_method": method,
                "symbol_match_confidence": float(conf),
                "transcript": bool(transcript_meta),
                "transcript_speaker_count": int(transcript_meta.get("speaker_count") or 0),
                "transcript_has_qa": bool(transcript_meta.get("has_qa")),
            }
        )
        event_meta = dict(meta)
        event_meta.update(
            {
                "taxonomy": meta.get("taxonomy") or [],
                "entities": meta.get("entities") or [],
                "matched_symbols": matched_symbols,
                "matched_symbol": symbol,
                "novelty": float(cluster["novelty"]),
                "sentiment_score": float(sentiment),
                "cluster_key": cluster["cluster_key"],
                "headline_key": cluster["headline_key"],
                "is_duplicate": bool(cluster["is_duplicate"]),
                "symbol_match_method": method,
                "symbol_match_confidence": float(conf),
                "body_len": len(text_blob),
                "transcript_meta": transcript_meta,
            }
        )
        event = dict(payload)
        event.update(
            {
                "event_type": "news",
                "symbol": symbol,
                "body": body[:8000],
                "ts_ms": ts_ms,
                "event_key": event_key,
                "dedupe_hash": f"{symbol or 'market'}:{cluster['cluster_key']}",
                "derived_features": derived,
                "meta_json": json.dumps(event_meta, separators=(",", ":"), sort_keys=True),
            }
        )
        output.append(
            {
                "event": event,
                "feature": {
                    "ts_ms": ts_ms,
                    "symbol": symbol,
                    "cluster_key": cluster["cluster_key"],
                    "headline_key": cluster["headline_key"],
                    "sentiment_score": float(sentiment),
                    "novelty_score": float(cluster["novelty"]),
                    "is_duplicate": bool(cluster["is_duplicate"]),
                    "duplicate_count": int(cluster["duplicate_count"]),
                    "company_match_method": method,
                    "company_match_conf": float(conf),
                    "source_count": int(cluster["source_count"]),
                    "meta_json": {
                        "matched_symbols": matched_symbols,
                        "title": title,
                        "source": event.get("source"),
                    },
                },
            }
        )
    return output


def refresh_news_symbol_features(con, symbol: str, bucket_sec: int = 3600) -> Optional[Dict[str, Any]]:
    sym = str(symbol or "").upper().strip()
    if not sym:
        return None
    now_ms = _now_ms()
    bucket_ms = max(60, int(bucket_sec)) * 1000
    bucket_ts_ms = int(now_ms // bucket_ms) * bucket_ms
    lookback_6h = now_ms - 6 * 3600_000
    rows = con.execute(
        """
        SELECT ts_ms, sentiment_score, novelty_score, is_duplicate, cluster_key
        FROM news_event_features
        WHERE symbol = ?
          AND ts_ms >= ?
        ORDER BY ts_ms DESC
        """,
        (sym, int(lookback_6h)),
    ).fetchall()
    if not rows:
        row = {
            "symbol": sym,
            "bucket_ts_ms": bucket_ts_ms,
            "bucket_sec": int(bucket_sec),
            "news_velocity": 0.0,
            "sentiment_trend": 0.0,
            "event_density": 0.0,
            "event_count": 0,
            "distinct_cluster_count": 0,
            "avg_sentiment": 0.0,
            "avg_novelty": 0.0,
            "duplicate_share": 0.0,
        }
        con.execute(
            """
            INSERT OR REPLACE INTO news_symbol_features(
              symbol, bucket_ts_ms, bucket_sec, news_velocity, sentiment_trend,
              event_density, event_count, distinct_cluster_count, avg_sentiment,
              avg_novelty, duplicate_share
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(row.get("symbol") or "").upper().strip(),
                int(row.get("bucket_ts_ms") or int(time.time() * 1000)),
                int(row.get("bucket_sec") or 3600),
                float(row.get("news_velocity") or 0.0),
                float(row.get("sentiment_trend") or 0.0),
                float(row.get("event_density") or 0.0),
                int(row.get("event_count") or 0),
                int(row.get("distinct_cluster_count") or 0),
                float(row.get("avg_sentiment") or 0.0),
                float(row.get("avg_novelty") or 0.0),
                float(row.get("duplicate_share") or 0.0),
            ),
        )
        return row

    sentiments_now: List[float] = []
    sentiments_prev: List[float] = []
    novelty_values: List[float] = []
    dupes = 0
    clusters_6h: Set[str] = set()
    velocity = 0.0
    for ts_ms, sentiment, novelty, is_duplicate, cluster_key in rows:
        age_h = max(0.0, (now_ms - int(ts_ms)) / 3600000.0)
        velocity += math.exp(-age_h)
        if age_h <= 3.0:
            sentiments_now.append(float(sentiment or 0.0))
        else:
            sentiments_prev.append(float(sentiment or 0.0))
        novelty_values.append(float(novelty or 0.0))
        dupes += 1 if int(is_duplicate or 0) else 0
        if cluster_key:
            clusters_6h.add(str(cluster_key))

    rows_24h = con.execute(
        """
        SELECT DISTINCT cluster_key
        FROM news_event_features
        WHERE symbol = ?
          AND ts_ms >= ?
          AND cluster_key IS NOT NULL
          AND cluster_key != ''
        """,
        (sym, int(now_ms - 24 * 3600_000)),
    ).fetchall()
    clusters_24h = {str(r[0]) for r in rows_24h or [] if r and r[0]}
    avg_sentiment = float(sum(float(v) for v in sentiments_now) / len(sentiments_now)) if sentiments_now else 0.0
    avg_novelty = float(sum(float(v) for v in novelty_values) / len(novelty_values)) if novelty_values else 0.0
    prev_sentiment = float(sum(float(v) for v in sentiments_prev) / len(sentiments_prev)) if sentiments_prev else 0.0
    row = {
        "symbol": sym,
        "bucket_ts_ms": bucket_ts_ms,
        "bucket_sec": int(bucket_sec),
        "news_velocity": float(velocity),
        "sentiment_trend": float(avg_sentiment - prev_sentiment),
        "event_density": float(len(clusters_24h) / 24.0),
        "event_count": int(len(rows)),
        "distinct_cluster_count": int(len(clusters_24h)),
        "avg_sentiment": avg_sentiment,
        "avg_novelty": avg_novelty,
        "duplicate_share": float(dupes / max(1, len(rows))),
    }
    con.execute(
        """
        INSERT OR REPLACE INTO news_symbol_features(
          symbol, bucket_ts_ms, bucket_sec, news_velocity, sentiment_trend,
          event_density, event_count, distinct_cluster_count, avg_sentiment,
          avg_novelty, duplicate_share
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(row.get("symbol") or "").upper().strip(),
            int(row.get("bucket_ts_ms") or int(time.time() * 1000)),
            int(row.get("bucket_sec") or 3600),
            float(row.get("news_velocity") or 0.0),
            float(row.get("sentiment_trend") or 0.0),
            float(row.get("event_density") or 0.0),
            int(row.get("event_count") or 0),
            int(row.get("distinct_cluster_count") or 0),
            float(row.get("avg_sentiment") or 0.0),
            float(row.get("avg_novelty") or 0.0),
            float(row.get("duplicate_share") or 0.0),
        ),
    )
    return row
