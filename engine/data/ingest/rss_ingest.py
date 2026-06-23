"""
FILE: rss_ingest.py

Ingestion pipeline module for `rss_ingest`.
"""

# dev_core/ingest/rss_ingest.py
import time
import hashlib
import random
import logging
import calendar
import os
from typing import List, Dict, Any, Optional, Tuple
import json
import re
import requests
from engine.artifacts.store import LocalArtifactStore
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
try:
    import feedparser
    _FEEDPARSER_IMPORT_ERROR = None
except Exception as _feedparser_import_error:
    feedparser = None  # type: ignore
    _FEEDPARSER_IMPORT_ERROR = _feedparser_import_error
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

LOG = get_logger("data.ingest.rss_ingest")
_WARNED_NONFATAL_KEYS: set[str] = set()
RSS_HTTP_UA = os.environ.get("RSS_HTTP_UA", "trading-system/1.0 rss-feed")
RSS_HTTP_RETRY_TOTAL = max(0, int(os.environ.get("RSS_HTTP_RETRY_TOTAL", "1")))


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="data_rss_ingest_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.data.ingest.rss_ingest",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)

# ------            -- ------------------------------------------------------
# Event enrichment helpers (additive, non-breaking)
# ------            -- ------------------------------------------------------

_TAXONOMY_RULES = {
    "crypto": r"\b(bitcoin|btc|ethereum|eth|crypto|blockchain|defi|stablecoin)\b",
    "macro": r"\b(fed|fomc|rates?|inflation|cpi|pce|gdp|jobs?|payrolls?)\b",
    "energy": r"\b(oil|crude|wti|brent|opec|energy|gasoline)\b",
    "earnings": r"\b(earnings|guidance|revenue|profit|loss)\b",
    "geopolitics": r"\b(war|sanction|military|conflict|iran|russia|china|ukraine)\b",
}

_ENTITY_RX = re.compile(r"\b[A-Z]{2,6}\b")


def _extract_taxonomy(text: str):
    tags = []
    t = (text or "").lower()
    for name, pat in _TAXONOMY_RULES.items():
        try:
            if re.search(pat, t, flags=re.IGNORECASE):
                tags.append(name)
        except re.error as e:
            _warn_nonfatal(
                "RSS_INGEST_TAXONOMY_REGEX_FAILED",
                e,
                once_key=f"taxonomy_regex:{name}",
                rule_name=str(name),
                pattern=pat[:120],
            )
            continue
    return tags


def _extract_entities(text: str):
    if not text:
        return []
    return sorted(set(_ENTITY_RX.findall(text)))

def _hash(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8", errors="ignore")).hexdigest()[:24]


def _clean(s: Optional[str]) -> str:
    return (s or "").strip()


def _store_rss_body(*, source_name: str, event_key: str, title: str, body: str, url: str) -> Dict[str, Any]:
    if not body:
        return {}
    alias = f"news:rss:{_hash(event_key or url or title)}"
    try:
        ref = LocalArtifactStore().put(
            body.encode("utf-8"),
            content_type="text/plain; charset=utf-8",
            kind="news",
            alias=alias,
            metadata={
                "provider": "rss",
                "source_name": str(source_name),
                "event_key": str(event_key),
                "title": str(title),
                "url": str(url),
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
            "RSS_INGEST_ARTIFACT_STORE_FAILED",
            exc,
            once_key=f"rss_artifact_store:{alias}",
            source_name=source_name,
            url=url[:160],
        )
        return {}


def _sleep_jitter(s: float) -> None:
    if s <= 0:
        return
    j = s * 0.2
    time.sleep(max(0.05, s + random.uniform(-j, j)))


def _make_session() -> requests.Session:
    # RSS sources are noisy and intermittently down, so shared retry policy lives
    # here instead of being reimplemented by each caller/source definition.
    sess = requests.Session()
    retry = Retry(
        total=RSS_HTTP_RETRY_TOTAL,
        connect=RSS_HTTP_RETRY_TOTAL,
        read=RSS_HTTP_RETRY_TOTAL,
        status=RSS_HTTP_RETRY_TOTAL,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    return sess


_SESSION = _make_session()


def _classify_rss_exception(error: BaseException) -> Dict[str, Any]:
    response = getattr(error, "response", None)
    status_code = int(getattr(response, "status_code", 0) or 0)
    if status_code == 429:
        retry_after = 60.0
        try:
            retry_after = max(1.0, float((getattr(response, "headers", {}) or {}).get("Retry-After") or 60.0))
        except Exception:
            retry_after = 60.0
        return {"classification": "rate_limited", "status_code": status_code, "retry_after_s": retry_after}
    if status_code == 401:
        return {"classification": "wrong_credentials", "status_code": status_code}
    if status_code == 403:
        return {"classification": "entitlement_missing", "status_code": status_code}
    if status_code == 503:
        return {"classification": "provider_unreachable", "status_code": status_code, "retry_after_s": 300.0}
    if status_code >= 400:
        return {"classification": "provider_unreachable", "status_code": status_code}
    return {"classification": "provider_unreachable", "status_code": 0, "error_type": type(error).__name__}


def fetch_rss(url: str, timeout_s: int = 15, max_attempts: int = 2) -> Any:
    if feedparser is None:
        raise RuntimeError(f"feedparser_unavailable:{_FEEDPARSER_IMPORT_ERROR}")
    headers = {
        "User-Agent": RSS_HTTP_UA,
        "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml, */*",
    }

    last_err: Optional[Exception] = None
    for attempt in range(1, int(max_attempts) + 1):
        try:
            r = _SESSION.get(url, headers=headers, timeout=timeout_s)
            if r.status_code >= 400:
                raise requests.HTTPError(f"{r.status_code} {r.reason}", response=r)
            return feedparser.parse(r.content)
        except Exception as e:
            last_err = e
            base = min(20.0, 1.5 ** max(0, attempt - 1))
            _sleep_jitter(base)
    assert last_err is not None
    raise last_err


def ingest_rss_sources(
    sources: List[Dict[str, Any]],
    max_items_per_source: int = 25,
    *,
    include_status: bool = False,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]] | Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Returns (items, errors)

    items:
      { ts_ms, source, title, body, url, event_key }

    errors:
      { source_name, url, error }
    """
    out: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    statuses: List[Dict[str, Any]] = []
    now_ms = int(time.time() * 1000)

    for src in sources or []:
        name = _clean(src.get("name")) or "rss"
        url = _clean(src.get("url"))
        if not url:
            row = {"source_name": name, "url": url, "status": "fail", "classification": "missing_url", "items": 0, "error": "missing_url"}
            errors.append({"source_name": name, "url": url, "error": "missing_url"})
            statuses.append(row)
            continue

        try:
            feed = fetch_rss(url)
        except Exception as e:
            classified = _classify_rss_exception(e)
            _warn_nonfatal(
                "RSS_INGEST_FETCH_FAILED",
                e,
                once_key=f"fetch_failed:{name}:{url}",
                source_name=name,
                url=url,
            )
            errors.append({"source_name": name, "url": url, "error": str(e), **classified})
            statuses.append(
                {
                    "source_name": name,
                    "url": url,
                    "status": "degraded" if classified.get("classification") in {"rate_limited", "provider_unreachable"} else "fail",
                    "classification": str(classified.get("classification") or "provider_unreachable"),
                    "items": 0,
                    "error": str(e),
                    **classified,
                }
            )
            continue

        entries = list(feed.entries or [])[:max_items_per_source]
        bozo = bool(getattr(feed, "bozo", False) or (isinstance(feed, dict) and feed.get("bozo")))
        if not entries:
            classification = "malformed_payload" if bozo else "empty_payload"
            row = {
                "source_name": name,
                "url": url,
                "status": "fail" if bozo else "degraded",
                "classification": classification,
                "items": 0,
                "error": str(getattr(feed, "bozo_exception", "") or classification)[:240] if bozo else "",
            }
            statuses.append(row)
            if bozo:
                errors.append({"source_name": name, "url": url, "error": row["error"], "classification": classification})
            continue
        source_count_before = len(out)
        for ent in entries:
            title = _clean(getattr(ent, "title", "") or ent.get("title"))
            link = _clean(getattr(ent, "link", "") or ent.get("link"))
            summary = _clean(getattr(ent, "summary", "") or ent.get("summary"))

            # Some feeds store the full text here
            try:
                if not summary:
                    content = getattr(ent, "content", None) or ent.get("content")
                    if content and isinstance(content, list) and content:
                        summary = _clean(getattr(content[0], "value", "") or content[0].get("value"))
            except Exception as e:
                _warn_nonfatal(
                    "RSS_INGEST_CONTENT_EXTRACT_FAILED",
                    e,
                    once_key=f"content_extract:{_hash(title[:120] or link[:120] or name)}",
                    source_name=name,
                    title=title[:80],
                    url=link[:160],
                )

            if not title and not summary and not link:
                continue

            ts_ms = now_ms
            try:
                pp = getattr(ent, "published_parsed", None) or ent.get("published_parsed")
                up = getattr(ent, "updated_parsed", None) or ent.get("updated_parsed")

                # feedparser returns time.struct_time; treat it as UTC
                if pp:
                    ts_ms = int(calendar.timegm(pp) * 1000)
                elif up:
                    ts_ms = int(calendar.timegm(up) * 1000)
            except Exception:
                ts_ms = now_ms

            base = link or (title + "|" + summary)
            event_key = f"rss:{name}:{_hash(base)}"

            text_blob = f"{title}\n{summary}"

            # Taxonomy/entities are lightweight first-pass hints. More expensive
            # relevance or embedding work should happen later in the event pipeline.
            meta = {
                "taxonomy": _extract_taxonomy(text_blob),
                "entities": _extract_entities(text_blob),
                "novelty": None,  # computed later once embeddings exist
                "ingest_source": "rss",
                "source_name": name,
            }
            meta.update(_store_rss_body(source_name=name, event_key=event_key, title=title, body=summary, url=link))

            out.append(
                {
                    "ts_ms": int(ts_ms),
                    "source": f"rss:{name}",
                    "title": title,
                    "body": summary[:8000],
                    "url": link,
                    "event_key": event_key,
                    "meta_json": json.dumps(meta, separators=(",", ":"), sort_keys=True),
                }
            )
        source_items = int(len(out) - source_count_before)
        statuses.append(
            {
                "source_name": name,
                "url": url,
                "status": "degraded" if bozo else "pass",
                "classification": "malformed_payload" if bozo else "success",
                "items": source_items,
                "error": str(getattr(feed, "bozo_exception", "") or "")[:240] if bozo else "",
            }
        )

    out.sort(key=lambda x: x["ts_ms"], reverse=True)
    if include_status:
        return out, errors, statuses
    return out, errors
