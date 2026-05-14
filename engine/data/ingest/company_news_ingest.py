import datetime as dt
import logging
from typing import Any, Dict, List, Sequence, Tuple

import requests

from engine.artifacts.store import LocalArtifactStore
from engine.data._credentials import get_data_credential
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger

FINNHUB_BASE = "https://finnhub.io/api/v1/company-news"
LOG = get_logger("engine.data.ingest.company_news_ingest")


def _finnhub_key() -> str:
    return get_data_credential("FINNHUB_API_KEY")


def _store_news_body(*, symbol: str, source_id: str, body: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
    if not body:
        return {}
    alias = f"news:finnhub:{symbol}:{source_id}"
    try:
        ref = LocalArtifactStore().put(
            body.encode("utf-8"),
            content_type="text/plain; charset=utf-8",
            kind="news",
            alias=alias,
            metadata={
                **dict(metadata or {}),
                "symbol": str(symbol),
                "source_id": str(source_id),
                "provider": "finnhub",
            },
        )
        return {
            "artifact_alias": alias,
            "artifact_sha256": ref.sha256,
            "artifact_size_bytes": int(ref.size),
            "content_type": ref.content_type,
        }
    except Exception as exc:
        log_failure(
            LOG,
            event="company_news_ingest_artifact_store_failed",
            code="COMPANY_NEWS_INGEST_ARTIFACT_STORE_FAILED",
            message="Company news artifact storage failed.",
            error=exc,
            level=logging.WARNING,
            component="engine.data.ingest.company_news_ingest",
            extra={"symbol": symbol, "source_id": source_id},
            persist=False,
        )
        return {}


def ingest_company_news(
    symbols: Sequence[str],
    *,
    lookback_days: int = 3,
    max_items_per_symbol: int = 10,
    timeout_s: int = 20,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    finnhub_key = _finnhub_key()
    if not finnhub_key:
        return [], []

    end_date = dt.date.today()
    start_date = end_date - dt.timedelta(days=max(1, int(lookback_days)))
    session = requests.Session()
    out: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    for raw_symbol in symbols or []:
        symbol = str(raw_symbol or "").strip().upper()
        if not symbol:
            continue
        try:
            response = session.get(
                FINNHUB_BASE,
                params={
                    "symbol": symbol,
                    "from": start_date.isoformat(),
                    "to": end_date.isoformat(),
                    "token": finnhub_key,
                },
                timeout=timeout_s,
            )
            response.raise_for_status()
            payload = response.json()
            items = payload if isinstance(payload, list) else []
        except Exception as exc:
            errors.append({"source_name": f"finnhub:{symbol}", "error": repr(exc)})
            log_failure(
                LOG,
                event="company_news_ingest_fetch_failed",
                code="COMPANY_NEWS_INGEST_FETCH_FAILED",
                message="Company news fetch failed.",
                error=exc,
                level=logging.WARNING,
                component="engine.data.ingest.company_news_ingest",
                extra={"symbol": symbol},
                persist=False,
            )
            continue

        for item in items[: max(1, int(max_items_per_symbol))]:
            try:
                ts_ms = int(float(item.get("datetime") or 0) * 1000)
            except Exception:
                ts_ms = 0
            headline = str(item.get("headline") or "").strip()
            summary = str(item.get("summary") or "").strip()
            url = str(item.get("url") or "").strip()
            source_id = str(item.get("id") or url or headline).strip()
            if not headline and not summary:
                continue
            artifact_meta = _store_news_body(
                symbol=symbol,
                source_id=source_id,
                body=summary,
                metadata={
                    "url": url,
                    "headline": headline,
                    "category": str(item.get("category") or "").strip(),
                },
            )
            out.append(
                {
                    "ts_ms": ts_ms,
                    "event_type": "news",
                    "symbol": symbol,
                    "source": "finnhub_company_news",
                    "source_id": source_id,
                    "title": headline or f"{symbol} company news",
                    "body": summary[:8000],
                    "url": url,
                    "event_key": f"finnhub_company_news:{symbol}:{source_id}",
                    "meta_json": {
                        "provider": "finnhub",
                        "related": str(item.get("related") or "").strip(),
                        "category": str(item.get("category") or "").strip(),
                        **artifact_meta,
                    },
                }
            )
    return out, errors
