"""Backfill FinBERT sentiment cache for historical news text."""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

from engine.nlp.cache import NlpCache
from engine.nlp.encoder import FinBertSentimentEncoder
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import (
    acquire_job_lock,
    connect,
    init_db,
    put_job_heartbeat,
    release_job_lock,
    touch_job_lock,
)

JOB_NAME = "embed_news"
OWNER = os.environ.get("JOB_OWNER", os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")))
PID = os.getpid()

BATCH_SIZE = max(1, int(os.environ.get("NLP_NEWS_BATCH_SIZE", "256")))
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))
FINBERT_MODEL_NAME = str(os.environ.get("NLP_FINBERT_MODEL_NAME", "ProsusAI/finbert") or "ProsusAI/finbert")
LOG = get_logger("engine.strategy.jobs.embed_news")
logging.basicConfig(level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO))


def _warn_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.jobs.embed_news",
        extra=extra or None,
        persist=False,
    )


def _fetch_candidates(limit: int) -> list[dict[str, Any]]:
    con = connect(readonly=True)
    try:
        rows = con.execute(
            """
            SELECT id, ts_ms, source, title, body, symbol, source_id, event_key
            FROM events
            WHERE COALESCE(source, '') != 'fmp_transcript'
              AND (
                event_type = 'news'
                OR source LIKE 'rss:%'
                OR source IN ('gdelt', 'finnhub_company_news')
              )
              AND LENGTH(COALESCE(title, '') || COALESCE(body, '')) > 0
            ORDER BY ts_ms ASC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows or []:
            title = str(row[3] or "").strip()
            body = str(row[4] or "").strip()
            text = "\n".join(part for part in (title, body) if part).strip()
            if not text:
                continue
            out.append(
                {
                    "event_id": int(row[0]),
                    "ts_ms": int(row[1] or 0),
                    "source": str(row[2] or "news"),
                    "text": text,
                    "symbol": str(row[5] or "").upper().strip() or None,
                    "source_id": row[6],
                    "event_key": row[7],
                }
            )
        return out
    finally:
        try:
            con.close()
        except Exception as exc:
            _warn_nonfatal("EMBED_NEWS_FETCH_CLOSE_FAILED", exc)


def run(limit: int | None = None) -> dict[str, Any]:
    init_db()
    candidates = _fetch_candidates(int(limit or BATCH_SIZE))
    started_ms = int(time.time() * 1000)
    if not candidates:
        return {"job": JOB_NAME, "rows_seen": 0, "encoded": 0, "cache_hits": 0, "ts_ms": started_ms}
    cache = NlpCache()
    with FinBertSentimentEncoder(model_name=FINBERT_MODEL_NAME, batch_size=32) as encoder:
        result = cache.get_or_encode_sentiments(
            [str(row["text"]) for row in candidates],
            encoder,
            source="news",
            ts=[int(row["ts_ms"] or 0) for row in candidates],
            symbol=[row.get("symbol") for row in candidates],
        )
    stats = {
        "job": JOB_NAME,
        "model_name": FINBERT_MODEL_NAME,
        "rows_seen": int(len(candidates)),
        "encoded": int(result.encoded),
        "cache_hits": int(result.hits),
        "cache_misses": int(result.misses),
        "ts_ms": int(time.time() * 1000),
    }
    try:
        from engine.runtime.metrics import emit_counter, emit_gauge

        emit_counter("nlp_news_rows_seen", int(len(candidates)), component="engine.strategy.jobs.embed_news", job=JOB_NAME)
        emit_gauge("nlp_news_cache_hits", int(result.hits), component="engine.strategy.jobs.embed_news", job=JOB_NAME)
    except Exception:
        logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)
    return stats


def main() -> None:
    if not acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
        raise SystemExit(2)
    try:
        touch_job_lock(JOB_NAME, OWNER, PID)
        put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps({"batch_size": BATCH_SIZE}))
        try:
            stats = run(limit=BATCH_SIZE)
        except Exception as exc:
            _warn_nonfatal("EMBED_NEWS_FAILED_OPEN", exc, batch_size=int(BATCH_SIZE))
            stats = {"job": JOB_NAME, "rows_seen": 0, "encoded": 0, "failed_open": True, "error": str(exc)}
        print(json.dumps(stats, separators=(",", ":"), sort_keys=True))
    finally:
        try:
            release_job_lock(JOB_NAME, OWNER, PID)
        except Exception as exc:
            _warn_nonfatal("EMBED_NEWS_RELEASE_LOCK_FAILED", exc)


if __name__ == "__main__":
    main()
