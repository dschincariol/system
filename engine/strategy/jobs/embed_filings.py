"""Backfill sentence-transformer embeddings for SEC filing paragraphs."""

from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import Any

from engine.nlp.cache import NlpCache
from engine.nlp.encoder import build_text_embedding_encoder, resolve_text_embedding_config
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

JOB_NAME = "embed_filings"
OWNER = os.environ.get("JOB_OWNER", os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")))
PID = os.getpid()

BATCH_SIZE = max(1, int(os.environ.get("NLP_FILINGS_BATCH_SIZE", "128")))
MAX_PARAGRAPHS_PER_FILING = max(1, int(os.environ.get("NLP_FILINGS_MAX_PARAGRAPHS", "64")))
FETCH_PRIMARY_DOC = str(os.environ.get("NLP_FILINGS_FETCH_PRIMARY_DOC", "0")).strip().lower() in {"1", "true", "yes", "on"}
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))
EMBED_CONFIG = resolve_text_embedding_config(kind="nlp")
MODEL_NAME = str(EMBED_CONFIG.model_name)
LOG = get_logger("engine.strategy.jobs.embed_filings")
logging.basicConfig(level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO))


def _warn_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.jobs.embed_filings",
        extra=extra or None,
        persist=False,
    )


def _json_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except Exception:
        return {}
    return dict(parsed) if isinstance(parsed, dict) else {}


def split_paragraphs(text: str, *, limit: int = MAX_PARAGRAPHS_PER_FILING) -> list[str]:
    raw = re.split(r"\n\s*\n|(?<=\.)\s{2,}", str(text or ""))
    out: list[str] = []
    for part in raw:
        cleaned = " ".join(str(part or "").split()).strip()
        if len(cleaned) < 40:
            continue
        out.append(cleaned[:4000])
        if len(out) >= int(limit):
            break
    return out


def _fetch_primary_doc(url: str) -> str:
    if not FETCH_PRIMARY_DOC or not url:
        return ""
    try:
        import requests

        response = requests.get(str(url), timeout=20, headers={"User-Agent": os.environ.get("SEC_USER_AGENT", "trading-system-nlp")})
        response.raise_for_status()
        return str(response.text or "")
    except Exception as exc:
        _warn_nonfatal("EMBED_FILINGS_PRIMARY_DOC_FETCH_FAILED", exc, url=str(url)[:180])
        return ""


def _filing_text(row: Any) -> str:
    items = _json_payload(row[8])
    candidates = [
        items.get("text"),
        items.get("body"),
        items.get("filing_text"),
        items.get("document_text"),
        _fetch_primary_doc(str(row[7] or "")),
        f"{row[0]} {row[2]} filing {row[6] or ''} {row[7] or ''}",
    ]
    return "\n\n".join(str(part or "").strip() for part in candidates if str(part or "").strip())


def _fetch_candidates(limit: int) -> list[dict[str, Any]]:
    con = connect(readonly=True)
    try:
        rows = con.execute(
            """
            SELECT symbol, accession, form, filed_date, report_date, cik, company_name,
                   primary_doc_url, items_json, source, ts_ms
            FROM sec_filings
            ORDER BY ts_ms ASC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        docs: list[dict[str, Any]] = []
        for row in rows or []:
            symbol = str(row[0] or "").upper().strip()
            ts_ms = int(row[10] or 0)
            paragraphs = split_paragraphs(_filing_text(row))
            for idx, paragraph in enumerate(paragraphs):
                docs.append(
                    {
                        "symbol": symbol,
                        "ts_ms": ts_ms,
                        "source": f"filing:{row[1]}:{idx}",
                        "text": paragraph,
                    }
                )
        return docs
    finally:
        try:
            con.close()
        except Exception as exc:
            _warn_nonfatal("EMBED_FILINGS_FETCH_CLOSE_FAILED", exc)


def run(limit: int | None = None) -> dict[str, Any]:
    init_db()
    docs = _fetch_candidates(int(limit or BATCH_SIZE))
    started_ms = int(time.time() * 1000)
    if not docs:
        return {"job": JOB_NAME, "paragraphs_seen": 0, "encoded": 0, "cache_hits": 0, "ts_ms": started_ms}
    cache = NlpCache()
    with build_text_embedding_encoder(EMBED_CONFIG) as encoder:
        result = cache.get_or_encode_embeddings(
            [str(row["text"]) for row in docs],
            encoder,
            source="filing",
            ts=[int(row["ts_ms"] or 0) for row in docs],
            symbol=[row.get("symbol") for row in docs],
        )
    return {
        "job": JOB_NAME,
        "model_name": MODEL_NAME,
        "model_namespace": EMBED_CONFIG.namespace,
        "embedding_backend": EMBED_CONFIG.backend,
        "model_metadata": EMBED_CONFIG.metadata,
        "paragraphs_seen": int(len(docs)),
        "encoded": int(result.encoded),
        "cache_hits": int(result.hits),
        "cache_misses": int(result.misses),
        "ts_ms": int(time.time() * 1000),
    }


def main() -> None:
    if not acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
        raise SystemExit(2)
    try:
        touch_job_lock(JOB_NAME, OWNER, PID)
        put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps({"batch_size": BATCH_SIZE}))
        try:
            stats = run(limit=BATCH_SIZE)
        except Exception as exc:
            _warn_nonfatal("EMBED_FILINGS_FAILED_OPEN", exc, batch_size=int(BATCH_SIZE))
            stats = {"job": JOB_NAME, "paragraphs_seen": 0, "encoded": 0, "failed_open": True, "error": str(exc)}
        print(json.dumps(stats, separators=(",", ":"), sort_keys=True))
    finally:
        try:
            release_job_lock(JOB_NAME, OWNER, PID)
        except Exception as exc:
            _warn_nonfatal("EMBED_FILINGS_RELEASE_LOCK_FAILED", exc)


if __name__ == "__main__":
    main()
