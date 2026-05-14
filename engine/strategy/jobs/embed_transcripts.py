"""Backfill transcript embeddings and Q&A FinBERT sentiment cache."""

from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import Any

from engine.nlp.cache import NlpCache
from engine.nlp.encoder import FinBertSentimentEncoder, SentenceTransformerEncoder
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

JOB_NAME = "embed_transcripts"
OWNER = os.environ.get("JOB_OWNER", os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")))
PID = os.getpid()

BATCH_SIZE = max(1, int(os.environ.get("NLP_TRANSCRIPTS_BATCH_SIZE", "64")))
MAX_SECTIONS_PER_TRANSCRIPT = max(1, int(os.environ.get("NLP_TRANSCRIPTS_MAX_SECTIONS", "96")))
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))
SENTENCE_MODEL_NAME = str(os.environ.get("NLP_SENTENCE_MODEL_NAME", "all-MiniLM-L6-v2") or "all-MiniLM-L6-v2")
FINBERT_MODEL_NAME = str(os.environ.get("NLP_FINBERT_MODEL_NAME", "ProsusAI/finbert") or "ProsusAI/finbert")
LOG = get_logger("engine.strategy.jobs.embed_transcripts")
logging.basicConfig(level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO))


def _warn_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.jobs.embed_transcripts",
        extra=extra or None,
        persist=False,
    )


def split_transcript_sections(text: str, *, limit: int = MAX_SECTIONS_PER_TRANSCRIPT) -> list[str]:
    raw = re.split(r"\n\s*\n|(?m)^(?:Operator|Analyst|Unknown Speaker|[A-Z][A-Za-z .'-]{1,60}):", str(text or ""))
    out: list[str] = []
    for part in raw:
        cleaned = " ".join(str(part or "").split()).strip()
        if len(cleaned) < 40:
            continue
        out.append(cleaned[:4000])
        if len(out) >= int(limit):
            break
    return out


def extract_qa_sections(text: str) -> list[str]:
    body = str(text or "")
    match = re.search(r"(question(?:-and-| and )answer|q\s*&\s*a)", body, flags=re.IGNORECASE)
    if match:
        body = body[match.start() :]
    sections = split_transcript_sections(body, limit=max(8, MAX_SECTIONS_PER_TRANSCRIPT // 2))
    return sections[: max(1, min(32, len(sections)))]


def _fetch_candidates(limit: int) -> list[dict[str, Any]]:
    con = connect(readonly=True)
    try:
        rows = con.execute(
            """
            SELECT id, ts_ms, source, title, body, symbol, source_id, event_key
            FROM events
            WHERE source = 'fmp_transcript'
              AND LENGTH(COALESCE(body, '')) > 0
            ORDER BY ts_ms ASC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows or []:
            out.append(
                {
                    "event_id": int(row[0]),
                    "ts_ms": int(row[1] or 0),
                    "source": str(row[2] or "fmp_transcript"),
                    "title": str(row[3] or ""),
                    "body": str(row[4] or ""),
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
            _warn_nonfatal("EMBED_TRANSCRIPTS_FETCH_CLOSE_FAILED", exc)


def run(limit: int | None = None) -> dict[str, Any]:
    init_db()
    transcripts = _fetch_candidates(int(limit or BATCH_SIZE))
    started_ms = int(time.time() * 1000)
    if not transcripts:
        return {"job": JOB_NAME, "transcripts_seen": 0, "encoded": 0, "qa_encoded": 0, "ts_ms": started_ms}
    cache = NlpCache()
    section_rows: list[dict[str, Any]] = []
    qa_rows: list[dict[str, Any]] = []
    for transcript in transcripts:
        sections = split_transcript_sections(str(transcript.get("body") or ""))
        for idx, section in enumerate(sections):
            section_rows.append(
                {
                    "symbol": transcript.get("symbol"),
                    "ts_ms": int(transcript.get("ts_ms") or 0),
                    "source": f"transcript:{transcript.get('event_id')}:{idx}",
                    "text": section,
                }
            )
        for idx, section in enumerate(extract_qa_sections(str(transcript.get("body") or ""))):
            qa_rows.append(
                {
                    "symbol": transcript.get("symbol"),
                    "ts_ms": int(transcript.get("ts_ms") or 0),
                    "source": f"transcript_qa:{transcript.get('event_id')}:{idx}",
                    "text": section,
                }
            )

    with (
        SentenceTransformerEncoder(model_name=SENTENCE_MODEL_NAME, batch_size=64) as sentence_encoder,
        FinBertSentimentEncoder(model_name=FINBERT_MODEL_NAME, batch_size=32) as finbert_encoder,
    ):
        sentence_result = cache.get_or_encode_embeddings(
            [str(row["text"]) for row in section_rows],
            sentence_encoder,
            source="transcript",
            ts=[int(row["ts_ms"] or 0) for row in section_rows],
            symbol=[row.get("symbol") for row in section_rows],
        )
        qa_result = cache.get_or_encode_sentiments(
            [str(row["text"]) for row in qa_rows],
            finbert_encoder,
            source="transcript_qa",
            ts=[int(row["ts_ms"] or 0) for row in qa_rows],
            symbol=[row.get("symbol") for row in qa_rows],
        )
    return {
        "job": JOB_NAME,
        "sentence_model_name": SENTENCE_MODEL_NAME,
        "finbert_model_name": FINBERT_MODEL_NAME,
        "transcripts_seen": int(len(transcripts)),
        "sections_seen": int(len(section_rows)),
        "encoded": int(sentence_result.encoded),
        "qa_sections_seen": int(len(qa_rows)),
        "qa_encoded": int(qa_result.encoded),
        "cache_hits": int(sentence_result.hits + qa_result.hits),
        "cache_misses": int(sentence_result.misses + qa_result.misses),
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
            _warn_nonfatal("EMBED_TRANSCRIPTS_FAILED_OPEN", exc, batch_size=int(BATCH_SIZE))
            stats = {"job": JOB_NAME, "transcripts_seen": 0, "encoded": 0, "failed_open": True, "error": str(exc)}
        print(json.dumps(stats, separators=(",", ":"), sort_keys=True))
    finally:
        try:
            release_job_lock(JOB_NAME, OWNER, PID)
        except Exception as exc:
            _warn_nonfatal("EMBED_TRANSCRIPTS_RELEASE_LOCK_FAILED", exc)


if __name__ == "__main__":
    main()
