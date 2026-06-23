"""
FILE: ingest_now.py

Job entrypoint or scheduled task for `ingest_now`.
"""

# ingest_now.py
import os
import json
import time
import logging
from pathlib import Path
from typing import Any

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.ingestion_status import record_pipeline_status
from engine.runtime.logging import get_logger
from engine.runtime.storage import (
    connect,
    init_db,
    acquire_job_lock,
    release_job_lock,
    touch_job_lock,
    put_job_heartbeat,
    put_finbert_sentiment_enrichment,
    put_normalized_event,
    put_news_event_feature,
    run_write_txn,
)
from engine.data.event_normalization import normalize_news_event
from engine.data.finbert_sentiment import USE_FINBERT_SENTIMENT, score_event_rows

from engine.data.default_symbols import parse_symbol_limit
from engine.data.ingest.rss_ingest import ingest_rss_sources
from engine.data.ingest.company_news_ingest import ingest_company_news
from engine.data.ingest.gdelt_ingest import ingest_gdelt_doc
from engine.data.ingest.news_enrichment import build_enriched_news_records, refresh_news_symbol_features
from engine.data.ingest.transcripts_ingest import ingest_transcripts
from services.data_source_manager import get_manager

JOB_NAME = "ingest_now"
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()

MAX_ITEMS_PER_SOURCE = int(os.environ.get("RSS_MAX_ITEMS_PER_SOURCE", "15"))
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))
HEARTBEAT_EVERY_S = float(os.environ.get("HEARTBEAT_EVERY_S", "10.0"))
POLL_SECONDS = float(os.environ.get("NEWS_POLL_SECONDS", "120.0"))
ENABLE_GDELT = str(os.environ.get("INGEST_NOW_ENABLE_GDELT", "0")).strip().lower() in ("1", "true", "yes", "on")
ENABLE_COMPANY_NEWS = str(os.environ.get("INGEST_NOW_ENABLE_COMPANY_NEWS", "1")).strip().lower() in ("1", "true", "yes", "on")
ENABLE_TRANSCRIPTS = str(os.environ.get("INGEST_NOW_ENABLE_TRANSCRIPTS", "1")).strip().lower() in ("1", "true", "yes", "on")
COMPANY_NEWS_SYMBOL_LIMIT = parse_symbol_limit(os.environ.get("COMPANY_NEWS_SYMBOL_LIMIT"), 600)
COMPANY_NEWS_LOOKBACK_DAYS = int(os.environ.get("COMPANY_NEWS_LOOKBACK_DAYS", "3"))
COMPANY_NEWS_MAX_ITEMS = int(os.environ.get("COMPANY_NEWS_MAX_ITEMS_PER_SYMBOL", "8"))
TRANSCRIPTS_MAX_ITEMS = int(os.environ.get("TRANSCRIPTS_MAX_ITEMS_PER_SYMBOL", "2"))
GDELT_SYMBOL_LIMIT = parse_symbol_limit(os.environ.get("GDELT_SYMBOL_LIMIT"), 600)
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [ingest_now] %(message)s",
)
LOG = get_logger("engine.data.jobs.ingest_now")


def _warn_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    log_failure(
        LOG,
        event="ingest_now_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.data.jobs.ingest_now",
        extra=extra or None,
        persist=False,
    )


def _run_once() -> None:
    started_ms = int(time.time() * 1000)
    manager = get_manager()
    cfg_path = Path(os.environ.get("RSS_SOURCES_FILE", "sources_rss.json"))
    rss_sources = manager.load_rss_sources()
    if rss_sources:
        cfg = {"sources": [{"name": row.get("name"), "url": row.get("url")} for row in rss_sources]}
    else:
        if not cfg_path.exists():
            raise RuntimeError(f"rss_sources_missing:{cfg_path}")
        cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    rss_result = ingest_rss_sources(
        cfg.get("sources", []),
        max_items_per_source=MAX_ITEMS_PER_SOURCE,
        include_status=True,
    )
    if isinstance(rss_result, tuple) and len(rss_result) == 3:
        items, raw_errors, rss_feed_statuses = rss_result
    else:
        items, raw_errors = rss_result
        rss_feed_statuses = []
    errors: list[dict[str, Any] | str] = [
        dict(item) if isinstance(item, dict) else str(item)
        for item in (raw_errors or [])
    ]
    conu = connect()
    try:
        from engine.data.universe import get_active_symbols
        tracked_symbols = get_active_symbols(conu, limit=COMPANY_NEWS_SYMBOL_LIMIT)
    finally:
        conu.close()

    if ENABLE_COMPANY_NEWS and tracked_symbols:
        company_items, company_errors = ingest_company_news(
            tracked_symbols,
            lookback_days=int(COMPANY_NEWS_LOOKBACK_DAYS),
            max_items_per_symbol=int(COMPANY_NEWS_MAX_ITEMS),
        )
        items.extend(company_items or [])
        errors.extend(company_errors or [])

    if ENABLE_TRANSCRIPTS and tracked_symbols:
        transcript_items, transcript_errors = ingest_transcripts(
            tracked_symbols,
            max_items_per_symbol=int(TRANSCRIPTS_MAX_ITEMS),
        )
        items.extend(transcript_items or [])
        errors.extend(transcript_errors or [])

    if ENABLE_GDELT:
        try:
            conu = connect()
            try:
                syms = get_active_symbols(conu, limit=GDELT_SYMBOL_LIMIT)
            finally:
                conu.close()

            gd_items, gd_errors = ingest_gdelt_doc(
                symbols=syms,
                lookback_minutes=int(os.environ.get("GDELT_LOOKBACK_MINUTES", "45")),
                maxrecords=int(os.environ.get("GDELT_MAXRECORDS", "250")),
            )
            items.extend(gd_items or [])
            errors.extend(gd_errors or [])
        except Exception as e:
            errors.append({"source_name": "gdelt", "error": repr(e)})

    event_rows = 0
    last_ingested_ts_ms = 0
    pending_finbert_inputs: list[dict[str, Any]] = []
    dropped_enrichment = 0
    if items:
        def _write(con):
            local_rows = 0
            local_last_ts_ms = 0
            local_finbert_inputs: list[dict[str, Any]] = []
            local_dropped_enrichment = 0
            for it in items:
                try:
                    enriched = build_enriched_news_records(con, it, allowed_symbols=tracked_symbols)
                    if not enriched:
                        local_dropped_enrichment += 1
                        continue
                    for row in enriched:
                        event_id = put_normalized_event(normalize_news_event(row["event"]), con=con)
                        if event_id:
                            feature_row = dict(row.get("feature") or {})
                            feature_row["event_id"] = int(event_id)
                            put_news_event_feature(feature_row, con=con)
                            if feature_row.get("symbol"):
                                refresh_news_symbol_features(con, str(feature_row["symbol"]))
                            local_rows += 1
                            local_last_ts_ms = max(local_last_ts_ms, int(row["event"].get("ts_ms") or 0))
                            if USE_FINBERT_SENTIMENT:
                                local_finbert_inputs.append(
                                    {
                                        "body": row["event"].get("body"),
                                        "event_id": int(event_id),
                                        "event_key": row["event"].get("event_key"),
                                        "event_type": row["event"].get("event_type"),
                                        "source": row["event"].get("source"),
                                        "source_id": row["event"].get("source_id"),
                                        "symbol": row["event"].get("symbol"),
                                        "title": row["event"].get("title"),
                                        "ts_ms": int(row["event"].get("ts_ms") or 0),
                                    }
                                )
                except Exception as e:
                    _warn_nonfatal("INGEST_NOW_PUT_EVENT_FAILED", e, item=repr(it)[:240])
            return local_rows, local_last_ts_ms, local_finbert_inputs, local_dropped_enrichment

        event_rows, last_ingested_ts_ms, pending_finbert_inputs, dropped_enrichment = run_write_txn(
            _write,
            table="events",
            operation="ingest_news_batch",
            context={"job": JOB_NAME, "items": int(len(items))},
        )

    if USE_FINBERT_SENTIMENT and pending_finbert_inputs:
        try:
            finbert_rows = score_event_rows(pending_finbert_inputs)
        except Exception as e:
            _warn_nonfatal(
                "INGEST_NOW_FINBERT_SCORE_FAILED",
                e,
                batch=int(len(pending_finbert_inputs)),
            )
            finbert_rows = []
        if finbert_rows:
            try:
                run_write_txn(
                    lambda con: [put_finbert_sentiment_enrichment(row, con=con) for row in finbert_rows],
                    table="news_event_features",
                    operation="ingest_finbert_sentiment_batch",
                    context={"job": JOB_NAME, "rows": int(len(finbert_rows))},
                )
            except Exception as e:
                _warn_nonfatal(
                    "INGEST_NOW_FINBERT_PERSIST_FAILED",
                    e,
                    batch=int(len(finbert_rows)),
                )

    dur_ms = int(time.time() * 1000) - started_ms
    raw_rows = len(items)
    ok = len(errors) == 0
    status = record_pipeline_status(
        JOB_NAME,
        ok=ok,
        raw_rows=raw_rows,
        event_rows=event_rows,
        last_ingested_ts_ms=(last_ingested_ts_ms or started_ms),
        error=("; ".join(str(e) for e in errors[:3])) if errors else None,
        latency_ms=dur_ms,
        meta={
            "sources_file": str(cfg_path),
            "max_items_per_source": int(MAX_ITEMS_PER_SOURCE),
            "source_errors": len(errors),
            "rss_feed_statuses": rss_feed_statuses[:50],
            "gdelt_enabled": bool(ENABLE_GDELT),
            "company_news_enabled": bool(ENABLE_COMPANY_NEWS),
            "transcripts_enabled": bool(ENABLE_TRANSCRIPTS),
            "dropped_enrichment": int(dropped_enrichment),
        },
    )
    logging.info(
        "news_cycle ok=%s raw_rows=%s event_rows=%s dropped_enrichment=%s source_errors=%s dur_ms=%s",
        ok,
        raw_rows,
        event_rows,
        int(dropped_enrichment),
        len(errors),
        dur_ms,
    )
    if errors:
        for error_row in errors[:10]:
            _warn_nonfatal(
                "INGEST_NOW_RSS_ERROR",
                RuntimeError(str(error_row)),
                source_error=repr(error_row)[:240],
            )
    manager.record_job_status(
        JOB_NAME,
        ok=bool(ok),
        message="ingest_now cycle complete",
        error=("; ".join(str(e) for e in errors[:3])) if errors else "",
        meta={
            "raw_rows": int(raw_rows),
            "event_rows": int(event_rows),
            "source_errors": len(errors),
            "rss_feed_statuses": rss_feed_statuses[:50],
            "dropped_enrichment": int(dropped_enrichment),
        },
    )
    put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))


def main() -> None:
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("ingest_now must be launched by supervisor")
        raise SystemExit(1)

    manager = get_manager()
    if not manager.is_job_enabled(JOB_NAME, default=True):
        manager.record_job_status(JOB_NAME, ok=True, message="ingest_now disabled by data source control plane")
        return

    init_db()

    if not acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
        logging.error("another instance is holding the job lock; exiting")
        raise SystemExit(2)

    last_hb_s = 0.0
    try:
        while True:
            if not manager.is_job_enabled(JOB_NAME, default=True):
                manager.record_job_status(JOB_NAME, ok=True, message="ingest_now disabled by data source control plane")
                break
            now_s = time.time()
            if (now_s - last_hb_s) >= HEARTBEAT_EVERY_S:
                touch_job_lock(JOB_NAME, OWNER, PID)
                put_job_heartbeat(
                    JOB_NAME,
                    OWNER,
                    PID,
                    extra_json=json.dumps(
                        {
                            "poll_seconds": float(POLL_SECONDS),
                            "max_items_per_source": int(MAX_ITEMS_PER_SOURCE),
                            "gdelt_enabled": bool(ENABLE_GDELT),
                        },
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                )
                last_hb_s = now_s

            try:
                _run_once()
            except Exception as e:
                logging.exception("news_cycle_failed")
                status = record_pipeline_status(
                    JOB_NAME,
                    ok=False,
                    raw_rows=0,
                    event_rows=0,
                    last_ingested_ts_ms=int(time.time() * 1000),
                    error=str(e),
                    meta={"poll_seconds": float(POLL_SECONDS)},
                )
                manager.record_job_status(
                    JOB_NAME,
                    ok=False,
                    message="ingest_now cycle failed",
                    error=str(e),
                    meta={"poll_seconds": float(POLL_SECONDS)},
                )
                put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))

            time.sleep(max(1.0, float(POLL_SECONDS)))
    finally:
        try:
            release_job_lock(JOB_NAME, OWNER, PID)
        except Exception as e:
            _warn_nonfatal("INGEST_NOW_LOCK_RELEASE_FAILED", e, job=JOB_NAME)


if __name__ == "__main__":
    main()
