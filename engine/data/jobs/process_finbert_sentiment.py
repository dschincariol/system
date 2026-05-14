"""
Backfill persisted FinBERT sentiment for normalized news events.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, List

from engine.data.finbert_sentiment import FINBERT_MODEL_NAME, USE_FINBERT_SENTIMENT, score_event_rows
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import (
    acquire_job_lock,
    connect,
    init_db,
    put_finbert_sentiment_enrichment,
    put_job_heartbeat,
    release_job_lock,
    run_write_txn,
    touch_job_lock,
)

JOB_NAME = "process_finbert_sentiment"
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()

BATCH_SIZE = max(1, int(os.environ.get("FINBERT_PROCESS_BATCH_SIZE", "100")))
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [process_finbert_sentiment] %(message)s",
)
LOG = get_logger("engine.data.jobs.process_finbert_sentiment")


def _warn_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    log_failure(
        LOG,
        event=str(code).lower(),
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.data.jobs.process_finbert_sentiment",
        extra=extra or None,
        persist=False,
    )


def _fetch_candidates(limit: int) -> List[Dict[str, Any]]:
    con = connect(readonly=True)
    try:
        rows = con.execute(
            """
            SELECT
              e.id,
              e.ts_ms,
              e.source,
              e.title,
              e.body,
              e.symbol,
              e.source_id,
              e.event_key
            FROM events e
            LEFT JOIN news_event_features nef
              ON nef.event_id = e.id
            WHERE e.event_type = 'news'
              AND COALESCE(nef.finbert_model_name, '') != ?
            ORDER BY e.ts_ms ASC
            LIMIT ?
            """,
            (str(FINBERT_MODEL_NAME), int(limit)),
        ).fetchall()
        return [
            {
                "body": row[4],
                "event_id": int(row[0]),
                "event_key": row[7],
                "event_type": "news",
                "source": row[2],
                "source_id": row[6],
                "symbol": row[5],
                "title": row[3],
                "ts_ms": int(row[1] or 0),
            }
            for row in (rows or [])
        ]
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("PROCESS_FINBERT_SENTIMENT_CLOSE_FAILED", e, scope="fetch_candidates")


def _persist_rows(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return

    def _write(conw) -> None:
        for row in rows:
            put_finbert_sentiment_enrichment(row, con=conw)

    run_write_txn(
        _write,
        table="news_event_features",
        operation="process_finbert_sentiment_batch",
        context={"job": JOB_NAME, "rows": int(len(rows))},
    )


def main() -> None:
    """Process one batch of unscored news rows with FinBERT sentiment."""
    init_db()
    if not USE_FINBERT_SENTIMENT:
        print(
            json.dumps(
                {
                    "job": JOB_NAME,
                    "model_name": FINBERT_MODEL_NAME,
                    "rows_processed": 0,
                    "skipped": True,
                    "ts_ms": int(time.time() * 1000),
                },
                separators=(",", ":"),
                sort_keys=True,
            )
        )
        return

    if not acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
        raise SystemExit(2)

    try:
        touch_job_lock(JOB_NAME, OWNER, PID)
        put_job_heartbeat(
            JOB_NAME,
            OWNER,
            PID,
            extra_json=json.dumps({"batch_size": int(BATCH_SIZE)}, separators=(",", ":"), sort_keys=True),
        )
        started_ms = int(time.time() * 1000)
        candidates = _fetch_candidates(BATCH_SIZE)
        if not candidates:
            print(
                json.dumps(
                    {
                        "job": JOB_NAME,
                        "model_name": FINBERT_MODEL_NAME,
                        "rows_processed": 0,
                        "ts_ms": int(started_ms),
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                )
            )
            return
        scored_rows = score_event_rows(candidates)
        _persist_rows(scored_rows)
        print(
            json.dumps(
                {
                    "job": JOB_NAME,
                    "model_name": FINBERT_MODEL_NAME,
                    "rows_processed": int(len(scored_rows)),
                    "ts_ms": int(time.time() * 1000),
                },
                separators=(",", ":"),
                sort_keys=True,
            )
        )
    finally:
        try:
            release_job_lock(JOB_NAME, OWNER, PID)
        except Exception as e:
            _warn_nonfatal("PROCESS_FINBERT_SENTIMENT_RELEASE_FAILED", e, job=JOB_NAME)


if __name__ == "__main__":
    main()
