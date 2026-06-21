"""Compute backend-aware news novelty and news-flow features.

README:
- Source: normalized news events, their symbol mappings, FinBERT/lexical
  sentiment, and persisted story embeddings.
- Cadence: periodic batch registered in ``engine.runtime.job_registry``
  with ``cadence_seconds=900``.
- Availability lag: novelty and features use ``availability_ts_ms`` from the
  ingested event timestamp and only compare to prior rows with availability
  <= the candidate story.
- Caveats: embedding-space isolation is strict; changing ``NEWS_EMBED_BACKEND``
  or model name starts a fresh novelty history for that space.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

from engine.data.news_flow import current_embedding_config, process_news_flow_batch
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.ingestion_status import record_pipeline_status
from engine.runtime.logging import get_logger
from engine.runtime.storage import (
    acquire_job_lock,
    init_db,
    put_job_heartbeat,
    release_job_lock,
    touch_job_lock,
)
from services.data_source_manager import get_manager

JOB_NAME = "process_news_flow"
OWNER = os.environ.get("JOB_OWNER", os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")))
PID = os.getpid()

BATCH_SIZE = max(1, int(os.environ.get("NEWS_FLOW_BATCH_SIZE", "100")))
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [process_news_flow] %(message)s",
)
LOG = get_logger("engine.data.jobs.process_news_flow")


def _warn_nonfatal(code: str, error: BaseException, **extra: Any) -> None:
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.data.jobs.process_news_flow",
        extra=extra or None,
        persist=False,
    )


def main() -> None:
    init_db()
    manager = get_manager()
    if not manager.is_job_enabled(JOB_NAME, default=True):
        manager.record_job_status(JOB_NAME, ok=True, message="process_news_flow disabled by data source control plane")
        raise SystemExit(0)
    if not acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
        raise SystemExit(2)

    started_ms = int(time.time() * 1000)
    cfg = current_embedding_config()
    try:
        touch_job_lock(JOB_NAME, OWNER, PID)
        put_job_heartbeat(
            JOB_NAME,
            OWNER,
            PID,
            extra_json=json.dumps(
                {"batch_size": int(BATCH_SIZE), "backend": cfg.backend, "model_name": cfg.model_name},
                separators=(",", ":"),
                sort_keys=True,
            ),
        )
        summary = process_news_flow_batch(limit=int(BATCH_SIZE), config=cfg, now_ms=started_ms)
        ok = not bool(summary.get("errors"))
        status = record_pipeline_status(
            JOB_NAME,
            ok=ok,
            raw_rows=int(summary.get("rows_seen") or 0),
            event_rows=int(summary.get("written") or 0),
            last_ingested_ts_ms=int(time.time() * 1000),
            error=("; ".join(str(e) for e in (summary.get("errors") or [])[:3])) if not ok else None,
            latency_ms=int(time.time() * 1000) - started_ms,
            meta=dict(summary),
        )
        manager.record_job_status(
            JOB_NAME,
            ok=ok,
            message="news flow batch complete" if ok else "news flow batch failed",
            error=("; ".join(str(e) for e in (summary.get("errors") or [])[:3])) if not ok else "",
            meta=dict(summary),
        )
        put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))
        print(json.dumps(dict(summary, job=JOB_NAME), separators=(",", ":"), sort_keys=True))
    except Exception as exc:
        _warn_nonfatal("PROCESS_NEWS_FLOW_FAILED_OPEN", exc, batch_size=int(BATCH_SIZE), backend=cfg.backend)
        manager.record_job_status(JOB_NAME, ok=False, message="news flow batch failed", error=str(exc))
        status = record_pipeline_status(
            JOB_NAME,
            ok=False,
            raw_rows=0,
            event_rows=0,
            last_ingested_ts_ms=int(time.time() * 1000),
            error=str(exc),
            latency_ms=int(time.time() * 1000) - started_ms,
            meta={"backend": cfg.backend, "model_name": cfg.model_name},
        )
        put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))
    finally:
        try:
            release_job_lock(JOB_NAME, OWNER, PID)
        except Exception as exc:
            _warn_nonfatal("PROCESS_NEWS_FLOW_RELEASE_FAILED", exc)


if __name__ == "__main__":
    main()
