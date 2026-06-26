"""Run one bounded batch of structured LLM event extraction."""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

from engine.data.llm_event_extraction import LLMEventExtractionConfig, run_llm_event_extraction_batch
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.ingestion_status import record_pipeline_status
from engine.runtime.logging import get_logger
from engine.runtime.storage import acquire_job_lock, init_db, put_job_heartbeat, release_job_lock, touch_job_lock
from services.data_source_manager import get_manager


JOB_NAME = "llm_event_extraction"
OWNER = os.environ.get("JOB_OWNER", os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")))
PID = os.getpid()
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [llm_event_extraction] %(message)s",
)
LOG = get_logger("engine.data.jobs.llm_event_extraction")


def _warn_nonfatal(code: str, error: BaseException, **extra: Any) -> None:
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.data.jobs.llm_event_extraction",
        extra=extra or None,
        persist=False,
    )


def main() -> None:
    init_db()
    manager = get_manager()
    if not manager.is_job_enabled(JOB_NAME, default=False):
        manager.record_job_status(JOB_NAME, ok=True, message="llm_event_extraction disabled by data source control plane")
        raise SystemExit(0)
    if not acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
        raise SystemExit(2)

    started_ms = int(time.time() * 1000)
    cfg = LLMEventExtractionConfig.from_env()
    try:
        touch_job_lock(JOB_NAME, OWNER, PID)
        put_job_heartbeat(
            JOB_NAME,
            OWNER,
            PID,
            extra_json=json.dumps(
                {
                    "provider": cfg.provider,
                    "model": cfg.model,
                    "max_docs": int(cfg.max_docs),
                    "max_cost_usd": float(cfg.max_cost_usd),
                    "enabled": bool(cfg.enabled),
                    "execution": False,
                },
                separators=(",", ":"),
                sort_keys=True,
            ),
        )
        summary = run_llm_event_extraction_batch(config=cfg)
        ok = not bool(summary.get("errors"))
        status = record_pipeline_status(
            JOB_NAME,
            ok=ok,
            raw_rows=int(summary.get("processed_docs") or 0),
            event_rows=int(summary.get("events_written") or 0),
            last_ingested_ts_ms=int(time.time() * 1000),
            error=("; ".join(str(e) for e in (summary.get("errors") or [])[:3])) if not ok else None,
            latency_ms=int(time.time() * 1000) - started_ms,
            meta=dict(summary),
        )
        manager.record_job_status(
            JOB_NAME,
            ok=ok,
            message="llm event extraction batch complete" if ok else "llm event extraction batch had rejections",
            error=("; ".join(str(e) for e in (summary.get("errors") or [])[:3])) if not ok else "",
            meta=dict(summary),
        )
        put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))
        print(json.dumps(dict(summary, job=JOB_NAME), separators=(",", ":"), sort_keys=True))
    except Exception as exc:
        _warn_nonfatal("LLM_EVENT_EXTRACTION_JOB_FAILED", exc)
        manager.record_job_status(JOB_NAME, ok=False, message="llm event extraction failed", error=str(exc))
        status = record_pipeline_status(
            JOB_NAME,
            ok=False,
            raw_rows=0,
            event_rows=0,
            last_ingested_ts_ms=int(time.time() * 1000),
            error=str(exc),
            latency_ms=int(time.time() * 1000) - started_ms,
            meta={"provider": cfg.provider, "model": cfg.model},
        )
        put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))
    finally:
        try:
            release_job_lock(JOB_NAME, OWNER, PID)
        except Exception as exc:
            _warn_nonfatal("LLM_EVENT_EXTRACTION_RELEASE_FAILED", exc)


if __name__ == "__main__":
    main()
