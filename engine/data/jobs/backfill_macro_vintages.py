"""One-shot ALFRED/FRED macro vintage backfill job.

README:
- Source: FRED/ALFRED ``series/observations`` API using realtime vintage
  windows and the existing ``FRED_API_KEY``.
- Cadence: manual/one-shot; normal polling of new vintages remains in
  ``poll_macro`` at the existing macro cadence.
- Availability lag: stored vintage rows carry availability equal to the
  vintage/release timestamp used by feature joins.
- Caveats: commodities and other non-revisioned series are stored with
  vintage equal to observation availability so the same PIT join can serve all
  macro features.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, List

from engine.data.factor_ingestion import backfill_macro_vintages
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.ingestion_status import record_pipeline_status
from engine.runtime.storage import init_db, put_job_heartbeat
from services.data_source_manager import get_manager

JOB_NAME = (os.environ.get("ENGINE_JOB_NAME") or "backfill_macro_vintages").strip() or "backfill_macro_vintages"
OWNER = os.environ.get("JOB_OWNER", os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")))
PID = os.getpid()

logging.basicConfig(
    level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format=f"%(asctime)s %(levelname)s [{JOB_NAME}] %(message)s",
)
LOGGER = logging.getLogger(__name__)


def _warn_nonfatal(code: str, error: BaseException, **extra: Any) -> None:
    log_failure(
        LOGGER,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component=__name__,
        extra=extra or None,
        persist=False,
    )


def _series_filter() -> List[str]:
    return [part.strip() for part in str(os.environ.get("MACRO_VINTAGE_BACKFILL_SERIES", "") or "").split(",") if part.strip()]


def main() -> None:
    init_db()
    manager = get_manager()
    started_ms = int(time.time() * 1000)
    try:
        summary = backfill_macro_vintages(
            series_ids=_series_filter() or None,
            force=str(os.environ.get("MACRO_VINTAGE_BACKFILL_FORCE", "0")).strip().lower() in {"1", "true", "yes", "on"},
            now_ms=started_ms,
        )
        ok = not bool(summary.get("errors"))
        status = record_pipeline_status(
            JOB_NAME,
            ok=ok,
            raw_rows=int(summary.get("vintage_rows") or 0),
            event_rows=0,
            last_ingested_ts_ms=int(time.time() * 1000),
            error=("; ".join(str(e) for e in (summary.get("errors") or [])[:3])) if not ok else None,
            latency_ms=int(time.time() * 1000) - started_ms,
            meta={
                "series": int(summary.get("series") or 0),
                "feature_rows": int(summary.get("feature_rows") or 0),
                "skipped": int(summary.get("skipped") or 0),
                "series_status": summary.get("series_status") or {},
            },
        )
        manager.record_job_status(
            JOB_NAME,
            ok=ok,
            message="macro vintage backfill complete" if ok else "macro vintage backfill failed",
            error=("; ".join(str(e) for e in (summary.get("errors") or [])[:3])) if not ok else "",
            meta={
                "series": int(summary.get("series") or 0),
                "vintage_rows": int(summary.get("vintage_rows") or 0),
                "feature_rows": int(summary.get("feature_rows") or 0),
                "skipped": int(summary.get("skipped") or 0),
            },
        )
        put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))
        if not ok:
            raise SystemExit(1)
    except SystemExit:
        raise
    except Exception as exc:
        _warn_nonfatal("BACKFILL_MACRO_VINTAGES_FAILED", exc)
        manager.record_job_status(JOB_NAME, ok=False, message="macro vintage backfill failed", error=str(exc))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
