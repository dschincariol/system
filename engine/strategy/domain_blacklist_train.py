"""
FILE: domain_blacklist_train.py

Placeholder training job for source-domain blacklisting. The schema hooks and
lock handling are in place, but the real training logic is intentionally left
for a later implementation that depends on finalized decision/outcome tables.
"""

import os
import logging

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import init_db, acquire_job_lock, release_job_lock

JOB_NAME = "train_domain_blacklist"
OWNER = os.environ.get("JOB_OWNER", "system")
PID = os.getpid()

LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "300"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [domain_blacklist_train] %(message)s",
)
LOG = get_logger("engine.strategy.domain_blacklist_train")


def _warn_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    log_failure(
        LOG,
        event="domain_blacklist_train_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.domain_blacklist_train",
        extra=extra or None,
        persist=False,
    )


def main():
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("options_poll must be launched by supervisor")
        raise SystemExit(1)

    init_db()

    if not acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
        raise SystemExit(2)

    try:
        # The file is intentionally a scaffold so the job can be registered
        # before the final attribution schema is available.
        # This trainer needs to join:
        # - decisions/predictions (domain/regime already logged in extra_json from process_events.py)
        # - realized outcomes (PnL attribution or labels or fills)
        #
        # We will fill this in AFTER you provide:
        #   dev_core/decision_log.py
        #   dev_core/validation.py
        #   dev_core/execution_ledger.py  (or whichever file stores realized PnL attribution)
        #
        # This file intentionally does not run until patched with your real schema.
        logging.info("trainer placeholder: upload decision_log.py + validation.py + execution_ledger.py to complete")
    finally:
        try:
            release_job_lock(JOB_NAME, OWNER, PID)
        except Exception as e:
            _warn_nonfatal("DOMAIN_BLACKLIST_TRAIN_LOCK_RELEASE_FAILED", e, job=JOB_NAME)

if __name__ == "__main__":
    main()
