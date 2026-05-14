"""
FILE: build_social_features.py

Operational helper script for `build_social_features`.
"""

"""
build_social_features.py

Aggregates raw social_posts into time-bucketed social_features.

- As-of safe
- Idempotent (INSERT OR REPLACE)
- Read-only w.r.t. trading
- Uses existing job locks / heartbeats
"""

import os
import time
import json
import logging
from collections import defaultdict

from engine.runtime.storage import (
    connect,
    init_db,
    acquire_job_lock,
    release_job_lock,
    touch_job_lock,
    put_job_heartbeat,
    checkpoint_if_due,
)

JOB_NAME = "build_social_features"
OWNER = os.environ.get("JOB_OWNER", os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")))
PID = os.getpid()

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [build_social_features] %(message)s",
)

BUCKET_SEC = int(os.environ.get("SOCIAL_FEATURE_BUCKET_SEC", "300"))
LOOKBACK_S = int(os.environ.get("SOCIAL_FEATURE_LOOKBACK_S", "86400"))

LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))
HEARTBEAT_EVERY_S = float(os.environ.get("HEARTBEAT_EVERY_S", "15"))
SLEEP_S = float(os.environ.get("SOCIAL_FEATURE_SLEEP_S", "30"))


def _bucket_start(ts_ms: int) -> int:
    return (ts_ms // (BUCKET_SEC * 1000)) * (BUCKET_SEC * 1000)


def main():
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("options_poll must be launched by supervisor")
        raise SystemExit(1)

    init_db()

    if not acquire_job_lock(JOB_NAME, OWNER, PID, stale_after_s=LOCK_STALE_AFTER_S):
        return

    try:
        last_hb = 0.0

        while True:
            now_ms = int(time.time() * 1000)
            start_ms = _bucket_start(now_ms - LOOKBACK_S * 1000)
            end_ms = _bucket_start(now_ms)

            con = connect()
            try:
                b = start_ms
                while b <= end_ms:
                    if time.time() - last_hb >= HEARTBEAT_EVERY_S:
                        touch_job_lock(JOB_NAME, OWNER, PID)
                        put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps({"bucket": b}))
                        last_hb = time.time()

                    rows = con.execute(
                        """
                        SELECT symbol, author_id_hash, platform
                        FROM social_posts
                        WHERE ts_ms >= ?
                          AND ts_ms < ?
                        """,
                        (b, b + BUCKET_SEC * 1000),
                    ).fetchall()

                    # This script builds compact bucket features from raw posts; it
                    # intentionally avoids heavy NLP so it can run frequently and safely.
                    per_sym = defaultdict(list)
                    per_sym_platforms = defaultdict(set)

                    for sym, ah, platform in rows:
                        per_sym[sym].append(ah)
                        if platform:
                            per_sym_platforms[sym].add(platform)

                    for sym, authors in per_sym.items():
                        uniq = set(a for a in authors if a)
                        cross_confirm = 1.0 if len(per_sym_platforms[sym]) >= 2 else 0.0

                        con.execute(
                            """
                            INSERT OR REPLACE INTO social_features(
                              symbol, bucket_ts_ms, bucket_sec,
                              mention_count, unique_authors,
                              cross_platform_confirm
                            )
                            VALUES (?, ?, ?, ?, ?, ?)
                            """,
                            (
                                sym,
                                b,
                                BUCKET_SEC,
                                len(authors),
                                len(uniq),
                                cross_confirm,
                            ),
                        )

                    b += BUCKET_SEC * 1000

                con.commit()
                checkpoint_if_due(writes=1)
            finally:
                con.close()

            time.sleep(SLEEP_S)

    finally:
        release_job_lock(JOB_NAME, OWNER, PID)


if __name__ == "__main__":
    main()
