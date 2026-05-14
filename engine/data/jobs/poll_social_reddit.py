"""
FILE: poll_social_reddit.py

Job entrypoint or scheduled task for `poll_social_reddit`.
"""

"""
poll_social_reddit.py

Selective Reddit ingestion for trading signals.

- Targets finance-relevant subreddits only
- Stores raw posts/comments into social_posts
- Uses same schema + safety guarantees as StockTwits
- Read-only, non-critical job
"""

import os
import time
import json
import hashlib
import logging
from typing import Any, List

try:
    import praw
    _PRAW_IMPORT_ERROR = None
except Exception as _praw_import_error:
    praw = None  # type: ignore
    _PRAW_IMPORT_ERROR = _praw_import_error

from engine.runtime.storage import (
    init_db,
    acquire_job_lock,
    release_job_lock,
    touch_job_lock,
    put_job_heartbeat,
    put_normalized_event,
    run_write_txn,
)
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.ingestion_status import record_pipeline_status
from engine.data._credentials import get_data_credential
from engine.data.event_normalization import normalize_social_event
from services.data_source_manager import get_manager

JOB_NAME = "poll_social_reddit"
OWNER = os.environ.get("JOB_OWNER", os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")))
PID = os.getpid()

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [poll_social_reddit] %(message)s",
)
LOGGER = logging.getLogger(__name__)
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
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
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _warn_state(code: str, message: str, **extra: Any) -> None:
    log_failure(
        LOGGER,
        event=str(code).lower(),
        code=str(code),
        message=str(message),
        error=None,
        level=logging.WARNING,
        component=__name__,
        extra=extra or None,
        persist=False,
    )

LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))
HEARTBEAT_EVERY_S = float(os.environ.get("HEARTBEAT_EVERY_S", "15"))

REDDIT_USER_AGENT = os.environ.get("REDDIT_USER_AGENT", "market-research-bot")

SUBREDDITS = os.environ.get(
    "REDDIT_SUBREDDITS",
    "wallstreetbets,stocks,investing,options,cryptocurrency,ethtrader",
).split(",")

POLL_LIMIT = int(os.environ.get("REDDIT_POLL_LIMIT", "50"))
SLEEP_S = float(os.environ.get("SOCIAL_POLL_SLEEP_S", "60"))

HASH_SALT = os.environ.get("SOCIAL_HASH_SALT", "social")


def _reddit_credentials() -> tuple[str, str]:
    return (
        get_data_credential("REDDIT_CLIENT_ID"),
        get_data_credential("REDDIT_CLIENT_SECRET"),
    )


def _sha(x: str) -> str:
    h = hashlib.sha256()
    h.update((HASH_SALT + "|" + str(x)).encode("utf-8", "ignore"))
    return h.hexdigest()


def _extract_symbols(text: str) -> List[str]:
    if not text:
        return []
    out = set()
    for tok in text.replace("$", " $").split():
        if tok.startswith("$") and tok[1:].isalpha() and 1 <= len(tok[1:]) <= 5:
            out.add(tok[1:].upper())
    return list(out)


def _bucket_ts_ms(ts_ms: int, bucket_sec: int = 300) -> int:
    bucket_ms = max(1, int(bucket_sec)) * 1000
    return int(ts_ms // bucket_ms) * bucket_ms


def _refresh_social_features(con, symbols: List[str], bucket_sec: int = 300) -> None:
    now_ms = int(time.time() * 1000)
    bstart = _bucket_ts_ms(now_ms, int(bucket_sec))
    bend = bstart + int(bucket_sec) * 1000

    for sym in sorted({str(s).upper().strip() for s in (symbols or []) if str(s).strip()}):
        row = con.execute(
            """
            SELECT
              COUNT(*) AS mention_count,
              COUNT(DISTINCT author_id_hash) AS unique_authors,
              COALESCE(SUM(COALESCE(like_count,0) + COALESCE(reply_count,0) + COALESCE(repost_count,0) + COALESCE(quote_count,0)), 0) AS engagement_now,
              COUNT(DISTINCT platform) AS platform_count
            FROM social_posts
            WHERE symbol = ?
              AND ts_ms >= ?
              AND ts_ms < ?
            """,
            (str(sym), int(bstart), int(bend)),
        ).fetchone()

        mention_count = int((row[0] if row else 0) or 0)
        unique_authors = int((row[1] if row else 0) or 0)
        engagement_now = float((row[2] if row else 0.0) or 0.0)
        platform_count = int((row[3] if row else 0) or 0)

        if mention_count <= 0:
            continue

        # These are deliberately lightweight heuristics. Richer sentiment/manip
        # modeling belongs downstream, not in the raw ingestion poller.
        mention_rate_z = float(mention_count)
        attention_shock = float(min(1.0, mention_count / 10.0))
        cross_platform_confirm = 1.0 if platform_count > 1 else 0.0

        con.execute(
            """
            INSERT INTO social_features(
              symbol, bucket_ts_ms, bucket_sec,
              mention_count, unique_authors, new_author_ratio,
              engagement_now, sentiment_mean, sentiment_dispersion,
              mention_rate_z, bot_likelihood_mean, promo_likelihood_mean,
              manip_risk, attention_shock, cross_platform_confirm
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, bucket_ts_ms, bucket_sec) DO UPDATE SET
              mention_count=excluded.mention_count,
              unique_authors=excluded.unique_authors,
              new_author_ratio=excluded.new_author_ratio,
              engagement_now=excluded.engagement_now,
              sentiment_mean=excluded.sentiment_mean,
              sentiment_dispersion=excluded.sentiment_dispersion,
              mention_rate_z=excluded.mention_rate_z,
              bot_likelihood_mean=excluded.bot_likelihood_mean,
              promo_likelihood_mean=excluded.promo_likelihood_mean,
              manip_risk=excluded.manip_risk,
              attention_shock=excluded.attention_shock,
              cross_platform_confirm=excluded.cross_platform_confirm
            """,
            (
                str(sym),
                int(bstart),
                int(bucket_sec),
                int(mention_count),
                int(unique_authors),
                0.0,
                float(engagement_now),
                0.0,
                0.0,
                float(mention_rate_z),
                0.0,
                0.0,
                0.0,
                float(attention_shock),
                float(cross_platform_confirm),
            ),
        )


def main():
    manager = get_manager()
    if not manager.is_job_enabled(JOB_NAME, default=True):
        manager.record_job_status(JOB_NAME, ok=True, message="reddit disabled by data source control plane")
        return

    if praw is None:
        _warn_state("POLL_SOCIAL_REDDIT_PRAW_UNAVAILABLE", "PRAW is unavailable; reddit polling is disabled.", error=repr(_PRAW_IMPORT_ERROR))
        record_pipeline_status(JOB_NAME, ok=False, error=str(_PRAW_IMPORT_ERROR), meta={"reason": "praw_unavailable"})
        manager.record_job_status(JOB_NAME, ok=False, message="reddit unavailable", error=str(_PRAW_IMPORT_ERROR))
        return

    reddit_client_id, reddit_client_secret = _reddit_credentials()
    if not reddit_client_id or not reddit_client_secret:
        _warn_state("POLL_SOCIAL_REDDIT_CREDENTIALS_MISSING", "Reddit credentials are missing; reddit polling is disabled.")
        record_pipeline_status(JOB_NAME, ok=False, error="reddit_credentials_missing", meta={"reason": "credentials_missing"})
        manager.record_job_status(JOB_NAME, ok=False, message="reddit credentials missing", error="reddit_credentials_missing")
        return

    init_db()

    if not acquire_job_lock(JOB_NAME, OWNER, PID, stale_after_s=LOCK_STALE_AFTER_S):
        logging.info("lock not acquired; exiting")
        return

    reddit = praw.Reddit(
        client_id=reddit_client_id,
        client_secret=reddit_client_secret,
        user_agent=REDDIT_USER_AGENT,
    )

    try:
        last_hb = 0.0

        while True:
            if not manager.is_job_enabled(JOB_NAME, default=True):
                manager.record_job_status(JOB_NAME, ok=True, message="reddit disabled by data source control plane")
                break

            now_s = time.time()
            if now_s - last_hb >= HEARTBEAT_EVERY_S:
                touch_job_lock(JOB_NAME, OWNER, PID)
                put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps({"stage": "poll"}))
                last_hb = now_s

            def _write(con):
                touched = set()
                raw_rows = 0
                event_rows = 0
                errors = []

                for sr in SUBREDDITS:
                    try:
                        subreddit = reddit.subreddit(sr.strip())
                        for post in subreddit.new(limit=POLL_LIMIT):
                            syms = _extract_symbols(f"{post.title} {post.selftext}")
                            if not syms:
                                continue

                            ts_ms = int(post.created_utc * 1000)
                            author_hash = _sha(post.author.name) if post.author else None

                            for sym in syms:
                                cur = con.execute(
                                    """
                                    INSERT OR IGNORE INTO social_posts(
                                      ts_ms, platform, symbol, post_id, author_id_hash,
                                      text, lang,
                                      like_count, reply_count, repost_count, quote_count,
                                      follower_count, is_spam, spam_reason
                                    )
                                    VALUES (?, 'reddit', ?, ?, ?, ?, 'en', ?, ?, NULL, NULL, NULL, 0, NULL)
                                    """,
                                    (
                                        ts_ms,
                                        sym,
                                        f"t3_{post.id}",
                                        author_hash,
                                        f"{post.title}\n{post.selftext}",
                                        int(post.score or 0),
                                        int(post.num_comments or 0),
                                    ),
                                )
                                raw_rows += 1 if int(cur.rowcount or 0) > 0 else 0
                                touched.add(str(sym).upper().strip())

                                try:
                                    put_normalized_event(
                                        normalize_social_event(
                                            {
                                                "ts_ms": int(ts_ms),
                                                "platform": "reddit",
                                                "subreddit": sr.strip(),
                                                "symbol": str(sym).upper(),
                                                "post_id": f"t3_{post.id}",
                                                "author_id_hash": author_hash,
                                                "like_count": int(post.score or 0),
                                                "reply_count": int(post.num_comments or 0),
                                                "title": f"{str(sym).upper()} reddit signal",
                                                "body": f"{post.title}\n{post.selftext}",
                                                "text": f"{post.title}\n{post.selftext}",
                                                "url": f"https://www.reddit.com{getattr(post, 'permalink', '')}",
                                                "event_key": f"social:reddit:{sym}:t3_{post.id}",
                                            }
                                        ),
                                        con=con,
                                    )
                                    event_rows += 1
                                except Exception as e:
                                    _warn_nonfatal("POLL_SOCIAL_REDDIT_EVENT_WRITE_FAILED", e, once_key=f"event_write:{sr}:{getattr(post, 'id', '')}", symbol=str(sym), subreddit=sr, post_id=getattr(post, "id", ""))
                    except Exception as e:
                        _warn_nonfatal("POLL_SOCIAL_REDDIT_SUBREDDIT_ERROR", e, once_key=f"subreddit:{sr}", subreddit=sr)
                        errors.append(f"{sr}:{e}")

                if touched:
                    _refresh_social_features(con, sorted(touched))

                return raw_rows, event_rows, errors, touched

            raw_rows, event_rows, errors, touched = run_write_txn(
                _write,
                table="social_posts",
                operation="ingest_reddit_batch",
                context={"job": JOB_NAME},
            )

            status = record_pipeline_status(
                JOB_NAME,
                ok=(len(errors) == 0),
                raw_rows=int(raw_rows),
                event_rows=int(event_rows),
                last_ingested_ts_ms=int(time.time() * 1000),
                error=("; ".join(errors[:3])) if errors else None,
                meta={"symbols_touched": sorted(touched)[:25], "subreddits": [s.strip() for s in SUBREDDITS if s.strip()]},
            )
            manager.record_job_status(
                JOB_NAME,
                ok=bool(len(errors) == 0),
                message="reddit cycle complete",
                error=("; ".join(errors[:3])) if errors else "",
                meta={"raw_rows": int(raw_rows), "event_rows": int(event_rows), "symbols_touched": sorted(touched)[:25]},
            )
            put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))

            time.sleep(SLEEP_S)

    finally:
        release_job_lock(JOB_NAME, OWNER, PID)


if __name__ == "__main__":
    main()
