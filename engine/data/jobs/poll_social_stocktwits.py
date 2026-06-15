"""
FILE: poll_social_stocktwits.py

Job entrypoint or scheduled task for `poll_social_stocktwits`.
"""


"""
poll_social_stocktwits.py

Ingest social messages from StockTwits (best-effort, safe defaults).

Writes to SQLite:
- social_posts

This job is non-critical: failures must not stop the rest of the system.
"""

import hashlib
import json
import os
import time
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

from engine.data.time_utils import utc_ms_from_datetime
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.storage import (
    init_db,
    acquire_job_lock,
    release_job_lock,
    touch_job_lock,
    put_job_heartbeat,
    put_normalized_event,
    run_write_txn,
)
from engine.runtime.ingestion_status import record_pipeline_status
from engine.data.event_normalization import normalize_social_event
from services.data_source_manager import get_manager

_REGION_MAP_CACHE = None

JOB_NAME = "poll_social_stocktwits"
OWNER = os.environ.get("JOB_OWNER", os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")))
PID = os.getpid()

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [poll_social_stocktwits] %(message)s",
)
LOG = logging.getLogger("engine.data.jobs.poll_social_stocktwits")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.data.jobs.poll_social_stocktwits",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)

LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))
HEARTBEAT_EVERY_S = float(os.environ.get("HEARTBEAT_EVERY_S", "15.0"))

# Public trending stream (no auth). This is the safest default.
ST_TRENDING_URL = os.environ.get("STOCKTWITS_TRENDING_URL", "https://api.stocktwits.com/api/2/streams/trending.json")

# Optional: attempt symbol streams (may require partner access)
ST_SYMBOL_URL_TMPL = os.environ.get(
    "STOCKTWITS_SYMBOL_URL_TMPL",
    "https://api.stocktwits.com/api/2/streams/symbol/{symbol}.json",
)

ST_TIMEOUT_S = float(os.environ.get("STOCKTWITS_TIMEOUT_S", "10.0"))
ST_SLEEP_S = float(os.environ.get("SOCIAL_POLL_SLEEP_S", "30.0"))

HASH_SALT = os.environ.get("SOCIAL_HASH_SALT", "social")


def _sha(s: str) -> str:
    h = hashlib.sha256()
    h.update((HASH_SALT + "|" + str(s)).encode("utf-8", "ignore"))
    return h.hexdigest()


def _parse_ts_ms(created_at: str) -> int:
    # StockTwits created_at is ISO 8601, e.g. 2020-01-01T12:34:56Z
    try:
        raw = str(created_at or "").strip()
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return utc_ms_from_datetime(parsed, field_name="stocktwits_created_at")
    except Exception as e:
        _warn_nonfatal("POLL_STOCKTWITS_TS_PARSE_FAILED", e, once_key="parse_ts_ms", created_at=str(created_at or ""))
        return int(time.time() * 1000)


def _safe_int(x) -> Optional[int]:
    try:
        if x is None:
            return None
        return int(x)
    except Exception as e:
        _warn_nonfatal("POLL_STOCKTWITS_SAFE_INT_FAILED", e, once_key="safe_int", value_type=type(x).__name__)
        return None


def _safe_get_msg_symbols(msg: Dict[str, Any]) -> List[str]:
    out = []
    try:
        syms = msg.get("symbols") or []
        for s in syms:
            sym = str((s or {}).get("symbol") or "").upper().strip()
            if sym:
                out.append(sym)
    except Exception as e:
        _warn_nonfatal("POLL_STOCKTWITS_SYMBOL_PARSE_FAILED", e, once_key="safe_get_msg_symbols")
    return out


def _fetch_json(url: str) -> tuple[Optional[Dict[str, Any]], str]:
    try:
        r = requests.get(url, timeout=float(ST_TIMEOUT_S))
        if r.status_code != 200:
            detail = str(r.text or "").strip().replace("\r", " ").replace("\n", " ")
            detail = detail[:160]
            return None, f"stocktwits_http_{int(r.status_code)}:{detail or 'non_200_response'}"
        return r.json(), ""
    except Exception as e:
        _warn_nonfatal("POLL_STOCKTWITS_FETCH_FAILED", e, once_key=f"fetch_json:{url}", url=str(url))
        return None, f"stocktwits_request_failed:{type(e).__name__}:{e}"


def _insert_message(con, *, platform: str, symbol: str, msg: Dict[str, Any]) -> tuple[int, int]:
    post_id = str(msg.get("id") or "")
    if not post_id:
        return 0, 0

    created_at = msg.get("created_at")
    ts_ms = _parse_ts_ms(str(created_at)) if created_at else int(time.time() * 1000)

    body = str(msg.get("body") or "")
    user = msg.get("user") or {}

    author_id = user.get("id")
    author_hash = _sha(str(author_id)) if author_id is not None else None

    like_count = _safe_int(msg.get("likes") or msg.get("like_count"))
    reply_count = _safe_int(msg.get("replies") or msg.get("reply_count"))
    repost_count = _safe_int(msg.get("reshares") or msg.get("repost_count"))
    quote_count = _safe_int(msg.get("quotes") or msg.get("quote_count"))
    follower_count = _safe_int(user.get("followers"))

    # Raw social rows are the canonical store; synthetic events are secondary
    # breadcrumbs so the rest of the pipeline can notice the attention spike.
    cur = con.execute(
        """
        INSERT OR IGNORE INTO social_posts(
          ts_ms, platform, symbol, post_id, author_id_hash,
          text, lang,
          like_count, reply_count, repost_count, quote_count,
          follower_count, is_spam, spam_reason
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL)
        """,
        (
            int(ts_ms),
            str(platform),
            str(symbol),
            str(post_id),
            author_hash,
            body,
            None,
            like_count,
            reply_count,
            repost_count,
            quote_count,
            follower_count,
        ),
    )
    raw_inserted = 1 if int(cur.rowcount or 0) > 0 else 0

    try:
        put_normalized_event(
            normalize_social_event(
                {
                    "ts_ms": int(ts_ms),
                    "platform": str(platform),
                    "symbol": str(symbol).upper(),
                    "post_id": str(post_id),
                    "author_id_hash": author_hash,
                    "like_count": like_count,
                    "reply_count": reply_count,
                    "repost_count": repost_count,
                    "quote_count": quote_count,
                    "follower_count": follower_count,
                    "title": f"{str(symbol).upper()} social signal",
                    "body": body,
                    "text": body,
                    "url": f"https://stocktwits.com/message/{post_id}",
                    "event_key": f"social:{platform}:{symbol}:{post_id}",
                }
            ),
            con=con,
        )
        return raw_inserted, 1
    except Exception as e:
        _warn_nonfatal(
            "POLL_STOCKTWITS_NORMALIZED_EVENT_FAILED",
            e,
            once_key=f"normalized_event:{platform}:{symbol}:{post_id}",
            platform=str(platform),
            symbol=str(symbol),
            post_id=str(post_id),
        )
        return raw_inserted, 0


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

        # Keep feature derivation simple and deterministic here. More advanced
        # social modeling should consume `social_posts` / `social_features` later.
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


def _poll_once(con):
    n = 0
    touched = set()
    event_rows = 0

    payload, fetch_error = _fetch_json(str(ST_TRENDING_URL))
    if fetch_error:
        raise RuntimeError(fetch_error)
    payload = payload or {}
    msgs = payload.get("messages") or []
    for msg in msgs:
        syms = _safe_get_msg_symbols(msg)
        for sym in syms:
            try:
                raw_inserted, event_inserted = _insert_message(con, platform="stocktwits", symbol=sym, msg=msg)
                touched.add(str(sym).upper().strip())
                n += int(raw_inserted)
                event_rows += int(event_inserted)
            except Exception as e:
                _warn_nonfatal("POLL_STOCKTWITS_INSERT_MESSAGE_FAILED", e, once_key="insert_message", symbol=str(sym))

    if touched:
        _refresh_social_features(con, sorted(touched))

    return int(n), int(event_rows), sorted(touched)


def main():
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("options_poll must be launched by supervisor")
        raise SystemExit(1)

    manager = get_manager()
    if not manager.is_job_enabled(JOB_NAME, default=True):
        manager.record_job_status(JOB_NAME, ok=True, message="stocktwits disabled by data source control plane")
        return

    init_db()

    if not acquire_job_lock(JOB_NAME, OWNER, PID, stale_after_s=int(LOCK_STALE_AFTER_S)):
        logging.info("lock not acquired; exiting")
        return

    try:
        last_hb_s = 0.0
        while True:
            if not manager.is_job_enabled(JOB_NAME, default=True):
                manager.record_job_status(JOB_NAME, ok=True, message="stocktwits disabled by data source control plane")
                break
            now_s = time.time()
            if (now_s - last_hb_s) >= float(HEARTBEAT_EVERY_S):
                try:
                    touch_job_lock(JOB_NAME, OWNER, PID)
                    put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps({"stage": "poll"}))
                except Exception as e:
                    _warn_nonfatal("POLL_STOCKTWITS_HEARTBEAT_FAILED", e, once_key="heartbeat", job_name=JOB_NAME)
                last_hb_s = now_s

            try:
                n, event_rows, touched = run_write_txn(
                    _poll_once,
                    table="social_posts",
                    operation="ingest_stocktwits_batch",
                    context={"job": JOB_NAME},
                )
                status = record_pipeline_status(
                    JOB_NAME,
                    ok=True,
                    raw_rows=int(n),
                    event_rows=int(event_rows),
                    last_ingested_ts_ms=int(time.time() * 1000),
                    meta={"symbols_touched": touched[:25]},
                )
                manager.record_job_status(
                    JOB_NAME,
                    ok=True,
                    message="stocktwits cycle complete",
                    meta={"raw_rows": int(n), "event_rows": int(event_rows), "symbols_touched": touched[:25]},
                )
                put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))
                if n:
                    logging.info("ingested=%d rows symbols=%s", int(n), ",".join(touched[:20]))
            except Exception as e:
                _warn_nonfatal("POLL_SOCIAL_STOCKTWITS_POLL_ERROR", e, once_key="poll_error")
                status = record_pipeline_status(
                    JOB_NAME,
                    ok=False,
                    raw_rows=0,
                    event_rows=0,
                    last_ingested_ts_ms=int(time.time() * 1000),
                    error=str(e),
                )
                manager.record_job_status(JOB_NAME, ok=False, message="stocktwits cycle failed", error=str(e))
                put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))

            time.sleep(float(ST_SLEEP_S))
    finally:
        try:
            release_job_lock(JOB_NAME, OWNER, PID)
        except Exception as e:
            _warn_nonfatal("POLL_STOCKTWITS_RELEASE_LOCK_FAILED", e, once_key="release_lock", job_name=JOB_NAME)


if __name__ == "__main__":
    main()
