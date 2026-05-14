"""
update_universe.py

Dynamic universe maintenance.

Strategy:
- Seed a conservative baseline universe
- Pull symbol candidates from persisted high-signal sources:
  - news
  - social
  - SEC filings
  - options activity
- Add a small corroboration boost from recent symbolized events
- Decay stale scores gradually
- Promote only quality-qualified symbols instead of always filling a fixed cap
"""

from __future__ import annotations

import json
import logging
import math
import os
import time
from typing import Dict, Iterable, List, Tuple

from engine.data.default_symbols import load_default_symbols, parse_symbol_limit
from engine.data.universe import extract_symbol_candidates, upsert_symbol
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.storage import (
    _ensure_sec_filings_schema,
    _ensure_universe_audit_schema,
    acquire_job_lock,
    connect,
    init_db,
    put_job_heartbeat,
    release_job_lock,
    touch_job_lock,
)

JOB_NAME = "update_universe"
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [update_universe] %(message)s",
)
LOG = logging.getLogger("engine.data.jobs.update_universe")
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
        component="engine.data.jobs.update_universe",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)

LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))
HEARTBEAT_EVERY_S = float(os.environ.get("HEARTBEAT_EVERY_S", "15.0"))

UNIVERSE_EVENT_LOOKBACK_S = int(os.environ.get("UNIVERSE_EVENT_LOOKBACK_S", "21600"))
UNIVERSE_NEWS_LOOKBACK_S = int(os.environ.get("UNIVERSE_NEWS_LOOKBACK_S", str(UNIVERSE_EVENT_LOOKBACK_S)))
UNIVERSE_SOCIAL_LOOKBACK_S = int(os.environ.get("UNIVERSE_SOCIAL_LOOKBACK_S", "21600"))
UNIVERSE_FILINGS_LOOKBACK_S = int(os.environ.get("UNIVERSE_FILINGS_LOOKBACK_S", str(2 * 86400)))
UNIVERSE_OPTIONS_LOOKBACK_S = int(os.environ.get("UNIVERSE_OPTIONS_LOOKBACK_S", "21600"))

UNIVERSE_ACTIVE_N = parse_symbol_limit(os.environ.get("UNIVERSE_ACTIVE_N"), 750)
UNIVERSE_WATCH_N = parse_symbol_limit(os.environ.get("UNIVERSE_WATCH_N"), 6000)

UNIVERSE_SOCIAL_BUCKET_SEC = int(os.environ.get("UNIVERSE_SOCIAL_BUCKET_SEC", "300"))
UNIVERSE_SOCIAL_Z_TH = float(os.environ.get("UNIVERSE_SOCIAL_Z_TH", "1.75"))
UNIVERSE_SOCIAL_MIN_AUTHORS = int(os.environ.get("UNIVERSE_SOCIAL_MIN_AUTHORS", "8"))
UNIVERSE_SOCIAL_MAX_MANIP_RISK = float(os.environ.get("UNIVERSE_SOCIAL_MAX_MANIP_RISK", "0.80"))

UNIVERSE_SCORE_DECAY_PER_RUN = float(os.environ.get("UNIVERSE_SCORE_DECAY_PER_RUN", "0.02"))
UNIVERSE_MIN_SCORE_FLOOR = float(os.environ.get("UNIVERSE_MIN_SCORE_FLOOR", "-5.0"))
UNIVERSE_SCORE_REL_FLOOR = float(os.environ.get("UNIVERSE_SCORE_REL_FLOOR", "0.55"))
UNIVERSE_SCORE_ABS_FLOOR = float(os.environ.get("UNIVERSE_SCORE_ABS_FLOOR", "0.85"))
UNIVERSE_SCORE_GAP_STOP = float(os.environ.get("UNIVERSE_SCORE_GAP_STOP", "0.40"))
UNIVERSE_QUAL_MIN_KEEP = int(os.environ.get("UNIVERSE_QUAL_MIN_KEEP", "12"))
UNIVERSE_PRICE_MAX_AGE_MS = int(os.environ.get("UNIVERSE_PRICE_MAX_AGE_MS", str(15 * 60 * 1000)))
UNIVERSE_NEW_SYMBOL_GRACE_MS = int(os.environ.get("UNIVERSE_NEW_SYMBOL_GRACE_MS", str(6 * 60 * 60 * 1000)))
UNIVERSE_ACTIVE_PRICE_REQUIRED = str(os.environ.get("UNIVERSE_ACTIVE_PRICE_REQUIRED", "1")).strip().lower() in ("1", "true", "yes", "on")

BASELINE_SYMBOLS = load_default_symbols()
BASELINE_SYMBOL_SET = {str(sym or "").upper().strip() for sym in BASELINE_SYMBOLS if str(sym or "").strip()}
CORE_SYMBOLS = list(BASELINE_SYMBOLS)
CORE_SYMBOL_SET = {sym for sym in CORE_SYMBOLS if sym}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_float(value, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception as e:
        _warn_nonfatal(
            "UPDATE_UNIVERSE_SAFE_FLOAT_FAILED",
            e,
            once_key="safe_float",
            value_type=type(value).__name__,
        )
        return float(default)
    if not math.isfinite(out):
        return float(default)
    return float(out)


def _safe_load_meta(meta_json: str) -> Dict:
    try:
        data = json.loads(meta_json) if meta_json else {}
    except Exception as e:
        _warn_nonfatal(
            "UPDATE_UNIVERSE_META_JSON_PARSE_FAILED",
            e,
            once_key="safe_load_meta",
            value_type=type(meta_json).__name__,
        )
        return {}
    return data if isinstance(data, dict) else {}


def _clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return float(max(lo, min(hi, float(value))))


def _freshness_multiplier(age_ms: int, half_life_s: int) -> float:
    age_s = max(0.0, float(age_ms) / 1000.0)
    if half_life_s <= 0:
        return 1.0
    return float(0.5 ** (age_s / float(half_life_s)))


def _seed_baseline(con) -> None:
    now_ms = _now_ms()
    for symbol in BASELINE_SYMBOLS:
        try:
            con.execute(
                """
                INSERT OR IGNORE INTO symbols(
                  symbol, asset_class, status, score,
                  created_ts_ms, updated_ts_ms
                )
                VALUES (?, ?, 'WATCH', 0.5, ?, ?)
                """,
                (symbol, "UNKNOWN", now_ms, now_ms),
            )
            con.execute(
                """
                UPDATE symbols
                SET status='WATCH', updated_ts_ms=?
                WHERE symbol=?
                  AND status='DISABLED'
                """,
                (int(now_ms), str(symbol or "").upper().strip()),
            )
        except Exception as e:
            _warn_nonfatal("UPDATE_UNIVERSE_SEED_BASELINE_FAILED", e, once_key="seed_baseline", symbol=str(symbol))


def _decay_scores(con) -> None:
    try:
        con.execute(
            """
            UPDATE symbols
            SET score = MAX(?, score * (1.0 - ?)),
                updated_ts_ms = ?
            """,
            (float(UNIVERSE_MIN_SCORE_FLOOR), float(UNIVERSE_SCORE_DECAY_PER_RUN), _now_ms()),
        )
    except Exception as e:
        _warn_nonfatal("UPDATE_UNIVERSE_DECAY_SCORES_FAILED", e, once_key="decay_scores")


def _touch_progress(kind: str, extra: Dict | None = None) -> None:
    try:
        touch_job_lock(JOB_NAME, OWNER, PID)
        put_job_heartbeat(
            JOB_NAME,
            OWNER,
            PID,
            extra_json=json.dumps({"stage": kind, **(extra or {})}, separators=(",", ":"), sort_keys=True),
        )
    except Exception as e:
        _warn_nonfatal("UPDATE_UNIVERSE_HEARTBEAT_FAILED", e, once_key="touch_progress", stage=str(kind))


def _news_rows(con, cutoff_ms: int) -> List[Tuple]:
    try:
        return con.execute(
            """
            SELECT symbol, bucket_ts_ms, news_velocity, event_density, event_count,
                   distinct_cluster_count, avg_novelty, duplicate_share, sentiment_trend
            FROM news_symbol_features
            WHERE bucket_ts_ms >= ?
            ORDER BY bucket_ts_ms DESC, event_density DESC, news_velocity DESC, symbol ASC
            """,
            (int(cutoff_ms),),
        ).fetchall() or []
    except Exception as e:
        _warn_nonfatal("UPDATE_UNIVERSE_NEWS_ROWS_FAILED", e, once_key="news_rows", cutoff_ms=int(cutoff_ms))
        return []


def _apply_news_candidates(con, now_ms: int) -> int:
    rows = _news_rows(con, now_ms - UNIVERSE_NEWS_LOOKBACK_S * 1000)
    latest: Dict[str, Tuple] = {}
    for row in rows:
        sym = str(row[0] or "").upper().strip()
        if sym and sym not in latest:
            latest[sym] = row

    upserts = 0
    for sym, row in latest.items():
        _, bucket_ts_ms, news_velocity, event_density, event_count, distinct_cluster_count, avg_novelty, duplicate_share, sentiment_trend = row
        density_norm = _clamp(_safe_float(event_density) / 0.25)
        velocity_norm = _clamp(_safe_float(news_velocity) / 4.0)
        novelty_norm = _clamp(_safe_float(avg_novelty))
        cluster_norm = _clamp(_safe_float(distinct_cluster_count) / 4.0)
        uniqueness_norm = _clamp(1.0 - _safe_float(duplicate_share))
        sentiment_norm = _clamp(abs(_safe_float(sentiment_trend)) / 0.6)
        freshness = _freshness_multiplier(now_ms - int(bucket_ts_ms or now_ms), 3 * 3600)
        boost = freshness * (
            0.12
            + 0.28 * density_norm
            + 0.18 * velocity_norm
            + 0.16 * novelty_norm
            + 0.12 * cluster_norm
            + 0.09 * uniqueness_norm
            + 0.05 * sentiment_norm
        )
        if int(event_count or 0) <= 0 or boost <= 0.04:
            continue
        upsert_symbol(
            con,
            sym,
            status="WATCH",
            score_delta=float(boost),
            last_seen_event_ts_ms=int(bucket_ts_ms or now_ms),
            meta={
                "source_news_score": float(boost),
                "source_news_event_density": float(_safe_float(event_density)),
                "source_news_event_count": int(event_count or 0),
                "source_news_clusters": int(distinct_cluster_count or 0),
                "source_news_avg_novelty": float(_safe_float(avg_novelty)),
            },
        )
        upserts += 1
    return int(upserts)


def _apply_social_candidates(con, now_ms: int) -> int:
    try:
        rows = con.execute(
            """
            SELECT symbol, bucket_ts_ms, mention_count, unique_authors, mention_rate_z,
                   attention_shock, cross_platform_confirm, engagement_now, manip_risk
            FROM social_features
            WHERE bucket_sec = ?
              AND bucket_ts_ms >= ?
            ORDER BY bucket_ts_ms DESC, mention_rate_z DESC, symbol ASC
            """,
            (int(UNIVERSE_SOCIAL_BUCKET_SEC), int(now_ms - UNIVERSE_SOCIAL_LOOKBACK_S * 1000)),
        ).fetchall() or []
    except Exception as e:
        _warn_nonfatal("UPDATE_UNIVERSE_FILINGS_ROWS_FAILED", e, once_key="filings_rows")
        return 0

    latest: Dict[str, Tuple] = {}
    for row in rows:
        sym = str(row[0] or "").upper().strip()
        if sym and sym not in latest:
            latest[sym] = row

    upserts = 0
    for sym, row in latest.items():
        _, bucket_ts_ms, mention_count, unique_authors, mention_rate_z, attention_shock, cross_platform_confirm, engagement_now, manip_risk = row
        z = _safe_float(mention_rate_z)
        authors = int(unique_authors or 0)
        risk = _safe_float(manip_risk)
        if z < UNIVERSE_SOCIAL_Z_TH and authors < UNIVERSE_SOCIAL_MIN_AUTHORS:
            continue
        if risk >= UNIVERSE_SOCIAL_MAX_MANIP_RISK:
            continue
        attention_norm = _clamp(_safe_float(attention_shock))
        confirm_norm = _clamp(_safe_float(cross_platform_confirm))
        authors_norm = _clamp(float(authors) / 40.0)
        z_norm = _clamp(z / 4.0)
        engagement_norm = _clamp(math.log1p(max(0.0, _safe_float(engagement_now))) / math.log(25.0))
        freshness = _freshness_multiplier(now_ms - int(bucket_ts_ms or now_ms), 2 * 3600)
        boost = freshness * (
            0.10
            + 0.28 * z_norm
            + 0.18 * authors_norm
            + 0.18 * attention_norm
            + 0.14 * confirm_norm
            + 0.12 * engagement_norm
            + 0.10 * _clamp(1.0 - risk)
        )
        if boost <= 0.05:
            continue
        upsert_symbol(
            con,
            sym,
            status="WATCH",
            score_delta=float(boost),
            last_seen_event_ts_ms=int(bucket_ts_ms or now_ms),
            meta={
                "source_social_score": float(boost),
                "source_social_z": float(z),
                "source_social_authors": int(authors),
                "source_social_attention_shock": float(_safe_float(attention_shock)),
                "source_social_manip_risk": float(risk),
                "source_social_mentions": int(mention_count or 0),
            },
        )
        upserts += 1
    return int(upserts)


def _filing_form_weight(form: str) -> float:
    text = str(form or "").upper().strip()
    if text in {"8-K", "10-Q", "10-K", "6-K", "S-1", "F-1", "13D", "SC 13D"}:
        return 1.0
    if text in {"13G", "SC 13G", "424B2", "424B3", "425", "DEF 14A"}:
        return 0.8
    if text in {"3", "4", "5", "144"}:
        return 0.55
    return 0.45


def _apply_filings_candidates(con, now_ms: int) -> int:
    try:
        rows = con.execute(
            """
            SELECT symbol, form, ts_ms
            FROM sec_filings
            WHERE ts_ms >= ?
            ORDER BY ts_ms DESC, symbol ASC
            """,
            (int(now_ms - UNIVERSE_FILINGS_LOOKBACK_S * 1000),),
        ).fetchall() or []
    except Exception as e:
        _warn_nonfatal("UPDATE_UNIVERSE_OPTIONS_ROWS_FAILED", e, once_key="options_rows")
        return 0

    grouped: Dict[str, List[Tuple[str, int]]] = {}
    for symbol, form, ts_ms in rows:
        sym = str(symbol or "").upper().strip()
        if not sym:
            continue
        grouped.setdefault(sym, []).append((str(form or "").upper().strip(), int(ts_ms or now_ms)))

    upserts = 0
    for sym, filings in grouped.items():
        weighted = 0.0
        forms: List[str] = []
        newest_ts_ms = 0
        for form, ts_ms in filings[:8]:
            age_ms = max(0, now_ms - int(ts_ms))
            weighted += _filing_form_weight(form) * _freshness_multiplier(age_ms, 24 * 3600)
            forms.append(form)
            newest_ts_ms = max(newest_ts_ms, int(ts_ms))
        filing_count = len(filings)
        intensity = _clamp(weighted / 2.0)
        count_norm = _clamp(float(filing_count) / 4.0)
        boost = 0.12 + 0.48 * intensity + 0.20 * count_norm
        if boost <= 0.05:
            continue
        upsert_symbol(
            con,
            sym,
            status="WATCH",
            score_delta=float(boost),
            last_seen_event_ts_ms=int(newest_ts_ms or now_ms),
            meta={
                "source_filings_score": float(boost),
                "source_filings_count": int(filing_count),
                "source_filings_recent_forms": ",".join(forms[:4]),
            },
        )
        upserts += 1
    return int(upserts)


def _apply_options_candidates(con, now_ms: int) -> int:
    try:
        rows = con.execute(
            """
            SELECT symbol, bucket_ts_ms, signal_score, unusual_volume_score,
                   unusual_volume_contracts, call_put_volume_ratio
            FROM options_symbol_features
            WHERE bucket_ts_ms >= ?
            ORDER BY bucket_ts_ms DESC, signal_score DESC, symbol ASC
            """,
            (int(now_ms - UNIVERSE_OPTIONS_LOOKBACK_S * 1000),),
        ).fetchall() or []
    except Exception as e:
        _warn_nonfatal("UPDATE_UNIVERSE_EVENT_ROWS_FAILED", e, once_key="event_rows")
        return 0

    latest: Dict[str, Tuple] = {}
    for row in rows:
        sym = str(row[0] or "").upper().strip()
        if sym and sym not in latest:
            latest[sym] = row

    upserts = 0
    for sym, row in latest.items():
        _, bucket_ts_ms, signal_score, unusual_volume_score, unusual_volume_contracts, call_put_volume_ratio = row
        signal_norm = _clamp(_safe_float(signal_score) / 0.75)
        unusual_norm = _clamp(math.log1p(max(0.0, _safe_float(unusual_volume_score))) / math.log(5.0))
        contract_norm = _clamp(float(int(unusual_volume_contracts or 0)) / 6.0)
        ratio = max(1e-6, _safe_float(call_put_volume_ratio, 1.0))
        flow_norm = _clamp(abs(math.log(ratio)) / 1.25)
        freshness = _freshness_multiplier(now_ms - int(bucket_ts_ms or now_ms), 3 * 3600)
        boost = freshness * (
            0.10
            + 0.34 * signal_norm
            + 0.28 * unusual_norm
            + 0.16 * contract_norm
            + 0.12 * flow_norm
        )
        if boost <= 0.05:
            continue
        upsert_symbol(
            con,
            sym,
            status="WATCH",
            score_delta=float(boost),
            last_seen_event_ts_ms=int(bucket_ts_ms or now_ms),
            meta={
                "source_options_score": float(boost),
                "source_options_signal_score": float(_safe_float(signal_score)),
                "source_options_unusual_volume_score": float(_safe_float(unusual_volume_score)),
                "source_options_unusual_contracts": int(unusual_volume_contracts or 0),
            },
        )
        upserts += 1
    return int(upserts)


def _event_type_weight(event_type: str) -> float:
    text = str(event_type or "").strip().lower()
    if text == "news":
        return 0.18
    if text == "social":
        return 0.14
    if text == "filing":
        return 0.22
    if text == "insider":
        return 0.20
    if text == "congressional":
        return 0.18
    if text == "options":
        return 0.20
    return 0.08


def _apply_event_corroboration(con, now_ms: int) -> int:
    try:
        rows = con.execute(
            """
            SELECT id, ts_ms, symbol, event_type, title, body, derived_features, meta_json
            FROM events
            WHERE ts_ms >= ?
              AND COALESCE(event_type, '') IN ('news', 'social', 'filing', 'insider', 'congressional', 'options')
            ORDER BY ts_ms DESC
            LIMIT 1500
            """,
            (int(now_ms - UNIVERSE_EVENT_LOOKBACK_S * 1000),),
        ).fetchall() or []
    except Exception as e:
        _warn_nonfatal("UPDATE_UNIVERSE_SYMBOL_ROWS_FAILED", e, once_key="disable_stale_symbols")
        return 0

    upserts = 0
    last_hb_s = 0.0
    for event_id, ts_ms, symbol, event_type, title, body, derived_features_json, meta_json in rows:
        now_s = time.time()
        if (now_s - last_hb_s) >= HEARTBEAT_EVERY_S:
            _touch_progress("event_corroboration", {"event_id": int(event_id), "event_type": str(event_type or "")})
            last_hb_s = now_s

        meta = _safe_load_meta(meta_json)
        derived = _safe_load_meta(derived_features_json)
        if not derived and isinstance(meta.get("derived_features"), dict):
            derived = dict(meta.get("derived_features") or {})
        novelty = _safe_float(meta.get("novelty"), _safe_float(derived.get("novelty"), 0.0))
        importance = _safe_float(derived.get("importance_floor"), 0.0)
        reliability = _safe_float(derived.get("source_reliability"), 0.0)
        freshness = _freshness_multiplier(now_ms - int(ts_ms or now_ms), 90 * 60)
        boost = freshness * (
            _event_type_weight(str(event_type or ""))
            + 0.10 * _clamp(novelty)
            + 0.06 * _clamp(importance)
            + 0.04 * _clamp(reliability)
        )
        if boost <= 0.04:
            continue

        text = f"{title or ''}\n{body or ''}"
        symbols = [str(symbol).upper().strip()] if str(symbol or "").strip() else extract_symbol_candidates(text)
        for sym in symbols[:4]:
            upsert_symbol(
                con,
                sym,
                status="WATCH",
                score_delta=float(boost),
                last_seen_event_ts_ms=int(ts_ms or now_ms),
                meta={
                    "source_event_score": float(boost),
                    "source_event_type": str(event_type or ""),
                    "last_event_id": int(event_id),
                },
            )
            upserts += 1
    return int(upserts)


def _disable_stale_symbols(con, now_ms: int) -> int:
    try:
        rows = con.execute(
            """
            SELECT symbol, created_ts_ms, updated_ts_ms, last_seen_event_ts_ms
            FROM symbols
            """
        ).fetchall() or []
    except Exception as e:
        _warn_nonfatal("UPDATE_UNIVERSE_SYMBOL_ROWS_FAILED", e, once_key="disable_stale_symbols")
        return 0

    disabled = 0
    for symbol, created_ts_ms, updated_ts_ms, last_seen_event_ts_ms in rows:
        sym = str(symbol or "").upper().strip()
        if not sym:
            continue
        if sym in BASELINE_SYMBOL_SET:
            continue
        try:
            quote = con.execute(
                """
                SELECT ts_ms
                FROM price_quotes
                WHERE symbol=?
                ORDER BY ts_ms DESC
                LIMIT 1
                """,
                (sym,),
            ).fetchone()
        except Exception as e:
            _warn_nonfatal(
                "UPDATE_UNIVERSE_LAST_QUOTE_LOOKUP_FAILED",
                e,
                once_key=f"last_quote_lookup:{sym}",
                symbol=str(sym),
            )
            quote = None
        try:
            price = con.execute(
                """
                SELECT ts_ms
                FROM prices
                WHERE symbol=?
                ORDER BY ts_ms DESC
                LIMIT 1
                """,
                (sym,),
            ).fetchone()
        except Exception as e:
            _warn_nonfatal(
                "UPDATE_UNIVERSE_LAST_PRICE_LOOKUP_FAILED",
                e,
                once_key=f"last_price_lookup:{sym}",
                symbol=str(sym),
            )
            price = None
        try:
            event = con.execute(
                """
                SELECT ts_ms
                FROM events
                WHERE symbol=?
                ORDER BY ts_ms DESC
                LIMIT 1
                """,
                (sym,),
            ).fetchone()
        except Exception as e:
            _warn_nonfatal(
                "UPDATE_UNIVERSE_LAST_EVENT_LOOKUP_FAILED",
                e,
                once_key=f"last_event_lookup:{sym}",
                symbol=str(sym),
            )
            event = None
        last_quote_ts_ms = int(quote[0]) if quote and quote[0] is not None else 0
        last_price_ts_ms = int(price[0]) if price and price[0] is not None else 0
        last_event_ts_ms = int(event[0]) if event and event[0] is not None else 0
        last_market_ts_ms = max(last_quote_ts_ms, last_price_ts_ms)
        recent_candidate_ts_ms = max(
            int(created_ts_ms or 0),
            int(last_seen_event_ts_ms or 0),
            int(last_event_ts_ms or 0),
        )
        if recent_candidate_ts_ms > 0 and (now_ms - recent_candidate_ts_ms) <= int(UNIVERSE_NEW_SYMBOL_GRACE_MS):
            continue
        if last_market_ts_ms <= 0 or (now_ms - last_market_ts_ms) > UNIVERSE_PRICE_MAX_AGE_MS:
            con.execute(
                "UPDATE symbols SET status='DISABLED', updated_ts_ms=? WHERE symbol=?",
                (int(now_ms), sym),
            )
            disabled += 1
    return int(disabled)


def _recent_market_symbols(con, now_ms: int) -> Dict[str, int]:
    out: Dict[str, int] = {}
    cutoff_ms = int(now_ms - int(UNIVERSE_PRICE_MAX_AGE_MS))
    for table_name in ("price_quotes", "prices"):
        try:
            rows = con.execute(
                f"""
                SELECT symbol, MAX(ts_ms) AS max_ts_ms
                FROM {table_name}
                WHERE ts_ms >= ?
                GROUP BY symbol
                """,
                (int(cutoff_ms),),
            ).fetchall() or []
        except Exception as e:
            _warn_nonfatal(
                "UPDATE_UNIVERSE_RECENT_MARKET_SYMBOLS_FAILED",
                e,
                once_key=f"recent_market_symbols:{table_name}",
                table_name=str(table_name),
            )
            rows = []
        for symbol, ts_ms in rows:
            sym = str(symbol or "").upper().strip()
            if not sym:
                continue
            out[sym] = max(int(out.get(sym) or 0), int(ts_ms or 0))
    return out


def _persist_symbol_universe(con, now_ms: int) -> Tuple[int, int, int]:
    try:
        symbol_rows = con.execute(
            """
            SELECT symbol, status, score, created_ts_ms, updated_ts_ms, last_seen_event_ts_ms, meta_json
            FROM symbols
            ORDER BY score DESC, updated_ts_ms DESC, symbol ASC
            """
        ).fetchall() or []
    except Exception as e:
        _warn_nonfatal("UPDATE_UNIVERSE_SYMBOL_UNIVERSE_LOAD_FAILED", e, once_key="persist_symbol_universe")
        return 0, 0, 0

    existing_rows = con.execute(
        """
        SELECT symbol, status, first_seen_ms, last_seen_ms, last_promoted_ms, last_demoted_ms, seen_n, meta_json
        FROM symbol_universe
        """
    ).fetchall() or []
    existing = {
        str(symbol or "").upper().strip(): {
            "status": str(status or "").upper().strip(),
            "first_seen_ms": int(first_seen_ms or 0),
            "last_seen_ms": int(last_seen_ms or 0),
            "last_promoted_ms": int(last_promoted_ms or 0),
            "last_demoted_ms": int(last_demoted_ms or 0),
            "seen_n": int(seen_n or 0),
            "meta_json": str(meta_json or ""),
        }
        for symbol, status, first_seen_ms, last_seen_ms, last_promoted_ms, last_demoted_ms, seen_n, meta_json in existing_rows
        if str(symbol or "").strip()
    }

    promoted = 0
    watchlisted = 0
    blocked = 0

    for symbol, status, score, created_ts_ms, updated_ts_ms, last_seen_event_ts_ms, meta_json in symbol_rows:
        sym = str(symbol or "").upper().strip()
        if not sym:
            continue
        sym_status = str(status or "WATCH").upper().strip()
        before = dict(existing.get(sym) or {})
        before_status = str(before.get("status") or "").upper().strip()
        universe_status = "BLOCKED"
        include = False
        if sym_status == "ACTIVE":
            universe_status = "ACTIVE"
            include = True
            promoted += 1
        elif sym_status == "WATCH":
            universe_status = "WATCH"
            include = True
            watchlisted += 1
        else:
            blocked += 1

        feature_meta = _safe_load_meta(str(meta_json or ""))
        reasons = sorted([str(k) for k in feature_meta.keys() if str(k).startswith("source_")])
        merged_meta = {
            "symbol_status": sym_status,
            "score": float(_safe_float(score)),
            "last_seen_event_ts_ms": int(last_seen_event_ts_ms or 0),
        }
        merged_meta.update(feature_meta)
        first_seen_ms = int(before.get("first_seen_ms") or created_ts_ms or updated_ts_ms or now_ms)
        last_seen_ms = int(now_ms if include else max(int(before.get("last_seen_ms") or 0), int(last_seen_event_ts_ms or 0), int(updated_ts_ms or 0), int(created_ts_ms or 0)))
        last_promoted_ms = int(before.get("last_promoted_ms") or 0)
        last_demoted_ms = int(before.get("last_demoted_ms") or 0)
        if universe_status == "ACTIVE" and before_status != "ACTIVE":
            last_promoted_ms = int(now_ms)
        if before_status == "ACTIVE" and universe_status != "ACTIVE":
            last_demoted_ms = int(now_ms)
        if before_status in ("WATCH", "ACTIVE") and universe_status == "BLOCKED":
            last_demoted_ms = int(now_ms)

        con.execute(
            """
            INSERT INTO symbol_universe(
              symbol, status, first_seen_ms, last_seen_ms, last_promoted_ms, last_demoted_ms, seen_n, meta_json
            )
            VALUES (?,?,?,?,?,?,?,?)
            ON CONFLICT(symbol) DO UPDATE SET
              status=excluded.status,
              last_seen_ms=excluded.last_seen_ms,
              last_promoted_ms=excluded.last_promoted_ms,
              last_demoted_ms=excluded.last_demoted_ms,
              seen_n=CASE WHEN excluded.status IN ('WATCH','ACTIVE') THEN symbol_universe.seen_n + 1 ELSE symbol_universe.seen_n END,
              meta_json=excluded.meta_json
            """,
            (
                sym,
                universe_status,
                int(first_seen_ms),
                int(last_seen_ms),
                int(last_promoted_ms) if last_promoted_ms > 0 else None,
                int(last_demoted_ms) if last_demoted_ms > 0 else None,
                max(1, int(before.get("seen_n") or 0) or 1),
                json.dumps(merged_meta, separators=(",", ":"), sort_keys=True),
            ),
        )
        con.execute(
            """
            INSERT OR REPLACE INTO universe_audit(
              ts_ms, symbol, status_before, status_after, include, score, reasons_json, features_json
            )
            VALUES (?,?,?,?,?,?,?,?)
            """,
            (
                int(now_ms),
                sym,
                before_status or sym_status,
                universe_status,
                1 if include else 0,
                float(_safe_float(score)),
                json.dumps(reasons, separators=(",", ":"), sort_keys=True),
                json.dumps(
                    {
                        "score": float(_safe_float(score)),
                        "symbol_status": sym_status,
                        "last_seen_event_ts_ms": int(last_seen_event_ts_ms or 0),
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ),
            ),
        )

    return int(promoted), int(watchlisted), int(blocked)


def _quality_limited_symbols(rows: Iterable[Tuple[str, float]], max_keep: int | None) -> List[str]:
    ordered = [(str(sym or "").upper().strip(), _safe_float(score)) for sym, score in rows or []]
    ordered = [(sym, score) for sym, score in ordered if sym]
    if not ordered:
        return []

    top_score = float(ordered[0][1])
    keep: List[str] = []
    prev_score = None
    hard_cap = int(max_keep) if max_keep is not None and int(max_keep) > 0 else len(ordered)
    min_keep = min(int(hard_cap), max(0, int(UNIVERSE_QUAL_MIN_KEEP)))

    for idx, (sym, score) in enumerate(ordered[: int(hard_cap)]):
        if idx >= min_keep:
            if score < float(UNIVERSE_SCORE_ABS_FLOOR):
                break
            if top_score > 0.0 and score < (top_score * float(UNIVERSE_SCORE_REL_FLOOR)):
                break
            if prev_score is not None and prev_score > 0.0:
                drop = (prev_score - score) / max(1e-9, abs(prev_score))
                if drop > float(UNIVERSE_SCORE_GAP_STOP):
                    break
        keep.append(sym)
        prev_score = float(score)
    return keep


def _assign_statuses(con) -> Tuple[int, int]:
    now_ms = _now_ms()
    recent_market = _recent_market_symbols(con, now_ms)
    row_limit = None
    if UNIVERSE_ACTIVE_N is not None or UNIVERSE_WATCH_N is not None:
        row_limit = max(int(UNIVERSE_ACTIVE_N or 0), int(UNIVERSE_WATCH_N or 0))

    sql = """
        SELECT symbol, score, asset_class, meta_json
        FROM symbols
        WHERE status != 'DISABLED'
        ORDER BY score DESC, updated_ts_ms DESC, symbol ASC
    """
    params: Tuple[object, ...] = ()
    if row_limit is not None and int(row_limit) > 0:
        sql += "\n        LIMIT ?"
        params = (int(row_limit),)
    rows = con.execute(sql, params).fetchall() or []

    watch_rows = [(symbol, score) for symbol, score, _asset_class, _meta_json in rows]
    watch_ranked = _quality_limited_symbols(watch_rows, UNIVERSE_WATCH_N)
    if CORE_SYMBOLS:
        merged_watch: List[str] = []
        seen_watch = set()
        for sym in CORE_SYMBOLS + watch_ranked:
            norm = str(sym or "").upper().strip()
            if not norm or norm in seen_watch:
                continue
            seen_watch.add(norm)
            merged_watch.append(norm)
        if UNIVERSE_WATCH_N is not None and int(UNIVERSE_WATCH_N) > 0:
            watch_ranked = merged_watch[: int(max(int(UNIVERSE_WATCH_N), len(CORE_SYMBOLS)))]
        else:
            watch_ranked = merged_watch
    active_ranked: List[str] = []
    row_map = {
        str(symbol or "").upper().strip(): {
            "score": float(_safe_float(score)),
            "asset_class": str(asset_class or "").upper().strip(),
            "meta": _safe_load_meta(str(meta_json or "")),
        }
        for symbol, score, asset_class, meta_json in rows
        if str(symbol or "").strip()
    }
    for sym in watch_ranked:
        if UNIVERSE_ACTIVE_N is not None and len(active_ranked) >= int(UNIVERSE_ACTIVE_N):
            break
        if sym in CORE_SYMBOL_SET:
            active_ranked.append(sym)
            continue
        if not UNIVERSE_ACTIVE_PRICE_REQUIRED:
            active_ranked.append(sym)
            continue
        if sym in recent_market:
            active_ranked.append(sym)
            continue
        info = row_map.get(sym) or {}
        meta_obj = info.get("meta") if isinstance(info, dict) else None
        meta = dict(meta_obj) if isinstance(meta_obj, dict) else {}
        news_count = int(meta.get("source_news_event_count") or 0)
        news_clusters = int(meta.get("source_news_clusters") or 0)
        social_authors = int(meta.get("source_social_authors") or 0)
        social_mentions = int(meta.get("source_social_mentions") or 0)
        if news_count >= 3 and news_clusters >= 2:
            continue
        if social_authors >= max(12, UNIVERSE_SOCIAL_MIN_AUTHORS) and social_mentions >= 15:
            continue
        continue
    active_set = set(active_ranked)
    watch_set = set(watch_ranked)

    if watch_ranked:
        con.execute(
            f"UPDATE symbols SET status='COOLDOWN', updated_ts_ms=? WHERE status != 'DISABLED' AND symbol NOT IN ({','.join('?' for _ in watch_ranked)})",
            (_now_ms(), *watch_ranked),
        )
    else:
        con.execute(
            "UPDATE symbols SET status='COOLDOWN', updated_ts_ms=? WHERE status != 'DISABLED'",
            (_now_ms(),),
        )

    if active_set:
        con.execute(
            f"UPDATE symbols SET status='ACTIVE', updated_ts_ms=? WHERE symbol IN ({','.join('?' for _ in active_ranked)})",
            (_now_ms(), *active_ranked),
        )

    watch_only = [sym for sym in watch_ranked if sym not in active_set]
    if watch_only:
        con.execute(
            f"UPDATE symbols SET status='WATCH', updated_ts_ms=? WHERE symbol IN ({','.join('?' for _ in watch_only)})",
            (_now_ms(), *watch_only),
        )

    return int(len(active_set)), int(len(watch_set))


def main() -> None:
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("update_universe must be launched by supervisor")
        raise SystemExit(1)

    init_db()

    if not acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
        logging.error("another instance is holding the job lock; exiting")
        raise SystemExit(2)

    started_ms = _now_ms()
    con = connect()
    try:
        _ensure_sec_filings_schema(con)
        _ensure_universe_audit_schema(con)
        now_ms = _now_ms()
        _seed_baseline(con)
        _decay_scores(con)

        source_counts = {
            "news": _apply_news_candidates(con, now_ms),
            "social": _apply_social_candidates(con, now_ms),
            "filings": _apply_filings_candidates(con, now_ms),
            "options": _apply_options_candidates(con, now_ms),
            "events": _apply_event_corroboration(con, now_ms),
        }

        disabled = _disable_stale_symbols(con, now_ms)
        active_count, watch_count = _assign_statuses(con)
        universe_active, universe_watch, universe_blocked = _persist_symbol_universe(con, now_ms)

        con.commit()
        dur_ms = _now_ms() - started_ms
        logging.info(
            "universe updated: active=%s watch=%s disabled=%s persisted_active=%s persisted_watch=%s persisted_blocked=%s sources=%s dur_ms=%s",
            active_count,
            watch_count,
            disabled,
            universe_active,
            universe_watch,
            universe_blocked,
            json.dumps(source_counts, separators=(",", ":"), sort_keys=True),
            dur_ms,
        )
    finally:
        try:
            con.close()
        finally:
            try:
                release_job_lock(JOB_NAME, OWNER, PID)
            except Exception as e:
                _warn_nonfatal("UPDATE_UNIVERSE_RELEASE_LOCK_FAILED", e, once_key="release_lock", job_name=JOB_NAME)


if __name__ == "__main__":
    main()
