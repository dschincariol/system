"""
Universe Discovery Engine.
"""

from __future__ import annotations

import json
import math
import os
import time
from typing import Any, Dict, List, Optional, Tuple

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect, init_db
from engine.strategy.model_intent import is_canonical_model_intent
from engine.strategy.model_v2 import get_current_regime

LOG = get_logger("engine.data.universe_discovery")
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
        level=30,
        component="engine.data.universe_discovery",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)

def _now_ms() -> int:
    return int(time.time() * 1000)


def _ensure_schema(con) -> None:
    con.executescript(
        """
CREATE TABLE IF NOT EXISTS universe_audit (
  ts_ms INTEGER NOT NULL,
  symbol TEXT NOT NULL,
  status_before TEXT,
  status_after TEXT,
  include INTEGER NOT NULL,
  score REAL,
  reasons_json TEXT,
  features_json TEXT,
  PRIMARY KEY (ts_ms, symbol)
);

CREATE INDEX IF NOT EXISTS idx_universe_audit_ts
  ON universe_audit(ts_ms);

CREATE INDEX IF NOT EXISTS idx_universe_audit_symbol_ts
  ON universe_audit(symbol, ts_ms);
"""
    )


def _safe_f(value, default=0.0):
    try:
        out = float(value)
    except Exception as e:
        _warn_nonfatal(
            "UNIVERSE_DISCOVERY_SAFE_FLOAT_FAILED",
            e,
            once_key="safe_f",
            value=repr(value),
            default=repr(default),
        )
        return float(default) if default is not None else None
    if not math.isfinite(out):
        return float(default) if default is not None else None
    return float(out)


def _safe_json_obj(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    if not isinstance(raw, str):
        return {}
    try:
        parsed = json.loads(raw or "{}")
    except Exception as e:
        _warn_nonfatal(
            "UNIVERSE_DISCOVERY_JSON_PARSE_FAILED",
            e,
            once_key="safe_json_obj",
            raw=repr(raw)[:512],
        )
        return {}
    return dict(parsed) if isinstance(parsed, dict) else {}


def _clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return float(max(lo, min(hi, float(value))))


def _freshness_multiplier(age_ms: int, half_life_s: int) -> float:
    age_s = max(0.0, float(age_ms) / 1000.0)
    if half_life_s <= 0:
        return 1.0
    return float(0.5 ** (age_s / float(half_life_s)))


def _load_model_universe_candidates(con, ts_ms: int) -> Dict[str, Dict[str, Any]]:
    lookback_s = int(os.environ.get("UNIVERSE_MODEL_INTENT_LOOKBACK_S", "21600"))
    limit_rows = int(os.environ.get("UNIVERSE_MODEL_INTENT_LIMIT", "500"))
    cutoff_ms = int(ts_ms) - max(0, int(lookback_s)) * 1000
    try:
        rows = con.execute(
            """
            SELECT symbol, explain_json, confidence, expected_z, ts_ms
            FROM alerts
            WHERE ts_ms >= ?
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            (int(cutoff_ms), int(limit_rows)),
        ).fetchall() or []
    except Exception as e:
        _warn_nonfatal(
            "UNIVERSE_DISCOVERY_MODEL_CANDIDATES_LOAD_FAILED",
            e,
            once_key="load_model_universe_candidates",
            ts_ms=int(ts_ms),
        )
        return {}

    out: Dict[str, Dict[str, Any]] = {}
    for symbol, explain_json_raw, confidence, expected_z, alert_ts_ms in rows:
        sym = str(symbol or "").upper().strip()
        if not sym:
            continue
        explain = _safe_json_obj(explain_json_raw)
        intent = explain.get("model_intent")
        if not is_canonical_model_intent(intent):
            continue
        if not bool(intent.get("include_in_universe", False)):
            continue
        universe_score = _safe_f(intent.get("universe_score"), None)
        trade_score = _safe_f(intent.get("score"), None)
        score = universe_score if universe_score is not None else trade_score
        if score is None:
            score = abs(_safe_f(intent.get("expected_z"), expected_z)) * max(
                0.0, _safe_f(intent.get("confidence"), confidence)
            )
        feats = {
            "source": "model_intent",
            "alert_ts_ms": int(alert_ts_ms or 0),
            "model_intent": dict(intent),
        }
        cur = out.get(sym)
        if cur is None or float(score) > float(cur.get("score") or 0.0):
            out[sym] = {
                "symbol": sym,
                "status": "WATCH",
                "score": float(score),
                "meta_json": json.dumps(feats, separators=(",", ":"), sort_keys=True),
            }
    return out


def _latest_quote(con, symbol: str) -> Optional[Tuple[int, float, float, float, float]]:
    try:
        row = con.execute(
            """
            SELECT ts_ms, last, bid, ask, volume
            FROM price_quotes
            WHERE symbol=?
            ORDER BY ts_ms DESC
            LIMIT 1
            """,
            (str(symbol),),
        ).fetchone()
        if not row:
            return None
        return int(row[0] or 0), _safe_f(row[1]), _safe_f(row[2]), _safe_f(row[3]), _safe_f(row[4])
    except Exception as e:
        _warn_nonfatal(
            "UNIVERSE_DISCOVERY_LATEST_QUOTE_FAILED",
            e,
            once_key=f"latest_quote:{symbol}",
            symbol=str(symbol),
        )
        return None


def _bar_tf_s(con, symbol: str) -> Optional[int]:
    try:
        row = con.execute(
            """
            SELECT tf_s
            FROM price_bars
            WHERE symbol=?
            GROUP BY tf_s
            ORDER BY tf_s ASC, COUNT(*) DESC
            LIMIT 1
            """,
            (str(symbol),),
        ).fetchone()
        if not row:
            return None
        return int(row[0])
    except Exception as e:
        _warn_nonfatal(
            "UNIVERSE_DISCOVERY_BAR_TF_LOOKUP_FAILED",
            e,
            once_key=f"bar_tf_s:{symbol}",
            symbol=str(symbol),
        )
        return None


def _returns_std_from_bars(con, symbol: str, lookback: int) -> Optional[float]:
    tf_s = _bar_tf_s(con, symbol)
    if tf_s is None:
        return None
    try:
        rows = con.execute(
            """
            SELECT c
            FROM price_bars
            WHERE symbol=?
              AND tf_s=?
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            (str(symbol), int(tf_s), int(lookback)),
        ).fetchall()
        if not rows or len(rows) < 5:
            return None
        closes = [float(r[0]) for r in rows if r and r[0] is not None]
        if len(closes) < 5:
            return None
        closes = list(reversed(closes))
        rets = []
        for idx in range(1, len(closes)):
            a = float(closes[idx - 1])
            b = float(closes[idx])
            if a > 0 and b > 0:
                rets.append(math.log(b / a))
        if len(rets) < 4:
            return None
        mu = sum(rets) / float(len(rets))
        var = sum((x - mu) ** 2 for x in rets) / float(max(1, len(rets) - 1))
        return math.sqrt(max(0.0, var))
    except Exception as e:
        _warn_nonfatal(
            "UNIVERSE_DISCOVERY_RETURNS_STD_FAILED",
            e,
            once_key=f"returns_std:{symbol}",
            symbol=str(symbol),
            lookback=int(lookback),
        )
        return None


def _avg_dollar_volume_from_bars(con, symbol: str, lookback: int = 20) -> Optional[float]:
    tf_s = _bar_tf_s(con, symbol)
    if tf_s is None:
        return None
    try:
        rows = con.execute(
            """
            SELECT c, v
            FROM price_bars
            WHERE symbol=?
              AND tf_s=?
              AND v IS NOT NULL
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            (str(symbol), int(tf_s), int(max(5, lookback))),
        ).fetchall()
    except Exception as e:
        _warn_nonfatal(
            "UNIVERSE_DISCOVERY_DOLLAR_VOLUME_QUERY_FAILED",
            e,
            once_key=f"dollar_volume:{symbol}",
            symbol=str(symbol),
            lookback=int(lookback),
        )
        return None
    vals = []
    for close, volume in rows or []:
        c = _safe_f(close, None)
        v = _safe_f(volume, None)
        if c is None or v is None or c <= 0.0 or v <= 0.0:
            continue
        vals.append(float(c) * float(v))
    if not vals:
        return None
    return float(sum(vals) / float(len(vals)))


def _corr(con, a: str, b: str, lookback: int) -> Optional[float]:
    try:
        ra = con.execute(
            """
            SELECT ts_ms, c
            FROM price_bars
            WHERE symbol=?
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            (str(a), int(lookback)),
        ).fetchall()
        rb = con.execute(
            """
            SELECT ts_ms, c
            FROM price_bars
            WHERE symbol=?
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            (str(b), int(lookback)),
        ).fetchall()
        if not ra or not rb:
            return None
        ma = {int(ts): _safe_f(c, None) for (ts, c) in ra if ts is not None and c is not None}
        mb = {int(ts): _safe_f(c, None) for (ts, c) in rb if ts is not None and c is not None}
        ts_common = sorted(set(ma.keys()) & set(mb.keys()))
        if len(ts_common) < 8:
            return None
        retsa = []
        retsb = []
        for idx in range(1, len(ts_common)):
            pa0, pa1 = ma[ts_common[idx - 1]], ma[ts_common[idx]]
            pb0, pb1 = mb[ts_common[idx - 1]], mb[ts_common[idx]]
            if pa0 > 0 and pa1 > 0 and pb0 > 0 and pb1 > 0:
                retsa.append(math.log(pa1 / pa0))
                retsb.append(math.log(pb1 / pb0))
        n = min(len(retsa), len(retsb))
        if n < 6:
            return None
        retsa = retsa[-n:]
        retsb = retsb[-n:]
        ma_ = sum(retsa) / n
        mb_ = sum(retsb) / n
        cov = sum((retsa[i] - ma_) * (retsb[i] - mb_) for i in range(n))
        va = sum((retsa[i] - ma_) ** 2 for i in range(n))
        vb = sum((retsb[i] - mb_) ** 2 for i in range(n))
        den = math.sqrt(max(1e-12, va * vb))
        return float(cov / den)
    except Exception as e:
        _warn_nonfatal(
            "UNIVERSE_DISCOVERY_CORRELATION_FAILED",
            e,
            once_key=f"corr:{a}:{b}",
            symbol_a=str(a),
            symbol_b=str(b),
            lookback=int(lookback),
        )
        return None


def _bounded_log_score(value: float, floor: float, ceiling: float) -> float:
    base = max(1.0, float(floor))
    cap = max(base + 1.0, float(ceiling))
    val = max(0.0, float(value))
    num = math.log1p(val) - math.log1p(base)
    den = math.log1p(cap) - math.log1p(base)
    if den <= 1e-9:
        return 0.0
    return _clamp(num / den)


def _filing_form_weight(form: str) -> float:
    text = str(form or "").upper().strip()
    if text in {"8-K", "10-Q", "10-K", "6-K", "S-1", "F-1", "13D", "SC 13D"}:
        return 1.0
    if text in {"13G", "SC 13G", "424B2", "424B3", "425", "DEF 14A"}:
        return 0.8
    if text in {"3", "4", "5", "144"}:
        return 0.55
    return 0.45


def _symbol_event_signal_features(con, symbol: str, ts_ms: int) -> Dict[str, Any]:
    now_ms = int(ts_ms)
    news_cutoff_ms = int(now_ms - int(os.environ.get("UNIVERSE_NEWS_LOOKBACK_S", "21600")) * 1000)
    social_cutoff_ms = int(now_ms - int(os.environ.get("UNIVERSE_SOCIAL_LOOKBACK_S", "21600")) * 1000)
    filings_cutoff_ms = int(now_ms - int(os.environ.get("UNIVERSE_FILINGS_LOOKBACK_S", str(2 * 86400))) * 1000)
    options_cutoff_ms = int(now_ms - int(os.environ.get("UNIVERSE_OPTIONS_LOOKBACK_S", "21600")) * 1000)

    out: Dict[str, Any] = {
        "news_signal": 0.0,
        "social_signal": 0.0,
        "filings_signal": 0.0,
        "options_signal": 0.0,
        "news_event_density": 0.0,
        "social_mention_z": 0.0,
        "filing_count": 0,
        "options_signal_raw": 0.0,
        "source_count": 0,
        "event_density_score": 0.0,
    }

    try:
        row = con.execute(
            """
            SELECT bucket_ts_ms, bucket_sec, news_velocity, event_density, event_count,
                   distinct_cluster_count, avg_novelty, duplicate_share
            FROM news_symbol_features
            WHERE symbol=?
            ORDER BY bucket_ts_ms DESC, bucket_sec ASC
            LIMIT 1
            """,
            (str(symbol),),
        ).fetchone()
    except Exception:
        row = None
    if row and int(row[0] or 0) >= news_cutoff_ms:
        freshness = _freshness_multiplier(now_ms - int(row[0] or now_ms), 3 * 3600)
        density_score = _clamp(_safe_f(row[3]) / 0.25)
        velocity_score = _clamp(_safe_f(row[2]) / 4.0)
        cluster_score = _clamp(_safe_f(row[5]) / 4.0)
        novelty_score = _clamp(_safe_f(row[6]))
        duplicate_pen = _clamp(_safe_f(row[7]))
        news_signal = max(
            0.0,
            freshness
            * (
                0.34 * density_score
                + 0.24 * velocity_score
                + 0.18 * novelty_score
                + 0.14 * cluster_score
                + 0.10 * _clamp(1.0 - duplicate_pen)
            ),
        )
        out["news_signal"] = float(news_signal)
        out["news_event_density"] = float(_safe_f(row[3]))
        if news_signal > 0.05:
            out["source_count"] += 1

    try:
        row = con.execute(
            """
            SELECT bucket_ts_ms, bucket_sec, mention_rate_z, unique_authors,
                   attention_shock, cross_platform_confirm, manip_risk
            FROM social_features
            WHERE symbol=?
            ORDER BY bucket_ts_ms DESC, bucket_sec ASC
            LIMIT 1
            """,
            (str(symbol),),
        ).fetchone()
    except Exception:
        row = None
    if row and int(row[0] or 0) >= social_cutoff_ms:
        freshness = _freshness_multiplier(now_ms - int(row[0] or now_ms), 2 * 3600)
        z_score = _clamp(_safe_f(row[2]) / 4.0)
        author_score = _clamp(_safe_f(row[3]) / 40.0)
        shock_score = _clamp(_safe_f(row[4]))
        confirm_score = _clamp(_safe_f(row[5]))
        manip_risk = _clamp(_safe_f(row[6]))
        social_signal = max(
            0.0,
            freshness
            * (
                0.34 * z_score
                + 0.22 * author_score
                + 0.20 * shock_score
                + 0.14 * confirm_score
                + 0.10 * _clamp(1.0 - manip_risk)
            ),
        )
        out["social_signal"] = float(social_signal)
        out["social_mention_z"] = float(_safe_f(row[2]))
        if social_signal > 0.05:
            out["source_count"] += 1

    try:
        rows = con.execute(
            """
            SELECT form, ts_ms
            FROM sec_filings
            WHERE symbol=?
              AND ts_ms >= ?
            ORDER BY ts_ms DESC
            LIMIT 8
            """,
            (str(symbol), int(filings_cutoff_ms)),
        ).fetchall() or []
    except Exception:
        rows = []
    if rows:
        filing_weight = 0.0
        newest_ts_ms = 0
        for form, filing_ts_ms in rows:
            filing_weight += _filing_form_weight(str(form or "")) * _freshness_multiplier(
                now_ms - int(filing_ts_ms or now_ms),
                24 * 3600,
            )
            newest_ts_ms = max(newest_ts_ms, int(filing_ts_ms or 0))
        out["filings_signal"] = float(_clamp(filing_weight / 2.0))
        out["filing_count"] = int(len(rows))
        out["filings_age_ms"] = int(max(0, now_ms - newest_ts_ms)) if newest_ts_ms > 0 else None
        if out["filings_signal"] > 0.05:
            out["source_count"] += 1

    try:
        row = con.execute(
            """
            SELECT bucket_ts_ms, bucket_sec, signal_score, unusual_volume_score,
                   unusual_volume_contracts, call_put_volume_ratio
            FROM options_symbol_features
            WHERE symbol=?
            ORDER BY bucket_ts_ms DESC, bucket_sec ASC
            LIMIT 1
            """,
            (str(symbol),),
        ).fetchone()
    except Exception:
        row = None
    if row and int(row[0] or 0) >= options_cutoff_ms:
        freshness = _freshness_multiplier(now_ms - int(row[0] or now_ms), 3 * 3600)
        signal_score = abs(_safe_f(row[2]))
        unusual_score = _safe_f(row[3])
        contract_score = _safe_f(row[4])
        ratio = max(1e-6, _safe_f(row[5], 1.0))
        flow_score = _clamp(abs(math.log(ratio)) / 1.25)
        options_signal = max(
            0.0,
            freshness
            * (
                0.40 * _clamp(signal_score / 0.75)
                + 0.32 * _clamp(math.log1p(max(0.0, unusual_score)) / math.log(5.0))
                + 0.16 * _clamp(contract_score / 6.0)
                + 0.12 * flow_score
            ),
        )
        out["options_signal"] = float(options_signal)
        out["options_signal_raw"] = float(signal_score)
        if options_signal > 0.05:
            out["source_count"] += 1

    out["event_density_score"] = float(
        0.44 * out["news_signal"]
        + 0.22 * out["social_signal"]
        + 0.18 * out["filings_signal"]
        + 0.16 * out["options_signal"]
    )
    return out


def _quality_limited_pool(
    pool: List[Dict[str, Any]],
    *,
    total_cap: int,
    min_keep: int,
    rel_floor: float,
    abs_floor: float,
    gap_stop: float,
) -> List[Dict[str, Any]]:
    if not pool or total_cap <= 0:
        return []
    top_score = float(pool[0].get("score") or 0.0)
    keep: List[Dict[str, Any]] = []
    prev_score = None
    min_keep = min(int(total_cap), max(0, int(min_keep)))

    for idx, item in enumerate(pool[: int(total_cap)]):
        score = float(item.get("score") or 0.0)
        if idx >= min_keep:
            if score < float(abs_floor):
                break
            if top_score > 0.0 and score < (top_score * float(rel_floor)):
                break
            if prev_score is not None and prev_score > 0.0:
                drop = (prev_score - score) / max(1e-9, abs(prev_score))
                if drop > float(gap_stop):
                    break
        keep.append(item)
        prev_score = score
    return keep


def discover_universe_once(
    con=None,
    ts_ms: Optional[int] = None,
) -> Dict[str, Any]:
    owns = False
    if con is None:
        con = connect()
        owns = True
    try:
        init_db()
        _ensure_schema(con)

        ts_ms = int(ts_ms or _now_ms())
        now_s = ts_ms / 1000.0

        cand_max = int(os.environ.get("UNIVERSE_CANDIDATE_MAX", "500"))
        active_target = int(os.environ.get("UNIVERSE_ACTIVE_TARGET", "80"))
        watch_target = int(os.environ.get("UNIVERSE_WATCH_TARGET", "140"))

        quote_max_age_s = float(os.environ.get("UNIVERSE_QUOTE_MAX_AGE_S", "120"))
        min_dollar_vol = float(os.environ.get("UNIVERSE_MIN_DOLLAR_VOL", "1000000"))
        liquidity_ideal = float(os.environ.get("UNIVERSE_LIQUIDITY_IDEAL", str(max(5_000_000, int(min_dollar_vol * 20)))))
        spread_bps_max = float(os.environ.get("UNIVERSE_SPREAD_BPS_MAX", "40"))
        vol_floor = float(os.environ.get("UNIVERSE_VOL_FLOOR", "0.004"))
        vol_target = float(os.environ.get("UNIVERSE_VOL_TARGET", "0.020"))
        vol_max = float(os.environ.get("UNIVERSE_VOL_MAX", "0.08"))
        min_signal_score = float(os.environ.get("UNIVERSE_MIN_SIGNAL_SCORE", "0.12"))
        base_signal_floor = float(os.environ.get("UNIVERSE_BASE_SIGNAL_FLOOR", "1.00"))

        dynamic_min_keep = int(os.environ.get("UNIVERSE_DYNAMIC_MIN_KEEP", str(max(12, active_target // 2))))
        dynamic_rel_floor = float(os.environ.get("UNIVERSE_SCORE_REL_FLOOR", "0.58"))
        dynamic_abs_floor = float(os.environ.get("UNIVERSE_SCORE_ABS_FLOOR", "1.20"))
        dynamic_gap_stop = float(os.environ.get("UNIVERSE_SCORE_GAP_STOP", "0.35"))

        corr_lookback = int(os.environ.get("UNIVERSE_CORR_LOOKBACK_BARS", "96"))
        corr_th = float(os.environ.get("UNIVERSE_CORR_TH", "0.85"))
        cluster_max = int(os.environ.get("UNIVERSE_CLUSTER_MAX", "200"))
        corr_max_pairs = int(os.environ.get("UNIVERSE_CORR_MAX_PAIRS", "5000"))
        corr_time_budget_ms = int(os.environ.get("UNIVERSE_CORR_TIME_BUDGET_MS", "1200"))

        regime = "MID"
        try:
            regime = str(get_current_regime("SPY") or "MID").upper()
        except Exception:
            regime = "MID"

        rows = con.execute(
            """
            SELECT symbol, status, score, COALESCE(meta_json, '')
            FROM symbols
            WHERE status != 'DISABLED'
            ORDER BY score DESC, symbol ASC
            LIMIT ?
            """,
            (int(cand_max),),
        ).fetchall() or []

        row_map: Dict[str, Tuple[str, str, float, str]] = {}
        for sym, status, base_score, meta_json in rows:
            key = str(sym or "").upper().strip()
            if not key:
                continue
            row_map[key] = (key, str(status or "WATCH"), _safe_f(base_score, 0.0), str(meta_json or ""))

        for sym, item in _load_model_universe_candidates(con, ts_ms).items():
            status = "WATCH"
            base_score = float(item.get("score") or 0.0)
            meta_json = str(item.get("meta_json") or "")
            if sym in row_map:
                _, old_status, old_score, old_meta = row_map[sym]
                status = old_status
                base_score = max(float(old_score), float(base_score))
                merged_meta = _safe_json_obj(old_meta)
                merged_meta["model_intent"] = _safe_json_obj(meta_json).get("model_intent") or merged_meta.get("model_intent")
                merged_meta["model_source_score"] = float(item.get("score") or 0.0)
                meta_json = json.dumps(merged_meta, separators=(",", ":"), sort_keys=True)
            row_map[sym] = (sym, status, base_score, meta_json)

        rows = sorted(
            list(row_map.values()),
            key=lambda row: (-float(row[2] or 0.0), str(row[0])),
        )[: int(cand_max)]

        scored: List[Dict[str, Any]] = []
        for sym, status, base_score, meta_json in rows:
            base = _safe_f(base_score, 0.0)
            reasons: List[str] = []
            feats: Dict[str, Any] = {"base_score": base, "regime": regime}
            meta = _safe_json_obj(meta_json)
            canonical_intent = meta.get("model_intent")
            if is_canonical_model_intent(canonical_intent):
                feats["model_intent"] = dict(canonical_intent)
                feats["model_source_score"] = _safe_f(meta.get("model_source_score"), 0.0)

            quote = _latest_quote(con, sym)
            if not quote:
                reasons.append("no_quote")
                scored.append(
                    {
                        "symbol": str(sym),
                        "status_before": str(status or "WATCH"),
                        "include": False,
                        "score": None,
                        "reasons": reasons,
                        "features": feats,
                    }
                )
                continue

            q_ts, last, bid, ask, volume = quote
            age_s = max(0.0, now_s - (q_ts / 1000.0))
            feats.update(
                {
                    "quote_ts_ms": int(q_ts),
                    "last": float(last),
                    "bid": float(bid),
                    "ask": float(ask),
                    "quote_volume": float(volume),
                    "quote_age_s": float(age_s),
                }
            )
            if age_s > quote_max_age_s:
                reasons.append("stale_quote")

            mid = (bid + ask) / 2.0 if (bid > 0 and ask > 0) else max(0.0, last)
            spread = max(0.0, ask - bid) if (bid > 0 and ask > 0) else 0.0
            spread_bps = (spread / mid) * 10000.0 if mid > 0 else 1e9
            feats.update({"mid": float(mid), "spread": float(spread), "spread_bps": float(spread_bps)})
            if spread_bps > spread_bps_max:
                reasons.append("spread_too_wide")

            quote_dollar_vol = max(0.0, float(last) * float(volume))
            bar_dollar_vol = _avg_dollar_volume_from_bars(con, sym, lookback=20)
            dollar_vol = max(float(quote_dollar_vol), float(bar_dollar_vol or 0.0))
            feats.update(
                {
                    "quote_dollar_vol": float(quote_dollar_vol),
                    "bar_dollar_vol": float(bar_dollar_vol or 0.0),
                    "dollar_vol": float(dollar_vol),
                }
            )
            if dollar_vol < min_dollar_vol:
                reasons.append("illiquid")

            vol_std = _returns_std_from_bars(con, sym, lookback=min(512, max(48, corr_lookback)))
            feats["ret_std"] = vol_std
            vol_signal = 0.0
            vol_excess_pen = 0.0
            if vol_std is None:
                reasons.append("no_bars")
            else:
                if vol_std >= vol_floor:
                    vol_signal = _clamp(vol_std / max(vol_target, 1e-6))
                if vol_std > vol_max:
                    vol_excess_pen = _clamp((vol_std - vol_max) / max(vol_max, 1e-6))
                    if regime in ("HIGH", "CRISIS"):
                        reasons.append("too_volatile_for_regime")
                if vol_std > (vol_max * 1.5):
                    reasons.append("too_volatile")

            event_signal = _symbol_event_signal_features(con, sym, ts_ms)
            feats.update(event_signal)
            if (
                float(event_signal.get("event_density_score") or 0.0) < min_signal_score
                and float(base) < base_signal_floor
                and not is_canonical_model_intent(canonical_intent)
            ):
                reasons.append("insufficient_event_density")

            include = len(reasons) == 0
            liq_score = _bounded_log_score(dollar_vol, min_dollar_vol, liquidity_ideal)
            spread_pen = _clamp(spread_bps / max(spread_bps_max, 1e-9))
            regime_bonus = 0.08 if regime == "LOW" else (-0.08 if regime in ("HIGH", "CRISIS") else 0.0)

            model_bonus = 0.0
            if is_canonical_model_intent(canonical_intent):
                model_bonus = min(1.5, max(0.0, _safe_f(canonical_intent.get("universe_score"), base) * 0.25))
                feats["model_bonus"] = float(model_bonus)

            opportunity_score = (
                float(base)
                + float(model_bonus)
                + 0.95 * float(liq_score)
                + 0.65 * float(vol_signal)
                + 1.20 * float(event_signal.get("event_density_score") or 0.0)
                + 0.18 * float(event_signal.get("source_count") or 0.0)
                - 0.80 * float(spread_pen)
                - 0.65 * float(vol_excess_pen)
                + float(regime_bonus)
            )
            feats.update(
                {
                    "liq_score": float(liq_score),
                    "vol_signal": float(vol_signal),
                    "vol_excess_pen": float(vol_excess_pen),
                    "spread_pen": float(spread_pen),
                    "regime_bonus": float(regime_bonus),
                }
            )
            scored.append(
                {
                    "symbol": str(sym),
                    "status_before": str(status or "WATCH"),
                    "include": bool(include),
                    "score": float(opportunity_score),
                    "reasons": reasons,
                    "features": feats,
                }
            )

        pool = [item for item in scored if item.get("include") and item.get("score") is not None]
        pool.sort(key=lambda item: (-float(item.get("score") or 0.0), str(item.get("symbol"))))

        quality_pool = _quality_limited_pool(
            pool,
            total_cap=int(max(1, watch_target)),
            min_keep=int(dynamic_min_keep),
            rel_floor=float(dynamic_rel_floor),
            abs_floor=float(dynamic_abs_floor),
            gap_stop=float(dynamic_gap_stop),
        )

        selected: List[Dict[str, Any]] = []
        pruned: List[Dict[str, Any]] = []
        considered = list(quality_pool[: int(max(1, cluster_max))])
        corr_pairs_evaluated = 0
        corr_time_exceeded = False
        corr_start_ms = _now_ms()

        for item in considered:
            sym = str(item["symbol"])
            ok = True
            for peer in selected[: max(1, active_target)]:
                if corr_pairs_evaluated >= int(corr_max_pairs):
                    corr_time_exceeded = True
                    break
                if (_now_ms() - corr_start_ms) > int(corr_time_budget_ms):
                    corr_time_exceeded = True
                    break
                corr_pairs_evaluated += 1
                corr = _corr(con, sym, str(peer["symbol"]), lookback=corr_lookback)
                if corr is None:
                    continue
                if abs(float(corr)) >= corr_th:
                    ok = False
                    item["include"] = False
                    item["reasons"] = list(item.get("reasons") or []) + [f"corr_pruned:{peer['symbol']}:{corr:.3f}"]
                    break
            if corr_time_exceeded:
                break
            if ok:
                selected.append(item)
            else:
                pruned.append(item)

        if corr_time_exceeded:
            processed = len(selected) + len(pruned)
            for item in considered[processed:]:
                if len(selected) >= int(watch_target):
                    break
                if item.get("include") and item.get("score") is not None:
                    selected.append(item)

        active = selected[: int(active_target)]
        watch_total = selected[: int(watch_target)]
        watch_only = watch_total[int(active_target):]

        active_syms = [str(item["symbol"]) for item in active]
        watch_total_syms = [str(item["symbol"]) for item in watch_total]
        watch_only_syms = [str(item["symbol"]) for item in watch_only]
        active_set = set(active_syms)
        watch_total_set = set(watch_total_syms)
        watch_only_set = set(watch_only_syms)

        selected_syms_sorted = sorted(watch_total_set)
        if selected_syms_sorted:
            con.execute(
                f"UPDATE symbols SET status='COOLDOWN', updated_ts_ms=? WHERE status != 'DISABLED' AND symbol NOT IN ({','.join('?' for _ in selected_syms_sorted)})",
                (int(ts_ms), *selected_syms_sorted),
            )
        else:
            con.execute(
                "UPDATE symbols SET status='COOLDOWN', updated_ts_ms=? WHERE status != 'DISABLED'",
                (int(ts_ms),),
            )

        if watch_only_syms:
            con.execute(
                f"UPDATE symbols SET status='WATCH', updated_ts_ms=? WHERE status != 'DISABLED' AND symbol IN ({','.join('?' for _ in watch_only_syms)})",
                (int(ts_ms), *watch_only_syms),
            )
        if active_syms:
            con.execute(
                f"UPDATE symbols SET status='ACTIVE', updated_ts_ms=? WHERE status != 'DISABLED' AND symbol IN ({','.join('?' for _ in active_syms)})",
                (int(ts_ms), *active_syms),
            )

        for item in scored:
            sym = str(item["symbol"])
            status_before = str(item.get("status_before") or "")
            include = 1 if bool(item.get("include")) else 0
            score = item.get("score")
            status_after = "COOLDOWN"
            if sym in active_set:
                status_after = "ACTIVE"
            elif sym in watch_only_set:
                status_after = "WATCH"
            elif status_before == "DISABLED":
                status_after = "DISABLED"

            feats = dict(item.get("features") or {})
            feats["corr_budget"] = {
                "corr_pairs_evaluated": int(corr_pairs_evaluated),
                "corr_time_exceeded": bool(corr_time_exceeded),
                "corr_max_pairs": int(corr_max_pairs),
                "corr_time_budget_ms": int(corr_time_budget_ms),
                "corr_th": float(corr_th),
                "corr_lookback": int(corr_lookback),
            }
            con.execute(
                """
                INSERT OR REPLACE INTO universe_audit(
                  ts_ms, symbol, status_before, status_after, include, score, reasons_json, features_json
                )
                VALUES (?,?,?,?,?,?,?,?)
                """,
                (
                    int(ts_ms),
                    sym,
                    status_before,
                    status_after,
                    int(include),
                    None if score is None else float(score),
                    json.dumps(item.get("reasons") or []),
                    json.dumps(feats, separators=(",", ":"), sort_keys=True),
                ),
            )

        return {
            "ok": True,
            "ts_ms": int(ts_ms),
            "regime": regime,
            "n_candidates": int(len(rows)),
            "n_scored": int(len(scored)),
            "n_pool": int(len(pool)),
            "n_quality_pool": int(len(quality_pool)),
            "n_active": int(len(active_set)),
            "n_watch": int(len(watch_only_set)),
            "n_watch_total": int(len(watch_total_set)),
            "corr_pairs_evaluated": int(corr_pairs_evaluated),
            "corr_time_exceeded": bool(corr_time_exceeded),
            "corr_max_pairs": int(corr_max_pairs),
            "corr_time_budget_ms": int(corr_time_budget_ms),
        }
    finally:
        if owns:
            try:
                con.commit()
            except Exception as e:
                _warn_nonfatal(
                    "UNIVERSE_DISCOVERY_COMMIT_FAILED",
                    e,
                    once_key="universe_discovery_commit",
                )
            try:
                con.close()
            except Exception as e:
                _warn_nonfatal(
                    "UNIVERSE_DISCOVERY_CLOSE_FAILED",
                    e,
                    once_key="universe_discovery_close",
                )
