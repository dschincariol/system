"""
FILE: provider_router.py

Data subsystem module for `provider_router`.
"""

"""
Cross-provider routing + anomaly detection + canonical health scoring
"""

import json
import os
import statistics
import time
from typing import Dict, Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.price_router import publish_price_event
from engine.runtime.storage import connect, run_write_txn

ANOMALY_THRESHOLD_BPS = float(os.environ.get("PROVIDER_ANOMALY_BPS", "25"))
STALE_THRESHOLD_MS = int(os.environ.get("PROVIDER_STALE_MS", "120000"))
QUORUM_BPS = float(os.environ.get("PROVIDER_QUORUM_BPS", "8"))
FAILOVER_MIN_SCORE = float(os.environ.get("PROVIDER_FAILOVER_MIN_SCORE", "0.15"))

_PROVIDER_STALE_MS = {
    "polygon_ws": int(os.environ.get("PROVIDER_STALE_MS_POLYGON_WS", "8000")),
    "polygon": int(os.environ.get("PROVIDER_STALE_MS_POLYGON", "30000")),
    "yfinance": int(os.environ.get("PROVIDER_STALE_MS_YFINANCE", "120000")),
    "ccxt": int(os.environ.get("PROVIDER_STALE_MS_CCXT", "120000")),
}

_PROVIDER_PRIORITY = [
    str(x).strip().lower()
    for x in os.environ.get("PROVIDER_PRIORITY", "polygon_ws,polygon,yfinance,ccxt").split(",")
    if str(x).strip()
]

_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        None,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=30,
        component="engine.data.provider_router",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _provider_stale_ms(provider: str) -> int:
    return int(_PROVIDER_STALE_MS.get(str(provider or "").strip().lower(), STALE_THRESHOLD_MS))


def now_ms():
    return int(time.time() * 1000)


def _bps(a: float, b: float) -> float:
    if not a or not b:
        return 0.0
    mid = (a + b) / 2.0
    return abs(a - b) / mid * 10000.0 if mid else 0.0


def _latest_provider_health(con) -> Dict[str, Dict]:
    rows = con.execute(
        """
        SELECT h.provider, h.ts_ms, h.ok, h.latency_ms, h.n_symbols, h.error
        FROM price_provider_health h
        JOIN (
            SELECT provider, MAX(ts_ms) AS max_ts_ms
            FROM price_provider_health
            GROUP BY provider
        ) latest
          ON latest.provider = h.provider
         AND latest.max_ts_ms = h.ts_ms
        """
    ).fetchall()

    out: Dict[str, Dict] = {}
    for provider, ts_ms, ok, latency_ms, n_symbols, error in rows or []:
        out[str(provider)] = {
            "ts_ms": int(ts_ms or 0),
            "ok": int(ok or 0),
            "latency_ms": int(latency_ms or 0),
            "n_symbols": int(n_symbols or 0),
            "error": (str(error) if error else None),
        }
    return out


def _latest_provider_session_meta(con) -> Dict[str, Dict]:
    try:
        rows = con.execute(
            """
            SELECT key, value, updated_ts_ms
            FROM runtime_meta
            WHERE key LIKE 'provider_session_%'
            """
        ).fetchall()
    except Exception as e:
        _warn_nonfatal(
            "PROVIDER_ROUTER_SESSION_META_QUERY_FAILED",
            e,
            once_key="latest_provider_session_meta",
        )
        return {}

    out: Dict[str, Dict] = {}
    for key, value, updated_ts_ms in rows or []:
        provider = str(key or "").replace("provider_session_", "", 1).strip().lower()
        if not provider:
            continue
        try:
            payload = json.loads(value) if value else {}
        except Exception as e:
            _warn_nonfatal(
                "PROVIDER_ROUTER_SESSION_META_PARSE_FAILED",
                e,
                once_key=f"session_meta_parse:{provider}",
                provider=str(provider),
            )
            payload = {}
        payload["meta_updated_ts_ms"] = int(updated_ts_ms or 0)
        out[provider] = payload
    return out


def _latency_score(latency_ms: int, stale_after_ms: int) -> float:
    latency_ms = max(0, int(latency_ms or 0))
    stale_after_ms = max(1, int(stale_after_ms or 1))
    return max(0.0, 1.0 - (float(latency_ms) / float(stale_after_ms)))


def compute_provider_health() -> Dict[str, Dict]:
    ts = now_ms()
    con = connect(readonly=True)
    try:
        # Provider health merges DB-side heartbeat rows with session-manager
        # runtime metadata so routing can reason about both polling and streams.
        latest = _latest_provider_health(con)
        session_meta = _latest_provider_session_meta(con)
        out: Dict[str, Dict] = {}

        providers = sorted(set(latest.keys()) | set(session_meta.keys()))
        for provider in providers:
            rec = dict(latest.get(provider) or {})
            sess = dict(session_meta.get(provider) or {})

            stale_ms = _provider_stale_ms(provider)
            health_ts_ms = int(rec.get("ts_ms") or 0)
            meta_ts_ms = int(sess.get("meta_updated_ts_ms") or 0)
            age_ms = max(0, ts - max(health_ts_ms, meta_ts_ms, 0))

            session_connected = bool(sess.get("connected")) if sess else False
            session_age_ms = int(sess.get("last_msg_age_ms") or 10**9) if sess else 10**9
            db_ok = bool(int(rec.get("ok") or 0) == 1 and age_ms <= stale_ms)
            session_ok = bool(session_connected and session_age_ms <= stale_ms)
            ok = bool(db_ok or session_ok)

            freshness_ms = min(
                int(rec.get("latency_ms") or age_ms),
                int(session_age_ms if session_age_ms < 10**9 else age_ms),
            )
            latency_score = _latency_score(freshness_ms, stale_ms)

            age_penalty = min(1.0, float(age_ms) / float(stale_ms))
            # Scores are routing heuristics, not hard truth. The goal is to rank
            # providers for failover/selection while remaining conservative.
            score = max(0.0, latency_score * (1.0 - age_penalty))
            if ok:
                score = max(FAILOVER_MIN_SCORE, latency_score)

            out[str(provider)] = {
                "provider": str(provider),
                "ok": bool(ok),
                "score": float(score),
                "latency_score": float(latency_score),
                "age_ms": int(age_ms),
                "stale_after_ms": int(stale_ms),
                "latency_ms": int(rec.get("latency_ms") or session_age_ms or age_ms),
                "n_symbols": int(rec.get("n_symbols") or sess.get("subscribed_symbol_count") or 0),
                "error": (rec.get("error") or sess.get("last_error")),
                "status": ("OK" if ok else "STALE"),
                "session_connected": bool(session_connected),
                "session_last_msg_age_ms": int(session_age_ms),
                "capabilities": dict(sess.get("capabilities") or {}),
                "manager_state": sess.get("manager_state"),
                "dedup_drop_count": int(sess.get("dedup_drop_count") or 0),
                "gap_event_count": int(sess.get("gap_event_count") or 0),
            }

        return out
    finally:
        con.close()


def _candidate_price(rec: Dict) -> Optional[float]:
    last = rec.get("last")
    if last is not None:
        try:
            return float(last)
        except Exception as e:
            _warn_nonfatal(
                "PROVIDER_ROUTER_LAST_PRICE_PARSE_FAILED",
                e,
                once_key="candidate_price_last",
                value=repr(last),
            )
            return None
    bid = rec.get("bid")
    ask = rec.get("ask")
    try:
        if bid is not None and ask is not None:
            return (float(bid) + float(ask)) / 2.0
    except Exception as e:
        _warn_nonfatal(
            "PROVIDER_ROUTER_BID_ASK_PARSE_FAILED",
            e,
            once_key="candidate_price_bid_ask",
            bid=repr(bid),
            ask=repr(ask),
        )
        return None
    return None


def select_best_quotes_from_snapshots(
    snapshot_by_provider: Dict[str, Dict[str, Dict]],
    provider_health: Optional[Dict[str, Dict]] = None,
    *,
    publish_selected: bool = True,
) -> Dict[str, Dict]:
    ts = now_ms()
    health = dict(provider_health or {})
    priority_rank = {name: idx for idx, name in enumerate(_PROVIDER_PRIORITY)}
    by_symbol: Dict[str, list] = {}

    for provider, snapshot in (snapshot_by_provider or {}).items():
        provider_s = str(provider or "").strip().lower()
        stale_ms = _provider_stale_ms(provider_s)
        h = health.get(provider_s) or {}
        provider_ok = bool(h.get("ok")) if h else True
        provider_score = float(h.get("score") or (1.0 if provider_ok else 0.0))
        latency_score = float(h.get("latency_score") or provider_score)
        capabilities = dict(h.get("capabilities") or {})
        session_connected = bool(h.get("session_connected"))
        session_last_msg_age_ms = int(h.get("session_last_msg_age_ms") or 10**9)
        polling_session_fresh = bool(
            capabilities.get("polling")
            and session_connected
            and session_last_msg_age_ms <= stale_ms
        )

        for symbol, rec in (snapshot or {}).items():
            if not isinstance(rec, dict):
                continue
            last = rec.get("last")
            bid = rec.get("bid")
            ask = rec.get("ask")
            if last is None and bid is None and ask is None:
                continue
            rec_ts_ms = int(rec.get("ts_ms") or 0)
            age_ms = max(0, ts - rec_ts_ms) if rec_ts_ms > 0 else 10**9
            effective_age_ms = min(age_ms, session_last_msg_age_ms) if polling_session_fresh else age_ms
            if effective_age_ms > stale_ms:
                continue
            by_symbol.setdefault(str(symbol), []).append(
                {
                    "provider": provider_s,
                    "record": dict(rec),
                    "age_ms": int(effective_age_ms),
                    "score": float(provider_score),
                    "latency_score": float(latency_score),
                    "ok": bool(provider_ok),
                }
            )

    out: Dict[str, Dict] = {}
    for symbol, candidates in by_symbol.items():
        candidates = [c for c in candidates if _candidate_price(c["record"]) is not None]
        if not candidates:
            continue

        prices = [_candidate_price(c["record"]) for c in candidates]
        prices = [float(p) for p in prices if p is not None]
        median_px = statistics.median(prices) if prices else None
        quorum = []
        if median_px is not None:
            for c in candidates:
                px = _candidate_price(c["record"])
                if px is None:
                    continue
                if _bps(float(px), float(median_px)) <= QUORUM_BPS:
                    quorum.append(c)

        primary_candidates = sorted(
            candidates,
            key=lambda c: (
                priority_rank.get(c["provider"], 999),
                0 if c["ok"] else 1,
                -float(c["score"]),
                -float(c["latency_score"]),
                int(c["age_ms"]),
                -int(c["record"].get("ts_ms") or 0),
            ),
        )
        primary = primary_candidates[0]

        # Prefer quorum-consistent quotes when available; otherwise fall back to
        # the best candidate rather than dropping the symbol entirely.
        pool = quorum if len(quorum) >= 2 else candidates
        pool = sorted(
            pool,
            key=lambda c: (
                0 if c["ok"] else 1,
                -float(c["latency_score"]),
                -float(c["score"]),
                priority_rank.get(c["provider"], 999),
                int(c["age_ms"]),
                -int(c["record"].get("ts_ms") or 0),
            ),
        )

        best = primary
        if not bool(primary.get("ok")):
            best = pool[0]
        elif len(quorum) >= 2 and all(str(c["provider"]) != str(primary["provider"]) for c in quorum):
            # If the preferred provider materially disagrees with the quorum, prefer the quorum.
            best = pool[0]
        rec = dict(best["record"])
        rec["provider"] = str(best["provider"])
        rec["source"] = str(best["provider"])

        failover_used = bool(best["provider"] != primary["provider"])
        failover_reason = None
        if failover_used:
            if not bool(primary.get("ok")):
                failover_reason = "primary_unhealthy"
            elif len(quorum) >= 2 and all(str(c["provider"]) != str(primary["provider"]) for c in quorum):
                failover_reason = "quorum_override"
            else:
                failover_reason = "provider_score_preferred"

        rec["failover_used"] = bool(failover_used)
        rec["failover_reason"] = failover_reason
        rec["preferred_provider"] = str(primary["provider"])
        rec["provider_age_ms"] = int(best["age_ms"])
        rec["provider_candidates"] = [str(c["provider"]) for c in pool]
        rec["latency_score"] = float(best["latency_score"])
        rec["provider_score"] = float(best["score"])
        rec["quorum_count"] = int(len(quorum))
        rec["quorum_price"] = float(median_px) if median_px is not None else None
        out[str(symbol)] = rec

        if publish_selected:
            try:
                publish_price_event(
                    {
                        "symbol": str(symbol),
                        "timestamp": int(rec.get("ts_ms") or int(time.time() * 1000)),
                        "provider": str(rec.get("provider") or "router"),
                        "bid": rec.get("bid"),
                        "ask": rec.get("ask"),
                        "last": rec.get("last"),
                        "volume": rec.get("volume"),
                        "latency_ms": int(rec.get("latency_ms") or 0),
                    },
                    component="engine.data.provider_router",
                )
            except Exception as e:
                _warn_nonfatal(
                    "PROVIDER_ROUTER_PUBLISH_PRICE_EVENT_FAILED",
                    e,
                    once_key=f"provider_router_publish_price_event_failed:{symbol}:{rec.get('provider')}",
                    symbol=str(symbol),
                    provider=str(rec.get("provider") or "router"),
                )
    return out


def detect_cross_provider_anomalies():
    ts = now_ms()

    con = connect(readonly=True)
    try:
        rows = con.execute(
            """
            SELECT r.symbol, r.provider, r.last, r.bid, r.ask, r.ts_ms
            FROM price_quotes_raw r
            JOIN (
                SELECT symbol, provider, MAX(ts_ms) AS max_ts_ms
                FROM price_quotes_raw
                WHERE ts_ms >= ?
                GROUP BY symbol, provider
            ) latest
              ON latest.symbol = r.symbol
             AND latest.provider = r.provider
             AND latest.max_ts_ms = r.ts_ms
            WHERE r.ts_ms >= ?
            """,
            (ts - STALE_THRESHOLD_MS, ts - STALE_THRESHOLD_MS),
        ).fetchall()
    finally:
        con.close()

    by_symbol: Dict[str, Dict[str, Dict]] = {}

    for sym, provider, last, bid, ask, pts in rows or []:
        by_symbol.setdefault(str(sym), {})[str(provider)] = {
            "last": last,
            "bid": bid,
            "ask": ask,
            "ts_ms": int(pts or 0),
        }

    anomalies = []
    for sym, providers in by_symbol.items():
        keys = sorted(providers.keys())
        if len(keys) < 2:
            continue

        for i in range(len(keys)):
            for j in range(i + 1, len(keys)):
                a = keys[i]
                b = keys[j]
                pa = providers[a]
                pb = providers[b]

                pxa = _candidate_price(pa)
                pxb = _candidate_price(pb)
                if pxa is None or pxb is None:
                    continue

                spread_bps = _bps(float(pxa), float(pxb))

                if spread_bps > ANOMALY_THRESHOLD_BPS:
                    anomalies.append(
                        (
                            ts,
                            sym,
                            a,
                            b,
                            float(spread_bps),
                            "cross_provider_spread",
                        )
                    )

    if not anomalies:
        return

    def _write(con):
        for row in anomalies:
            con.execute(
                """
                INSERT OR REPLACE INTO price_anomalies
                (ts_ms, symbol, provider_a, provider_b, spread_diff_bps, reason)
                VALUES (?,?,?,?,?,?)
                """,
                row,
            )

    run_write_txn(_write)


def route_best_price(symbol: str) -> Optional[float]:
    ts = now_ms()
    con = connect(readonly=True)
    try:
        health = compute_provider_health()

        rows = con.execute(
            """
            SELECT r.provider, r.last, r.bid, r.ask, r.ts_ms
            FROM price_quotes_raw r
            JOIN (
                SELECT provider, MAX(ts_ms) AS max_ts_ms
                FROM price_quotes_raw
                WHERE symbol = ?
                GROUP BY provider
            ) latest
              ON latest.provider = r.provider
             AND latest.max_ts_ms = r.ts_ms
            WHERE r.symbol = ?
            """,
            (symbol, symbol),
        ).fetchall()

        candidates = []
        for provider, last, bid, ask, ts_ms in rows or []:
            provider_s = str(provider).strip().lower()
            quote_ts_ms = int(ts_ms or 0)
            quote_age_ms = max(0, ts - quote_ts_ms)
            quote_stale_ms = _provider_stale_ms(provider_s)
            if quote_age_ms > quote_stale_ms:
                continue

            rec = {"last": last, "bid": bid, "ask": ask, "ts_ms": quote_ts_ms}
            px = _candidate_price(rec)
            if px is None:
                continue

            h = health.get(provider_s) or {}
            candidates.append({
                "provider": provider_s,
                "record": rec,
                "age_ms": quote_age_ms,
                "score": float(h.get("score") or 0.0),
                "latency_score": float(h.get("latency_score") or 0.0),
                "ok": bool(h.get("ok")),
            })

        if not candidates:
            return None

        selected = select_best_quotes_from_snapshots(
            {c["provider"]: {symbol: c["record"]} for c in candidates},
            provider_health=health,
            publish_selected=False,
        )

        rec = selected.get(symbol)
        if not rec:
            return None

        px = _candidate_price(rec)
        return float(px) if px is not None else None

    finally:
        con.close()


def compute_provider_score(health: dict) -> float:
    """
    Deterministic provider scoring:
    higher is better
    """

    if not health:
        return -1.0

    latency = float(health.get("latency_ms") or 1e6)
    age = float(health.get("age_ms") or 1e6)
    ok = bool(health.get("ok"))

    if not ok:
        return -1e9

    # weights
    latency_score = max(0.0, 1000.0 - latency)
    freshness_score = max(0.0, 1000.0 - age)

    return (latency_score * 0.6) + (freshness_score * 0.4)


def select_best_provider_by_score(provider_health: dict) -> str | None:
    best = None
    best_score = -1e12

    for name, health in (provider_health or {}).items():
        score = compute_provider_score(health)
        if score > best_score:
            best_score = score
            best = name

    return best
