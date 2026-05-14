"""
FILE: news_domain.py

Extracts source domains from events and looks up domain-level confidence or
blocklist rules. This is the source-quality layer for event-driven signals.
"""

import json
from urllib.parse import urlparse

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect

LOG = get_logger("engine.strategy.news_domain")
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
        component="engine.strategy.news_domain",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)

def extract_domain(url: str, meta_json: str | None = None) -> str:
    # Prefer structured ingestion metadata when available because raw URLs are
    # not always canonical or even present.
    try:
        if meta_json:
            m = json.loads(meta_json)
            if isinstance(m, dict):
                gd = m.get("gdelt")
                if isinstance(gd, dict) and gd.get("domain"):
                    return str(gd.get("domain")).lower().strip()
    except Exception as e:
        _warn_nonfatal("NEWS_DOMAIN_META_PARSE_FAILED", e, once_key="extract_domain_meta")

    try:
        u = (url or "").strip()
        if not u:
            return ""
        host = urlparse(u).netloc.lower().strip()
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception as e:
        _warn_nonfatal("NEWS_DOMAIN_URL_PARSE_FAILED", e, once_key="extract_domain_url", url=str(url)[:200])
        return ""


def is_domain_blocked(domain: str, symbol: str) -> bool:
    d = (domain or "").lower().strip()
    s = (symbol or "").upper().strip()
    if not d or not s:
        return False

    con = connect()
    try:
        # symbol-specific rule wins, then global '*'
        row = con.execute(
            """
            SELECT status
            FROM domain_blacklist
            WHERE domain=? AND symbol IN (?, '*')
            ORDER BY CASE WHEN symbol=? THEN 0 ELSE 1 END
            LIMIT 1
            """,
            (d, s, s),
        ).fetchone()
        if not row:
            return False
        return str(row[0] or "").upper() == "BLOCK"
    except Exception as e:
        _warn_nonfatal("NEWS_DOMAIN_BLOCK_CHECK_FAILED", e, once_key=f"is_domain_blocked:{domain}:{symbol}", domain=str(domain), symbol=str(symbol))
        return False
    finally:
        con.close()


def domain_conf_multiplier(domain: str, symbol: str, regime: str, horizon_s: int) -> float:
    d = (domain or "").lower().strip()
    s = (symbol or "").upper().strip()
    r = (regime or "MID").upper().strip()
    h = int(horizon_s or 0)
    if not d or not s or h <= 0:
        return 1.0

    con = connect()
    try:
        row = con.execute(
            """
            SELECT mean_edge, win_rate, n
            FROM domain_perf
            WHERE domain=? AND symbol=? AND regime=? AND horizon_s=?
            """,
            (d, s, r, h),
        ).fetchone()
        if not row:
            return 1.0

        mean_edge = row[0]
        win_rate = row[1]
        n = int(row[2] or 0)
        if n < 30:
            return 1.0

        # Simple stable mapping:
        # - if mean_edge negative => downweight
        # - if win_rate low => downweight
        mult = 1.0
        try:
            if mean_edge is not None and float(mean_edge) < 0.0:
                mult *= 0.85
        except Exception as e:
            _warn_nonfatal("NEWS_DOMAIN_EDGE_PARSE_FAILED", e, once_key="mean_edge_parse", domain=str(d), symbol=str(s))
        try:
            if win_rate is not None and float(win_rate) < 0.45:
                mult *= 0.90
        except Exception as e:
            _warn_nonfatal("NEWS_DOMAIN_EDGE_PARSE_FAILED", e, once_key="win_rate_parse", domain=str(d), symbol=str(s))

        return float(max(0.50, min(1.10, mult)))
    except Exception as e:
        _warn_nonfatal(
            "NEWS_DOMAIN_CONF_MULTIPLIER_FAILED",
            e,
            once_key=f"conf_multiplier:{domain}:{symbol}:{regime}:{horizon_s}",
            domain=str(domain),
            symbol=str(symbol),
            regime=str(regime),
            horizon_s=repr(horizon_s)[:120],
        )
        return 1.0
    finally:
        con.close()
