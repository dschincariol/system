"""
FILE: size_policy.py

Loads the latest learned size policy and maps confidence buckets into size
factors. This is the read path used by live sizing code after an offline
training job has persisted policy points.
"""

import json
from typing import Optional, Dict, Any, List

from engine.runtime.storage import connect, init_db


def _safe_json_dict(raw: Any) -> Dict[str, Any]:
    try:
        value = json.loads(raw or "{}")
    except Exception:
        return {}
    return dict(value) if isinstance(value, dict) else {}


def load_latest_size_policy(con=None) -> Optional[Dict[str, Any]]:
    """
    Returns the latest size policy blob with decoded bucket points.
    """
    init_db()
    owns = False
    if con is None:
        con = connect()
        owns = True
    try:
        columns = set()
        try:
            columns = {
                str(row[1] if len(row) > 1 else "").lower()
                for row in (con.execute("PRAGMA table_info(size_policy)").fetchall() or [])
            }
        except Exception:
            columns = set()

        legacy_shape = not {"lookback_days", "buckets"}.issubset(columns)
        if legacy_shape:
            r = con.execute(
                """
                SELECT id, ts_ms, method, params_json, metrics_json
                FROM size_policy
                ORDER BY ts_ms DESC
                LIMIT 1
                """
            ).fetchone()
        else:
            r = con.execute(
                """
                SELECT id, ts_ms, lookback_days, buckets, method, params_json, metrics_json
                FROM size_policy
                ORDER BY ts_ms DESC
                LIMIT 1
                """
            ).fetchone()
        if not r:
            return None
        if legacy_shape:
            pid, ts_ms, method, pj, mj = r
            params = _safe_json_dict(pj)
            metrics = _safe_json_dict(mj)
            lookback_days = int(params.get("lookback_days") or 0)
            buckets = int(params.get("buckets") or 0)
        else:
            pid, ts_ms, lookback_days, buckets, method, pj, mj = r
            params = _safe_json_dict(pj)
            metrics = _safe_json_dict(mj)
            lookback_days = int(lookback_days or params.get("lookback_days") or 0)
            buckets = int(buckets or params.get("buckets") or 0)

        pts = con.execute(
            """
            SELECT bucket_idx, conf_lo, conf_hi, n, mean_net_ret, std_net_ret, factor
            FROM size_policy_points
            WHERE policy_id=?
            ORDER BY bucket_idx ASC
            """,
            (int(pid),),
        ).fetchall()

        # Keep the payload human-readable because this object is often logged
        # or surfaced in diagnostics rather than only consumed programmatically.
        points: List[Dict[str, Any]] = []
        for bi, clo, chi, n, mnr, sdr, f in pts or []:
            points.append({
                "bucket_idx": int(bi),
                "conf_lo": float(clo),
                "conf_hi": float(chi),
                "n": int(n),
                "mean_net_ret": float(mnr),
                "std_net_ret": float(sdr),
                "factor": float(f),
            })

        return {
            "policy_id": int(pid),
            "ts_ms": int(ts_ms),
            "lookback_days": int(lookback_days),
            "buckets": int(buckets),
            "method": str(method),
            "params": params,
            "metrics": metrics,
            "points": points,
        }
    finally:
        if owns:
            con.close()


def size_factor(policy: Optional[Dict[str, Any]], conf: float, drawdown: float = 0.0) -> float:
    """
    Map confidence -> [0..1] factor using latest learned buckets.
    Optionally multiply by dd_factor(drawdown) if present in policy params.

    Falls back to 1.0 if no policy exists.
    """
    if policy is None:
        return 1.0

    try:
        c = float(conf)
    except Exception:
        c = 0.0
    if c <= 0:
        base = 0.0
    else:
        pts = policy.get("points") or []
        base = None
        for p in pts:
            if c >= float(p["conf_lo"]) and c < float(p["conf_hi"]):
                base = float(p.get("factor", 1.0))
                break
        if base is None:
            base = float(pts[-1].get("factor", 1.0)) if pts else 1.0
        base = max(0.0, min(1.0, float(base)))

    # Drawdown compression is optional so old policies still remain valid.
    dd_mult = 1.0
    try:
        dd = max(0.0, min(1.0, float(drawdown or 0.0)))
    except Exception:
        dd = 0.0

    try:
        params = policy.get("params") or {}
        dd_points = params.get("dd_points") or []
        for p in dd_points:
            lo = float(p.get("dd_lo", 0.0))
            hi = float(p.get("dd_hi", 1.0))
            if dd >= lo and dd < hi:
                dd_mult = float(p.get("factor", 1.0))
                break
        if dd_points and dd >= float(dd_points[-1].get("dd_lo", 0.0)):
            dd_mult = float(dd_points[-1].get("factor", dd_mult))
        dd_mult = max(0.0, min(1.0, float(dd_mult)))
    except Exception:
        dd_mult = 1.0

    return max(0.0, min(1.0, float(base) * float(dd_mult)))
