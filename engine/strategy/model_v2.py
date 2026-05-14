"""Baseline regime-statistics model family used by the trading ML stack.

This file is the lightweight statistical model behind `regime_stats_v2`. It
learns priors and spillover effects from labeled history, then serves those
artifacts back into the predictor as a cheap, stable model family and fallback.
"""

import math
import os
import time
import logging
from typing import List, Optional, Tuple

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.storage import connect, init_db
from engine.runtime.event_log import record_regime_change
from engine.runtime.runtime_meta import meta_get, meta_set
from engine.runtime import price_cache as runtime_price_cache

LOG = logging.getLogger("model_v2")


def _warn_nonfatal(event: str, error: BaseException, **extra) -> None:
    log_failure(
        LOG,
        event=event,
        code=event,
        message=event,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.model_v2",
        extra=extra,
        persist=False,
    )

SCHEMA = """
CREATE TABLE IF NOT EXISTS model_stats_regime (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_ms INTEGER NOT NULL,
  symbol TEXT NOT NULL,
  horizon_s INTEGER NOT NULL,
  regime TEXT NOT NULL,
  n INTEGER NOT NULL,
  mean_impact_z REAL NOT NULL,
  UNIQUE(symbol, horizon_s, regime)
);

CREATE TABLE IF NOT EXISTS model_stats (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_ms INTEGER NOT NULL,
  symbol TEXT NOT NULL,
  horizon_s INTEGER NOT NULL,
  n INTEGER NOT NULL,
  mean_impact_z REAL NOT NULL,
  UNIQUE(symbol, horizon_s)
);

CREATE TABLE IF NOT EXISTS spillover_beta (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_ms INTEGER NOT NULL,
  target_symbol TEXT NOT NULL,
  driver_symbol TEXT NOT NULL,
  horizon_s INTEGER NOT NULL,
  n INTEGER NOT NULL,
  beta REAL NOT NULL,
  UNIQUE(target_symbol, driver_symbol, horizon_s)
);

CREATE INDEX IF NOT EXISTS idx_msreg_sym ON model_stats_regime(symbol, horizon_s);
CREATE INDEX IF NOT EXISTS idx_ms_sym ON model_stats(symbol, horizon_s);
CREATE INDEX IF NOT EXISTS idx_spill_tgt ON spillover_beta(target_symbol, horizon_s);
"""

DEFAULT_REGIMES = ["LOW", "MID", "HIGH"]
DEFAULT_MODEL_NAME = str(os.environ.get("MODEL_V2_NAME", "regime_stats_v2") or "regime_stats_v2").strip()
_RUNTIME_DB_FALLBACK = str(os.environ.get("MODEL_V2_RUNTIME_DB_FALLBACK", "") or "").strip().lower()


def _active_version_meta_key(model_name: str) -> str:
    return f"active_model_version:{str(model_name or DEFAULT_MODEL_NAME).strip()}"


def get_live_model_version(model_name: str = DEFAULT_MODEL_NAME) -> Optional[str]:
    try:
        value = str(meta_get(_active_version_meta_key(model_name), "") or "").strip()
        return value or None
    except Exception as e:
        _warn_nonfatal(
            "model_v2_live_version_read_failed",
            e,
            model_name=str(model_name),
        )
        return None


def set_live_model_version(model_name: str, model_version: Optional[str]) -> None:
    if not model_version:
        return
    meta_set(_active_version_meta_key(model_name), str(model_version))


def init_model_db():
    init_db()


def classify_regime(vol: float) -> str:
    """
    Deterministic regime bucketing from a volatility proxy.
    Used at label-time (labeling.py, label_due_events.py).

    Thresholds aligned to get_current_regime() levels:
      LOW  if vol < 0.004
      HIGH if vol > 0.012
      MID  otherwise

    If vol is missing/invalid, returns MID.
    """
    try:
        v = float(vol)
    except Exception as e:
        _warn_nonfatal("model_v2_classify_regime_parse_failed", e, vol=repr(vol))
        return "MID"

    if not math.isfinite(v) or v <= 0:
        return "MID"

    if v < 0.004:
        return "LOW"
    if v > 0.012:
        return "HIGH"
    return "MID"


def _classify_recent_prices(prices: List[float]) -> Tuple[str, Optional[float]]:
    if len(prices or []) < 30:
        return "MID", None

    rets = []
    for idx in range(1, len(prices)):
        prev = float(prices[idx - 1] or 0.0)
        current = float(prices[idx] or 0.0)
        if prev <= 0.0 or current <= 0.0:
            continue
        rets.append((current / prev) - 1.0)

    if len(rets) < 20:
        return "MID", None

    vol = float(math.sqrt(sum(r * r for r in rets) / len(rets)))
    return str(classify_regime(vol)), float(vol)


def _recent_prices_from_runtime_cache(symbol: str, *, limit: int = 120) -> List[float]:
    try:
        snapshot = runtime_price_cache.get_symbol_snapshot(str(symbol), allow_db_recovery=False)
    except Exception as e:
        _warn_nonfatal(
            "model_v2_runtime_cache_load_failed",
            e,
            symbol=str(symbol),
        )
        return []

    points = [
        float(point.price)
        for point in list(snapshot.points or ())
        if int(point.ts_ms) > 0 and float(point.price) > 0.0
    ]
    if not points:
        return []
    return list(points[-max(1, int(limit)) :])


def _runtime_db_fallback_allowed() -> bool:
    raw = str(_RUNTIME_DB_FALLBACK or "").strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    mode_name = str(os.environ.get("ENGINE_MODE", "safe") or "safe").strip().lower() or "safe"
    return mode_name == "safe"


def get_current_regime(symbol: str) -> str:
    # This is a cheap online regime estimate derived from recent prices so the
    # predictor can work without a heavyweight external state dependency.
    reg = "MID"
    vol = None

    cached_prices = _recent_prices_from_runtime_cache(str(symbol), limit=120)
    if cached_prices:
        reg, vol = _classify_recent_prices(cached_prices)
        try:
            record_regime_change(
                symbol=str(symbol),
                regime=str(reg),
                vol=(float(vol) if vol is not None else None),
            )
        except Exception as e:
            _warn_nonfatal(
                "model_v2_record_regime_change_failed",
                e,
                symbol=str(symbol),
                regime=str(reg),
            )
        return str(reg)

    if not _runtime_db_fallback_allowed():
        return str(reg)

    con = connect()
    try:
        try:
            rows = con.execute(
                """
                SELECT price
                FROM prices
                WHERE symbol=?
                ORDER BY ts_ms DESC
                LIMIT 120
                """,
                (str(symbol),),
            ).fetchall()
        except Exception:
            rows = []

        if rows and len(rows) >= 30:
            px = [float(r[0]) for r in reversed(rows) if r and r[0] is not None and float(r[0]) > 0.0]
            reg, vol = _classify_recent_prices(px)

        try:
            record_regime_change(
                symbol=str(symbol),
                regime=str(reg),
                vol=(float(vol) if vol is not None else None),
                con=con,
            )
        except Exception as e:
            _warn_nonfatal(
                "model_v2_record_regime_change_failed",
                e,
                symbol=str(symbol),
                regime=str(reg),
            )

        return str(reg)
    finally:
        con.close()


def get_regime_prior(
    symbol: str,
    horizon_s: int,
    *,
    model_version: Optional[str] = None,
    model_name: str = DEFAULT_MODEL_NAME,
) -> Tuple[float, int, str]:
    """
    Returns (mean_z, n, regime).
    If no regime row exists, returns (0,0,regime).
    """
    init_model_db()
    reg = get_current_regime(str(symbol))
    selected_version = str(model_version or get_live_model_version(model_name) or "").strip()
    con = connect()
    try:
        row = None
        if selected_version:
            try:
                row = con.execute(
                    """
                    SELECT mean_impact_z, n
                    FROM model_stats_regime_versions
                    WHERE model_name=? AND model_version=? AND symbol=? AND horizon_s=? AND regime=?
                    """,
                    (str(model_name), str(selected_version), str(symbol), int(horizon_s), str(reg)),
                ).fetchone()
            except Exception:
                row = None

        if row is None:
            try:
                row = con.execute(
                    """
                    SELECT mean_impact_z, n
                    FROM model_stats_regime
                    WHERE symbol=? AND horizon_s=? AND regime=?
                    """,
                    (str(symbol), int(horizon_s), str(reg)),
                ).fetchone()
            except Exception as e:
                _warn_nonfatal(
                    "model_v2_regime_prior_lookup_failed",
                    e,
                    symbol=str(symbol),
                    horizon_s=int(horizon_s),
                    regime=str(reg),
                )
                return 0.0, 0, str(reg)

        if not row:
            return 0.0, 0, str(reg)

        return float(row[0]), int(row[1]), str(reg)
    finally:
        con.close()


def get_spillover_betas(
    target_symbol: str,
    horizon_s: int,
    *,
    model_version: Optional[str] = None,
    model_name: str = DEFAULT_MODEL_NAME,
) -> List[Tuple[str, float, int]]:
    """
    Returns list of (driver_symbol, beta, n) for target_symbol/horizon_s.
    """
    init_model_db()
    selected_version = str(model_version or get_live_model_version(model_name) or "").strip()
    con = connect()
    try:
        rows = []
        if selected_version:
            try:
                rows = con.execute(
                    """
                    SELECT driver_symbol, beta, n
                    FROM spillover_beta_versions
                    WHERE model_name=? AND model_version=? AND target_symbol=? AND horizon_s=?
                    ORDER BY n DESC
                    """,
                    (str(model_name), str(selected_version), str(target_symbol), int(horizon_s)),
                ).fetchall()
            except Exception:
                rows = []

        if not rows:
            try:
                rows = con.execute(
                    """
                    SELECT driver_symbol, beta, n
                    FROM spillover_beta
                    WHERE target_symbol=? AND horizon_s=?
                    ORDER BY n DESC
                    """,
                    (str(target_symbol), int(horizon_s)),
                ).fetchall()
            except Exception as e:
                _warn_nonfatal(
                    "model_v2_spillover_beta_lookup_failed",
                    e,
                    target_symbol=str(target_symbol),
                    horizon_s=int(horizon_s),
                )
                return []

        out = []
        for drv, beta, n in rows or []:
            out.append((str(drv), float(beta), int(n)))
        return out
    finally:
        con.close()


def train_regime_stats(
    symbols: List[str],
    horizons: List[int],
    lookback_days: int = 90,
    *,
    model_version: Optional[str] = None,
    model_name: str = DEFAULT_MODEL_NAME,
    publish_live: bool = False,
) -> int:
    """
    Builds:
      - model_stats_regime (per regime mean + n)
      - model_stats (global mean + n)
    using labels joined with prices-derived regime at prediction time.
    """
    init_model_db()
    now_ms = int(time.time() * 1000)
    cutoff_ms = now_ms - int(lookback_days) * 24 * 3600 * 1000

    con = connect()
    try:
        # labels table must exist
        try:
            rows = con.execute(
                """
                SELECT l.symbol, l.horizon_s, l.impact_z
                FROM labels l
                JOIN events e ON e.id = l.event_id
                WHERE e.ts_ms >= ?
                  AND l.impact_z IS NOT NULL
                """,
                (int(cutoff_ms),),
            ).fetchall()
        except Exception as e:
            _warn_nonfatal("model_v2_training_label_query_failed", e, cutoff_ms=int(cutoff_ms))
            return 0

        # bucket by current regime of symbol (best-effort)
        # (simple + stable; avoids storing regime per label)
        from collections import defaultdict

        by_reg = defaultdict(list)   # (sym,h,reg) -> [z]
        by_glob = defaultdict(list)  # (sym,h) -> [z]

        symset = set(str(s) for s in (symbols or []))
        hset = set(int(h) for h in (horizons or []))

        for sym, h, z in rows or []:
            sym = str(sym)
            h = int(h)
            if symset and sym not in symset:
                continue
            if hset and h not in hset:
                continue
            try:
                zz = float(z)
            except Exception as e:
                _warn_nonfatal(
                    "model_v2_training_label_parse_failed",
                    e,
                    symbol=str(sym),
                    horizon_s=int(h),
                )
                continue

            reg = get_current_regime(sym)
            by_reg[(sym, h, reg)].append(zz)
            by_glob[(sym, h)].append(zz)

        cur = con.cursor()
        updated = 0

        target_version = str(model_version or "").strip()

        # upsert regime stats
        for (sym, h, reg), zs in by_reg.items():
            n = int(len(zs))
            if n <= 0:
                continue
            mean_z = float(sum(zs) / n)
            if target_version:
                cur.execute(
                    """
                    INSERT INTO model_stats_regime_versions(
                      model_name, model_version, ts_ms, symbol, horizon_s, regime, n, mean_impact_z
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(model_name, model_version, symbol, horizon_s, regime) DO UPDATE SET
                      ts_ms=excluded.ts_ms,
                      n=excluded.n,
                      mean_impact_z=excluded.mean_impact_z
                    """,
                    (
                        str(model_name),
                        str(target_version),
                        int(now_ms),
                        str(sym),
                        int(h),
                        str(reg),
                        int(n),
                        float(mean_z),
                    ),
                )
            if publish_live or not target_version:
                cur.execute(
                    """
                    INSERT INTO model_stats_regime(ts_ms, symbol, horizon_s, regime, n, mean_impact_z)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(symbol, horizon_s, regime) DO UPDATE SET
                      ts_ms=excluded.ts_ms,
                      n=excluded.n,
                      mean_impact_z=excluded.mean_impact_z
                    """,
                    (int(now_ms), str(sym), int(h), str(reg), int(n), float(mean_z)),
                )
            updated += 1

        # upsert global stats
        for (sym, h), zs in by_glob.items():
            n = int(len(zs))
            if n <= 0:
                continue
            mean_z = float(sum(zs) / n)
            if target_version:
                cur.execute(
                    """
                    INSERT INTO model_stats_versions(
                      model_name, model_version, ts_ms, symbol, horizon_s, n, mean_impact_z
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(model_name, model_version, symbol, horizon_s) DO UPDATE SET
                      ts_ms=excluded.ts_ms,
                      n=excluded.n,
                      mean_impact_z=excluded.mean_impact_z
                    """,
                    (
                        str(model_name),
                        str(target_version),
                        int(now_ms),
                        str(sym),
                        int(h),
                        int(n),
                        float(mean_z),
                    ),
                )
            if publish_live or not target_version:
                cur.execute(
                    """
                    INSERT INTO model_stats(ts_ms, symbol, horizon_s, n, mean_impact_z)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(symbol, horizon_s) DO UPDATE SET
                      ts_ms=excluded.ts_ms,
                      n=excluded.n,
                      mean_impact_z=excluded.mean_impact_z
                    """,
                    (int(now_ms), str(sym), int(h), int(n), float(mean_z)),
                )
            updated += 1

        con.commit()
        if target_version and publish_live:
            set_live_model_version(str(model_name), str(target_version))
        return int(updated)
    finally:
        con.close()


def publish_model_version(model_version: str, *, model_name: str = DEFAULT_MODEL_NAME) -> int:
    init_model_db()
    version = str(model_version or "").strip()
    if not version:
        return 0

    con = connect()
    try:
        rows_regime = con.execute(
            """
            SELECT ts_ms, symbol, horizon_s, regime, n, mean_impact_z
            FROM model_stats_regime_versions
            WHERE model_name=? AND model_version=?
            """,
            (str(model_name), str(version)),
        ).fetchall()
        rows_global = con.execute(
            """
            SELECT ts_ms, symbol, horizon_s, n, mean_impact_z
            FROM model_stats_versions
            WHERE model_name=? AND model_version=?
            """,
            (str(model_name), str(version)),
        ).fetchall()

        cur = con.cursor()
        written = 0
        for ts_ms, symbol, horizon_s, regime, n, mean_impact_z in rows_regime or []:
            cur.execute(
                """
                INSERT INTO model_stats_regime(ts_ms, symbol, horizon_s, regime, n, mean_impact_z)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, horizon_s, regime) DO UPDATE SET
                  ts_ms=excluded.ts_ms,
                  n=excluded.n,
                  mean_impact_z=excluded.mean_impact_z
                """,
                (int(ts_ms), str(symbol), int(horizon_s), str(regime), int(n), float(mean_impact_z)),
            )
            written += 1
        for ts_ms, symbol, horizon_s, n, mean_impact_z in rows_global or []:
            cur.execute(
                """
                INSERT INTO model_stats(ts_ms, symbol, horizon_s, n, mean_impact_z)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(symbol, horizon_s) DO UPDATE SET
                  ts_ms=excluded.ts_ms,
                  n=excluded.n,
                  mean_impact_z=excluded.mean_impact_z
                """,
                (int(ts_ms), str(symbol), int(horizon_s), int(n), float(mean_impact_z)),
            )
            written += 1
        con.commit()
        set_live_model_version(str(model_name), str(version))
        return int(written)
    finally:
        con.close()


def train_regime_model(
    *,
    rows,
    horizon_s: int,
    regime: Optional[str] = None,
    shadow: bool = False,
):
    now_ms = int(time.time() * 1000)
    n = 0
    sum_z = 0.0

    for r in rows or []:
        try:
            z = float(r[3])
        except Exception as e:
            _warn_nonfatal("model_v2_stat_row_parse_failed", e, row=repr(r))
            continue
        sum_z += z
        n += 1

    mean_z = float(sum_z / n) if n > 0 else 0.0

    model = {
        "model_kind": "shadow_regime_stats",
        "model_ts_ms": int(now_ms),
        "horizon_s": int(horizon_s),
        "regime": str(regime or "global"),
        "shadow": bool(shadow),
        "train_rows": int(n),
    }

    metrics = {
        "train_rows": int(n),
        "mean_realized_z": float(mean_z),
        "horizon_s": int(horizon_s),
        "regime": str(regime or "global"),
        "shadow": bool(shadow),
    }

    return model, metrics
