"""
FILE: hierarchical_allocator.py

Runtime subsystem module for `hierarchical_allocator`.
"""

# engine/runtime/hierarchical_allocator.py
"""
Hierarchical Capital Allocator (Sleeves -> Strategies)

- Level 0: Global window/bucket config
- Level 1: Sleeve scoring + correlation-aware sleeve weights
- Level 2: Strategy scoring inside sleeve + correlation-aware intra-sleeve weights
- Output: per-strategy global allocation weights (sleeve_weight * strategy_weight_in_sleeve)

Data source (preferred):
  - execution_capital_efficiency (engine/execution/execution_ledger.py)

Persists (additive, fail-open):
  - sleeve_metrics (window_days=0)
  - sleeve_allocations (window_days=0)
  - strategy_metrics (window_days=0) merged (does not remove existing keys)
  - strategy_allocations (window_days=0) allocations_json = per-strategy global weights

Config (optional env):
  HIER_ALLOC_WINDOW_S=86400
  HIER_ALLOC_BUCKET_S=900

  SLEEVE_RISK_BUDGETS_JSON='{"equities":0.50,"options":0.30,"futures":0.20}'
  STRATEGY_RISK_BUDGETS_JSON='{"my_strategy":0.25,...}'
  STRATEGY_SLEEVE_MAP_JSON='{"my_strategy":"equities",...}'

  HIER_ALLOC_CORR_GAMMA_SLEEVE=1.5
  HIER_ALLOC_CORR_GAMMA_STRAT=1.5

  HIER_ALLOC_DD_TH=0.10
  HIER_ALLOC_DD_FLOOR=0.10

  HIER_ALLOC_MIN_SHARE=0.0
  HIER_ALLOC_MAX_SHARE=1.0

  HIER_ALLOC_SCORE_FLOOR=0.0
"""

import json
import logging
import math
import os
import time
from typing import Any, Dict, List, Optional, Tuple

from engine.runtime.allocator_status import _safe_float, _table_exists
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger


DEFAULT_WINDOW_S = int(os.environ.get("HIER_ALLOC_WINDOW_S", "86400"))
DEFAULT_BUCKET_S = int(os.environ.get("HIER_ALLOC_BUCKET_S", "900"))

CORR_GAMMA_SLEEVE = float(os.environ.get("HIER_ALLOC_CORR_GAMMA_SLEEVE", "1.5"))
CORR_GAMMA_STRAT = float(os.environ.get("HIER_ALLOC_CORR_GAMMA_STRAT", "1.5"))

DD_TH = float(os.environ.get("HIER_ALLOC_DD_TH", "0.10"))
DD_FLOOR = float(os.environ.get("HIER_ALLOC_DD_FLOOR", "0.10"))

MIN_SHARE = float(os.environ.get("HIER_ALLOC_MIN_SHARE", "0.0"))
MAX_SHARE = float(os.environ.get("HIER_ALLOC_MAX_SHARE", "1.0"))

SCORE_FLOOR = float(os.environ.get("HIER_ALLOC_SCORE_FLOOR", "0.0"))

_SLEEVE_BUDGETS_RAW = os.environ.get("SLEEVE_RISK_BUDGETS_JSON", "").strip()
_STRATEGY_BUDGETS_RAW = os.environ.get("STRATEGY_RISK_BUDGETS_JSON", "").strip()
_STRATEGY_SLEEVE_MAP_RAW = os.environ.get("STRATEGY_SLEEVE_MAP_JSON", "").strip()
LOG = get_logger("engine.runtime.hierarchical_allocator")
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
        component="engine.runtime.hierarchical_allocator",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _now_ms() -> int:
    return int(time.time() * 1000)

def _parse_json_dict(raw: str) -> Dict[str, Any]:
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else {}
    except Exception as e:
        _warn_nonfatal(
            "HIER_ALLOC_PARSE_JSON_DICT_FAILED",
            e,
            once_key=f"parse_json_dict:{str(raw)[:80]}",
            raw_preview=str(raw)[:200],
        )
        return {}


def _parse_budget_map(raw: str) -> Dict[str, float]:
    obj = _parse_json_dict(raw)
    out: Dict[str, float] = {}
    for k, v in (obj or {}).items():
        try:
            kk = str(k).strip()
            if not kk:
                continue
            # Budgets are clamps, not signals; negative values are treated as
            # configuration errors and collapsed to zero.
            out[kk] = max(0.0, float(v))
        except Exception as e:
            _warn_nonfatal(
                "HIER_ALLOC_BUDGET_VALUE_PARSE_FAILED",
                e,
                once_key=f"budget_value:{k}",
                budget_key=str(k),
                raw_value=v,
            )
            continue
    return out


def _bucket_ts(ts_ms: int, bucket_s: int) -> int:
    b = int(max(1, int(bucket_s)))
    return int((int(ts_ms) // int(b * 1000)) * int(b * 1000))


def _stddev(vals: List[float]) -> float:
    if not vals:
        return 0.0
    if len(vals) == 1:
        return 0.0
    m = sum(vals) / float(len(vals))
    var = sum((x - m) ** 2 for x in vals) / float(len(vals) - 1)
    return math.sqrt(max(0.0, var))


def _corr(a: List[float], b: List[float]) -> float:
    if not a or not b or len(a) != len(b) or len(a) < 3:
        return 0.0
    ma = sum(a) / len(a)
    mb = sum(b) / len(b)
    va = sum((x - ma) ** 2 for x in a)
    vb = sum((y - mb) ** 2 for y in b)
    if va <= 1e-12 or vb <= 1e-12:
        return 0.0
    cov = sum((a[i] - ma) * (b[i] - mb) for i in range(len(a)))
    return float(cov / math.sqrt(va * vb))


def _max_drawdown_from_pnl_series(pnl_series: List[Tuple[int, float]]) -> float:
    if not pnl_series:
        return 0.0

    eq = 0.0
    peak = 0.0
    max_dd = 0.0

    for _, pnl in pnl_series:
        eq += float(pnl)
        if eq > peak:
            peak = eq
        dd = peak - eq
        if dd > max_dd:
            max_dd = dd

    denom = max(1e-9, abs(float(peak)))
    return float(max_dd) / float(denom)


def _drawdown_scale(dd: float) -> float:
    dd_th = max(1e-6, float(DD_TH))
    if float(dd) <= float(dd_th):
        return 1.0
    sc = 1.0 - ((float(dd) - float(dd_th)) / float(dd_th))
    return float(max(float(DD_FLOOR), float(sc)))


def _load_strategy_registry_meta(con) -> Dict[str, Dict[str, Any]]:
    if not _table_exists(con, "strategy_registry"):
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    try:
        rows = con.execute(
            """
            SELECT strategy_name, meta_json
            FROM strategy_registry
            """
        ).fetchall()
        for sname, mj in rows or []:
            try:
                name = str(sname or "").strip()
                if not name:
                    continue
                meta = json.loads(mj or "{}")
                out[name] = meta if isinstance(meta, dict) else {}
            except Exception as e:
                _warn_nonfatal(
                    "HIER_ALLOC_REGISTRY_META_PARSE_FAILED",
                    e,
                    once_key=f"registry_meta:{sname}",
                    strategy_name=str(sname or ""),
                    raw_meta=mj,
                )
                out[str(sname)] = {}
    except Exception as e:
        _warn_nonfatal(
            "HIER_ALLOC_REGISTRY_META_LOAD_FAILED",
            e,
            once_key="registry_meta_load",
        )
        return out
    return out


def _strategy_to_sleeve(con, sname: str, explicit_map: Dict[str, Any], meta_map: Dict[str, Dict[str, Any]]) -> str:
    s = str(sname or "").strip()
    if not s:
        return "default"

    # Explicit env config wins so operators can override registry metadata
    # without editing strategy records in-place.
    if s in explicit_map:
        try:
            v = explicit_map.get(s)
            if isinstance(v, str) and v.strip():
                return v.strip()
        except Exception as e:
            _warn_nonfatal(
                "HIER_ALLOC_EXPLICIT_SLEEVE_MAP_FAILED",
                e,
                once_key="explicit_sleeve_map",
                strategy_name=str(s),
            )

    meta = meta_map.get(s) or {}
    for key in ("sleeve", "asset_class", "assetClass"):
        try:
            v = meta.get(key)
            if isinstance(v, str) and v.strip():
                return v.strip()
        except Exception as e:
            _warn_nonfatal(
                "HIER_ALLOC_META_SLEEVE_LOOKUP_FAILED",
                e,
                once_key=f"meta_sleeve_lookup:{s}:{key}",
                strategy_name=str(s),
                meta_key=str(key),
            )
            continue

    return "default"


def _read_exec_cap_eff(con, since_ms: int, now_ms: int) -> List[Tuple[int, str, float, float, float]]:
    if not _table_exists(con, "execution_capital_efficiency"):
        return []
    try:
        rows = con.execute(
            """
            SELECT ts_ms, strategy_name, pnl_net, return_per_risk, drawdown_contrib
            FROM execution_capital_efficiency
            WHERE ts_ms BETWEEN ? AND ?
              AND strategy_name IS NOT NULL
              AND strategy_name <> ''
            ORDER BY ts_ms ASC
            """,
            (int(since_ms), int(now_ms)),
        ).fetchall()
    except Exception as e:
        _warn_nonfatal(
            "HIER_ALLOC_EXEC_CAP_EFF_READ_FAILED",
            e,
            once_key="exec_cap_eff_read",
            since_ms=int(since_ms),
            now_ms=int(now_ms),
        )
        return []

    out: List[Tuple[int, str, float, float, float]] = []
    for r in rows or []:
        try:
            ts_ms = int(r[0] or 0)
            name = str(r[1] or "").strip()
            if not name:
                continue
            pnl = _safe_float(r[2], 0.0)
            rpr = _safe_float(r[3], 0.0)
            ddc = _safe_float(r[4], 0.0)
            out.append((ts_ms, name, pnl, rpr, ddc))
        except Exception as e:
            _warn_nonfatal(
                "HIER_ALLOC_EXEC_CAP_EFF_ROW_PARSE_FAILED",
                e,
                once_key=f"exec_cap_eff_row:{r}",
                raw_row=str(r),
            )
            continue
    return out


def compute_and_persist_hier_allocations(con, *, now_ms: Optional[int] = None) -> Dict[str, Any]:
    ts_ms = int(now_ms) if now_ms is not None else _now_ms()
    window_s = int(max(60, int(DEFAULT_WINDOW_S)))
    bucket_s = int(max(60, int(DEFAULT_BUCKET_S)))
    since_ms = int(ts_ms) - int(window_s * 1000)

    sleeve_budgets = _parse_budget_map(_SLEEVE_BUDGETS_RAW)
    strat_budgets = _parse_budget_map(_STRATEGY_BUDGETS_RAW)

    explicit_sleeve_map = _parse_json_dict(_STRATEGY_SLEEVE_MAP_RAW)

    meta_map = _load_strategy_registry_meta(con)

    rows = _read_exec_cap_eff(con, since_ms=since_ms, now_ms=ts_ms)
    if not rows:
        return {
            "ok": False,
            "ts_ms": int(ts_ms),
            "window_s": int(window_s),
            "bucket_s": int(bucket_s),
            "sleeves": {},
            "strategies": {},
            "reason": "no_execution_capital_efficiency_rows",
        }

    # Bucket PnL by sleeve and strategy first so both sleeve-level and
    # intra-sleeve scoring are based on the same aligned return windows.
    # This keeps correlation penalties internally consistent.
    sleeve_bucket: Dict[str, Dict[int, float]] = {}
    strat_bucket: Dict[str, Dict[int, float]] = {}
    strat_rpr: Dict[str, List[float]] = {}
    strat_ddc: Dict[str, List[float]] = {}
    strat_sleeve: Dict[str, str] = {}

    for r_ts_ms, sname, pnl, rpr, ddc in rows:
        sleeve = _strategy_to_sleeve(con, sname, explicit_sleeve_map, meta_map)
        strat_sleeve[str(sname)] = str(sleeve)

        bts = _bucket_ts(int(r_ts_ms), bucket_s=bucket_s)

        sleeve_bucket.setdefault(str(sleeve), {})
        sleeve_bucket[str(sleeve)][bts] = float(sleeve_bucket[str(sleeve)].get(bts, 0.0)) + float(pnl)

        strat_bucket.setdefault(str(sname), {})
        strat_bucket[str(sname)][bts] = float(strat_bucket[str(sname)].get(bts, 0.0)) + float(pnl)

        strat_rpr.setdefault(str(sname), []).append(float(rpr))
        strat_ddc.setdefault(str(sname), []).append(float(ddc))

    sleeves = sorted(sleeve_bucket.keys())
    if not sleeves:
        sleeves = ["default"]

    all_buckets = sorted({b for m in sleeve_bucket.values() for b in m.keys()})

    sleeve_series: Dict[str, List[float]] = {}
    sleeve_pnl_series: Dict[str, List[Tuple[int, float]]] = {}

    for sl in sleeves:
        m = sleeve_bucket.get(sl) or {}
        sleeve_series[sl] = [float(m.get(b, 0.0)) for b in all_buckets]
        sleeve_pnl_series[sl] = [(int(b), float(m.get(b, 0.0))) for b in all_buckets]

    # Correlation penalties intentionally reduce concentration in sleeves or
    # strategies that are behaving like the same book under different names.
    # Sleeve correlation penalty
    sleeve_corr_pen: Dict[str, float] = {}
    sleeve_avg_abs_corr: Dict[str, float] = {}
    for i, si in enumerate(sleeves):
        abs_corrs = []
        for j, sj in enumerate(sleeves):
            if i == j:
                continue
            c = _corr(sleeve_series[si], sleeve_series[sj])
            abs_corrs.append(abs(float(c)))
        ac = sum(abs_corrs) / float(len(abs_corrs)) if abs_corrs else 0.0
        sleeve_avg_abs_corr[si] = float(ac)
        pen = 1.0 / (1.0 + max(0.0, float(CORR_GAMMA_SLEEVE)) * float(ac))
        sleeve_corr_pen[si] = float(max(0.0, min(1.0, pen)))

    # Sleeve scores -> sleeve weights
    # Sleeve budgets act as top-down capital policy; the score still depends on
    # realized performance, but budgets constrain how much a sleeve can win.
    sleeve_details: Dict[str, Dict[str, Any]] = {}
    sleeve_scores: Dict[str, float] = {}

    for sl in sleeves:
        rets = sleeve_series.get(sl) or []
        mu = sum(rets) / float(len(rets)) if rets else 0.0
        sd = _stddev(rets)
        sharpe = float(mu / sd) if sd > 1e-12 else 0.0
        dd = _max_drawdown_from_pnl_series(sleeve_pnl_series.get(sl) or [])
        dd_sc = _drawdown_scale(dd)

        base = float(sharpe)
        score = float(base) * float(dd_sc) * float(sleeve_corr_pen.get(sl, 1.0))

        budget = float(sleeve_budgets.get(sl, 1.0))
        if budget < 0.0:
            budget = 0.0
        score *= float(budget)

        if float(score) < float(SCORE_FLOOR):
            score = float(SCORE_FLOOR)

        sleeve_scores[sl] = float(max(0.0, score))
        sleeve_details[sl] = {
            "window_s": int(window_s),
            "bucket_s": int(bucket_s),
            "mean_bucket_pnl": float(mu),
            "std_bucket_pnl": float(sd),
            "sharpe_bucket": float(sharpe),
            "max_drawdown_proxy": float(dd),
            "dd_scale": float(dd_sc),
            "avg_abs_corr": float(sleeve_avg_abs_corr.get(sl, 0.0)),
            "corr_penalty": float(sleeve_corr_pen.get(sl, 1.0)),
            "risk_budget": float(budget),
            "raw_score": float(sleeve_scores[sl]),
        }

    sleeve_total = sum(float(v) for v in sleeve_scores.values())
    if sleeve_total <= 1e-12:
        sleeve_total = float(len(sleeves) or 1)
        for sl in sleeves:
            sleeve_scores[sl] = 1.0

    sleeve_w = {sl: float(sleeve_scores[sl]) / float(sleeve_total) for sl in sleeves}

    # Clamp and renormalize sleeves
    for sl in sleeves:
        w = float(sleeve_w.get(sl, 0.0))
        w = max(float(MIN_SHARE), min(float(MAX_SHARE), float(w)))
        sleeve_w[sl] = float(w)
    gross = sum(float(sleeve_w.get(sl, 0.0)) for sl in sleeves)
    if gross > 1e-12:
        for sl in sleeves:
            sleeve_w[sl] = float(sleeve_w.get(sl, 0.0)) / float(gross)

    # Strategy weights are normalized inside each sleeve, then multiplied by
    # the sleeve weight to produce global allocations.
    # Build per-sleeve bucket alignment
    strat_details: Dict[str, Dict[str, Any]] = {}
    strat_global_alloc: Dict[str, float] = {}
    strat_in_sleeve_alloc: Dict[str, float] = {}

    for sl in sleeves:
        # strategies in this sleeve
        sl_strats = sorted([s for s, s_sl in (strat_sleeve or {}).items() if str(s_sl) == str(sl)])
        if not sl_strats:
            continue

        # align buckets for this sleeve
        sl_buckets = sorted({b for s in sl_strats for b in (strat_bucket.get(s) or {}).keys()})
        if not sl_buckets:
            sl_buckets = all_buckets[:] if all_buckets else []

        sl_series: Dict[str, List[float]] = {}
        sl_pnl_series: Dict[str, List[Tuple[int, float]]] = {}
        for s in sl_strats:
            m = strat_bucket.get(s) or {}
            sl_series[s] = [float(m.get(b, 0.0)) for b in sl_buckets]
            sl_pnl_series[s] = [(int(b), float(m.get(b, 0.0))) for b in sl_buckets]

        # intra-sleeve correlation penalty per strategy
        s_corr_pen: Dict[str, float] = {}
        s_avg_abs_corr: Dict[str, float] = {}
        for i, si in enumerate(sl_strats):
            abs_corrs = []
            for j, sj in enumerate(sl_strats):
                if i == j:
                    continue
                c = _corr(sl_series[si], sl_series[sj])
                abs_corrs.append(abs(float(c)))
            ac = sum(abs_corrs) / float(len(abs_corrs)) if abs_corrs else 0.0
            s_avg_abs_corr[si] = float(ac)
            pen = 1.0 / (1.0 + max(0.0, float(CORR_GAMMA_STRAT)) * float(ac))
            s_corr_pen[si] = float(max(0.0, min(1.0, pen)))

        # scores
        s_scores: Dict[str, float] = {}
        for s in sl_strats:
            rets = sl_series.get(s) or []
            mu = sum(rets) / float(len(rets)) if rets else 0.0
            sd = _stddev(rets)
            sharpe = float(mu / sd) if sd > 1e-12 else 0.0
            dd = _max_drawdown_from_pnl_series(sl_pnl_series.get(s) or [])
            dd_sc = _drawdown_scale(dd)

            rpr_vals = strat_rpr.get(s) or []
            rpr_mean = sum(rpr_vals) / float(len(rpr_vals)) if rpr_vals else 0.0

            ddc_vals = strat_ddc.get(s) or []
            ddc_mean = sum(ddc_vals) / float(len(ddc_vals)) if ddc_vals else 0.0

            base = float(sharpe) + float(rpr_mean)
            score = float(base) * float(dd_sc) * float(s_corr_pen.get(s, 1.0))

            budget = float(strat_budgets.get(s, 1.0))
            if budget < 0.0:
                budget = 0.0
            score *= float(budget)

            if float(score) < float(SCORE_FLOOR):
                score = float(SCORE_FLOOR)

            s_scores[s] = float(max(0.0, score))

            strat_details[s] = {
                "sleeve": str(sl),
                "window_s": int(window_s),
                "bucket_s": int(bucket_s),
                "mean_bucket_pnl": float(mu),
                "std_bucket_pnl": float(sd),
                "sharpe_bucket": float(sharpe),
                "max_drawdown_proxy": float(dd),
                "dd_scale": float(dd_sc),
                "avg_abs_corr": float(s_avg_abs_corr.get(s, 0.0)),
                "corr_penalty": float(s_corr_pen.get(s, 1.0)),
                "return_per_risk_unit": float(rpr_mean),
                "drawdown_contribution": float(ddc_mean),
                "risk_budget": float(budget),
                "raw_score": float(s_scores[s]),
            }

        tot = sum(float(v) for v in s_scores.values())
        if tot <= 1e-12:
            tot = float(len(sl_strats) or 1)
            for s in sl_strats:
                s_scores[s] = 1.0

        s_w = {s: float(s_scores[s]) / float(tot) for s in sl_strats}

        # clamp and renormalize in-sleeve
        for s in sl_strats:
            w = float(s_w.get(s, 0.0))
            w = max(float(MIN_SHARE), min(float(MAX_SHARE), float(w)))
            s_w[s] = float(w)
        gg = sum(float(s_w.get(s, 0.0)) for s in sl_strats)
        if gg > 1e-12:
            for s in sl_strats:
                s_w[s] = float(s_w.get(s, 0.0)) / float(gg)

        # global alloc = sleeve_w * in-sleeve
        for s in sl_strats:
            strat_in_sleeve_alloc[s] = float(s_w.get(s, 0.0))
            strat_global_alloc[s] = float(sleeve_w.get(sl, 0.0)) * float(s_w.get(s, 0.0))

    # Persist (best-effort, fail-open) because allocation telemetry should not
    # break the trading loop if schema drift or DB pressure occurs.
    try:
        # sleeve_metrics upsert (window_days=0)
        for sl in sleeves:
            mj = dict(sleeve_details.get(sl) or {})
            con.execute(
                """
                INSERT INTO sleeve_metrics(sleeve_name, window_days, ts_ms, metrics_json, is_active)
                VALUES (?,?,?,?,?)
                ON CONFLICT(sleeve_name, window_days) DO UPDATE SET
                  ts_ms=excluded.ts_ms,
                  metrics_json=excluded.metrics_json,
                  is_active=excluded.is_active
                """,
                (
                    str(sl),
                    0,
                    int(ts_ms),
                    json.dumps(mj, separators=(",", ":"), sort_keys=True),
                    1,
                ),
            )

        con.execute(
            """
            INSERT INTO sleeve_allocations(ts_ms, window_days, allocations_json, reason_json)
            VALUES (?,?,?,?)
            ON CONFLICT(ts_ms, window_days) DO UPDATE SET
              allocations_json=excluded.allocations_json,
              reason_json=excluded.reason_json
            """,
            (
                int(ts_ms),
                0,
                json.dumps(sleeve_w, separators=(",", ":"), sort_keys=True),
                json.dumps(
                    {
                        "window_s": int(window_s),
                        "bucket_s": int(bucket_s),
                        "sleeves": sleeves,
                        "sleeve_budgets": sleeve_budgets,
                        "corr_gamma_sleeve": float(CORR_GAMMA_SLEEVE),
                        "dd_th": float(DD_TH),
                        "dd_floor": float(DD_FLOOR),
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ),
            ),
        )

        # strategy_metrics merge (window_days=0) + is_active
        for s, det in (strat_details or {}).items():
            merged = dict(det or {})
            merged["efficiency_score"] = float(merged.get("raw_score", 0.0))
            merged["allocator_kind"] = "hierarchical"
            merged["allocator_ts_ms"] = int(ts_ms)
            merged["allocator_sleeve_weight"] = float(sleeve_w.get(str(merged.get("sleeve")), 0.0))
            merged["allocator_in_sleeve_weight"] = float(strat_in_sleeve_alloc.get(str(s), 0.0))
            merged["allocator_global_weight"] = float(strat_global_alloc.get(str(s), 0.0))

            # preserve existing keys (execution_ledger writes base metrics); additive merge only
            try:
                row = con.execute(
                    """
                    SELECT metrics_json
                    FROM strategy_metrics
                    WHERE strategy_name=? AND window_days=0
                    """,
                    (str(s),),
                ).fetchone()
                if row and row[0]:
                    try:
                        base = json.loads(row[0] or "{}")
                        if isinstance(base, dict):
                            base.update(merged)
                            merged = base
                    except Exception as e:
                        _warn_nonfatal(
                            "HIER_ALLOC_STRATEGY_METRICS_PARSE_FAILED",
                            e,
                            once_key="strategy_metrics_parse",
                            strategy_name=str(s),
                        )
            except Exception as e:
                _warn_nonfatal(
                    "HIER_ALLOC_STRATEGY_METRICS_LOAD_FAILED",
                    e,
                    once_key="strategy_metrics_load",
                    strategy_name=str(s),
                )

            con.execute(
                """
                INSERT INTO strategy_metrics(strategy_name, window_days, ts_ms, metrics_json, is_active)
                VALUES (?,?,?,?,?)
                ON CONFLICT(strategy_name, window_days) DO UPDATE SET
                  ts_ms=excluded.ts_ms,
                  metrics_json=excluded.metrics_json,
                  is_active=excluded.is_active
                """,
                (
                    str(s),
                    0,
                    int(ts_ms),
                    json.dumps(merged, separators=(",", ":"), sort_keys=True),
                    1,
                ),
            )

        # strategy_allocations snapshot (window_days=0) stores per-strategy global weights
        con.execute(
            """
            INSERT INTO strategy_allocations(ts_ms, window_days, allocations_json, reason_json)
            VALUES (?,?,?,?)
            ON CONFLICT(ts_ms, window_days) DO UPDATE SET
              allocations_json=excluded.allocations_json,
              reason_json=excluded.reason_json
            """,
            (
                int(ts_ms),
                0,
                json.dumps(strat_global_alloc, separators=(",", ":"), sort_keys=True),
                json.dumps(
                    {
                        "window_s": int(window_s),
                        "bucket_s": int(bucket_s),
                        "corr_gamma_sleeve": float(CORR_GAMMA_SLEEVE),
                        "corr_gamma_strat": float(CORR_GAMMA_STRAT),
                        "sleeve_budgets": sleeve_budgets,
                        "strategy_budgets": strat_budgets,
                        "strategy_sleeve_map_env_present": bool(_STRATEGY_SLEEVE_MAP_RAW),
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ),
            ),
        )
    except Exception as e:
        _warn_nonfatal(
            "HIER_ALLOC_PERSIST_FAILED",
            e,
            once_key="hier_alloc_persist",
            ts_ms=int(ts_ms),
            strategy_count=int(len(strat_details)),
            sleeve_count=int(len(sleeve_details)),
        )

    return {
        "ok": True,
        "ts_ms": int(ts_ms),
        "window_s": int(window_s),
        "bucket_s": int(bucket_s),
        "sleeves": {
            "weights": dict(sleeve_w),
            "details": dict(sleeve_details),
        },
        "strategies": {
            "weights": dict(strat_global_alloc),
            "details": dict(strat_details),
            "strategy_to_sleeve": dict(strat_sleeve),
        },
    }
