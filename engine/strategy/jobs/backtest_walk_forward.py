"""
FILE: backtest_walk_forward.py

Operational helper script for `backtest_walk_forward`.
"""

# backtest_walk_forward.py
"""
Walk-forward backtest (SQLite-native).

For each labeled event (in chronological order):
- build a "past-only" KNN predictor using event_embeddings + per-symbol
  price feature-store snapshots computed strictly as-of this event ts
- predict expected_z
- evaluate vs realized impact_z

This is intentionally simple and honest:
- no leakage (uses only past)
- measures directional accuracy + MAE

Outputs summary per (symbol,horizon_s).
"""

import json
import math
import time
import os
import logging
import hashlib
import sys
from pathlib import Path
from typing import Dict, Any, Tuple, List, Optional

import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

LOG = get_logger("engine.strategy.jobs.backtest_walk_forward")


def _warn_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    log_failure(
        LOG,
        event="backtest_walk_forward_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.jobs.backtest_walk_forward",
        extra=extra or None,
        persist=False,
    )

from engine.runtime.storage import connect, init_db, run_write_txn
from engine.model_registry import get_best_model, load_model
from engine.strategy.validation import init_validation_db
from engine.strategy.model_lifecycle import record_version_performance
from engine.data import price_cache
from engine.data.feature_store import (
    FEATURE_NAMES,
    FEATURE_SET_TAG,
    get_features_asof,
)

SYMBOLS = ["SPY", "BTC", "OIL"]
HORIZONS = [300, 3600]

TOP_K = int(os.environ.get("WF_TOP_K", "8"))
HALF_LIFE_DAYS = float(os.environ.get("WF_HALF_LIFE_DAYS", "7.0"))
WARMUP_MIN_PAST = int(os.environ.get("WF_WARMUP_MIN_PAST", "10"))
MAX_EVENTS = int(os.environ.get("WF_MAX_EVENTS", "0"))  # 0 = no limit
WF_MODEL_SELECTION = str(os.environ.get("WF_MODEL_SELECTION", "best") or "best").strip().lower()
WF_MODEL_NAME = str(os.environ.get("WF_MODEL_NAME", "") or "").strip()
WF_MODEL_VERSION = str(os.environ.get("WF_MODEL_VERSION", "") or "").strip()
WF_REQUIRE_REGISTERED_MODEL = os.environ.get("WF_REQUIRE_REGISTERED_MODEL", "0") == "1"
_SUPPORTED_MODEL_SELECTIONS = {"best", "active", "explicit", "none"}

MS_PER_DAY = 24 * 3600 * 1000

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [backtest_walk_forward] %(message)s",
)


def _time_decay_weight(past_ts_ms: int, now_ms: int) -> float:
    age_days = max(0.0, (now_ms - past_ts_ms) / MS_PER_DAY)
    return math.exp(-age_days / max(1e-9, HALF_LIFE_DAYS))


def _make_run_id(params: Dict[str, Any]) -> str:
    raw = json.dumps(params, sort_keys=True).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:16]


def _ensure_schema(con) -> None:
    init_db()


def _feature_vec(
    con,
    cache: Dict[Tuple[str, int], np.ndarray],
    *,
    symbol: str,
    ts_ms: int,
) -> np.ndarray:
    key = (str(symbol).upper().strip(), int(ts_ms))
    if key in cache:
        return cache[key]

    snap = get_features_asof(
        symbol=str(symbol),
        ts_ms=int(ts_ms),
        price_cache=price_cache,
        con=con,
        persist=False,
    )
    vec = np.asarray(
        list((snap or {}).get("vector") or [float(((snap or {}).get("features") or {}).get(name, 0.0)) for name in FEATURE_NAMES]),
        dtype=np.float32,
    )
    cache[key] = vec
    return vec


def _normalize_symbol(symbol: str) -> str:
    return str(symbol or "").upper().strip()


def _catalog_model_summary(record: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(record, dict):
        return None
    return {
        "id": int(record.get("id") or 0),
        "model_name": str(record.get("model_name") or ""),
        "version": str(record.get("version") or ""),
        "model_kind": str(record.get("model_kind") or ""),
        "status": str(record.get("status") or ""),
        "is_active": bool(record.get("is_active")),
        "selection_metric_name": (
            str(record.get("best_metric_name") or record.get("selection_metric_name") or "").strip() or None
        ),
        "selection_metric_value": (
            float(record.get("best_metric_value"))
            if record.get("best_metric_value") is not None
            else (
                float(record.get("selection_metric_value"))
                if record.get("selection_metric_value") is not None
                else None
            )
        ),
    }


def _resolve_catalog_model(symbol: str) -> Optional[Dict[str, Any]]:
    symbol_u = _normalize_symbol(symbol)
    if not symbol_u:
        return None
    if WF_MODEL_SELECTION not in _SUPPORTED_MODEL_SELECTIONS:
        raise ValueError(
            f"unsupported WF_MODEL_SELECTION={WF_MODEL_SELECTION!r}; expected one of {sorted(_SUPPORTED_MODEL_SELECTIONS)}"
        )
    if WF_MODEL_VERSION and not WF_MODEL_NAME:
        raise ValueError("WF_MODEL_VERSION requires WF_MODEL_NAME")
    if WF_MODEL_SELECTION == "none":
        return None
    if WF_MODEL_VERSION:
        return load_model(symbol_u, model_name=WF_MODEL_NAME, version=WF_MODEL_VERSION)
    if WF_MODEL_SELECTION == "explicit":
        if not WF_MODEL_NAME:
            raise ValueError("WF_MODEL_SELECTION=explicit requires WF_MODEL_NAME")
        return load_model(symbol_u, model_name=WF_MODEL_NAME)
    if WF_MODEL_SELECTION == "active":
        return load_model(symbol_u, model_name=(WF_MODEL_NAME or None), active_only=True)
    rec = get_best_model(symbol_u, model_name=(WF_MODEL_NAME or None))
    if rec is not None:
        return rec
    if WF_MODEL_NAME:
        return load_model(symbol_u, model_name=WF_MODEL_NAME, active_only=True) or load_model(
            symbol_u, model_name=WF_MODEL_NAME
        )
    return None


def _resolve_walk_forward_registry(symbols: List[str]) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    resolved_models: Dict[str, Dict[str, Any]] = {}
    missing_symbols: List[str] = []
    for symbol in symbols:
        symbol_u = _normalize_symbol(symbol)
        rec = _resolve_catalog_model(symbol_u)
        if rec is None:
            missing_symbols.append(symbol_u)
            continue
        resolved_models[symbol_u] = dict(rec)
    snapshot = {
        "selection_mode": str(WF_MODEL_SELECTION),
        "model_name_filter": (str(WF_MODEL_NAME) if WF_MODEL_NAME else None),
        "model_version_filter": (str(WF_MODEL_VERSION) if WF_MODEL_VERSION else None),
        "require_registered_model": bool(WF_REQUIRE_REGISTERED_MODEL),
        "selected_models": {
            str(symbol): _catalog_model_summary(rec)
            for symbol, rec in sorted(resolved_models.items())
        },
        "missing_symbols": list(sorted(missing_symbols)),
    }
    return resolved_models, snapshot


def main() -> int:
    init_db()
    init_validation_db()
    con = connect()
    try:
        _ensure_schema(con)
        resolved_models, registry_snapshot = _resolve_walk_forward_registry(SYMBOLS)
        if WF_REQUIRE_REGISTERED_MODEL and registry_snapshot.get("missing_symbols"):
            missing_symbols = ",".join(str(x) for x in registry_snapshot.get("missing_symbols") or [])
            logging.error("walk_forward missing required registered models symbols=%s", missing_symbols)
            print(f"Missing registered models for symbols: {missing_symbols}")
            return 2

        params = {
            "symbols": SYMBOLS,
            "horizons": HORIZONS,
            "top_k": TOP_K,
            "half_life_days": HALF_LIFE_DAYS,
            "warmup_min_past": WARMUP_MIN_PAST,
            "max_events": MAX_EVENTS,
            "feature_set_tag": FEATURE_SET_TAG,
            "registry": registry_snapshot,
        }
        run_id = _make_run_id(params)
        logging.info(
            "WALK_FORWARD registry_selection mode=%s selected=%s missing=%s",
            str(registry_snapshot.get("selection_mode") or ""),
            len(registry_snapshot.get("selected_models") or {}),
            len(registry_snapshot.get("missing_symbols") or {}),
        )

        # Chronological ordering is the whole point of this script: it is meant to
        # be an honest past-only benchmark, not a convenience full-sample score.
        # Pull labeled events with embeddings, chronological
        rows = con.execute(
            """
            SELECT e.id, e.ts_ms, emb.vec
            FROM events e
            JOIN event_embeddings emb ON emb.event_id = e.id
            WHERE EXISTS (
              SELECT 1 FROM labels l
              WHERE l.event_id=e.id AND l.impact_z IS NOT NULL
            )
            ORDER BY e.ts_ms ASC
            """
        ).fetchall()

        if not rows:
            print("No labeled embedded events found.")
            return 0

        if MAX_EVENTS and len(rows) > MAX_EVENTS:
            rows = rows[:MAX_EVENTS]

        # Labels map: (event_id, symbol, horizon_s) -> impact_z
        lbls = con.execute(
            """
            SELECT event_id, symbol, horizon_s, impact_z
            FROM labels
            WHERE impact_z IS NOT NULL
            """
        ).fetchall()
        label_map: Dict[Tuple[int, str, int], float] = {
            (int(eid), str(sym), int(h)): float(z) for (eid, sym, h, z) in lbls
        }

        # Pre-decode vectors and timestamps
        ids: List[int] = [int(r[0]) for r in rows]
        tss: List[int] = [int(r[1]) for r in rows]
        vecs: List[np.ndarray] = [np.frombuffer(r[2], dtype=np.float32) for r in rows]
        feature_cache: Dict[Tuple[str, int], np.ndarray] = {}

        # Accumulators per (sym,h)
        agg: Dict[Tuple[str, int], Dict[str, Any]] = {}
        for sym in SYMBOLS:
            for h in HORIZONS:
                agg[(str(sym), int(h))] = {"n": 0, "mae_sum": 0.0, "dir_ok": 0}

        # Each step rebuilds the candidate neighbor set from past events only.
        # That is slower than vectorizing the whole backtest, but avoids leakage.
        # Walk-forward loop
        for i in range(len(ids)):
            eid = ids[i]
            ts_now = tss[i]
            q = vecs[i]

            if i < WARMUP_MIN_PAST:
                continue

            for sym in SYMBOLS:
                q_feat = _feature_vec(con, feature_cache, symbol=str(sym), ts_ms=int(ts_now))
                q_full = np.concatenate([q.astype(np.float32, copy=False), q_feat]).astype(np.float32, copy=False)

                past_pairs: List[Tuple[int, int]] = []
                past_full: List[np.ndarray] = []
                for peid, pts, pvec in zip(ids[:i], tss[:i], vecs[:i]):
                    p_feat = _feature_vec(con, feature_cache, symbol=str(sym), ts_ms=int(pts))
                    past_pairs.append((int(peid), int(pts)))
                    past_full.append(
                        np.concatenate([pvec.astype(np.float32, copy=False), p_feat]).astype(np.float32, copy=False)
                    )

                if len(past_full) == 0:
                    continue

                query = np.expand_dims(q_full, axis=0).astype(np.float32, copy=False)
                past_matrix = np.stack(past_full).astype(np.float32, copy=False)
                sims = cosine_similarity(query, past_matrix)[0]

                for h in HORIZONS:
                    key_now = (int(eid), str(sym), int(h))
                    if key_now not in label_map:
                        continue

                    scored = []
                    for (peid, pts), sim in zip(past_pairs, sims):
                        if sim <= 0:
                            continue
                        k = (int(peid), str(sym), int(h))
                        if k not in label_map:
                            continue
                        decay = _time_decay_weight(int(pts), int(ts_now))
                        w = float(sim) * float(decay)
                        if w <= 0:
                            continue
                        scored.append((w, float(label_map[k])))

                    if not scored:
                        continue

                    scored.sort(reverse=True, key=lambda x: x[0])
                    top = scored[:TOP_K]
                    wsum = sum(w for w, _ in top)
                    if wsum <= 0:
                        continue

                    pred = sum(w * z for w, z in top) / wsum
                    real = float(label_map[key_now])

                    m = agg[(str(sym), int(h))]
                    m["n"] += 1
                    m["mae_sum"] += abs(pred - real)
                    if (pred >= 0 and real >= 0) or (pred < 0 and real < 0):
                        m["dir_ok"] += 1

        # Results are stored as governance/audit artifacts. They are not directly
        # consumed by the live runtime as a trading signal.
        # Finalize metrics and store
        now_ms = int(time.time() * 1000)
        summary: Dict[str, Any] = {
            "run_id": run_id,
            "params": params,
            "per_key": {},
        }

        for sym in SYMBOLS:
            for h in HORIZONS:
                m = agg[(str(sym), int(h))]
                n = int(m["n"])
                if n <= 0:
                    continue
                mae = float(m["mae_sum"]) / float(n)
                acc = float(m["dir_ok"]) / float(n)
                selected_model = resolved_models.get(_normalize_symbol(sym))
                selected_model_name = str(selected_model.get("model_name") or "") if selected_model else None
                selected_model_version = str(selected_model.get("version") or "") if selected_model else None
                selected_model_kind = str(selected_model.get("model_kind") or "") if selected_model else None

                summary["per_key"][f"{sym}:{h}"] = {
                    "n": n,
                    "mae": mae,
                    "dir_acc": acc,
                    "model_name": selected_model_name,
                    "model_version": selected_model_version,
                    "model_kind": selected_model_kind,
                }

                def _write_score(conw):
                    conw.execute(
                        """
                        INSERT OR REPLACE INTO walk_forward_scores(
                          run_id, symbol, horizon_s, ts_ms, n, mae, dir_acc,
                          model_name, model_version, model_kind
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            str(run_id),
                            str(sym),
                            int(h),
                            int(now_ms),
                            int(n),
                            float(mae),
                            float(acc),
                            selected_model_name,
                            selected_model_version,
                            selected_model_kind,
                        ),
                    )

                run_write_txn(_write_score)
                if selected_model_name and selected_model_version:
                    try:
                        record_version_performance(
                            model_name=str(selected_model_name),
                            model_version=str(selected_model_version),
                            metric_scope=f"walk_forward:{_normalize_symbol(sym)}:{int(h)}",
                            metrics={
                                "wf_mae": float(mae),
                                "wf_dir_acc": float(acc),
                            },
                            sample_n=int(n),
                            meta={
                                "run_id": str(run_id),
                                "symbol": _normalize_symbol(sym),
                                "horizon_s": int(h),
                                "model_kind": (str(selected_model_kind) if selected_model_kind else None),
                                "registry_model_id": int(selected_model.get("id") or 0),
                                "feature_set_tag": str(FEATURE_SET_TAG),
                                "selection_mode": str(WF_MODEL_SELECTION),
                            },
                        )
                    except Exception as e:
                        _warn_nonfatal(
                            "BACKTEST_WALK_FORWARD_RECORD_VERSION_PERFORMANCE_FAILED",
                            e,
                            model_name=str(selected_model_name),
                            model_version=str(selected_model_version),
                            symbol=str(sym),
                            horizon_s=int(h),
                            run_id=str(run_id),
                        )

        # Run-level metrics (simple aggregates over keys)
        all_keys = list(summary["per_key"].values())
        if all_keys:
            total_n = sum(int(x.get("n", 0)) for x in all_keys)
            w_mae = 0.0
            w_acc = 0.0
            for x in all_keys:
                wn = float(x.get("n", 0))
                if wn <= 0:
                    continue
                w_mae += wn * float(x.get("mae", 0.0))
                w_acc += wn * float(x.get("dir_acc", 0.0))
            run_metrics = {
                "total_n": int(total_n),
                "mae": float(w_mae / max(1.0, float(total_n))),
                "dir_acc": float(w_acc / max(1.0, float(total_n))),
                "n_keys": int(len(all_keys)),
            }
        else:
            run_metrics = {"total_n": 0, "mae": None, "dir_acc": None, "n_keys": 0}

        def _write_run(conw):
            conw.execute(
                """
                INSERT OR REPLACE INTO walk_forward_runs(run_id, params_json, metrics_json, ts_ms, model_selection_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    str(run_id),
                    json.dumps(params, separators=(",", ":"), sort_keys=True),
                    json.dumps(run_metrics, separators=(",", ":"), sort_keys=True),
                    int(now_ms),
                    json.dumps(registry_snapshot, separators=(",", ":"), sort_keys=True),
                ),
            )

        run_write_txn(_write_run)

        logging.info(
            "WALK_FORWARD run=%s total_n=%s mae=%s dir_acc=%s n_keys=%s",
            str(run_id),
            int(run_metrics.get("total_n") or 0),
            run_metrics.get("mae"),
            run_metrics.get("dir_acc"),
            int(run_metrics.get("n_keys") or 0),
        )

        print("\nWALK-FORWARD SUMMARY")
        for sym in SYMBOLS:
            for h in HORIZONS:
                k = f"{sym}:{h}"
                if k not in summary["per_key"]:
                    continue
                x = summary["per_key"][k]
                print(f"{sym} h={h} n={int(x['n'])} MAE={float(x['mae']):.3f} DirAcc={float(x['dir_acc']):.3f}")

        print("\nRUN METRICS")
        print(json.dumps({"run_id": run_id, "metrics": run_metrics}, indent=2))

        print("\nDONE (tables: walk_forward_runs, walk_forward_scores)")
        try:
            con.commit()
        except Exception as e:
            _warn_nonfatal("BACKTEST_WALK_FORWARD_COMMIT_FAILED", e, run_id=str(run_id))
        return 0

    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
