"""Run combinatorial purged CV backtests with Almgren-Chriss costs."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List

import numpy as np

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from engine.backtest.cpcv import CombinatorialPurgedKFold
from engine.backtest.deflated_sharpe import deflated_sharpe_ratio
from engine.runtime.storage import (
    acquire_job_lock,
    connect,
    init_db,
    put_job_heartbeat,
    record_backtest_cpcv_path_result,
    release_job_lock,
    touch_job_lock,
)
from engine.strategy.cpcv import _apply_transaction_costs_to_returns, cpcv_cost_config_from_env

JOB_NAME = "backtest_cpcv"
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))
MS_PER_DAY = 86_400_000


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    return out if math.isfinite(out) else float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _json_load_file(path: str) -> dict:
    text = Path(path).read_text(encoding="utf-8")
    value = json.loads(text)
    if not isinstance(value, dict):
        raise ValueError("config JSON must be an object")
    return value


def _default_config() -> Dict[str, Any]:
    return {
        "model_id": str(os.environ.get("CPCV_MODEL_ID", "temporal_predictor") or "temporal_predictor"),
        "lookback_days": _safe_int(os.environ.get("CPCV_LOOKBACK_DAYS"), 548),
        "n_splits": _safe_int(os.environ.get("CPCV_N_SPLITS"), 6),
        "n_test_splits": _safe_int(os.environ.get("CPCV_N_TEST_SPLITS"), 2),
        "embargo_pct": _safe_float(os.environ.get("CPCV_EMBARGO_PCT"), 0.01),
        "holding_horizon_bars": _safe_int(os.environ.get("CPCV_HOLDING_HORIZON_BARS"), 2),
        "notional": _safe_float(os.environ.get("CPCV_TRADE_NOTIONAL"), 100_000.0),
        "adv": _safe_float(os.environ.get("CPCV_ADV"), 10_000_000.0),
        "sigma_daily": _safe_float(os.environ.get("CPCV_SIGMA_DAILY"), 200.0),
        "participation": _safe_float(os.environ.get("CPCV_PARTICIPATION"), 0.10),
        "half_spread_bps": _safe_float(os.environ.get("CPCV_HALF_SPREAD_BPS"), 1.0),
        "max_rows": _safe_int(os.environ.get("CPCV_MAX_ROWS"), 0),
    }


def load_config(path: str | None = None) -> Dict[str, Any]:
    cfg = _default_config()
    if path:
        cfg.update(_json_load_file(path))
    cfg["n_splits"] = max(2, int(cfg.get("n_splits") or 6))
    cfg["n_test_splits"] = max(1, min(int(cfg.get("n_test_splits") or 2), int(cfg["n_splits"]) - 1))
    cfg["embargo_pct"] = max(0.0, float(cfg.get("embargo_pct") or 0.0))
    cfg["holding_horizon_bars"] = max(1, int(cfg.get("holding_horizon_bars") or 1))
    cfg["run_id"] = hashlib.sha256(json.dumps(cfg, sort_keys=True, default=str).encode("utf-8")).hexdigest()[:16]
    return cfg


def _query_prediction_rows(con, cfg: Dict[str, Any]) -> list:
    model_id = str(cfg.get("model_id") or "").strip()
    cutoff_ms = int(time.time() * 1000) - int(cfg.get("lookback_days") or 548) * MS_PER_DAY
    params: list[Any] = [int(cutoff_ms)]
    model_filter = ""
    if model_id:
        model_filter = "AND (p.model_id=? OR p.model_name=? OR p.model_version=?)"
        params.extend([model_id, model_id, model_id])
    sql = f"""
        SELECT COALESCE(p.ts_ms, e.ts_ms) AS ts_ms,
               COALESCE(p.symbol, l.symbol) AS symbol,
               COALESCE(p.horizon_s, l.horizon_s) AS horizon_s,
               p.predicted_z,
               COALESCE(p.confidence, 0.0) AS confidence,
               l.impact_z
        FROM predictions p
        JOIN labels l
          ON l.event_id = p.event_id
         AND l.symbol = p.symbol
         AND l.horizon_s = p.horizon_s
        LEFT JOIN events e ON e.id = l.event_id
        WHERE COALESCE(p.ts_ms, e.ts_ms) >= ?
          AND l.impact_z IS NOT NULL
          AND p.predicted_z IS NOT NULL
          {model_filter}
        ORDER BY COALESCE(p.ts_ms, e.ts_ms) ASC, symbol ASC, horizon_s ASC
    """
    try:
        return con.execute(sql, tuple(params)).fetchall() or []
    except Exception:
        return []


def _query_label_rows(con, cfg: Dict[str, Any]) -> list:
    cutoff_ms = int(time.time() * 1000) - int(cfg.get("lookback_days") or 548) * MS_PER_DAY
    try:
        return con.execute(
            """
            SELECT e.ts_ms, l.symbol, l.horizon_s, NULL AS predicted_z,
                   0.0 AS confidence, l.impact_z
            FROM labels l
            JOIN events e ON e.id = l.event_id
            WHERE e.ts_ms >= ?
              AND l.impact_z IS NOT NULL
            ORDER BY e.ts_ms ASC, l.symbol ASC, l.horizon_s ASC
            """,
            (int(cutoff_ms),),
        ).fetchall() or []
    except Exception:
        return []


def _query_price_label_rows(con, cfg: Dict[str, Any]) -> list:
    cutoff_ms = int(time.time() * 1000) - int(cfg.get("lookback_days") or 548) * MS_PER_DAY
    try:
        return con.execute(
            """
            SELECT ts_pred_ms AS ts_ms, symbol, horizon_s,
                   NULL AS predicted_z, 0.0 AS confidence,
                   COALESCE(ret_z, ret) AS impact_z
            FROM labels_price
            WHERE ts_pred_ms >= ?
              AND COALESCE(ret_z, ret) IS NOT NULL
            ORDER BY ts_pred_ms ASC, symbol ASC, horizon_s ASC
            """,
            (int(cutoff_ms),),
        ).fetchall() or []
    except Exception:
        return []


def _query_temporal_predictor_rows(con, cfg: Dict[str, Any]) -> list:
    if not bool(cfg.get("use_temporal_predictor", True)):
        return []
    model_id = str(cfg.get("model_id") or "").strip().lower()
    if model_id and "temporal_predictor" not in model_id:
        return []
    try:
        from engine.strategy.temporal_predictor import predict_temporal_live
    except Exception:
        return []

    source_rows = _query_label_rows(con, cfg) or _query_price_label_rows(con, cfg)
    if not source_rows:
        return []
    max_rows = int(cfg.get("max_rows") or 0)
    if max_rows > 0:
        source_rows = source_rows[-max_rows:]

    out = []
    for ts_ms, symbol, horizon_s, _predicted_z, _confidence, impact_z in source_rows:
        sym = str(symbol or "").upper().strip()
        horizon = int(horizon_s or 0)
        if not sym or horizon <= 0:
            continue
        try:
            predictions = predict_temporal_live(con, int(ts_ms or 0), [sym], [horizon])
        except Exception:
            predictions = None
        item = (predictions or {}).get((sym, horizon)) if isinstance(predictions, dict) else None
        if not item:
            continue
        pred_z, conf, _explain = item
        out.append((int(ts_ms or 0), sym, horizon, float(pred_z), float(conf), float(impact_z)))
    return out


def load_dataset(con, cfg: Dict[str, Any]) -> Dict[str, Any]:
    rows = _query_temporal_predictor_rows(con, cfg)
    source = "temporal_predictor"
    uses_precomputed_predictions = bool(rows)
    if not rows:
        rows = _query_prediction_rows(con, cfg)
        source = "predictions"
        uses_precomputed_predictions = bool(rows)
    if not rows:
        rows = _query_price_label_rows(con, cfg)
        source = "labels_price"
        uses_precomputed_predictions = False
    if not rows:
        rows = _query_label_rows(con, cfg)
        source = "labels"
        uses_precomputed_predictions = False

    max_rows = int(cfg.get("max_rows") or 0)
    if max_rows > 0:
        rows = rows[-max_rows:]

    records: list[dict] = []
    last_y_by_key: dict[tuple[str, int], float] = {}
    symbol_ids: dict[str, int] = {}
    for ts_ms, symbol, horizon_s, predicted_z, confidence, impact_z in rows:
        sym = str(symbol or "").upper().strip()
        horizon = int(horizon_s or 0)
        if not sym or horizon <= 0:
            continue
        y = _safe_float(impact_z, float("nan"))
        if not math.isfinite(y):
            continue
        if sym not in symbol_ids:
            symbol_ids[sym] = len(symbol_ids)
        lag = float(last_y_by_key.get((sym, horizon), 0.0))
        pred = predicted_z
        if pred is None:
            pred = lag
        pred_f = _safe_float(pred, 0.0)
        conf_f = _safe_float(confidence, 0.0)
        records.append(
            {
                "ts_ms": int(ts_ms or 0),
                "symbol": sym,
                "horizon_s": horizon,
                "predicted_z": float(pred_f),
                "confidence": float(conf_f),
                "lag_impact": float(lag),
                "symbol_id": int(symbol_ids[sym]),
                "impact_z": float(y),
            }
        )
        last_y_by_key[(sym, horizon)] = float(y)

    if not records:
        return {
            "rows": [],
            "X": np.zeros((0, 1), dtype=float),
            "y": np.zeros((0,), dtype=float),
            "source": source,
            "uses_precomputed_predictions": bool(uses_precomputed_predictions),
        }

    max_horizon = max(1, max(int(row["horizon_s"]) for row in records))
    max_symbol = max(1, max(int(row["symbol_id"]) for row in records))
    X = np.asarray(
        [
            [
                float(row["predicted_z"]),
                float(row["confidence"]),
                float(row["lag_impact"]),
                float(row["horizon_s"]) / float(max_horizon),
                float(row["symbol_id"]) / float(max_symbol),
            ]
            for row in records
        ],
        dtype=float,
    )
    y = np.asarray([float(row["impact_z"]) for row in records], dtype=float)
    return {
        "rows": records,
        "X": X,
        "y": y,
        "source": source,
        "uses_precomputed_predictions": bool(uses_precomputed_predictions),
    }


class _LinearModel:
    def __init__(self, ridge: float = 1e-8):
        self.ridge = float(ridge)
        self.coef_: np.ndarray | None = None

    def fit(self, X: np.ndarray, y: np.ndarray) -> "_LinearModel":
        design = np.concatenate([np.ones((X.shape[0], 1), dtype=float), np.asarray(X, dtype=float)], axis=1)
        gram = design.T @ design
        if self.ridge > 0.0:
            penalty = np.eye(gram.shape[0], dtype=float) * self.ridge
            penalty[0, 0] = 0.0
            gram = gram + penalty
        target = design.T @ np.asarray(y, dtype=float).reshape(-1)
        try:
            self.coef_ = np.linalg.solve(gram, target)
        except np.linalg.LinAlgError:
            self.coef_ = np.linalg.lstsq(design, np.asarray(y, dtype=float).reshape(-1), rcond=None)[0]
        return self

    def predict(self, X: np.ndarray) -> np.ndarray:
        if self.coef_ is None:
            raise RuntimeError("model_not_fit")
        design = np.concatenate([np.ones((X.shape[0], 1), dtype=float), np.asarray(X, dtype=float)], axis=1)
        return np.asarray(design @ self.coef_, dtype=float)


def _sharpe(returns: Iterable[float]) -> float:
    arr = np.asarray([] if returns is None else list(returns), dtype=float).reshape(-1)
    arr = arr[np.isfinite(arr)]
    if arr.size <= 1:
        return 0.0
    std = float(arr.std(ddof=1))
    if std <= 1e-12:
        return 0.0
    return float(arr.mean() / std * math.sqrt(float(arr.size)))


def _max_drawdown(returns: Iterable[float]) -> float:
    arr = np.asarray([] if returns is None else list(returns), dtype=float).reshape(-1)
    if arr.size == 0:
        return 0.0
    equity = np.cumprod(1.0 + arr)
    peak = np.maximum.accumulate(equity)
    drawdown = (equity / np.maximum(peak, 1e-12)) - 1.0
    return float(abs(np.min(drawdown)))


def run_cpcv_backtest(cfg: Dict[str, Any], *, con=None, persist: bool = True) -> Dict[str, Any]:
    owns_connection = con is None
    if con is None:
        init_db()
        con = connect()
    try:
        dataset = load_dataset(con, cfg)
        rows = list(dataset.get("rows") or [])
        X = np.asarray(dataset.get("X"), dtype=float)
        y = np.asarray(dataset.get("y"), dtype=float).reshape(-1)
        uses_precomputed = bool(dataset.get("uses_precomputed_predictions"))
        if y.size < int(cfg.get("n_splits") or 6):
            return {
                "ok": False,
                "status": "insufficient_samples",
                "n_samples": int(y.size),
                "paths": [],
                "dataset_source": str(dataset.get("source") or ""),
            }

        label_end = np.arange(y.size, dtype=float) + float(max(0, int(cfg.get("holding_horizon_bars") or 1)))
        splitter = CombinatorialPurgedKFold(
            n_splits=int(cfg.get("n_splits") or 6),
            n_test_splits=int(cfg.get("n_test_splits") or 2),
            embargo=float(cfg.get("embargo_pct") or 0.0),
            label_end_times=label_end,
        )
        cost_config = cpcv_cost_config_from_env(cfg)

        paths: list[dict] = []
        for path_index, (train_idx, test_idx) in enumerate(splitter.split(X)):
            if train_idx.size < 2 or test_idx.size < 1:
                continue
            if uses_precomputed:
                pred = X[test_idx, 0]
                model_source = str(dataset.get("source") or "predictions")
            else:
                model = _LinearModel().fit(X[train_idx], y[train_idx])
                pred = model.predict(X[test_idx])
                model_source = "fold_trained_linear_baseline"
            gross = np.sign(pred) * y[test_idx]
            net, cost_meta = _apply_transaction_costs_to_returns(
                pred,
                y[test_idx],
                cost_config=cost_config,
            )
            path = {
                "path_index": int(path_index),
                "train_size": int(train_idx.size),
                "test_size": int(test_idx.size),
                "sharpe": float(_sharpe(net)),
                "frictionless_sharpe": float(_sharpe(gross)),
                "total_return": float(np.prod(1.0 + net) - 1.0),
                "max_drawdown": float(_max_drawdown(net)),
                "returns": [float(value) for value in net.tolist()],
                "cost_adjusted_returns": [float(value) for value in net.tolist()],
                "frictionless_returns": [float(value) for value in gross.tolist()],
                "cost_model": dict(cost_config),
                "costs": dict(cost_meta),
                "model_source": str(model_source),
                "test_start_ts": int(rows[int(test_idx[0])]["ts_ms"]),
                "test_end_ts": int(rows[int(test_idx[-1])]["ts_ms"]),
            }
            paths.append(path)

        sharpes = [float(path["sharpe"]) for path in paths]
        dsr_best = deflated_sharpe_ratio(sharpes)
        dsr_by_path = {
            int(path["path_index"]): deflated_sharpe_ratio(
                sharpes,
                realized_sharpe=float(path["sharpe"]),
                n_trials=len(sharpes),
            )
            for path in paths
        }

        if persist:
            ts_ms = int(time.time() * 1000)
            for path in paths:
                dsr = dsr_by_path[int(path["path_index"])]
                record_backtest_cpcv_path_result(
                    ts=ts_ms,
                    model_id=str(cfg.get("model_id") or "temporal_predictor"),
                    cfg=cfg,
                    path_index=int(path["path_index"]),
                    sharpe=float(path["sharpe"]),
                    deflated_sharpe=float(dsr.deflated_sharpe),
                    n_trials=int(len(paths)),
                    total_return=float(path["total_return"]),
                    max_drawdown=float(path["max_drawdown"]),
                    payload={
                        **path,
                        "deflated_sharpe": dsr.to_dict(),
                        "deflated_sharpe_best": dsr_best.to_dict(),
                        "metric_basis": "cost_adjusted",
                        "dataset_source": str(dataset.get("source") or ""),
                    },
                    con=con,
                )
            con.commit()

        return {
            "ok": bool(paths),
            "status": "evaluated" if paths else "no_valid_paths",
            "model_id": str(cfg.get("model_id") or ""),
            "n_samples": int(y.size),
            "n_paths": int(len(paths)),
            "sharpes": sharpes,
            "mean_sharpe": float(np.mean(sharpes)) if sharpes else 0.0,
            "deflated_sharpe": dsr_best.to_dict(),
            "paths": paths,
            "metric_basis": "cost_adjusted",
            "frictionless_mean_sharpe": (
                float(np.mean([float(path.get("frictionless_sharpe") or 0.0) for path in paths])) if paths else 0.0
            ),
            "dataset_source": str(dataset.get("source") or ""),
            "uses_precomputed_predictions": bool(uses_precomputed),
        }
    finally:
        if owns_connection and con is not None:
            con.close()


def _run_cli(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run CPCV backtest with Almgren-Chriss costs")
    parser.add_argument("--config", default="", help="Optional JSON config path")
    args = parser.parse_args(argv)
    cfg = load_config(args.config or None)
    result = run_cpcv_backtest(cfg, persist=True)
    print(json.dumps({k: v for k, v in result.items() if k != "paths"}, indent=2, sort_keys=True))
    return 0 if bool(result.get("ok")) else 1


def main(argv: List[str] | None = None) -> int:
    init_db()

    if not acquire_job_lock(JOB_NAME, OWNER, PID, stale_after_s=LOCK_STALE_AFTER_S):
        return 0

    try:
        touch_job_lock(JOB_NAME, OWNER, PID)
        put_job_heartbeat(
            JOB_NAME,
            OWNER,
            PID,
            extra_json=json.dumps({"phase": "start"}, separators=(",", ":"), sort_keys=True),
        )
        rc = int(_run_cli(argv) or 0)
        touch_job_lock(JOB_NAME, OWNER, PID)
        put_job_heartbeat(
            JOB_NAME,
            OWNER,
            PID,
            extra_json=json.dumps({"phase": "done", "rc": rc}, separators=(",", ":"), sort_keys=True),
        )
        return rc
    finally:
        release_job_lock(JOB_NAME, OWNER, PID)


if __name__ == "__main__":
    raise SystemExit(main())
